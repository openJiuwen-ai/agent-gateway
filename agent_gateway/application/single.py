# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Single-process host retaining the original staged startup and shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import logging
import os
import time
import uuid as uuid_module
from typing import Any

from e2a.gateway_normalize import e2a_from_agent_fields
from agent_gateway.common.protocol_methods import ReqMethod
from agent_gateway.application.connection import _wait_for_web_channel_listening
from agent_gateway.application.inbound import MessagePreprocessor
from agent_gateway.application.services import assemble_services
from agent_gateway.application.web import assemble_web
from agent_gateway.application.tui_acp import assemble_tui_acp
from agent_gateway.telemetry import shutdown_gateway_telemetry

logger = logging.getLogger("gateway")
_AGENT_PREWARM_EXCLUDED_CHANNELS = frozenset({"acp", "a2a"})
_FEISHU_DEFAULT_API_BASE = "https://open.feishu.cn"
_DINGTALK_DEFAULT_API_BASE = "https://api.dingtalk.com"
_DINGTALK_DEFAULT_OAPI_BASE = "https://oapi.dingtalk.com"
_XIAOYI_DEFAULT_PUSH_URL = "https://hag.cloud.huawei.com/open-ability-agent/v1/agent-webhook"


def _agent_prewarm_enabled() -> bool:
    """Return whether background session prewarming is switched on.

    Prewarming is opt-in via JIUWENSWARM_AGENT_PREWARM; when off the Gateway
    must not emit agent.prewarm.sync requests or related log noise.
    """
    from agent_gateway.runtime.agent_prewarm import prewarm_enabled_by_env

    return prewarm_enabled_by_env()


@dataclass
class GatewayRestartRequest:
    requested: bool = False
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)


async def _wait_for_gateway_tasks_or_restart(
    tasks_to_wait: list[asyncio.Task],
    restart_request: GatewayRestartRequest,
    shutdown_event: asyncio.Event,
) -> bool:
    restart_task = asyncio.create_task(restart_request.ready_event.wait(), name="gateway-restart")
    shutdown_task = asyncio.create_task(shutdown_event.wait(), name="gateway-shutdown")
    service_tasks = set(tasks_to_wait)
    wait_tasks = set(service_tasks)
    wait_tasks.update((restart_task, shutdown_task))
    try:
        while wait_tasks:
            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
            if shutdown_task in done:
                return False
            if restart_task in done:
                return restart_request.requested or restart_request.ready_event.is_set()
            for task in done:
                wait_tasks.discard(task)
                service_tasks.discard(task)
                if task.cancelled():
                    raise asyncio.CancelledError
                exc = task.exception()
                if exc is not None:
                    if restart_request.requested:
                        logger.warning("[App] service task failed while Gateway restart is pending: %s", exc)
                        return True
                    raise exc
            if not service_tasks:
                return restart_request.requested or restart_request.ready_event.is_set()
        return restart_request.requested or restart_request.ready_event.is_set()
    finally:
        if not restart_task.done():
            restart_task.cancel()
            try:
                await restart_task
            except asyncio.CancelledError:
                pass
        if not shutdown_task.done():
            shutdown_task.cancel()
            try:
                await shutdown_task
            except asyncio.CancelledError:
                pass


async def start_web_core(
    web_channel: Any,
    channel_manager: Any,
    cron_scheduler: Any,
    config_updater: Any,
    log_stage: Callable[[str], None],
) -> asyncio.Task[None]:
    """Keep Web ready before dispatch, Cron, and optional TUI/ACP construction."""
    web_task = asyncio.create_task(web_channel.start(), name="web-channel")
    try:
        await _wait_for_web_channel_listening(web_channel, web_task)
        log_stage("web_channel_listening")
        await channel_manager.start_dispatch()
        log_stage("message_dispatch_started")
        await cron_scheduler.start()
        if config_updater is not None:
            await config_updater.start()
        log_stage("cron_scheduler_started")
        await asyncio.sleep(0.20)
        log_stage("web_core_ready")
    except BaseException:
        web_task.cancel()
        await asyncio.gather(web_task, return_exceptions=True)
        raise
    return web_task


async def run_single(
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
        web_ws_deflate: bool = False,
        *,
        config_saved_factory: Callable[..., Any],
        restart_gateway: Callable[[], None],
        process_start_t0: float,
        startup_import_phases: list[tuple[str, float]],
        preprocessor: MessagePreprocessor,
        connect_with_retry: Callable[..., Awaitable[None]],
) -> None:
    from agent_gateway.common.config import get_config
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _DummyBus,
        _normalize_feishu_conf,
        _normalize_xiaoyi_conf,
    )
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_connect import WebProxyChannelConfig
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_listen import (
        WebPortManager,
        register_3rdagent_web_method,
    )

    logger.info("[App] Gateway starting, connecting AgentServer: %s", agent_server_url)
    for import_stage, marked_at in startup_import_phases:
        logger.info(
            "[App] startup import stage=%s process_elapsed=%.2fs",
            import_stage,
            marked_at - process_start_t0,
        )
    # 阶段耗时基准:冻结 EXE 排查启动超时要用各阶段时间戳对齐 Desktop 日志。
    startup_t0 = time.monotonic()

    def log_startup_stage(stage: str) -> None:
        logger.info(
            "[App] startup stage=%s process_elapsed=%.2fs run_elapsed=%.2fs",
            stage,
            time.monotonic() - process_start_t0,
            time.monotonic() - startup_t0,
        )

    log_startup_stage("run_entered")
    restart_request = GatewayRestartRequest()

    services = await assemble_services(
        agent_server_url, log_stage=log_startup_stage, startup_t0=startup_t0,
        connect_with_retry=connect_with_retry,
    )
    client = services.client
    third_agent = services.third_agent
    agent_server_ext = services.agent_server_ext
    using_adx_router = services.using_adx_router
    message_handler = services.message_handler
    im_inbound = services.im_inbound
    im_outbound = services.im_outbound
    cron_scheduler = services.cron_scheduler
    cron_controller = services.cron_controller
    heartbeat_controller = services.heartbeat_controller
    heartbeat_service = services.heartbeat_service
    config_updater = services.config_updater
    channel_manager = services.channel_manager
    updater_service = services.updater_service
    initial_channels_conf = services.initial_channels_conf
    full_cfg = services.full_cfg
    prewarm_sync_debounce_task: asyncio.Task[None] | None = None

    async def _sync_agent_prewarm_channels() -> None:
        if using_adx_router or not _agent_prewarm_enabled():
            return
        try:
            prewarm_channels = {
                channel
                for channel in channel_manager.enabled_channels
                if channel.lower() not in _AGENT_PREWARM_EXCLUDED_CHANNELS
            }
            env = e2a_from_agent_fields(
                request_id=f"agent-prewarm-sync-{uuid_module.uuid4().hex[:8]}",
                channel_id="",
                req_method=ReqMethod.AGENT_PREWARM_SYNC,
                params={
                    "enabled_channels": sorted(prewarm_channels),
                },
            )
            resp = await client.send_request(env)
            if not getattr(resp, "ok", False):
                raise RuntimeError(
                    f"agent.prewarm.sync rejected: {getattr(resp, 'payload', None)}"
                )
        except Exception as exc:  # noqa: BLE001 - prewarm never blocks Gateway config
            logger.warning("[App] agent.prewarm.sync failed (non-fatal): %s", exc)

    async def _periodic_agent_prewarm_sync() -> None:
        while True:
            await asyncio.sleep(60)
            await _sync_agent_prewarm_channels()

    def _schedule_agent_prewarm_sync(
        name: str, *, delay_seconds: float = 1.0
    ) -> None:
        """Coalesce startup/config/channel churn into one settled sync."""
        nonlocal prewarm_sync_debounce_task
        if using_adx_router or not _agent_prewarm_enabled():
            return
        previous = prewarm_sync_debounce_task
        if previous is not None and not previous.done():
            previous.cancel()

        async def _delayed_sync() -> None:
            try:
                await asyncio.sleep(max(0.0, delay_seconds))
                await _sync_agent_prewarm_channels()
            except asyncio.CancelledError:
                return

        prewarm_sync_debounce_task = asyncio.create_task(
            _delayed_sync(), name=name
        )

    _on_config_saved = config_saved_factory(
        client=client, message_handler=message_handler, cron_controller=cron_controller,
        schedule_agent_prewarm_sync=_schedule_agent_prewarm_sync,
        restart_request=restart_request,
    )

    web_channel, homepage_request_dispatched = assemble_web(
        agent_client=client, message_handler=message_handler, channel_manager=channel_manager,
        extension_registry=services.extension_registry, on_config_saved=_on_config_saved,
        heartbeat_service=heartbeat_service, cron_controller=cron_controller,
        heartbeat_controller=heartbeat_controller, updater_service=updater_service,
        host=web_host, port=web_port, path=web_path, ws_permessage_deflate=web_ws_deflate,
        preprocessor=preprocessor, log_stage=log_startup_stage,
    )
    web_task = await start_web_core(
        web_channel, channel_manager, cron_scheduler, config_updater, log_startup_stage,
    )
    tui_assembly = await assemble_tui_acp(
        agent_client=client, message_handler=message_handler, channel_manager=channel_manager,
        third_agent=third_agent, on_config_saved=_on_config_saved,
        cron_controller=cron_controller, heartbeat_controller=heartbeat_controller,
        preprocessor=preprocessor, log_stage=log_startup_stage,
    )
    protocol_runtime = tui_assembly.protocol_runtime

    a2a_server_enabled = str(os.getenv("A2A_SERVER_ENABLED", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    a2a_channel = None
    a2a_task: asyncio.Task | None = None
    if a2a_server_enabled:
        # A2A is disabled in the normal Desktop path.  Avoid importing its
        # protocol server and dependencies unless this optional listener is on.
        from agent_gateway.channel_manager.protocol.a2a.a2a_connect import (
            A2AChannel,
            A2AChannelConfig,
        )

        a2a_channel = A2AChannel(
            A2AChannelConfig(
                enabled=True,
                host=str(os.getenv("A2A_SERVER_HOST", "127.0.0.1")).strip() or "127.0.0.1",
                port=int(os.getenv("A2A_SERVER_PORT", "19100")),
                rpc_path=str(os.getenv("A2A_SERVER_PATH", "/a2a")).strip() or "/a2a",
                protocol_version=str(os.getenv("A2A_SERVER_PROTOCOL_VERSION", "1.0.0")).strip() or "1.0.0",
                card_path=str(
                    os.getenv("A2A_SERVER_CARD_PATH", "/.well-known/agent-card.json")
                ).strip() or "/.well-known/agent-card.json",
                extended_card_path=str(
                    os.getenv("A2A_SERVER_EXTENDED_CARD_PATH", "/agent/authenticatedExtendedCard")
                ).strip() or "/agent/authenticatedExtendedCard",
                app_name=str(
                    os.getenv("A2A_SERVER_APP_NAME", "JiuwenSwarm Gateway A2A Server")
                ).strip() or "JiuwenSwarm Gateway A2A Server",
                app_description=str(
                    os.getenv("A2A_SERVER_APP_DESCRIPTION", "A2A ingress for JiuwenSwarm Gateway")
                ).strip() or "A2A ingress for JiuwenSwarm Gateway",
                app_version=str(os.getenv("A2A_SERVER_APP_VERSION", "0.1.0")).strip() or "0.1.0",
                expose_reasoning=str(os.getenv("A2A_SERVER_EXPOSE_REASONING", "true")).strip().lower()
                not in {"0", "false", "no", "off"},
            ),
            _DummyBus(),
        )
        channel_manager.register_channel(a2a_channel)
        a2a_task = asyncio.create_task(a2a_channel.start(), name="a2a-channel")

        # Keep gateway startup non-blocking; surface background A2A boot failures with actionable logs.

        def _on_a2a_task_done(task: asyncio.Task) -> None:
            try:
                task.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "[App] A2A server failed to start: %s. "
                    "If A2A is enabled, install optional dependency with "
                    "`uv sync --extra a2a` or `pip install \"agent-gateway[a2a]\"`.",
                    exc,
                )

        a2a_task.add_done_callback(_on_a2a_task_done)

    feishu_channel = None
    feishu_task = None
    feishu_enterprise_channels: dict[str, FeishuChannel] = {}
    feishu_enterprise_tasks: dict[str, asyncio.Task] = {}
    xiaoyi_channel = None
    xiaoyi_task = None
    telegram_channel = None
    telegram_task = None
    discord_channel = None
    discord_task = None
    slack_channel = None
    slack_task = None
    whatsapp_channel = None
    whatsapp_task = None
    wecom_channel = None
    wecom_task = None
    wechat_channel = None
    wechat_task = None
    ssh_channel = None
    ssh_task = None

    def _set_agentos_ssh_key_issuer(
        issuer,
        *,
        ephemeral_key_ttl_sec: float = 300.0,
    ) -> None:
        """Inject/clear SshChannel as ephemeral key issuer on AgentOSRouter extension."""
        setter = getattr(agent_server_ext, "set_key_issuer", None)
        if not callable(setter):
            return
        try:
            setter(issuer, ephemeral_key_ttl_sec=ephemeral_key_ttl_sec)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[App] failed to set AgentOS SSH key issuer: %s", exc)

    _last_channels_conf: dict = {}

    def _should_restart_channel(channel_name: str, old_conf: dict, new_conf: dict) -> bool:
        old_channel_conf = old_conf.get(channel_name) if isinstance(old_conf, dict) else None
        new_channel_conf = new_conf.get(channel_name) if isinstance(new_conf, dict) else None
        if (old_channel_conf is None) != (new_channel_conf is None):
            return True
        if old_channel_conf is None:
            return False
        return old_channel_conf != new_channel_conf

    async def _stop_channel(channel, task, channel_name: str, background_wait: bool = False, *, unregister: bool = True) -> None:
        if task is not None:
            task.cancel()
            if background_wait:

                async def wait_cancel():
                    try:
                        await task
                    except (TypeError, asyncio.CancelledError):
                        logger.info("[App] cancelled previous %sChannel task", channel_name.capitalize())
                    except Exception as e:  # noqa: BLE001
                        logger.warning(
                            "[App] ignored exception while waiting for previous %sChannel task: %s",
                            channel_name.capitalize(),
                            e,
                        )

                asyncio.create_task(wait_cancel(), name=f"wait_{channel_name}_cancel")
            else:
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "[App] timeout while waiting for %sChannel task cancellation",
                        channel_name.capitalize(),
                    )
                except asyncio.CancelledError:
                    pass
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "[App] ignored exception while waiting for previous %sChannel task: %s",
                        channel_name.capitalize(),
                        e,
                    )

        if channel is not None:
            try:
                await asyncio.wait_for(channel.stop(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("[App] timeout while stopping %sChannel", channel_name.capitalize())
            except Exception as e:  # noqa: BLE001
                logger.warning("[App] failed to stop previous %sChannel: %s", channel_name.capitalize(), e)
            if unregister:
                channel_manager.unregister_channel(channel.channel_id)

    def _is_channel_enabled(conf: dict | None, required_fields: list[str]) -> tuple[bool, str]:
        if conf is None:
            return False, "missing or invalid config"
        enabled_raw = conf.get("enabled", None)
        if enabled_raw is None:
            all_fields_present = all(conf.get(f) for f in required_fields)
            return all_fields_present, f"missing {','.join(required_fields)}" if not all_fields_present else ""
        return bool(enabled_raw), "enabled = false" if not enabled_raw else ""

    def _create_dingtalk_channel(settings: dict):
        from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
            DingTalkChannel,
            DingTalkConfig,
        )

        return DingTalkChannel(DingTalkConfig.model_validate(settings), _DummyBus())

    async def _stop_dingtalk_channel(channel) -> None:
        # Already taken out of the registry; unregistering by channel id would drop newer instances.
        await _stop_channel(
            channel, getattr(channel, "start_task", None), "dingtalk", background_wait=True, unregister=False,
        )

    from agent_gateway.channel_manager.im_platforms.dingtalk.lifecycle import DingTalkChannelSet

    dingtalk_channels = DingTalkChannelSet(
        channel_manager,
        create_channel=_create_dingtalk_channel,
        stop_channel=_stop_dingtalk_channel,
        api_base=_DINGTALK_DEFAULT_API_BASE,
        oapi_base=_DINGTALK_DEFAULT_OAPI_BASE,
    )

    async def _apply_channel_config(conf: dict) -> None:
        nonlocal feishu_channel, feishu_task, xiaoyi_channel, xiaoyi_task
        nonlocal telegram_channel, telegram_task
        nonlocal discord_channel, discord_task
        nonlocal slack_channel, slack_task
        nonlocal whatsapp_channel, whatsapp_task
        nonlocal wecom_channel, wecom_task
        nonlocal wechat_channel, wechat_task
        nonlocal ssh_channel, ssh_task
        nonlocal _last_channels_conf
        nonlocal feishu_enterprise_channels, feishu_enterprise_tasks

        # === 新增：入口归一化（必须在 _should_restart_channel 之前） ===
        if isinstance(conf, dict):
            feishu_raw = conf.get("feishu")
            if isinstance(feishu_raw, dict):
                conf["feishu"] = _normalize_feishu_conf(feishu_raw)
            xiaoyi_raw = conf.get("xiaoyi")
            if isinstance(xiaoyi_raw, dict):
                conf["xiaoyi"] = _normalize_xiaoyi_conf(xiaoyi_raw)
        # ==========================================================

        restart_pending = channel_manager.pop_channel_restart_pending()
        changed_channels: list[str] = []
        for channel_name in [
            "feishu",
            "feishu_enterprise",
            "xiaoyi",
            "dingtalk",
            "welink",
            "telegram",
            "whatsapp",
            "discord",
            "slack",
            "wecom",
            "wechat",
            "ssh",
            "web_proxy",
        ]:
            if _should_restart_channel(channel_name, _last_channels_conf, conf) or channel_name in restart_pending:
                if channel_name in restart_pending and not _should_restart_channel(
                        channel_name, _last_channels_conf, conf
                ):
                    logger.info(
                        "[App] channels.%s force restart requested; cached runtime state must be dropped",
                        channel_name,
                    )
                changed_channels.append(channel_name)
        _last_channels_conf = dict(conf or {})

        if "feishu" in changed_channels:
            feishu_conf = conf.get("feishu") if isinstance(conf, dict) else {}

            # ---- 停止旧 apps 实例（从 channel_manager 查找） ----
            # 先 pop 再停止，避免 _stop_channel 内部 unregister_channel(channel_id)
            # 批量删除后后续 key 访问抛 KeyError
            for ch in channel_manager.pop_channels_by_id("feishu"):
                await _stop_channel(ch, getattr(ch, "start_task", None), f"feishu_app[{ch.app_id}]")
            # 统一注销 adapter（所有 feishu app 共享 "feishu" 标识）
            im_inbound.unregister_adapter("feishu")
            im_outbound.unregister_adapter("feishu")

            # 单变量置空（不再使用，但保持 nonlocal 兼容）
            feishu_channel, feishu_task = None, None

            apps = feishu_conf.get("apps") or []
            if not apps:
                logger.info("[App] channels.feishu.apps empty, FeishuChannel disabled")
            else:
                from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
                    FeishuChannel,
                    FeishuConfig,
                )
                for app in apps:
                    if not app.get("enabled", True):
                        continue
                    enabled, reason = _is_channel_enabled(app, ["app_id", "app_secret"])
                    if not enabled:
                        logger.info("[App] channels.feishu.apps[].%s, skipping", reason)
                        continue

                    app_id = str(app.get("app_id") or "").strip()
                    persistent_agent_id = str(app.get("persistent_agent_id") or "").strip()
                    channel_id = "feishu"

                    feishu_config = FeishuConfig(
                        enabled=True,
                        app_id=app_id,
                        app_secret=str(app.get("app_secret") or "").strip(),
                        agent_id=persistent_agent_id,
                        bot_open_id=str(app.get("bot_open_id") or "").strip() if persistent_agent_id else "",
                        encrypt_key=str(app.get("encrypt_key") or "").strip(),
                        verification_token=str(app.get("verification_token") or "").strip(),
                        allow_from=app.get("allow_from") or [],
                        enable_streaming=bool(app.get("enable_streaming", True)),
                        chat_id=str(app.get("chat_id") or "").strip(),
                        channel_id=channel_id,
                        last_chat_id=str(app.get("last_chat_id") or "").strip(),
                        last_open_id=str(app.get("last_open_id") or "").strip(),
                        group_digital_avatar=bool(app.get("group_digital_avatar", False)),
                        my_user_id=str(app.get("my_user_id") or app.get("my_open_id") or "").strip(),
                        bot_name=str(app.get("bot_name") or "").strip(),
                        enable_memory=bool(app.get("enable_memory", False)),
                        message_merge_window_ms=int(app.get("message_merge_window_ms", 15000)),
                        api_base=str(app.get("api_base") or "").strip() or _FEISHU_DEFAULT_API_BASE,
                    )
                    # 数字分身 adapter（共享 "feishu" key）
                    feishu_adapter = None
                    if feishu_config.group_digital_avatar:
                        from agent_gateway.channel_manager.im_platforms.feishu.feishu_im_adapter import (
                            FeishuIMPlatformAdapter,
                        )
                        feishu_adapter = FeishuIMPlatformAdapter(
                            my_open_id=feishu_config.my_user_id,
                            bot_name=feishu_config.bot_name,
                        )
                        im_inbound.register_adapter(channel_id, feishu_adapter)
                        im_outbound.register_adapter(channel_id, feishu_adapter)

                    channel = FeishuChannel(feishu_config, _DummyBus(), im_platform_adapter=feishu_adapter)
                    channel_manager.register_channel(channel)
                    task = asyncio.create_task(channel.start(), name=f"feishu-{app_id}")
                    channel.start_task = task  # 挂到 channel 对象上，不另存 dict
                    logger.info("[App] FeishuChannel(app=%s) registered from channels.feishu.apps", app_id)

        if "feishu_enterprise" in changed_channels:
            for bot_key, task in list(feishu_enterprise_tasks.items()):
                await _stop_channel(
                    feishu_enterprise_channels.get(bot_key),
                    task,
                    f"feishu_enterprise[{bot_key}]",
                )
            for _old_ch in feishu_enterprise_channels.values():
                _old_ch_id = getattr(_old_ch, "_channel_id", "") or getattr(_old_ch, "name", "")
                if _old_ch_id:
                    im_inbound.unregister_adapter(_old_ch_id)
                    im_outbound.unregister_adapter(_old_ch_id)
            feishu_enterprise_channels = {}
            feishu_enterprise_tasks = {}

            enterprise_conf = conf.get("feishu_enterprise") if isinstance(conf, dict) else None
            if not isinstance(enterprise_conf, dict):
                logger.info(
                    "[App] channels.feishu_enterprise missing or invalid; "
                    "FeishuEnterpriseChannel disabled"
                )
            else:
                from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
                    FeishuChannel,
                    FeishuConfig,
                )
                for bot_key, bot_conf_raw in enterprise_conf.items():
                    if not isinstance(bot_key, str) or not bot_key.strip():
                        continue
                    bot_conf = bot_conf_raw if isinstance(bot_conf_raw, dict) else None
                    if bot_conf is None:
                        logger.info("[App] channels.feishu_enterprise.%s invalid config, skipping", bot_key)
                        continue
                    enabled, reason = _is_channel_enabled(bot_conf, ["app_id", "app_secret"])
                    if not enabled:
                        logger.info(
                            "[App] channels.feishu_enterprise.%s.%s, FeishuEnterpriseChannel disabled",
                            bot_key,
                            reason,
                        )
                        continue

                    bot_key = bot_key.strip()
                    app_id = str(bot_conf.get("app_id") or "").strip()
                    persistent_agent_id = str(bot_conf.get("persistent_agent_id") or "").strip()
                    channel_id = f"feishu_enterprise:{app_id}"
                    feishu_config = FeishuConfig(
                        enabled=True,
                        app_id=app_id,
                        app_secret=str(bot_conf.get("app_secret") or "").strip(),
                        agent_id=persistent_agent_id,
                        bot_open_id=str(bot_conf.get("bot_open_id") or "").strip() if persistent_agent_id else "",
                        encrypt_key=str(bot_conf.get("encrypt_key") or "").strip(),
                        verification_token=str(bot_conf.get("verification_token") or "").strip(),
                        allow_from=bot_conf.get("allow_from") or [],
                        enable_streaming=bool(bot_conf.get("enable_streaming", True)),
                        chat_id=str(bot_conf.get("chat_id") or "").strip(),
                        channel_id=channel_id,
                        bot_key=bot_key,
                        last_chat_id=str(bot_conf.get("last_chat_id") or "").strip(),
                        last_open_id=str(bot_conf.get("last_open_id") or "").strip(),
                        my_user_id=str(bot_conf.get("my_user_id") or "").strip(),
                        bot_name=str(bot_conf.get("bot_name") or "").strip(),
                        group_digital_avatar=bool(bot_conf.get("group_digital_avatar", False)),
                        enable_memory=bool(bot_conf.get("enable_memory", False)),
                        api_base=str(bot_conf.get("api_base") or "").strip() or _FEISHU_DEFAULT_API_BASE,
                    )
                    feishu_adapter = None
                    if feishu_config.group_digital_avatar:
                        from agent_gateway.channel_manager.im_platforms.feishu.feishu_im_adapter import (
                            FeishuIMPlatformAdapter,
                        )
                        feishu_adapter = FeishuIMPlatformAdapter(
                            my_open_id=feishu_config.my_user_id,
                            bot_name=feishu_config.bot_name,
                        )
                        im_inbound.register_adapter(channel_id, feishu_adapter)
                        im_outbound.register_adapter(channel_id, feishu_adapter)
                    channel = FeishuChannel(feishu_config, _DummyBus(), im_platform_adapter=feishu_adapter)
                    channel_manager.register_channel(channel)
                    task = asyncio.create_task(channel.start(), name=f"feishu-enterprise-{bot_key}")
                    feishu_enterprise_channels[bot_key] = channel
                    feishu_enterprise_tasks[bot_key] = task
                    logger.info(
                        "[App] registered FeishuChannel(%s) from config.yaml.channels.feishu_enterprise.%s",
                        bot_key,
                        channel_id,
                    )

        if "xiaoyi" in changed_channels:
            xiaoyi_conf = conf.get("xiaoyi") if isinstance(conf, dict) else {}

            # ---- 停止旧 apps 实例（从 channel_manager 查找） ----
            # 先 pop 再停止，避免 _stop_channel 内部 unregister_channel(channel_id)
            # 批量删除后后续 key 访问抛 KeyError
            for ch in channel_manager.pop_channels_by_id("xiaoyi"):
                await _stop_channel(ch, getattr(ch, "start_task", None), f"xiaoyi_app[{ch.app_id}]")

            # 单变量置空（不再使用，但保持 nonlocal 兼容）
            xiaoyi_channel, xiaoyi_task = None, None

            apps = xiaoyi_conf.get("apps") or []
            if not apps:
                logger.info("[App] channels.xiaoyi.apps empty, XiaoyiChannel disabled")
            else:
                from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
                    XiaoyiChannel,
                    XiaoyiChannelConfig,
                )
                for app in apps:
                    if not app.get("enabled", True):
                        continue
                    enabled, reason = _is_channel_enabled(app, ["ak", "sk", "agent_id"])
                    if not enabled:
                        logger.info("[App] channels.xiaoyi.apps[].%s, skipping", reason)
                        continue

                    api_id = str(app.get("api_id") or "").strip()
                    is_default = app.get("is_default", False) or len(apps) == 1
                    if not api_id and not is_default:
                        logger.warning(
                            "[App] channels.xiaoyi.apps[].api_id required for non-default, skipping"
                        )
                        continue

                    channel_id = "xiaoyi"
                    config = XiaoyiChannelConfig(
                        enabled=True,
                        channel_id=channel_id,
                        mode=str(app.get("mode") or "xiaoyi_channel").strip(),
                        ak=str(app.get("ak") or "").strip(),
                        sk=str(app.get("sk") or "").strip(),
                        agent_id=str(app.get("agent_id") or "").strip(),
                        api_id=api_id,
                        enable_streaming=bool(app.get("enable_streaming", True)),
                        # 以下字段 _normalize_xiaoyi_conf 会自动填充缺省值，
                        # 此处显式读取以兼容直接使用 apps 格式的场景
                        ws_url1=str(app.get("ws_url1") or "").strip(),
                        ws_url2=str(app.get("ws_url2") or "").strip(),
                        uid=str(app.get("uid") or "").strip(),
                        api_key=str(app.get("api_key") or "").strip(),
                        push_id=str(app.get("push_id") or "").strip(),
                        push_url=str(app.get("push_url") or "").strip() or _XIAOYI_DEFAULT_PUSH_URL,
                        file_upload_url=str(app.get("file_upload_url") or "").strip(),
                    )
                    channel = XiaoyiChannel(config, _DummyBus())
                    channel_manager.register_channel(channel)
                    task = asyncio.create_task(channel.start(), name=f"xiaoyi-{api_id or 'default'}")
                    channel.start_task = task  # 挂到 channel 对象上，不另存 dict
                    logger.info("[App] XiaoyiChannel(api_id=%s) from channels.xiaoyi.apps", api_id or "default")

        if "welink" in changed_channels:
            from agent_gateway.channel_manager.im_platforms.welink.welink_runtime import WelinkRuntime

            runtime = getattr(channel_manager, "_welink_runtime", None)
            if runtime is None:
                runtime = WelinkRuntime(channel_manager, _DummyBus(), message_handler.handle_persistent_welink_event)
                channel_manager._welink_runtime = runtime
            try:
                await runtime.apply(conf.get("welink") or {})
            except Exception:
                channel_manager.mark_channel_restart_pending("welink")
                raise

        if "dingtalk" in changed_channels:
            # Restarts are serialized and always read the latest DingTalk settings.
            for app_id in await dingtalk_channels.restart():
                logger.info("[App] DingTalkChannel registered: app_id=%s", app_id)

        if "telegram" in changed_channels:
            telegram_conf = conf.get("telegram") if isinstance(conf, dict) else None
            await _stop_channel(telegram_channel, telegram_task, "telegram")
            telegram_channel, telegram_task = None, None

            if isinstance(telegram_conf, dict):
                enabled, reason = _is_channel_enabled(telegram_conf, ["bot_token"])
                if not enabled:
                    logger.info("[App] channels.telegram.%s, TelegramChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import (
                        TelegramChannel,
                        TelegramChannelConfig,
                    )
                    telegram_config = TelegramChannelConfig(
                        enabled=True,
                        bot_token=str(telegram_conf.get("bot_token") or "").strip(),
                        allow_from=telegram_conf.get("allow_from") or [],
                        parse_mode=str(telegram_conf.get("parse_mode") or "Markdown").strip(),
                        group_chat_mode=str(telegram_conf.get("group_chat_mode") or "mention").strip(),
                    )
                    telegram_channel = TelegramChannel(telegram_config, _DummyBus())
                    channel_manager.register_channel(telegram_channel)
                    telegram_task = asyncio.create_task(telegram_channel.start(), name="telegram")
                    logger.info("[App] TelegramChannel registered from config.yaml.channels.telegram")
            else:
                logger.info("[App] channels.telegram missing or invalid, TelegramChannel disabled")

        if "discord" in changed_channels:
            discord_conf = conf.get("discord") if isinstance(conf, dict) else None
            await _stop_channel(discord_channel, discord_task, "discord")
            discord_channel, discord_task = None, None

            if isinstance(discord_conf, dict):
                enabled, reason = _is_channel_enabled(discord_conf, ["bot_token"])
                if not enabled:
                    logger.info("[App] channels.discord.%s, DiscordChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.discord.discord_connect import (
                        DiscordChannel,
                        DiscordChannelConfig,
                    )
                    discord_config = DiscordChannelConfig(
                        enabled=True,
                        bot_token=str(discord_conf.get("bot_token") or "").strip(),
                        application_id=str(discord_conf.get("application_id") or "").strip(),
                        guild_id=str(discord_conf.get("guild_id") or "").strip(),
                        channel_id=str(discord_conf.get("channel_id") or "").strip(),
                        allow_from=discord_conf.get("allow_from") or [],
                        block_dm=(str(discord_conf.get("block_dm")).lower() in ["true", "1"]) or False,
                    )
                    discord_channel = DiscordChannel(discord_config, _DummyBus())
                    channel_manager.register_channel(discord_channel)
                    discord_task = asyncio.create_task(discord_channel.start(), name="discord")
                    logger.info("[App] DiscordChannel registered from config.yaml.channels.discord")
            else:
                logger.info("[App] channels.discord missing or invalid, DiscordChannel disabled")

        if "slack" in changed_channels:
            slack_conf = conf.get("slack") if isinstance(conf, dict) else None
            await _stop_channel(slack_channel, slack_task, "slack")
            slack_channel, slack_task = None, None

            if isinstance(slack_conf, dict):
                enabled, reason = _is_channel_enabled(slack_conf, ["bot_token", "app_token"])
                if not enabled:
                    logger.info("[App] channels.slack.%s, SlackChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.slack.slack_connect import (
                        SlackChannel,
                        SlackChannelConfig,
                    )
                    reply_in_thread_raw = slack_conf.get("reply_in_thread", True)
                    reply_in_thread = (
                        str(reply_in_thread_raw).strip().lower() in ("true", "1", "yes", "on")
                        if isinstance(reply_in_thread_raw, str)
                        else bool(reply_in_thread_raw)
                    )
                    slack_config = SlackChannelConfig(
                        enabled=True,
                        bot_token=str(slack_conf.get("bot_token") or "").strip(),
                        app_token=str(slack_conf.get("app_token") or "").strip(),
                        allow_from=slack_conf.get("allow_from") or [],
                        allowed_channel_ids=slack_conf.get("allowed_channel_ids") or [],
                        default_channel_id=str(slack_conf.get("default_channel_id") or "").strip(),
                        reply_in_thread=reply_in_thread,
                    )
                    slack_channel = SlackChannel(slack_config, _DummyBus())
                    channel_manager.register_channel(slack_channel)
                    slack_task = asyncio.create_task(slack_channel.start(), name="slack")
                    logger.info("[App] SlackChannel registered from config.yaml.channels.slack")
            else:
                logger.info("[App] channels.slack missing or invalid, SlackChannel disabled")

        if "whatsapp" in changed_channels:
            whatsapp_conf = conf.get("whatsapp") if isinstance(conf, dict) else None
            await _stop_channel(whatsapp_channel, whatsapp_task, "whatsapp")
            whatsapp_channel, whatsapp_task = None, None

            if isinstance(whatsapp_conf, dict):
                bridge_ws_url = str(whatsapp_conf.get("bridge_ws_url") or "ws://127.0.0.1:19600/ws").strip()
                default_jid = str(whatsapp_conf.get("default_jid") or "").strip()
                allow_from = whatsapp_conf.get("allow_from") or []
                enable_streaming = bool(whatsapp_conf.get("enable_streaming", True))
                auto_start_bridge = bool(whatsapp_conf.get("auto_start_bridge", False))
                bridge_command = str(
                    whatsapp_conf.get("bridge_command") or "node scripts/whatsapp-bridge.js"
                ).strip()
                bridge_workdir = str(whatsapp_conf.get("bridge_workdir") or "").strip()
                bridge_env_raw = whatsapp_conf.get("bridge_env") or {}
                bridge_env = bridge_env_raw if isinstance(bridge_env_raw, dict) else {}

                enabled_raw = whatsapp_conf.get("enabled", None)
                if enabled_raw is None:
                    enabled = bool(bridge_ws_url)
                else:
                    enabled = bool(enabled_raw)

                if not enabled:
                    logger.info("[App] channels.whatsapp.enabled = false, WhatsAppChannel disabled")
                elif not bridge_ws_url:
                    logger.info("[App] channels.whatsapp missing bridge_ws_url, WhatsAppChannel disabled")
                else:
                    from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import (
                        WhatsAppChannel,
                        WhatsAppChannelConfig,
                    )
                    whatsapp_config = WhatsAppChannelConfig(
                        enabled=True,
                        enable_streaming=enable_streaming,
                        bridge_ws_url=bridge_ws_url,
                        allow_from=allow_from,
                        default_jid=default_jid,
                        auto_start_bridge=auto_start_bridge,
                        bridge_command=bridge_command,
                        bridge_workdir=bridge_workdir,
                        bridge_env={str(k): str(v) for k, v in bridge_env.items()},
                    )
                    whatsapp_channel = WhatsAppChannel(whatsapp_config, _DummyBus())
                    channel_manager.register_channel(whatsapp_channel)
                    whatsapp_task = asyncio.create_task(whatsapp_channel.start(), name="whatsapp")
                    logger.info("[App] WhatsAppChannel registered from config.yaml.channels.whatsapp")
            else:
                logger.info("[App] channels.whatsapp missing or invalid, WhatsAppChannel disabled")

        if "wecom" in changed_channels:
            wecom_conf = conf.get("wecom") if isinstance(conf, dict) else None
            await _stop_channel(wecom_channel, wecom_task, "wecom")
            wecom_channel, wecom_task = None, None

            if isinstance(wecom_conf, dict):
                enabled, reason = _is_channel_enabled(wecom_conf, ["bot_id", "secret"])
                if not enabled:
                    logger.info("[App] channels.wecom.%s, WecomChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import (
                        WecomChannel,
                        WecomConfig,
                    )
                    wecom_config = WecomConfig(
                        enabled=True,
                        bot_id=str(wecom_conf.get("bot_id") or "").strip(),
                        secret=str(wecom_conf.get("secret") or "").strip(),
                        ws_url=str(wecom_conf.get("ws_url") or "wss://openws.work.weixin.qq.com").strip(),
                        allow_from=wecom_conf.get("allow_from") or [],
                        enable_streaming=bool(wecom_conf.get("enable_streaming", True)),
                        send_thinking_message=bool(wecom_conf.get("send_thinking_message", True)),
                        group_digital_avatar=bool(wecom_conf.get("group_digital_avatar", False)),
                        my_user_id=str(wecom_conf.get("my_user_id") or "").strip(),
                        bot_name=str(wecom_conf.get("bot_name") or "").strip(),
                        enable_memory=bool(wecom_conf.get("enable_memory", False)),
                    )
                    # 数字分身：创建 adapter 并注册到 pipeline
                    wecom_adapter = None
                    if wecom_config.group_digital_avatar:
                        from agent_gateway.channel_manager.im_platforms.wecom.wecom_im_adapter import (
                            WecomIMPlatformAdapter,
                        )
                        wecom_adapter = WecomIMPlatformAdapter(
                            my_user_id=wecom_config.my_user_id,
                            bot_name=wecom_config.bot_name,
                        )
                        im_inbound.register_adapter("wecom", wecom_adapter)
                        im_outbound.register_adapter("wecom", wecom_adapter)
                    wecom_channel = WecomChannel(wecom_config, _DummyBus(), im_platform_adapter=wecom_adapter)
                    channel_manager.register_channel(wecom_channel)
                    wecom_task = asyncio.create_task(wecom_channel.start(), name="wecom")
                    logger.info("[App] WecomChannel registered from config.yaml.channels.wecom")
            else:
                logger.info("[App] channels.wecom missing or invalid, WecomChannel disabled")

        if "wechat" in changed_channels:
            wechat_conf = conf.get("wechat") if isinstance(conf, dict) else None
            await _stop_channel(wechat_channel, wechat_task, "wechat")
            wechat_channel, wechat_task = None, None

            if isinstance(wechat_conf, dict):
                enabled, reason = _is_channel_enabled(wechat_conf, [])
                if not enabled:
                    logger.info("[App] channels.wechat.%s, WechatChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
                        WechatChannel,
                        WechatConfig,
                    )
                    wechat_config = WechatConfig(
                        enabled=True,
                        base_url=str(wechat_conf.get("base_url") or "https://ilinkai.weixin.qq.com").strip(),
                        bot_token=str(wechat_conf.get("bot_token") or "").strip(),
                        ilink_bot_id=str(wechat_conf.get("ilink_bot_id") or "").strip(),
                        ilink_user_id=str(wechat_conf.get("ilink_user_id") or "").strip(),
                        allow_from=wechat_conf.get("allow_from") or [],
                        auto_login=bool(wechat_conf.get("auto_login", True)),
                        qrcode_poll_interval_sec=float(wechat_conf.get("qrcode_poll_interval_sec", 2.0)),
                        long_poll_timeout_sec=int(wechat_conf.get("long_poll_timeout_sec", 45)),
                        backoff_base_sec=float(wechat_conf.get("backoff_base_sec", 1.0)),
                        backoff_max_sec=float(wechat_conf.get("backoff_max_sec", 30.0)),
                        credential_file=str(
                            wechat_conf.get("credential_file") or "~/.wx-ai-bridge/credentials.json"
                        ).strip(),
                        enable_streaming=bool(wechat_conf.get("enable_streaming", True)),
                    )
                    wechat_channel = WechatChannel(wechat_config, _DummyBus())
                    channel_manager.register_channel(wechat_channel)
                    wechat_task = asyncio.create_task(wechat_channel.start(), name="wechat")
                    logger.info("[App] WechatChannel registered from config.yaml.channels.wechat")
            else:
                logger.info("[App] channels.wechat missing or invalid, WechatChannel disabled")

        if "ssh" in changed_channels:
            ssh_conf = conf.get("ssh") if isinstance(conf, dict) else None
            await _stop_channel(ssh_channel, ssh_task, "ssh")
            ssh_channel, ssh_task = None, None
            # Clear issuer whenever SSH channel is torn down / reconfigured.
            _set_agentos_ssh_key_issuer(None)

            if isinstance(ssh_conf, dict):
                # 南向经 agent client（如 agentos_router -> yuanrong）动态解析。
                enabled, reason = _is_channel_enabled(ssh_conf, ["listen_port"])
                full_cfg = get_config()
                gateway_cfg = full_cfg.get("gateway") if isinstance(full_cfg, dict) else {}
                agent_client_cfg = (
                    gateway_cfg.get("agent_client") if isinstance(gateway_cfg, dict) else {}
                )
                client_type = (
                    str(agent_client_cfg.get("type") or "websocket").strip().lower()
                    if isinstance(agent_client_cfg, dict)
                    else "websocket"
                )
                if not enabled:
                    logger.info("[App] channels.ssh.%s, SshChannel disabled", reason)
                elif client_type not in {"agentos_router", "adx_router"}:
                    logger.warning(
                        "[App] channels.ssh.enabled=true but gateway.agent_client.type=%s "
                        "(require agentos_router or adx_router); SshChannel will not start",
                        client_type,
                    )
                else:
                    # SSH is optional and disabled for the normal Desktop path;
                    # keep its cryptography/runtime imports out of Gateway cold start.
                    from agent_gateway.channel_manager.protocol.ssh.ssh_connect import (
                        SshChannel,
                        SshChannelConfig,
                    )
                    from agent_gateway.extensions.auth.ssh_key_registry import (
                        KeyRegistry,
                    )

                    ssh_config = SshChannelConfig.from_dict({**ssh_conf, "enabled": True})
                    ssh_channel = SshChannel(
                        ssh_config,
                        _DummyBus(),
                        key_registry=KeyRegistry(),
                    )
                    channel_manager.register_channel(ssh_channel)
                    ssh_task = asyncio.create_task(ssh_channel.start(), name="ssh")

                    def _on_ssh_task_done(task: asyncio.Task) -> None:
                        try:
                            task.result()
                        except asyncio.CancelledError:
                            return
                        except Exception as exc:  # noqa: BLE001
                            logger.error(
                                "[App] SSH channel failed to start: %s. "
                                "If SSH is enabled, install optional dependency with "
                                "`uv sync --extra ssh` or `pip install \"agent-gateway[ssh]\"`.",
                                exc,
                            )

                    ssh_task.add_done_callback(_on_ssh_task_done)
                    if ssh_config.auth.enabled:
                        _set_agentos_ssh_key_issuer(
                            ssh_channel.key_issuer,
                            ephemeral_key_ttl_sec=ssh_config.auth.ephemeral_key_ttl_sec,
                        )
                    logger.info(
                        "[App] SshChannel registered from config.yaml.channels.ssh "
                        "(listen %s:%s -> MessageHandler; southbound via agent client; auth=%s)",
                        ssh_config.listen_host,
                        ssh_config.listen_port,
                        ssh_config.auth.enabled,
                    )
            else:
                logger.info("[App] channels.ssh missing or invalid, SshChannel disabled")

        if "web_proxy" in changed_channels:
            web_proxy_conf = conf.get("web_proxy") if isinstance(conf, dict) else None
            web_channel.web_proxy_enabled = False
            web_channel.web_proxy_auth_enabled = True
            web_channel.web_resolver = None
            web_channel.web_runtime_release = None
            previous_watcher = getattr(web_channel, "web_registry_watcher", None)
            if previous_watcher is not None:
                await previous_watcher.stop()
                web_channel.web_registry_watcher = None
            previous_manager = getattr(web_channel, "web_port_manager", None)
            if previous_manager is not None:
                await previous_manager.close_all()
            if isinstance(web_proxy_conf, dict):
                enabled, reason = _is_channel_enabled(web_proxy_conf, [])
                full_cfg = get_config()
                gateway_cfg = full_cfg.get("gateway") if isinstance(full_cfg, dict) else {}
                agent_client_cfg = (
                    gateway_cfg.get("agent_client") if isinstance(gateway_cfg, dict) else {}
                )
                client_type = (
                    str(agent_client_cfg.get("type") or "websocket").strip().lower()
                    if isinstance(agent_client_cfg, dict)
                    else "websocket"
                )
                wp_config = WebProxyChannelConfig.from_dict(web_proxy_conf)
                if not enabled:
                    logger.info("[App] channels.web_proxy.%s, web proxy disabled", reason or "enabled=false")
                elif client_type != "agentos_router":
                    logger.warning(
                        "[App] channels.web_proxy.enabled=true but gateway.agent_client.type=%s "
                        "(require agentos_router); web proxy will not start",
                        client_type,
                    )
                else:
                    web_resolver = getattr(agent_server_ext, "resolve_web_endpoint", None)
                    if not callable(web_resolver):
                        logger.warning(
                            "[App] channels.web_proxy.enabled=true but agent_server_ext has no "
                            "resolve_web_endpoint; web proxy will not start"
                        )
                    else:
                        web_release = getattr(
                            agent_server_ext, "release_web_endpoint", None
                        )

                        async def _held_web_resolver(
                            user_id: str,
                            agent_type: str,
                            protocol: str,
                            _resolve=web_resolver,
                        ) -> str | None:
                            return await _resolve(
                                user_id, agent_type, protocol, acquire=True
                            )

                        token_resolver = getattr(agent_server_ext, "acquire_web_endpoint", None)

                        web_channel.web_proxy_enabled = wp_config.enabled
                        web_channel.web_proxy_auth_enabled = wp_config.auth_enabled
                        web_channel.web_proxy_config = wp_config
                        web_channel.web_resolver = (
                            token_resolver if callable(token_resolver)
                            else _held_web_resolver
                        )
                        web_channel.web_runtime_release = (
                            web_release if callable(web_release) else None
                        )
                        manager = WebPortManager(web_channel, wp_config)
                        web_channel.web_port_manager = manager
                        manager.start()
                        from agent_gateway.runtime.cron.factory import (
                            load_cron_store_settings,
                        )
                        from agent_gateway.extensions.agentos.registry_watcher.service import (
                            RegistryWatcherService,
                        )

                        endpoints = list(load_cron_store_settings(full_cfg).endpoints)
                        watcher = RegistryWatcherService(
                            etcd_endpoints=endpoints,
                            on_desired=manager.reconcile,
                        )
                        web_channel.web_registry_watcher = watcher
                        await watcher.start()
                        logger.info(
                            "[App] 3rd-agent web proxy: listeners driven by registry_watcher "
                            "(listen %s, auth=%s, etcd=%s)",
                            wp_config.listen_host,
                            "on" if wp_config.auth_enabled else "off",
                            "on" if watcher.enabled else "off",
                        )
            else:
                logger.info("[App] channels.web_proxy missing or invalid, web proxy disabled")

        _schedule_agent_prewarm_sync(
            "agent-prewarm-sync-after-channel-change",
            delay_seconds=1.0,
        )

    channel_manager.set_config_callback(_apply_channel_config)

    prewarm_sync_task = (
        asyncio.create_task(
            _periodic_agent_prewarm_sync(),
            name="agent-prewarm-periodic-sync",
        )
        if not using_adx_router and _agent_prewarm_enabled()
        else None
    )

    # 主动推荐：按 config 自动注册/删除 proactive.tick 定时 job
    try:
        from agent_gateway.cron.proactive_cron_sync import sync_proactive_tick_job
        await sync_proactive_tick_job(cron_controller, get_config())
    except Exception as e:  # noqa: BLE001  # 兜底：启动时 proactive job 注册失败不阻断 Gateway 启动
        logger.warning("[App] proactive.tick auto-register failed (non-fatal): %s", e)
    # 先同步完成监听绑定，避免 IDE/ACP 子进程在端口尚未就绪时连接导致多次重试。
    await protocol_runtime.start()
    logger.info(
        "[App] gateway server listening (elapsed %.2fs)",
        time.monotonic() - startup_t0,
    )
    log_startup_stage("gateway_server_listening")
    protocol_runtime_task = asyncio.create_task(
        protocol_runtime.wait_until_closed(),
        name="tui-acp-protocol-runtime",
    )
    if web_channel is not None:
        logger.info(
            "[App] started: Web ws://%s:%s%s  AgentServer: %s  Press Ctrl+C to exit. (elapsed %.2fs)",
            web_host,
            web_port,
            web_path,
            agent_server_url,
            time.monotonic() - startup_t0,
        )
    log_startup_stage("ready")

    channel_retry_tasks: set[asyncio.Task[None]] = set()

    def _schedule_channel_config_retry(
        channel_name: str,
        failed_conf: dict[str, Any],
        *,
        expected_revision: int,
    ) -> None:
        """Retry one failed optional channel without delaying Web/Cron startup.

        A retry is abandoned when another configuration write occurs.  This is
        important for a user disabling or editing the channel from Settings:
        an old startup snapshot must never resurrect that configuration.
        """
        retry_conf = dict(failed_conf or {})

        async def _retry() -> None:
            nonlocal expected_revision
            delay_seconds = 2.0
            attempt = 0
            while True:
                await asyncio.sleep(delay_seconds)
                if channel_manager.get_conf_revision(channel_name) != expected_revision:
                    logger.info(
                        "[App] cancelled automatic channel retry after configuration changed: %s",
                        channel_name,
                    )
                    return
                attempt += 1
                try:
                    await channel_manager.set_conf(channel_name, retry_conf)
                except Exception as exc:  # noqa: BLE001 - optional channel isolation
                    logger.warning(
                        "[App] automatic channel retry failed; will retry again: "
                        "channel=%s attempt=%d delay=%.1fs error=%s",
                        channel_name,
                        attempt,
                        delay_seconds,
                        exc,
                    )
                    # ``set_conf`` rolls its visible snapshot back after the
                    # callback error.  Apply an empty config so the callback's
                    # change detector also rolls back before the next retry.
                    try:
                        await channel_manager.set_conf(channel_name, {})
                    except Exception:  # noqa: BLE001 - next retry can still recover
                        logger.warning(
                            "[App] failed to reset channel after automatic retry error: %s",
                            channel_name,
                            exc_info=True,
                        )
                    expected_revision = channel_manager.get_conf_revision(channel_name)
                    delay_seconds = min(delay_seconds * 2, 60.0)
                    continue
                logger.info(
                    "[App] automatic channel retry succeeded: channel=%s attempt=%d",
                    channel_name,
                    attempt,
                )
                return

        task = asyncio.create_task(
            _retry(), name=f"initial-channel-retry-{channel_name}"
        )
        channel_retry_tasks.add(task)
        task.add_done_callback(channel_retry_tasks.discard)

    async def _apply_initial_channel_config() -> None:
        # Optional IM/SSH setup contains synchronous construction work.  Do
        # not merely wait for the WebSocket handshake: wait until the first
        # homepage request has entered MessageHandler, then leave a short
        # window for its batch of project/session RPCs.  Cron is already
        # running; the timeout covers headless/TUI-only deployments.
        try:
            await asyncio.wait_for(homepage_request_dispatched.wait(), timeout=5.0)
            log_startup_stage("homepage_request_dispatched")
            await asyncio.sleep(0.75)
        except asyncio.TimeoutError:
            logger.info(
                "[App] no Web homepage request within 5s; applying optional channel configuration"
            )
        channel_config_t0 = time.monotonic()
        failed_channels: list[str] = []
        for channel_name, channel_conf in initial_channels_conf.items():
            if not isinstance(channel_name, str):
                logger.warning(
                    "[App] skipped invalid initial channel config key: %r",
                    channel_name,
                )
                continue
            try:
                await channel_manager.set_conf(channel_name, channel_conf)
            except Exception as exc:  # noqa: BLE001 - optional channel isolation
                failed_channels.append(channel_name)
                logger.exception(
                    "[App] optional channel configuration failed; "
                    "Gateway/Web/Cron will remain available: channel=%s error=%s",
                    channel_name,
                    exc,
                )
                # ``set_conf`` restores ChannelManager's config snapshot on
                # callback failure, while the callback's own change detector
                # has already seen the bad value.  Apply an empty config once
                # to align that detector with the rolled-back state, ensuring
                # a later settings save can retry this channel.
                try:
                    await channel_manager.set_conf(channel_name, {})
                except Exception:  # noqa: BLE001 - disabled cleanup is best-effort
                    logger.warning(
                        "[App] failed to reset optional channel after startup error: %s",
                        channel_name,
                        exc_info=True,
                    )
                _schedule_channel_config_retry(
                    channel_name,
                    channel_conf if isinstance(channel_conf, dict) else {},
                    expected_revision=channel_manager.get_conf_revision(channel_name),
                )
        logger.info(
            "[App] initial channel configuration applied in background "
            "(elapsed %.2fs, failed=%s)",
            time.monotonic() - channel_config_t0,
            failed_channels or "none",
        )
        log_startup_stage("channel_configuration_applied")
        _schedule_agent_prewarm_sync(
            "agent-prewarm-sync-after-startup",
            delay_seconds=3.0,
        )

    channel_config_task = asyncio.create_task(
        _apply_initial_channel_config(),
        name="initial-channel-configuration",
    )
    log_startup_stage("channel_configuration_scheduled")

    restart_requested = False
    try:
        tasks_to_wait = [
            task
            for task in (protocol_runtime_task, web_task, channel_config_task)
            if task is not None
        ]
        if tasks_to_wait:
            restart_requested = await _wait_for_gateway_tasks_or_restart(
                tasks_to_wait,
                restart_request,
                shutdown_event,
            )
    except asyncio.CancelledError:
        pass
    finally:
        if config_updater is not None:
            await config_updater.stop()
        if not channel_config_task.done():
            channel_config_task.cancel()
            try:
                await channel_config_task
            except asyncio.CancelledError:
                pass
        for retry_task in tuple(channel_retry_tasks):
            retry_task.cancel()
        if channel_retry_tasks:
            await asyncio.gather(*channel_retry_tasks, return_exceptions=True)
        if prewarm_sync_debounce_task is not None:
            prewarm_sync_debounce_task.cancel()
            try:
                await prewarm_sync_debounce_task
            except asyncio.CancelledError:
                pass
        if prewarm_sync_task is not None:
            prewarm_sync_task.cancel()
            try:
                await prewarm_sync_task
            except asyncio.CancelledError:
                pass
        if a2a_task is not None:
            a2a_task.cancel()
            try:
                await a2a_task
            except asyncio.CancelledError:
                pass
        if a2a_channel is not None:
            await a2a_channel.stop()
            channel_manager.unregister_channel(a2a_channel.channel_id)
        if protocol_runtime_task is not None:
            protocol_runtime_task.cancel()
            try:
                await protocol_runtime_task
            except asyncio.CancelledError:
                pass
        await tui_assembly.stop()
        if web_channel is not None:
            await web_channel.stop()
        if web_task is not None:
            web_task.cancel()
            try:
                await web_task
            except asyncio.CancelledError:
                pass
        if feishu_channel is not None and feishu_task is not None:
            feishu_task.cancel()
            try:
                await feishu_task
            except asyncio.CancelledError:
                pass
            await feishu_channel.stop()
        for bot_key, task in list(feishu_enterprise_tasks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            channel = feishu_enterprise_channels.get(bot_key)
            if channel is not None:
                await channel.stop()
        if xiaoyi_channel is not None and xiaoyi_task is not None:
            xiaoyi_task.cancel()
            try:
                await xiaoyi_task
            except asyncio.CancelledError:
                pass
            await xiaoyi_channel.stop()
        # ---- 从 channel_manager 清理所有动态注册的 channel 实例 ----
        runtime = getattr(channel_manager, "_welink_runtime", None)
        if runtime is not None:
            await runtime.stop()
        for _cid in ("feishu", "xiaoyi", "dingtalk"):
            for ch in channel_manager.pop_channels_by_id(_cid):
                task = getattr(ch, "start_task", None)
                if task is not None:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                await ch.stop()
        # -----------------------------------
        if telegram_channel is not None and telegram_task is not None:
            telegram_task.cancel()
            try:
                await telegram_task
            except asyncio.CancelledError:
                pass
            await telegram_channel.stop()
        if discord_channel is not None and discord_task is not None:
            discord_task.cancel()
            try:
                await discord_task
            except asyncio.CancelledError:
                pass
            await discord_channel.stop()
        if slack_channel is not None and slack_task is not None:
            slack_task.cancel()
            try:
                await slack_task
            except asyncio.CancelledError:
                pass
            await slack_channel.stop()
        if whatsapp_channel is not None and whatsapp_task is not None:
            whatsapp_task.cancel()
            try:
                await whatsapp_task
            except asyncio.CancelledError:
                pass
            await whatsapp_channel.stop()
        if wecom_channel is not None and wecom_task is not None:
            wecom_task.cancel()
            try:
                await wecom_task
            except asyncio.CancelledError:
                pass
            await wecom_channel.stop()
        if wechat_channel is not None and wechat_task is not None:
            wechat_task.cancel()
            try:
                await wechat_task
            except asyncio.CancelledError:
                pass
            await wechat_channel.stop()
        if ssh_channel is not None and ssh_task is not None:
            ssh_task.cancel()
            try:
                await ssh_task
            except asyncio.CancelledError:
                pass
            await ssh_channel.stop()
            _set_agentos_ssh_key_issuer(None)

        await services.stop()
        await shutdown_gateway_telemetry()
        logger.info("[App] Gateway stopped")

    if restart_requested:
        restart_gateway()


