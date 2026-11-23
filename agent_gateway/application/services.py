# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Construct the existing single-process service graph without public listeners."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
import os
import time
from typing import Any, TYPE_CHECKING

from agent_gateway.application.configuration import _load_gateway_runtime_config
from agent_gateway.application.connection import _connect_with_retry
from agent_gateway.common.log_sanitize import mask_sensitive
from agent_gateway.telemetry import configure_gateway_telemetry

logger = logging.getLogger("gateway")

_REMOVED_AGENT_CLIENT_TYPES = frozenset({"yuanrong"})


def _reject_removed_agent_client_type(agent_client_config: dict[str, Any]) -> None:
    """已移除的 ``gateway.agent_client.type`` 直接拒绝，不静默回落 WebSocket."""
    client_type = str(agent_client_config.get("type") or "").strip().lower()
    if client_type in _REMOVED_AGENT_CLIENT_TYPES:
        raise RuntimeError(
            f"gateway.agent_client.type={client_type} 已移除（FaaS invoke 链路已删除），"
            "请改用 agentos_router（YuanRong 部署）或 websocket（本地 AgentServer）"
        )




@dataclass
class GatewayServices:
    """References to one service graph, not a registry or a second implementation."""

    client: Any
    third_agent: Any
    agent_server_ext: Any
    using_adx_router: Any
    extension_registry: Any
    extension_manager: Any
    storage_ctx: Any
    message_handler: Any
    im_inbound: Any
    im_outbound: Any
    cron_store: Any
    cron_scheduler: Any
    cron_controller: Any
    heartbeat_controller: Any
    heartbeat_service: Any
    config_updater: Any
    channel_manager: Any
    updater_service: Any
    initial_channels_conf: Any
    full_cfg: Any
    _hold_release_task: Any
    _cleanup_task: Any

    async def stop(self) -> None:
        await self.cron_scheduler.stop()
        await self.channel_manager.stop_dispatch()
        await self.heartbeat_service.stop()
        await self.message_handler.stop_forwarding()
        await self.client.disconnect()

        if self.storage_ctx is not None:
            try:
                from agent_gateway.storage_assembly.setup import (
                    teardown_gateway_storage_repositories,
                )

                await teardown_gateway_storage_repositories(self.storage_ctx)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[App] gateway storage teardown failed: %s", exc)

        self._hold_release_task.cancel()
        try:
            await self._hold_release_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[App] abandoned hold release task failed")
        self._cleanup_task.cancel()
        try:
            await self._cleanup_task
        except (asyncio.CancelledError, Exception):
            pass



async def assemble_services(
    agent_server_url: str,
    *,
    log_stage: Callable[[str], None],
    startup_t0: float,
    connect_with_retry: Callable[..., Awaitable[None]] = _connect_with_retry,
    required_client_type: str | tuple[str, ...] | None = None,
) -> GatewayServices:
    from openjiuwen.core.runner import Runner
    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.cleanup import start_background_cleanup
    from agent_gateway.common.config import get_config
    from agent_gateway.common.config_panel.config_set_handlers import CONFIG_SET_ENV_MAP
    from agent_gateway.common.updater import UpdaterService
    from agent_gateway.cron import CronController, CronSchedulerService, create_gateway_cron_store
    from agent_gateway.extensions.manager import ExtensionManager
    from agent_gateway.extensions.registry import ExtensionRegistry
    from agent_gateway.health_check import GatewayHealthCheckService, HealthCheckConfig
    from agent_gateway.heartbeat import HeartbeatControllerProxy
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    callback_framework = Runner.callback_framework
    extension_registry = ExtensionRegistry.create_instance(
        callback_framework=callback_framework,
        config={},
        logger=logger,
    )
    from agent_gateway.agent_client_mode import (
        _validate_agent_client_registration,
        read_agent_client_mode,
        resolve_agent_server_client,
        validate_agent_client_configuration,
    )

    agent_client_mode = read_agent_client_mode()
    validate_agent_client_configuration(get_config() or {}, mode=agent_client_mode)
    extension_manager = ExtensionManager(registry=extension_registry)
    log_stage("extension_manager_created")
    await extension_manager.load_all_extensions()
    logger.info(
        "[App] extensions loaded: %d (elapsed %.2fs)",
        len(extension_manager.list_extensions()),
        time.monotonic() - startup_t0,
    )
    log_stage("extensions_loaded")

    # 企业版在 MessageHandler / config_poll 之前装配存储屏蔽层并注入各配置模块。
    # 个人版不进入该路径，配置读写保持原来的文件直读。
    storage_ctx = None
    from agent_gateway.enterprise import is_enterprise

    if is_enterprise():
        from agent_gateway.extensions.redis import init_gateway_redis_from_config
        from agent_gateway.storage_assembly.setup import start_gateway_storage

        try:
            full_cfg = get_config()
            await init_gateway_redis_from_config(dict(full_cfg or {}))
            storage_ctx = await start_gateway_storage(full_cfg)
        except Exception:
            logger.exception(
                "[App] enterprise Gateway storage wiring failed; aborting startup"
            )
            raise
        log_stage("storage_repositories_wired")

    max_retries = int(os.getenv("AGENT_CONNECT_RETRY", "20"))
    retry_interval = float(os.getenv("AGENT_CONNECT_RETRY_INTERVAL", "3"))

    # 从扩展注册表获取 AgentServerClient（WebSocket 或其他扩展实现）
    agent_server_ext = extension_registry.get_agent_server_client_extension()
    full_cfg = get_config()
    _validate_agent_client_registration(full_cfg, agent_server_ext, required_type=required_client_type)
    configure_gateway_telemetry(
        full_cfg or {},
        user_id_formatter=lambda value: mask_sensitive(f"user_id={value}").removeprefix("user_id="),
    )
    gateway_config = full_cfg.get("gateway") or {}
    agent_client_config = gateway_config.get("agent_client") or {}
    _reject_removed_agent_client_type(agent_client_config)
    using_adx_router = str(agent_client_config.get("type") or "").strip().lower() == "adx_router"
    if using_adx_router:
        if agent_server_ext is None or agent_server_ext.metadata.id != "adx_router":
            raise RuntimeError("Selected adx_router extension failed to load; check gateway.adx configuration")
    client = resolve_agent_server_client(
        agent_server_ext,
        mode=agent_client_mode,
        websocket_factory=lambda: WebSocketAgentServerClient(
            ping_interval=20.0, ping_timeout=600.0,
        ),
    )
    if agent_server_ext is not None:
        logger.info("[App] using extension AgentServerClient: %s", agent_server_ext.metadata.name)

    from agent_gateway.routing.third_agent import get_unsupported_third_agent

    third_agent_ext = extension_registry.get_third_agent_extension()
    if third_agent_ext is not None:
        logger.info("[App] using extension ThirdAgent: %s", third_agent_ext.metadata.name)
        third_agent = third_agent_ext.get_third_agent()
    else:
        third_agent = get_unsupported_third_agent()

    # 如果是 WebSocket 客户端，需要连接；如果是 YuanrongFrontendAgentClient，无需连接
    if isinstance(client, WebSocketAgentServerClient):
        await connect_with_retry(
            client,
            agent_server_url,
            max_retries=max_retries,
            interval=retry_interval,
        )
    else:
        # YuanrongFrontendAgentClient 是 HTTP 客户端，无需连接
        await client.connect("")
    log_stage("agent_server_connected")


    message_handler = MessageHandler(client)
    await message_handler.start_forwarding()

    # IM Pipeline 初始化（数字分身）
    from agent_gateway.im_pipeline.im_inbound import IMInboundPipeline
    from agent_gateway.im_pipeline.im_outbound import IMOutboundPipeline
    im_inbound = IMInboundPipeline()
    im_outbound = IMOutboundPipeline()
    message_handler.set_inbound_pipeline(im_inbound)
    message_handler.set_outbound_pipeline(im_outbound)

    cron_store = await create_gateway_cron_store()
    from agent_gateway.common.model_migration import migrate_legacy_cron_selections
    from agent_gateway.runtime.cron.factory import load_cron_store_settings

    # Shared etcd jobs need the target user's catalog; that migration runs on
    # the per-user resource/reference boundary, never using Gateway's catalog.
    if load_cron_store_settings(get_config()).backend == "file":
        try:
            from agent_gateway.common.remote_model_catalog import (
                RemoteModelCatalog,
                fetch_agent_model_rows,
            )

            rows = await fetch_agent_model_rows(
                agent_client=client,
                channel_id="gateway-cron-migration",
            )
            if rows is None:
                logger.warning(
                    "Cron model-selection migration skipped: "
                    "AgentServer models.list is unavailable; "
                    "will retry on the next Gateway startup"
                )
            else:
                await migrate_legacy_cron_selections(
                    cron_store,
                    catalog=RemoteModelCatalog(rows),
                    user_id=None,
                    remote_validated=True,
                )
        except Exception:  # noqa: BLE001
            logger.warning("Cron model-selection migration deferred", exc_info=True)
    cron_scheduler = CronSchedulerService(
        store=cron_store,
        agent_client=client,
        message_handler=message_handler,
    )
    cron_controller = CronController.get_instance(store=cron_store, scheduler=cron_scheduler)
    cron_controller.bind_agent_client(client)
    message_handler.set_cron_controller(cron_controller)
    # gateway 自有的 cron 工厂注册进注入点：common 的 model_resource 处理
    # 器经 provider("cron_store_factory") 创建 gateway 侧 cron store
    from agent_gateway.common.external_providers import register as _register_provider
    from agent_gateway.runtime.cron import factory as _cron_factory

    _register_provider("cron_store_factory", _cron_factory)
    # session_metadata 域 WS 远端实现的客户端绑定（仅远端预演模式消费）
    from agent_gateway.routing.session_metadata_remote import (
        bind_agent_client as _bind_session_meta_client,
    )

    _bind_session_meta_client(client)
    # mcp_registry 域 WS 远端实现（oauth 回调）的客户端绑定，同款装配
    from agent_gateway.routing.mcp_remote import bind_agent_client as _bind_mcp_client

    _bind_mcp_client(client)

    # Heartbeat Store/Controller/Scheduler/Execution live in AgentServer.
    # Gateway keeps only the stable public-interface proxy wired to Web/TUI.
    heartbeat_controller = HeartbeatControllerProxy(client)

    full_cfg, health_check_cfg, channels_cfg = _load_gateway_runtime_config(
        message_handler
    )
    log_stage("runtime_config_loaded")

    client.set_or_update_server_config(
        config=dict(full_cfg or {}),
        env={env_key: (os.getenv(env_key) or "") for env_key in CONFIG_SET_ENV_MAP.values()},
    )

    # Data-plane settings are AgentOS-only. A personal Gateway using etcd for
    # cron must not start this service. Only agent_sandbox.idle_timeout is
    # managed, so no user reload/fanout is needed.
    config_updater = None
    from agent_gateway.routing.e2a_proxy import is_agentos_routing_client
    if is_agentos_routing_client(client) and callable(getattr(client, "apply_remote_overrides", None)):
        from agent_gateway.extensions.agentos.config_updater.service import (
            ConfigUpdaterService,
        )
        from agent_gateway.runtime.cron.factory import load_cron_store_settings

        config_updater = ConfigUpdaterService(
            etcd_endpoints=list(load_cron_store_settings(full_cfg).endpoints),
            config=full_cfg,
            config_provider=get_config,
            overrides_handler=client.apply_remote_overrides,
        )

    if isinstance(health_check_cfg, dict):
        cfg_every = health_check_cfg.get("every")
        cfg_target = health_check_cfg.get("target")
        cfg_active_hours = health_check_cfg.get("active_hours")
    else:
        cfg_every = None
        cfg_target = None
        cfg_active_hours = None

    heartbeat_interval = float(
        os.getenv("HEALTH_CHECK_INTERVAL")
        or os.getenv("HEARTBEAT_INTERVAL")
        or (str(cfg_every) if cfg_every is not None else "60")
    )
    health_check_timeout_raw = os.getenv("HEALTH_CHECK_TIMEOUT") or os.getenv(
        "HEARTBEAT_TIMEOUT"
    )
    heartbeat_timeout = (
        float(health_check_timeout_raw) if health_check_timeout_raw else None
    )
    heartbeat_relay_channel = (
        os.getenv("HEALTH_CHECK_RELAY_CHANNEL_ID")
        or os.getenv("HEARTBEAT_RELAY_CHANNEL_ID")
        or (str(cfg_target) if cfg_target is not None else "web")
    )

    heartbeat_config = HealthCheckConfig(
        interval_seconds=heartbeat_interval,
        timeout_seconds=heartbeat_timeout,
        relay_channel_id=heartbeat_relay_channel,
        active_hours=cfg_active_hours if isinstance(cfg_active_hours, dict) else None,
    )
    heartbeat_service = GatewayHealthCheckService(
        client,
        heartbeat_config,
        message_handler=message_handler,
    )
    await heartbeat_service.start()
    log_stage("heartbeat_started")

    from agent_gateway.enterprise.team.abandoned_hold import (
        run_abandoned_hold_release_loop,
    )

    _hold_release_task = asyncio.create_task(
        run_abandoned_hold_release_loop(),
        name="abandoned-hold-release",
    )
    _cleanup_task = start_background_cleanup()

    initial_channels_conf: dict = channels_cfg if isinstance(channels_cfg, dict) else {}
    # Optional integrations are applied after the Web/Cron core is available.
    # Start with no applied integration config, then install each entry
    # independently below.  This prevents one bad optional channel from
    # suppressing every later channel or terminating the Gateway.
    from agent_gateway.channel_manager.persistent_output import resolve_output_file

    channel_manager = ChannelManager(message_handler, config={})
    channel_manager.set_persistent_output_file_resolver(resolve_output_file)
    # 回填引用：MessageHandler 实例化早于 ChannelManager，广播全局事件时需经它取 web channel。
    message_handler.set_channel_manager(channel_manager)

    updater_service = UpdaterService()
    return GatewayServices(
        client=client,
        third_agent=third_agent,
        agent_server_ext=agent_server_ext,
        using_adx_router=using_adx_router,
        extension_registry=extension_registry,
        extension_manager=extension_manager,
        storage_ctx=storage_ctx,
        message_handler=message_handler,
        im_inbound=im_inbound,
        im_outbound=im_outbound,
        cron_store=cron_store,
        cron_scheduler=cron_scheduler,
        cron_controller=cron_controller,
        heartbeat_controller=heartbeat_controller,
        heartbeat_service=heartbeat_service,
        config_updater=config_updater,
        channel_manager=channel_manager,
        updater_service=updater_service,
        initial_channels_conf=initial_channels_conf,
        full_cfg=full_cfg,
        _hold_release_task=_hold_release_task,
        _cleanup_task=_cleanup_task,
    )
