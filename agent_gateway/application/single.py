# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Single-process host retaining the original staged startup and shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import logging
import time
import uuid as uuid_module
from typing import Any

from e2a.gateway_normalize import e2a_from_agent_fields
from agent_gateway.common.protocol_methods import ReqMethod
from agent_gateway.application.connection import (
    _stop_web_runtime,
    _wait_for_web_channel_listening,
)
from agent_gateway.application.inbound import MessagePreprocessor
from agent_gateway.application.optional_channels import OptionalChannels
from agent_gateway.application.services import assemble_services
from agent_gateway.application.web import assemble_web
from agent_gateway.application.tui_acp import assemble_tui_acp
from agent_gateway.telemetry import shutdown_gateway_telemetry

logger = logging.getLogger("gateway")
_AGENT_PREWARM_EXCLUDED_CHANNELS = frozenset({"acp", "a2a"})


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
        if config_updater is not None:
            await config_updater.start()
        log_stage("web_core_ready")
    except BaseException:
        await _stop_web_runtime(web_channel, web_task)
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
    web_channel = web_task = tui_assembly = optional_channels = None
    protocol_runtime_task = prewarm_sync_task = prewarm_sync_debounce_task = None
    config_updater = None
    restart_requested = False
    try:
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
        services.start_preparation()
        tui_assembly = await assemble_tui_acp(
            agent_client=client, message_handler=message_handler, channel_manager=channel_manager,
            third_agent=third_agent, on_config_saved=_on_config_saved,
            cron_controller=cron_controller, heartbeat_controller=heartbeat_controller,
            preprocessor=preprocessor, log_stage=log_startup_stage,
        )
        protocol_runtime = tui_assembly.protocol_runtime

        optional_channels = OptionalChannels(
            channel_manager=channel_manager, message_handler=message_handler,
            im_inbound=im_inbound, im_outbound=im_outbound,
            agent_server_ext=agent_server_ext, web_channel=web_channel,
            initial_channels_conf=initial_channels_conf,
            homepage_request_dispatched=homepage_request_dispatched,
            schedule_agent_prewarm_sync=_schedule_agent_prewarm_sync,
            log_stage=log_startup_stage,
        )
        optional_channels.start_a2a()
        optional_channels.install()

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

        channel_config_task = optional_channels.schedule_initial_configuration()

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
        if optional_channels is not None:
            await optional_channels.stop_configuration()
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
        if optional_channels is not None:
            await optional_channels.stop_a2a()
        if protocol_runtime_task is not None:
            protocol_runtime_task.cancel()
            try:
                await protocol_runtime_task
            except asyncio.CancelledError:
                pass
        if tui_assembly is not None:
            await tui_assembly.stop()
        if web_task is not None:
            await _stop_web_runtime(web_channel, web_task)
        if optional_channels is not None:
            await optional_channels.stop_channels()

        await services.stop()
        await shutdown_gateway_telemetry()
        logger.info("[App] Gateway stopped")

    if restart_requested:
        restart_gateway()
