# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Standalone Gateway entrypoint (split deployment).

This process starts:
- Gateway MessageHandler + ChannelManager
- WebChannel websocket server (browser inbound)
- Heartbeat service
- Cron scheduler service (triggers remote AgentServer via ws)

It connects to a remote/local AgentServer WebSocket endpoint.

Supports ``--dotenv <path>`` for multi-instance isolation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid as uuid_module
from dataclasses import dataclass, field
from typing import Any

# Include entry-module import/configuration work in later startup phase logs.
# PyInstaller boot time is intentionally outside this boundary.
_PROCESS_START_T0 = time.monotonic()
_STARTUP_IMPORT_PHASES: list[tuple[str, float]] = [("entry", _PROCESS_START_T0)]


def _mark_startup_import_phase(stage: str) -> None:
    _STARTUP_IMPORT_PHASES.append((stage, time.monotonic()))

from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import Message
from agent_gateway.common.protocol_methods import ReqMethod, register_edition_req_methods

# --- Early --dotenv parsing (before jiuwenswarm imports) ---
from agent_gateway.startup.bootstrap import (
    apply_group_environment,
    configure_process_logging,
    initialize_early_environment,
    prepare_group_workspace,
)

initialize_early_environment(_mark_startup_import_phase)
configure_process_logging(_mark_startup_import_phase)

# Standalone entrypoints retain workspace preparation; Desktop/app already do
# it before spawning us and pass the marker to avoid duplicate disk work.
prepare_group_workspace(_mark_startup_import_phase)

_mark_startup_import_phase("openjiuwen_and_channel_base_imported")

# --- Now safe to import jiuwenswarm modules ---
from agent_gateway.routing.protocol_runtime import (
    ProtocolRuntime,
    ProtocolRuntimeConfig,
    RouteConfig,
    _build_event_frame,
)

# Preserve public class imports while assembly uses the canonical names.
GatewayServer = ProtocolRuntime
GatewayServerConfig = ProtocolRuntimeConfig

from agent_gateway.common.debug_dump import (
    install_async_dump_handler,
    install_crash_exit_handler,
)
from agent_gateway.common.paths import (
    apply_free_search_runtime_defaults,
    get_env_file,
    get_user_workspace_dir,
)
from agent_gateway.common.process_supervision import (
    GATEWAY_RESTART_EXIT_CODE,
    SUPERVISOR_PID_ENV,
)
from agent_gateway.telemetry import shutdown_gateway_telemetry

_mark_startup_import_phase("gateway_core_imports_loaded")

_mark_startup_import_phase("logging_configured")

apply_group_environment(_mark_startup_import_phase)

register_edition_req_methods()

logger = logging.getLogger("gateway")

from agent_gateway.application.config_saved_effects import sync_saved_topic_recommendation
from agent_gateway.application.connection import _connect_with_retry, _wait_for_web_channel_listening
from agent_gateway.application.inbound import (
    MessagePreprocessor,
    _refresh_expired_login_credential,
    _normalize_gateway_message,
    _inject_session_work_mode,
)
from agent_gateway.application.single import (
    GatewayRestartRequest,
    _AGENT_PREWARM_EXCLUDED_CHANNELS,
    _agent_prewarm_enabled,
    _wait_for_gateway_tasks_or_restart,
)

# Preserve imports that callers and tests still take from the legacy entrypoint.
from agent_gateway.application.connection import (  # noqa: F401
    _stop_web_runtime,
    _wait_for_web_channel_listening,
)
from agent_gateway.application.services import _reject_removed_agent_client_type  # noqa: F401

# IM 平台官方 API 域名（仅作为 config.yaml 缺字段时的加载兜底，不在 Config 类里硬编码）
_FEISHU_DEFAULT_API_BASE = "https://open.feishu.cn"
_DINGTALK_DEFAULT_API_BASE = "https://api.dingtalk.com"    # 新版 v1.0 接口域名
_DINGTALK_DEFAULT_OAPI_BASE = "https://oapi.dingtalk.com"  # 旧版 media 接口域名
_XIAOYI_DEFAULT_PUSH_URL = "https://hag.cloud.huawei.com/open-ability-agent/v1/agent-webhook"



async def _normalize_and_forward_message(msg, channel_manager) -> bool:
    return await MessagePreprocessor(
        _refresh_expired_login_credential, _normalize_gateway_message, _inject_session_work_mode,
    ).forward(msg, channel_manager)


def _running_under_supervisor() -> bool:
    """Whether our jiuwenswarm.app supervisor is alive right now and really is our parent.

    On Windows os.getppid() keeps reporting the original PID even after the
    supervisor dies (no reparenting), so a stale env match alone would make us
    os._exit(GATEWAY_RESTART_EXIT_CODE) with nobody left to respawn. A recycled
    PID necessarily belongs to a process younger than us, so requiring the
    parent to be live and not created after us rejects both cases.
    """
    if os.environ.get(SUPERVISOR_PID_ENV) != str(os.getppid()):
        return False
    import psutil

    try:
        return psutil.Process(os.getppid()).create_time() <= psutil.Process().create_time()
    except psutil.Error:
        return False


def _exec_gateway_restart() -> None:
    if _running_under_supervisor():
        # Under jiuwenswarm.app, let the supervisor respawn us. os.execv on
        # Windows starts a new PID and exits this one with 0, which the
        # supervisor reads as the Gateway stopping and tears the service down.
        # os._exit keeps execv's no-cleanup semantics; the OS frees the ports
        # and the gateway lock.
        logger.info("[App] requesting Gateway restart from supervisor")
        os._exit(GATEWAY_RESTART_EXIT_CODE)  # pylint: disable=protected-access  # public API; underscore is historical
    if os.environ.get(SUPERVISOR_PID_ENV):
        # Env says we were spawned by a supervisor, yet the parent check in
        # _running_under_supervisor failed. execv exits 0, which that live
        # supervisor reads as "Gateway stopped" and tears the WHOLE stack
        # down (AgentServer included) — log the deciding values so a later
        # post-mortem does not have to guess.
        import psutil

        try:
            parent = psutil.Process(os.getppid())
            parent_desc = f"alive, created {parent.create_time():.0f}"
        except psutil.Error:
            parent_desc = "not running"
        logger.warning(
            "[App] %s=%s but parent pid %s is %s; execv restart will be read "
            "as a stop by the supervisor",
            SUPERVISOR_PID_ENV,
            os.environ.get(SUPERVISOR_PID_ENV),
            os.getppid(),
            parent_desc,
        )
    logger.info("[App] .env updated, restarting Gateway...")
    os.execv(sys.executable, [sys.executable, *sys.argv])


def _schedule_gateway_restart(
        restart_request: GatewayRestartRequest | None = None,
        *,
        delay: float = 2.0,
) -> None:
    if restart_request is not None:
        restart_request.requested = True

    def _request_restart() -> None:
        if restart_request is not None:
            restart_request.ready_event.set()
            return
        _exec_gateway_restart()

    try:
        loop = asyncio.get_running_loop()
        loop.call_later(delay, _request_restart)
    except RuntimeError:
        _request_restart()


def _build_config_saved_callback(
    *,
    client: Any,
    message_handler: Any = None,
    cron_controller: Any,
    schedule_agent_prewarm_sync: Any,
    restart_request: GatewayRestartRequest,
):
    from agent_gateway.application.config_saved_effects import ConfigSavedEffectCoordinator
    from agent_gateway.channel_manager.web.app_web_handlers import spawn_background_task

    async def sync_proactive_tick(config_payload: dict[str, Any] | None) -> None:
        from agent_gateway.cron.proactive_cron_sync import sync_proactive_tick_job

        await sync_proactive_tick_job(cron_controller, config_payload)

    from agent_gateway.channel_manager.web.channel_sync import schedule_channel_snapshot

    return ConfigSavedEffectCoordinator(
        client=client,
        schedule_prewarm=schedule_agent_prewarm_sync,
        sync_proactive_tick=sync_proactive_tick,
        schedule_channel_snapshot=schedule_channel_snapshot,
        schedule_restart=lambda: _schedule_gateway_restart(restart_request),
        spawn_background=spawn_background_task,
    )


async def _run(
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
        web_ws_deflate: bool = False,
) -> None:
    """Retain the public entrypoint while delegating to the single host."""
    from agent_gateway.application.single import run_single

    await run_single(
        agent_server_url=agent_server_url, web_host=web_host, web_port=web_port,
        web_path=web_path, shutdown_event=shutdown_event, web_ws_deflate=web_ws_deflate,
        config_saved_factory=_build_config_saved_callback,
        restart_gateway=_exec_gateway_restart,
        process_start_t0=_PROCESS_START_T0,
        startup_import_phases=_STARTUP_IMPORT_PHASES,
        preprocessor=MessagePreprocessor(
            _refresh_expired_login_credential, _normalize_gateway_message, _inject_session_work_mode,
        ),
        connect_with_retry=_connect_with_retry,
    )


async def _run_with_shutdown_signals(
    *,
    agent_server_url: str,
    web_host: str,
    web_port: int,
    web_path: str,
    web_ws_deflate: bool = False,
) -> None:
    """Translate process signals into one idempotent Gateway shutdown request."""

    from agent_gateway.startup.lifecycle import run_with_shutdown_signals

    async def run(shutdown_event: asyncio.Event) -> None:
        await _run(
            agent_server_url=agent_server_url,
            web_host=web_host,
            web_port=web_port,
            web_path=web_path,
            shutdown_event=shutdown_event,
            web_ws_deflate=web_ws_deflate,
        )

    await run_with_shutdown_signals(run, shutdown_gateway_telemetry)


def main() -> None:
    from agent_gateway.dotenv_early import get_parsed_dotenv

    parser = argparse.ArgumentParser(
        prog="agent-gateway",
        description="Start JiuwenSwarm Gateway + Channels (split deployment; connects to jiuwenswarm-agentserver).",
    )
    parser.add_argument(
        "--agent-server-url",
        "-u",
        default=None,
        metavar="URL",
        help="AgentServer WebSocket URL (default: AGENT_SERVER_URL or ws://AGENT_SERVER_HOST:AGENT_SERVER_PORT).",
    )
    parser.add_argument(
        "--host",
        "-H",
        default=None,
        metavar="HOST",
        help="WebChannel bind host (default: WEB_HOST or 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=None,
        metavar="PORT",
        help="WebChannel bind port (default: WEB_PORT or 19000).",
    )
    parser.add_argument(
        "--web-path",
        default=None,
        metavar="PATH",
        help="WebChannel ws path (default: WEB_PATH or /ws).",
    )
    parser.add_argument(
        "--name",
        metavar="<name>",
        help="Start a named instance from instances.yaml.",
    )
    parser.add_argument(
        "--dotenv",
        metavar="<path>",
        help="Load environment from .env file (processed at startup, not used here).",
    )
    args = parser.parse_args()

    # Handle --name: check if bootstrap .env was loaded successfully
    # (parse_dotenv_early() already processed it, this is just a fallback check)
    if args.name:
        if get_parsed_dotenv() is None:
            # Early parsing failed - instance not found or workspace missing
            # Error was already printed by parse_dotenv_early()
            raise SystemExit(1)

    # gateway 进程注入点装配（bridge 已退役）：session_metadata 与
    # mcp_registry 两域注册 WS 远端实现——gateway 仓不再加载任何
    # server 侧模块；a2ui/team/mcp_state/mcp_credential 域在本进程
    # 零消费，保持未注册（调用得到 ExternalProviderUnregistered 降级）。
    from agent_gateway.routing.mcp_remote import install as _install_mcp_registry_remote
    from agent_gateway.routing.session_metadata_remote import (
        install as _install_session_metadata_remote,
    )

    _install_session_metadata_remote()
    _install_mcp_registry_remote()

    default_host = os.getenv("AGENT_SERVER_HOST", "127.0.0.1")
    default_port = os.getenv("AGENT_SERVER_PORT") or os.getenv("AGENT_PORT", "18092")
    agent_server_url = (
            args.agent_server_url
            or os.getenv("AGENT_SERVER_URL")
            or f"ws://{default_host}:{default_port}"
    )
    web_host = args.host or os.getenv("WEB_HOST", "127.0.0.1")
    web_port = args.port or int(os.getenv("WEB_PORT", "19000"))
    web_path = args.web_path or os.getenv("WEB_PATH", "/ws")
    # ws permessage-deflate compresses every frame with zlib; on loopback/LAN
    # the CPU cost outweighs the bandwidth saving, so it stays off by default.
    # Public-network deployments can re-enable it with WEB_WS_PERMESSAGE_DEFLATE=1.
    web_ws_deflate = (os.getenv("WEB_WS_PERMESSAGE_DEFLATE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    install_crash_exit_handler("gateway")
    install_async_dump_handler("gateway")

    # Per-workspace singleton lock: prevents a second Gateway process from
    # serving the same workspace (two CronSchedulerService instances over one
    # cron_jobs.json => duplicate cron executions).
    from agent_gateway.startup.lifecycle import gateway_workspace_lock

    with gateway_workspace_lock(get_user_workspace_dir()):
        asyncio.run(
            _run_with_shutdown_signals(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )


if __name__ == "__main__":
    main()
