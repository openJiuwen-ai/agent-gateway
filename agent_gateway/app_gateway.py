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
from collections import deque
from copy import copy
import inspect
import json
import logging
import os
import re
import signal
import sys
import time
import uuid as uuid_module
from dataclasses import dataclass, field, replace
from typing import AbstractSet, Any, Awaitable, Callable
from urllib.parse import parse_qs, urlparse

# Include entry-module import/configuration work in later startup phase logs.
# PyInstaller boot time is intentionally outside this boundary.
_PROCESS_START_T0 = time.monotonic()
_STARTUP_IMPORT_PHASES: list[tuple[str, float]] = [("entry", _PROCESS_START_T0)]


def _mark_startup_import_phase(stage: str) -> None:
    _STARTUP_IMPORT_PHASES.append((stage, time.monotonic()))

from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import (
    PROMPT_SUGGESTION_READY_EVENT,
    TOPIC_STARTERS_CONTROL_CHANGED_EVENT,
    TOPIC_STARTERS_READY_EVENT,
    Message,
    Mode,
)
from agent_gateway.common.protocol_methods import ReqMethod, register_edition_req_methods

from agent_gateway.channel_manager.web.req_method_routing import (
    ROUTE_DENIED,
    ROUTE_FORWARD_UNKNOWN,
    ROUTE_LOCAL,
    route_req_method,
)

from agent_gateway.common.ws_diagnostics import (
    describe_ws_exception,
    describe_ws_peer,
    format_ws_diagnostics,
)

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

from agent_gateway.channel_manager.base import BaseWebChannel

_mark_startup_import_phase("openjiuwen_and_channel_base_imported")

# --- Now safe to import jiuwenswarm modules ---
from agent_gateway.routing.protocol_runtime import (
    ProtocolRuntime,
    ProtocolRuntimeConfig,
    RouteConfig,
    _build_event_frame,
)

GatewayServer = ProtocolRuntime
GatewayServerConfig = ProtocolRuntimeConfig
from agent_gateway.channel_manager.web.media import (
    normalize_chat_media_attachments,
)
from agent_gateway.common.debug_dump import (
    install_async_dump_handler,
    install_crash_exit_handler,
)
from agent_gateway.common.paths import (
    apply_free_search_runtime_defaults,
    get_env_file,
    get_infra_data_dir,
)
from agent_gateway.common.process_supervision import (
    GATEWAY_RESTART_EXIT_CODE,
    SUPERVISOR_PID_ENV,
)
from agent_gateway.common.security.ws_origin import (
    extract_handshake_request,
    get_header_value,
)
from agent_gateway.common.request_identity import apply_routing_metadata, normalize_routing_identity
from agent_gateway.routing.agent_request_timeout import (
    AGENT_SERVER_TIMEOUT_CODE,
    AGENT_SERVER_TIMEOUT_ERROR,
    coerce_client_timeout_ms,
    resolve_agent_request_timeout_seconds,
)
from agent_gateway.routing.route_binding import GatewayRouteBinding
from agent_gateway.common.log_sanitize import mask_sensitive
from agent_gateway.telemetry import (
    attach_message_trace,
    configure_gateway_telemetry,
    get_message_trace,
    shutdown_gateway_telemetry,
    trace_stage,
)

_mark_startup_import_phase("gateway_core_imports_loaded")

_mark_startup_import_phase("logging_configured")

apply_group_environment(_mark_startup_import_phase)

register_edition_req_methods()

logger = logging.getLogger("gateway")

_AGENT_PREWARM_EXCLUDED_CHANNELS = frozenset({"acp", "a2a"})
_REMOVED_AGENT_CLIENT_TYPES = frozenset({"yuanrong"})


def _reject_removed_agent_client_type(agent_client_config: dict[str, Any]) -> None:
    """已移除的 ``gateway.agent_client.type`` 直接拒绝，不静默回落 WebSocket."""
    client_type = str(agent_client_config.get("type") or "").strip().lower()
    if client_type in _REMOVED_AGENT_CLIENT_TYPES:
        raise RuntimeError(
            f"gateway.agent_client.type={client_type} 已移除（FaaS invoke 链路已删除），"
            "请改用 agentos_router（YuanRong 部署）或 websocket（本地 AgentServer）"
        )


def _agent_prewarm_enabled() -> bool:
    """Return whether background session prewarming is switched on.

    Prewarming is opt-in via JIUWENSWARM_AGENT_PREWARM; when off the Gateway
    must not emit agent.prewarm.sync requests or related log noise.
    """
    from agent_gateway.runtime.agent_prewarm import prewarm_enabled_by_env

    return prewarm_enabled_by_env()

# IM 平台官方 API 域名（仅作为 config.yaml 缺字段时的加载兜底，不在 Config 类里硬编码）


def _resolve_health_check_config(full_cfg: dict[str, Any] | None) -> dict[str, Any] | None:
    """Prefer health_check and read legacy heartbeat probe keys during migration."""
    if not isinstance(full_cfg, dict):
        return None
    current = full_cfg.get("health_check")
    if isinstance(current, dict):
        return current
    legacy = full_cfg.get("heartbeat")
    if not isinstance(legacy, dict):
        return None
    migrated = {
        key: legacy[key]
        for key in ("every", "target", "active_hours")
        if key in legacy
    }
    return migrated or None


def _load_gateway_runtime_config(
    message_handler: Any,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    """Load Gateway config without coupling optional migrations to channels.

    A failed legacy HealthCheck migration must not discard otherwise readable
    channel configuration. Evolution config propagation is optional for the
    same reason: it may fail independently without changing the loaded config.
    """
    import agent_gateway.common.config as config_module

    try:
        if config_module.migrate_legacy_heartbeat_probe_config():
            logger.info(
                "[App] migrated legacy heartbeat probe config to health_check"
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[App] failed to migrate legacy heartbeat probe config; "
            "continuing with the existing config: %s",
            exc,
        )

    try:
        full_cfg = config_module.get_config()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[App] failed to read config.yaml, using defaults: %s",
            exc,
        )
        return {}, None, None

    if not isinstance(full_cfg, dict):
        logger.warning(
            "[App] config.yaml root must be an object, using defaults"
        )
        return {}, None, None

    health_check_cfg = _resolve_health_check_config(full_cfg)
    if (
        isinstance(health_check_cfg, dict)
        and not isinstance(full_cfg.get("health_check"), dict)
    ):
        logger.warning(
            "[App] legacy heartbeat probe config detected; "
            "migrate it to health_check"
        )
    channels_cfg = full_cfg.get("channels")
    return (
        full_cfg,
        health_check_cfg,
        channels_cfg if isinstance(channels_cfg, dict) else None,
    )





def _auth_session_id(msg) -> str | None:
    """从入站消息里取调用方的**登录会话** id（不是对话 session_id）。

    浏览器在 HTTP 上用 cookie，但对话走的是 WebSocket，cookie 不会出现在每条
    消息里，所以前端把登录会话 id 放进 ``metadata.auth_session``。它就是
    ``X-Auth-Session`` 头里的那个值——同一个凭据换了条通道，不构成新的信任假设。
    """
    metadata = getattr(msg, "metadata", None)
    if not isinstance(metadata, dict):
        return None
    value = str(metadata.get("auth_session") or "").strip()
    return value or None


def _normalize_model_auth(params: dict[str, Any], msg) -> None:
    """给登录模型挂上本次调用方的凭据。失败不阻断转发。"""
    try:
        from agent_gateway.auth.passthrough import normalize_model_auth

        normalize_model_auth(params, _auth_session_id(msg))
    except Exception:  # noqa: BLE001 — 凭据注入失败不该让整条消息发不出去
        logger.debug("[App] 模型凭据注入失败", exc_info=True)


#: 转发前等登录凭据续期的上限。续期本身的 HTTP 超时是 15 秒，这里只是兜住排队等锁等意外。
_LOGIN_REFRESH_WAIT_S = 20.0


async def _refresh_expired_login_credential(msg) -> None:
    """用登录模型、而凭据已过期时，先把凭据续好再转发。

    放置超过id_token有效期1小时后发的第一条消息，转发时 token 已过期，挂不上
    凭据，AgentServer会当成未登录；用户点一下账号再回来又好了。
    这里把续期提前到转发之前：放线程里等，不卡事件循环；每条入站消息各自一个 task，
    也不挡同一连接的其他请求；同一账号并发的几次续期在 ``try_refresh`` 里合并成一次。
    续不了（refresh_token失效等）就照旧转发，由AgentServer提示重新登录。
    """
    try:
        from agent_gateway.auth.passthrough import expired_login_session
        from agent_gateway.auth.service import get_auth_service

        params = msg.params if isinstance(msg.params, dict) else {}
        session = expired_login_session(params, _auth_session_id(msg))
        if session is None:
            return
        await asyncio.wait_for(
            asyncio.to_thread(get_auth_service().try_refresh, session),
            timeout=_LOGIN_REFRESH_WAIT_S,
        )
    except asyncio.TimeoutError:
        logger.warning("[App] 转发前续期登录凭据超时，本次按原凭据转发 id=%s", getattr(msg, "id", None))
    except Exception:  # noqa: BLE001 — 续期失败不该让整条消息发不出去
        logger.debug("[App] 转发前续期登录凭据失败", exc_info=True)


def _normalize_gateway_message(msg):

    req_method = getattr(msg, "req_method", None) or ReqMethod.CHAT_SEND
    params = dict(msg.params or {})
    if "query" not in params and "content" in params:
        params["query"] = params["content"]
    if req_method == ReqMethod.CHAT_RESUME:
        req_method = ReqMethod.CHAT_CANCEL
        params.setdefault("intent", "resume")

    method_val = req_method.value
    is_stream = bool(
        msg.is_stream
        or method_val in (ReqMethod.CHAT_SEND.value, ReqMethod.HISTORY_GET.value)
    )

    # 登录模型的凭据在这里挂上，随请求带给AgentServer
    _normalize_model_auth(params, msg)

    normalized = Message(
        id=msg.id,
        type=msg.type,
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params=params,
        timestamp=msg.timestamp,
        ok=msg.ok,
        req_method=req_method,
        mode=msg.mode,
        is_stream=is_stream,
        stream_seq=msg.stream_seq,
        stream_id=msg.stream_id,
        metadata=msg.metadata,
        app_id=getattr(msg, "app_id", None),
        user_id=getattr(msg, "user_id", None),
    )
    attach_message_trace(normalized, get_message_trace(msg))
    return normalized


async def _normalize_and_forward_message(msg, channel_manager) -> bool:
    with trace_stage("preprocess.credentials"):
        await _refresh_expired_login_credential(msg)
    normalized = _normalize_gateway_message(msg)
    # ACP/直连转发路径(session.create 等)也需注入 work_mode 归一化,
    # 与 _norm_and_forward(Web/TUI 主路径)保持一致。否则直连 AgentServer 的
    # 调用方传真实 project_id + 合法但不匹配的 work_mode 时,AgentServer 侧
    # _work_mode_explicit marker 缺失,不会返回设计要求的 BAD_REQUEST。
    method_val = getattr(getattr(msg, "req_method", None), "value", None) or ""
    if method_val == "session.create":
        _inject_session_work_mode(normalized)
    await channel_manager.deliver_to_message_handler(normalized)
    logger.info("[App] Gateway inbound -> MessageHandler: id=%s channel_id=%s", msg.id, msg.channel_id)
    return False


def _inject_session_work_mode(msg: Message) -> None:
    """为 ``session.create`` 请求注入 work_mode 归一化(主路径兜底)。

    与 fallback ``_session_create`` 共用同一 helper
    ``resolve_session_work_mode_params``,保持主路径/fallback 一致。

    成功时写回归一化后的 ``project_id`` / ``project_dir`` / ``work_mode`` 到
    ``msg.params``;失败时(非法 work_mode)不写回,保留原始 params 由后续
    AgentServer 或 fallback ``_session_create`` 返回 BAD_REQUEST。

    本函数做参数归一化 + **TUI project_dir 预解析**(设计文档 §5.3.5):
    - TUI 通道下,若 project_id 为默认项目且 project_dir 非空绝对路径,
      按 work_mode="code" 查找/创建 code 项目,将真实 project_id 写回 params。
    - 非 TUI 通道仅做纯参数归一化,最终 work_mode 以 Project 记录为准的
      校验由 ``_session_create`` / AgentServer 侧完成。

    显式性判定(``has_explicit_work_mode``)由 ``resolve_session_work_mode_params``
    随 binding 返回。但 gateway → AgentServer 是跨进程通信,binding 结果不能
    直接传递。因此 gateway 将 ``has_explicit_work_mode`` 注入 params 的
    ``_work_mode_explicit`` 字段作为传输标记,AgentServer 消费后立即 pop。
    AgentServer 直连调用方(非 gateway 路径)不携带此标记,此时 AgentServer
    通过重新调用 ``resolve_session_work_mode_params`` 获取 binding 中的
    ``has_explicit_work_mode``(直连场景 params 为原始值,计算结果正确)。
    """
    params = getattr(msg, "params", None)
    if not isinstance(params, dict):
        return
    channel_id = getattr(msg, "channel_id", None)
    try:
        from agent_gateway.runtime.work_mode import resolve_session_work_mode_params
        binding = resolve_session_work_mode_params(params, channel_id=channel_id)
    except Exception:  # noqa: BLE001
        # 归一化异常时不写回,保留原始 params 由后续处理
        return
    if binding.error:
        return

    resolved_project_id = binding.project_id
    resolved_project_dir = binding.project_dir
    resolved_work_mode = binding.work_mode

    # 注意:TUI 通道的 session.create 不走 forward 路径(不在 CLI_FORWARD_REQ_METHODS),
    # TUI 的 cwd→code project 绑定由 AgentServer 侧在 SESSION_CREATE 处理时通过
    # find_or_create_code_project_for_tui_params 完成（经 E2A 转发后解析）。
    # 此处仅处理 WEB/ACP 通道的 work_mode 绑定注入。

    params["project_id"] = resolved_project_id
    params["project_dir"] = resolved_project_dir
    params["work_mode"] = resolved_work_mode
    # _work_mode_explicit: 跨进程传输"请求是否显式传了 work_mode"标志。
    # AgentServer 消费后立即 pop,不持久化、不序列化到日志。
    # 该标志区分"用户显式传 work_mode"(需做 Project 一致性校验)与
    # "gateway 注入通道默认 work_mode"(跳过一致性校验,以 Project 记录为准)。
    params["_work_mode_explicit"] = binding.has_explicit_work_mode


class _InboundGatewayServer:
    """Gateway internal inbound service for forwarding channel messages to MessageHandler."""

    def __init__(self, inbound_handler):
        self._inbound_handler = inbound_handler
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._serve_loop(), name="gateway-inbound-server")

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def handle_message(self, msg) -> bool:
        await self._queue.put(msg)
        return True

    async def _serve_loop(self) -> None:
        while self._running:
            try:
                msg = await self._queue.get()
            except asyncio.CancelledError:
                break
            try:
                handled = self._inbound_handler(msg)
                if asyncio.iscoroutine(handled):
                    await handled
            except Exception:  # noqa: BLE001
                logger.exception("[App] Gateway inbound handling failed: id=%s", getattr(msg, "id", None))


async def _connect_with_retry(
        client,
        uri: str,
        *,
        max_retries: int = 20,
        interval: float = 3.0,
) -> None:
    """连接 AgentServer, 失败重试.

    相比固定 3s 间隔: 先做轻量 TCP 端口探测, 端口通了再 ws 握手, 避免 AgentServer
    尚未 listen 时反复走完整 WS 握手; 重试间隔改为指数退避 (0.2s 起, 翻倍至上限
    ``interval``), 让 AgentServer 一旦 listen 即在亚秒级连上, 而非硬等下一个 3s 节拍.
    """
    import socket as _socket
    from urllib.parse import urlparse as _urlparse

    parsed = _urlparse(uri)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)

    def _tcp_ready(timeout: float = 0.5) -> bool:
        try:
            with _socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    # Desktop always connects to a loopback AgentServer it has just spawned.
    # Use a short local probe cadence there; retain the existing conservative
    # exponential backoff for remote AgentServer deployments.
    local_agent = host in {"127.0.0.1", "localhost", "::1"}
    backoff = 0.05 if local_agent else 0.2
    max_backoff = min(interval, 0.25) if local_agent else interval
    if local_agent:
        # Allow roughly 60s for the local AgentServer to start at a 0.25s cadence.
        max_retries = max(max_retries, 240)
    for attempt in range(1, max_retries + 1):
        # 先 TCP 探测: 端口未通则不浪费一次 WS 握手, 直接进入退避等待.
        if not _tcp_ready(timeout=0.05 if local_agent else 0.5):
            if attempt >= max_retries:
                logger.error(
                    "[App] connect AgentServer failed after %d tries: port %s not listening  uri=%s",
                    attempt, port, uri,
                )
                raise ConnectionRefusedError(f"AgentServer port {port} not listening")
            logger.info(
                "[App] AgentServer port %s not listening yet (%d/%d), retry in %.2fs...",
                port, attempt, max_retries, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        try:
            await client.connect(uri)
            logger.info("[App] connected to AgentServer: %s", uri)
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                logger.error(
                    "[App] connect AgentServer failed after %d tries: %s  last=%s",
                    attempt,
                    uri,
                    exc,
                )
                raise
            logger.warning(
                "[App] connect AgentServer failed (%d/%d): %s  retry in %.2fs...",
                attempt,
                max_retries,
                exc,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def _wait_for_web_channel_listening(
    channel: Any,
    task: asyncio.Task[None],
    *,
    timeout: float = 5.0,
) -> None:
    """Wait until WebChannel has actually bound its listening socket.

    Scheduling ``WebChannel.start`` does not imply listening readiness. Use
    its public listener boundary before the potentially expensive channel
    config pass; Desktop and the static proxy rely on that port at cold start.
    """
    ready = asyncio.create_task(channel.wait_ready(), name="web-channel-ready")
    try:
        done, _ = await asyncio.wait({ready, task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            task.result()
            raise RuntimeError("WebChannel stopped before it began listening")
        if ready not in done:
            raise TimeoutError(f"WebChannel did not bind its listening socket within {timeout:.0f}s")
        ready.result()
        logger.info(
            "[App] WebChannel listening: ws://%s:%s%s",
            channel.config.host,
            channel.config.port,
            channel.config.path,
        )
    finally:
        ready.cancel()
        await asyncio.gather(ready, return_exceptions=True)


async def _stop_web_runtime(
    web_channel: Any,
    web_task: asyncio.Task[None] | None,
) -> None:
    """Stop the Web channel before its long-running start task exits.

    Stopping the channel can make a still-starting task fail with its own
    ``RuntimeError``. That error must not replace a primary startup failure such
    as a readiness timeout originating in the caller, so only cancel a task that
    is still running and consume the result without re-raising it.
    """
    try:
        if web_channel is not None:
            await web_channel.stop()
    finally:
        if web_task is not None:
            if not web_task.done():
                web_task.cancel()
            await asyncio.gather(web_task, return_exceptions=True)


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


@dataclass
class GatewayRestartRequest:
    requested: bool = False
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)


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
















def _build_acp_route_binding(
        *,
        path: str,
        channel_id: str,
        forward_methods: frozenset[str],
        forward_no_local_handler_methods: frozenset[str],
        on_message_cb,
) -> GatewayRouteBinding:
    return GatewayRouteBinding(
        path=path,
        channel_id=channel_id,
        forward_methods=forward_methods,
        forward_no_local_handler_methods=forward_no_local_handler_methods,
    )


async def sync_saved_topic_recommendation(
    client: Any,
    *,
    updated_env_keys: Any,
    reload_scopes: Any,
    config_payload: dict[str, Any] | None,
) -> None:
    """Ask AgentServer to apply a saved topic switch or model.

    ``fetch_agent_unary`` reports a down or rejecting AgentServer as
    ``(False, payload)`` and does not raise. The config file is already saved,
    so this only logs; it does not fail the save.
    """
    from agent_gateway.routing.e2a_proxy import fetch_agent_unary

    try:
        ok, payload = await fetch_agent_unary(
            agent_client=client,
            req_method=ReqMethod.RECOMMENDATION,
            params={
                "action": "apply_saved_config",
                "updated_env_keys": sorted(updated_env_keys or []),
                "reload_scopes": sorted(reload_scopes),
                "config": dict(config_payload or {}),
            },
            session_id="sess_reload",
            user_id="agentos_test",
            channel_id="web",
            label="recommendation",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[App] topic recommendation config sync failed: %s", exc)
        return
    if ok:
        return
    detail = payload if isinstance(payload, dict) else {}
    logger.warning(
        "[App] topic recommendation config sync failed: code=%s error=%s",
        detail.get("code"),
        detail.get("error"),
    )


def _build_route_config_map(bindings: list[GatewayRouteBinding]) -> dict[str, RouteConfig]:
    return {
        binding.path: RouteConfig(
            path=binding.path,
            channel_id=binding.channel_id,
            forward_methods=binding.forward_methods,
            forward_no_local_handler_methods=binding.forward_no_local_handler_methods,
            inbound_interceptor=binding.inbound_interceptor,
            outbound_interceptor=binding.outbound_interceptor,
            cleanup_handler=binding.cleanup_handler,
            disconnect_handler=binding.disconnect_handler,
            session_bind_handler=binding.session_bind_handler,
            ws_channel=binding.ws_channel,
        )
        for binding in bindings
    }


async def _run(
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
        web_ws_deflate: bool = False,
) -> None:
    # IM 平台 (dingtalk/feishu/whatsapp/wechat/xiaoyi/telegram/discord/slack/wecom) 均为
    # 惰性 import: 仅在对应 channel enabled 分支内导入, 避免冷启动时为禁用平台
    # 读取其重依赖 (slack_bolt/telegram/redis/aiohttp 等), 缩短 Gateway 就绪时间。
    from openjiuwen.core.runner import Runner
    from agent_gateway.application.optional_channels import OptionalChannels

    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_listen import (
        register_3rdagent_web_method,
    )
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        _FORWARD_REQ_METHODS,
        WebHandlersBindParams,
        _DummyBus,
        _register_web_handlers,
        spawn_background_task,
    )
    from agent_gateway.channel_manager.web.web_connect import (
        WebChannel,
        WebChannelConfig,
    )
    from agent_gateway.cleanup import start_background_cleanup
    from agent_gateway.common.config import get_config
    from agent_gateway.common.config_panel.config_set_handlers import CONFIG_SET_ENV_MAP
    from agent_gateway.common.updater import UpdaterService
    from agent_gateway.cron import (
        CronController,
        CronSchedulerService,
        create_gateway_cron_store,
    )
    from agent_gateway.extensions.manager import ExtensionManager
    from agent_gateway.extensions.registry import ExtensionRegistry
    from agent_gateway.health_check import (
        GatewayHealthCheckService,
        HealthCheckConfig,
    )
    from agent_gateway.heartbeat import HeartbeatControllerProxy
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    logger.info("[App] Gateway starting, connecting AgentServer: %s", agent_server_url)
    for import_stage, marked_at in _STARTUP_IMPORT_PHASES:
        logger.info(
            "[App] startup import stage=%s process_elapsed=%.2fs",
            import_stage,
            marked_at - _PROCESS_START_T0,
        )
    # 阶段耗时基准:冻结 EXE 排查启动超时要用各阶段时间戳对齐 Desktop 日志。
    startup_t0 = time.monotonic()

    def log_startup_stage(stage: str) -> None:
        logger.info(
            "[App] startup stage=%s process_elapsed=%.2fs run_elapsed=%.2fs",
            stage,
            time.monotonic() - _PROCESS_START_T0,
            time.monotonic() - startup_t0,
        )

    log_startup_stage("run_entered")
    restart_request = GatewayRestartRequest()

    callback_framework = Runner.callback_framework
    extension_registry = ExtensionRegistry.create_instance(
        callback_framework=callback_framework,
        config={},
        logger=logger,
    )
    from agent_gateway.agent_client_mode import (
        ROUTER_CLIENT_TYPES,
        read_agent_client_mode,
        resolve_agent_server_client,
        validate_agent_client_configuration,
    )

    agent_client_mode = read_agent_client_mode()
    validate_agent_client_configuration(get_config() or {}, mode=agent_client_mode)
    extension_manager = ExtensionManager(registry=extension_registry)
    log_startup_stage("extension_manager_created")
    await extension_manager.load_all_extensions()
    logger.info(
        "[App] extensions loaded: %d (elapsed %.2fs)",
        len(extension_manager.list_extensions()),
        time.monotonic() - startup_t0,
    )
    log_startup_stage("extensions_loaded")

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
        log_startup_stage("storage_repositories_wired")

    max_retries = int(os.getenv("AGENT_CONNECT_RETRY", "20"))
    retry_interval = float(os.getenv("AGENT_CONNECT_RETRY_INTERVAL", "3"))

    # 从扩展注册表获取 AgentServerClient（WebSocket 或 Yuanrong 等扩展实现）
    agent_server_ext = extension_registry.get_agent_server_client_extension()
    full_cfg = get_config()
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

    # WebSocket 客户端经 _connect_with_retry 重试连接；扩展客户端（agentos_router
    # 等）由网关直接调用一次 connect()，不走重试
    if isinstance(client, WebSocketAgentServerClient):
        await _connect_with_retry(
            client,
            agent_server_url,
            max_retries=max_retries,
            interval=retry_interval,
        )
    else:
        await client.connect("")
    log_startup_stage("agent_server_connected")

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
    log_startup_stage("runtime_config_loaded")

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
    log_startup_stage("heartbeat_started")

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

    async def _on_config_saved(
            updated_env_keys: set[str] | None = None,
            *,
            env_updates: dict[str, str] | None = None,
            config_payload: dict[str, Any] | None = None,
            reload_options: dict[str, Any] | None = None,
    ) -> bool:
        browser_runtime_keys = {
            "MODEL_PROVIDER",
            "MODEL_NAME",
            "API_BASE",
            "API_KEY",
            "VIDEO_PROVIDER",
            "VIDEO_MODEL_NAME",
            "VIDEO_API_BASE",
            "VIDEO_API_KEY",
            "AUDIO_PROVIDER",
            "AUDIO_MODEL_NAME",
            "AUDIO_API_BASE",
            "AUDIO_API_KEY",
            "VISION_PROVIDER",
            "VISION_MODEL_NAME",
            "VISION_API_BASE",
            "VISION_API_KEY",
        }
        _reload_max_retries = 3
        _reload_retry_backoff_base = 2.0  # seconds: 2, 4, 8

        client.set_or_update_server_config(
            config=dict(config_payload or {}),
            env=dict(env_updates or {}),
        )

        from agent_gateway.common.config_panel.config_set_handlers import (
            needs_agent_reload,
        )

        reload_scopes = {
            str(scope)
            for scope in (dict(reload_options or {}).get("reload_scopes") or [])
            if isinstance(scope, str) and scope
        }

        if needs_agent_reload(reload_scopes):
            # reload 是全局配置热重载，不绑定特定 user。但 faas 沙箱 acquire instance
            # 需要 X-Session-Context（由 envelope.user_id 透传成 sessionCtxID）；user_id 为空
            # 时 faas 拿不到 sessionCtxID，acquire lease 走 init 路径失败
            # （"connect runtime failed"），invocation 60s 超时，首登 config.set 后前端卡死+1001。
            # user_id 还必须是 faas 沙箱配置了可工作区的真实用户：faas 沙箱按 user_id
            # 映射 /home/<user>/.jiuwenswarm 工作区，非配置用户（infra/root）建沙箱失败
            # （code 80004 "Failed to create sandbox"）。agentos_test 是 agentos 部署的默认
            # 用户（gateway-config.yaml ssh username），有完整工作区，复用其 warm instance。
            reload_env = e2a_from_agent_fields(
                request_id=f"agent-reload-{uuid_module.uuid4().hex[:8]}",
                channel_id="",
                session_id="sess_reload",
                user_id="agentos_test",
                req_method=ReqMethod.AGENT_RELOAD_CONFIG,
                params={
                    # config: full config snapshot after save; Agent should prefer this over local yaml.
                    "config": dict(config_payload or {}),
                    # env: incremental environment updates; missing keys mean unchanged.
                    "env": dict(env_updates or {}),
                    **dict(reload_options or {}),
                },
            )

            def persistent_models_unapplied(response: Any) -> bool:
                payload = getattr(response, "payload", None)
                if isinstance(payload, dict) and payload.get("code") == "PERSISTENT_MODELS_NOT_APPLIED":
                    logger.warning("[App] worker could not apply persistent models: %s", payload.get("error"))
                    return True
                return False

            reload_ok = False
            last_error: Exception | None = None
            runtime_failed_reject = False
            for attempt in range(_reload_max_retries + 1):
                try:
                    reload_resp = await client.send_request(reload_env)
                    if getattr(reload_resp, "ok", False):
                        reload_ok = True
                        break
                    err_payload = getattr(reload_resp, "payload", None) or {}
                    if persistent_models_unapplied(reload_resp):
                        return False
                    err_msg = (
                        err_payload.get("error")
                        if isinstance(err_payload, dict)
                        else err_payload
                    )
                    err_str = str(err_msg or "")
                    # ValidationError 是配置格式问题，不需要重试也不需要重启 gateway
                    if any(kw in err_str for kw in ("ValidationError", "validation error", "Field required")):
                        logger.warning("[App] agent.reload_config validation error (non-fatal): %s", err_str)
                        return False
                    last_error = RuntimeError(f"agent.reload_config rejected: {err_msg}")
                    # AgentServer runtime 处于 FAILED：server 侧已因本次 reload 请求
                    # 触发 worker 重启自愈；Gateway 自重启修复不了它，只会断开前端。
                    runtime_failed_reject = (
                        "RUNTIME_FAILED" in err_str or "runtime failed" in err_str.lower()
                    )
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    runtime_failed_reject = False

                if attempt < _reload_max_retries:
                    delay = _reload_retry_backoff_base ** (attempt + 1)
                    logger.warning(
                        "[App] agent.reload_config attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1, _reload_max_retries + 1, delay, last_error,
                    )
                    await asyncio.sleep(delay)

            if not reload_ok and runtime_failed_reject:
                # 恢复尝试未达成时再多等两轮：worker 可能仍在 warming（import 慢）。
                for _attempt in range(2):
                    await asyncio.sleep(8.0)
                    try:
                        reload_resp = await client.send_request(reload_env)
                        if getattr(reload_resp, "ok", False):
                            reload_ok = True
                            break
                        if persistent_models_unapplied(reload_resp):
                            return False
                    except Exception as e:  # noqa: BLE001
                        last_error = e
                if reload_ok:
                    logger.info("[App] agent.reload_config succeeded after runtime recovery")

            if not reload_ok and not runtime_failed_reject:
                logger.critical(
                    "[App] agent.reload_config failed after %d attempts, scheduling restart: %s",
                    _reload_max_retries + 1, last_error,
                )
                _schedule_gateway_restart(restart_request)
                return False

            if not reload_ok:
                # RUNTIME_FAILED：重启 Gateway 修复不了 AgentServer 的 runtime
                # （反而把前端断线）。保持 Gateway 在线；配置已写盘，下次保存
                # 会再次触发 reload 并重新尝试恢复。
                logger.critical(
                    "[App] agent.reload_config rejected with RUNTIME_FAILED; "
                    "AgentServer runtime recovery failed, keeping Gateway up: %s",
                    last_error,
                )
                return False

            # reload 成功：config 变更后让 agent 侧 prewarm channels 落到新配置。
            _schedule_agent_prewarm_sync(
                "agent-prewarm-sync-after-config",
                delay_seconds=3.0,
            )

            if updated_env_keys and (browser_runtime_keys & set(updated_env_keys)):
                restart_env = e2a_from_agent_fields(
                    request_id=f"browser-restart-{uuid_module.uuid4().hex[:8]}",
                    channel_id="",
                    session_id="sess_reload",
                    user_id="agentos_test",
                    req_method=ReqMethod.BROWSER_RUNTIME_RESTART,
                )
                try:
                    await client.send_request(restart_env)
                except Exception as e:  # noqa: BLE001
                    logger.warning("[App] browser runtime restart failed (non-fatal): %s", e)

        # 主动推荐：enabled 变更时同步 proactive.tick job（创建/删除）
        proactive_keys = {
            "proactive_recommendation_enabled",
        }
        if updated_env_keys and (proactive_keys & set(updated_env_keys)):
            try:
                from agent_gateway.cron.proactive_cron_sync import (
                    sync_proactive_tick_job,
                )
                await sync_proactive_tick_job(cron_controller, config_payload)
            except Exception as e:  # noqa: BLE001  # 兜底：proactive 同步失败不阻断配置保存
                logger.warning("[App] proactive.tick sync on config save failed: %s", e)

        # YAML is already on disk. Pool seed and model swap run on AgentServer.
        should_sync_topic = bool(
            (updated_env_keys and "topic_recommendation_enabled" in updated_env_keys)
            or "model" in reload_scopes
        )
        if should_sync_topic:
            spawn_background_task(
                sync_saved_topic_recommendation(
                    client,
                    updated_env_keys=updated_env_keys,
                    reload_scopes=reload_scopes,
                    config_payload=config_payload,
                ),
                name="topic-recommendation-config-sync",
            )
        return True

    web_channel = None
    tui_channel = None
    web_config = WebChannelConfig(
        enabled=True,
        host=web_host,
        port=web_port,
        path=web_path,
        ws_permessage_deflate=web_ws_deflate,
    )
    web_channel = WebChannel(web_config, _DummyBus(), agent_client=client)
    register_3rdagent_web_method(web_channel)

    async def show_prompt_suggestion(session_id: str, request_id: str, payload: dict[str, Any]) -> None:
        """Forward a suggestion AgentServer already checked and recorded."""
        if payload.get("event_type") != PROMPT_SUGGESTION_READY_EVENT:
            return
        await web_channel.send(Message(
            id=f"prompt-suggestion:{payload.get('suggestion_id')}",
            type="event",
            channel_id=web_channel.channel_id,
            session_id=session_id,
            params={},
            timestamp=time.time(),
            ok=True,
            payload=payload,
        ))

    async def show_recommendation_event(event_type: str, payload: dict[str, Any]) -> None:
        if event_type in {TOPIC_STARTERS_READY_EVENT, TOPIC_STARTERS_CONTROL_CHANGED_EVENT}:
            await web_channel.broadcast_event(event_type, payload)

    message_handler.on_prompt_suggestion = show_prompt_suggestion
    message_handler.on_recommendation_event = show_recommendation_event

    # 注入 Git diff 监控注册表(设计文档阶段10):
    # 1. 让 ``_mark_git_watcher_dirty`` 能通过 ``channel.git_watcher_registry`` 唤醒轮询
    # 2. 通过 ``set_channel`` 让 registry 拿到 send_event 的发送句柄
    from agent_gateway.channel_manager.web.git_diff_watcher import (
        get_git_diff_watcher_registry,
    )
    _git_watcher_registry = get_git_diff_watcher_registry()
    web_channel.git_watcher_registry = _git_watcher_registry
    _git_watcher_registry.set_channel(web_channel)

    # Git watch 状态计算委托：diff 状态、项目解析与工作目录访问都在目标
    # AgentServer 注入目录完成，Gateway 只做指纹比对与事件推送（方案 §10.6 C）。
    async def _git_diff_status_fetcher(request: dict):
        from agent_gateway.routing.e2a_proxy import fetch_git_diff_status

        hunk_paths = request.get("hunk_paths")
        return await fetch_git_diff_status(
            agent_client=client,
            project_id=request.get("project_id"),
            session_id=request.get("session_id") or None,
            include_files=request.get("include_files"),
            include_hunks=request.get("include_hunks"),
            hunk_paths=list(hunk_paths) if hunk_paths else None,
            user_id=request.get("user_id"),
            channel_id="web",
        )

    _git_watcher_registry.set_diff_status_fetcher(_git_diff_status_fetcher)

    _register_web_handlers(
        WebHandlersBindParams(
            channel=web_channel,
            agent_client=client,
            message_handler=message_handler,
            channel_manager=channel_manager,
            on_config_saved=_on_config_saved,
            heartbeat_service=heartbeat_service,
            cron_controller=cron_controller,
            heartbeat_controller=heartbeat_controller,
            updater_service=updater_service,
        )
    )

    async def _media_normalizer(params: dict[str, Any], session_id: str | None) -> None:
        # A1g：小图落盘经 E2A 到 AgentServer，闭包捕获启动装配的 client
        await normalize_chat_media_attachments(
            params, session_id, agent_client=client, user_id=None
        )

    extension_registry.bind_application_plugins(
        web_channel,
        agent_client=client,
        media_attachment_normalizer=_media_normalizer,
    )
    log_startup_stage("web_handlers_registered")

    def _make_norm_and_forward(
            forward_methods: set[str] | frozenset[str],
            no_local_methods: set[str] | frozenset[str],
            source_label: str,
            local_methods: AbstractSet[str] | None = None,
    ):
        async def _norm_and_forward(msg: Message) -> bool:
            req_method = getattr(msg, "req_method", None)
            if req_method is None:
                # 无法解析的方法不能进入 _normalize_gateway_message（那里会回退
                # 成 CHAT_SEND），交给本地查找返回 METHOD_NOT_FOUND。
                return False
            method_val = getattr(req_method, "value", None) or ""
            route = route_req_method(
                method_val,
                forward_methods=forward_methods,
                local_methods=local_methods,
            )
            if route in (ROUTE_LOCAL, ROUTE_DENIED):
                # 本地方法不转发；命中拒绝前缀时也不外发，交由本地 handler
                # 查找统一返回 METHOD_NOT_FOUND。
                return False
            with trace_stage("preprocess.credentials"):
                await _refresh_expired_login_credential(msg)
            normalized = _normalize_gateway_message(msg)
            # session.create 主路径注入 work_mode 归一化(与 fallback _session_create
            # 共用同一 helper resolve_session_work_mode_params,保持主路径/fallback 一致):
            # 成功时写回归一化后的 project_id/project_dir/work_mode 到 params,
            # 转发到 AgentServer 后由其 session.create 处理逻辑使用;
            # 失败时(非法 work_mode)不写回,保留原始 params 由 AgentServer 或
            # fallback _session_create 返回 BAD_REQUEST。
            if method_val == "session.create":
                _inject_session_work_mode(normalized)
            await channel_manager.deliver_to_message_handler(normalized)
            logger.debug("[App] %s 入站 -> MessageHandler: id=%s channel_id=%s", source_label, msg.id, msg.channel_id)
            if route == ROUTE_FORWARD_UNKNOWN or method_val in no_local_methods:
                return True
            return False

        return _norm_and_forward

    homepage_request_dispatched = asyncio.Event()
    web_norm_and_forward = _make_norm_and_forward(
        _FORWARD_REQ_METHODS,
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        "Web",
        # 本地 handler 注册表即本地白名单：不在转发表内的已注册方法留在本地，
        # 其余未知方法默认透传给 AgentServer。视图是实时的，后注册方法同样生效。
        local_methods=web_channel.registered_methods(),
    )
    channel_manager.register_channel_with_inbound(web_channel, web_norm_and_forward)

    async def _mark_homepage_request_dispatched(callback, msg: Message):
        result = callback(msg) if callback is not None else None
        if inspect.isawaitable(result):
            result = await result
        if not homepage_request_dispatched.is_set():
            homepage_request_dispatched.set()
            logger.info(
                "[App] first Web request dispatched; optional channel configuration may proceed"
            )
        return result

    web_channel.wrap_message_callback(_mark_homepage_request_dispatched)

    # The Desktop homepage only depends on WebChannel, MessageHandler and
    # Cron. Bind and serve that path before importing/constructing ACP and
    # TUI routes, which are independent optional entrypoints.
    web_task = asyncio.create_task(web_channel.start(), name="web-channel")
    await _wait_for_web_channel_listening(web_channel, web_task)
    log_startup_stage("web_channel_listening")

    await channel_manager.start_dispatch()
    log_startup_stage("message_dispatch_started")
    await cron_scheduler.start()
    if config_updater is not None:
        await config_updater.start()
    log_startup_stage("cron_scheduler_started")

    # Give the browser event loop a chance to complete its pending WebSocket
    # handshake and deliver connection.ack before optional route setup does
    # synchronous construction work.
    await asyncio.sleep(0.20)
    log_startup_stage("web_core_ready")

    # ── V2: TUI 独立 Channel（出站契约 + 五维索引）──
    # GatewayServer 仍是 /tui ws 宿主 + 入站帧解析 + local handler 派发（install 仍挂
    # gateway_server）；TuiChannel 只接管出站 send 与 ws 五维索引，被 GatewayServer
    # 在 forward 分支委托 register_ws/unregister_ws。移除原 register_external_channel("tui",
    # gateway_server)，tui 出站不再走 GatewayServer.send（其 routing_target 反查缺五维索引）。
    from agent_gateway.channel_manager.tui.tui_channel import (
        TuiChannel,
        TuiChannelConfig,
    )
    from agent_gateway.channel_manager.tui.tui_connect import (
        CLI_FORWARD_NO_LOCAL_HANDLER_METHODS,
        CLI_FORWARD_REQ_METHODS,
        CliRouteBindParams,
        build_cli_route_binding,
    )

    tui_channel = TuiChannel(TuiChannelConfig(enabled=True), _DummyBus())
    tui_norm_and_forward = _make_norm_and_forward(
        CLI_FORWARD_REQ_METHODS,
        CLI_FORWARD_NO_LOCAL_HANDLER_METHODS,
        "TUI",
    )
    channel_manager.register_channel_with_inbound(tui_channel, tui_norm_and_forward)

    # Web/TUI 注册后再订阅连接钩子，否则 get_channel 拿不到 channel。
    subscribe_fn = getattr(client, "set_channel_manager", None)
    if callable(subscribe_fn):
        subscribe_fn(channel_manager)

    acp_inbound_server = _InboundGatewayServer(
        lambda msg: _normalize_and_forward_message(msg, channel_manager)
    )
    await acp_inbound_server.start()

    route_bindings = [
        _build_acp_route_binding(
            path="/acp",
            channel_id="acp",
            forward_methods=_FORWARD_REQ_METHODS,
            forward_no_local_handler_methods=_FORWARD_NO_LOCAL_HANDLER_METHODS,
            on_message_cb=acp_inbound_server.handle_message,
        ),
        build_cli_route_binding(
            CliRouteBindParams(
                agent_client=client,
                message_handler=message_handler,
                third_agent=third_agent,
                on_config_saved=_on_config_saved,
                path="/tui",
                channel_id="tui",
                cron_controller=cron_controller,
                heartbeat_controller=heartbeat_controller,
                ws_channel=tui_channel,
            )
        ),
    ]

    gateway_server_config = GatewayServerConfig(
        enabled=True,
        host=os.getenv("GATEWAY_HOST", "127.0.0.1"),
        port=int(os.getenv("GATEWAY_PORT", "19001")),
        routes=_build_route_config_map(route_bindings),
    )
    gateway_server = GatewayServer(gateway_server_config, _DummyBus())
    gateway_server.message_handler_ref = message_handler
    for binding in route_bindings:
        route_config = gateway_server_config.routes[binding.path]
        # tui 出站已由 TuiChannel 接管（register_channel_with_inbound），
        # 不再把 gateway_server 注册为 tui channel；acp 等仍走 gateway_server。
        # 但 install（register_cli_handlers 注册 local handler 如 _chat_send）
        # 仍须执行——local handler 依赖 gateway_server 上的 session/request 索引。
        if route_config.channel_id != "tui":
            channel_manager.register_external_channel(route_config.channel_id, gateway_server)
        if binding.install is not None:
            binding.install(gateway_server)
    gateway_server.on_message(acp_inbound_server.handle_message)
    log_startup_stage("gateway_routes_registered")

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
    await gateway_server.start()
    logger.info(
        "[App] gateway server listening (elapsed %.2fs)",
        time.monotonic() - startup_t0,
    )
    log_startup_stage("gateway_server_listening")
    gateway_server_task = asyncio.create_task(
        gateway_server.wait_until_closed(),
        name="acp-gateway-server",
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

    restart_requested = False
    try:
        tasks_to_wait = [
            task
            for task in (gateway_server_task, web_task, channel_config_task)
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
        await optional_channels.stop_a2a()
        if gateway_server_task is not None:
            gateway_server_task.cancel()
            try:
                await gateway_server_task
            except asyncio.CancelledError:
                pass
        await gateway_server.stop()
        await acp_inbound_server.stop()
        if tui_channel is not None:
            await tui_channel.stop()
        await _stop_web_runtime(web_channel, web_task)

        await optional_channels.stop_channels()

        await cron_scheduler.stop()
        await channel_manager.stop_dispatch()
        await heartbeat_service.stop()
        await message_handler.stop_forwarding()
        await client.disconnect()

        if storage_ctx is not None:
            try:
                from agent_gateway.storage_assembly.setup import (
                    teardown_gateway_storage_repositories,
                )

                await teardown_gateway_storage_repositories(storage_ctx)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[App] gateway storage teardown failed: %s", exc)

        _hold_release_task.cancel()
        try:
            await _hold_release_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[App] abandoned hold release task failed")
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except (asyncio.CancelledError, Exception):
            pass

        await shutdown_gateway_telemetry()
        logger.info("[App] Gateway stopped")

    if restart_requested:
        _exec_gateway_restart()


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

    with gateway_workspace_lock(get_infra_data_dir()):
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
