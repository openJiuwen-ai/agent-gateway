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
import inspect
import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, urlparse

# Include entry-module import/configuration work in later startup phase logs.
# PyInstaller boot time is intentionally outside this boundary.
_PROCESS_START_T0 = time.monotonic()
_STARTUP_IMPORT_PHASES: list[tuple[str, float]] = [("entry", _PROCESS_START_T0)]


def _mark_startup_import_phase(stage: str) -> None:
    _STARTUP_IMPORT_PHASES.append((stage, time.monotonic()))

from openjiuwen_protocol.message import (
    PROMPT_SUGGESTION_READY_EVENT,
    TOPIC_STARTERS_CONTROL_CHANGED_EVENT,
    TOPIC_STARTERS_READY_EVENT,
    Message,
)
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from agent_gateway.common.media_capability_config import (
    migrate_media_capability_switches,
)
from agent_gateway.common.ws_diagnostics import (
    describe_ws_exception,
    describe_ws_peer,
    format_ws_diagnostics,
)

# user_id 白名单: 仅允许字母数字及 _-, 拒绝路径遍历字符
_SAFE_USER_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
# --- Early --dotenv parsing (before jiuwenswarm imports) ---
from agent_gateway.dotenv_early import load_dotenv_runtime, parse_dotenv_early

parse_dotenv_early("agent-gateway")
_mark_startup_import_phase("dotenv_parsed")

# Gateway 自带日志初始化：共同安装时母仓已配置则复用，独立安装时按同一路径自建
from agent_gateway.common.logging_setup import setup_gateway_logger

setup_gateway_logger()
_mark_startup_import_phase("logging_ready")

# Standalone entrypoints retain workspace preparation; Desktop/app already do
# it before spawning us and pass the marker to avoid duplicate disk work.
from agent_gateway.common.paths import (
    cleanup_stale_openjiuwen_descs,
    prepare_runtime_workspace,
)

cleanup_stale_openjiuwen_descs()
if os.environ.get("JIUWENSWARM_RUNTIME_WORKSPACE_READY") != "1":
    prepare_runtime_workspace(cleanup_stale_descs=False)
_mark_startup_import_phase("runtime_workspace_ready")

from openjiuwen.core.common.logging import (
    LogManager,  # pylint: disable=wrong-import-order
)

from agent_gateway.channel_manager.base import BaseWebChannel
from agent_gateway.connection_registry import ConnectionRegistry

_mark_startup_import_phase("openjiuwen_and_channel_base_imported")

# --- Now safe to import jiuwenswarm modules ---
from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpGatewayBridge
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
    get_root_dir,
    get_user_workspace_dir,
)
from agent_gateway.common.process_supervision import (
    GATEWAY_RESTART_EXIT_CODE,
    SUPERVISOR_PID_ENV,
)
from agent_gateway.common.security.ws_origin import (
    extract_handshake_request,
    get_header_value,
)
from agent_gateway.config_saved import (
    ConfigSavedContext,
    handle_config_saved,
)
from agent_gateway.message_normalization import (
    inject_session_work_mode as _inject_session_work_mode,
    normalize_and_forward_message as _normalize_and_forward_message,
    normalize_gateway_message as _normalize_gateway_message,
    refresh_expired_login_credential as _refresh_expired_login_credential,
)
from agent_gateway.outbound_delivery import OutboundMessageDelivery
from agent_gateway.inbound_server import (
    InboundGatewayServer as _InboundGatewayServer,
)
from agent_gateway.inbound_request_dispatcher import InboundRequestDispatcher
from agent_gateway.restart import (
    GatewayRestartRequest,
    exec_gateway_restart as _exec_gateway_restart,
    schedule_gateway_restart as _schedule_gateway_restart,
    wait_for_gateway_tasks_or_restart as _wait_for_gateway_tasks_or_restart,
)
from agent_gateway.route_config import (
    RouteConfig,
    GatewayServerConfig,
    LocalHandlerContext as _LocalHandlerContext,
    build_acp_route_binding as _build_acp_route_binding,
    build_route_config_map as _build_route_config_map,
)
from agent_gateway.agent_prewarm import AgentPrewarmCoordinator
from agent_gateway.runtime_config import load_gateway_runtime_config
from agent_gateway.routing.route_binding import GatewayRouteBinding
from agent_gateway.startup_retry import (
    _connect_with_retry,
    _wait_for_web_channel_listening,
)

_mark_startup_import_phase("gateway_core_imports_loaded")

_logging_yaml = get_root_dir() / "config" / "logging.yaml"
if _logging_yaml.exists():
    from openjiuwen.core.common.logging.log_config import configure_log

    configure_log(str(_logging_yaml))
else:
    # Reduce openjiuwen internal logs (keep Gateway logs)
    for _lg in LogManager.get_all_loggers().values():
        _lg.set_level(logging.CRITICAL)
_mark_startup_import_phase("logging_configured")

_env_file = get_env_file()
load_dotenv_runtime(dotenv_path=_env_file, override=True)
migrate_media_capability_switches(_env_file)
apply_free_search_runtime_defaults()
_mark_startup_import_phase("runtime_environment_applied")

logger = logging.getLogger("gateway")

# Keep gateway idle-finalize fallback aligned with ACP channel default.
_PROMPT_IDLE_FINALIZE_SECONDS = 3.0

class GatewayServer(BaseWebChannel):
    """通用多路路由 WebSocket Gateway Server。

    支持多个路径（如 /acp、/cli），每条路径可以有独立的 channel_id 和本地 handler。
    本地 handler 优先处理请求，未处理或无匹配则 forward 到 MessageHandler。
    """

    def __init__(self, config: GatewayServerConfig, router) -> None:
        super().__init__(config, router)
        self.config = config
        self.bus = router
        self._server = None
        self._running = False
        self._on_message_cb = None
        self._connections = ConnectionRegistry(self._client_route_key)
        self._clients = self._connections.clients
        # V2: key 升级为 (channel_id, session_id, agent_ref_str|None)，
        # 兼容旧 2-tuple key（agent_ref 为 None 时回退）。
        self._request_to_client = self._connections.request_to_client
        self._session_to_client = self._connections.session_to_client
        # ACP 延迟绑定：session 在握手期先挂起，等首个 agent 请求 promote。
        self._pending_session_clients = self._connections.pending_session_clients
        self.message_handler_ref = None
        self._acp_bridge = AcpGatewayBridge(
            self._dispatch_on_message,
            bind_session_client=self._bind_acp_session_client,
            channel_id="acp",
            idle_finalize_seconds=lambda: _PROMPT_IDLE_FINALIZE_SECONDS,
        )
        self._outbound_delivery = OutboundMessageDelivery(
            routes=self.config.routes,
            request_to_client=self._request_to_client,
            session_to_client=self._session_to_client,
            client_route_key=self._client_route_key,
            extract_routing_session_id=self._extract_routing_session_id,
            lookup_client=self._lookup_client,
            find_channel_clients=self._find_channel_clients,
            acp_bridge=self._acp_bridge,
        )
        self._inbound_dispatcher = InboundRequestDispatcher(self)
        self._install_default_route_hooks()

    @staticmethod
    def _extract_ws_user_id(ws: Any) -> str | None:
        """从 WebSocket 握手 Header 或 URL 查询参数读取 user_id。

        优先从 X-User-Id Header 读取（TUI / 代理注入场景）；
        浏览器 ``new WebSocket()`` 无法设置自定义 Header，回退到 URL query string。
        """
        headers = (
            getattr(getattr(ws, "request", None), "headers", None)
            or getattr(ws, "request_headers", None)
        )
        raw = get_header_value(headers, "X-User-Id")
        if raw is not None:
            text = str(raw).strip()
            if text and _SAFE_USER_ID_RE.match(text):
                return text

        raw_path = (
            getattr(ws, "path", "")
            or getattr(getattr(ws, "request", None), "path", "")
        )
        if raw_path:
            parsed = urlparse(raw_path)
            qs = parse_qs(parsed.query)
            user_ids = qs.get("user_id", [])
            if user_ids:
                candidate = str(user_ids[0]).strip()
                if candidate and _SAFE_USER_ID_RE.match(candidate):
                    return candidate
        return None

    @staticmethod
    def _connection_user_id(ws: Any) -> str | None:
        """返回连接建立时缓存的 user_id（来自握手 Header）。"""
        uid = getattr(ws, "_gateway_user_id", None)
        if uid is None:
            return None
        text = str(uid).strip()
        return text or None

    @staticmethod
    def _connection_agent_type(ws: Any) -> str:
        """返回当前 WS 连接绑定的 agent_type（缺省 jiuwenswarm）。"""
        raw = getattr(ws, "_gateway_agent_type", None)
        text = str(raw or "jiuwenswarm").strip().lower()
        return text or "jiuwenswarm"

    @staticmethod
    def _invoke_local_handler(
        handler: Callable[..., Awaitable[None]],
        ctx: _LocalHandlerContext,
    ) -> Awaitable[None]:
        kwargs: dict[str, Any] = {}
        if "user_id" in inspect.signature(handler).parameters:
            kwargs["user_id"] = ctx.user_id
        return handler(ctx.ws, ctx.req_id, ctx.params, ctx.session_id, **kwargs)

    @staticmethod
    def _client_route_key(
        channel_id: str | None,
        scoped_id: str | None,
        agent_ref: Any = None,
    ) -> tuple | None:
        """构造客户端路由键。

        V2: 可选 agent_ref 作为第三维，支撑同 session 多 agent_ref 共存（场景 2）。
        传入 agent_ref 时返回 (channel_id, scoped_id, agent_ref_str) 三元组；
        不传时返回 (channel_id, scoped_id) 二元组，兼容旧路径。
        """
        channel = str(channel_id or "").strip()
        scope = str(scoped_id or "").strip()
        if not channel or not scope:
            return None
        if agent_ref is not None:
            ar_str = ""
            if hasattr(agent_ref, "mode") and hasattr(agent_ref, "id"):
                ar_str = f"{agent_ref.mode}:{agent_ref.id}"
            elif isinstance(agent_ref, dict):
                ar_str = f"{agent_ref.get('mode', '')}:{agent_ref.get('id', '')}"
            elif isinstance(agent_ref, str) and agent_ref.strip():
                ar_str = agent_ref.strip()
            if ar_str:
                return (channel, scope, ar_str)
        return (channel, scope)

    def _find_channel_clients(self, channel_id: str) -> list[Any]:
        return self._connections.find_channel_clients(channel_id)

    def _lookup_client(
        self,
        table: dict[tuple, Any],
        channel_id: str | None,
        scope: str | None,
        agent_ref: Any = None,
    ) -> Any | None:
        """按 (channel, scope, agent_ref?) 查找客户端 ws，2↔3 元组双向兜底。

        - 优先精确匹配（agent_ref 非空时 3 元组，否则 2 元组）。
        - 3 元组 MISS → 降级 2 元组（响应侧丢 agent_ref 时仍能命中）。
        - 2 元组 MISS → 升级扫描所有以 (channel, scope) 开头的 3 元组
          （注册侧有 agent_ref、查找侧丢 agent_ref 时仍能命中）。
        - 裸 scope 字符串兜底。
        防御性双向兜底，避免链路某环节丢 agent_ref 导致响应 dropped（设计 §6.3）。
        """
        key = self._client_route_key(channel_id, scope, agent_ref)
        if key is None:
            return None
        ws = table.get(key)
        if ws is not None:
            return ws
        # 3 → 2 降级
        if len(key) >= 3:
            ws = table.get((key[0], key[1]))
            if ws is not None:
                return ws
        # 2 → 3 升级扫描
        if len(key) == 2:
            for k, v in table.items():
                # (channel, scope, agent_ref) 三元组，前两段与 (channel, scope) 匹配
                if isinstance(k, tuple) and len(k) == 3 and k[:2] == key:
                    return v
        # 裸 scope 兜底
        return table.get(scope)

    def is_session_bound_to_client(self, channel_id: str, session_id: str, ws: Any) -> bool:
        return self._connections.is_session_bound_to_client(
            channel_id, session_id, ws
        )

    def get_active_session_ids(
        self,
        channel_id: str,
        exclude_ws: Any = None,
    ) -> set[str]:
        """返回在指定 channel 下、仍处于活跃连接绑定的 session_id 集合。

        用于 session.list 标记 active_in_window，供前端在 /resume 前拦截冲突会话。
        排除 exclude_ws（通常是发起 session.list 请求的连接本身），并跳过已关闭的 ws，
        与实时防线（forward 阶段 SESSION_IN_USE 检查）口径保持一致。
        """
        return self._connections.active_session_ids(channel_id, exclude_ws)

    @staticmethod
    def _extract_routing_session_id(msg, *, include_top_level: bool = True) -> str | None:
        """Best-effort session id from message fields for outbound event routing."""
        if include_top_level:
            sid = getattr(msg, "session_id", None)
            if sid is not None and str(sid).strip():
                return str(sid).strip()
        payload = getattr(msg, "payload", None)
        if isinstance(payload, dict):
            sid = payload.get("session_id")
            if sid is not None and str(sid).strip():
                return str(sid).strip()
        params = getattr(msg, "params", None)
        if isinstance(params, dict):
            sid = params.get("session_id")
            if sid is not None and str(sid).strip():
                return str(sid).strip()
        return None

    @staticmethod
    def _ws_is_open(ws: Any) -> bool:
        return ws is not None and not bool(getattr(ws, "closed", False))

    async def _send_frame_to_ws(
        self,
        ws: Any,
        frame: dict[str, Any],
        *,
        channel_id: str | None,
        request_id: str | None,
        session_id: str | None,
    ) -> bool:
        if not self._ws_is_open(ws):
            return False
        try:
            await ws.send(json.dumps(frame, ensure_ascii=False))
            return True
        except ConnectionClosed:
            logger.info(
                "[GatewayServer] WebSocket closed while sending: channel_id=%s session_id=%s id=%s",
                channel_id,
                session_id,
                request_id,
            )
            return False

    def on_message(self, callback) -> None:
        self._on_message_cb = callback

    async def _dispatch_on_message(self, msg) -> bool:
        if self._on_message_cb is None:
            return False
        result = self._on_message_cb(msg)
        if asyncio.iscoroutine(result):
            result = await result
        return bool(result)

    def _bind_acp_session_client(self, session_id: str, ws: Any, agent_ref: Any = None) -> None:
        self._connections.bind_acp_session_client(session_id, ws, agent_ref)

    async def _bind_route_session_client(
        self,
        route: RouteConfig,
        session_id: str,
        ws: Any,
    ) -> bool:
        return await self._connections.bind_route_session_client(
            route, session_id, ws
        )

    async def _promote_pending_session_client(
        self,
        route: RouteConfig,
        session_key: tuple[str, str],
    ) -> None:
        await self._connections.promote_pending_session_client(
            route, session_key
        )

    def _get_message_handler(self):
        return self.message_handler_ref

    def _install_default_route_hooks(self) -> None:
        for route in self.config.routes.values():
            if route.channel_id != "acp":
                continue
            if route.inbound_interceptor is not None and route.outbound_interceptor is not None:
                continue
            route.inbound_interceptor = route.inbound_interceptor or self._acp_bridge.inbound_intercept
            route.outbound_interceptor = route.outbound_interceptor or self._acp_bridge.outbound_intercept
            route.cleanup_handler = route.cleanup_handler or self._acp_bridge.cleanup

    async def _process_request(self, *args: Any) -> Any:
        """Reject hooked upgrades (TUI) before 101 when IAM fails."""
        path, request_headers = extract_handshake_request(args)
        request_path = urlparse(path or "").path or (path or "")
        route, _matched = self._resolve_route(request_path)
        ws_channel = getattr(route, "ws_channel", None) if route is not None else None
        channel_name = str(getattr(route, "channel_id", "") or "")
        if ws_channel is None or not await ws_channel.handshake_auth_denied(
            path=path, headers=request_headers, channel=channel_name
        ):
            return None
        logger.warning(
            "GatewayServer 握手拒绝 path=%s channel=%s reason=unauthorized",
            request_path,
            channel_name,
        )
        return ws_channel.unauthorized_handshake_response(args)

    def _resolve_route(self, request_path: str) -> tuple[RouteConfig | None, str]:
        """按精确路径匹配路由；支持常见变体（如尾部斜杠）以避免客户端握手失败。"""
        routes = self.config.routes
        p = (request_path or "").strip()
        if not p:
            return None, request_path
        if p in routes:
            return routes[p], p
        if p != "/" and p.endswith("/") and p.rstrip("/") in routes:
            return routes[p.rstrip("/")], p.rstrip("/")
        if not p.endswith("/") and f"{p}/" in routes:
            return routes[f"{p}/"], f"{p}/"
        return None, p

    async def wait_until_closed(self) -> None:
        """阻塞至底层 WebSocket 服务完全关闭（与 :meth:`start` 配对供主循环持有任务）。"""
        if self._server is None:
            return
        await self._server.wait_closed()

    def register_local_handler(self, path: str, method: str, handler: Callable[..., Awaitable[None]]) -> None:
        """为指定路径注册本地方法 handler。"""
        route = self.config.routes.get(path)
        if route is None:
            route = RouteConfig(path=path, channel_id=path.strip("/"))
            self.config.routes[path] = route
        route.local_handlers[method] = handler

    async def send_response(
            self,
            ws: Any,
            req_id: str,
            *,
            ok: bool,
            payload: dict[str, Any] | None = None,
            error: str | None = None,
            code: str | None = None,
    ) -> None:
        """向指定客户端发送 res 帧（供本地 handler 使用）。"""
        frame: dict[str, Any] = {
            "type": "res",
            "id": req_id,
            "ok": ok,
            "payload": payload or {},
        }
        if not ok:
            frame["error"] = error or "request failed"
            if code:
                frame["code"] = code
        try:
            await ws.send(json.dumps(frame, ensure_ascii=False))
        except Exception:
            logger.debug("send_response failed (client disconnected?)", exc_info=True)

    async def send_event(
            self,
            ws: Any,
            event: str,
            payload: dict[str, Any],
    ) -> None:
        """向指定客户端发送 event 帧（供本地 handler 使用）。"""
        frame: dict[str, Any] = {"type": "event", "event": event, "payload": payload}
        try:
            await ws.send(json.dumps(frame, ensure_ascii=False))
        except Exception:
            logger.debug("send_event failed (client disconnected?)", exc_info=True)

    async def start(self) -> None:
        if self._running or not self.config.enabled:
            return
        try:
            from websockets.legacy.server import serve as ws_serve
        except Exception:  # pragma: no cover
            from websockets import serve as ws_serve

        ws_max_size = 8 * 2**20  # 8 MB — matches AgentServer link

        self._server = await ws_serve(
            self._connection_handler,
            self.config.host,
            self.config.port,
            process_request=self._process_request,
            ping_interval=20,
            ping_timeout=600,
            max_size=ws_max_size,
        )
        self._running = True
        paths = ", ".join(self.config.routes.keys())
        logger.info(
            "[App] Gateway server started: ws://%s:%s [%s]",
            self.config.host,
            self.config.port,
            paths,
        )

    async def stop(self) -> None:
        self._running = False
        close_tasks = [client.close(code=1001, reason="server shutdown") for client in list(self._clients)]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        self._connections.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info("[App] Gateway server stopped")

    async def send(self, msg, *, routing_target: "RoutingTarget | None" = None) -> None:
        await self._outbound_delivery.send(msg, routing_target=routing_target)

    async def _connection_handler(self, ws: Any, path: str | None = None) -> None:
        raw_path = path if path is not None else getattr(ws, "path", "")
        parsed = urlparse(raw_path)
        request_path = parsed.path or raw_path
        remote = getattr(ws, "remote_address", None)

        route, matched_path = self._resolve_route(request_path)
        if route is None:
            await ws.close(code=1008, reason=f"unsupported path: {request_path}")
            return

        self._connections.add_client(ws)

        ws_user_id = self._extract_ws_user_id(ws)
        setattr(ws, "_gateway_user_id", ws_user_id)
        setattr(ws, "_gateway_agent_type", "jiuwenswarm")
        uid_marker = "" if ws_user_id else " uid_empty=yes"
        handshake_sid = ""
        try:
            handshake_sid = str((parse_qs(parsed.query).get("session_id") or [""])[0] or "")
        except Exception:
            handshake_sid = ""
        logger.info(
            "[Gateway] WS handshake X-User-Id: user_id=%r%s channel=%s path=%s session_id=%s",
            ws_user_id,
            uid_marker,
            route.channel_id,
            matched_path,
            handshake_sid,
            extra={"session_id": handshake_sid} if handshake_sid else {},
        )

        # 触发连接钩子（GatewayServer 自身 + 外部 ws_channel，如 TuiChannel 鉴权）
        for hook in self._connect_hooks:
            try:
                result = hook(ws)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:  # pragma: no cover
                logger.warning(
                    "WebChannel on_connect hook error: %s",
                    format_ws_diagnostics(
                        {"remote": remote, "path": request_path},
                        describe_ws_peer(ws),
                        describe_ws_exception(e),
                    ),
                )
        ws_channel = getattr(route, "ws_channel", None)
        if ws_channel is not None:
            for hook in getattr(ws_channel, "_connect_hooks", []):
                try:
                    result = hook(ws)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.warning(
                        "%s on_connect hook error: %s",
                        type(ws_channel).__name__,
                        format_ws_diagnostics(
                            {"remote": remote, "path": request_path},
                            describe_ws_peer(ws),
                            describe_ws_exception(e),
                        ),
                    )

        # 上报连接事件
        if route.ws_channel is not None:
            reporter = getattr(route.ws_channel, "report_connect", None)
            if callable(reporter):
                reporter(ws)

        # connection.ack
        try:
            await ws.send(json.dumps({
                "type": "event",
                "event": "connection.ack",
                "payload": {
                    "protocol_version": "1.0",
                    "transport": route.channel_id,
                },
            }, ensure_ascii=False))
        except Exception:
            self._clients.discard(ws)
            return

        normal_close = False
        try:
            async for raw in ws:
                await self._handle_raw_message(ws, raw, matched_path, route)
            normal_close = True
        except ConnectionClosedError:
            logger.info("[App] WebSocket connection closed: channel=%s", route.channel_id)
        finally:
            if normal_close:
                logger.info(
                    "[App] WebSocket connection closed (normal): channel=%s",
                    route.channel_id,
                )
            (
                stale_request_keys,
                stale_session_keys,
                stale_pending_session_keys,
            ) = self._connections.remove_client(ws)
            # V2: 委托 ws_channel（tui 的 TuiChannel）摘除死 ws + 清 per-ws writer。
            # 放在 _session_to_client 清理之后、disconnect_handler 之前；ws_channel
            # 反查不依赖 _session_to_client，顺序无强约束，但先摘可避免后续投递命中死 ws。
            if route.ws_channel is not None:
                try:
                    await route.ws_channel.unregister_ws(ws)
                except Exception:
                    logger.warning(
                        "GatewayServer delegate unregister_ws to ws_channel failed: path=%s",
                        request_path, exc_info=True,
                    )
                # 上报断连事件
                reporter = getattr(route.ws_channel, "report_disconnect", None)
                if callable(reporter):
                    reporter(ws)
            if route.disconnect_handler is not None:
                try:
                    # Pass stale_request_keys so the handler can recover session_ids
                    # via in-flight stream bookkeeping even when _session_to_client
                    # was overwritten by a subsequent reconnect on the same session.
                    result = route.disconnect_handler(
                        ws, stale_session_keys, stale_request_keys,
                    )
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    logger.warning(
                        "GatewayServer disconnect handler failed: path=%s",
                        request_path,
                        exc_info=True,
                    )
            elif route.cleanup_handler is not None:
                try:
                    route.cleanup_handler(ws)
                except Exception:
                    logger.warning(
                        "GatewayServer cleanup handler failed: path=%s",
                        request_path,
                        exc_info=True,
                    )
            for session_key in stale_session_keys:
                await self._promote_pending_session_client(route, session_key)

            for hook in self._disconnect_hooks:
                try:
                    result = hook(ws, None)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:  # pragma: no cover
                    logger.warning(
                        "%s on_disconnect hook error: %s",
                    )

    async def _handle_raw_message(self, ws: Any, raw: str, request_path: str, route: RouteConfig) -> None:
        await self._inbound_dispatcher.handle_raw_message(
            ws, raw, request_path, route
        )


async def _run(
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
) -> None:
    # IM 平台 (dingtalk/feishu/whatsapp/wechat/xiaoyi/telegram/discord/slack/wecom) 均为
    # 惰性 import: 仅在对应 channel enabled 分支内导入, 避免冷启动时为禁用平台
    # 读取其重依赖 (slack_bolt/telegram/redis/aiohttp 等), 缩短 Gateway 就绪时间。
    from openjiuwen.core.runner import Runner

    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_connect import (
        WebProxyChannelConfig,
    )
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_listen import (
        WebPortManager,
        register_3rdagent_web_method,
    )
    from agent_gateway.channel_manager.web.app_web_handlers import (
        WebHandlersBindParams,
        _register_web_handlers,
    )
    from agent_gateway.channel_manager.web.web_handler_constants import (
        _DummyBus,
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        _FORWARD_REQ_METHODS,
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
    extension_manager = ExtensionManager(registry=extension_registry)
    log_startup_stage("extension_manager_created")
    await extension_manager.load_all_extensions()
    logger.info(
        "[App] extensions loaded: %d (elapsed %.2fs)",
        len(extension_manager.list_extensions()),
        time.monotonic() - startup_t0,
    )
    log_startup_stage("extensions_loaded")

    max_retries = int(os.getenv("AGENT_CONNECT_RETRY", "20"))
    retry_interval = float(os.getenv("AGENT_CONNECT_RETRY_INTERVAL", "3"))

    # 从扩展注册表获取 AgentServerClient（WebSocket 或 Yuanrong 等扩展实现）
    agent_server_ext = extension_registry.get_agent_server_client_extension()
    gateway_config = get_config().get("gateway") or {}
    agent_client_config = gateway_config.get("agent_client") or {}
    using_adx_router = str(agent_client_config.get("type") or "").strip().lower() == "adx_router"
    if using_adx_router:
        if agent_server_ext is None or agent_server_ext.metadata.id != "adx_router":
            raise RuntimeError("Selected adx_router extension failed to load; check gateway.adx configuration")
    if agent_server_ext is not None:
        logger.info("[App] using extension AgentServerClient: %s", agent_server_ext.metadata.name)
        client = agent_server_ext.get_client()
    else:
        client = WebSocketAgentServerClient(ping_interval=20.0, ping_timeout=600.0)

    from agent_gateway.routing.third_agent import get_unsupported_third_agent

    third_agent_ext = extension_registry.get_third_agent_extension()
    if third_agent_ext is not None:
        logger.info("[App] using extension ThirdAgent: %s", third_agent_ext.metadata.name)
        third_agent = third_agent_ext.get_third_agent()
    else:
        third_agent = get_unsupported_third_agent()

    # 如果是 WebSocket 客户端，需要连接；如果是 YuanrongFrontendAgentClient，无需连接
    if isinstance(client, WebSocketAgentServerClient):
        await _connect_with_retry(
            client,
            agent_server_url,
            max_retries=max_retries,
            interval=retry_interval,
        )
    else:
        # YuanrongFrontendAgentClient 是 HTTP 客户端，无需连接
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
            await migrate_legacy_cron_selections(cron_store, user_id=None)
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

    full_cfg, health_check_cfg, channels_cfg = await load_gateway_runtime_config(
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

    _cleanup_task = start_background_cleanup()

    initial_channels_conf: dict = channels_cfg if isinstance(channels_cfg, dict) else {}
    # Optional integrations are applied after the Web/Cron core is available.
    # Start with no applied integration config, then install each entry
    # independently below.  This prevents one bad optional channel from
    # suppressing every later channel or terminating the Gateway.
    channel_manager = ChannelManager(message_handler, config={})
    # 回填引用：MessageHandler 实例化早于 ChannelManager，广播全局事件时需经它取 web channel。
    message_handler.set_channel_manager(channel_manager)

    updater_service = UpdaterService()
    prewarm_coordinator = AgentPrewarmCoordinator(
        client=client,
        channel_manager=channel_manager,
        using_adx_router=using_adx_router,
    )
    _periodic_agent_prewarm_sync = prewarm_coordinator.periodic_agent_prewarm_sync
    _schedule_agent_prewarm_sync = prewarm_coordinator.schedule_agent_prewarm_sync

    config_saved_context = ConfigSavedContext(
        client=client,
        message_handler=message_handler,
        cron_controller=cron_controller,
        restart_request=restart_request,
        schedule_agent_prewarm_sync=_schedule_agent_prewarm_sync,
        schedule_gateway_restart=_schedule_gateway_restart,
    )

    async def _on_config_saved(
            updated_env_keys: set[str] | None = None,
            *,
            env_updates: dict[str, str] | None = None,
            config_payload: dict[str, Any] | None = None,
            reload_options: dict[str, Any] | None = None,
    ) -> bool:
        return await handle_config_saved(
            config_saved_context,
            updated_env_keys=updated_env_keys,
            env_updates=env_updates,
            config_payload=config_payload,
            reload_options=reload_options,
        )

    web_channel = None
    tui_channel = None
    web_config = WebChannelConfig(
        enabled=True,
        host=web_host,
        port=web_port,
        path=web_path,
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
    ):
        async def _norm_and_forward(msg: Message) -> bool:
            method_val = getattr(getattr(msg, "req_method", None), "value", None) or ""
            if method_val not in forward_methods:
                return False
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
            logger.info("[App] %s 入站 -> MessageHandler: id=%s channel_id=%s", source_label, msg.id, msg.channel_id)
            if method_val in no_local_methods:
                return True
            return False

        return _norm_and_forward

    homepage_request_dispatched = asyncio.Event()
    web_norm_and_forward = _make_norm_and_forward(
        _FORWARD_REQ_METHODS,
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        "Web",
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

    from agent_gateway.optional_channel_configurator import OptionalChannelConfigurator
    optional_channel_configurator = OptionalChannelConfigurator(
        channel_manager=channel_manager,
        web_channel=web_channel,
        agent_server_ext=agent_server_ext,
        im_inbound=im_inbound,
        im_outbound=im_outbound,
        dummy_bus_cls=_DummyBus,
        get_config=get_config,
        schedule_agent_prewarm_sync=_schedule_agent_prewarm_sync,
        web_proxy_config_cls=WebProxyChannelConfig,
        web_port_manager_cls=WebPortManager,
    )
    channel_manager.set_config_callback(optional_channel_configurator.apply)

    prewarm_sync_task = (
        asyncio.create_task(
            _periodic_agent_prewarm_sync(),
            name="agent-prewarm-periodic-sync",
        )
        if prewarm_coordinator.should_run_periodic
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
        await prewarm_coordinator.stop_debounce()
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
        if web_task is not None:
            web_task.cancel()
            try:
                await web_task
            except asyncio.CancelledError:
                pass
        if web_channel is not None:
            await web_channel.stop()

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
        for _cid in ("feishu", "xiaoyi"):
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
        if dingtalk_channel is not None and dingtalk_task is not None:
            dingtalk_task.cancel()
            try:
                await dingtalk_task
            except (TypeError, asyncio.CancelledError):
                pass
            await dingtalk_channel.stop()
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

        await cron_scheduler.stop()
        await channel_manager.stop_dispatch()
        await heartbeat_service.stop()
        await message_handler.stop_forwarding()
        await client.disconnect()

        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except (asyncio.CancelledError, Exception):
            pass

        logger.info("[App] Gateway stopped")

    if restart_requested:
        _exec_gateway_restart()


async def _run_with_shutdown_signals(
    *,
    agent_server_url: str,
    web_host: str,
    web_port: int,
    web_path: str,
) -> None:
    """Translate process signals into one idempotent Gateway shutdown request."""

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    previous_handlers: dict[signal.Signals, Any] = {}

    def request_shutdown(_signum: int, _frame: Any) -> None:
        loop.call_soon_threadsafe(shutdown_event.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, request_shutdown)
    try:
        await _run(
            agent_server_url=agent_server_url,
            web_host=web_host,
            web_port=web_port,
            web_path=web_path,
            shutdown_event=shutdown_event,
        )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


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

    install_crash_exit_handler("gateway")
    install_async_dump_handler("gateway")

    # Per-workspace singleton lock: prevents a second Gateway process from
    # serving the same workspace (two CronSchedulerService instances over one
    # cron_jobs.json => duplicate cron executions).
    from agent_gateway.gateway_lock import GatewayLock

    gateway_lock = GatewayLock(get_user_workspace_dir())
    if not gateway_lock.acquire():
        logger.error(
            "[App] Another Gateway is already serving this workspace; "
            "refusing duplicate instance (lock: %s)",
            gateway_lock.lock_path,
        )
        raise SystemExit(1)
    try:
        asyncio.run(
            _run_with_shutdown_signals(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
            )
        )
    finally:
        gateway_lock.release()


if __name__ == "__main__":
    main()
