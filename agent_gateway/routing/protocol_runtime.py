# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""TUI/ACP WebSocket protocol runtime, independent of application startup."""

from __future__ import annotations

import asyncio
from collections import deque
from copy import copy
import inspect
import json
import logging
import re
import time
import uuid as uuid_module
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from e2a.message import Message, Mode
from websockets.exceptions import ConnectionClosedError

from agent_gateway.channel_manager.base import BaseWebChannel
from agent_gateway.common.request_method import parse_request_method
from agent_gateway.common.security.ws_origin import extract_handshake_request, get_header_value
from agent_gateway.common.ws_diagnostics import (
    describe_ws_exception,
    describe_ws_peer,
    format_ws_diagnostics,
)
from agent_gateway.routing.agent_request_timeout import (
    AGENT_SERVER_TIMEOUT_CODE,
    AGENT_SERVER_TIMEOUT_ERROR,
    coerce_client_timeout_ms,
    resolve_agent_request_timeout_seconds,
)
from agent_gateway.common.request_identity import apply_routing_metadata, normalize_routing_identity
from agent_gateway.routing.session_sharing import RoutingTarget

if TYPE_CHECKING:
    from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpGatewayBridge
    from agent_gateway.transport.websocket_listener import WebSocketListener

logger = logging.getLogger("gateway")

_SAFE_USER_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
# Keep gateway idle-finalize fallback aligned with ACP channel default.
_PROMPT_IDLE_FINALIZE_SECONDS = 3.0




_TUI_REQUEST_PREFIX = "tui-forward-"
_REQUEST_REFERENCE_FIELDS = ("request_id", "turn_request_id", "source_request_id", "target_request_id")
# Backstop for a TUI session.switch whose reply never reaches the gateway.
# MessageHandler normally answers within its own unary timeout; the margin lets
# that reply win. The fallback applies when the timeout policy sets no limit.
_TUI_SWITCH_DEADLINE_MARGIN_SECONDS = 10.0
_TUI_SWITCH_DEADLINE_FALLBACK_SECONDS = 55.0
# Answered unary TUI requests whose correlation is kept per socket, so follow-up
# requests and late events can still reference recent ones.
_TUI_ANSWERED_ORIGINS_KEPT = 256
# A TUI socket that drops mid-turn normally reconnects within seconds. Its Sessions
# and in-flight request correlations are held this long for the same user's next
# socket. Shorter than MessageHandler's 60 s disconnect cancel, so a re-attach that
# is still allowed always comes before that cancel fires.
_TUI_REATTACH_GRACE_SECONDS = 55.0
_TUI_FOREGROUND_METHODS = frozenset({
    "session.create", "session.switch", "history.get", "chat.send", "chat.resume",
})


def _build_event_frame(msg) -> dict[str, Any]:
    event_name = "chat.final"
    if msg.event_type is not None:
        event_name = msg.event_type.value
    if isinstance(msg.payload, dict):
        payload = {**msg.payload}
        payload.setdefault("session_id", msg.session_id)
    else:
        payload = {"session_id": msg.session_id, "content": str(msg.payload or "")}
    return {"type": "event", "event": event_name, "payload": payload}


@dataclass
class RouteConfig:
    """单条路由的配置（/acp, /cli 等）。"""

    path: str
    channel_id: str
    forward_methods: frozenset[str] = frozenset()
    forward_no_local_handler_methods: frozenset[str] = frozenset()
    local_handlers: dict[str, Callable[..., Awaitable[None]]] = field(default_factory=dict)
    inbound_interceptor: Callable[..., Awaitable[bool]] | None = None
    outbound_interceptor: Callable[..., Awaitable[bool]] | None = None
    cleanup_handler: Callable[..., Any] | None = None
    disconnect_handler: Callable[..., Any] | None = None
    session_bind_handler: Callable[..., Any] | None = None
    # V2: 委托 ws 注册的外部 Channel（如 tui 的 TuiChannel）。
    # GatewayServer 仍作 ws 宿主 + 入站帧解析，但把 ws + RoutingKey 委托注册进该
    # Channel 的五维索引（_clients_by_key / _ws_by_id），出站由 ChannelManager 派发
    # 到该 Channel.send（按 delivery.ws_id 物理寻址）。None 表示该 route 无需委托
    # （如 acp 自带 _session_to_client 反查，web 有独立 WebChannel 不经此路径）。
    ws_channel: Any = None


@dataclass(frozen=True)
class _TuiRequestOrigin:
    ws: Any
    client_id: str
    user_id: str | None
    request_key: tuple
    is_stream: bool = False
    session_id: str = ""
    adopted: bool = False  # Taken over from a dropped socket by a reconnected window.


@dataclass
class _DetachedTuiSession:
    """What a dropped TUI socket held for one Session, kept for the re-attach grace."""

    user_id: str | None
    scope: tuple[str, ...]
    expires_at: float
    origins: dict[str, _TuiRequestOrigin]  # By upstream ID; their ws is cleared.
    routing_keys: list[Any]


@dataclass
class _PendingTuiSwitch:
    route: RouteConfig
    session_id: str
    ws: Any
    request_key: tuple  # (channel, upstream ID[, agent_ref])
    routing_key: Any = None
    deadline: asyncio.TimerHandle | None = None


@dataclass
class ProtocolRuntimeConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 19001
    routes: dict[str, RouteConfig] = field(default_factory=dict)
    path: str | None = None
    channel_id: str | None = None

    def __post_init__(self) -> None:
        if self.routes:
            return
        path = str(self.path or "").strip()
        channel_id = str(self.channel_id or "").strip()
        if path and channel_id:
            self.routes[path] = RouteConfig(path=path, channel_id=channel_id)


@dataclass(frozen=True)
class _LocalHandlerContext:
    ws: Any
    req_id: str
    params: dict[str, Any]
    session_id: str
    user_id: str | None


class ProtocolRuntime(BaseWebChannel):
    """通用多路路由 WebSocket Gateway Server。

    支持多个路径（如 /acp、/cli），每条路径可以有独立的 channel_id 和本地 handler。
    本地 handler 优先处理请求，未处理或无匹配则 forward 到 MessageHandler。
    """

    def __init__(self, config: ProtocolRuntimeConfig, router) -> None:
        from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpGatewayBridge

        super().__init__(config, router)
        self.config = config
        self.bus = router
        self._listener: WebSocketListener | None = None
        self._running = False
        self._on_message_cb = None
        self._clients: set[Any] = set()
        # V2: key 升级为 (channel_id, session_id, agent_ref_str|None)，
        # 兼容旧 2-tuple key（agent_ref 为 None 时回退）。
        self._request_to_client: dict[tuple, Any] = {}
        self._tui_request_origins: dict[str, _TuiRequestOrigin] = {}
        self._session_to_client: dict[tuple, Any] = {}
        # ACP 延迟绑定：session 在握手期先挂起，等首个 agent 请求 promote。
        self._pending_session_clients: dict[tuple[str, str], Any] = {}
        self._pending_tui_switches: dict[tuple, _PendingTuiSwitch] = {}
        self._detached_tui_sessions: dict[str, _DetachedTuiSession] = {}
        # Work kept off the send path: deferred bind hooks and switch-deadline replies.
        self._session_bind_tasks: set[asyncio.Task] = set()
        self.message_handler_ref = None
        self._acp_bridge = AcpGatewayBridge(
            self._dispatch_on_message,
            bind_session_client=self._bind_acp_session_client,
            channel_id="acp",
            idle_finalize_seconds=lambda: _PROMPT_IDLE_FINALIZE_SECONDS,
        )
        self._install_default_route_hooks()
        for route in config.routes.values():
            self._bind_tui_ws_channel_handlers(route)

    def _bind_tui_ws_channel_handlers(self, route: RouteConfig) -> None:
        """Bind TUI callbacks that may be injected before or after construction."""
        if route.channel_id != "tui" or route.ws_channel is None:
            return
        route.ws_channel.session_switch_response_handler = self._complete_tui_session_switch
        route.ws_channel.request_outbound_resolver = self._tui_request_outbound

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
        return [
            client_ws for key, client_ws in self._session_to_client.items()
            if isinstance(key, tuple) and key[0] == channel_id and not getattr(client_ws, "closed", False)
        ]

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
        session_key = self._client_route_key(channel_id, session_id)
        if session_key is None:
            return False
        return self._session_to_client.get(session_key) is ws

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
        active: set[str] = set()
        for key, client_ws in self._session_to_client.items():
            if not isinstance(key, tuple) or len(key) < 2:
                continue
            if key[0] != channel_id:
                continue
            if client_ws is exclude_ws:
                continue
            if bool(getattr(client_ws, "closed", False)):
                continue
            session_id = key[1]
            if isinstance(session_id, str) and session_id:
                active.add(session_id)
        return active

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
        session_key = self._client_route_key("acp", session_id, agent_ref)
        if session_key is not None:
            self._session_to_client[session_key] = ws

    @staticmethod
    def _tui_routing_scope(ws: Any) -> tuple[str, ...]:
        routing = getattr(ws, "_web_routing", None) or {}
        return tuple(str(routing.get(name) or "") for name in ("group_id", "bot_id", "gateway_id"))

    def _detach_tui_socket(self, route: RouteConfig, ws: Any, session_keys: list[tuple]) -> None:
        """Record what a dropped TUI socket held, before its tables are cleared.

        Must not await: the reconnected socket's first request can arrive while
        the rest of this socket's teardown is still running.
        """
        now = time.monotonic()
        for sid in [sid for sid, record in self._detached_tui_sessions.items() if record.expires_at <= now]:
            del self._detached_tui_sessions[sid]
        own = {uid: origin for uid, origin in self._tui_request_origins.items() if origin.ws is ws}
        # Lets unclaimed_tui_disconnect_keys() tell which requests belong to a
        # Session that another socket holds by the time the cancel is scheduled.
        ws._tui_stale_request_sessions = {origin.request_key: origin.session_id for origin in own.values()}
        if getattr(ws, "_jiuwenswarm_tui_user_exit", False):
            return  # The window quit: nothing will come back for its Sessions.
        routing_keys_for = getattr(route.ws_channel, "routing_keys_for", None)
        routing_keys = routing_keys_for(ws) if callable(routing_keys_for) else []
        for key in session_keys:
            sid = key[1]
            self._detached_tui_sessions[sid] = _DetachedTuiSession(
                user_id=self._connection_user_id(ws),
                scope=self._tui_routing_scope(ws),
                expires_at=now + _TUI_REATTACH_GRACE_SECONDS,
                # All of them: a running stream may be older than any number of later
                # requests. Answered unary ones are already capped per socket.
                origins={
                    uid: replace(origin, ws=None) for uid, origin in own.items() if origin.session_id == sid
                },
                routing_keys=[rk for rk in routing_keys if getattr(rk, "session_id", None) == sid],
            )

    def _detached_tui_session_matches(self, record: _DetachedTuiSession, ws: Any) -> bool:
        user_id = self._connection_user_id(ws)
        return (
            record.expires_at > time.monotonic()
            and user_id is not None
            and record.user_id == user_id
            and record.scope == self._tui_routing_scope(ws)
        )

    def _tui_request_reattaches(self, ws: Any, method: str, session_id: str) -> bool:
        """Whether a non-foreground request re-attaches a reconnected window.

        The TUI reconnects on a new socket without announcing its Session; the
        only hint is the session_id it attaches to every request. The hint is
        ignored for session.* requests, where it can name another Session
        (preview, rename, delete), and for a socket that already holds or is
        switching to a Session.
        """
        if method.startswith("session.") or method == "tui.disconnect":
            return False
        record = self._detached_tui_sessions.get(session_id)
        if record is None or not self._detached_tui_session_matches(record, ws):
            return False
        if any(owner is ws for owner in self._session_to_client.values()):
            return False
        if any(pending.ws is ws for pending in self._pending_tui_switches.values()):
            return False
        holder = self._lookup_client(self._session_to_client, "tui", session_id)
        return not self._ws_is_open(holder) and self._pending_tui_switch_owner(session_id) is None

    def _adopt_detached_tui_session(self, session_id: str, ws: Any) -> None:
        """A new binding ends a Session's detached state. The same user's socket in
        the same routing scope (a reconnected window) takes over the dropped
        socket's request correlations, so in-flight output reaches it."""
        record = self._detached_tui_sessions.pop(session_id, None)
        if record is None or not self._detached_tui_session_matches(record, ws):
            return
        for upstream_id, origin in record.origins.items():
            if upstream_id in self._tui_request_origins:
                continue
            self._tui_request_origins[upstream_id] = replace(origin, ws=ws, adopted=True)
            self._request_to_client[origin.request_key] = ws
        ws._tui_adopted_routing_keys = list(record.routing_keys)

    def unclaimed_tui_disconnect_keys(
        self, ws: Any, session_keys: list[tuple], request_keys: list[tuple],
    ) -> tuple[list[tuple], list[tuple]]:
        """A dropped socket's keys that no open socket has taken over since.

        The disconnect handler runs after awaits, so the window may already have
        reconnected; scheduling the disconnect cancel for its Session then would
        end a turn that is being watched again. Requests of a Session another
        window holds are left alone for the same reason.
        """
        def held(session_id: str) -> bool:
            return self._ws_is_open(self._lookup_client(self._session_to_client, "tui", session_id))

        request_sessions = getattr(ws, "_tui_stale_request_sessions", None) or {}
        return (
            [key for key in session_keys if not held(key[1])],
            [
                key for key in request_keys
                if not self._ws_is_open(self._request_to_client.get(key))
                and not held(request_sessions.get(key, ""))
            ],
        )

    async def _register_tui_ws(self, route: RouteConfig, ws: Any, routing_key: Any) -> None:
        try:
            await route.ws_channel.register_ws(ws, routing_key)
        except Exception:
            logger.warning(
                "GatewayServer delegate _register to ws_channel failed: channel_id=%s",
                route.channel_id, exc_info=True,
            )

    async def _bind_request_session(self, route: RouteConfig, session_id: str, ws: Any) -> bool:
        """Bind a Session for an inbound request."""
        ws_channel = route.ws_channel if route.channel_id == "tui" else None
        if ws_channel is not None:
            # The socket needs its writer before the bind can adopt a dropped socket's
            # in-flight output, which may arrive while the bind hook is still running.
            await self._register_tui_ws(route, ws, None)
        bound = await self._bind_route_session_client(route, session_id, ws)
        routing_keys = getattr(ws, "_tui_adopted_routing_keys", None)
        if routing_keys:
            ws._tui_adopted_routing_keys = None
            if bound and ws_channel is not None:
                # A local-only request registers no routing key of its own.
                for routing_key in routing_keys:
                    await self._register_tui_ws(route, ws, routing_key)
        return bound

    async def _bind_route_session_client(
        self,
        route: RouteConfig,
        session_id: str,
        ws: Any,
        *,
        defer_bind_hook: bool = False,
    ) -> bool:
        session_key = self._client_route_key(route.channel_id, session_id)
        if session_key is None:
            return False
        existing_ws = self._session_to_client.get(session_key)
        if route.channel_id == "tui" and self._pending_tui_switch_owner(session_id) is not None:
            return False
        if (
            existing_ws is not None
            and existing_ws is not ws
            and not bool(getattr(existing_ws, "closed", False))
        ):
            if self._ws_is_open(ws):
                self._pending_session_clients[session_key] = ws
            return False
        self._session_to_client[session_key] = ws
        if route.channel_id == "tui" and existing_ws is not ws:
            ws._lifecycle_binding_generation = getattr(ws, "_lifecycle_binding_generation", 0) + 1
            # Before any await, so the first request's references already resolve.
            self._adopt_detached_tui_session(session_id, ws)
        if self._pending_session_clients.get(session_key) is ws:
            self._pending_session_clients.pop(session_key, None)
        if existing_ws is ws or route.session_bind_handler is None:
            return True
        try:
            result = route.session_bind_handler(route.channel_id, session_id)
            if asyncio.iscoroutine(result):
                if defer_bind_hook:
                    # Outbound callers must not stall every connection on the hook's AgentServer calls.
                    task = asyncio.create_task(self._await_session_bind_hook(route.channel_id, session_id, result))
                    self._session_bind_tasks.add(task)
                    task.add_done_callback(self._session_bind_tasks.discard)
                else:
                    await result
        except Exception:
            logger.warning(
                "GatewayServer session bind handler failed: channel_id=%s session_id=%s",
                route.channel_id,
                session_id,
                exc_info=True,
            )
        return True

    @staticmethod
    async def _await_session_bind_hook(channel_id: str, session_id: str, hook: Awaitable[Any]) -> None:
        try:
            await hook
        except Exception:
            logger.warning(
                "GatewayServer session bind handler failed: channel_id=%s session_id=%s",
                channel_id, session_id, exc_info=True,
            )

    def _pending_tui_switch_owner(self, session_id: str) -> Any | None:
        """Return the open socket whose pending ``session.switch`` reserves ``session_id``."""
        return next((
            pending.ws for pending in self._pending_tui_switches.values()
            if pending.session_id == session_id and self._ws_is_open(pending.ws)
        ), None)

    async def _complete_tui_session_switch(self, msg: Any) -> bool:
        if getattr(msg, "type", None) != "res" or getattr(msg, "channel_id", None) != "tui":
            return False
        pending = self._lookup_client(self._pending_tui_switches, "tui", msg.id, getattr(msg, "agent_ref", None))
        if pending is None:
            return False
        # The reply arrived: the deadline must not fire while it is being committed.
        self._cancel_tui_switch_deadline(pending)
        route, session_id, ws = pending.route, pending.session_id, pending.ws
        origin = self._tui_request_origin(msg)
        response_session = getattr(msg, "session_id", None)
        if origin is None or origin.ws is not ws or (response_session and response_session != session_id):
            await self._reject_tui_session_switch(msg, pending)
            return True
        payload = dict(msg.payload or {}) if isinstance(msg.payload, dict) else {}
        accepted = bool(msg.ok) and payload.get("switched") is not False
        registration_failed = False
        try:
            if self._ws_is_open(ws) and accepted and route.ws_channel is not None and pending.routing_key is not None:
                await route.ws_channel.register_ws(ws, pending.routing_key, evict_previous=True)
        except Exception:
            registration_failed = True
            logger.warning(
                "GatewayServer TUI session.switch registration failed: channel_id=%s session_id=%s",
                route.channel_id,
                session_id,
                exc_info=True,
            )
        finally:
            # Keep the target reserved until the subscription commit finishes.
            self._release_pending_tui_switch(pending)
        if not self._ws_is_open(ws):
            return True
        if registration_failed:
            failed_origin = self._settle_failed_tui_switch(pending)
            if failed_origin is not None:
                await self._send_tui_switch_failure(
                    pending,
                    failed_origin,
                    "session switch registration failed",
                    "INTERNAL_ERROR",
                )
            return True
        if accepted:
            # Runs inside the shared outbound dispatcher: defer the hook's
            # metadata lookup rather than stall every connection's output.
            await self._bind_route_session_client(route, session_id, ws, defer_bind_hook=True)
            ws._tui_adopted_routing_keys = None  # The switch registered its own routing key.
        await self._send_tui_reply(route, ws, self._tui_client_message(msg, origin))
        self._note_tui_request_answered(msg.id, origin)
        return True

    @staticmethod
    def _cancel_tui_switch_deadline(pending: _PendingTuiSwitch) -> None:
        if pending.deadline is not None:
            pending.deadline.cancel()
            pending.deadline = None

    def _release_pending_tui_switch(self, pending: _PendingTuiSwitch) -> None:
        self._cancel_tui_switch_deadline(pending)
        if self._pending_tui_switches.get(pending.request_key) is pending:
            del self._pending_tui_switches[pending.request_key]

    def _settle_failed_tui_switch(self, pending: _PendingTuiSwitch) -> _TuiRequestOrigin | None:
        """Release a failed switch's reservation and drop its correlation, so a later
        reply is discarded. Returns the origin if its window is still waiting."""
        self._release_pending_tui_switch(pending)
        self._request_to_client.pop(pending.request_key, None)
        origin = self._tui_request_origins.pop(pending.request_key[1], None)
        if origin is None or origin.ws is not pending.ws or not self._ws_is_open(pending.ws):
            return None
        return origin

    async def _send_tui_switch_failure(
        self, pending: _PendingTuiSwitch, origin: _TuiRequestOrigin, error: str, code: str,
    ) -> None:
        await self._send_tui_reply(pending.route, pending.ws, Message(
            id=origin.client_id, type="res", channel_id="tui", session_id=pending.session_id,
            params={}, timestamp=time.time(), ok=False, payload={"error": error, "code": code},
        ))

    async def _reject_tui_session_switch(self, msg: Any, pending: _PendingTuiSwitch) -> None:
        """Settle a switch whose only reply failed validation: AgentServer answers each
        upstream ID once, so release the reservation and tell the window it failed."""
        origin = self._settle_failed_tui_switch(pending)
        if origin is None:
            return
        logger.warning("GatewayServer rejected TUI session.switch reply: target=%s reply_session=%s",
                       pending.session_id, getattr(msg, "session_id", None))
        await self._send_tui_switch_failure(
            pending, origin, "session switch reply did not match the request", "INTERNAL_ERROR",
        )

    def _expire_tui_session_switch(self, pending: _PendingTuiSwitch) -> None:
        """Deadline callback: no reply reached the gateway. Without this the window
        answers OPERATION_IN_PROGRESS to every later switch and the target stays
        reserved against other windows until the socket disconnects."""
        pending.deadline = None
        if self._pending_tui_switches.get(pending.request_key) is not pending:
            return
        # Settle synchronously so a reply racing the timer is discarded, not delivered twice.
        origin = self._settle_failed_tui_switch(pending)
        if origin is None:
            return
        logger.warning("GatewayServer TUI session.switch got no reply: target=%s", pending.session_id)
        task = asyncio.create_task(self._send_tui_switch_failure(
            pending, origin, AGENT_SERVER_TIMEOUT_ERROR, AGENT_SERVER_TIMEOUT_CODE,
        ))
        self._session_bind_tasks.add(task)
        task.add_done_callback(self._session_bind_tasks.discard)

    async def _send_tui_reply(self, route: RouteConfig, ws: Any, outgoing: Any) -> None:
        """Send a TUI reply in order with the socket's queued outbound frames."""
        enqueue = getattr(route.ws_channel, "enqueue_message", None)
        if callable(enqueue) and enqueue(ws, outgoing):
            return
        payload = outgoing.payload if isinstance(outgoing.payload, dict) else {}
        await self.send_response(
            ws, outgoing.id, ok=bool(outgoing.ok), payload=outgoing.payload,
            error=payload.get("error"), code=payload.get("code"),
        )

    def _tui_request_origin(self, msg: Any) -> _TuiRequestOrigin | None:
        origin = self._tui_request_origins.get(msg.id)
        if origin is None:
            return None
        owner = self._lookup_client(self._request_to_client, "tui", msg.id, getattr(msg, "agent_ref", None))
        if owner is not origin.ws or self._connection_user_id(owner) != origin.user_id:
            return None
        response_user = getattr(msg, "user_id", None)
        if response_user and str(response_user) != str(origin.user_id or ""):
            return None
        return origin

    def _tui_client_message(self, msg: Any, origin: _TuiRequestOrigin) -> Any:
        outgoing = copy(msg)
        outgoing.id = origin.client_id
        if isinstance(getattr(msg, "payload", None), dict):
            outgoing.payload = dict(msg.payload)
            for field in _REQUEST_REFERENCE_FIELDS:
                value = outgoing.payload.get(field)
                referenced = self._tui_request_origins.get(value) if isinstance(value, str) else None
                if referenced is not None and referenced.ws is origin.ws:
                    outgoing.payload[field] = referenced.client_id
        return outgoing

    def _note_tui_request_answered(self, upstream_id: str, origin: _TuiRequestOrigin) -> None:
        """Bound the correlations kept for a socket's answered unary requests.

        Without this every forwarded request stays in ``_tui_request_origins`` and
        ``_request_to_client`` until disconnect, and the TUI's periodic requests
        make both grow for as long as the window is open. Stream origins are kept:
        the gateway cannot see where a stream ends, and there is one per turn.
        """
        if origin.is_stream:
            return
        answered = getattr(origin.ws, "_tui_answered_requests", None)
        if answered is None:
            answered = origin.ws._tui_answered_requests = deque()
        answered.append(upstream_id)
        while len(answered) > _TUI_ANSWERED_ORIGINS_KEPT:
            evicted = self._tui_request_origins.pop(answered.popleft(), None)
            if evicted is not None:
                self._request_to_client.pop(evicted.request_key, None)

    def _tui_request_outbound(self, msg: Any) -> tuple[Any, Any] | None:
        origin = self._tui_request_origin(msg)
        if origin is not None:
            is_reply = getattr(msg, "type", None) == "res"
            if origin.adopted and is_reply:
                # The reply answers the dropped socket's request table. Whoever
                # holds the Session now may be another process with its own IDs.
                return None, None
            outgoing = self._tui_client_message(msg, origin)
            if is_reply:
                self._note_tui_request_answered(msg.id, origin)
            return origin.ws, outgoing
        if str(msg.id or "").startswith(_TUI_REQUEST_PREFIX):
            # Expired or mismatched correlations must never fall back to a
            # Session now owned by another window.
            return None, None
        return None

    async def _promote_pending_session_client(
        self,
        route: RouteConfig,
        session_key: tuple[str, str],
    ) -> None:
        pending_ws = self._pending_session_clients.pop(session_key, None)
        if not self._ws_is_open(pending_ws):
            return
        existing_ws = self._session_to_client.get(session_key)
        if (
            existing_ws is not None
            and existing_ws is not pending_ws
            and self._ws_is_open(existing_ws)
        ):
            return
        if route.channel_id == "tui" and existing_ws is not pending_ws:  # As in _bind_route_session_client.
            pending_ws._lifecycle_binding_generation = getattr(pending_ws, "_lifecycle_binding_generation", 0) + 1
        self._session_to_client[session_key] = pending_ws
        if route.session_bind_handler is None:
            return
        try:
            result = route.session_bind_handler(route.channel_id, session_key[1])
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.warning(
                "GatewayServer pending session bind handler failed: channel_id=%s session_id=%s",
                route.channel_id,
                session_key[1],
                exc_info=True,
            )

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
        if route is not None:
            self._bind_tui_ws_channel_handlers(route)
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
        if self._listener is None:
            return
        await self._listener.wait_closed()

    def register_local_handler(self, path: str, method: str, handler: Callable[..., Awaitable[None]]) -> None:
        """为指定路径注册本地方法 handler。"""
        route = self.config.routes.get(path)
        if route is None:
            route = RouteConfig(path=path, channel_id=path.strip("/"))
            self.config.routes[path] = route
            self._bind_tui_ws_channel_handlers(route)
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
        from agent_gateway.transport.websocket_listener import WebSocketListener

        for route in self.config.routes.values():
            self._bind_tui_ws_channel_handlers(route)

        listener = WebSocketListener(
            self._connection_handler,
            host=self.config.host,
            port=self.config.port,
            process_request=self._process_request,
        )
        self._listener = listener
        await listener.start()
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
        self._clients.clear()
        self._request_to_client.clear()
        self._tui_request_origins.clear()
        self._session_to_client.clear()
        self._pending_session_clients.clear()
        for pending_switch in list(self._pending_tui_switches.values()):
            self._release_pending_tui_switch(pending_switch)
        for task in self._session_bind_tasks:
            task.cancel()
        await asyncio.gather(*self._session_bind_tasks, return_exceptions=True)
        if self._listener is not None:
            await self._listener.stop()
        # Connection handlers finish their teardown while the server closes.
        self._detached_tui_sessions.clear()
        logger.info("[App] Gateway server stopped")

    async def send(self, msg, *, routing_target: RoutingTarget | None = None) -> None:
        if await self._complete_tui_session_switch(msg):
            return
        # V2: 提取 agent_ref，优先 3 元组查找，2↔3 双向兜底（_lookup_client）。
        _agent_ref = getattr(msg, "agent_ref", None) or None
        _send_cid = getattr(msg, "channel_id", None)
        _send_sid = getattr(msg, "session_id", None)
        _send_id = getattr(msg, "id", None)
        ws: Any = None
        if _send_cid == "tui" and (routing_target is None or getattr(msg, "type", None) == "res"):
            resolved = self._tui_request_outbound(msg)
            if resolved is not None:
                ws, msg = resolved
                if not self._ws_is_open(ws):
                    return
        # 三级级联 fallback 查找投递目标 ws：任一级命中可用（非 None 且未关闭）ws 即短路，
        # 不再执行后续更低优先级的查找。三级依次为：
        #   1) routing_target（team fan_out 投递，经 dispatch_to_session 调用）
        #   2) request_id 精确回响应（常规 res/event 回到发起请求的 ws）
        #   3) session_id 兜底（同 session 任意活跃 ws，或无 session_id 时广播）

        # ── 第 1 级：fan_out 投递（team 模式经 dispatch_to_session 调用）──
        # GatewayServer 不像 BaseWsChannel 维护 ws_id 物理索引，这里按
        # routing_target.routing_keys 的 session_id 维度查 _session_to_client，
        # 命中任一仍活跃的 ws 即投递。保证 TUI/ACP 误入 team 路径时 GodView/
        # mention fan_out 不会因签名不符而静默丢弃。
        if routing_target is not None and ws is None:
            _rt_keys = getattr(routing_target, "routing_keys", None) or []
            for _rk in _rt_keys:
                _rk_sid = getattr(_rk, "session_id", None)
                if not _rk_sid:
                    continue
                _cand = self._lookup_client(
                    self._session_to_client,
                    getattr(_rk, "channel_id", None) or _send_cid,
                    _rk_sid,
                    getattr(_rk, "agent_ref", None),
                )
                if _cand is not None and not bool(getattr(_cand, "closed", False)):
                    ws = _cand
                    break
        # ── 第 2 级：request_id 精确回响应（回到发起该请求的 ws）──
        if ws is None:
            ws = self._lookup_client(
                self._request_to_client,
                _send_cid,
                _send_id,
                _agent_ref,
            )
        # ── 第 3 级：session_id 兜底（同 session 任意活跃 ws）──
        # 前级返回 None 或已关闭的 ws 时，仍尝试按 session 路由。
        if ws is None or bool(getattr(ws, "closed", False)):
            ws = self._lookup_client(
                self._session_to_client,
                _send_cid,
                _send_sid,
                _agent_ref,
            )
        if ws is None or bool(getattr(ws, "closed", False)):
            if ws is None:
                channel_id = getattr(msg, "channel_id", None)
                # 多 TUI 窗口：有 session_id 时精确路由；无 session_id 时广播（如 cron 推送到 TUI）。
                if channel_id and channel_id != "acp":
                    if msg.type == "res":
                        payload_data = dict(msg.payload or {}) if isinstance(msg.payload, dict) else {}
                        frame = {"type": "res", "id": msg.id, "ok": bool(msg.ok), "payload": payload_data}
                    else:
                        frame = _build_event_frame(msg)

                    session_id = self._extract_routing_session_id(msg, include_top_level=False)
                    if session_id:
                        session_key = self._client_route_key(channel_id, session_id)
                        if session_key:
                            client = self._session_to_client.get(session_key)
                            if client is not None and not bool(getattr(client, "closed", False)):
                                data = json.dumps(frame, ensure_ascii=False)
                                try:
                                    await client.send(data)
                                except Exception:
                                    logger.debug(
                                        "[GatewayServer] session-routed send failed: session_id=%s",
                                        session_id,
                                        exc_info=True,
                                    )
                                return
                    elif not self._extract_routing_session_id(msg, include_top_level=True):
                        clients = self._find_channel_clients(channel_id)
                        if clients:
                            data = json.dumps(frame, ensure_ascii=False)
                            logger.info(
                                "[GatewayServer] broadcast fallback (no session_id): "
                                "channel_id=%s clients=%d id=%s type=%s",
                                channel_id, len(clients), getattr(msg, "id", None), msg.type,
                            )
                            await asyncio.gather(
                                *[c.send(data) for c in clients],
                                return_exceptions=True,
                            )
                            return
                # delivery target 没注册上：附 sess_table_keys 便于反查为何反查不到 ws
                logger.warning(
                    "[GatewayServer] message dropped: no WebSocket client found for"
                    " channel_id=%s session_id=%s id=%s sess_table_keys=%s",
                    getattr(msg, "channel_id", None),
                    getattr(msg, "session_id", None),
                    getattr(msg, "id", None),
                    [str(k) for k in self._session_to_client.keys()],
                )
            else:
                logger.warning(
                    "[GatewayServer] ws already closed, drop: channel_id=%s id=%s session_id=%s ws=%s",
                    _send_cid, _send_id, _send_sid, hex(id(ws)),
                )
            return

        if getattr(msg, "channel_id", None) == "acp":
            handled = await self._acp_bridge.send_message(msg, ws)
            if handled:
                return

        # 让 route 的 outbound_interceptor 有机会拦截
        for route in self.config.routes.values():
            if route.channel_id == msg.channel_id and route.outbound_interceptor is not None:
                try:
                    handled = route.outbound_interceptor(msg, ws)
                    if asyncio.iscoroutine(handled):
                        handled = await handled
                    if handled:
                        return
                except Exception:
                    logger.warning(
                        "GatewayServer outbound interceptor failed: channel_id=%s",
                        msg.channel_id,
                        exc_info=True,
                    )
                break

        if msg.type == "res":
            payload = dict(msg.payload or {}) if isinstance(msg.payload, dict) else {}
            frame: dict[str, Any] = {
                "type": "res",
                "id": msg.id,
                "ok": bool(msg.ok),
                "payload": payload,
            }
            if not msg.ok:
                frame["error"] = str(payload.get("error") or "request failed")
                # 提升 payload.code 为顶层 code(与本地 handler 的 send_response
                # 错误帧结构一致,设计文档 §1.3 前端按顶层 code 分流)
                code_val = payload.get("code")
                if isinstance(code_val, str) and code_val.strip():
                    frame["code"] = code_val.strip()
            await ws.send(json.dumps(frame, ensure_ascii=False))
            return

        frame = _build_event_frame(msg)
        await ws.send(json.dumps(frame, ensure_ascii=False))

    async def _teardown_connection(
        self,
        route: RouteConfig,
        ws: Any,
        request_path: str,
        *,
        normal_close: bool,
    ) -> None:
        if normal_close:
            logger.info(
                "[App] WebSocket connection closed (normal): channel=%s",
                route.channel_id,
            )
        self._clients.discard(ws)
        stale_request_keys = [
            key for key, client in self._request_to_client.items() if client is ws
        ]
        stale_session_keys = [
            key for key, client in self._session_to_client.items() if client is ws
        ]
        if route.channel_id == "tui":
            self._detach_tui_socket(route, ws, stale_session_keys)
        for request_key in stale_request_keys:
            self._request_to_client.pop(request_key, None)
            self._tui_request_origins.pop(request_key[1], None)
            pending_switch = self._pending_tui_switches.get(request_key)
            if pending_switch is not None:
                self._release_pending_tui_switch(pending_switch)
        for session_key in stale_session_keys:
            self._session_to_client.pop(session_key, None)
        stale_pending_session_keys = [
            key for key, client in self._pending_session_clients.items() if client is ws
        ]
        for session_key in stale_pending_session_keys:
            self._pending_session_clients.pop(session_key, None)
        # Delegate dead-ws removal to the channel. Keep this after
        # _session_to_client cleanup and before disconnect_handler so later
        # deliveries cannot select the dead socket.
        if route.ws_channel is not None:
            try:
                await route.ws_channel.unregister_ws(ws)
            except Exception:
                logger.warning(
                    "GatewayServer delegate unregister_ws to ws_channel failed: path=%s",
                    request_path,
                    exc_info=True,
                )
            reporter = getattr(route.ws_channel, "report_disconnect", None)
            if callable(reporter):
                reporter(ws)
        if route.disconnect_handler is not None:
            try:
                # Pass stale request keys so the handler can recover session IDs
                # from in-flight stream bookkeeping even after a reconnect
                # overwrote _session_to_client for the same session.
                result = route.disconnect_handler(
                    ws,
                    stale_session_keys,
                    stale_request_keys,
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
            except Exception as e:
                logger.warning(
                    "%s on_disconnect hook error: %s",
                    type(self).__name__,
                    e,
                )

    async def _connection_handler(self, ws: Any, path: str | None = None) -> None:
        raw_path = path if path is not None else getattr(ws, "path", "")
        parsed = urlparse(raw_path)
        request_path = parsed.path or raw_path
        remote = getattr(ws, "remote_address", None)

        route, matched_path = self._resolve_route(request_path)
        if route is None:
            await ws.close(code=1008, reason=f"unsupported path: {request_path}")
            return

        self._clients.add(ws)

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

        try:
            # 上报连接事件
            if route.ws_channel is not None:
                reporter = getattr(route.ws_channel, "report_connect", None)
                if callable(reporter):
                    reporter(ws)

            if route.channel_id == "tui":
                # Routing scope: the authenticated user, then group/bot/gateway (headers over URL query).
                headers = getattr(getattr(ws, "request", None), "headers", None) or getattr(ws, "request_headers", None)
                query, fields = parse_qs(parsed.query), ("group_id", "bot_id", "gateway_id")
                ws._web_routing = normalize_routing_identity(
                    {"user_id": self._connection_user_id(ws)},
                    {field: get_header_value(headers, "X-" + field.replace("_", "-").title()) for field in fields},
                    {field: (query.get(field) or [""])[0] for field in fields},
                )
        except Exception:
            logger.warning(
                "GatewayServer pre-ack connection setup failed: channel_id=%s path=%s",
                route.channel_id,
                request_path,
                exc_info=True,
            )
            await self._teardown_connection(route, ws, request_path, normal_close=False)
            raise

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
            await self._teardown_connection(route, ws, request_path, normal_close=False)
            return

        normal_close = False
        try:
            async for raw in ws:
                await self._handle_raw_message(ws, raw, matched_path, route)
            normal_close = True
        except ConnectionClosedError:
            logger.info("[App] WebSocket connection closed: channel=%s", route.channel_id)
        finally:
            await self._teardown_connection(route, ws, request_path, normal_close=normal_close)

    async def _handle_raw_message(self, ws: Any, raw: str, request_path: str, route: RouteConfig) -> None:

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(
                json.dumps(
                    {"type": "res", "id": "", "ok": False, "error": "invalid json"},
                    ensure_ascii=False,
                )
            )
            return

        if route.channel_id == "acp":
            handled = await self._acp_bridge.handle_jsonrpc_request(ws, data)
            if handled:
                return

        if route.channel_id == "acp" and self._acp_bridge.is_jsonrpc_request(data):
            return

        # route 级别的原始帧拦截（如 ACP JSON-RPC response）
        if route.inbound_interceptor is not None:
            try:
                handled = route.inbound_interceptor(ws, data)
                if asyncio.iscoroutine(handled):
                    handled = await handled
                if handled:
                    return
            except Exception:
                logger.warning(
                    "GatewayServer inbound interceptor failed: path=%s",
                    request_path,
                    exc_info=True,
                )

        if not isinstance(data, dict) or data.get("type") != "req":
            await ws.send(
                json.dumps(
                    {"type": "res", "id": "", "ok": False, "error": "invalid request"},
                    ensure_ascii=False,
                )
            )
            return

        req_id = str(data.get("id") or "").strip()
        method = str(data.get("method") or "").strip()
        params = data.get("params") if isinstance(data.get("params"), dict) else {}
        if not req_id or not method:
            await ws.send(
                json.dumps(
                    {"type": "res", "id": req_id, "ok": False, "error": "invalid request"},
                    ensure_ascii=False,
                )
            )
            return

        explicit_session_id = bool(str(params.get("session_id") or "").strip())
        session_id = (str(params.get("session_id") or "").strip()) or req_id
        session_key = self._client_route_key(route.channel_id, session_id) if explicit_session_id else None

        req_user_id = self._connection_user_id(ws)
        # Only foreground operations select the TUI window's Session. A target
        # ID on preview, rename, or lifecycle requests is not a selection. The one
        # exception is a reconnected window taking back the Session it dropped.
        binds_session = route.channel_id != "tui" or method in _TUI_FOREGROUND_METHODS or (
            explicit_session_id and self._tui_request_reattaches(ws, method, session_id)
        )

        # 1. forward 优先：方法在 forward_methods 中则转发到 MessageHandler
        if method in route.forward_methods:
            req_method = parse_request_method(method)

            if req_method is None:
                await ws.send(
                    json.dumps(
                        {"type": "res", "id": req_id, "ok": False, "error": f"unknown method: {method}"},
                        ensure_ascii=False,
                    )
                )
                return

            # V2: 提取 agent_ref 用于路由键（支撑场景 2 同 session 切 mode 不串窗）。
            # 客户端未发送 agent_ref 时不合成——保持 2 元组注册，与响应查找侧
            # （AgentServer 普通模式不回带 agent_ref → msg.agent_ref=None → 2 元组）
            # 对称，避免 3/2 元组不匹配导致响应消息被丢弃（设计 §1.1 非 team 零改动）。
            _agent_ref = params.get("agent_ref")

            upstream_id = _TUI_REQUEST_PREFIX + uuid_module.uuid4().hex if route.channel_id == "tui" else req_id
            request_key = self._client_route_key(route.channel_id, upstream_id, _agent_ref)
            # 升级 session_key 为 3 元组（含 agent_ref），与 _session_to_client 键一致。
            session_key = (
                self._client_route_key(route.channel_id, session_id, _agent_ref)
                if explicit_session_id else None
            )
            # 检测 session 是否已在其他窗口打开：若已绑定到另一个仍活跃的连接，
            # 拒绝当前请求，避免多窗口 session 冲突（事件路由串台、binding 被覆盖）。
            # 前端 session.list 的 active_in_window 标记是"事后快照"，此处是实时防线。
            if session_key is not None:
                existing_ws = self._session_to_client.get(session_key)
                if existing_ws is None and route.channel_id == "tui":
                    existing_ws = self._pending_tui_switch_owner(session_id)
                if (
                    existing_ws is not None
                    and existing_ws is not ws
                    and not bool(getattr(existing_ws, "closed", False))
                ):
                    logger.warning(
                        "GatewayServer reject %s: session %s already active in another %s connection",
                        method, session_id, route.channel_id,
                    )
                    await self.send_response(
                        ws, req_id, ok=False,
                        error=f"Session {session_id} is already active in another window. Close that window first.",
                        code="SESSION_IN_USE",
                    )
                    return
            deferred_switch = route.channel_id == "tui" and method == "session.switch" and session_key is not None
            if deferred_switch and any(pending.ws is ws for pending in self._pending_tui_switches.values()):
                await self.send_response(
                    ws, req_id, ok=False, error="Session switch already pending", code="OPERATION_IN_PROGRESS",
                )
                return
            if request_key is not None:
                self._request_to_client[request_key] = ws
            if route.channel_id == "tui":
                self._tui_request_origins[upstream_id] = _TuiRequestOrigin(
                    ws, req_id, req_user_id, request_key=request_key,
                    is_stream=bool(data.get("is_stream", False)),
                    session_id=session_id if explicit_session_id else "",
                )
            if deferred_switch:
                pending_switch = _PendingTuiSwitch(route, session_id, ws, request_key)
                wait_seconds = resolve_agent_request_timeout_seconds(
                    channel_id=route.channel_id, method=method, is_stream=False,
                    client_timeout_ms=data.get("timeout_ms"),
                ) or _TUI_SWITCH_DEADLINE_FALLBACK_SECONDS
                pending_switch.deadline = asyncio.get_running_loop().call_later(
                    wait_seconds + _TUI_SWITCH_DEADLINE_MARGIN_SECONDS,
                    self._expire_tui_session_switch, pending_switch,
                )
                self._pending_tui_switches[request_key] = pending_switch
            elif session_key is not None and binds_session:
                await self._bind_request_session(route, session_id, ws)

            default_mode = Mode.CODE_NORMAL if route.channel_id == "tui" else Mode.AGENT
            mode = Mode.from_raw(params.get("mode"), default=default_mode)

            # 确保 mode 被设置到 params 中，以便后续转发到 AgentServer。
            # Issue #4876: params.mode 传非字符串（数组/对象等）时不能原样透传，
            # 用 from_raw 的解析结果（含默认值兜底）覆盖；字符串值保持原样不动。
            params = dict(params)
            if route.channel_id == "tui" and any(field in params for field in _REQUEST_REFERENCE_FIELDS):
                references = {
                    o.client_id: rid for rid, o in self._tui_request_origins.items() if o.ws is ws and rid != upstream_id
                }
                for field in _REQUEST_REFERENCE_FIELDS:
                    if isinstance(params.get(field), str) and params[field] in references:
                        params[field] = references[params[field]]
            if not isinstance(params.get("mode"), str):
                params["mode"] = mode.value

            # 连接级 agent_type 为 SSOT，注入后续转发请求
            # （3rdagent.switch 由 TUI local_handler 写回 ws._gateway_agent_type）
            params["agent_type"] = self._connection_agent_type(ws)

            # V2: agent_ref 全链路透传（阶段2）。
            # tui 客户端从不发 agent_ref（_agent_ref 恒 None）→ 按 mode/agent_id 合成 AgentRef，
            # 用于：(1) 委托 _register 的 RoutingKey.agent_ref；(2) msg.agent_ref 经 E2A 透传到
            # AgentServer，chunk 回带同 agent_ref；(3) _maybe_register_godview 用 msg.agent_ref 构造
            # GodView RoutingKey.agent_ref（与入站注册同源，routing_keys 兜底能命中）。
            # acp 等非 ws_channel route 不合成（保留原 _agent_ref，走各自路径）。
            from agent_gateway.routing.keys import AgentRef as _AgentRef

            _resolved_agent_ref = _agent_ref
            if route.ws_channel is not None and not isinstance(_resolved_agent_ref, _AgentRef):
                _agent_id_raw = str(params.get("agent_id") or "default").strip() or "default"
                _resolved_agent_ref = _AgentRef(mode=mode.value, id=_agent_id_raw)

            # V2: 委托 ws 注册进 route.ws_channel（tui 的 TuiChannel）的五维索引。
            # GatewayServer 仍保留 _session_to_client/_request_to_client 用于入站响应反查
            # （send_response / local handler 回包），但出站 chunk 的精确路由改由
            # TuiChannel 的 _ws_by_id（物理寻址）/ _clients_by_key（五维逻辑）承担。
            # 同步把 ws_id 注入 metadata，供 _maybe_register_godview 构造带真 ws_id 的
            # TuiDeliveryTarget（修掉原 tui _kind="group" + ws_id 恒空导致投递不到）。
            _ws_id_for_metadata = ""
            if route.ws_channel is not None:
                from agent_gateway.routing.keys import RoutingKey

                # user_id 取 "tui"：与 _maybe_register_godview 的 _user 兜底（... or _ch）
                # 同源，保证 GodView 订阅的 RoutingKey.user_id 与此处入站注册一致。
                _tui_rk = RoutingKey(
                    user_id="tui",
                    channel_id=route.channel_id,
                    app_id="default",
                    agent_ref=_resolved_agent_ref,
                    session_id=session_id,
                )
                if deferred_switch:
                    self._pending_tui_switches[request_key].routing_key = _tui_rk
                try:
                    await route.ws_channel.register_ws(
                        ws, None if deferred_switch or not binds_session else _tui_rk,
                        evict_previous=route.channel_id == "tui" and binds_session,
                    )
                    _ws_id_for_metadata = getattr(ws, "_jiuwen_ws_id", "") or ""
                except Exception:
                    logger.warning(
                        "GatewayServer delegate _register to ws_channel failed: channel_id=%s session_id=%s",
                        route.channel_id, session_id, exc_info=True,
                    )

            # 从 params 中提取 cwd/project_dir，注入到 metadata 中
            # cwd 供 message_handler 解析 @file 引用；project_dir 供 session.list 按项目过滤
            metadata = apply_routing_metadata({"method": method}, getattr(ws, "_web_routing", None))
            if _ws_id_for_metadata:
                metadata["ws_id"] = _ws_id_for_metadata
            cwd = params.get("cwd")
            if cwd and isinstance(cwd, str) and cwd.strip():
                metadata["cwd"] = cwd.strip()
            project_dir = params.get("project_dir")
            if project_dir and isinstance(project_dir, str) and project_dir.strip():
                metadata["project_dir"] = project_dir.strip()
                # 记录会话首条消息时所在的 git 分支，供 /resume 按分支过滤（Ctrl+B）。
                # 非 git/detached/失败时为哨兵 "HEAD"。
                from agent_gateway.common.paths import resolve_git_branch

                metadata["git_branch"] = resolve_git_branch(project_dir.strip())
            client_timeout_ms = coerce_client_timeout_ms(data.get("timeout_ms"))
            if route.channel_id == "tui" and client_timeout_ms is not None:
                metadata["client_timeout_ms"] = client_timeout_ms

            is_stream = bool(data.get("is_stream", False))

            msg = Message(
                id=upstream_id,
                type="req",
                channel_id=route.channel_id,
                session_id=session_id,
                params=params,
                timestamp=time.time(),
                ok=True,
                req_method=req_method,
                mode=mode,
                metadata=metadata,
                is_stream=is_stream,
                # V2: 透传 agent_ref 到请求 Message，经 E2A 线协议传到 AgentServer，
                # 响应侧 chunk/response 回带，gateway 查找侧 3 元组匹配（设计 §6.3）。
                # 阶段2：tui ws_channel route 用合成的 _resolved_agent_ref（与 _register /
                # GodView 同源）；非 ws_channel route 保留原 _agent_ref。
                agent_ref=_resolved_agent_ref,
                user_id=req_user_id,
            )

            if self._on_message_cb is not None:
                try:
                    result = self._on_message_cb(msg)
                    if asyncio.iscoroutine(result):
                        await result
                except BaseException:
                    if deferred_switch:
                        self._release_pending_tui_switch(pending_switch)
                    raise

            # ACP route may receive legacy ``type=req`` frames from some clients.
            # They should be forwarded upstream without falling through to the
            # generic "unknown method" error path.
            if route.channel_id == "acp":
                return

            # 如果在 forward_no_local_handler_methods 中，不需要本地 ack
            if method in route.forward_no_local_handler_methods:
                return

        # 2. 本地 handler：发 ack 或纯本地处理
        local_handler = route.local_handlers.get(method)
        if local_handler is not None:
            if session_key is not None and binds_session:
                await self._bind_request_session(route, session_id, ws)
            try:
                await self._invoke_local_handler(
                    local_handler,
                    _LocalHandlerContext(
                        ws=ws,
                        req_id=req_id,
                        params=params,
                        session_id=session_id,
                        user_id=req_user_id,
                    ),
                )
            except Exception as e:
                ws_closed = bool(getattr(ws, "closed", False))
                if ws_closed:
                    logger.warning("GatewayServer local handler aborted on closed ws (%s): %s", method, e)
                    return
                logger.error("GatewayServer local handler error (%s): %s", method, e)
                try:
                    await self.send_response(
                        ws, req_id, ok=False,
                        error=f"handler error: {e}", code="INTERNAL_ERROR",
                    )
                except Exception:
                    logger.debug(
                        "GatewayServer failed to send handler error response: method=%s id=%s",
                        method,
                        req_id,
                        exc_info=True,
                    )
            return

        # 3. 无 forward 也无本地 handler
        await ws.send(
            json.dumps(
                {"type": "res", "id": req_id, "ok": False, "error": f"unknown method: {method}"},
                ensure_ascii=False,
            )
        )
