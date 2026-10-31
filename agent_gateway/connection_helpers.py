# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure connection identity and route-key helpers for GatewayServer."""

from __future__ import annotations

import inspect
import re
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from agent_gateway.common.security.ws_origin import get_header_value
from agent_gateway.route_config import LocalHandlerContext


# user_id 白名单: 仅允许字母数字及 _-, 拒绝路径遍历字符
_SAFE_USER_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def extract_ws_user_id(ws: Any) -> str | None:
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


def connection_user_id(ws: Any) -> str | None:
    """返回连接建立时缓存的 user_id（来自握手 Header）。"""
    uid = getattr(ws, "_gateway_user_id", None)
    if uid is None:
        return None
    text = str(uid).strip()
    return text or None


def connection_agent_type(ws: Any) -> str:
    """返回当前 WS 连接绑定的 agent_type（缺省 jiuwenswarm）。"""
    raw = getattr(ws, "_gateway_agent_type", None)
    text = str(raw or "jiuwenswarm").strip().lower()
    return text or "jiuwenswarm"


def invoke_local_handler(
    handler: Callable[..., Awaitable[None]],
    ctx: _LocalHandlerContext,
) -> Awaitable[None]:
    kwargs: dict[str, Any] = {}
    if "user_id" in inspect.signature(handler).parameters:
        kwargs["user_id"] = ctx.user_id
    return handler(ctx.ws, ctx.req_id, ctx.params, ctx.session_id, **kwargs)


def client_route_key(
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


def extract_routing_session_id(
    msg: Any,
    *,
    include_top_level: bool = True,
) -> str | None:
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


def ws_is_open(ws: Any) -> bool:
    return ws is not None and not bool(getattr(ws, "closed", False))
