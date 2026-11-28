# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway proxies for the Designer execution-graph RPCs."""

from __future__ import annotations

import json
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.application.personal_settings import allows_settings_origin
from agent_gateway.common.ws_limits import AGENT_WS_MAX_MESSAGE_BYTES
from agent_gateway.routing.e2a_proxy import proxy_unary_request

#: AgentServer capability behind the personal Web profile: it answers a marked
#: Designer request with canvas ownership and refuses a write based on a canvas
#: version that is no longer stored.
PERSONAL_DESIGNER_CAPABILITY = "personal_designer_v1"
_PERSONAL_DESIGNER_MARKER = "_personal_designer_v1"
_BASE_VERSION_PARAM = "base_updated_at"
#: Identity is the connection's alone, and only the Gateway may say it vouched
#: for a request: both are dropped when a caller supplies them.
_CALLER_RESERVED_PARAMS = ("user_id", _PERSONAL_DESIGNER_MARKER)
_ID_FIELDS = ("project_id", "session_id", "graph_id", "run_id", "node_id", "create_token")
#: A project, session, canvas or run id is a directory or file name on the
#: AgentServer (a session, a canvas, ``runs/<run_id>.json``). Its own ID
#: checks refuse anything that could step out of that directory; the same is
#: refused here, first.
_STORAGE_ID_FIELDS = frozenset({"project_id", "session_id", "graph_id", "run_id"})
_STORAGE_ID_PATH_CHARS = frozenset('/\\:\x00<>"|?*')
_MAX_ID_LENGTH = 256
#: The AgentServer link refuses a frame past ``AGENT_WS_MAX_MESSAGE_BYTES``. A
#: request that cannot cross it is answered here instead of reaching the
#: shared link; the envelope around the params is far smaller than the headroom.
_MAX_PARAMS_BYTES = AGENT_WS_MAX_MESSAGE_BYTES - 64 * 1024

_DESIGNER_METHODS: tuple[tuple[ReqMethod, float | None], ...] = (
    (ReqMethod.DESIGNER_WORKSPACE_CREATE, None),
    (ReqMethod.DESIGNER_WORKSPACE_GET, None),
    (ReqMethod.DESIGNER_WORKSPACE_SESSION_CREATE, None),
    (ReqMethod.DESIGNER_WORKSPACE_SESSION_COMPOSE, None),
    (ReqMethod.DESIGNER_GRAPH_GET, None),
    (ReqMethod.DESIGNER_GRAPH_LIST, None),
    (ReqMethod.DESIGNER_GRAPH_SAVE, None),
    (ReqMethod.DESIGNER_GRAPH_PATCH, None),
    (ReqMethod.DESIGNER_MEDIA_CAPABILITIES, None),
    (ReqMethod.DESIGNER_RUN_START, None),
    (ReqMethod.DESIGNER_RUN_GET, None),
    (ReqMethod.DESIGNER_RUN_PAUSE, None),
    (ReqMethod.DESIGNER_RUN_CANCEL, None),
)


PERSONAL_DESIGNER_METHODS = frozenset(
    method.value for method, _ in _DESIGNER_METHODS if method != ReqMethod.DESIGNER_RUN_PAUSE
)


class DesignerSubscriptions:
    """Deliver canvas events to sockets after the AS verifies their access."""

    def __init__(self, channel):
        self.channel = channel
        self.by_session: dict[str, dict[int, Any]] = {}
        self.by_socket: dict[int, dict[str, None]] = {}

    async def disconnect(self, ws, _sessions):
        key = id(ws)
        for sid in self.by_socket.pop(key, {}):
            self._remove(key, sid)

    def _remove(self, key: int, sid: str) -> None:
        sockets = self.by_session[sid]
        sockets.pop(key, None)
        if not sockets:
            self.by_session.pop(sid)

    def add(self, ws, sid: str) -> None:
        if getattr(ws, "closed", False):
            return
        key = id(ws)
        sessions = self.by_socket.setdefault(key, {})
        sessions.pop(sid, None)
        sessions[sid] = None
        self.by_session.setdefault(sid, {})[key] = ws
        if len(sessions) > 32:
            expired = next(iter(sessions))
            sessions.pop(expired)
            self._remove(key, expired)

    async def send(self, sid: str, event: str, payload: dict[str, Any]) -> None:
        for ws in tuple(self.by_session.get(sid, {}).values()):
            if not getattr(ws, "closed", False):
                await self.channel.send_event(ws, event, {**payload, "session_id": sid})


def register_designer_handlers(channel, resolve_client):
    def proxy(req_method: ReqMethod, timeout_seconds: float | None):
        async def handler(ws, req_id, params, session_id, user_id=None):
            await proxy_unary_request(
                channel=channel,
                agent_client=resolve_client(),
                ws=ws,
                req_id=req_id,
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=req_method,
                label=req_method.value,
                timeout_seconds=timeout_seconds,
            )

        return handler

    for req_method, timeout_seconds in _DESIGNER_METHODS:
        channel.register_method(req_method.value, proxy(req_method, timeout_seconds))


def validate_personal_designer_params(params: Any) -> dict[str, Any]:
    """Return the params the personal Web profile forwards for a Designer request.

    The canvas body itself is left to the AgentServer, which validates the
    graph. Identifier and version fields are checked here so a malformed one
    never reaches it; a project, session, canvas or run id may not contain a
    path separator or a relative segment, because each names a file there.

    Raises:
        ValueError: The caller supplied a reserved field, a malformed
            identifier or version, or a body the AgentServer link cannot
            carry. The message never repeats the caller's value.
    """
    if not isinstance(params, dict):
        raise ValueError("params must be an object")
    forwarded = {
        key: value for key, value in params.items() if key not in _CALLER_RESERVED_PARAMS
    }
    for field in _ID_FIELDS:
        value = forwarded.get(field)
        if value is None:
            continue
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > _MAX_ID_LENGTH
            or not value.isprintable()
        ):
            raise ValueError(f"{field} must be a bounded identifier")
        if field in _STORAGE_ID_FIELDS and (
            value in (".", "..") or ".." in value or any(char in _STORAGE_ID_PATH_CHARS for char in value)
        ):
            raise ValueError(f"{field} must be a single storage component")
    version = forwarded.get(_BASE_VERSION_PARAM)
    if version is not None and (
        isinstance(version, bool) or not isinstance(version, int) or version < 0
    ):
        raise ValueError(f"{_BASE_VERSION_PARAM} must be a non-negative integer")
    try:
        size = len(json.dumps(forwarded, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("params must be a JSON object") from exc
    if size > _MAX_PARAMS_BYTES:
        raise ValueError("designer request is too large for the AgentServer link")
    return forwarded


def register_personal_designer_handlers(channel, resolve_client):
    """Register the Designer RPCs for the account-free local Web profile.

    Same southbound contract as :func:`register_designer_handlers`, behind the
    Web Origin gate the other personal Web operations use. The AgentServer
    must advertise ``personal_designer_v1``; without it nothing is forwarded.
    Each request carries the Gateway's marker, and the AgentServer's refusal
    (code and details, such as the version it holds) is passed back as is.
    """
    subscriptions = DesignerSubscriptions(channel)
    channel.designer_subscriptions = subscriptions
    channel.on_disconnect(subscriptions.disconnect)

    def proxy(req_method: ReqMethod, timeout_seconds: float | None):
        async def handler(ws, req_id, params, session_id, user_id=None):
            if not allows_settings_origin(getattr(ws, "request_headers", None)):
                await channel.send_response(
                    ws,
                    req_id,
                    ok=False,
                    error="This operation requires an allowed Web Origin",
                    code="FORBIDDEN",
                )
                return
            try:
                forwarded = validate_personal_designer_params(params)
            except ValueError as exc:
                await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
                return
            async def subscribe(response):
                metadata = getattr(response, "metadata", None)
                sid = metadata.get("_designer_subscribed_session") if isinstance(metadata, dict) else None
                if isinstance(sid, str) and sid:
                    subscriptions.add(ws, sid)

            await proxy_unary_request(
                channel=channel,
                agent_client=resolve_client(),
                ws=ws,
                req_id=req_id,
                params=forwarded,
                session_id=session_id,
                user_id=user_id,
                req_method=req_method,
                label=req_method.value,
                timeout_seconds=timeout_seconds,
                required_capability=PERSONAL_DESIGNER_CAPABILITY,
                extra_params={_PERSONAL_DESIGNER_MARKER: True},
                preserve_error_payload=True,
                response_validator=subscribe,
            )

        return handler

    for req_method, timeout_seconds in _DESIGNER_METHODS:
        if req_method == ReqMethod.DESIGNER_RUN_PAUSE:
            # The page never calls it and the AgentServer answers NOT_IMPLEMENTED.
            continue
        channel.register_method(req_method.value, proxy(req_method, timeout_seconds))
