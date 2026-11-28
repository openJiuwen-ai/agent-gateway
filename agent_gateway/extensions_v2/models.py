# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway extension event inputs and outputs.

These objects are the contract between the Gateway trigger points, the event
bus and extension packages. Every event lives in this module so a package only
imports one place to build its input.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

EVENT_GATEWAY_REQUEST = "gateway.request"
#: One Upgrade attempt, before ``accept()``: allow, deny or keep the V1 decision.
EVENT_GATEWAY_HANDSHAKE = "gateway.handshake"
EVENT_UI_RENDER = "ui.render"
EVENT_GATEWAY_DISCONNECT = "gateway.disconnect"
#: Host actions a package asks for through Ops; each runs as its own chain with a CORE default.
EVENT_UI_APPEARANCE = "ui.appearance"
EVENT_UI_DOWNLOAD = "ui.download"
EVENT_UI_SPLASH_PREVIEW = "ui.splash.preview"

#: The single upstream WebUI method that carries browser ui.* events.
WEBUI_DISPATCH_METHOD = "webui.dispatch"

#: Synthetic session id used when a frame has no chat session. The event bus
#: requires a non-empty session id for every call.
GATEWAY_SESSION_PREFIX = "gateway:"


def _empty_mapping() -> Mapping[str, Any]:
    return MappingProxyType({})


def _empty_headers() -> Mapping[str, str]:
    return MappingProxyType({})


def gateway_session_id(connection_id: str) -> str:
    """Return the synthetic session id bound to one browser connection."""

    return f"{GATEWAY_SESSION_PREFIX}{connection_id}" if connection_id else GATEWAY_SESSION_PREFIX


class GatewayRoute(StrEnum):
    """Where a frame goes after ``gateway.request`` returns."""

    LOCAL = "local"
    FORWARD = "forward"
    CONSUME = "consume"
    DENY = "deny"

    @classmethod
    def parse(cls, value: object) -> "GatewayRoute | None":
        """Return the matching route, or None for an unknown value."""

        if isinstance(value, GatewayRoute):
            return value
        if isinstance(value, str):
            try:
                return cls(value.strip().lower())
            except ValueError:
                return None
        return None


@dataclass(frozen=True, slots=True)
class GatewayRequestInput:
    """One inbound WS frame before native dispatch.

    ``headers`` mirrors ``request_context.headers`` of the browser frame with
    lower-cased names. The authentication gate only looks for the presence of
    ``authorization``; validating the token is the job of a USER handler.
    """

    method: str
    params: Mapping[str, Any] = field(default_factory=_empty_mapping)
    session_id: str | None = None
    connection_id: str = ""
    user_id: str | None = None
    channel_id: str = "web"
    headers: Mapping[str, str] = field(default_factory=_empty_headers)
    request_context: Mapping[str, Any] = field(default_factory=_empty_mapping)
    transport: str = "websocket"
    path: str = ""
    request_id: str = ""
    #: Trusted routing scope cached at connect time (``group_id``/``bot_id``/``gateway_id``).
    #: Never derived from ``params``: ``chat.send``'s ``group_id`` is a session id.
    routing: Mapping[str, str] = field(default_factory=_empty_headers)
    #: Which trigger point raised the event: ``ws_frame`` / ``http_pre_route`` / ``http_dispatch``.
    entry: str = "ws_frame"


@dataclass(frozen=True, slots=True)
class GatewayHandshakeInput:
    """One Upgrade attempt, carrying the real transport facts (before ``accept()``)."""

    path: str = ""
    headers: Mapping[str, str] = field(default_factory=_empty_headers)
    query: Mapping[str, str] = field(default_factory=_empty_headers)
    remote_addr: str = ""
    channel_id: str = "web"
    transport: str = "websocket"


@dataclass(frozen=True, slots=True)
class GatewayHandshakeOutput:
    """Handshake decision: no opinion by default so the V1 hook still decides."""

    handled: bool = False
    #: Deny by default: a handler that claims the handshake without ``allow=True``
    #: must not accidentally accept the connection.
    allow: bool = False
    user_id: str | None = None
    extensions: Mapping[str, Any] = field(default_factory=_empty_mapping)
    #: When True and ``user_id`` is set, the caller binds it as the connection identity.
    bind_connection_identity: bool = False
    error: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class GatewayDisconnectInput:
    """Lifecycle notification before a browser connection is discarded."""

    connection_id: str


@dataclass(frozen=True, slots=True)
class GatewayRequestOutput:
    """Frame disposition decided by the handler chain."""

    handled: bool = False
    route: str = GatewayRoute.FORWARD.value
    params: Mapping[str, Any] | None = None
    response: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None
    meta: Mapping[str, Any] | None = None
    #: Verified principal a handler returns for the HTTP business dispatch point.
    user_id: str | None = None
    #: ``verified`` lets the dispatcher overwrite ``X-User-Id``; anything else keeps ``client_trust``.
    identity_source: str | None = None


@dataclass(frozen=True, slots=True)
class HttpFetchInput:
    """One outbound HTTP request made by a Gateway extension."""

    url: str
    method: str = "GET"
    headers: Mapping[str, str] = field(default_factory=_empty_headers)
    body: Any = None
    #: ``0`` means "use ``HttpPolicy.timeout_ms``"; a positive value overrides it.
    timeout_ms: int = 0
    follow_redirects: bool = False


@dataclass(frozen=True, slots=True)
class HttpFetchOutput:
    """Result of one ``ops.http.fetch`` call."""

    status: int = 0
    headers: Mapping[str, str] = field(default_factory=_empty_headers)
    body: Any = None
    elapsed_ms: int = 0
    code: str | None = None
