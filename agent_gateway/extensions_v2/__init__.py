# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway-side extension runtime entry points.

``WebChannel`` calls :func:`init_gateway_extensions` on the first gateway frame,
:func:`intercept_gateway_frame` for every inbound frame and
:func:`notify_gateway_disconnect` when a socket closes. The singleton itself
lives in :mod:`agent_gateway.extensions_v2.assembly`; this module keeps
the channel-facing surface stable.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from agent_gateway.extensions_v2.assembly import (
    install_gateway_extensions,
    shutdown_gateway_extensions,
)
from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.models import GatewayHandshakeOutput
from agent_gateway.extensions_v2.runtime import (
    GatewayHttpDenyInjectionError,
    GatewayRuntime,
)

logger = logging.getLogger(__name__)


async def init_gateway_extensions(
    channel: Any,
    config: GatewayExtensionConfig | Mapping[str, Any] | None = None,
) -> GatewayRuntime | None:
    """Create and start the runtime for ``channel``; idempotent."""

    assembly = await install_gateway_extensions(channel, config)
    return assembly.runtime if assembly is not None else None


async def intercept_gateway_frame(
    ws: Any,
    req_id: str,
    method: str,
    params: Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
    *,
    request_context: Mapping[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Run one frame through ``gateway.request``.

    ``headers`` carries ``request_context.headers`` from the browser frame so the
    USER handlers can validate credentials.
    ``visited=True`` means the frame was answered by the extension runtime and
    the native dispatch must be skipped. Any failure fails open: the frame
    continues on the path it would have taken without extensions.
    """

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None or not isinstance(params, Mapping):
        return False, dict(params) if isinstance(params, Mapping) else {}
    try:
        return await runtime.intercept(ws, req_id, method, dict(params), headers, request_context=request_context)
    except Exception as exc:  # noqa: BLE001 - fail open to the native path
        logger.warning("[GatewayExtensions] frame interception failed, forwarding: %s", exc)
        return False, dict(params)


async def authenticate_gateway_handshake(
    channel: Any,
    *,
    path: str,
    headers: Any = None,
    query: Any = None,
    remote_addr: str = "",
    channel_id: str = "web",
    transport: str = "websocket",
) -> GatewayHandshakeOutput | None:
    """Run one Upgrade through ``gateway.handshake`` before ``accept()``.

    The runtime is started explicitly first: without it, "no subscriber" must
    not be mistaken for "allow". ``None`` means "no V2 opinion"; the caller
    keeps the V1 hook decision. This entry never sets the channel's
    ``_extensions_active`` flag, so the first business frame still announces
    the connection via :func:`notify_gateway_connect`.
    """

    try:
        runtime = await init_gateway_extensions(channel)
    except Exception as exc:  # noqa: BLE001 - a failed assembly falls back to V1
        logger.warning("[GatewayExtensions] handshake runtime unavailable: %s", exc)
        return None
    if runtime is None:
        return None
    try:
        return await runtime.handshake(
            path=path,
            headers=headers,
            query=query,
            remote_addr=remote_addr,
            channel_id=channel_id,
            transport=transport,
        )
    except Exception as exc:  # noqa: BLE001 - handshake must fall back to V1
        logger.warning("[GatewayExtensions] handshake interception failed: %s", exc)
        return None


async def intercept_gateway_http(
    *,
    channel: Any = None,
    method: str,
    params: Mapping[str, Any],
    headers: Any = None,
    session_id: str | None = None,
    request_id: str = "",
    routing: Mapping[str, str] | None = None,
    user_id: str | None = None,
    connection_id: str = "",
    channel_id: str = "web",
    outbound: Any = None,
) -> tuple[bool, dict[str, Any], str | None]:
    """Run one HTTP business dispatch through ``gateway.request`` (P4).

    Returns ``(handled, params, verified_user_id)``. ``handled=True`` means a
    synthetic response frame was injected into ``outbound`` and the native HTTP
    dispatch must be skipped. The runtime is assembled on demand (like the
    handshake entry) so purely HTTP clients still hit the P4 trigger point;
    any failure falls back to the native HTTP path.
    """

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None and channel is not None:
        try:
            start = getattr(channel, "start_extensions", None)
            ensure = getattr(channel, "ensure_extensions", None)
            if callable(start):
                # Reuse the channel lifecycle so shutdown owns the runtime it started.
                await start()
                runtime = get_gateway_runtime()
            elif callable(ensure):
                await ensure()
                runtime = get_gateway_runtime()
            else:
                runtime = await init_gateway_extensions(channel)
        except Exception as exc:  # noqa: BLE001 - a failed assembly falls back to the native path
            logger.warning("[GatewayExtensions] http dispatch runtime unavailable: %s", exc)
            return False, dict(params) if isinstance(params, Mapping) else {}, None
    if runtime is None or not isinstance(params, Mapping):
        return False, dict(params) if isinstance(params, Mapping) else {}, None
    try:
        return await runtime.intercept_http(
            method=method,
            params=dict(params),
            headers=headers,
            session_id=session_id,
            request_id=request_id,
            routing=routing,
            user_id=user_id,
            connection_id=connection_id,
            channel_id=channel_id,
            outbound=outbound,
        )
    except GatewayHttpDenyInjectionError:
        # The request was denied and the synthetic answer could not be sent;
        # the caller must fail it instead of executing the native handler.
        raise
    except Exception as exc:  # noqa: BLE001 - fail back to the native HTTP path
        logger.warning("[GatewayExtensions] http dispatch interception failed, forwarding: %s", exc)
        return False, dict(params), None


async def notify_gateway_connect(ws: Any) -> None:
    """Announce a connection so it receives declarations; no-op while unavailable.

    Every connection receives declarations, including the empty ``packages`` of a
    runtime without client-capability packages. A browser that reconnects after a
    restart with every plugin disabled otherwise waits for a snapshot that never
    arrives and rejects each business request as CLIENT_DECLARATIONS_REQUIRED (S_06).
    """

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None:
        return
    try:
        connection_id = await runtime.connection_id(ws)
        if connection_id:
            await runtime.browser.connect(ws, connection_id)
    except Exception as exc:  # noqa: BLE001 - announcement must not break the connection
        logger.warning("[GatewayExtensions] connect notification failed: %s", exc)


async def notify_gateway_disconnect(ws: Any, session_ids: Any = None) -> None:
    """Release connection-scoped extension state."""

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None:
        return
    try:
        await runtime.on_disconnect(ws, session_ids)
    except Exception as exc:  # noqa: BLE001 - disconnect cleanup must not raise
        logger.warning("[GatewayExtensions] disconnect cleanup failed: %s", exc)
