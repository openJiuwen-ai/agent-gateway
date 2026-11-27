# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Northbound southbound-client contract and fixed-target implementation."""

from contextlib import aclosing
from typing import Any, AsyncIterator, Awaitable, Callable

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.models import E2AEnvelope

from agent_gateway.agent_client_mode import resolve_agent_server_client
from agent_gateway.common.client.agent_client import AgentServerClient
from agent_gateway.southbound.contracts import ConnectionTarget, PushEvent, TransportError, TransportKind
from agent_gateway.southbound.control.interfaces import StaticControl
from agent_gateway.southbound.control.local import FixedTargetControl
from agent_gateway.southbound.data.transport import StaticWebSocketTransport
from agent_gateway.southbound.orchestration.interfaces import RoutePolicy, RouteRequest
from agent_gateway.southbound.orchestration.static_router import StaticRouter


class SouthboundAgentClient(AgentServerClient):
    """Common northbound entry point with explicit deployment capabilities.

    agent_client_mode identifies the client mode. The runtime selection and
    Web handlers use "runtime" explicitly; other values describe the client.
    Extension registration still validates configuration against extension IDs.

    shared_directory_layout indicates a shared single-user directory and is
    consumed by is_legacy_shared_directory_client in channel handlers.
    requires_initial_connection tells application.services to connect to the
    configured URI with retries during startup.
    managed_routing is consumed by is_agentos_routing_client for managed
    per-user AgentOS/ADX routing.
    """

    agent_client_mode: str | None = None

    shared_directory_layout = False
    requires_initial_connection = False
    managed_routing = False


class StaticAgentClient(SouthboundAgentClient):
    """Fixed-target orchestration using an immutable AgentServer target."""

    agent_client_mode = "websocket"
    requires_initial_connection = True

    def __init__(self, control: StaticControl, router: RoutePolicy, transport: StaticWebSocketTransport,
                 *, shared_directory_layout: bool = False) -> None:
        self._control = control
        self._router = router
        self._transport = transport
        self.shared_directory_layout = shared_directory_layout

    @property
    def server_ready(self) -> bool:
        return self._transport.server_ready

    @property
    def agent_ready(self) -> bool:
        return self._transport.agent_ready

    @property
    def can_reconnect(self) -> bool:
        return self._transport.can_reconnect

    async def connect(self, uri: str) -> None:
        target = self._control.get_target()
        if uri != target.endpoint:
            raise ValueError("connection URL does not match the configured AgentServer")
        await self._transport.start()
        try:
            await self._transport.connect(target)
        except BaseException:
            await self._transport.close()
            raise

    async def disconnect(self) -> None:
        await self._transport.close()

    def set_or_update_server_config(self, *, config: dict[str, Any],
                                    env: dict[str, str] | None = None) -> None:
        """The fixed local AgentServer does not consume config snapshots."""

    def set_server_push_handler(self, handler: Callable[[dict[str, Any]], Awaitable[None]] | None) -> None:
        if handler is None:
            self._transport.set_push_handler(None)
            return

        async def receive(event: PushEvent) -> None:
            await handler(event.payload)

        self._transport.set_push_handler(receive)

    async def send_request(self, envelope: E2AEnvelope, *, timeout: float | None = None) -> AgentResponse:
        async with self._router.resolve(RouteRequest(envelope, allow_create=False)) as route:
            try:
                result = await self._transport.send_request(route.targets[0], envelope, timeout=timeout)
            except TransportError as exc:
                if exc.__cause__ is not None:
                    raise exc.__cause__
                raise
            return result.payload

    async def send_request_stream(self, envelope: E2AEnvelope) -> AsyncIterator[AgentResponseChunk]:
        async with self._router.resolve(RouteRequest(envelope, allow_create=False)) as route:
            try:
                async with aclosing(self._transport.send_request_stream(route.targets[0], envelope)) as stream:
                    async for result in stream:
                        yield result.payload
            except TransportError as exc:
                if exc.__cause__ is not None:
                    raise exc.__cause__
                raise


def build_static_client(uri: str, *, shared_directory_layout: bool = False) -> SouthboundAgentClient:
    target = ConnectionTarget(TransportKind.WEBSOCKET, uri)
    control = FixedTargetControl(target)
    return StaticAgentClient(
        control, StaticRouter(control), StaticWebSocketTransport(target),
        shared_directory_layout=shared_directory_layout,
    )


def assemble_client(extension: Any, *, mode: str, enterprise: bool, uri: str) -> AgentServerClient:
    """Reuse extension-owned clients and select the default fixed deployment.

    Preserve extension-declared layout capabilities, except for the legacy
    WebSocket client whose layout is determined by deployment assembly.
    """
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    def websocket_factory() -> AgentServerClient:
        if not enterprise:
            return build_static_client(uri, shared_directory_layout=True)
        client = WebSocketAgentServerClient(ping_interval=20.0, ping_timeout=600.0)
        client.shared_directory_layout = False
        return client

    client = resolve_agent_server_client(extension, mode=mode, websocket_factory=websocket_factory)
    if extension is not None and isinstance(client, WebSocketAgentServerClient):
        client.shared_directory_layout = False
    return client
