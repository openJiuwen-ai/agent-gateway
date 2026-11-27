# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Northbound client facade for the fixed-target southbound assembly."""

from contextlib import aclosing
from typing import Any, AsyncIterator, Awaitable, Callable

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.models import E2AEnvelope

from agent_gateway.common.client.agent_client import AgentServerClient
from agent_gateway.southbound.contracts import ConnectionTarget, PushEvent, TransportError, TransportKind
from agent_gateway.southbound.control.interfaces import StaticControl
from agent_gateway.southbound.control.local import FixedTargetControl
from agent_gateway.southbound.data.transport import StaticWebSocketTransport
from agent_gateway.southbound.orchestration.interfaces import RoutePolicy, RouteRequest
from agent_gateway.southbound.orchestration.static_router import StaticRouter


class SouthboundAgentClient(AgentServerClient):
    """Personal-mode orchestration using an immutable local target."""

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
    return SouthboundAgentClient(
        control, StaticRouter(control), StaticWebSocketTransport(target),
        shared_directory_layout=shared_directory_layout,
    )
