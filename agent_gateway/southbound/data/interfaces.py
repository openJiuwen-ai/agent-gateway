# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Target-specific transport interfaces, independent of control and routing."""

from __future__ import annotations

from typing import AsyncContextManager, AsyncIterator, Awaitable, Callable, Protocol, TypeVar

from e2a.models import E2AEnvelope

from agent_gateway.southbound.contracts import (
    ConnectionTarget, PushEvent, StreamResult, UnaryResult,
)

PushHandler = Callable[[PushEvent], Awaitable[None]]

TunnelRequest = TypeVar("TunnelRequest", contravariant=True)
TunnelSession = TypeVar("TunnelSession", covariant=True)


class DataTransport(Protocol):
    """Own connections and request multiplexing, never agent lifecycles.

    Start prepares local state only and repeated start is safe. Connect establishes transport readiness
    for a specific target; it does not imply agent execution readiness.
    Closing an iterator or cancelling its consumer releases request resources
    and propagates cancellation; neither implies a business interrupt.
    Reconnecting a connection does not authorize resending a business request.
    """

    async def start(self) -> None: ...

    async def connect(self, target: ConnectionTarget) -> None:
        """Reuse or establish this exact target; clean up partial failures."""
        ...

    async def disconnect(self, target: ConnectionTarget) -> None:
        """Release this target's connection, never delete its remote instance."""
        ...

    async def close(self) -> None:
        """Idempotently stop admission and release owned tasks/connections.

        Borrowed clients are not closed; pending waiters must be unblocked.
        """
        ...

    async def send_request(
        self, target: ConnectionTarget, envelope: E2AEnvelope,
        *, timeout: float | None = None,
    ) -> UnaryResult: ...

    def send_request_stream(
        self, target: ConnectionTarget, envelope: E2AEnvelope,
    ) -> AsyncIterator[StreamResult]: ...


class PushTransport(Protocol):
    """Push readiness is distinct from unary connection readiness."""

    async def subscribe(self, target: ConnectionTarget, handler: PushHandler) -> None:
        """Return once the push subscription is ready; emit actual source."""
        ...

    async def unsubscribe(self, target: ConnectionTarget) -> None: ...


class TunnelTransport(Protocol[TunnelRequest, TunnelSession]):
    """SSH/HTTP/WS proxy forwarding without selecting or allocating agents."""

    def open(
        self, target: ConnectionTarget, request: TunnelRequest,
    ) -> AsyncContextManager[TunnelSession]:
        """Release the stream and owned resources on exit, including cancellation."""
        ...
