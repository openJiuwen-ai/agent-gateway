# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Values crossing southbound module boundaries, not authoritative agent state."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from e2a.agent_models import AgentResponse, AgentResponseChunk

from agent_gateway.common.security.link_mtls import MTLSDeploymentIdentity


class TransportKind(str, Enum):
    WEBSOCKET = "websocket"
    HTTP_SSE = "http_sse"
    SSH = "ssh"
    HTTP_PROXY = "http_proxy"
    WEBSOCKET_PROXY = "websocket_proxy"


@dataclass(frozen=True)
class ConnectionTarget:
    """Resolved, immutable connection snapshot, never a live AgentRuntime.

    Managed adapters use sandbox_id as instance_id and agent_id as generation;
    delegated adapters use pod_id and the existing mTLS binding identity.
    Static targets need neither. Credentials must not appear in diagnostics.
    Connection reuse must respect identity, generation, binding and headers,
    not just the endpoint URL.
    """

    transport: TransportKind
    endpoint: str = field(repr=False)
    instance_id: str | None = None
    generation: str | None = None
    binding: MTLSDeploymentIdentity | None = None
    headers: tuple[tuple[str, str], ...] = field(default=(), repr=False)


@dataclass(frozen=True)
class Source:
    """Actual connection origin, not a target inferred from a later route.

    The target is the connection snapshot used for this event. Session and
    request identifiers are business correlation, not authentication evidence.
    """

    target: ConnectionTarget
    request_id: str | None = None
    session_id: str | None = None


T = TypeVar("T")


@dataclass(frozen=True)
class Sourced(Generic[T]):
    """An unchanged protocol result plus out-of-band transport provenance."""

    payload: T
    source: Source


UnaryResult = Sourced[AgentResponse]
StreamResult = Sourced[AgentResponseChunk]
# Keep the existing push wire format, including fields outside parsed chunks.
PushEvent = Sourced[dict[str, object]]


class DeliveryState(str, Enum):
    NOT_SENT = "not_sent"
    REJECTED_BEFORE_EXECUTION = "rejected_before_execution"
    MAY_HAVE_EXECUTED = "may_have_executed"


class TransportError(RuntimeError):
    """Transport failure, not an E2A business rejection or a retry decision.

    Use NOT_SENT or REJECTED_BEFORE_EXECUTION only with positive evidence;
    ambiguous failures are MAY_HAVE_EXECUTED. Preserve the original exception
    as the cause when translating an existing transport error. Never translate
    asyncio.CancelledError into this exception.
    """

    def __init__(self, message: str, *, delivery: DeliveryState, source: Source) -> None:
        super().__init__(message)
        self.delivery = delivery
        self.source = source
