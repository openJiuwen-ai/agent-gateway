# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Internal routing strategy input and output, independent of transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncContextManager, Protocol

from e2a.models import E2AEnvelope

from agent_gateway.southbound.contracts import ConnectionTarget, Source


@dataclass(frozen=True)
class RouteRequest:
    """Keep E2A identity/routing context intact; never infer tenant from URL."""

    envelope: E2AEnvelope
    allow_create: bool
    # Reverse RPC must use the recorded origin, never a fresh routing lookup.
    origin: Source | None = None


@dataclass(frozen=True)
class RouteSelection:
    """One execution target normally; interrupt may select multiple targets.

    An empty selection means no existing target for a no-create operation.
    The routing strategy retains authoritative manager/runtime state.
    """

    targets: tuple[ConnectionTarget, ...]
    session_id: str | None = None
    route_request_id: str | None = None


class RoutePolicy(Protocol):
    def resolve(self, request: RouteRequest) -> AsyncContextManager[RouteSelection]:
        """Resolve and protect targets for the duration of execution.

        Enter acquires any needed activity references, exit releases them on
        success, failure or cancellation. Static routing can be a no-op context;
        delegated routing may coordinate touch without owning a local agent.
        A failed entry must clean up partially acquired resources itself.
        A supplied origin must be honored or rejected; it must not silently
        fall back to a different target. allow_create=False forbids cold start.
        """
        ...
