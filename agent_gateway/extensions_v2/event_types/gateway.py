# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway connection events: ``gateway.request`` frames and ``gateway.handshake``."""

from __future__ import annotations

from openjiuwen.core.events import EventHandler, ExtensionEventBus

from agent_gateway.extensions_v2.event_types.matching import match_pattern_subset
from agent_gateway.extensions_v2.models import (
    EVENT_GATEWAY_HANDSHAKE,
    EVENT_GATEWAY_REQUEST,
    GatewayHandshakeInput,
    GatewayHandshakeOutput,
)


def register_gateway_request_events(bus: ExtensionEventBus, core_handler: EventHandler) -> None:
    """Register ``gateway.request`` with its CORE handler."""

    bus.register_event_type(EVENT_GATEWAY_REQUEST, match_pattern_subset, core_handler)


def build_gateway_handshake_core_handler() -> EventHandler:
    """Build the CORE handler for ``gateway.handshake``.

    The default is "no opinion": the caller keeps the V1 hook decision. A USER
    handler must explicitly set ``handled=True`` to take over the handshake.
    """

    async def gateway_handshake_core(event, ops, next_handler):
        del ops, next_handler
        if not isinstance(event.event_input, GatewayHandshakeInput):
            raise TypeError("gateway.handshake requires GatewayHandshakeInput")
        return GatewayHandshakeOutput(handled=False, allow=True)

    return gateway_handshake_core


def register_gateway_handshake_events(bus: ExtensionEventBus, core_handler: EventHandler) -> None:
    """Register ``gateway.handshake`` with its CORE handler."""

    bus.register_event_type(EVENT_GATEWAY_HANDSHAKE, match_pattern_subset, core_handler)
