# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Demo package A: ``gateway.handshake`` plus the WS frame gate.

Only packages under test use this token; it is not a credential.
"""

from __future__ import annotations

from agent_gateway.extensions_v2.models import (
    EVENT_GATEWAY_HANDSHAKE,
    EVENT_GATEWAY_REQUEST,
    GatewayHandshakeOutput,
    GatewayRequestOutput,
)

DEMO_TOKEN = "demo-token"


def _bearer(value: object) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("bearer "):
        return text[7:].strip()
    return text


def _token_from_headers(headers: object) -> str:
    getter = getattr(headers, "get", None)
    return _bearer(getter("authorization", "")) if callable(getter) else ""


class WsAuthGate:
    """Allow ``demo-token`` at Upgrade time; deny unauthenticated WS frames."""

    def register(self, registrar) -> None:
        registrar.register_event_handler(EVENT_GATEWAY_HANDSHAKE, self.on_handshake)
        registrar.register_event_handler(
            EVENT_GATEWAY_REQUEST,
            self.on_frame,
            {"transport": "websocket"},
        )

    async def on_handshake(self, event, ops, next_handler):
        del ops, next_handler
        request = event.event_input
        token = _bearer(request.query.get("token")) or _token_from_headers(request.headers)
        if token == DEMO_TOKEN:
            return GatewayHandshakeOutput(
                handled=True,
                allow=True,
                user_id="demo-user",
                extensions={"source": "demo.ws.auth"},
                bind_connection_identity=True,
            )
        return GatewayHandshakeOutput(
            handled=True,
            allow=False,
            error={"code": "UNAUTHORIZED", "message": "token required"},
        )

    async def on_frame(self, event, ops, next_handler):
        del ops
        request = event.event_input
        if request.user_id:
            return await next_handler(event)
        return GatewayRequestOutput(
            handled=True,
            route="deny",
            error={"code": "UNAUTHORIZED", "message": "handshake token required"},
        )


def create_extension() -> WsAuthGate:
    return WsAuthGate()
