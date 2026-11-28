# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Demo package B: entry-aware HTTP policy (P3 pre-route, P4 dispatch).

Only packages under test use this token; it is not a credential.
"""

from __future__ import annotations

from agent_gateway.extensions_v2.models import (
    EVENT_GATEWAY_REQUEST,
    GatewayRequestOutput,
)

DEMO_TOKEN = "demo-token"
BLOCKED_PATH = "/blocked"


def _bearer(value: object) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("bearer "):
        return text[7:].strip()
    return text


class HttpPolicy:
    """Deny blocked pre-route paths, consume an echo, verify an identity."""

    def register(self, registrar) -> None:
        registrar.register_event_handler(
            EVENT_GATEWAY_REQUEST,
            self.on_request,
            {"transport": "http"},
        )

    async def on_request(self, event, ops, next_handler):
        del ops
        request = event.event_input
        if request.entry == "http_pre_route":
            if request.path == BLOCKED_PATH:
                return GatewayRequestOutput(
                    handled=True,
                    route="deny",
                    error={"code": "FORBIDDEN", "message": "blocked by demo policy"},
                )
            return await next_handler(event)
        if request.entry == "http_dispatch":
            if request.method == "demo.echo":
                return GatewayRequestOutput(
                    handled=True,
                    route="consume",
                    response={
                        "entry": request.entry,
                        "params": dict(request.params),
                        "routing": dict(request.routing),
                    },
                )
            if request.method == "demo.whoami":
                getter = getattr(request.headers, "get", None)
                token = _bearer(getter("authorization", "")) if callable(getter) else ""
                if token == DEMO_TOKEN:
                    return GatewayRequestOutput(
                        route="forward",
                        user_id="demo-user",
                        identity_source="verified",
                    )
                return GatewayRequestOutput(
                    handled=True,
                    route="deny",
                    error={"code": "UNAUTHORIZED", "message": "token required"},
                )
        return await next_handler(event)


def create_extension() -> HttpPolicy:
    return HttpPolicy()
