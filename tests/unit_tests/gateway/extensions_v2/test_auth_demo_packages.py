# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""End-to-end checks for the two demo packages under ``tests/fixtures/gateway_auth_demo``.

The packages are loaded from disk through the real manifest/loader path, so the
checks cover the contract an extension author actually sees.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from openjiuwen.core.events import Event, SessionContext

from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.models import (
    EVENT_GATEWAY_REQUEST,
    GatewayRequestInput,
)
from agent_gateway.extensions_v2.runtime import GatewayRuntime

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "gateway_auth_demo"
DEMO_TOKEN = "demo-token"


class FakeChannel:
    channel_id = "web"

    def __init__(self) -> None:
        self.responses: list[dict] = []

    async def ensure_ws_id(self, ws) -> str:
        if not getattr(ws, "_jiuwen_ws_id", ""):
            ws._jiuwen_ws_id = "ws-demo"
        return ws._jiuwen_ws_id

    def connection_user_id(self, ws):
        return getattr(ws, "_user", None)

    def routing_identity(self, ws):
        return dict(getattr(ws, "_web_routing", {}) or {})

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append(
            {"id": req_id, "ok": ok, "payload": payload or {}, "error": error, "code": code}
        )


class FakeOutbound:
    def __init__(self) -> None:
        self.frames: list[dict] = []
        self._jiuwen_ws_id = "http-1"

    def accept_frame(self, frame: dict) -> None:
        self.frames.append(frame)


async def _runtime(tmp_path) -> tuple[GatewayRuntime, FakeChannel]:
    channel = FakeChannel()
    runtime = GatewayRuntime(
        channel=channel,
        config=GatewayExtensionConfig(install_dir=FIXTURES, data_dir=tmp_path / "data"),
    )
    await runtime.start()
    return runtime, channel


def _ws(user: str | None = None) -> SimpleNamespace:
    ws = SimpleNamespace(closed=False)
    if user is not None:
        ws._user = user
    return ws


# ── package A: demo.ws.auth ────────────────────────────────────────────────


async def test_demo_ws_auth_allows_the_demo_token_at_handshake(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    try:
        decision = await runtime.handshake(
            path="/ws", headers={}, query={"token": DEMO_TOKEN}, transport="websocket",
        )

        assert decision is not None
        assert decision.handled is True and decision.allow is True
        assert decision.user_id == "demo-user"
        assert decision.bind_connection_identity is True
    finally:
        await runtime.shutdown()


async def test_demo_ws_auth_denies_a_missing_token_at_handshake(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    try:
        decision = await runtime.handshake(path="/ws", headers={}, query={}, transport="websocket")

        assert decision is not None
        assert decision.handled is True and decision.allow is False
        assert decision.error == {"code": "UNAUTHORIZED", "message": "token required"}
    finally:
        await runtime.shutdown()


async def test_demo_ws_auth_denies_an_unauthenticated_frame(tmp_path):
    runtime, channel = await _runtime(tmp_path)
    try:
        visited, params = await runtime.intercept(_ws(), "r1", "chat.send", {"q": "hi"})

        assert visited is True
        assert params == {"q": "hi"}
        assert channel.responses == [{
            "id": "r1", "ok": False, "payload": {},
            "error": "handshake token required", "code": "UNAUTHORIZED",
        }]
    finally:
        await runtime.shutdown()


async def test_demo_ws_auth_forwards_an_authenticated_frame(tmp_path):
    runtime, channel = await _runtime(tmp_path)
    try:
        visited, params = await runtime.intercept(_ws("demo-user"), "r2", "chat.send", {"q": "hi"})

        assert visited is False
        assert params == {"q": "hi"}
        assert channel.responses == []
    finally:
        await runtime.shutdown()


# ── package B: demo.http.policy ────────────────────────────────────────────


async def test_demo_http_policy_denies_a_blocked_pre_route_path(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    try:
        request = GatewayRequestInput(
            method="GET",
            transport="http",
            path="/blocked",
            entry="http_pre_route",
            headers={"authorization": f"Bearer {DEMO_TOKEN}"},
        )
        result = await runtime.bus.trigger(
            Event(EVENT_GATEWAY_REQUEST, request),
            SessionContext("gateway:pre-route"),
        )

        assert result.route == "deny"
        assert result.error == {"code": "FORBIDDEN", "message": "blocked by demo policy"}
    finally:
        await runtime.shutdown()


async def test_demo_http_policy_forwards_an_unblocked_pre_route_path(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    try:
        request = GatewayRequestInput(
            method="GET", transport="http", path="/assets/a.css", entry="http_pre_route",
        )
        result = await runtime.bus.trigger(
            Event(EVENT_GATEWAY_REQUEST, request),
            SessionContext("gateway:pre-route"),
        )

        assert result.handled is False
        assert result.route == "forward"
    finally:
        await runtime.shutdown()


async def test_demo_http_policy_consumes_an_echo_dispatch(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    outbound = FakeOutbound()
    try:
        handled, _, verified = await runtime.intercept_http(
            method="demo.echo",
            params={"x": 1},
            request_id="h1",
            routing={"group_id": "g1", "user_id": "ignored"},
            outbound=outbound,
        )

        assert handled is True
        assert verified is None
        assert outbound.frames == [{
            "type": "res", "id": "h1", "ok": True,
            "payload": {"entry": "http_dispatch", "params": {"x": 1}, "routing": {"group_id": "g1"}},
        }]
    finally:
        await runtime.shutdown()


async def test_demo_http_policy_overrides_identity_after_verification(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    try:
        handled, _, verified = await runtime.intercept_http(
            method="demo.whoami",
            params={},
            headers={"authorization": f"Bearer {DEMO_TOKEN}"},
            user_id="spoofed",
            outbound=FakeOutbound(),
        )

        assert handled is False
        assert verified == "demo-user"
    finally:
        await runtime.shutdown()


async def test_demo_http_policy_denies_whoami_without_the_token(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    outbound = FakeOutbound()
    try:
        handled, _, verified = await runtime.intercept_http(
            method="demo.whoami",
            params={},
            headers={"authorization": "Bearer wrong"},
            request_id="h2",
            outbound=outbound,
        )

        assert handled is True
        assert verified is None
        assert outbound.frames == [{
            "type": "res", "id": "h2", "ok": False,
            "error": "token required", "code": "UNAUTHORIZED", "payload": {},
        }]
    finally:
        await runtime.shutdown()


async def test_demo_http_policy_leaves_other_methods_to_the_native_path(tmp_path):
    runtime, _ = await _runtime(tmp_path)
    outbound = FakeOutbound()
    try:
        handled, params, verified = await runtime.intercept_http(
            method="chat.send", params={"session_id": "s1"}, outbound=outbound,
        )

        assert handled is False
        assert params == {"session_id": "s1"}
        assert verified is None
        assert outbound.frames == []
    finally:
        await runtime.shutdown()
