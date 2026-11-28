# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Auth trigger points: ``gateway.handshake`` plus the P3/P4 HTTP entries."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from openjiuwen.core.events import Event, SessionContext

from agent_gateway.channel_manager.base import BaseWebChannel
from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.models import (
    EVENT_GATEWAY_HANDSHAKE,
    EVENT_GATEWAY_REQUEST,
    GatewayHandshakeInput,
    GatewayHandshakeOutput,
    GatewayRequestOutput,
)
from agent_gateway.extensions_v2.registry import HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime
from agent_gateway.extensions_v2.webui.http import attach_browser_routes


class FakeChannel:
    """Minimal stand-in for WebChannel used by the runtime."""

    channel_id = "web"

    def __init__(self, routing: dict[str, str] | None = None) -> None:
        self.responses: list[dict] = []
        self._ids: dict[int, str] = {}
        self._routing = dict(routing or {})

    async def ensure_ws_id(self, ws) -> str:
        key = id(ws)
        if key not in self._ids:
            self._ids[key] = f"ws-{len(self._ids) + 1}"
            setattr(ws, "_jiuwen_ws_id", self._ids[key])
        return self._ids[key]

    def connection_user_id(self, ws):
        return getattr(ws, "_user", None)

    def routing_identity(self, ws):
        return dict(self._routing)

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append(
            {"id": req_id, "ok": ok, "payload": payload or {}, "error": error, "code": code}
        )


class FakeOutbound:
    """Captures the synthetic frames an HTTP trigger point injects."""

    def __init__(self) -> None:
        self.frames: list[dict] = []
        self._jiuwen_ws_id = "http-1"

    def accept_frame(self, frame: dict) -> None:
        self.frames.append(frame)


def _ws(user: str | None = None) -> SimpleNamespace:
    ws = SimpleNamespace(closed=False)
    if user is not None:
        ws._user = user
    return ws


def _package(handler, event_type: str = EVENT_GATEWAY_REQUEST) -> LoadedPackage:
    return LoadedPackage(
        package_id="demo",
        version="1.0.0",
        root=Path("/tmp/demo"),
        handlers=(HandlerDeclaration(event_type, handler, {}, "local-1"),),
    )


async def _runtime(
    tmp_path, channel: FakeChannel | None = None
) -> GatewayRuntime:
    runtime = GatewayRuntime(
        channel=channel or FakeChannel(),
        config=GatewayExtensionConfig(install_dir=tmp_path, data_dir=tmp_path / "data"),
    )
    await runtime.start()
    return runtime


# ── gateway.handshake ──────────────────────────────────────────────────────


async def test_handshake_core_defaults_to_no_opinion(tmp_path):
    runtime = await _runtime(tmp_path)

    result = await runtime.bus.trigger(
        Event(EVENT_GATEWAY_HANDSHAKE, GatewayHandshakeInput(path="/ws")),
        SessionContext("gateway:handshake"),
    )

    assert isinstance(result, GatewayHandshakeOutput)
    assert result.handled is False
    assert result.allow is True


async def test_handshake_without_user_handler_stays_silent(tmp_path):
    runtime = await _runtime(tmp_path)

    decision = await runtime.handshake(path="/ws", headers={}, query={})

    assert decision is None


async def test_handshake_passes_transport_facts_to_the_handler(tmp_path):
    runtime = await _runtime(tmp_path)
    observed = []

    async def capture(event, ops, next_handler):
        observed.append(event.event_input)
        return await next_handler(event)

    runtime.publish(_package(capture, EVENT_GATEWAY_HANDSHAKE))
    try:
        await runtime.handshake(
            path="/ws?x=1",
            headers={"Authorization": "Bearer tk_1"},
            query={"token": "tk_1"},
            remote_addr="client.example:53000",
            channel_id="web",
            transport="websocket",
        )

        request = observed[0]
        assert isinstance(request, GatewayHandshakeInput)
        assert request.path == "/ws?x=1"
        assert request.headers == {"authorization": "Bearer tk_1"}
        assert request.query == {"token": "tk_1"}
        assert request.remote_addr == "client.example:53000"
        assert request.transport == "websocket"
    finally:
        await runtime.shutdown()


async def test_handshake_returns_the_handler_decision(tmp_path):
    runtime = await _runtime(tmp_path)

    async def decide(event, ops, next_handler):
        if event.event_input.query.get("token") == "good":
            return GatewayHandshakeOutput(
                handled=True, allow=True, user_id="alice", bind_connection_identity=True,
            )
        return GatewayHandshakeOutput(handled=True, allow=False, error={"message": "bad token"})

    runtime.publish(_package(decide, EVENT_GATEWAY_HANDSHAKE))
    try:
        allowed = await runtime.handshake(path="/ws", headers={}, query={"token": "good"})
        denied = await runtime.handshake(path="/ws", headers={}, query={"token": "bad"})

        assert allowed is not None and allowed.allow is True
        assert allowed.user_id == "alice"
        assert allowed.bind_connection_identity is True
        assert denied is not None and denied.handled is True and denied.allow is False
        assert denied.error == {"message": "bad token"}
    finally:
        await runtime.shutdown()


async def test_handshake_handler_failure_falls_back_to_no_opinion(tmp_path):
    runtime = await _runtime(tmp_path)

    async def broken(event, ops, next_handler):
        raise RuntimeError("boom")

    runtime.publish(_package(broken, EVENT_GATEWAY_HANDSHAKE))
    try:
        decision = await runtime.handshake(path="/ws", headers={}, query={})

        # The chain swallows the failure and reaches the CORE no-opinion output,
        # so the caller still keeps the V1 hook decision.
        assert decision is not None
        assert decision.handled is False
        assert decision.allow is True
    finally:
        await runtime.shutdown()


async def test_handshake_timeout_falls_back_to_no_opinion(tmp_path, monkeypatch):
    from agent_gateway.extensions_v2 import runtime as runtime_module

    runtime = await _runtime(tmp_path)

    async def slow(event, ops, next_handler):
        await asyncio.sleep(0.05)
        return GatewayHandshakeOutput(handled=True, allow=False)

    runtime.publish(_package(slow, EVENT_GATEWAY_HANDSHAKE))
    monkeypatch.setattr(runtime_module, "REQUEST_TIMEOUT_S", 0.01)
    try:
        assert await runtime.handshake(path="/ws", headers={}, query={}) is None
    finally:
        await runtime.shutdown()


async def test_handshake_entry_returns_none_without_a_runtime(monkeypatch):
    from agent_gateway import extensions_v2

    monkeypatch.setattr(extensions_v2, "init_gateway_extensions", AsyncMock(return_value=None))

    assert await extensions_v2.authenticate_gateway_handshake(SimpleNamespace(), path="/ws") is None


async def test_handshake_entry_falls_back_when_assembly_fails(monkeypatch):
    from agent_gateway import extensions_v2

    monkeypatch.setattr(
        extensions_v2,
        "init_gateway_extensions",
        AsyncMock(side_effect=RuntimeError("broken install")),
    )

    assert await extensions_v2.authenticate_gateway_handshake(SimpleNamespace(), path="/ws") is None


async def test_handshake_entry_returns_the_runtime_decision(monkeypatch):
    from agent_gateway import extensions_v2

    decision = GatewayHandshakeOutput(handled=True, allow=False)
    runtime = SimpleNamespace(handshake=AsyncMock(return_value=decision))
    monkeypatch.setattr(extensions_v2, "init_gateway_extensions", AsyncMock(return_value=runtime))

    result = await extensions_v2.authenticate_gateway_handshake(
        SimpleNamespace(), path="/ws", headers={}, query={},
    )

    assert result is decision
    runtime.handshake.assert_awaited_once()


# ── routing / entry ────────────────────────────────────────────────────────


async def test_frame_carries_entry_and_routing_without_user_id(tmp_path):
    channel = FakeChannel(routing={"group_id": "g1", "bot_id": "b1", "gateway_id": "gw1", "user_id": "u1"})
    runtime = await _runtime(tmp_path, channel)
    observed = []

    async def capture(event, ops, next_handler):
        observed.append(event.event_input)
        return await next_handler(event)

    runtime.publish(_package(capture))
    try:
        await runtime.intercept(_ws(), "r1", "chat.send", {})
        request = observed[0]
        assert request.entry == "ws_frame"
        assert request.routing == {"group_id": "g1", "bot_id": "b1", "gateway_id": "gw1"}
    finally:
        await runtime.shutdown()


def test_base_channel_routing_identity_filters_user_id():
    ws = SimpleNamespace(
        _web_routing={"group_id": "g1", "bot_id": " ", "gateway_id": "gw1", "user_id": "u1"}
    )

    assert BaseWebChannel.routing_identity(SimpleNamespace(), ws) == {
        "group_id": "g1",
        "gateway_id": "gw1",
    }


# ── P4: HTTP business dispatch ─────────────────────────────────────────────


async def test_http_dispatch_is_silent_without_handlers(tmp_path):
    runtime = await _runtime(tmp_path)
    outbound = FakeOutbound()

    handled, params, verified = await runtime.intercept_http(
        method="chat.send", params={"session_id": "s1"}, outbound=outbound,
    )

    assert handled is False
    assert params == {"session_id": "s1"}
    assert verified is None
    assert outbound.frames == []


async def test_http_dispatch_deny_injects_a_synthetic_frame(tmp_path):
    runtime = await _runtime(tmp_path)
    outbound = FakeOutbound()

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(
            handled=True, route="deny", error={"code": "AUTH_REQUIRED", "message": "login first"},
        )

    runtime.publish(_package(handler))
    try:
        handled, _, verified = await runtime.intercept_http(
            method="chat.send", params={}, request_id="h1", outbound=outbound,
        )

        assert handled is True
        assert verified is None
        assert outbound.frames == [{
            "type": "res", "id": "h1", "ok": False,
            "error": "login first", "code": "AUTH_REQUIRED", "payload": {},
        }]
    finally:
        await runtime.shutdown()


async def test_http_dispatch_consume_injects_a_success_frame(tmp_path):
    runtime = await _runtime(tmp_path)
    outbound = FakeOutbound()

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(handled=True, route="consume", response={"source": "plugin"})

    runtime.publish(_package(handler))
    try:
        handled, _, _ = await runtime.intercept_http(
            method="config.get", params={}, request_id="h2", outbound=outbound,
        )

        assert handled is True
        assert outbound.frames == [{
            "type": "res", "id": "h2", "ok": True, "payload": {"source": "plugin"},
        }]
    finally:
        await runtime.shutdown()


async def test_http_dispatch_forward_only_trusts_verified_identity(tmp_path):
    runtime = await _runtime(tmp_path)

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(
            route="forward", params={"session_id": "s1", "rewritten": True},
            user_id="alice", identity_source="verified",
        )

    runtime.publish(_package(handler))
    try:
        handled, params, verified = await runtime.intercept_http(
            method="chat.send", params={"session_id": "s1"},
        )

        assert handled is False
        assert params == {"session_id": "s1", "rewritten": True}
        assert verified == "alice"
    finally:
        await runtime.shutdown()


async def test_http_dispatch_ignores_unverified_identity(tmp_path):
    runtime = await _runtime(tmp_path)

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(route="forward", user_id="alice", identity_source="client_trust")

    runtime.publish(_package(handler))
    try:
        _, _, verified = await runtime.intercept_http(method="chat.send", params={})

        assert verified is None
    finally:
        await runtime.shutdown()


async def test_http_dispatch_local_route_falls_back_to_native(tmp_path):
    runtime = await _runtime(tmp_path)
    outbound = FakeOutbound()

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(handled=True, route="local")

    runtime.publish(_package(handler))
    try:
        handled, params, _ = await runtime.intercept_http(
            method="demo.echo", params={"v": 1}, outbound=outbound,
        )

        assert handled is False
        assert params == {"v": 1}
        assert outbound.frames == []
    finally:
        await runtime.shutdown()


async def test_http_dispatch_broken_chain_falls_back(tmp_path):
    runtime = await _runtime(tmp_path)
    outbound = FakeOutbound()

    async def handler(event, ops, next_handler):
        raise RuntimeError("boom")

    runtime.publish(_package(handler))
    try:
        handled, params, verified = await runtime.intercept_http(
            method="chat.send", params={"a": 1}, outbound=outbound,
        )

        assert handled is False
        assert params == {"a": 1}
        assert verified is None
        assert outbound.frames == []
    finally:
        await runtime.shutdown()


async def test_http_dispatch_deny_without_outbound_fails_closed(tmp_path):
    from agent_gateway.extensions_v2.runtime import GatewayHttpDenyInjectionError

    runtime = await _runtime(tmp_path)

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(
            handled=True, route="deny", error={"code": "AUTH_REQUIRED", "message": "login first"},
        )

    runtime.publish(_package(handler))
    try:
        with pytest.raises(GatewayHttpDenyInjectionError):
            await runtime.intercept_http(
                method="chat.send", params={"x": 1}, request_id="h1",
            )
    finally:
        await runtime.shutdown()


async def test_http_dispatch_consume_without_outbound_falls_back_to_native(tmp_path):
    runtime = await _runtime(tmp_path)

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(handled=True, route="consume", response={"ok": True})

    runtime.publish(_package(handler))
    try:
        handled, params, _ = await runtime.intercept_http(
            method="chat.send", params={"x": 1}, request_id="h1",
        )

        assert handled is False
        assert params == {"x": 1}
    finally:
        await runtime.shutdown()


async def test_http_dispatch_deny_fails_closed_when_injection_fails(tmp_path):
    from agent_gateway.extensions_v2.runtime import GatewayHttpDenyInjectionError

    runtime = await _runtime(tmp_path)

    class BrokenOutbound:
        def accept_frame(self, frame: dict) -> None:
            raise RuntimeError("outbound closed")

    async def handler(event, ops, next_handler):
        return GatewayRequestOutput(
            handled=True, route="deny", error={"code": "AUTH_REQUIRED", "message": "login first"},
        )

    runtime.publish(_package(handler))
    try:
        with pytest.raises(GatewayHttpDenyInjectionError):
            await runtime.intercept_http(
                method="chat.send", params={"x": 1}, request_id="h1", outbound=BrokenOutbound(),
            )
    finally:
        await runtime.shutdown()


async def test_http_dispatch_entry_propagates_deny_injection_failure(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.extensions_v2 import assembly
    from agent_gateway.extensions_v2.runtime import GatewayHttpDenyInjectionError

    async def intercept_http(**_: object) -> tuple[bool, dict, None]:
        raise GatewayHttpDenyInjectionError("cannot answer denied request")

    runtime = SimpleNamespace(intercept_http=intercept_http)
    monkeypatch.setattr(assembly, "get_gateway_runtime", lambda: runtime)

    with pytest.raises(GatewayHttpDenyInjectionError):
        await extensions_v2.intercept_gateway_http(
            method="chat.send", params={"a": 1}, request_id="h1",
        )


async def test_http_dispatch_entry_assembles_the_runtime_on_demand(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.extensions_v2 import assembly

    runtime = SimpleNamespace(intercept_http=AsyncMock(return_value=(False, {"a": 1}, None)))
    init = AsyncMock(return_value=runtime)
    monkeypatch.setattr(extensions_v2, "init_gateway_extensions", init)
    monkeypatch.setattr(assembly, "get_gateway_runtime", lambda: None)

    handled, params, verified = await extensions_v2.intercept_gateway_http(
        channel=SimpleNamespace(), method="chat.send", params={"a": 1},
    )

    init.assert_awaited_once()
    assert (handled, params, verified) == (False, {"a": 1}, None)


async def test_http_dispatch_entry_uses_channel_lifecycle(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.extensions_v2 import assembly

    runtime = SimpleNamespace(intercept_http=AsyncMock(return_value=(True, {"a": 1}, None)))
    started = {"value": False}

    async def start_extensions() -> None:
        started["value"] = True

    channel = SimpleNamespace(start_extensions=start_extensions)
    monkeypatch.setattr(
        assembly, "get_gateway_runtime", lambda: runtime if started["value"] else None,
    )

    handled, params, verified = await extensions_v2.intercept_gateway_http(
        channel=channel, method="chat.send", params={"a": 1},
    )

    assert started["value"] is True
    assert (handled, params, verified) == (True, {"a": 1}, None)


# ── P3: HTTP pre-route ─────────────────────────────────────────────────────


def test_http_pre_route_marks_the_entry():
    observed = []

    class Browser:
        enabled = True

    class Bus:
        async def trigger(self, event, session):
            observed.append(event.event_input)
            return GatewayRequestOutput(route="forward")

    runtime = SimpleNamespace(browser=Browser(), bus=Bus())
    app = FastAPI()

    @app.get("/ping")
    def ping():
        return {"ok": True}

    attach_browser_routes(app, runtime_provider=lambda: runtime)
    response = TestClient(app).get("/ping")

    assert response.status_code == 200
    assert len(observed) == 1
    assert observed[0].entry == "http_pre_route"
    assert observed[0].transport == "http"
    assert observed[0].path == "/ping"


def test_http_pre_route_keeps_user_id_out_of_routing(monkeypatch):
    from agent_gateway.channel_manager.web import web_http_dispatch as dispatch

    monkeypatch.setattr(
        dispatch,
        "resolve_http_routing_identity",
        lambda headers, host: {
            "user_id": "alice",
            "group_id": "g1",
            "bot_id": "b1",
            "gateway_id": "gw1",
        },
    )
    observed = []

    class Browser:
        enabled = True

    class Bus:
        async def trigger(self, event, session):
            observed.append(event.event_input)
            return GatewayRequestOutput(route="forward")

    app = FastAPI()

    @app.get("/ping")
    def ping():
        return {"ok": True}

    attach_browser_routes(app, runtime_provider=lambda: SimpleNamespace(browser=Browser(), bus=Bus()))
    response = TestClient(app).get("/ping")

    assert response.status_code == 200
    assert observed[0].user_id == "alice"
    assert dict(observed[0].routing) == {
        "group_id": "g1",
        "bot_id": "b1",
        "gateway_id": "gw1",
    }


# ── P1 merge with the V1 hook ──────────────────────────────────────────────


class _RecordingWebSocket:
    def __init__(self) -> None:
        self.headers = {"authorization": "Bearer tk_1"}
        self.query_params = {"token": "tk_1"}
        self.url = SimpleNamespace(path="/ws", query="token=tk_1")
        self.client = SimpleNamespace(host="127.0.0.1", port=50000)
        self.denials: list = []

    async def send_denial_response(self, response) -> None:
        self.denials.append(response)


async def test_handshake_v2_deny_skips_the_v1_hook(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def deny(channel, websocket, path, remote):
        return GatewayHandshakeOutput(handled=True, allow=False)

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", deny)
    channel = SimpleNamespace(authenticate_handshake=AsyncMock())
    websocket = _RecordingWebSocket()

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(channel, websocket)

    assert denied is True
    assert authenticated is None and verified is None
    assert len(websocket.denials) == 1
    channel.authenticate_handshake.assert_not_awaited()


async def test_handshake_v2_allow_keeps_v1_as_a_second_gate_and_wins_identity(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def allow(channel, websocket, path, remote):
        return GatewayHandshakeOutput(
            handled=True, allow=True, user_id="alice", bind_connection_identity=True,
        )

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", allow)
    channel = SimpleNamespace(
        authenticate_handshake=AsyncMock(
            return_value=SimpleNamespace(success=True, user_id="legacy", extensions={"connection_user_id": "declared"}),
        ),
    )
    websocket = _RecordingWebSocket()

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(channel, websocket)

    assert denied is False
    assert authenticated == "alice"
    assert verified == "alice"
    channel.authenticate_handshake.assert_awaited_once()


async def test_handshake_v2_allow_still_obeys_a_v1_denial(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def allow(channel, websocket, path, remote):
        return GatewayHandshakeOutput(handled=True, allow=True, user_id="alice")

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", allow)
    channel = SimpleNamespace(
        authenticate_handshake=AsyncMock(return_value=SimpleNamespace(success=False, user_id=None)),
    )
    websocket = _RecordingWebSocket()

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(channel, websocket)

    assert denied is True
    assert authenticated is None and verified is None
    assert len(websocket.denials) == 1


async def test_handshake_without_v2_keeps_the_v1_identity(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def silent(channel, websocket, path, remote):
        return None

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", silent)
    channel = SimpleNamespace(
        authenticate_handshake=AsyncMock(
            return_value=SimpleNamespace(success=True, user_id="legacy", extensions={"connection_user_id": "declared"}),
        ),
    )

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(
        channel, _RecordingWebSocket(),
    )

    assert denied is False
    assert authenticated == "legacy"
    assert verified == "declared"


async def test_handshake_unbound_v2_user_is_not_a_connection_identity(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def allow(channel, websocket, path, remote):
        return GatewayHandshakeOutput(handled=True, allow=True, user_id="alice")

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", allow)
    channel = SimpleNamespace(authenticate_handshake=AsyncMock(return_value=None))

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(
        channel, _RecordingWebSocket(),
    )

    assert denied is False
    assert authenticated is None and verified is None


async def test_handshake_unbound_v2_user_keeps_the_v1_identity(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def allow(channel, websocket, path, remote):
        return GatewayHandshakeOutput(handled=True, allow=True, user_id="alice")

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", allow)
    channel = SimpleNamespace(
        authenticate_handshake=AsyncMock(
            return_value=SimpleNamespace(success=True, user_id="legacy", extensions={"connection_user_id": "declared"}),
        ),
    )

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(
        channel, _RecordingWebSocket(),
    )

    assert denied is False
    assert authenticated == "legacy"
    assert verified == "declared"


async def test_handshake_bound_v2_user_is_bound_without_a_v1_hook(monkeypatch):
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def allow(channel, websocket, path, remote):
        return GatewayHandshakeOutput(
            handled=True, allow=True, user_id="alice", bind_connection_identity=True,
        )

    monkeypatch.setattr(app_module, "_gateway_handshake_decision", allow)
    channel = SimpleNamespace(authenticate_handshake=AsyncMock(return_value=None))

    denied, authenticated, verified = await app_module._reject_unauthorized_handshake(
        channel, _RecordingWebSocket(),
    )

    assert denied is False
    assert authenticated == "alice"
    assert verified == "alice"


async def test_gateway_handshake_decision_passes_request_facts(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    captured = {}

    async def fake(channel, **kwargs):
        captured.update(kwargs)
        return GatewayHandshakeOutput(handled=False)

    monkeypatch.setattr(extensions_v2, "authenticate_gateway_handshake", fake)
    channel = SimpleNamespace(start_extensions=AsyncMock())

    decision = await app_module._gateway_handshake_decision(
        channel, _RecordingWebSocket(), "/ws", "client.example:53000",
    )

    assert decision is not None and decision.handled is False
    assert captured["path"] == "/ws"
    assert captured["query"] == {"token": "tk_1"}
    assert captured["remote_addr"] == "client.example:53000"
    assert captured["transport"] == "websocket"
    channel.start_extensions.assert_awaited_once()


async def test_gateway_handshake_decision_falls_back_on_error(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    async def broken(channel, **kwargs):
        raise RuntimeError("bus unavailable")

    monkeypatch.setattr(extensions_v2, "authenticate_gateway_handshake", broken)
    channel = SimpleNamespace(start_extensions=AsyncMock())

    decision = await app_module._gateway_handshake_decision(
        channel, _RecordingWebSocket(), "/ws", "client.example:53000",
    )

    assert decision is None


async def test_gateway_handshake_decision_rejects_duplicated_identity_headers(monkeypatch):
    from agent_gateway import extensions_v2
    from agent_gateway.channel_manager.web import web_channel_app as app_module

    authenticate = AsyncMock(return_value=GatewayHandshakeOutput(handled=False))
    monkeypatch.setattr(extensions_v2, "authenticate_gateway_handshake", authenticate)

    class _DuplicateHeaders:
        def items(self):
            return [
                ("Authorization", "Bearer token-a"),
                ("authorization", "Bearer token-b"),
            ]

    websocket = _RecordingWebSocket()
    websocket.headers = _DuplicateHeaders()

    decision = await app_module._gateway_handshake_decision(
        SimpleNamespace(), websocket, "/ws", "client.example:53000",
    )

    assert decision is not None and decision.handled is True and decision.allow is False
    assert decision.error["code"] == "DUPLICATE_HEADER"
    authenticate.assert_not_awaited()
