# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import json
from types import SimpleNamespace

from agent_gateway.connection_registry import ConnectionRegistry
from agent_gateway.inbound_request_dispatcher import InboundRequestDispatcher
from agent_gateway.route_config import LocalHandlerContext, RouteConfig


class FakeWs:
    def __init__(self, *, closed=False):
        self.closed = closed
        self._gateway_user_id = "user-1"
        self._gateway_agent_type = "web-agent"
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


class FakeServer:
    def __init__(self):
        self.acp_calls = []
        self._acp_bridge = SimpleNamespace(
            handle_jsonrpc_request=self._handle_jsonrpc,
            is_jsonrpc_request=lambda data: data.get("jsonrpc") == "2.0",
        )
        self._connections = ConnectionRegistry(self._client_route_key)
        self._on_message_cb = None
        self.sent_responses = []

    @staticmethod
    def _client_route_key(channel_id, scoped_id, agent_ref=None):
        if not channel_id or not scoped_id:
            return None
        if agent_ref is not None:
            return (channel_id, scoped_id, str(agent_ref))
        return (channel_id, scoped_id)

    @staticmethod
    def _connection_user_id(ws):
        return getattr(ws, "_gateway_user_id", None)

    @staticmethod
    def _connection_agent_type(ws):
        return str(getattr(ws, "_gateway_agent_type", None) or "jiuwenswarm")

    @staticmethod
    async def _invoke_local_handler(handler, ctx):
        return await handler(ctx)

    async def send_response(self, ws, req_id, *, ok, error, code=None):
        payload = {"type": "res", "id": req_id, "ok": ok, "error": error}
        if code is not None:
            payload["code"] = code
        self.sent_responses.append(payload)
        await ws.send(json.dumps(payload, ensure_ascii=False))

    async def _handle_jsonrpc(self, ws, data):
        self.acp_calls.append((ws, data))
        return False


def build_route(*, channel_id="web"):
    return RouteConfig(path="/cli", channel_id=channel_id)


async def test_invalid_json_is_rejected_without_dispatch():
    ws = FakeWs()
    dispatcher = InboundRequestDispatcher(FakeServer())

    await dispatcher.handle_raw_message(ws, "{bad", "/cli", build_route())

    assert ws.sent == [
        '{"type": "res", "id": "", "ok": false, "error": "invalid json"}'
    ]


async def test_invalid_request_is_rejected():
    ws = FakeWs()
    dispatcher = InboundRequestDispatcher(FakeServer())

    await dispatcher.handle_raw_message(ws, "{}", "/cli", build_route())

    assert ws.sent == [
        '{"type": "res", "id": "", "ok": false, "error": "invalid request"}'
    ]


async def test_acp_bridge_can_short_circuit_dispatch():
    ws = FakeWs()
    server = FakeServer()
    server.acp_calls = []

    async def handle_jsonrpc_request(_ws, _data):
        server.acp_calls.append((_ws, _data))
        return True

    server._acp_bridge.handle_jsonrpc_request = handle_jsonrpc_request
    dispatcher = InboundRequestDispatcher(server)
    data = {"type": "req", "id": "req-1", "method": "chat.send", "params": {}}

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/acp", build_route(channel_id="acp"))

    assert server.acp_calls == [(ws, data)]
    assert ws.sent == []


async def test_acp_jsonrpc_request_does_not_fall_through_to_unknown_method():
    ws = FakeWs()
    server = FakeServer()
    server._acp_bridge.is_jsonrpc_request = lambda data: data.get("jsonrpc") == "2.0"
    dispatcher = InboundRequestDispatcher(server)
    data = {"jsonrpc": "2.0", "id": "req-1", "method": "chat.send", "params": {}}

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/acp", build_route(channel_id="acp"))

    assert ws.sent == []


async def test_inbound_interceptor_failure_does_not_stop_dispatch():
    ws = FakeWs()
    route = build_route()

    async def failing_interceptor(_ws, _data):
        raise RuntimeError("interceptor failed")

    route.inbound_interceptor = failing_interceptor
    dispatcher = InboundRequestDispatcher(FakeServer())
    data = {"type": "req", "id": "req-1", "method": "missing.method", "params": {}}

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert ws.sent == [
        '{"type": "res", "id": "req-1", "ok": false, "error": "unknown method: missing.method"}'
    ]


async def test_session_conflict_is_rejected_before_dispatch():
    ws = FakeWs()
    active_ws = FakeWs()
    server = FakeServer()
    server._connections.session_to_client[("web", "sess-1")] = active_ws
    dispatcher = InboundRequestDispatcher(server)
    route = build_route()
    route.forward_methods = frozenset({"chat.send"})
    data = {
        "type": "req",
        "id": "req-1",
        "method": "chat.send",
        "params": {"session_id": "sess-1"},
    }

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert server.sent_responses == [
        {
            "type": "res",
            "id": "req-1",
            "ok": False,
            "error": "Session sess-1 is already active in another window. Close that window first.",
            "code": "SESSION_IN_USE",
        }
    ]
    assert ws.sent == [
        json.dumps(server.sent_responses[0], ensure_ascii=False)
    ]


async def test_forward_request_registers_routes_and_builds_message():
    ws = FakeWs()
    server = FakeServer()
    messages = []
    server._on_message_cb = messages.append
    dispatcher = InboundRequestDispatcher(server)
    route = build_route()
    route.forward_methods = frozenset({"chat.send"})
    data = {
        "type": "req",
        "id": "req-1",
        "method": "chat.send",
        "params": {"session_id": "sess-1", "query": "hello"},
    }

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert server._connections.request_to_client == {("web", "req-1"): ws}
    assert server._connections.session_to_client == {("web", "sess-1"): ws}
    assert len(messages) == 1
    msg = messages[0]
    assert msg.id == "req-1"
    assert msg.type == "req"
    assert msg.channel_id == "web"
    assert msg.session_id == "sess-1"
    assert msg.params["agent_type"] == "web-agent"
    assert msg.user_id == "user-1"


async def test_local_handler_receives_context():
    ws = FakeWs()
    server = FakeServer()
    contexts = []
    dispatcher = InboundRequestDispatcher(server)
    route = build_route()

    async def handler(ctx):
        contexts.append(ctx)

    route.local_handlers = {"session.ping": handler}
    data = {
        "type": "req",
        "id": "req-1",
        "method": "session.ping",
        "params": {"session_id": "sess-1"},
    }

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert len(contexts) == 1
    ctx = contexts[0]
    assert isinstance(ctx, LocalHandlerContext)
    assert ctx.ws is ws
    assert ctx.req_id == "req-1"
    assert ctx.params == {"session_id": "sess-1"}
    assert ctx.session_id == "sess-1"
    assert ctx.user_id == "user-1"


async def test_local_handler_error_sends_internal_error():
    ws = FakeWs()
    server = FakeServer()
    dispatcher = InboundRequestDispatcher(server)
    route = build_route()

    async def handler(_ctx):
        raise RuntimeError("handler failed")

    route.local_handlers = {"ping": handler}
    data = {"type": "req", "id": "req-1", "method": "ping", "params": {}}

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert server.sent_responses == [
        {
            "type": "res",
            "id": "req-1",
            "ok": False,
            "error": "handler error: handler failed",
            "code": "INTERNAL_ERROR",
        }
    ]
    assert ws.sent == [
        json.dumps(server.sent_responses[0], ensure_ascii=False)
    ]


async def test_closed_ws_local_handler_error_is_not_answered():
    ws = FakeWs(closed=True)
    server = FakeServer()
    dispatcher = InboundRequestDispatcher(server)
    route = build_route()

    async def handler(_ctx):
        raise RuntimeError("handler failed")

    route.local_handlers = {"ping": handler}
    data = {"type": "req", "id": "req-1", "method": "ping", "params": {}}

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", route)

    assert ws.sent == []
    assert server.sent_responses == []


async def test_unknown_method_is_rejected():
    ws = FakeWs()
    dispatcher = InboundRequestDispatcher(FakeServer())
    data = {
        "type": "req",
        "id": "req-1",
        "method": "missing.method",
        "params": {},
    }

    await dispatcher.handle_raw_message(ws, json.dumps(data), "/cli", build_route())

    assert ws.sent == [
        '{"type": "res", "id": "req-1", "ok": false, "error": "unknown method: missing.method"}'
    ]
