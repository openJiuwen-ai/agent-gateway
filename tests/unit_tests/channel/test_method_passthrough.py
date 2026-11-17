"""Unknown methods traverse real Web WS/HTTP dispatch without a vocabulary."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from e2a.gateway_normalize import message_to_e2a
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.channel_manager.web.web_http_dispatch import dispatch_http_request
from agent_gateway.common.protocol_ids import InvalidProtocolId


class Peer:
    remote_address = ("127.0.0.1", 12345)
    closed = False

    def __init__(self):
        self.frames = []

    async def send(self, raw):
        self.frames.append(json.loads(raw))


@pytest.fixture
def channel():
    return WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())


@pytest.mark.asyncio
@pytest.mark.parametrize("method", [
    "future.capability.execute", "client.attach", "mcp_app.call_tool",
    "command.btw", "skills.future.operation",
    "swarmflow.pause", "swarmflow.resume", "swarmflow.stop",
])
@pytest.mark.parametrize("stream", [False, True])
async def test_unregistered_ws_method_preserves_envelope(channel, method, stream):
    peer = Peer()
    seen = []

    async def upstream(message):
        seen.append(message_to_e2a(message))
        return True

    channel.on_message(upstream)
    params = {
        "session_id": "sess-new-method", "nested": {"content": [1, False, None]},
    }
    if method in {"swarmflow.pause", "swarmflow.resume", "swarmflow.stop"}:
        params["run_id"] = "run-1"
    try:
        await channel._handle_raw_message(peer, json.dumps({
            "type": "req", "id": "new-method", "method": method,
            "params": params, "is_stream": stream,
        }), {})
        assert len(seen) == 1
        envelope = seen[0]
        assert type(envelope.method) is str
        assert envelope.method == method
        assert envelope.request_id == "new-method"
        assert envelope.params == params
        assert envelope.is_stream is stream
        assert not peer.frames
    finally:
        await channel.unregister_ws(peer)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["updater.check", "channel.feishu.bind_agent", "plugin.local.action"])
async def test_registered_ws_method_is_local_and_can_be_unregistered(channel, method):
    peer = Peer()
    local = AsyncMock()
    upstream = AsyncMock(return_value=True)
    channel.register_method(method, local)
    channel.on_message(upstream)
    raw = json.dumps({"type": "req", "id": "local", "method": method, "params": {}})
    await channel._handle_raw_message(peer, raw, {})
    local.assert_awaited_once()
    upstream.assert_not_awaited()
    channel.unregister_method(method)
    await channel._handle_raw_message(peer, raw, {})
    upstream.assert_awaited_once()
    assert not peer.frames


@pytest.mark.asyncio
async def test_ws_method_is_stripped_before_dispatch(channel):
    peer = Peer()
    seen = []

    async def upstream(message):
        seen.append(message)
        return True

    channel.on_message(upstream)
    await channel._handle_raw_message(peer, json.dumps({
        "type": "req", "id": "strip", "method": " future.execute ",
        "params": {},
    }), {})

    assert seen[0].req_method == "future.execute"
    assert not peer.frames
    await channel.unregister_ws(peer)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["chat.send", "chat.interrupt", "history.get"])
async def test_ack_and_forward_each_run_once(channel, method):
    peer = Peer()
    order = []

    async def ack(ws, req_id, params, session_id):
        order.append("ack")

    async def upstream(message):
        order.append("forward")
        return True

    channel.register_method(method, ack)
    channel.on_message(upstream)
    await channel._handle_raw_message(peer, json.dumps({
        "type": "req", "id": "ack", "method": method, "params": {},
    }), {})
    assert order == (["ack", "forward"] if method == "chat.send" else ["forward", "ack"])


@pytest.mark.asyncio
async def test_http_future_method_is_forwarded(channel, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    outbound, _, _ = await dispatch_http_request(
        channel, method="skills.future.operation", params={"input": [1, 2]},
        request_id="http-future",
    )
    try:
        message = upstream.await_args.args[0]
        assert message.req_method == "skills.future.operation"
        assert message.params == {"input": [1, 2]}
    finally:
        await channel.unregister_request_outbound(outbound)
        await outbound.close()


@pytest.mark.asyncio
async def test_http_method_is_stripped_before_dispatch(channel):
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    outbound, _, _ = await dispatch_http_request(
        channel, method=" future.http.execute ", params={}, request_id="http-strip",
    )
    try:
        assert upstream.await_args.args[0].req_method == "future.http.execute"
    finally:
        await channel.unregister_request_outbound(outbound)
        await outbound.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"run_id": "bad\nrun"},
        {"run_id": "x" * 257},
        {"run_id": "run-1", "workflow_run_id": "run-2"},
    ],
)
async def test_http_swarmflow_rejects_invalid_ids_before_dispatch(channel, params):
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)

    with pytest.raises(InvalidProtocolId):
        await dispatch_http_request(
            channel, method="swarmflow.stop", params=params, request_id="invalid-run",
        )

    upstream.assert_not_awaited()


@pytest.mark.asyncio
async def test_enterprise_unknown_skill_write_is_rejected_before_forward(channel, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    outbound, _, _ = await dispatch_http_request(
        channel, method="skills.future.operation", params={}, request_id="enterprise-guard",
    )
    try:
        upstream.assert_not_awaited()
        response = await asyncio.wait_for(
            outbound.wait_response("enterprise-guard", timeout=1), timeout=1,
        )
        assert response["code"] == "FORBIDDEN"
    finally:
        await channel.unregister_request_outbound(outbound)
        await outbound.close()


@pytest.mark.parametrize("method", [
    "team.create",
    "team.get",
    "team.stop",
    "team.member.join",
    "team.member.leave",
    "team.members.list",
    "team.quota.get",
])
def test_personal_edition_blocks_enterprise_team_control_methods(
    method, monkeypatch,
):
    from agent_gateway.channel_manager.web.invoke import (
        is_enterprise_write_forbidden,
    )

    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    assert is_enterprise_write_forbidden(method)


def test_personal_edition_keeps_legacy_team_methods_available(monkeypatch):
    from agent_gateway.channel_manager.web.invoke import (
        is_enterprise_write_forbidden,
    )

    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    assert not is_enterprise_write_forbidden("team.list")
    assert not is_enterprise_write_forbidden("team.delete")


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["ws", "http"])
async def test_bus_forward_does_not_also_return_method_not_found(channel, transport):
    channel.send_response = AsyncMock()
    params = {"opaque": [1, False, None]}
    if transport == "ws":
        await channel._handle_raw_message(Peer(), json.dumps({
            "type": "req", "id": "bus-forward", "method": "future.bus.request",
            "params": params,
        }), {})
    else:
        outbound, _, _ = await dispatch_http_request(
            channel, method="future.bus.request", params=params,
            request_id="bus-forward",
        )
        await channel.unregister_request_outbound(outbound)
        await outbound.close()
    assert channel.bus.pending_incoming_count == 1
    message = await asyncio.wait_for(channel.bus.wait_for_user_message(), timeout=1)
    assert channel.bus.pending_incoming_count == 0
    assert message.req_method == "future.bus.request"
    assert message.params == params
    channel.send_response.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["ws", "http"])
async def test_unhandled_upstream_callback_returns_service_unavailable(channel, transport):
    channel.on_message(AsyncMock(return_value=False))
    channel.send_response = AsyncMock()
    if transport == "ws":
        await channel._handle_raw_message(Peer(), json.dumps({
            "type": "req", "id": "callback-rejected", "method": "future.callback.request",
            "params": {},
        }), {})
    else:
        outbound, _, _ = await dispatch_http_request(
            channel, method="future.callback.request", params={},
            request_id="callback-rejected",
        )
        await channel.unregister_request_outbound(outbound)
        await outbound.close()
    channel.send_response.assert_awaited_once()
    assert channel.send_response.await_args.kwargs["code"] == "SERVICE_UNAVAILABLE"


@pytest.mark.parametrize("stream", [False, True])
def test_public_http_rpc_forwards_future_methods(channel, stream, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes

    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    seen = []

    async def upstream(message):
        seen.append(message)
        outbound = channel.delivery.lookup(message.metadata["ws_id"])
        if message.is_stream:
            outbound.accept_frame({"type": "event", "event": "chat.final", "payload": {"content": "future result"}})
        else:
            await channel.send_response(outbound, message.id, ok=True, payload={"result": "future result"})
        return True

    channel.on_message(upstream)
    app = FastAPI()
    register_web_http_api_routes(app, channel, enterprise=False)
    params = {"input": {"opaque": [False, None, 42]}}
    with TestClient(app) as client:
        response = client.post("/api/v1/rpc", json={
            "method": "future.http.execute", "params": params, "is_stream": stream,
        }, headers={"X-Request-Id": "future-http"})
    assert response.status_code == 200
    assert "future result" in response.text
    assert len(seen) == 1
    assert type(seen[0].req_method) is str
    assert seen[0].req_method == "future.http.execute"
    assert seen[0].params == params
    assert seen[0].is_stream is stream
    assert not channel.delivery.request_outbounds


@pytest.mark.asyncio
async def test_http_registered_plugin_remains_local(channel):
    local = AsyncMock()
    upstream = AsyncMock(return_value=True)
    channel.register_method("plugin.local.action", local)
    channel.on_message(upstream)
    outbound, _, _ = await dispatch_http_request(channel, method="plugin.local.action", params={})
    try:
        local.assert_awaited_once()
        upstream.assert_not_awaited()
    finally:
        await channel.unregister_request_outbound(outbound)


def test_public_http_rpc_runs_registered_plugin_locally(channel, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes

    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    seen = []

    async def local(outbound, request_id, params, session_id):
        seen.append(params)
        await channel.send_response(outbound, request_id, ok=True, payload={"local": True})

    channel.register_method("plugin.local.action", local)
    app = FastAPI()
    register_web_http_api_routes(app, channel, enterprise=False)
    with TestClient(app) as client:
        response = client.post("/api/v1/rpc", json={
            "method": "plugin.local.action", "params": {"opaque": [None, False]},
        })
    assert response.status_code == 200
    assert response.json()["data"] == {"local": True}
    assert seen == [{"opaque": [None, False]}]
    upstream.assert_not_awaited()
    assert not channel.delivery.request_outbounds


@pytest.mark.parametrize("body,status", [
    ({}, 422),
    ({"method": 42}, 422),
    ({"method": ""}, 422),
    ({"method": "   "}, 400),
    ({"method": "future.execute", "params": []}, 422),
])
def test_public_http_rpc_rejects_invalid_envelopes(channel, body, status):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes

    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    app = FastAPI()
    register_web_http_api_routes(app, channel, enterprise=False)
    with TestClient(app) as client:
        response = client.post("/api/v1/rpc", json=body)
    assert response.status_code == status
    upstream.assert_not_awaited()
    assert not channel.delivery.request_outbounds


@pytest.mark.parametrize("stream", [False, True])
def test_public_http_rpc_rejects_invalid_swarmflow_run_id(channel, stream):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes

    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    app = FastAPI()
    register_web_http_api_routes(app, channel, enterprise=False)
    with TestClient(app) as client:
        response = client.post("/api/v1/rpc", json={
            "method": "swarmflow.stop",
            "params": {"run_id": "bad\nrun"},
            "is_stream": stream,
        })

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "BAD_REQUEST"
    upstream.assert_not_awaited()
    assert not channel.delivery.request_outbounds


@pytest.mark.asyncio
async def test_dispatch_without_callback_or_bus_returns_service_unavailable():
    from types import SimpleNamespace
    from e2a.message import Message
    from agent_gateway.channel_manager.web.invoke import dispatch_web_request

    unavailable = SimpleNamespace(rpc=SimpleNamespace(method_handlers={}, on_message_cb=None),
                                  send_response=AsyncMock())
    peer = Peer()
    message = Message(id="offline", type="req", channel_id="web", session_id="s1",
                      timestamp=0, ok=True, req_method="future.execute", params={})
    await dispatch_web_request(unavailable, method="future.execute", params={},
                               request_id="offline", outbound=peer, session_id="s1",
                               user_message=message)
    assert unavailable.send_response.await_args.kwargs["code"] == "SERVICE_UNAVAILABLE"


def test_normalization_preserves_unregistered_method_and_routing():
    from e2a.message import Message
    from agent_gateway.app_gateway import _normalize_gateway_message

    message = Message(
        id="opaque", type="req", channel_id="web", session_id="s1", timestamp=0, ok=True,
        req_method="future.capability.execute", is_stream=True,
        params={"content": {"raw": [1, False]}, "model": {"opaque": True}},
        user_id="u1", app_id="app1", chat_id="chat1", provider="web",
        agent_ref={"mode": "agent", "id": "worker1"}, metadata={"ws_id": "ws1"},
    )
    normalized = _normalize_gateway_message(message)
    assert normalized is message
    assert "query" not in normalized.params
    envelope = message_to_e2a(normalized)
    assert envelope.method == "future.capability.execute"
    assert envelope.params == message.params
    assert envelope.user_id == "u1"
    assert envelope.is_stream


def test_normalization_replaces_non_string_mode_before_forwarding():
    from e2a.message import Message, Mode
    from agent_gateway.app_gateway import _normalize_gateway_message

    raw_mode = [{"unexpected": True}]
    message = Message(
        id="mode", type="req", channel_id="web", session_id="s1", timestamp=0,
        ok=True, req_method="future.capability.execute", mode=Mode.CODE_NORMAL,
        params={"mode": raw_mode},
    )

    normalized = _normalize_gateway_message(message)

    expected_mode = Mode.from_raw(Mode.CODE_NORMAL).value
    assert normalized is not message
    assert normalized.params["mode"] == expected_mode
    assert message.params["mode"] is raw_mode
    assert message_to_e2a(normalized).params["mode"] == expected_mode


def test_chat_normalization_retains_all_message_fields():
    from e2a.message import Message
    from agent_gateway.app_gateway import _normalize_gateway_message

    message = Message(
        id="chat", type="req", channel_id="web", session_id="s1", timestamp=0, ok=True,
        req_method="chat.send", params={"content": "hello"},
        user_id="u1", app_id="app1", chat_id="chat1", provider="web",
        agent_ref={"mode": "agent", "id": "worker1"}, metadata={"ws_id": "ws1"},
    )
    normalized = _normalize_gateway_message(message)
    for field in ("user_id", "app_id", "chat_id", "provider", "agent_ref", "metadata"):
        assert getattr(normalized, field) == getattr(message, field)
    assert normalized.params["query"] == "hello"
    assert "query" not in message.params


@pytest.mark.asyncio
async def test_default_forward_keeps_opaque_payload(monkeypatch):
    from e2a.message import Message
    from types import SimpleNamespace
    from agent_gateway import app_gateway

    monkeypatch.setattr(app_gateway, "_refresh_expired_login_credential", AsyncMock())
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    params = {"project_id": "example", "content": {"raw": True}}
    message = Message(
        id="opaque", type="req", channel_id="web", session_id="s1", timestamp=0, ok=True,
        req_method="future.execute", params=dict(params),
    )
    await app_gateway._normalize_and_forward_message(message, manager)
    manager.deliver_to_message_handler.assert_awaited_once_with(message)
    assert message.params == params


@pytest.mark.asyncio
async def test_session_create_receives_normalized_work_mode(monkeypatch):
    from e2a.message import Message
    from types import SimpleNamespace
    from agent_gateway import app_gateway

    monkeypatch.setattr(app_gateway, "_refresh_expired_login_credential", AsyncMock())
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    message = Message(
        id="create", type="req", channel_id="web", session_id="s1", timestamp=0, ok=True,
        req_method="session.create",
        params={"project_id": "example", "content": {"raw": True}},
    )
    await app_gateway._normalize_and_forward_message(message, manager)
    forwarded = manager.deliver_to_message_handler.await_args.args[0]
    assert forwarded is message
    assert forwarded.params["project_id"] == "example"
    assert forwarded.params["project_dir"] == ""
    assert forwarded.params["work_mode"]
    assert forwarded.params["_work_mode_explicit"] is False
    assert "query" not in forwarded.params
