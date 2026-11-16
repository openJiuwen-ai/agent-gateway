# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise A4P through the raw WS entry point, including route registration."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from openjiuwen_protocol.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.app_web_handlers import (
    WebHandlersBindParams, _register_web_handlers,
)
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.routing.keys import AgentRef, RoutingKey


class Socket:
    remote_address = ("127.0.0.1", 12345)
    _web_routing = {"user_id": "alice"}
    _web_connection_user_id = "alice"

    async def send(self, data):
        pass


def route(mode, user="alice"):
    return RoutingKey(user_id=user, channel_id="web", app_id="default",
                      session_id="s", agent_ref=AgentRef(mode=mode, id="coder"))


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["complete", "reject", "reprepare"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("reconnect", [False, True])
async def test_approval_preserves_request_identity(method, reverse, reconnect):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    ws = Socket()
    code, work = route("agent.code.normal"), route("agent")
    routes = [code, work] if not reverse else [work, code]
    try:
        for item in routes:
            await channel.register_ws(ws, item)
        await channel.register_ws(ws, routes[0])  # repeated registration cannot choose identity
        await channel.send(Message(
            id="event", type="event", channel_id="web", session_id="s",
            params={}, timestamp=0, ok=True, app_id="default",
            agent_ref=code.agent_ref, metadata={"user_id": "alice"},
            payload={"event_type": "a4p.authorization_request", "requestId": "approval"},
        ))
        if reconnect:
            await channel.unregister_ws(ws)
            ws = Socket()
            for item in routes:
                await channel.register_ws(ws, item)
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": f"a4p.authorization.{method}",
            "params": {"session_id": "s", "requestId": "approval"},
        }), {})
        envelope = client.send_request.call_args.args[0]
        assert envelope.user_id == "alice"
        assert envelope.channel_context["agent_ref"] == {"mode": "agent.code.normal", "id": "coder"}
        assert set(await channel.routing_keys_for_ws(ws)) == set(routes)
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["complete", "pending", "reject"])
async def test_unknown_request_cannot_guess_between_routes(method):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock())
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    ws = Socket()
    try:
        for item in [route("agent"), route("agent.code.normal")]:
            await channel.register_ws(ws, item)
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": f"a4p.authorization.{method}",
            "params": {"session_id": "s", "requestId": "unknown"},
        }), {})
        client.send_request.assert_not_called()
        assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_ROUTE_AMBIGUOUS"
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
async def test_single_code_route_without_event_is_not_polluted():
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    ws = Socket()
    code = route("agent.code.normal")
    try:
        await channel.register_ws(ws, code)
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": "a4p.authorization.complete",
            "params": {"session_id": "s", "requestId": "approval"},
        }), {})
        assert client.send_request.call_args.args[0].channel_context["agent_ref"] == {
            "mode": "agent.code.normal", "id": "coder"}
        assert await channel.routing_keys_for_ws(ws) == [code]
    finally:
        await channel.unregister_ws(ws)


def test_request_hint_is_scoped_to_user_app_and_session():
    from dataclasses import replace

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    code = route("agent.code.normal")
    channel._remember_a4p_route(Message(
        id="event", type="event", channel_id="web", session_id="s", params={},
        timestamp=0, ok=True, app_id="default", agent_ref=code.agent_ref,
        metadata={"user_id": "alice"},
        payload={"event_type": "a4p.authorization_request", "requestId": "approval"},
    ))
    assert channel.a4p_request_route([route("agent")], "approval") == code
    for other in [replace(code, user_id="bob"), replace(code, app_id="other"),
                  replace(code, session_id="other")]:
        assert channel.a4p_request_route([other], "approval") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["complete", "pending"])
@pytest.mark.parametrize("user,app,session,allowed", [
    ("alice", "default", "s", True), ("bob", "default", "s", False),
    ("alice", "other", "s", False), ("alice", "default", "other", False),
    (None, "default", "s", False),
])
async def test_fresh_connection_can_complete_only_its_cached_request(user, app, session, allowed, method):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    old = Socket()
    await channel.register_ws(old, route("agent.code.normal"))
    channel._remember_a4p_route(Message(
        id="event", type="event", channel_id="web", session_id="s", params={},
        timestamp=0, ok=True, app_id="default", agent_ref=route("agent.code.normal").agent_ref,
        metadata={"user_id": "alice"},
        payload={"event_type": "a4p.authorization_request", "requestId": "approval"},
    ))
    await channel.unregister_ws(old)
    ws = Socket()
    ws._web_connection_user_id = user
    ws._jiuwen_app_id = app
    # The handshake has a physical connection, but no route for the requested SID.
    from dataclasses import replace
    await channel.register_ws(ws, replace(route("agent", user=user or "anonymous"),
                                         app_id=app, session_id="handshake"))
    try:
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": f"a4p.authorization.{method}",
            "params": {"session_id": session, **({"requestId": "approval"} if method == "complete" else {})},
        }), {})
        if allowed:
            assert client.send_request.call_args.args[0].channel_context["agent_ref"]["mode"] == "agent.code.normal"
        else:
            client.send_request.assert_not_called()
            assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_SESSION_REQUIRED"
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("multiple,explicit_mode,expected", [
    (False, False, True), (True, False, False), (True, True, True),
])
async def test_chunk_missing_identity_uses_only_unique_source_route(multiple, explicit_mode, expected, caplog):
    import logging
    from agent_gateway.message_handler.message_handler import MessageHandler

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    ws = Socket()
    code = route("agent.code.normal")
    await channel.register_ws(ws, code)
    try:
        if multiple:
            await channel.register_ws(ws, route("agent"))
        chunk = SimpleNamespace(
            request_id="event", channel_id="web", metadata={}, is_complete=False,
            agent_ref={"mode": "agent.code.normal"} if explicit_mode else None,
            payload={"event_type": "a4p.authorization_request", "requestId": "approval"},
        )
        msg = MessageHandler._chunk_to_message(
            chunk, "s", metadata={"ws_id": ws._jiuwen_ws_id},
        )
        with caplog.at_level(logging.DEBUG):
            channel._remember_a4p_route(msg)
        result = channel.a4p_request_route([code], "approval")
        assert result == (code if expected else None)
        if not expected:
            assert "source candidates=2" in caplog.text
    finally:
        await channel.unregister_ws(ws)


def test_missing_identity_without_source_is_logged(caplog):
    import logging

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    with caplog.at_level(logging.DEBUG):
        channel._remember_a4p_route(Message(
            id="event", type="event", channel_id="web", session_id="s", params={},
            timestamp=0, ok=True,
            payload={"event_type": "a4p.authorization_request", "requestId": "approval"},
        ))
    assert not channel._a4p_request_routes
    assert "route hint skipped requestId=approval session=s: missing=" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["pending", "complete", "reject", "reprepare"])
@pytest.mark.parametrize("mode", ["agent.code.normal", "agent"])
@pytest.mark.parametrize("local,found", [(True, True), (True, False), (False, True)])
async def test_anonymous_refresh_restores_only_local_host_session(monkeypatch, method, mode, local, found):
    from agent_gateway.routing import e2a_proxy

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    fetch = AsyncMock(return_value=(found, {"mode": mode} if found else {}))
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", fetch)
    monkeypatch.setattr(e2a_proxy, "is_legacy_shared_directory_client", lambda ac: local)
    old = Socket()
    old._web_connection_user_id = None
    await channel.register_ws(old, route(mode, user=str(old.remote_address)))
    await channel.unregister_ws(old)
    ws = Socket()
    ws._web_connection_user_id = None
    ws._web_routing = {}
    ws.remote_address = ("127.0.0.1", 54321)
    ws._jiuwen_app_id = "default"
    # Refresh registers a new handshake session, with no target-session route.
    from dataclasses import replace
    await channel.register_ws(ws, replace(route("agent", user=str(ws.remote_address)),
                                         session_id="handshake"))
    try:
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": f"a4p.authorization.{method}",
            "params": {"session_id": "s", "mode": "untrusted", "requestId": "approval"},
        }), {})
        if local and found:
            assert client.send_request.call_args.args[0].channel_context["agent_ref"] == {
                "mode": mode, "id": "default"}
        else:
            client.send_request.assert_not_called()
            assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_SESSION_REQUIRED"
        assert fetch.await_count == int(local)
    finally:
        await channel.unregister_ws(ws)
