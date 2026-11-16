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
