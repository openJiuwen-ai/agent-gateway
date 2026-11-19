# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise A4P Web ingress and authorization route registration."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import Message

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
@pytest.mark.parametrize("legacy_session", [None, "new", "archived-session"])
@pytest.mark.parametrize("operation,config", [("get", {}), ("update", {"enabled": True})])
async def test_a4p_config_websocket_without_chat_route(operation, config, legacy_session):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={"enabled": True})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.routing_keys_for_ws = AsyncMock()
    callback = AsyncMock()
    channel.on_message(callback)
    ws = Socket()
    ws._jiuwen_app_id = "settings-app"
    params = {**config, "user_id": "untrusted", "bot_id": "untrusted"}
    if legacy_session:
        params["session_id"] = legacy_session
    method = f"a4p.config.{operation}"

    await channel._handle_raw_message(ws, json.dumps({
        "type": "req", "id": "rpc", "method": method, "params": params,
    }), {})

    callback.assert_not_called()
    channel.routing_keys_for_ws.assert_not_called()
    assert not channel._ws_sessions.get(id(ws))
    client.send_request.assert_awaited_once()
    envelope = client.send_request.call_args.args[0]
    assert envelope.method == method
    assert envelope.session_id is None
    assert envelope.user_id == "alice"
    assert envelope.channel_context["app_id"] == "settings-app"
    assert envelope.agent_ref is None
    assert envelope.params == config
    assert channel.send_response.call_args.kwargs["ok"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_session", [False, True])
async def test_a4p_config_http_without_chat_route(monkeypatch, legacy_session):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes
    from agent_gateway.extensions.runtime_management_extension.pod_rest_map import assemble_rest_request

    monkeypatch.setenv("GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS", "1")
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={"enabled": True})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.routing_keys_for_ws = AsyncMock()
    app = FastAPI()
    register_web_http_api_routes(app, channel, enterprise=True)
    headers = {
        "X-User-Id": "alice", "X-App-Id": "settings-app",
        "X-Group-Id": "group-1", "X-Bot-Id": "bot-1",
    }
    path = "/api/v1/a4p/config"
    body = {"enabled": True, "user_id": "untrusted", "bot_id": "untrusted"}
    if legacy_session:
        body["session_id"] = "new"
        headers["X-Session-Id"] = "new"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        read = await http.get(path + ("?session_id=new" if legacy_session else ""), headers=headers)
        write = await http.put(path, headers=headers, json=body)
        rejected = await http.put(path, headers=headers, json={
            "enabled": False, "app_id": "untrusted",
        })

    assert read.json()["data"] == write.json()["data"] == {"enabled": True}
    assert rejected.status_code == 400
    channel.routing_keys_for_ws.assert_not_called()
    assert client.send_request.await_count == 2
    for call, suffix, config in zip(
        client.send_request.await_args_list, ("get", "update"), ({}, {"enabled": True}),
    ):
        envelope = call.args[0]
        assert envelope.method == f"a4p.config.{suffix}"
        assert envelope.session_id is None
        assert envelope.user_id == "alice"
        assert envelope.channel_context["app_id"] == "settings-app"
        assert envelope.channel_context["routing"] == {"group_id": "group-1", "bot_id": "bot-1"}
        assert envelope.params == config
        rest = assemble_rest_request(envelope, base_url="http://agent")
        assert "X-Session-Id" not in rest.headers
        assert rest.headers["X-User-Id"] == "alice"
        assert rest.headers["X-Bot-Id"] == "bot-1"
        assert rest.query is None
        assert rest.json_body == (config or None)


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
            # A scoped metadata lookup is allowed, but no authorization RPC
            # may be sent using a different user's/app's cached identity.
            assert all(call.args[0].method == "session.get_metadata"
                       for call in client.send_request.call_args_list)
            assert channel.send_response.call_args.kwargs["code"] == (
                "A4P_SESSION_IDENTITY_UNAVAILABLE" if method == "pending" and not allowed
                else "A4P_WEB_SESSION_REQUIRED")
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
            # A scoped metadata lookup is allowed, but no authorization RPC
            # may be sent using a different user's/app's cached identity.
            assert all(call.args[0].method == "session.get_metadata"
                       for call in client.send_request.call_args_list)
            assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_SESSION_REQUIRED"
        assert fetch.await_count == int(local)
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
@pytest.mark.parametrize("lookup", ["code", "missing", "failed", "exception"])
async def test_pending_metadata_failure_never_uses_default_route(monkeypatch, lookup, ambiguous):
    from agent_gateway.routing import e2a_proxy
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    ws = SimpleNamespace(_web_connection_user_id=None, _jiuwen_app_id="default")
    # No remote_address attribute: HTTP-like adapters must also be supported.
    channel.routing_keys_for_ws = AsyncMock(return_value=[route("agent")])
    if ambiguous:
        channel._a4p_request_routes[("alice", "default", "s", "old")] = route("agent")
        channel._a4p_request_routes[("alice", "default", "s", "new")] = route("agent.code.normal")
    fetch = AsyncMock(return_value=(lookup != "failed", {
        "mode": "agent.code.normal"} if lookup == "code" else {}))
    if lookup == "exception":
        fetch.side_effect = TimeoutError()
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", fetch)
    monkeypatch.setattr(e2a_proxy, "is_legacy_shared_directory_client", lambda ac: True)
    await channel._method_handlers["a4p.authorization.pending"](ws, "rpc", {}, "s")
    if lookup != "code":
        client.send_request.assert_not_called()
        assert channel.send_response.call_args.kwargs["code"] == "A4P_SESSION_IDENTITY_UNAVAILABLE"
        return
    assert client.send_request.call_args.args[0].channel_context["agent_ref"]["mode"] == (
        "agent.code.normal" if lookup == "code" else "agent")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["complete", "reject", "reprepare"])
@pytest.mark.parametrize("success", [True, False])
async def test_request_cache_cleanup_requires_business_success(method, success):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(return_value=SimpleNamespace(
        ok=True, payload={"ok": success, "pending": {"requestId": "replacement"}})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    ws = Socket()
    code = route("agent.code.normal")
    key = (code.user_id, code.app_id, code.session_id, "approval")
    channel._a4p_request_routes[key] = code
    await channel.register_ws(ws, code)
    try:
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": f"a4p.authorization.{method}",
            "params": {"session_id": "s", "requestId": "approval"},
        }), {})
        assert (key in channel._a4p_request_routes) is not success
    finally:
        await channel.unregister_ws(ws)


def test_terminal_event_cleans_only_its_request_scope():
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    code = route("agent.code.normal")
    for user in ["alice", "bob"]:
        channel._a4p_request_routes[(user, "default", "s", "approval")] = route("agent.code.normal", user)
    channel._remember_a4p_route(Message(
        id="event", type="event", channel_id="web", session_id="s", params={},
        timestamp=0, ok=True, app_id="default", agent_ref=code.agent_ref,
        metadata={"user_id": "alice"},
        payload={"event_type": "a4p.authorization_terminated", "requestId": "approval"},
    ))
    assert ("alice", "default", "s", "approval") not in channel._a4p_request_routes
    assert ("bob", "default", "s", "approval") in channel._a4p_request_routes


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_routes", [[], ["agent", "agent.code.normal"]])
async def test_pending_restores_host_identity_without_request_hint(monkeypatch, existing_routes):
    from agent_gateway.routing import e2a_proxy

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={"pending": []})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    channel.on_message(lambda msg: None)
    ws = Socket()
    ws._jiuwen_app_id = "default"
    fetch = AsyncMock(return_value=(True, {
        "mode": "agent.code.normal", "work_mode": "code",
        "agent_ref": {"mode": "agent.code.normal", "id": "special-coder"},
    }))
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", fetch)
    monkeypatch.setattr(e2a_proxy, "is_legacy_shared_directory_client", lambda ac: False)
    try:
        for mode in existing_routes:
            await channel.register_ws(ws, route(mode))
        # Keep legacy registration: returning to S through session.switch adds
        # a default Work subscription. A4P must resolve identity independently.
        channel.register_method("session.switch", AsyncMock())
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "switch", "method": "session.switch",
            "params": {"session_id": "s"},
        }), {})
        subscriptions = set(await channel.routing_keys_for_ws(ws))
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "rpc", "method": "a4p.authorization.pending",
            "params": {"session_id": "s"},
        }), {})
        envelope = client.send_request.call_args.args[0]
        assert envelope.channel_context["agent_ref"] == {
            "mode": "agent.code.normal", "id": "special-coder"}
        assert set(await channel.routing_keys_for_ws(ws)) == subscriptions
        assert fetch.call_args.kwargs["user_id"] == "alice"
        assert fetch.call_args.kwargs["metadata"]["app_id"] == "default"
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.get_metadata", "history.get", "session.switch"])
async def test_reading_code_session_keeps_legacy_registration(method):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.on_message(lambda msg: None)
    channel.register_method(method, AsyncMock())
    ws = Socket()
    code = route("agent.code.normal")
    try:
        await channel.register_ws(ws, code)
        # Switch to another session, then read S again with SID only.
        from dataclasses import replace
        await channel.register_ws(ws, replace(route("agent"), session_id="other"))
        await channel._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "read", "method": method,
            "params": {"session_id": "s"},
        }), {})
        refs = {rk.agent_ref.mode for rk in await channel.routing_keys_for_ws(ws) if rk.session_id == "s"}
        assert refs == {"agent.code.normal", "agent"}
    finally:
        await channel.unregister_ws(ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("ids,host_id,expected", [
    ([], None, "default"), (["coder-a"], None, "coder-a"),
    (["coder-a", "coder-b"], None, None),
    (["coder-a", "coder-b"], "host-coder", "host-coder"),
])
@pytest.mark.parametrize("http", [False, True])
async def test_missing_agent_id_defaults_only_without_candidates(monkeypatch, ids, host_id, expected, http):
    from dataclasses import replace
    from agent_gateway.routing import e2a_proxy

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    ws = Socket()
    ws._jiuwen_app_id = "default"
    ws.is_http_outbound = http
    code = route("agent.code.normal")
    # Other users, apps and modes must not contribute to agent-ID ambiguity.
    candidates = [replace(code, agent_ref=AgentRef(mode=code.agent_ref.mode, id=value)) for value in ids]
    candidates += [replace(code, user_id="bob"), replace(code, app_id="other"), route("agent")]
    channel.routing_keys_for_ws = AsyncMock(return_value=candidates + candidates)
    meta = {"mode": "agent.code.normal"}
    if host_id:
        meta["agent_id"] = host_id
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", AsyncMock(return_value=(True, meta)))
    monkeypatch.setattr(e2a_proxy, "is_legacy_shared_directory_client", lambda ac: True)
    await channel._method_handlers["a4p.authorization.pending"](ws, "rpc", {}, "s", user_id="alice")
    if expected is None:
        client.send_request.assert_not_called()
        assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_ROUTE_AMBIGUOUS"
    else:
        assert client.send_request.call_args.args[0].channel_context["agent_ref"] == {
            "mode": "agent.code.normal", "id": expected}


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_ids,host_id,expected", [
    (["original-coder"], None, "original-coder"),
    (["coder-a", "coder-b"], None, None),
    ([], None, "default"),
    (["coder-a", "coder-b"], "host-coder", "host-coder"),
])
async def test_pending_uses_cached_mode_identity_before_default(monkeypatch, cached_ids, host_id, expected):
    from dataclasses import replace
    from agent_gateway.routing import e2a_proxy

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={})))
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    channel.send_response = AsyncMock()
    ws = Socket()
    ws._jiuwen_app_id = "default"
    # Current connection has no Code subscription. Multiple pending identities
    # force metadata recovery rather than the single cached-route shortcut.
    work = route("agent")
    channel.routing_keys_for_ws = AsyncMock(return_value=[work])
    cached = [work, route("team")]
    cached += [replace(route("agent.code.normal"), agent_ref=AgentRef(
        mode="agent.code.normal", id=agent_id)) for agent_id in cached_ids]
    for i, candidate in enumerate(cached):
        channel._a4p_request_routes[("alice", "default", "s", str(i))] = candidate
    meta = {"mode": "agent.code.normal"}
    if host_id:
        meta["agent_id"] = host_id
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", AsyncMock(return_value=(True, meta)))
    monkeypatch.setattr(e2a_proxy, "is_legacy_shared_directory_client", lambda ac: True)
    await channel._method_handlers["a4p.authorization.pending"](ws, "rpc", {}, "s", user_id="alice")
    if expected is None:
        client.send_request.assert_not_called()
        assert channel.send_response.call_args.kwargs["code"] == "A4P_WEB_ROUTE_AMBIGUOUS"
    else:
        assert client.send_request.call_args.args[0].channel_context["agent_ref"] == {
            "mode": "agent.code.normal", "id": expected}
