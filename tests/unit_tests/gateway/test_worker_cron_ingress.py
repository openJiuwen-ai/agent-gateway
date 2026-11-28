# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Owned Cron ingress: connection identity, project gate and TUI owner routing."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import EventType, Message

WEB_IDENTITY = "_web_connection_user_id"
TUI_IDENTITY = "_gateway_user_id"


def _bind(web, protocol):
    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

    web.register_method = lambda method, bound: web._method_handlers.__setitem__(method, bound)
    web.connection_user_id = staticmethod(
        lambda ws: getattr(ws, WEB_IDENTITY, None) or None
    )
    bind_owned_cron_handlers(web, protocol)
    return web


def _web(handler, method="cron.job.create"):
    return SimpleNamespace(_method_handlers={method: handler}, send_response=AsyncMock())


def _protocol(routes=None, *, send_response=None):
    return SimpleNamespace(
        config=SimpleNamespace(routes=routes or {}), send_response=send_response or AsyncMock(),
    )


async def test_cron_handlers_replace_payload_identity_with_connection_owner():
    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, user_id))

    route = SimpleNamespace(local_handlers={"cron.job.list": handler})
    web = _bind(_web(handler), _protocol({"/tui": route}))
    original = {"user_id": "bob", "group_id": "other", "bot_id": "other", "name": "test"}
    await web._method_handlers["cron.job.create"](
        SimpleNamespace(**{WEB_IDENTITY: "alice"}), "r1", original, None, user_id="bob")
    await route.local_handlers["cron.job.list"](
        SimpleNamespace(**{TUI_IDENTITY: "alice"}), "r2", original, None, user_id="bob")
    assert seen == [({"user_id": "alice", "group_id": "", "bot_id": "", "name": "test"}, "alice")] * 2
    # The client's dict is copied, never mutated.
    assert original == {"user_id": "bob", "group_id": "other", "bot_id": "other", "name": "test"}


@pytest.mark.parametrize("identity", ["", None, "default", "  "])
async def test_anonymous_or_default_cron_cannot_claim_a_payload_user(identity):
    handler = AsyncMock()
    for entry, attribute in (("web", WEB_IDENTITY), ("tui", TUI_IDENTITY)):
        protocol = _protocol()
        web = _bind(_web(handler, "cron.job.list"), protocol)
        if entry == "tui":
            protocol.config.routes["tui"] = SimpleNamespace(local_handlers={"cron.job.list": handler})
            from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

            bind_owned_cron_handlers(web, protocol)
            bound = protocol.config.routes["tui"].local_handlers["cron.job.list"]
        else:
            bound = web._method_handlers["cron.job.list"]
        ws = SimpleNamespace(**{attribute: identity})
        await bound(ws, "r1", {"user_id": "alice"}, None, user_id="alice")
        handler.assert_not_awaited()
        sink = web.send_response if entry == "web" else protocol.send_response
        assert sink.call_args.kwargs["ok"] is False
        assert sink.call_args.kwargs["code"] == "CRON_IDENTITY_REQUIRED"


@pytest.mark.parametrize("method,field,params", [
    ("cron.job.create", "params", {"project_id": "real-project"}),
    ("cron.job.create", "params", {"project_dir": "/tmp/project"}),
    ("cron.job.update", "params", {"id": "j1", "patch": {"project_id": "real-project"}}),
    ("cron.job.update", "params", {"id": "j1", "patch": {"project_dir": "/tmp/project"}}),
])
async def test_non_default_project_is_refused_before_the_legacy_handler(method, field, params):
    from agent_gateway.application.worker_cron_ingress import CRON_PANEL_METHODS, bind_owned_cron_handlers

    handler = AsyncMock()
    web = _web(handler, method)
    protocol = _protocol()
    web.register_method = lambda name, bound: web._method_handlers.__setitem__(name, bound)
    web.connection_user_id = staticmethod(lambda ws: getattr(ws, WEB_IDENTITY, None) or None)
    bind_owned_cron_handlers(web, protocol)
    original = dict(params)
    await web._method_handlers[method](SimpleNamespace(**{WEB_IDENTITY: "alice"}), "r1", params, None)
    handler.assert_not_awaited()
    assert web.send_response.call_args.kwargs["code"] == "CRON_PROJECT_DISABLED"
    assert params == original
    assert method in CRON_PANEL_METHODS


async def test_default_project_and_model_errors_reach_the_handler_and_are_sanitized():
    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, user_id))
        if params.get("name") == "login":
            raise ValueError("Unknown model 'x'. Available models: a, secret-alias")
        if params.get("name") == "down":
            raise RuntimeError("sk-live-SECRET-reason")
        if params.get("name") == "missing":
            raise KeyError("job not found")

    web = _web(handler)
    protocol = _protocol()
    web.register_method = lambda name, bound: web._method_handlers.__setitem__(name, bound)
    web.connection_user_id = staticmethod(lambda ws: getattr(ws, WEB_IDENTITY, None) or None)
    bind_owned_cron_handlers(web, protocol)
    bound = web._method_handlers["cron.job.create"]
    ws = SimpleNamespace(**{WEB_IDENTITY: "alice"})
    await bound(ws, "r1", {"name": "ok", "project_id": "default", "model_name": "m"}, None)
    assert seen[-1] == ({"name": "ok", "project_id": "default", "model_name": "m",
                        "user_id": "alice", "group_id": "", "bot_id": ""}, "alice")
    for name, code in (("login", "BAD_REQUEST"), ("down", "CRON_UNAVAILABLE"), ("missing", "NOT_FOUND")):
        web.send_response.reset_mock()
        await bound(ws, "r1", {"name": name, "_auth_session": "client-session"}, None)
        assert "_auth_session" not in seen[-1][0]
        kwargs = web.send_response.call_args.kwargs
        assert kwargs["ok"] is False and kwargs["code"] == code
        assert "secret" not in kwargs["error"] and "sk-live" not in kwargs["error"]


async def test_update_patch_copies_strip_private_keys_and_non_dict_params_are_rejected():
    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append(params)

    web = _web(handler, "cron.job.update")
    protocol = _protocol()
    web.register_method = lambda name, bound: web._method_handlers.__setitem__(name, bound)
    web.connection_user_id = staticmethod(lambda ws: getattr(ws, WEB_IDENTITY, None) or None)
    bind_owned_cron_handlers(web, protocol)
    bound = web._method_handlers["cron.job.update"]
    ws = SimpleNamespace(**{WEB_IDENTITY: "alice"})
    patch = {"name": "renamed", "_auth_session": "client-session"}
    await bound(ws, "r1", {"id": "j1", "patch": patch}, None)
    assert seen == [{"id": "j1", "patch": {"name": "renamed"}, "user_id": "alice", "group_id": "", "bot_id": ""}]
    assert patch == {"name": "renamed", "_auth_session": "client-session"}
    for params in ("text", 7, []):
        await bound(ws, "r2", params, None)
    assert web.send_response.call_args.kwargs["code"] == "BAD_REQUEST"
    await bound(ws, "r3", None, None)
    assert seen[-1] == {"user_id": "alice", "group_id": "", "bot_id": ""}


async def test_owned_tui_cron_filters_users_even_when_they_share_one_worker(monkeypatch):
    from agent_gateway.channel_manager.tui.tui_channel import TuiChannel, TuiChannelConfig
    from agent_gateway.routing.keys import AgentRef, RoutingKey

    class Peer:
        closed = False

        def __init__(self, user):
            self._gateway_user_id = user

    alice, charlie = Peer("alice"), Peer("charlie")
    channel = TuiChannel(TuiChannelConfig(cron_owner_filter=True))
    for user, peer in [("alice", alice), ("charlie", charlie)]:
        await channel.register_ws(peer, RoutingKey(user, "tui", "default", AgentRef(mode="agent", id="default"), ""))
    recipients = []
    monkeypatch.setattr(channel, "_enqueue_send", lambda peer, frame: recipients.append(peer))
    await channel.send(Message(id="push", type="event", channel_id="tui", params={}, timestamp=1, session_id=None,
                               ok=True, event_type=EventType.CHAT_FINAL,
                               payload={"content": "alice result", "user_id": "alice", "cron": {"job_id": "j1"}}))
    assert recipients == [alice]
    recipients.clear()
    await channel.send(Message(id="push2", type="event", channel_id="tui", params={}, timestamp=1, session_id=None,
                               ok=True, event_type=EventType.CHAT_FINAL,
                               payload={"content": "owner missing", "cron": {"job_id": "j2"}}))
    assert recipients == []
    for peer in (alice, charlie):
        await channel.unregister_ws(peer)
    await channel.stop()


async def test_tui_connection_is_registered_for_cron_without_first_chat():
    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers
    from agent_gateway.channel_manager.tui.tui_channel import TuiChannel

    channel = TuiChannel()
    route = SimpleNamespace(local_handlers={}, ws_channel=channel)
    web = SimpleNamespace(_method_handlers={})
    bind_owned_cron_handlers(web, _protocol({"/tui": route}))

    class Peer:
        _gateway_user_id = "alice"

    peer = Peer()
    for hook in channel._connect_hooks:
        await hook(peer)
    assert channel.config.cron_owner_filter
    assert any(peer in sockets for sockets in channel._clients_by_key.values())
    anonymous = SimpleNamespace(_gateway_user_id="")
    for hook in channel._connect_hooks:
        await hook(anonymous)
    assert not any(anonymous in sockets for sockets in channel._clients_by_key.values())
    await channel.unregister_ws(peer)
    assert not channel._clients_by_key
    await channel.stop()


async def test_tui_cron_handler_uses_connection_user_and_rejects_anonymously():
    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, user_id))

    channel = SimpleNamespace(config=SimpleNamespace(cron_owner_filter=False), on_connect=lambda hook: None)
    route = SimpleNamespace(local_handlers={"cron.job.run_now": handler}, ws_channel=channel)
    protocol = _protocol({"/tui": route})
    bind_owned_cron_handlers(SimpleNamespace(_method_handlers={}), protocol)
    bound = route.local_handlers["cron.job.run_now"]
    await bound(SimpleNamespace(**{TUI_IDENTITY: "alice"}), "r1", {"id": "j1", "user_id": "mallory"}, None,
                user_id="alice")
    assert seen == [({"id": "j1", "user_id": "alice", "group_id": "", "bot_id": ""}, "alice")]
    assert channel.config.cron_owner_filter is True
    await bound(SimpleNamespace(**{TUI_IDENTITY: None}), "r2", {"id": "j1"}, None, user_id=None)
    assert len(seen) == 1
    assert protocol.send_response.call_args.kwargs["code"] == "CRON_IDENTITY_REQUIRED"


async def test_tui_rejection_falls_back_to_the_socket_frame():
    import json

    from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

    handler = AsyncMock()
    channel = SimpleNamespace(config=SimpleNamespace(cron_owner_filter=False), on_connect=lambda hook: None)
    route = SimpleNamespace(local_handlers={"cron.job.delete": handler}, ws_channel=channel)
    protocol = SimpleNamespace(config=SimpleNamespace(routes={"/tui": route}))
    bind_owned_cron_handlers(SimpleNamespace(_method_handlers={}), protocol)
    ws = SimpleNamespace(**{TUI_IDENTITY: "default"}, send=AsyncMock())
    await route.local_handlers["cron.job.delete"](ws, "r1", {"id": "j1"}, None, user_id="default")
    handler.assert_not_awaited()
    ws.send.assert_awaited_once()
    assert json.loads(ws.send.call_args.args[0]) == {
        "type": "res", "id": "r1", "ok": False,
        "error": "Cron requires a named connection user", "code": "CRON_IDENTITY_REQUIRED",
    }
