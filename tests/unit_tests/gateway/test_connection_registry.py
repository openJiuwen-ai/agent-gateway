# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.connection_registry import ConnectionRegistry


class FakeWs:
    def __init__(self, *, closed=False):
        self.closed = closed


def build_registry():
    def client_route_key(channel_id, scoped_id, agent_ref=None):
        if not channel_id or not scoped_id:
            return None
        if agent_ref is not None:
            return (channel_id, scoped_id, str(agent_ref))
        return (channel_id, scoped_id)

    return ConnectionRegistry(client_route_key)


async def test_remove_client_removes_all_keys_for_one_ws():
    registry = build_registry()
    ws = FakeWs()
    other = FakeWs()
    registry.add_client(ws)
    registry.add_client(other)
    registry.bind_request(("web", "req-1", None), ws)
    registry.bind_request(("web", "req-2", None), other)
    registry.session_to_client[("web", "sess-1")] = ws
    registry.session_to_client[("web", "sess-2", "agent:1")] = ws
    registry.pending_session_clients[("web", "sess-3")] = ws

    request_keys, session_keys, pending_keys = registry.remove_client(ws)

    assert request_keys == [("web", "req-1", None)]
    assert session_keys == [("web", "sess-1"), ("web", "sess-2", "agent:1")]
    assert pending_keys == [("web", "sess-3")]
    assert registry.clients == {other}
    assert registry.request_to_client == {
        ("web", "req-2", None): other,
    }
    assert registry.session_to_client == {}
    assert registry.pending_session_clients == {}


async def test_pending_client_is_promoted_after_old_ws_disconnect():
    registry = build_registry()
    old_ws = FakeWs()
    new_ws = FakeWs()
    route = SimpleNamespace(
        channel_id="web",
        session_bind_handler=None,
    )
    session_key = ("web", "sess-1")

    registry.add_client(old_ws)
    registry.session_to_client[session_key] = old_ws
    assert await registry.bind_route_session_client(
        route, "sess-1", new_ws
    ) is False
    assert registry.pending_session_clients[session_key] is new_ws

    registry.remove_client(old_ws)
    await registry.promote_pending_session_client(route, session_key)

    assert registry.session_to_client[session_key] is new_ws
    assert session_key not in registry.pending_session_clients


async def test_closed_pending_client_is_not_promoted():
    registry = build_registry()
    old_ws = FakeWs()
    pending_ws = FakeWs(closed=True)
    route = SimpleNamespace(channel_id="web", session_bind_handler=None)
    session_key = ("web", "sess-1")

    registry.add_client(old_ws)
    registry.session_to_client[session_key] = old_ws
    await registry.bind_route_session_client(route, "sess-1", pending_ws)
    assert session_key not in registry.pending_session_clients
    registry.remove_client(old_ws)

    await registry.promote_pending_session_client(route, session_key)

    assert session_key not in registry.session_to_client
    assert session_key not in registry.pending_session_clients


async def test_session_bind_handler_error_keeps_binding():
    registry = build_registry()
    ws = FakeWs()

    async def failing_handler(*_args):
        raise RuntimeError("handler failed")

    route = SimpleNamespace(
        channel_id="web",
        session_bind_handler=failing_handler,
    )

    assert await registry.bind_route_session_client(
        route, "sess-1", ws
    ) is True
    assert registry.session_to_client[("web", "sess-1")] is ws


async def test_lookup_client_supports_two_and_three_tuple_keys():
    registry = build_registry()
    table = {
        ("web", "sess-1", "agent:1"): "three",
        ("web", "sess-2"): "two",
    }

    assert registry.lookup_client(
        table, "web", "sess-1", "agent:1"
    ) == "three"
    assert registry.lookup_client(
        table, "web", "sess-1"
    ) == "three"
    assert registry.lookup_client(
        table, "web", "sess-2", "agent:1"
    ) == "two"
    assert registry.lookup_client(
        table, "web", "missing"
    ) is None
