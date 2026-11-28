# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A static Worker delivers each outbound frame to its owner's sockets only."""

import asyncio
import json
from typing import Any

import pytest
from e2a.agent_models import AgentRef
from e2a.message import EventType, Message, ReqMethod

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.tui.tui_channel import TuiChannel
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.routing.keys import RoutingKey
from agent_gateway.routing.owner_delivery import OwnerDelivery

pytestmark = pytest.mark.asyncio


class _Socket:
    def __init__(self, user: str | None, socket_id: str) -> None:
        self.closed = False
        self.sent: list[Any] = []
        self._jiuwen_ws_id = socket_id
        # One object plays a Web socket and a TUI/ACP socket alike.
        self._web_connection_user_id = user
        self._gateway_user_id = user

    async def send(self, data: Any) -> None:
        self.sent.append(data)

    def frames(self) -> list[dict[str, Any]]:
        return [json.loads(item) for item in self.sent if isinstance(item, str)]


def _wire(channel, socket: _Socket) -> _Socket:
    channel._send_queues[socket._jiuwen_ws_id] = asyncio.Queue()
    return socket


def _key(user: str, session: str = "s1") -> RoutingKey:
    return RoutingKey(user_id=user, channel_id="web", app_id="default", agent_ref=AgentRef(mode="", id=""),
                      session_id=session)


def _message(event: str, *, user: str | None = "alice", request: str = "r1", session: str = "s1") -> Message:
    return Message(id=request, type="event", channel_id="web", session_id=session, params={},
                   timestamp=0, ok=True, req_method=ReqMethod.CHAT_SEND, user_id=user,
                   event_type=EventType(event), payload={"event_type": event, "session_id": session})


def _gate(requests: dict[str, str] | None = None, sessions: dict[str, str] | None = None) -> OwnerDelivery:
    requests, sessions = requests or {}, sessions or {}
    with pytest.raises(ValueError):
        OwnerDelivery(request_owner="nope", session_owner=lambda _sid: None)
    return OwnerDelivery(
        request_owner=lambda rid: requests.get(rid), session_owner=lambda sid: sessions.get(sid),
    )


async def _settled() -> None:
    for _ in range(4):
        await asyncio.sleep(0)


async def test_owner_is_resolved_from_its_request_then_the_message_then_its_session() -> None:
    gate = _gate({"r-req": "bob"}, {"s-session": "carol"})
    assert gate.of_message(_message("chat.delta", user="alice")) == "alice"
    # The recorded route of a request outranks what the Message declares.
    assert gate.of_message(_message("chat.delta", user="alice", request="r-req")) == "bob"
    assert gate.of_message(_message("chat.delta", user=None, request="r-req")) == "bob"
    assert gate.of_message(_message("chat.delta", user=None, request="unknown", session="s-session")) == "carol"
    assert gate.of_message(_message("chat.delta", user=None, request="unknown")) is None
    assert gate.of_message(_message("chat.delta", user="  ")) is None
    assert gate.of_request(" ") is None and gate.of_session(None) is None


async def test_only_a_named_owner_on_its_own_socket_is_allowed() -> None:
    assert OwnerDelivery.allows("alice", "alice")
    for user, owner in (("alice", "bob"), ("alice", None), (None, "alice"), (None, None), ("", "alice")):
        assert not OwnerDelivery.allows(user, owner)


async def test_scope_binds_the_owner_and_restores_the_previous_one() -> None:
    gate = _gate()
    assert OwnerDelivery.owner_in_scope() is None
    with gate.scope("alice"):
        assert OwnerDelivery.owner_in_scope() == "alice"
        with gate.scope("bob"):
            assert OwnerDelivery.owner_in_scope() == "bob"
        assert OwnerDelivery.owner_in_scope() == "alice"
    assert OwnerDelivery.owner_in_scope() is None
    with pytest.raises(RuntimeError):
        with gate.scope("alice"):
            raise RuntimeError("boom")
    assert OwnerDelivery.owner_in_scope() is None


@pytest.mark.parametrize("make", [lambda: WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter()),
                                  TuiChannel])
async def test_ungated_channels_keep_their_fan_out(make) -> None:
    channel = make()
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket(None, "b"))
    await channel.register_ws(alice, _key("alice"))
    await channel.register_ws(bob, _key("bob", "s2"))
    # A session event reaches the session's socket; a frame never needs an owner.
    await channel.send(_message("chat.delta", user=None))
    await _settled()
    assert len(alice.frames()) == 1 and bob.frames() == []


@pytest.mark.parametrize("make", [lambda: WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter()),
                                  TuiChannel])
async def test_gated_message_reaches_the_owner_session_socket_only(make) -> None:
    channel = make()
    channel.owner_delivery = _gate({"r1": "alice"})
    alice, mallory = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("mallory", "m"))
    await channel.register_ws(alice, _key("alice"))
    # Another user's socket bound to the same session id: shared tables are not rewritten.
    await channel.register_ws(mallory, _key("mallory"))
    await channel.send(_message("chat.delta", user=None))
    await _settled()
    assert [frame["event"] for frame in alice.frames()] == ["chat.delta"]
    assert mallory.frames() == []


@pytest.mark.parametrize("make", [lambda: WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter()),
                                  TuiChannel])
async def test_gated_message_without_any_route_is_dropped(make) -> None:
    channel = make()
    channel.owner_delivery = _gate()
    alice = _wire(channel, _Socket("alice", "a"))
    await channel.register_ws(alice, _key("alice"))
    await channel.send(_message("chat.delta", user=None))
    await _settled()
    assert alice.frames() == []


@pytest.mark.parametrize("make", [lambda: WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter()),
                                  TuiChannel])
async def test_message_naming_another_user_never_reaches_that_users_socket(make) -> None:
    channel = make()
    # The recorded route of the request names a user the Message does not.
    channel.owner_delivery = _gate({"r1": "alice"})
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("bob", "b"))
    await channel.register_ws(alice, _key("alice"))
    await channel.register_ws(bob, _key("bob"))
    # It follows the request to Alice's socket and never reaches Bob's.
    await channel.send(_message("chat.delta", user="bob"))
    await _settled()
    assert [frame["event"] for frame in alice.frames()] == ["chat.delta"] and bob.frames() == []
    # A Message that names a user without any route reaches nobody.
    await channel.send(_message("chat.delta", user="mallory", request="unknown"))
    await _settled()
    assert len(alice.frames()) == 1 and bob.frames() == []


async def test_web_direct_reply_resolves_the_owner_from_the_request_route() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.owner_delivery = _gate({"req-a": "alice"})
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("bob", "b"))
    await channel.send_response(alice, "req-a", ok=True, payload={"answer": 1})
    await channel.send_response(bob, "req-a", ok=True, payload={"answer": 1})
    await channel.send_response(alice, "never-admitted", ok=False, error="refused")
    queued = lambda socket: list(channel._send_queues[socket._jiuwen_ws_id]._queue)  # noqa: E731
    assert [frame["id"] for frame in queued(alice)] == ["req-a"]
    assert queued(bob) == []


async def test_web_session_event_uses_the_session_route_or_the_named_owner() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.owner_delivery = _gate(sessions={"s-alice": "alice"})
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("bob", "b"))
    await channel.register_ws(alice, _key("alice", "s-alice"))
    await channel.register_ws(bob, _key("bob", "s-alice"))
    await channel.send_session_event("s-alice", "trace.updated", {"revision": 1})
    await _settled()
    assert [frame["event"] for frame in alice.frames()] == ["trace.updated"]
    assert bob.frames() == []
    # An event sent to one socket directly needs a scope or a session route.
    await channel.send_event(bob, "trace.updated", {"revision": 2})
    await channel.send_session_event("s-unknown", "trace.updated", {"revision": 3})
    await _settled()
    assert bob.frames() == []
    assert len(alice.frames()) == 1


async def test_web_broadcast_branches_stay_with_the_owner() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.owner_delivery = _gate({"notice": "alice"})
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("bob", "b"))
    await channel.register_ws(alice, _key("alice"))
    await channel.register_ws(bob, _key("bob"))
    # A session-less broadcast: delivered to the owner's sockets only.
    await channel.send(_message("health_check.relay", user=None, request="notice", session=""))
    await _settled()
    assert [frame["event"] for frame in alice.frames()] == ["health_check.relay"]
    assert bob.frames() == []
    # Without a route the same broadcast reaches nobody.
    await channel.send(_message("health_check.relay", user=None, request="unknown", session=""))
    await _settled()
    assert len(alice.frames()) == 1 and bob.frames() == []


async def test_web_http_outbound_passes_the_same_gate() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.owner_delivery = _gate({"req-a": "alice"})
    accepted: list[Any] = []

    class _Outbound(_Socket):
        is_http_outbound = True

        def accept_frame(self, frame: Any) -> None:
            accepted.append(frame)

    outbound = _Outbound("alice", "http-a")
    with channel.owner_delivery.scope("bob"):
        channel._enqueue_send(outbound, {"type": "res", "id": "req-a"})
    assert accepted == []
    with channel.owner_delivery.scope("alice"):
        channel._enqueue_send(outbound, {"type": "res", "id": "req-a"})
    assert [frame["id"] for frame in accepted] == ["req-a"]


async def test_tui_in_order_reply_is_scoped_by_the_protocol_runtime() -> None:
    channel = TuiChannel()
    channel.owner_delivery = _gate({"gateway-request": "alice"})
    alice, bob = _wire(channel, _Socket("alice", "a")), _wire(channel, _Socket("bob", "b"))
    reply = _message("chat.delta", user=None, request="client-id")
    # Without a scope the reply queue of a gated channel stays empty.
    assert channel.enqueue_message(alice, reply)
    assert alice._jiuwen_ws_id and channel._send_queues["a"].qsize() == 0
    with channel.owner_delivery.scope("alice"):
        assert channel.enqueue_message(alice, reply)
    assert channel._send_queues["a"].qsize() == 1
    with channel.owner_delivery.scope("alice"):
        channel.enqueue_message(bob, reply)
    assert channel._send_queues["b"].qsize() == 0
