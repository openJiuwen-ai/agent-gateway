# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Client request IDs are scoped to their originating TUI connection."""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from agent_gateway import app_gateway
from agent_gateway.app_gateway import GatewayServer, GatewayServerConfig, RouteConfig
from agent_gateway.channel_manager.tui.tui_channel import TuiChannel
from agent_gateway.channel_manager.tui.tui_connect import CliRouteBindParams, build_cli_route_binding


class _Socket:
    path = "/tui"
    closed = False

    def __init__(self, user_id):
        self._gateway_user_id = user_id
        self.request_headers = {"X-User-Id": user_id}
        self.frames = []

    async def send(self, data):
        self.frames.append(json.loads(data))

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    def contents(self):
        """Stream output received (not the handshake's connection.ack)."""
        return [frame["payload"].get("content") for frame in self.frames if frame.get("event") == "chat.delta"]


class _MessageHandler:
    """Records the disconnect-cancel calls the TUI route makes."""

    def __init__(self):
        self.unscheduled = []
        self.scheduled = []

    def cancel_scheduled_disconnect_cancel(self, channel_id, session_id):
        self.unscheduled.append(session_id)
        return True

    async def schedule_cancel_agent_sessions_on_disconnect(self, session_keys, *, stale_request_keys=None, **kwargs):
        self.scheduled.append(([key[1] for key in session_keys], [key[1] for key in stale_request_keys or []]))


@pytest_asyncio.fixture(params=[
    ("alice", "bob", False), ("alice", "bob", True),
    ("alice", "alice", False), ("alice", "alice", True),
])
async def pair(request):
    alice, bob, delegated = request.param
    sockets = [_Socket(alice), _Socket(bob)]
    channel = TuiChannel() if delegated else None
    handler = _MessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(ws_channel=channel, message_handler=handler))
    route = RouteConfig(
        path="/tui", channel_id="tui", ws_channel=channel,
        forward_methods=binding.forward_methods,
        forward_no_local_handler_methods=binding.forward_no_local_handler_methods,
        session_bind_handler=binding.session_bind_handler,
        disconnect_handler=binding.disconnect_handler,
    )
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    forwarded = []
    server.on_message(forwarded.append)

    async def request(ws, client_id, method, session_id, is_stream=False, **params):
        await server._handle_raw_message(ws, json.dumps({
            "type": "req", "id": client_id, "method": method, "is_stream": is_stream,
            "params": {"session_id": session_id, **params},
        }), "/tui", route)

    async def send(index, client_id, method="mode.get", target=None, is_stream=False, **params):
        before = len(forwarded)
        await request(sockets[index], client_id, method, target or f"old-{index}", is_stream, **params)
        assert len(forwarded) == before + 1
        return forwarded[-1]

    async def respond(msg, **overrides):
        response = SimpleNamespace(**{
            "type": "res", "channel_id": "tui", "id": msg.id,
            "session_id": msg.session_id, "user_id": msg.user_id, "ok": True,
            "payload": {"request_id": msg.id, "session_id": msg.session_id},
            **overrides,
        })
        await (channel or server).send(response)
        assert response.id == msg.id  # Delivery must not mutate the upstream frame.

    async def wait_frames(index, count):
        async def delivered():
            while len(sockets[index].frames) < count:
                await asyncio.sleep(0)
        await asyncio.wait_for(delivered(), 2)

    try:
        for index, ws in enumerate(sockets):
            await server._bind_route_session_client(route, f"old-{index}", ws)
        yield SimpleNamespace(
            server=server, sockets=sockets, send=send, respond=respond, wait_frames=wait_frames,
            request=request, route=route, handler=handler, forwarded=forwarded,
        )
    finally:
        await asyncio.gather(*server._session_bind_tasks, return_exceptions=True)
        for ws in sockets:
            await server._connection_handler(ws)
        watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)
        if channel is not None:
            await channel.stop()


@pytest.mark.asyncio
async def test_identical_client_ids_route_replies_and_references_to_their_own_socket(pair):
    requests = [await pair.send(index, "req-1") for index in range(2)]
    assert requests[0].id != requests[1].id
    assert all(msg.id != "req-1" for msg in requests)
    for index in (1, 0):  # Complete out of order.
        await pair.respond(requests[index])
        await pair.wait_frames(index, 1)
        frame = pair.sockets[index].frames[0]
        assert frame["id"] == frame["payload"]["request_id"] == "req-1"
        assert frame["payload"]["session_id"] == f"old-{index}"
        if index == 1:
            assert pair.sockets[0].frames == []
    for index in range(2):
        followup = await pair.send(index, "followup-1", request_id="req-1")
        assert followup.params["request_id"] == requests[index].id


@pytest.mark.asyncio
async def test_identical_switch_ids_complete_only_the_originating_connection(pair):
    requests = [await pair.send(index, "switch-1", "session.switch", f"new-{index}") for index in range(2)]
    assert requests[0].id != requests[1].id
    # A reply carrying the other window's identity/target cannot complete a
    # switch. AgentServer answers each request once, so it settles window 0's
    # switch as failed instead of leaving it pending; window 1 sees nothing.
    await pair.respond(requests[0], user_id=requests[1].user_id, session_id="new-1")
    await pair.wait_frames(0, 1)
    rejected = pair.sockets[0].frames[0]
    assert (rejected["id"], rejected["ok"], rejected["code"]) == ("switch-1", False, "INTERNAL_ERROR")
    assert pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])
    assert pair.sockets[1].frames == []
    # A later reply for the settled request is discarded.
    await pair.respond(requests[0])
    await pair.respond(requests[1])
    await pair.wait_frames(1, 1)
    assert (pair.sockets[1].frames[0]["id"], pair.sockets[1].frames[0]["ok"]) == ("switch-1", True)
    assert pair.server.is_session_bound_to_client("tui", "new-1", pair.sockets[1])
    assert pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])
    assert len(pair.sockets[0].frames) == 1
    # The window is not left with a stale pending switch.
    retry = await pair.send(0, "switch-2", "session.switch", "new-0")
    await pair.respond(retry)
    await pair.wait_frames(0, 2)
    assert (pair.sockets[0].frames[1]["id"], pair.sockets[0].frames[1]["ok"]) == ("switch-2", True)
    assert pair.server.is_session_bound_to_client("tui", "new-0", pair.sockets[0])
    assert pair.server._pending_tui_switches == {}


@pytest.mark.asyncio
async def test_switch_reply_stays_behind_frames_already_queued_for_the_window(pair):
    if pair.server.config.routes["/tui"].ws_channel is None:
        pytest.skip("ordering applies to TuiChannel's per-socket writer")
    earlier = await pair.send(0, "req-1")
    switch = await pair.send(0, "switch-1", "session.switch", "new-0")
    await pair.respond(earlier)  # Queued on the window's writer, not yet flushed.
    await pair.respond(switch)
    await pair.wait_frames(0, 2)
    assert [frame["id"] for frame in pair.sockets[0].frames] == ["req-1", "switch-1"]


@pytest.mark.asyncio
async def test_mismatched_switch_reply_releases_the_target_for_other_windows(pair):
    switch = await pair.send(0, "switch-1", "session.switch", "shared")
    await pair.respond(switch, session_id="elsewhere")
    await pair.wait_frames(0, 1)
    assert pair.sockets[0].frames[0]["ok"] is False
    assert pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])
    assert pair.server._pending_tui_switches == {}
    # A retained reservation would answer SESSION_IN_USE here (not forwarded).
    other = await pair.send(1, "switch-1", "session.switch", "shared")
    await pair.respond(other)
    await pair.wait_frames(1, 1)
    assert pair.sockets[1].frames[0]["ok"] is True
    assert pair.server.is_session_bound_to_client("tui", "shared", pair.sockets[1])


@pytest.mark.asyncio
async def test_switch_completion_does_not_wait_for_lifecycle_membership_lookup(pair, monkeypatch):
    lookup_started = asyncio.Event()
    release_lookup = asyncio.Event()

    async def slow_metadata(**kwargs):
        assert kwargs["req_method"].value == "session.get_metadata"
        lookup_started.set()
        await release_lookup.wait()
        return False, {"code": "NOT_FOUND"}

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", slow_metadata)
    switch = await pair.send(0, "switch-1", "session.switch", "new-0")
    # Switch replies are delivered by the shared outbound dispatcher, which
    # must not wait on an AgentServer round-trip for one window's binding.
    await asyncio.wait_for(pair.respond(switch), 1)
    await pair.wait_frames(0, 1)
    assert pair.sockets[0].frames[0]["id"] == "switch-1"
    assert pair.server.is_session_bound_to_client("tui", "new-0", pair.sockets[0])
    await asyncio.wait_for(lookup_started.wait(), 1)
    assert pair.server._session_bind_tasks
    release_lookup.set()
    await asyncio.wait_for(asyncio.gather(*pair.server._session_bind_tasks), 1)
    assert not pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])


async def _settle(turns=20):
    for _ in range(turns):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_unanswered_switch_expires_and_releases_the_target(pair, monkeypatch):
    monkeypatch.setattr(app_gateway, "resolve_agent_request_timeout_seconds", lambda **kwargs: 0.01)
    monkeypatch.setattr(app_gateway, "_TUI_SWITCH_DEADLINE_MARGIN_SECONDS", 0.0)
    switch = await pair.send(0, "switch-1", "session.switch", "shared")
    # No reply reaches the gateway. The deadline settles the switch as failed.
    await pair.wait_frames(0, 1)
    expired = pair.sockets[0].frames[0]
    assert (expired["id"], expired["ok"], expired["code"]) == ("switch-1", False, "AGENT_SERVER_TIMEOUT")
    assert pair.server._pending_tui_switches == {}
    assert pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])
    # A late reply must not report a switch that never bound.
    await pair.respond(switch)
    await _settle()
    assert len(pair.sockets[0].frames) == 1
    assert not pair.server.is_session_bound_to_client("tui", "shared", pair.sockets[0])
    # Another window can take the target, and this window can switch again.
    other = await pair.send(1, "switch-1", "session.switch", "shared")
    await pair.respond(other)
    await pair.wait_frames(1, 1)
    assert pair.sockets[1].frames[0]["ok"] is True
    assert pair.server.is_session_bound_to_client("tui", "shared", pair.sockets[1])
    retry = await pair.send(0, "switch-2", "session.switch", "new-0")
    await pair.respond(retry)
    await pair.wait_frames(0, 2)
    assert (pair.sockets[0].frames[1]["id"], pair.sockets[0].frames[1]["ok"]) == ("switch-2", True)
    assert pair.server.is_session_bound_to_client("tui", "new-0", pair.sockets[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("settled_by", ["reply", "mismatched reply", "disconnect", "stop"])
async def test_settled_switch_cancels_its_deadline(pair, settled_by):
    switch = await pair.send(0, "switch-1", "session.switch", "new-0")
    deadline = next(iter(pair.server._pending_tui_switches.values())).deadline
    assert deadline is not None and not deadline.cancelled()
    if settled_by == "reply":
        await pair.respond(switch)
    elif settled_by == "mismatched reply":
        await pair.respond(switch, session_id="elsewhere")
    elif settled_by == "disconnect":
        await pair.server._connection_handler(pair.sockets[0])
    else:
        await pair.server.stop()
    assert deadline.cancelled()
    assert pair.server._pending_tui_switches == {}
    if settled_by in {"reply", "mismatched reply"}:
        await pair.wait_frames(0, 1)
        assert pair.sockets[0].frames[0]["ok"] is (settled_by == "reply")
        await _settle()
        assert len(pair.sockets[0].frames) == 1  # Exactly one answer.


@pytest.mark.asyncio
@pytest.mark.parametrize(("timeout_ms", "longest"), [(None, 55.0), (10_000, 10.0), (10**9, 55.0)])
async def test_switch_deadline_follows_the_unary_timeout_policy(pair, timeout_ms, longest):
    frame = {"type": "req", "id": "switch-1", "method": "session.switch", "params": {"session_id": "new-0"}}
    if timeout_ms is not None:
        frame["timeout_ms"] = timeout_ms
    await pair.server._handle_raw_message(
        pair.sockets[0], json.dumps(frame), "/tui", pair.server.config.routes["/tui"],
    )
    deadline = next(iter(pair.server._pending_tui_switches.values())).deadline
    remaining = deadline.when() - asyncio.get_running_loop().time()
    # A huge client timeout cannot reserve the target for as long.
    assert 0 < remaining <= longest + app_gateway._TUI_SWITCH_DEADLINE_MARGIN_SECONDS


@pytest.mark.asyncio
async def test_answered_unary_correlations_are_bounded_per_socket(pair, monkeypatch):
    """A window's periodic requests must not grow the correlation tables until it disconnects."""
    monkeypatch.setattr(app_gateway, "_TUI_ANSWERED_ORIGINS_KEPT", 3)
    server = pair.server
    stream = await pair.send(0, "stream-1", is_stream=True)
    unary = []
    for number in range(6):
        unary.append(await pair.send(0, f"req-{number}"))
        await pair.respond(unary[-1])
    await pair.wait_frames(0, 6)
    assert [frame["id"] for frame in pair.sockets[0].frames] == [f"req-{number}" for number in range(6)]

    kept = server._tui_request_origins
    assert [request.id in kept for request in unary] == [False, False, False, True, True, True]
    assert stream.id in kept  # The gateway cannot see where a stream ends.
    forwarded_keys = {key[1] for key in server._request_to_client if key[1].startswith("tui-forward-")}
    assert forwarded_keys == set(kept)  # Both tables shrink together.

    # The older stream still routes its output with the client's ID restored.
    await pair.respond(
        stream, type="event", event_type=SimpleNamespace(value="chat.delta"),
        payload={"content": "late", "request_id": stream.id},
    )
    await pair.wait_frames(0, 7)
    assert pair.sockets[0].frames[6]["payload"] == {
        "content": "late", "request_id": "stream-1", "session_id": "old-0",
    }
    # Recent answered requests stay referable; evicted ones no longer translate,
    # and a duplicate reply for one is dropped rather than routed by session.
    assert (await pair.send(0, "followup-a", request_id="req-5")).params["request_id"] == unary[5].id
    assert (await pair.send(0, "followup-b", request_id="req-0")).params["request_id"] == "req-0"
    await pair.respond(unary[0])
    await _settle()
    assert len(pair.sockets[0].frames) == 7
    assert pair.sockets[1].frames == []


async def _drop_mid_stream(pair):
    """Window 0 starts a streamed turn in its Session ``old-0``; then its socket drops."""
    stream = await pair.send(0, "chat-1", is_stream=True)
    await pair.server._connection_handler(pair.sockets[0])
    return stream


async def _stream_more(pair, stream):
    """AgentServer keeps streaming the turn under its original upstream ID."""
    await pair.respond(
        stream, type="event", event_type=SimpleNamespace(value="chat.delta"),
        payload={"content": "still-streaming", "request_id": stream.id},
    )
    await _settle()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["mode.get", "test.local", "history.get"])
async def test_reconnected_window_takes_back_its_session_and_in_flight_output(pair, method):
    """The TUI reconnects on a new socket and announces nothing. Its next request,
    forwarded or local-only, re-attaches it to the Session its old socket dropped."""
    server, handler = pair.server, pair.handler

    async def local_only(ws, req_id, params, session_id):
        await server.send_response(ws, req_id, ok=True, payload={})

    server.register_local_handler("/tui", "test.local", local_only)
    stream = await _drop_mid_stream(pair)
    assert handler.scheduled[-1][0] == ["old-0"]  # MessageHandler will cancel in 60 s...

    fresh = _Socket(pair.sockets[0]._gateway_user_id)
    await pair.request(fresh, "c2", method, "old-0", request_id="chat-1")
    assert server.is_session_bound_to_client("tui", "old-0", fresh)
    assert handler.unscheduled.count("old-0") == 2  # ...unless the window comes back.
    if method == "mode.get":
        # References to the turn already resolve on the first request.
        assert pair.forwarded[-1].params["request_id"] == stream.id

    await _stream_more(pair, stream)
    delta = next(frame for frame in fresh.frames if frame.get("event") == "chat.delta")
    assert delta["payload"]["content"] == "still-streaming"
    assert delta["payload"]["request_id"] == "chat-1"  # The client's own ID is restored.
    assert pair.sockets[1].contents() == []

    # A reply to the dropped socket's request is not delivered: the Session's new
    # holder may be another process with its own request IDs.
    replies = [frame for frame in fresh.frames if frame.get("type") == "res"]
    await pair.respond(stream)
    await _settle()
    assert [frame for frame in fresh.frames if frame.get("type") == "res"] == replies


@pytest.mark.asyncio
@pytest.mark.parametrize("who", ["another user", "another routing scope"])
async def test_detached_session_is_not_handed_to_another_identity(pair, who):
    stream = await _drop_mid_stream(pair)
    stranger = _Socket("mallory" if who == "another user" else pair.sockets[0]._gateway_user_id)
    if who == "another routing scope":
        stranger._web_routing = {"group_id": "other-group"}
    await pair.request(stranger, "c2", "mode.get", "old-0")
    assert not pair.server.is_session_bound_to_client("tui", "old-0", stranger)
    # A foreground request may take the free Session, but never the dropped socket's output.
    await pair.request(stranger, "c3", "history.get", "old-0")
    assert pair.server.is_session_bound_to_client("tui", "old-0", stranger)
    await _stream_more(pair, stream)
    assert stranger.contents() == []


@pytest.mark.asyncio
async def test_only_a_reconnecting_windows_own_request_reattaches(pair):
    await _drop_mid_stream(pair)
    user = pair.sockets[0]._gateway_user_id
    fresh, quitting = _Socket(user), _Socket(user)
    # session.* requests can name another Session (preview, rename), so their ID is no hint.
    for method in ("session.rename", "session.preview"):
        await pair.request(fresh, f"c-{method}", method, "old-0")
        assert not pair.server.is_session_bound_to_client("tui", "old-0", fresh)
    await pair.request(quitting, "c-exit", "tui.disconnect", "old-0")
    assert not pair.server.is_session_bound_to_client("tui", "old-0", quitting)
    # A window that already holds a Session is not reconnecting.
    await pair.send(1, "c-other", target="old-0")
    assert pair.server.is_session_bound_to_client("tui", "old-1", pair.sockets[1])
    assert not pair.server.is_session_bound_to_client("tui", "old-0", pair.sockets[1])

    await pair.request(fresh, "c-own", "mode.get", "old-0")
    assert pair.server.is_session_bound_to_client("tui", "old-0", fresh)


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["grace expired", "user exit"])
async def test_nothing_is_held_for_an_expired_or_exited_window(pair, monkeypatch, why):
    if why == "grace expired":
        monkeypatch.setattr(app_gateway, "_TUI_REATTACH_GRACE_SECONDS", -1.0)
    else:
        pair.sockets[0]._jiuwenswarm_tui_user_exit = True
    stream = await _drop_mid_stream(pair)
    if why == "user exit":
        assert pair.server._detached_tui_sessions == {}
    fresh = _Socket(pair.sockets[0]._gateway_user_id)
    await pair.request(fresh, "c2", "mode.get", "old-0")
    assert not pair.server.is_session_bound_to_client("tui", "old-0", fresh)
    await pair.request(fresh, "c3", "history.get", "old-0")  # Foreground still binds.
    assert pair.server.is_session_bound_to_client("tui", "old-0", fresh)
    assert pair.server._detached_tui_sessions == {}
    await _stream_more(pair, stream)
    assert fresh.contents() == []


@pytest.mark.asyncio
async def test_reconnect_during_teardown_does_not_schedule_the_disconnect_cancel(pair):
    """The old socket's disconnect handler runs after awaits (a writer flush can take
    seconds). A cancel scheduled after the window re-attached would never be called off."""
    server, route, handler = pair.server, pair.route, pair.handler
    fresh = _Socket(pair.sockets[0]._gateway_user_id)
    finish_teardown = route.disconnect_handler

    async def teardown_with_reconnect(ws, session_keys, request_keys):
        route.disconnect_handler = finish_teardown
        await pair.request(fresh, "c2", "mode.get", "old-0")
        await finish_teardown(ws, session_keys, request_keys)

    await pair.send(0, "chat-1", is_stream=True)
    route.disconnect_handler = teardown_with_reconnect
    await server._connection_handler(pair.sockets[0])
    assert server.is_session_bound_to_client("tui", "old-0", fresh)
    assert handler.scheduled == []


@pytest.mark.asyncio
async def test_another_windows_session_is_untouched_when_the_turn_starter_drops(pair):
    """Window 0 starts a turn in old-0 and moves on; window 1 takes old-0; window 0 drops."""
    server, route = pair.server, pair.route
    stream = await pair.send(0, "chat-1", is_stream=True)
    await server._bind_route_session_client(route, "elsewhere", pair.sockets[0])
    assert not server.is_session_bound_to_client("tui", "old-0", pair.sockets[0])
    await server._bind_route_session_client(route, "old-0", pair.sockets[1])
    await server._connection_handler(pair.sockets[0])

    sessions, requests = pair.handler.scheduled[-1]
    assert sessions == ["elsewhere"]
    assert stream.id not in requests  # old-0's work is window 1's to keep or cancel.
    assert "old-0" not in server._detached_tui_sessions
    await _stream_more(pair, stream)
    assert pair.sockets[1].contents() == []


@pytest.mark.asyncio
async def test_running_stream_is_handed_back_however_many_requests_followed_it(pair):
    """The turn in progress is usually the oldest request of a busy window, so no
    limit on what is kept at disconnect may push it out."""
    stream = await pair.send(0, "chat-1", is_stream=True)
    for number in range(300):  # More than the answered-request cap.
        await pair.respond(await pair.send(0, f"req-{number}"))
    await pair.server._connection_handler(pair.sockets[0])
    assert stream.id in pair.server._detached_tui_sessions["old-0"].origins

    fresh = _Socket(pair.sockets[0]._gateway_user_id)
    await pair.request(fresh, "c2", "mode.get", "old-0")
    await _stream_more(pair, stream)
    assert fresh.contents() == ["still-streaming"]


@pytest.mark.asyncio
async def test_back_to_back_switches_leave_the_window_on_the_last_one(pair):
    """Both switches are accepted before either deferred bind hook has run. The
    earlier hook must not release the newer binding."""
    server, ws = pair.server, pair.sockets[0]
    await pair.respond(await pair.send(0, "switch-a", "session.switch", "new-a"))
    await pair.respond(await pair.send(0, "switch-b", "session.switch", "new-b"))
    assert len(server._session_bind_tasks) == 2  # Neither hook has finished.
    await asyncio.gather(*server._session_bind_tasks)
    await pair.wait_frames(0, 2)
    assert [(frame["id"], frame["ok"]) for frame in ws.frames] == [("switch-a", True), ("switch-b", True)]
    assert {key[1] for key, owner in server._session_to_client.items() if owner is ws} == {"new-b"}


@pytest.mark.asyncio
async def test_adopted_output_survives_a_second_drop(pair):
    stream = await _drop_mid_stream(pair)
    user = pair.sockets[0]._gateway_user_id
    second, third = _Socket(user), _Socket(user)
    await pair.request(second, "c2", "mode.get", "old-0")
    await pair.server._connection_handler(second)
    await pair.request(third, "c3", "mode.get", "old-0")
    assert pair.server.is_session_bound_to_client("tui", "old-0", third)
    await _stream_more(pair, stream)
    assert third.contents() == ["still-streaming"]
    assert second.contents() == []


@pytest.mark.asyncio
async def test_disconnected_origin_cannot_complete_or_deliver_to_a_replacement_window(pair):
    old_request = await pair.send(0, "req-1", target="replacement")
    old_switch = await pair.send(0, "switch-1", "session.switch", "replacement")
    await pair.server._connection_handler(pair.sockets[0])
    disconnected_frames = list(pair.sockets[0].frames)
    replacement = await pair.send(1, "switch-1", "session.switch", "replacement")
    await pair.respond(old_switch)
    assert pair.server.is_session_bound_to_client("tui", "old-1", pair.sockets[1])
    assert pair.sockets[1].frames == []
    await pair.respond(replacement)
    await pair.wait_frames(1, 1)
    await pair.respond(old_request)
    await pair.respond(old_switch)
    assert pair.sockets[0].frames == disconnected_frames
    assert len(pair.sockets[1].frames) == 1
