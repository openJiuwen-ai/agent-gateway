# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Real static Workers over the existing northbound handlers and transport.

Every Worker is a child process that serves the named users of its slot from
local AgentServer stand-ins. The tests speak the public Web, TUI and ACP
protocols and inject ``send_push`` frames on the real gateway connections.
"""

import asyncio
from contextlib import closing
import json
import socket

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from agent_gateway.processes.capabilities import management_preflight
from tests.unit_tests.gateway.routed_agent_server import RoutedAgentServer, push_wire

pytestmark = pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")

POLICY = LocalIdentityPolicy()


def _users_of(slot: int, count: int, *, skip: tuple[str, ...] = ()) -> list[str]:
    """Named users whose fixed affinity selects ``slot``."""
    affinity = FixedUserHash(WorkerManifest((0, 1)), POLICY)
    from agent_gateway.common.ingress_identity import resolve_local_identity

    found = []
    for index in range(5000):
        user = f"user-{index}"
        if user in skip:
            continue
        identity = resolve_local_identity(f"user_id={user}".encode(), (), entry="web", policy=POLICY)
        if affinity.select(identity) == slot:
            found.append(user)
            if len(found) == count:
                return found
    raise AssertionError("not enough users for the slot")


def _target(user: str, index: int, slot: int, url: str) -> SouthboundTarget:
    return SouthboundTarget(
        target_id=f"target-{index}", physical_id=f"physical-{index}", generation=1, user_id=user,
        agent_type="jiuwenswarm", owner_slot=slot, ws_url=url,
    )


def _chat(request: str, session: str | None = None, **params) -> str:
    body = {"content": "hello", **params}
    if session is not None:
        body["session_id"] = session
    return json.dumps({"type": "req", "id": request, "method": "chat.send", "is_stream": True, "params": body})


async def _next(ws, timeout: float = 10.0) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def _until(ws, event: str, *, timeout: float = 15.0) -> list[dict]:
    """Every frame up to and including the first ``event``."""
    frames = []
    async with asyncio.timeout(timeout):
        while True:
            frame = await _next(ws)
            frames.append(frame)
            if frame.get("event") == event:
                return frames


async def _quiet(*sockets, seconds: float = 0.6) -> None:
    """No socket receives anything in the given time."""
    async def expect_nothing(ws):
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(seconds):
                await ws.recv()

    await asyncio.gather(*(expect_nothing(ws) for ws in sockets))


async def _open(group, user: str):
    host, port = group.addresses[0]
    ws = await connect(f"ws://{host}:{port}/ws?user_id={user}", proxy=None, open_timeout=10)
    try:
        assert (await _next(ws))["event"] == "connection.ack"
    except BaseException:
        await ws.close()
        raise
    return ws


@pytest.fixture
async def static_group(tmp_path, monkeypatch):
    """Two Worker slots: slot 0 serves two users on two targets, slot 1 one user."""
    for name, directory in (("HOME", "home"), ("JIUWENSWARM_HOME", "home"),
                            ("JIUWENSWARM_DATA_DIR", "data"), ("AGENT_GATEWAY_DATA_DIR", "data"),
                            ("AGENTOS_GATEWAY_LOG_DIR", "logs")):
        path = tmp_path / directory
        path.mkdir(exist_ok=True)
        monkeypatch.setenv(name, str(path))
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    alice, bob = _users_of(0, 2)
    carol = _users_of(1, 1)[0]
    ghost = _users_of(0, 1, skip=(alice, bob))[0]
    agents = {}
    for name in ("A", "B", "C"):
        agents[name] = await RoutedAgentServer(name).start()
    routes = StaticRouteDirectory(
        revision=1, identity_policy=POLICY, manifest=WorkerManifest((0, 1)),
        targets=(
            _target(alice, 0, 0, agents["A"].url), _target(bob, 1, 0, agents["B"].url),
            _target(carol, 2, 1, agents["C"].url),
        ),
    )
    group = WorkerGroup(None, workers=2, web_port=0, protocol_port=0, handoff_timeout=2,
                        southbound_routes=routes)
    try:
        await group.start()
        yield group, agents, {"alice": alice, "bob": bob, "carol": carol, "ghost": ghost}
    finally:
        try:
            await group.stop()
        finally:
            for agent in agents.values():
                await agent.stop()
    assert all(process.returncode is not None for process in group.pool.processes)
    for address in group.addresses:
        with closing(socket.socket()) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(address)


def _stream(frames: list[dict]) -> list[str]:
    """The Agent stream events among the frames; gateway notices are left out."""
    return [frame["event"] for frame in frames if frame.get("event") in ("chat.delta", "chat.final")]


async def _first(ws, event: str, *, timeout: float = 15.0) -> dict:
    """The first frame of one event; earlier frames are consumed."""
    async with asyncio.timeout(timeout):
        while True:
            frame = await _next(ws)
            if frame.get("event") == event:
                return frame


def _envelopes(agent: RoutedAgentServer, method: str | None = None) -> list[dict]:
    return [item for item in agent.requests if method is None or item.get("method") == method]


async def test_each_named_user_reaches_own_target_and_own_socket(static_group):
    group, agents, users = static_group
    async with await _open(group, users["alice"]) as alice, await _open(group, users["bob"]) as bob:
        await alice.send(_chat("a-1", "session-a"))
        await bob.send(_chat("b-1", "session-b"))
        alice_frames, bob_frames = await asyncio.gather(
            _until(alice, "chat.final"), _until(bob, "chat.final"),
        )
    # Each request was accepted once and streamed once, to its own socket.
    assert [frame for frame in alice_frames if frame.get("type") == "res"] == [
        {"type": "res", "id": "a-1", "ok": True, "payload": {"accepted": True, "session_id": "session-a"}},
    ]
    assert _stream(alice_frames) == ["chat.delta", "chat.final"]
    assert _stream(bob_frames) == ["chat.delta", "chat.final"]
    assert not any(frame.get("type") == "res" and frame.get("id") == "b-1" for frame in alice_frames)
    assert all("B-" not in json.dumps(frame) for frame in alice_frames)
    assert all("A-" not in json.dumps(frame) for frame in bob_frames)
    # ...and each request was dispatched once, to the target of its user only.
    for agent, user, request in (("A", "alice", "a-1"), ("B", "bob", "b-1")):
        sent = _envelopes(agents[agent], "chat.send")
        assert [(item["request_id"], item.get("user_id")) for item in sent] == [(request, users[user])]
    assert not agents["A"].has_request("b-1") and not agents["B"].has_request("a-1")
    assert _envelopes(agents["C"]) == []


async def test_session_of_one_user_is_closed_for_another_and_held_stream_survives(static_group):
    group, agents, users = static_group
    agents["A"].hold.add("a-hold")
    async with await _open(group, users["alice"]) as alice, await _open(group, users["bob"]) as bob:
        await alice.send(_chat("a-hold", "session-a"))
        await agents["A"].wait_request("a-hold")
        # The session id belongs to the target that confirmed it for Alice.
        await bob.send(_chat("b-steal", "session-a"))
        with pytest.raises(ConnectionClosed) as closed:
            await _until(bob, "chat.final")
        assert closed.value.rcvd.code == 1008
        assert not agents["A"].has_request("b-steal") and not agents["B"].has_request("b-steal")
    async with await _open(group, users["bob"]) as bob:
        # A new chat of Bob on the same channel does not replace Alice's stream.
        await bob.send(_chat("b-2", "session-b"))
        assert _stream(await _until(bob, "chat.final")) == ["chat.delta", "chat.final"]
        await asyncio.sleep(0.3)
        assert _envelopes(agents["A"], "chat.interrupt") == [] and _envelopes(agents["B"], "chat.interrupt") == []
        agents["A"].release("a-hold")
    async with await _open(group, users["alice"]) as alice:
        # A fresh connection of the owner, once the session is confirmed for it.
        await alice.send(_chat("a-2", "session-a"))
        frames = await _until(alice, "chat.final")
        assert frames[-1]["payload"]["content"] == "A-final"


async def test_held_stream_stays_alive_and_is_delivered_to_its_owner_only(static_group):
    group, agents, users = static_group
    agents["A"].hold.add("a-hold")
    async with await _open(group, users["alice"]) as alice, await _open(group, users["bob"]) as bob:
        await alice.send(_chat("a-hold", "session-a"))
        await agents["A"].wait_request("a-hold")
        assert (await _first(alice, "chat.delta"))["payload"]["content"] == "A-delta"
        # Bob's ordinary chat neither cancels nor shares Alice's in-flight work.
        await bob.send(_chat("b-1", "session-b"))
        await _until(bob, "chat.final")
        await _quiet(alice, bob)
        assert _envelopes(agents["A"], "chat.interrupt") == []
        agents["A"].release("a-hold")
        frames = await _until(alice, "chat.final")
        assert frames[-1]["payload"]["content"] == "A-final"
        assert [item["request_id"] for item in _envelopes(agents["A"], "chat.send")] == ["a-hold"]


async def test_pushes_follow_request_and_session_routes_only(static_group):
    group, agents, users = static_group
    agents["A"].hold.add("a-hold")
    async with await _open(group, users["alice"]) as alice, await _open(group, users["bob"]) as bob:
        await alice.send(_chat("a-hold", "session-a"))
        await agents["A"].wait_request("a-hold")
        assert (await _first(alice, "chat.delta"))["payload"]["content"] == "A-delta"
        # 1. A request-bound frame from the target that owns the request.
        await agents["A"].push(push_wire(
            "a-hold", "session-a", event="chat.delta", channel="web", content="pushed-by-owner",
        ))
        frame = await _next(alice)
        assert (frame["event"], frame["payload"]["content"]) == ("chat.delta", "pushed-by-owner")
        # 2. A session-scoped frame for a session the sending target confirmed.
        await agents["A"].push(push_wire(
            "", "session-a", event="trace.updated", channel="web", trace_id="trace-1", revision=1,
        ))
        frame = await _first(alice, "trace.updated")
        assert frame["payload"]["trace_id"] == "trace-1"
        # 3. Another target names Alice's request or session: no route, dropped.
        await agents["B"].push(push_wire(
            "a-hold", "session-a", event="chat.delta", channel="web", content="foreign-request",
        ))
        await agents["B"].push(push_wire(
            "", "session-a", event="trace.updated", channel="web", trace_id="trace-2", revision=2,
        ))
        # 4. An unassociated frame has no owner on any Worker and is not claimed.
        await agents["A"].push(push_wire(
            "never-sent", "session-ghost", event="chat.delta", channel="web", content="unsolicited",
        ))
        await agents["A"].push(push_wire(
            "", "session-ghost", event="trace.updated", channel="web", trace_id="trace-3", revision=3,
        ))
        await _quiet(alice, bob)
        agents["A"].release("a-hold")
        frames = await _until(alice, "chat.final")
        assert "foreign-request" not in json.dumps(frames) and "unsolicited" not in json.dumps(frames)
        await _quiet(bob)
    assert [item["request_id"] for item in _envelopes(agents["A"], "chat.send")] == ["a-hold"]


async def test_user_without_a_target_and_request_without_a_user_are_refused(static_group):
    group, agents, users = static_group
    async with await _open(group, users["ghost"]) as ghost:
        await ghost.send(_chat("g-1", "session-g"))
        with pytest.raises(ConnectionClosed) as closed:
            await _until(ghost, "chat.final")
        assert closed.value.rcvd.code == 1008
    assert not any(agent.has_request("g-1") for agent in agents.values())
    # A frame that declares the user of somebody else is refused for the same reason.
    async with await _open(group, users["alice"]) as alice:
        await alice.send(_chat("a-lie", "session-a", user_id=users["bob"]))
        with pytest.raises(ConnectionClosed) as closed:
            await _until(alice, "chat.final")
        assert closed.value.rcvd.code == 1008
    assert not any(agent.has_request("a-lie") for agent in agents.values())
    # A user of the other slot is served by that slot's own target.
    async with await _open(group, users["carol"]) as carol:
        await carol.send(_chat("c-1", "session-c"))
        frames = await _until(carol, "chat.final")
    assert frames[-1]["payload"]["content"] == "C-final"
    assert [item.get("user_id") for item in _envelopes(agents["C"], "chat.send")] == [users["carol"]]
    assert not agents["A"].has_request("c-1") and not agents["B"].has_request("c-1")


async def test_tui_and_acp_connections_of_one_slot_stay_with_their_user(static_group):
    group, agents, users = static_group
    _host, port = group.addresses[1]
    headers = {"X-User-Id": users["alice"]}
    async with (
        connect(f"ws://127.0.0.1:{port}/tui", additional_headers=headers, proxy=None, open_timeout=10) as tui,
        connect(f"ws://127.0.0.1:{port}/acp?user_id={users['bob']}", proxy=None, open_timeout=10) as acp,
    ):
        # TUI: a request of Alice is answered from Alice's target.
        await tui.send(json.dumps({"type": "req", "id": "tui-1", "method": "chat.send", "is_stream": True,
                                   "params": {"session_id": "session-tui", "content": "hi"}}))
        frames = []
        async with asyncio.timeout(15):
            while True:
                frame = json.loads(await tui.recv())
                frames.append(frame)
                if frame.get("event") == "chat.final":
                    break
        assert [item for item in frames if item.get("type") == "res"] == [
            {"type": "res", "id": "tui-1", "ok": True, "payload": {"accepted": True, "session_id": "session-tui"}},
        ]
        assert frames[-1]["payload"]["content"] == "A-final"
        # The TUI handler forwards under ids of its own; what holds is that
        # every envelope, the session probes included, went to Alice's target.
        assert _envelopes(agents["A"], "chat.send") and all(
            item.get("user_id") == users["alice"] for item in agents["A"].requests
        )
        assert all(item.get("method") == "session.get_metadata" for item in agents["B"].requests)
        alice_chats = len(_envelopes(agents["A"], "chat.send"))
        await acp.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "session/new", "params": {}}))
        async with asyncio.timeout(15):
            while True:
                created = json.loads(await acp.recv())
                if created.get("id") == 1:
                    break
        assert created.get("result", {}).get("sessionId")
        bob_session = created["result"]["sessionId"]
        await acp.send(json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "session/prompt",
            "params": {"sessionId": bob_session, "prompt": [{"type": "text", "text": "hi"}]},
        }))
        async with asyncio.timeout(15):
            while agents["B"].requests == [] or not any(
                item.get("method") == "chat.send" for item in agents["B"].requests
            ):
                await asyncio.sleep(0.05)
        acp_frames = []
        async with asyncio.timeout(15):
            while json.loads(acp_frames[-1] if acp_frames else "{}").get("id") != 2:
                acp_frames.append(await asyncio.wait_for(acp.recv(), 15))
        # ACP: Bob's prompt reached Bob's target under Bob's name only, and
        # added nothing to Alice's target beyond its session probe.
        bob_chats = _envelopes(agents["B"], "chat.send")
        assert len(bob_chats) == 1 and bob_chats[0].get("user_id") == users["bob"]
        assert all(item.get("user_id") == users["bob"] for item in agents["B"].requests)
        assert len(_envelopes(agents["A"], "chat.send")) == alice_chats
        # ACP: Bob's session belongs to Bob's target; Alice's TUI frame is
        # refused at the socket, before any handler or target sees it.
        sent = (len(_envelopes(agents["A"], "chat.send")), len(_envelopes(agents["B"], "chat.send")))
        await tui.send(json.dumps({"type": "req", "id": "tui-2", "method": "chat.send",
                                   "params": {"session_id": bob_session, "content": "steal"}}))
        with pytest.raises(ConnectionClosed) as closed:
            # Earlier frames may still be queued; none of them may answer the
            # refused request.
            async with asyncio.timeout(10):
                while True:
                    frame = json.loads(await tui.recv())
                    assert frame.get("id") != "tui-2" and "steal" not in json.dumps(frame), frame
        assert closed.value.rcvd.code == 1008
        await asyncio.sleep(0.3)
        assert (len(_envelopes(agents["A"], "chat.send")), len(_envelopes(agents["B"], "chat.send"))) == sent
        assert "steal" not in json.dumps(agents["A"].requests + agents["B"].requests)


async def test_unreachable_target_stops_the_group_with_its_code(tmp_path, monkeypatch):
    for name, directory in (("HOME", "home"), ("JIUWENSWARM_HOME", "home"),
                            ("JIUWENSWARM_DATA_DIR", "data"), ("AGENT_GATEWAY_DATA_DIR", "data"),
                            ("AGENTOS_GATEWAY_LOG_DIR", "logs")):
        (tmp_path / directory).mkdir(exist_ok=True)
        monkeypatch.setenv(name, str(tmp_path / directory))
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
    live = await RoutedAgentServer("live").start()
    group = None
    try:
        alice, bob = _users_of(0, 2)
        routes = StaticRouteDirectory(
            revision=1, identity_policy=POLICY, manifest=WorkerManifest((0,)),
            targets=(_target(alice, 0, 0, live.url), _target(bob, 1, 0, f"ws://127.0.0.1:{dead_port}/agent")),
        )
        group = WorkerGroup(None, workers=1, web_port=0, protocol_port=0, handoff_timeout=1,
                            southbound_routes=routes)
        # The pool waits for every Worker in one TaskGroup, so a Worker that
        # failed before it was ready surfaces as the group of that failure.
        with pytest.raises(ExceptionGroup) as failed:
            await asyncio.wait_for(group.start(), 40)
        assert failed.group_contains(RuntimeError, match=r"Worker 0: .*FAILED \(SOUTHBOUND_UNREACHABLE\)")
        assert group.addresses == ()
        assert all(process.returncode is not None for process in group.pool.processes)
        # The reachable target was connected and released again.
        assert all(item.get("method") != "chat.send" for item in live.requests)
    finally:
        if group is not None:
            await group.stop()
        await live.stop()
