# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise lifecycle routing through the actual TUI handshake and bindings."""

import asyncio
import json
from collections import defaultdict
from types import SimpleNamespace

import pytest

from agent_gateway.app_gateway import GatewayServer, GatewayServerConfig, RouteConfig
from agent_gateway.channel_manager.tui import tui_connect
from agent_gateway.channel_manager.tui.tui_connect import CliRouteBindParams, build_cli_route_binding
from agent_gateway.channel_manager.tui.tui_channel import TuiChannel
from agent_gateway.channel_manager.web import lifecycle_handlers
from agent_gateway.routing.keys import TuiDeliveryTarget
from agent_gateway.routing.session_sharing import RoutingTarget


class _Socket:
    def __init__(self, session_id, query="", headers=None):
        self.path = "/tui" + query
        self.request_headers = {"X-User-Id": "u1", **(headers or {})}
        self.session_id = session_id
        self.closed = False
        self.frames = []
        self.incoming = asyncio.Queue()
        self.bound = asyncio.Event()
        self.incoming.put_nowait(json.dumps({
            "type": "req", "id": session_id, "method": "history.get",
            "params": {"session_id": session_id},
        }))

    def __aiter__(self):
        return self

    async def __anext__(self):
        frame = await self.incoming.get()
        if frame is None:
            raise StopAsyncIteration
        return frame

    async def send(self, data):
        frame = json.loads(data)
        self.frames.append(frame)
        if frame.get("type") == "res":
            self.bound.set()

    async def close(self, **kwargs):
        self.closed = True

    @property
    def project_events(self):
        return [frame for frame in self.frames if frame.get("event", "").startswith("project.")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["archive", "unarchive", "delete"])
@pytest.mark.parametrize("routed", [False, True])
@pytest.mark.parametrize("delegated", [False, True])
async def test_tui_project_replay_isolates_real_connections_and_deduplicates(monkeypatch, kind, routed, delegated):
    # Legacy connections also need project isolation when all tenant fields are empty.
    query = "?group_id=g1&bot_id=b1&gateway_id=gw1" if routed else ""
    first = _Socket("s1", query)
    second = _Socket("s2", query)
    third = _Socket("s3", "?group_id=g2&bot_id=b2&gateway_id=gw2")
    sockets = [first, second, third]
    project_by_session = {"s1": "p1", "s2": "p2", "s3": "p1"}
    metadata_failures = set()
    snapshots = defaultdict(asyncio.Queue)
    ready = asyncio.Event()
    processed = asyncio.Queue()
    metadata_calls = []
    metadata_pauses = {}
    forwarded = []
    original_sleep = asyncio.sleep

    async def fake_call(**kwargs):
        routing = kwargs["metadata"].get("routing", {})
        scope = routing.get("group_id", "")
        if kwargs["req_method"].value == "session.get_metadata":
            metadata_calls.append(kwargs)
            sid = kwargs["params"]["session_id"]
            pause = metadata_pauses.pop(sid, None)
            if pause is not None:
                started, release = pause
                started.set()
                await release.wait()
            if sid in metadata_failures:
                return False, {"code": "SERVICE_UNAVAILABLE"}
            project_id = project_by_session.get(sid)
            return (True, {"project_id": project_id}) if project_id else (False, {"code": "NOT_FOUND"})
        if kwargs["req_method"].value == "session.unarchive":
            return True, {"results": [{"session_id": "s1", "project_id": "p1", "ok": True, "restored": True}]}
        assert kwargs["req_method"].value == "project.lifecycle"
        await ready.wait()
        return True, {"events": await snapshots[scope].get()}

    async def poll_again(delay):
        processed.put_nowait(True)
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    async def preview_request(env):
        assert env.method == "session.preview"
        return SimpleNamespace(ok=True, payload={"session_id": env.params["session_id"]})

    binding = build_cli_route_binding(CliRouteBindParams(
        agent_client=SimpleNamespace(server_ready=True, send_request=preview_request),
        ws_channel=TuiChannel() if delegated else None,
    ))
    route = RouteConfig(
        path="/tui", channel_id="tui", session_bind_handler=binding.session_bind_handler,
        forward_methods=frozenset({"session.switch", "chat.send"}),
        forward_no_local_handler_methods=frozenset({"session.switch"}),
        ws_channel=binding.ws_channel,
    )
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)

    async def switched(msg):
        forwarded.append(msg)
        assert msg.metadata["user_id"] == "u1"
        assert msg.metadata.get("routing", {}) == {
            key: value for key, value in first._web_routing.items() if key != "user_id"
        }
        if msg.req_method.value == "chat.send":
            return
        if delegated:
            assert msg.metadata["ws_id"] == first._jiuwen_ws_id
            assert all(key.session_id != msg.session_id for key in binding.ws_channel._clients_by_key)
        assert not server.is_session_bound_to_client("tui", msg.session_id, first)
        response = SimpleNamespace(
            type="res", channel_id="tui", id=msg.id, session_id=msg.session_id,
            ok=msg.session_id != "rejected", payload={"session_id": msg.session_id},
        )
        await (binding.ws_channel or server).send(response)

    server.on_message(switched)
    connections = [asyncio.create_task(server._connection_handler(ws)) for ws in sockets]

    def entry(project_id, revision=1):
        return {
            "event": "project.lifecycle", "kind": kind, "completed": True,
            "payload": {"resource_id": project_id, "project_id": project_id, "revision": revision},
            "result": {"deleted": True},
        }

    scope = "g1" if routed else ""
    blocked = {
        "event": "session.lifecycle", "kind": "delete", "completed": False,
        "payload": {"resource_id": "s4", "project_id": "p1", "revision": 1, "execution_blocked": True},
        "result": {"session_id": "s4", "finalizing": True},
    }

    async def poll(events):
        snapshots[scope].put_nowait(events)
        await asyncio.wait_for(processed.get(), 2)

    try:
        await asyncio.wait_for(asyncio.gather(*(ws.bound.wait() for ws in sockets)), 2)
        assert first._web_routing == {
            "user_id": "u1", **({"group_id": "g1", "bot_id": "b1", "gateway_id": "gw1"} if routed else {}),
        }
        ready.set()
        # Only pump the first tenant's watcher; it must not deliver to the third window.
        await poll([entry("p1"), entry("p2"), blocked])
        assert [f["payload"]["project_id"] for f in first.project_events] == ["p1", "p1"]
        assert [f["payload"]["project_id"] for f in second.project_events] == ["p2", "p2"]
        assert third.project_events == []
        suffix = {"archive": "archived", "unarchive": "unarchived", "delete": "deleted"}[kind]
        assert [f["event"] for f in first.project_events] == ["project.lifecycle", f"project.{suffix}"]
        expected_metadata = {"user_id": "u1"}
        if routed:
            expected_metadata["routing"] = {"group_id": "g1", "bot_id": "b1", "gateway_id": "gw1"}
        assert all(call["metadata"] == expected_metadata for call in metadata_calls if call["session_id"] != "s3")

        await poll([entry("p1"), entry("p2")])
        assert len(first.project_events) == len(second.project_events) == 2

        # A transient metadata failure must neither broadcast nor consume the revision.
        metadata_failures.add("s1")
        await poll([entry("p1", 2)])
        assert len(first.project_events) == 2
        metadata_failures.clear()
        await poll([entry("p1", 2)])
        assert len(first.project_events) == 4
        assert len(second.project_events) == 2

        # Session moves change subscriptions; stale membership must not survive a move.
        project_by_session["s2"] = "p3"
        await poll([entry("p2", 2), entry("p3")])
        assert [f["payload"]["project_id"] for f in second.project_events] == ["p2", "p2", "p3", "p3"]

        # Deletion removes metadata before its final project event is polled.
        project_by_session.pop("s1")
        await poll([entry("p1", 3)])
        assert len(first.project_events) == 6
        assert third.project_events == []

        # A later connection gets the snapshot even when its shared watcher is old.
        late = _Socket("s4", query)
        project_by_session["s4"] = "p1"
        sockets.append(late)
        connections.append(asyncio.create_task(server._connection_handler(late)))
        await asyncio.wait_for(late.bound.wait(), 2)
        await poll([entry("p1", 3), blocked])
        assert len(late.project_events) == 2
        assert len(first.project_events) == 6
        await poll([entry("p1", 3), blocked])
        assert [f["event"] for f in late.frames if f.get("event") == "session.lifecycle"] == ["session.lifecycle"]

        # The same project ID in another tenant is polled and delivered separately.
        snapshots["g2"].put_nowait([entry("p1", 99)])
        await asyncio.wait_for(processed.get(), 2)
        assert len(third.project_events) == 2
        assert len(first.project_events) == 6
        assert len(late.project_events) == 2
        third_calls = [call for call in metadata_calls if call["session_id"] == "s3"]
        assert third_calls and all(call["metadata"] == {
            "user_id": "u1", "routing": {"group_id": "g2", "bot_id": "b2", "gateway_id": "gw2"},
        } for call in third_calls)

        # Immediate project-restored notifications use the same project boundary.
        await route.local_handlers["session.unarchive"](
            first, "restore", {"session_ids": ["s1"]}, "s1", "u1",
        )
        assert first.project_events[-1]["event"] == "project.restored"
        assert len(first.project_events) == 7
        assert len(second.project_events) == 4
        assert len(third.project_events) == 2
        assert len(late.project_events) == 3

        # Switching a real connection must release its old Session and project.
        # Switch back as well: a previously used Session must bind afresh.
        project_by_session["s1"] = "p1"
        project_by_session["s5"] = "p5"
        before_metadata = len(metadata_calls)
        first.bound.clear()
        first.incoming.put_nowait(json.dumps({
            "type": "req", "id": "rejected", "method": "session.switch", "params": {"session_id": "rejected"},
        }))
        await asyncio.wait_for(first.bound.wait(), 2)
        assert first.frames[-1]["ok"] is False
        assert server.is_session_bound_to_client("tui", "s1", first)
        assert not server.is_session_bound_to_client("tui", "rejected", first)
        assert first._tui_lifecycle_session_projects == {"s1": "p1"}
        assert "s1" in server.get_active_session_ids("tui", exclude_ws=second)
        assert server._pending_tui_switches == {}
        assert len(metadata_calls) == before_metadata
        if delegated:
            assert all(key.session_id != "rejected" for key in binding.ws_channel._clients_by_key)
        before = len(first.project_events)
        await poll([entry("p1", 4), entry("rejected-project", 4)])
        assert [f["payload"]["project_id"] for f in first.project_events[before:]] == ["p1", "p1"]
        for sid, project_id, revision in [("s5", "p5", 4), ("s1", "p1", 5)]:
            first.bound.clear()
            first.incoming.put_nowait(json.dumps({
                "type": "req", "id": f"switch-{sid}", "method": "session.switch",
                "params": {"session_id": sid},
            }))
            await asyncio.wait_for(first.bound.wait(), 2)
            assert {
                key[1] for key, owner in server._session_to_client.items() if owner is first
            } == {sid}
            assert first._tui_lifecycle_session_projects == {sid: project_id}
            before = len(first.project_events)
            await poll([entry("p1", revision), entry("p5", revision)])
            assert [f["payload"]["project_id"] for f in first.project_events[before:]] == [project_id] * 2

        # Preview is a successful read, not a foreground Session switch.
        before_metadata = len(metadata_calls)
        first.bound.clear()
        first.incoming.put_nowait(json.dumps({
            "type": "req", "id": "preview", "method": "session.preview", "params": {"session_id": "s5"},
        }))
        await asyncio.wait_for(first.bound.wait(), 2)
        assert first.frames[-1]["ok"] is True
        assert server.is_session_bound_to_client("tui", "s1", first)
        assert not server.is_session_bound_to_client("tui", "s5", first)
        assert first._tui_lifecycle_session_projects == {"s1": "p1"}
        assert len(metadata_calls) == before_metadata
        before = len(first.project_events)
        await poll([entry("p1", 6), entry("p5", 6)])
        assert [f["payload"]["project_id"] for f in first.project_events[before:]] == ["p1", "p1"]
        first.bound.clear()
        first.incoming.put_nowait(json.dumps({
            "type": "req", "id": "chat", "method": "chat.send", "params": {"session_id": "s1"},
        }))
        await asyncio.wait_for(first.bound.wait(), 2)
        assert first.frames[-1]["ok"] is True

        # Switch while this window's lookup is pending, then while another
        # window's lookup holds up delivery of an already-resolved membership.
        for held_sid, target_sid, project_id, revision in [("s1", "s5", "p5", 7), ("s2", "s1", "p1", 8)]:
            started, release = asyncio.Event(), asyncio.Event()
            metadata_pauses[held_sid] = (started, release)
            before = len(first.project_events)
            pending_poll = asyncio.create_task(poll([entry("p1", revision), entry("p5", revision)]))
            await asyncio.wait_for(started.wait(), 2)
            try:
                first.bound.clear()
                first.incoming.put_nowait(json.dumps({
                    "type": "req", "id": f"race-{revision}", "method": "session.switch",
                    "params": {"session_id": target_sid},
                }))
                await asyncio.wait_for(first.bound.wait(), 2)
                assert first.frames[-1]["ok"] is True
            finally:
                release.set()
                await asyncio.wait_for(pending_poll, 2)
            assert first.project_events[before:] == []
            assert first._tui_lifecycle_session_projects == {target_sid: project_id}
            await poll([entry("p1", revision), entry("p5", revision)])
            assert [f["payload"]["project_id"] for f in first.project_events[before:]] == [project_id] * 2
            if delegated and target_sid == "s5":
                await server._handle_raw_message(first, json.dumps({
                    "type": "req", "id": "old-in-flight", "method": "chat.send", "params": {"session_id": "s5"},
                }), "/tui", route)
                in_flight_id = forwarded[-1].id

        if delegated:
            # Another window can take the released Session without its replies
            # leaking to this window. Old requests retain their explicit owner.
            channel = binding.ws_channel
            second.bound.clear()
            await server._handle_raw_message(second, json.dumps({
                "type": "req", "id": "other-chat", "method": "chat.send", "params": {"session_id": "s5"},
            }), "/tui", route)
            assert {key.session_id for key, owners in channel._clients_by_key.items() if first in owners} == {"s1"}
            for req_id in ("other-untracked", in_flight_id):
                await channel.send(SimpleNamespace(
                    type="res", channel_id="tui", id=req_id, session_id="s5", ok=True, payload={},
                ))
            await channel.send(SimpleNamespace(
                type="event", channel_id="tui", id=in_flight_id, session_id="s5",
                event_type=SimpleNamespace(value="chat.delta"), payload={"content": "old-stream", "request_id": in_flight_id},
            ))
            await channel.send(SimpleNamespace(
                type="res", channel_id="tui", id="socket-barrier", session_id="s5", ok=True, payload={},
            ), routing_target=RoutingTarget(intent="godview", delivery=TuiDeliveryTarget(ws_id=first._jiuwen_ws_id)))

            async def delivered_to_both():
                while not (
                    any(f.get("id") == "socket-barrier" for f in first.frames)
                    and any(f.get("id") == "other-untracked" for f in second.frames)
                ):
                    await original_sleep(0)

            await asyncio.wait_for(delivered_to_both(), 2)
            assert not any(f.get("id") == "other-untracked" for f in first.frames)
            assert any(f.get("id") == "old-in-flight" for f in first.frames)
            assert not any(f.get("id") == "old-in-flight" for f in second.frames)
            assert any(f.get("payload", {}).get("content") == "old-stream" for f in first.frames)
            assert next(f["payload"]["request_id"] for f in first.frames if f.get("payload", {}).get("content") == "old-stream") == "old-in-flight"
            assert not any(f.get("payload", {}).get("content") == "old-stream" for f in second.frames)
    finally:
        for ws in sockets:
            ws.incoming.put_nowait(None)
        await asyncio.gather(*connections)
        watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(("delete_method", "exists_at_bind"), [
    (None, True),
    ("session.delete", False), ("session.delete", True),
    ("project.sessions.delete_archived", False), ("project.sessions.delete_archived", True),
])
async def test_tui_deletion_before_first_poll_preserves_project_membership(
    monkeypatch, delete_method, exists_at_bind,
):
    ws = _Socket("s1")
    ws._gateway_user_id = "u1"
    projects = {"s1": "p1"} if exists_at_bind else {}
    poll_ready = asyncio.Event()
    delivered = asyncio.Event()

    async def fake_call(**kwargs):
        method = kwargs["req_method"].value
        sid = kwargs["params"].get("session_id")
        if method == "session.get_metadata":
            project_id = projects.get(sid)
            return (True, {"project_id": project_id}) if project_id else (False, {"code": "NOT_FOUND"})
        if method == delete_method:
            assert ws._tui_lifecycle_session_projects == {"s1": "p1"}
            assert projects.pop("s1") == "p1"
            return True, {"results": [{"session_id": "s1", "project_id": "p1", "ok": True}]}
        assert method == "project.lifecycle"
        await poll_ready.wait()
        return True, {"events": [{
            "event": "project.lifecycle", "kind": "delete", "completed": True,
            "payload": {"resource_id": "p1", "project_id": "p1", "revision": 1},
            "result": {"deleted": True},
        }]}

    async def stop_after_poll(delay):
        delivered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", stop_after_poll)
    binding = build_cli_route_binding(CliRouteBindParams(agent_client=object()))
    route = RouteConfig(path="/tui", channel_id="tui", session_bind_handler=binding.session_bind_handler)
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    try:
        await server._bind_route_session_client(route, "s1", ws)
        assert ws.project_events == []
        if delete_method is None:
            projects.pop("s1")  # Another client deletes it before the first snapshot.
        else:
            projects["s1"] = "p1"  # Creation may have completed after binding.
            params = {"session_id": "s1"} if delete_method == "session.delete" else {"project_id": "p1"}
            await route.local_handlers[delete_method](ws, "delete", params, "s1", "u1")
        poll_ready.set()
        await asyncio.wait_for(delivered.wait(), 2)
        assert [frame["event"] for frame in ws.project_events] == ["project.lifecycle", "project.deleted"]
    finally:
        watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive", "session.delete"])
async def test_tui_lifecycle_frame_targets_session_ids_not_the_ambient_session_id(monkeypatch, method):
    """The TUI client adds its active Session as session_id to every request, so
    /archive, /unarchive and /delete arrive with both fields."""
    ws = _Socket("active")
    ws._gateway_user_id = "u1"
    forwarded = []

    async def fake_call(**kwargs):
        name = kwargs["req_method"].value
        if name == "session.get_metadata":
            return False, {"code": "NOT_FOUND"}
        if name == "project.lifecycle":
            await asyncio.Event().wait()
        forwarded.append((name, kwargs["params"]))
        return True, {"succeeded_count": 1, "failed_count": 0, "results": [
            {"session_id": "target", "ok": True, "accepted": True, "finalizing": True},
        ]}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    binding = build_cli_route_binding(CliRouteBindParams(agent_client=object()))
    route = RouteConfig(path="/tui", channel_id="tui", session_bind_handler=binding.session_bind_handler)
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    try:
        await server._bind_route_session_client(route, "active", ws)
        await server._handle_raw_message(ws, json.dumps({
            "type": "req", "id": "tui_1", "method": method, "timeout_ms": 30000,
            "params": {"session_ids": ["target"], "session_id": "active", "project_dir": "/p", "cwd": "/p"},
        }), "/tui", route)
        response = [frame for frame in ws.frames if frame.get("type") == "res"][-1]
        assert (response["id"], response["ok"]) == ("tui_1", True)
        assert forwarded == [(method, {"session_ids": ["target"], "project_dir": "/p", "cwd": "/p"})]
        assert server.is_session_bound_to_client("tui", "active", ws)
        assert not server.is_session_bound_to_client("tui", "target", ws)
    finally:
        watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)


async def _watched_window(monkeypatch, projects, failing=frozenset()):
    """Bind one real TUI window to ``s1``; ``poll(events)`` pumps one watcher poll."""
    ws = _Socket("s1")
    ws._gateway_user_id = "u1"
    metadata_calls = []
    snapshots = asyncio.Queue()
    processed = asyncio.Queue()
    original_sleep = asyncio.sleep

    async def fake_call(**kwargs):
        if kwargs["req_method"].value == "session.get_metadata":
            sid = kwargs["params"]["session_id"]
            metadata_calls.append(sid)
            if sid in failing:
                return False, {"code": "SERVICE_UNAVAILABLE"}
            project_id = projects.get(sid)
            return (True, {"project_id": project_id}) if project_id else (False, {"code": "NOT_FOUND"})
        assert kwargs["req_method"].value == "project.lifecycle"
        return True, {"events": await snapshots.get()}

    async def poll_again(delay):
        processed.put_nowait(True)
        await original_sleep(0)

    async def poll(events):
        snapshots.put_nowait(events)
        await asyncio.wait_for(processed.get(), 2)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    binding = build_cli_route_binding(CliRouteBindParams(agent_client=object()))
    route = RouteConfig(path="/tui", channel_id="tui", session_bind_handler=binding.session_bind_handler)
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    await server._bind_route_session_client(route, "s1", ws)
    return ws, metadata_calls, poll


async def _cancel_watchers():
    watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
    for task in watchers:
        task.cancel()
    await asyncio.gather(*watchers, return_exceptions=True)


def _project_deleted(project_id, revision):
    return {
        "event": "project.lifecycle", "kind": "delete", "completed": True,
        "payload": {"resource_id": project_id, "project_id": project_id, "revision": revision},
        "result": {"deleted": True},
    }


@pytest.mark.asyncio
async def test_idle_polls_reuse_the_window_project_membership(monkeypatch):
    """The watcher polls every 2 s; an idle window must not query AgentServer each time."""
    failing = set()
    ws, metadata_calls, poll = await _watched_window(monkeypatch, {"s1": "p1"}, failing)
    try:
        assert len(metadata_calls) == 1  # Bind-time lookup.
        await poll([_project_deleted("p1", 1)])  # New window, new revision.
        assert len(metadata_calls) == 2
        assert [frame["event"] for frame in ws.project_events] == ["project.lifecycle", "project.deleted"]
        for _ in range(5):
            await poll([_project_deleted("p1", 1)])
        assert len(metadata_calls) == 2
        assert len(ws.project_events) == 2

        await poll([_project_deleted("p1", 2)])  # A new project revision forces a lookup.
        assert len(metadata_calls) == 3
        assert len(ws.project_events) == 4

        # A failed lookup delivers nothing and is retried even with nothing new.
        failing.add("s1")
        await poll([_project_deleted("p1", 3)])
        assert (len(metadata_calls), len(ws.project_events)) == (4, 4)
        failing.clear()
        await poll([_project_deleted("p1", 3)])
        assert (len(metadata_calls), len(ws.project_events)) == (5, 6)
        await poll([_project_deleted("p1", 3)])
        assert (len(metadata_calls), len(ws.project_events)) == (5, 6)

        # Membership older than the refresh interval is looked up again.
        monkeypatch.setattr(tui_connect, "_TUI_LIFECYCLE_MEMBERSHIP_REFRESH_SECONDS", 0.0)
        await poll([_project_deleted("p1", 3)])
        assert (len(metadata_calls), len(ws.project_events)) == (6, 6)
    finally:
        await _cancel_watchers()


@pytest.mark.asyncio
async def test_session_created_after_binding_is_resolved_by_the_following_polls(monkeypatch):
    """A window can bind before session.create finishes. Its project must be known
    before another client can delete it, so polls keep asking until it resolves."""
    projects = {}
    ws, metadata_calls, poll = await _watched_window(monkeypatch, projects)
    try:
        await poll([])
        await poll([])
        assert len(metadata_calls) == 3  # Bind, then every poll while unknown.
        projects["s1"] = "p1"
        await poll([])
        assert len(metadata_calls) == 4
        assert ws._tui_lifecycle_session_projects == {"s1": "p1"}
        await poll([])
        await poll([])
        assert len(metadata_calls) == 4
        projects.pop("s1")  # Deleted elsewhere; its last project still gets the event.
        await poll([_project_deleted("p1", 1)])
        assert [frame["event"] for frame in ws.project_events] == ["project.lifecycle", "project.deleted"]
    finally:
        await _cancel_watchers()


@pytest.mark.asyncio
async def test_bind_hook_finds_a_window_bound_under_a_three_part_key(monkeypatch):
    """Binding keys may carry an agent_ref part. The hook must still keep one
    active Session per window and resolve its project membership."""
    async def fake_call(**kwargs):
        if kwargs["req_method"].value == "session.get_metadata":
            return True, {"project_id": "p1"}
        await asyncio.Event().wait()

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    binding = build_cli_route_binding(CliRouteBindParams(agent_client=object()))
    route = RouteConfig(path="/tui", channel_id="tui", session_bind_handler=binding.session_bind_handler)
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    ws = _Socket("s1")
    ws._gateway_user_id = "u1"
    server._session_to_client[("tui", "old")] = ws
    server._session_to_client[("tui", "s1", "code:default")] = ws
    try:
        await binding.session_bind_handler("tui", "s1")
        assert [key for key, owner in server._session_to_client.items() if owner is ws] == [
            ("tui", "s1", "code:default"),
        ]
        assert ws._tui_lifecycle_session_projects == {"s1": "p1"}
    finally:
        await _cancel_watchers()


async def _until(condition, attempts=100):
    for _ in range(attempts):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition not reached")


@pytest.mark.asyncio
async def test_overlapping_membership_lookups_share_the_project_cache(monkeypatch):
    captured = {}
    original_register = lifecycle_handlers.register_lifecycle_handlers

    def capture(*args, **kwargs):
        captured.update(kwargs)
        return original_register(*args, **kwargs)

    projects = {}
    gates = []

    async def fake_call(**kwargs):
        assert kwargs["req_method"].value == "session.get_metadata"
        gate = asyncio.Event()
        gates.append(gate)
        await gate.wait()
        project_id = projects.get(kwargs["params"]["session_id"])
        return (True, {"project_id": project_id}) if project_id else (False, {"code": "NOT_FOUND"})

    monkeypatch.setattr(lifecycle_handlers, "register_lifecycle_handlers", capture)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_call)
    binding = build_cli_route_binding(CliRouteBindParams(agent_client=object()))
    route = RouteConfig(path="/tui", channel_id="tui")
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    binding.install(server)
    project_ids_for_client = captured["project_ids_for_client"]
    ws = _Socket("s1")
    server._session_to_client[("tui", "s1")] = ws

    projects["s1"] = "p1"  # A new Session, persisted after its bind-time lookup missed.
    refresh = asyncio.create_task(project_ids_for_client("u1", ws))  # Pre-delete refresh.
    await _until(lambda: len(gates) == 1)
    poll = asyncio.create_task(project_ids_for_client("u1", ws))  # Overlapping watcher poll.
    await _until(lambda: len(gates) == 2)
    gates[0].set()
    assert await refresh == {"p1"}
    projects.pop("s1")  # The deletion lands before the poll's lookup is answered.
    gates[1].set()

    assert await poll == {"p1"}
    assert ws._tui_lifecycle_session_projects == {"s1": "p1"}


@pytest.mark.asyncio
async def test_promoted_tui_client_gets_a_new_binding_generation():
    route = RouteConfig(path="/tui", channel_id="tui")
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    holder, waiter = _Socket("s1"), _Socket("s1")
    assert await server._bind_route_session_client(route, "s1", holder)
    assert not await server._bind_route_session_client(route, "s1", waiter)  # Queued behind the holder.
    generation = getattr(waiter, "_lifecycle_binding_generation", 0)
    holder.closed = True
    await server._promote_pending_session_client(route, ("tui", "s1"))
    assert server._session_to_client[("tui", "s1")] is waiter
    # Lookups captured before the promotion must be invalidated, as for a direct bind.
    assert waiter._lifecycle_binding_generation == generation + 1


@pytest.mark.asyncio
async def test_tui_handshake_routing_uses_headers_and_authenticated_user():
    route = RouteConfig(path="/tui", channel_id="tui")
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), SimpleNamespace())
    ws = _Socket("s1", "?user_id=spoofed&group_id=query&bot_id=query&gateway_id=query", {
        "X-Group-Id": "group", "X-Bot-Id": "bot", "X-Gateway-Id": "gateway",
    })
    ws.incoming = asyncio.Queue()
    ws.incoming.put_nowait(None)

    def authenticate(connection):
        connection._gateway_user_id = "authenticated"

    server._connect_hooks.append(authenticate)
    await server._connection_handler(ws)
    assert ws._web_routing == {
        "user_id": "authenticated", "group_id": "group", "bot_id": "bot", "gateway_id": "gateway",
    }
