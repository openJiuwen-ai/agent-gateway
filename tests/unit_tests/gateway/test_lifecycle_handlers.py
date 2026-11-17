"""Keep the Gateway busy guard until lifecycle cleanup actually completes."""

from __future__ import annotations

import asyncio

import pytest

from agent_gateway.channel_manager.web import lifecycle_handlers


class _FakeChannel:
    channel_id = "web"

    def __init__(self) -> None:
        self.methods: dict[str, object] = {}
        self.responses: list[dict] = []
        self.cleared: list[str] = []
        self.clients: list[object] = []
        self.events: list[tuple[object, str, dict]] = []

    def register_method(self, method, handler) -> None:
        self.methods[method] = handler

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None) -> None:
        self.responses.append(
            {"req_id": req_id, "ok": ok, "payload": payload, "error": error, "code": code}
        )

    async def send_event(self, ws, event, payload) -> None:
        self.events.append((ws, event, payload))

    def clear_session_busy(self, session_id: str) -> None:
        self.cleared.append(session_id)

    def connection_user_id(self, ws) -> str:
        return "u1"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", [
    "session.archive", "session.delete", "project.sessions.archive", "project.sessions.delete_archived",
])
async def test_accepted_lifecycle_keeps_busy_flags(monkeypatch, method):
    channel = _FakeChannel()
    payload = {
        "succeeded_count": 2,
        "failed_count": 0,
        "results": [
            {"session_id": "s1", "ok": True, "accepted": True, "finalizing": True, "stop_pending": True},
            {"session_id": "s2", "ok": True, "accepted": True},
        ],
    }

    async def fake_call(*args, **kwargs):
        return True, payload

    monkeypatch.setattr(
        lifecycle_handlers, "fetch_agent_unary", fake_call
    )
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    handler = channel.methods[method]
    await handler(object(), "req-1", {"session_ids": ["s1", "s2"]}, None, "u1")

    assert channel.cleared == []
    assert channel.responses[-1]["ok"] is True


@pytest.mark.asyncio
async def test_completed_archive_clears_busy_flags(monkeypatch):
    channel = _FakeChannel()

    async def fake_call(*args, **kwargs):
        return True, {"results": [
            {"session_id": "s1", "ok": True, "accepted": True, "finalizing": False, "stop_pending": False},
            {"session_id": "s2", "ok": False},
        ]}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods["session.archive"](object(), "req", {"session_ids": ["s1", "s2"]}, None, "u1")

    assert channel.cleared == ["s1"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "event"),
    [
        ("session.archive", "session.archived"),
        ("session.delete", "session.deleted"),
    ],
)
async def test_pending_lifecycle_does_not_emit_closed_event(monkeypatch, method, event):
    channel = _FakeChannel()
    ws = object()

    async def fake_call(*args, **kwargs):
        return True, {
            "results": [
                {
                    "session_id": "pending",
                    "ok": True,
                    "accepted": True,
                    "finalizing": True,
                    "stop_pending": True,
                },
                {
                    "session_id": "completed",
                    "ok": True,
                    "accepted": True,
                    "finalizing": False,
                    "stop_pending": False,
                },
            ]
        }

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods[method](ws, "req", {"session_ids": ["pending", "completed"]}, None, "u1")

    assert [(sent_event, payload["session_id"]) for _, sent_event, payload in channel.events] == [
        (event, "completed")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive"])
@pytest.mark.parametrize(
    ("params", "expected_ids"),
    [
        ({"session_id": "s1"}, ["s1"]),
        ({"session_ids": ["s1"]}, ["s1"]),
        ({"session_ids": ["s1", "s2"]}, ["s1", "s2"]),
    ],
)
async def test_session_lifecycle_forwards_canonical_session_ids(
    monkeypatch, method, params, expected_ids
):
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    original_params = dict(params)
    await channel.methods[method](object(), "req", params, None, "u1")

    assert len(calls) == 1
    assert calls[0]["params"] == {"session_ids": expected_ids}
    assert params == original_params
    assert channel.responses[-1]["ok"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive", "session.delete"])
@pytest.mark.parametrize("session_id", ["", None, 123, " \t\n"])
async def test_single_session_lifecycle_rejects_invalid_session_id(
    monkeypatch, method, session_id
):
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods[method](
        object(), "req", {"session_id": session_id}, None, "u1"
    )

    assert calls == []
    assert channel.responses[-1]["ok"] is False
    assert channel.responses[-1]["error"] == "invalid session_id"
    assert channel.responses[-1]["code"] == "BAD_REQUEST"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive", "session.delete"])
@pytest.mark.parametrize("ambient", ["active-session", "s2", None, "", 123])
async def test_session_ids_take_precedence_over_an_accompanying_session_id(monkeypatch, method, ambient):
    """The TUI attaches its active Session as session_id to every request. Beside
    session_ids it is not a target and must not be forwarded: AgentServer adds
    session_id to the delete targets."""
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    params = {"session_id": ambient, "session_ids": ["s2"], "cwd": "/project"}
    original_params = dict(params)
    await channel.methods[method](object(), "req", params, "active-session", "u1")

    assert [call["params"] for call in calls] == [{"session_ids": ["s2"], "cwd": "/project"}]
    assert params == original_params
    assert channel.responses[-1]["ok"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive", "session.delete"])
@pytest.mark.parametrize(
    ("ids", "error"),
    [
        (None, "session_ids must contain 1 to 100 IDs"),
        ([], "session_ids must contain 1 to 100 IDs"),
        (["s%d" % index for index in range(101)], "session_ids must contain 1 to 100 IDs"),
        (["s2", ""], "invalid resource ID"),
        (["s2", 7], "invalid resource ID"),
    ],
)
async def test_invalid_session_ids_do_not_fall_back_to_session_id(monkeypatch, method, ids, error):
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods[method](
        object(), "req", {"session_id": "s1", "session_ids": ids}, None, "u1"
    )

    assert calls == []
    assert channel.responses[-1]["ok"] is False
    assert channel.responses[-1]["code"] == "BAD_REQUEST"
    assert channel.responses[-1]["error"] == error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "error"),
    [
        ("session.archive", "session_ids is required"),
        ("session.unarchive", "session_ids is required"),
        ("session.delete", "session_id is required"),
    ],
)
@pytest.mark.parametrize("fallback_session_id", [None, "req-1"])
async def test_session_lifecycle_rejects_missing_ids(monkeypatch, method, error, fallback_session_id):
    """Channels pass a request-scoped session_id when params omit one; it is not a target."""
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods[method](object(), "req-1", {}, fallback_session_id, "u1")

    assert calls == []
    assert channel.responses[-1]["ok"] is False
    assert channel.responses[-1]["error"] == error
    assert channel.responses[-1]["code"] == "BAD_REQUEST"


@pytest.mark.asyncio
@pytest.mark.parametrize("already_completed", [False, True])
async def test_lifecycle_completion_poll_clears_busy_flags(monkeypatch, already_completed):
    channel = _FakeChannel()
    channel.clients.append(object())
    polls = 0
    original_sleep = asyncio.sleep

    async def fake_call(*args, **kwargs):
        nonlocal polls
        polls += 1
        completed = already_completed or polls > 1
        return True, {"events": [{
            "event": "session.lifecycle",
            "kind": "archive",
            "completed": completed,
            "payload": {"resource_id": "s1", "project_id": "p1", "revision": 2 if completed else 1},
            "result": {"session_id": "s1", "ok": True},
        }]}

    async def poll_again(delay):
        if already_completed or polls > 1:
            channel.clients.clear()
        else:
            assert channel.cleared == []
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    channel.ensure_lifecycle_watch("u1")
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)

    assert channel.cleared == ["s1"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["archive", "delete"])
@pytest.mark.parametrize("pending_on_first_poll", [False, True])
@pytest.mark.parametrize("pending", [
    {"finalizing": True, "stop_pending": False},
    {"finalizing": False, "stop_pending": True},
    {"finalizing": True, "stop_pending": True},
    {"accepted": True},
])
async def test_completed_poll_waits_for_pending_cleanup(
    monkeypatch, kind, pending_on_first_poll, pending
):
    channel = _FakeChannel()
    ws = object()
    channel.clients.append(ws)
    pending_entry = {
        "event": "session.lifecycle",
        "kind": kind,
        "completed": True,
        "payload": {
            "resource_id": "s1", "project_id": "p1", "revision": 1,
            "execution_blocked": True,
        },
        "result": {"session_id": "s1", "ok": True, **pending},
    }
    completed_entry = {
        **pending_entry,
        "payload": {**pending_entry["payload"], "revision": 2},
        "result": {
            "session_id": "s1", "ok": True, "accepted": True,
            "finalizing": False, "stop_pending": False,
        },
    }
    snapshots = [[pending_entry], [pending_entry], [completed_entry], [completed_entry]]
    if not pending_on_first_poll:
        snapshots.insert(0, [])
    polls = 0
    original_sleep = asyncio.sleep

    async def fake_call(*args, **kwargs):
        nonlocal polls
        events = snapshots[polls]
        polls += 1
        return True, {"events": events}

    async def poll_again(delay):
        if polls == len(snapshots):
            channel.clients.clear()
        elif polls < len(snapshots) - 1:
            assert channel.cleared == []
            assert all(event == "session.lifecycle" for _, event, _ in channel.events)
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(
        channel, lambda: None, lambda: None,
        session_event_clients=lambda user_id, session_id: channel.clients,
    )
    channel.ensure_lifecycle_watch("u1")
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)

    assert polls == len(snapshots)
    assert channel.cleared == ["s1"]
    closed_event = "session.archived" if kind == "archive" else "session.deleted"
    assert [event for _, event, _ in channel.events] == [
        "session.lifecycle", "session.lifecycle", closed_event,
    ]
    assert channel.events[-1][2] == {**completed_entry["result"], "project_id": "p1"}


@pytest.mark.asyncio
async def test_failed_archive_does_not_clear_busy_flags(monkeypatch):
    channel = _FakeChannel()

    async def fake_call(*args, **kwargs):
        return False, {
            "succeeded_count": 0,
            "failed_count": 1,
            "results": [{"session_id": "s1", "ok": False, "code": "SESSION_BUSY"}],
        }

    monkeypatch.setattr(
        lifecycle_handlers, "fetch_agent_unary", fake_call
    )
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    handler = channel.methods["session.archive"]
    await handler(object(), "req-2", {"session_ids": ["s1"]}, None, "u1")

    assert channel.cleared == []


@pytest.mark.asyncio
async def test_lifecycle_watch_uses_and_isolates_connection_tenant(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    channel = _FakeChannel()
    same = SimpleNamespace(_web_routing={"user_id": "u1", "group_id": "g1", "bot_id": "b1"})
    other = SimpleNamespace(_web_routing={"user_id": "u1", "group_id": "g2", "bot_id": "b2"})
    channel.clients = [same, other]
    channel.send_event = AsyncMock()
    calls = []
    original_sleep = asyncio.sleep

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {"events": [{
            "event": "session.lifecycle", "kind": "archive", "completed": True,
            "payload": {"resource_id": "s1", "project_id": "p1", "revision": len(calls)},
            "result": {"session_id": "s1", "ok": True},
        }]}

    async def poll_again(delay):
        if len(calls) >= 2:
            channel.clients.clear()
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: object(), lambda: None)
    channel.ensure_lifecycle_watch("u1", same)
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)
    assert len(calls) == 2
    assert all(call["metadata"] == {"user_id": "u1", "routing": {"group_id": "g1", "bot_id": "b1"}} for call in calls)
    assert channel.send_event.await_count == 2
    assert all(call.args[0] is same for call in channel.send_event.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["archive", "unarchive", "delete"])
async def test_tui_project_events_reach_only_clients_in_watcher_scope(monkeypatch, kind):
    from types import SimpleNamespace

    channel = _FakeChannel()
    first = SimpleNamespace(_web_routing={"group_id": "g1"})
    second = SimpleNamespace(_web_routing={"group_id": "g1"})
    other = SimpleNamespace(_web_routing={"group_id": "g2"})
    channel.clients = [first, second, other]
    polls = 0
    original_sleep = asyncio.sleep

    async def project_ids_for_client(user_id, client_ws, refresh=True):
        return {"p1"}

    async def fake_call(**kwargs):
        nonlocal polls
        polls += 1
        return True, {"events": [{
            "event": "project.lifecycle", "kind": kind, "completed": True,
            "payload": {"resource_id": "p1", "project_id": "p1", "revision": 1},
            "result": {"deleted": True},
        }]}

    async def poll_again(delay):
        assert len(channel.events) == 4
        if polls == 2:
            channel.clients.clear()
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(
        channel, lambda: None, lambda: None,
        clients_for_user=lambda user_id: channel.clients,
        session_event_clients=lambda user_id, session_id: [],
        project_ids_for_client=project_ids_for_client,
    )
    channel.ensure_lifecycle_watch("u1", first)
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)

    suffix = {"archive": "archived", "unarchive": "unarchived", "delete": "deleted"}[kind]
    assert [(ws is first, ws is second, event) for ws, event, _ in channel.events] == [
        (True, False, "project.lifecycle"), (True, False, f"project.{suffix}"),
        (False, True, "project.lifecycle"), (False, True, f"project.{suffix}"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "params"), [
    ("session.delete", {"session_id": "s1"}),
    ("project.sessions.delete_archived", {"project_id": "p1"}),
    ("session.unarchive", {"session_ids": ["s1"]}),
])
async def test_failed_membership_lookup_does_not_fail_the_lifecycle_request(monkeypatch, caplog, method, params):
    """Project membership is context for later events. A lookup that raises must
    neither abort the request before it is forwarded nor answer it a second time."""
    channel = _FakeChannel()
    ws = object()
    channel.clients.append(ws)
    forwarded = []

    async def fake_call(**kwargs):
        name = kwargs["req_method"].value
        if name == "project.lifecycle":
            return False, {}
        forwarded.append(name)
        return True, {"results": [{"session_id": "s1", "project_id": "p1", "ok": True, "restored": True}]}

    async def project_ids_for_client(user_id, client_ws, refresh=True):
        raise RuntimeError("AgentServer unreachable")

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(
        channel, lambda: None, lambda: None, project_ids_for_client=project_ids_for_client,
    )
    try:
        with caplog.at_level("WARNING", logger=lifecycle_handlers.__name__):
            await channel.methods[method](ws, "req", params, None, "u1")
    finally:
        watchers = [task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events"]
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)

    assert forwarded == [method]
    assert [response["ok"] for response in channel.responses] == [True]
    assert "project membership lookup" in caplog.text
    assert not any(event == "project.restored" for _, event, _ in channel.events)


@pytest.mark.asyncio
async def test_replacement_socket_does_not_inherit_replay_through_a_recycled_id(monkeypatch):
    """CPython can give a new socket the id() of a socket that just closed."""
    channel = _FakeChannel()

    class _Socket:
        pass

    first, second = _Socket(), _Socket()
    channel.clients.append(first)
    builtin_id = id
    # Simulate the replacement window reusing the closed window's address.
    monkeypatch.setattr(
        lifecycle_handlers, "id",
        lambda obj: builtin_id(first) if obj is second else builtin_id(obj),
        raising=False,
    )
    polls = 0
    original_sleep = asyncio.sleep

    async def fake_call(*args, **kwargs):
        nonlocal polls
        polls += 1
        return True, {"events": [{
            "event": "project.lifecycle", "kind": "archive", "completed": False,
            "payload": {"resource_id": "p1", "project_id": "p1", "revision": 1},
            "result": {},
        }]}

    async def project_ids_for_client(user_id, ws, refresh=True):
        return {"p1"}

    async def poll_again(delay):
        if polls == 1:
            channel.clients[:] = [second]  # The first window closes; a new one opens.
        else:
            channel.clients.clear()
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(
        channel, lambda: None, lambda: None, project_ids_for_client=project_ids_for_client,
    )
    channel.ensure_lifecycle_watch("u1")
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)

    assert [(ws is first, ws is second, event) for ws, event, _ in channel.events] == [
        (True, False, "project.lifecycle"), (False, True, "project.lifecycle"),
    ]
