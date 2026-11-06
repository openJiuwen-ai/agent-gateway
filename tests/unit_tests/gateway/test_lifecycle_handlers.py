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
async def test_single_session_lifecycle_accepts_session_id(monkeypatch, method):
    channel = _FakeChannel()
    calls = []

    async def fake_call(**kwargs):
        calls.append(kwargs)
        return True, {}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    lifecycle_handlers.register_lifecycle_handlers(channel, lambda: None, lambda: None)
    await channel.methods[method](object(), "req", {"session_id": "s1"}, None, "u1")

    assert calls[0]["params"] == {"session_id": "s1"}
    assert channel.responses[-1]["ok"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["session.archive", "session.unarchive"])
@pytest.mark.parametrize("session_id", ["", None, 123])
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
    assert channel.responses[-1]["error"] == "session_id is required"
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

    async def fake_call(**kwargs):
        nonlocal polls
        polls += 1
        return True, {"events": [{
            "event": "project.lifecycle", "kind": kind, "completed": True,
            "payload": {"resource_id": "p1", "project_id": "p1", "revision": polls},
            "result": {"deleted": True},
        }]}

    async def poll_again(delay):
        if polls == 2:
            channel.clients.clear()
        await original_sleep(0)

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_call)
    monkeypatch.setattr(lifecycle_handlers.asyncio, "sleep", poll_again)
    lifecycle_handlers.register_lifecycle_handlers(
        channel, lambda: None, lambda: None,
        clients_for_user=lambda user_id: channel.clients,
        session_event_clients=lambda user_id, session_id: [],
    )
    channel.ensure_lifecycle_watch("u1", first)
    watcher = next(task for task in asyncio.all_tasks() if task.get_name() == "web-lifecycle-events")
    await asyncio.wait_for(watcher, 1)

    suffix = {"archive": "archived", "unarchive": "unarchived", "delete": "deleted"}[kind]
    assert [(ws is first, ws is second, event) for ws, event, _ in channel.events] == [
        (True, False, "project.lifecycle"), (True, False, f"project.{suffix}"),
        (False, True, "project.lifecycle"), (False, True, f"project.{suffix}"),
    ]
