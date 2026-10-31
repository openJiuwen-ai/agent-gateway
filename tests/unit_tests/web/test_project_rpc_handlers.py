# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.project_rpc_handlers import (
    register_project_handlers,
    register_project_pinned_sessions_handler,
)
from agent_gateway.channel_manager.web.project_git_rpc_handlers import (
    register_project_git_handlers,
)


BASE_ROUTES = [
    "project.list",
    "project.info",
    "project.get_sessions",
    "project.get_cron_sessions",
    "project.create",
    "project.rename",
    "project.pin",
    "project.remove",
    "project.restore",
]

PINNED_ROUTES = ["project.pinned_sessions"]

GIT_ROUTES = [
    "project.git.status",
    "project.git.probe",
    "project.git.init",
    "project.git.switch_branch",
    "project.git.create_branch",
    "project.git.commit",
    "project.git.push",
    "project.git.diff_status",
    "project.git.turn_diff_list",
    "project.git.turn_diff",
]


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []
        self.events = []
        self.clients = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    def connection_user_id(self, ws):
        return getattr(ws, "user_id", None)

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})

    async def send_event(self, ws, event, payload):
        self.events.append({"ws": ws, "event": event, **payload})


def _register(agent_client=object(), cron_controller=object()):
    channel = _FakeChannel()
    _resolve = lambda ref: ref
    register_project_handlers(
        channel,
        agent_client,
        _resolve,
        cron_controller=cron_controller,
        _schedule_agent_prewarm_sync=lambda name: None,
    )
    return channel


def test_project_base_routes_are_registered_in_original_order():
    channel = _register()

    assert list(channel.methods) == BASE_ROUTES


def test_project_pinned_session_route_is_registered():
    channel = _FakeChannel()
    register_project_pinned_sessions_handler(channel, object(), lambda ref: ref)

    assert list(channel.methods) == PINNED_ROUTES


def test_project_git_routes_are_registered_in_original_order():
    channel = _FakeChannel()
    register_project_git_handlers(channel, object(), lambda ref: ref)

    assert list(channel.methods) == GIT_ROUTES


@pytest.mark.asyncio
async def test_project_create_runs_after_success(monkeypatch):
    from agent_gateway.channel_manager.web import project_rpc_handlers

    prewarm = []
    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)
        on_done = kwargs.get("on_done")
        if on_done:
            await on_done(True, {"restored": True, "project_id": "p1"})

    monkeypatch.setattr(project_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register()
    channel.methods["project.create"]._schedule_agent_prewarm_sync = lambda name: prewarm.append(name)

    await channel.methods["project.create"](
        object(), "rpc-1", {}, "session-1", user_id="u1"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["req_method"].value == "project.create"
    assert forwarded[0]["preserve_error_payload"] is True


@pytest.mark.asyncio
async def test_project_remove_rejects_default_project():
    channel = _register()

    await channel.methods["project.remove"](
        object(), "rpc-2", {"project_id": "default"}, "session-2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "default project cannot be removed",
            "code": "FORBIDDEN",
        }
    ]


@pytest.mark.asyncio
async def test_project_remove_success_cleans_watcher_busy_and_broadcasts(monkeypatch):
    from agent_gateway.channel_manager.web import project_rpc_handlers

    forwarded = []

    async def fetch_agent_unary(**kwargs):
        forwarded.append(kwargs)
        return True, {"stopped_sessions": ["s1", ""]}

    class cron:
        @staticmethod
        async def hide_project_jobs(project_id, commit):
            await commit()
            return {"stopped_cron_jobs": 2}

    class registry:
        def __init__(self):
            self.cleaned = []

        def cleanup_project(self, project_id):
            self.cleaned.append(project_id)

    class owner:
        user_id = "u1"

    monkeypatch.setattr(project_rpc_handlers, "fetch_agent_unary", fetch_agent_unary)
    channel = _register(cron_controller=cron)
    watcher = registry()
    channel.git_watcher_registry = watcher
    channel.clients = [owner]
    channel.clear_session_busy = lambda session_id: channel.events.append(
        {"session_cleared": session_id}
    )

    await channel.methods["project.remove"](
        object(), "rpc-3", {"project_id": "p1"}, "session-3", user_id="u1"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["req_method"].value == "project.remove"
    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": True,
            "payload": {
                "stopped_sessions": ["s1", ""],
                "stopped_cron_jobs": 2,
            },
        }
    ]
    assert watcher.cleaned == ["p1"]
    assert channel.events == [
        {"session_cleared": "s1"},
        {"ws": owner, "event": "project.removed", "project_id": "p1"},
    ]


@pytest.mark.asyncio
async def test_project_restore_broadcasts_after_success(monkeypatch):
    from agent_gateway.channel_manager.web import project_rpc_handlers

    prewarm = []
    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)
        on_done = kwargs.get("on_done")
        if on_done:
            await on_done(True, {"project_id": "p1"})

    monkeypatch.setattr(project_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register()
    class owner:
        user_id = "u1"
    channel.methods["project.restore"]._schedule_agent_prewarm_sync = lambda name: prewarm.append(name)
    channel.clients = [owner]

    await channel.methods["project.restore"](
        object(), "rpc-4", {"project_id": "p1"}, "session-4", user_id="u1"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["req_method"].value == "project.restore"
    assert channel.events == [{"ws": owner, "event": "project.restored", "project_id": "p1"}]


@pytest.mark.asyncio
async def test_project_git_commit_marks_watcher_dirty(monkeypatch):
    from agent_gateway.channel_manager.web import project_git_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)
        on_done = kwargs.get("on_done")
        if on_done:
            on_done(True, {})

    monkeypatch.setattr(project_git_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _FakeChannel()
    register_project_git_handlers(channel, object(), lambda ref: ref)
    dirty = []
    channel.git_watcher_registry = type("Registry", (), {})()
    channel.git_watcher_registry.mark_dirty = lambda project_id: dirty.append(project_id)

    await channel.methods["project.git.commit"](
        object(), "rpc-5", {"project_id": "p1"}, "session-5"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["req_method"].value == "project.git.commit"
    assert forwarded[0]["preserve_error_payload"] is True
    assert dirty == ["p1"]
