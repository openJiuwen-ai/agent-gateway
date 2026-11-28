# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Personal Web operations reuse the regular handlers behind a local caller check."""

from types import SimpleNamespace

import pytest
from starlette.datastructures import Headers

from agent_gateway.application import web as app_web
from agent_gateway.application import personal_web_operations
from agent_gateway.application.personal_settings import PERSONAL_SETTINGS_METHODS
from agent_gateway.application.personal_web_operations import (
    PERSONAL_WEB_OPERATIONS, protect_personal_web_operation,
)
from agent_gateway.application.worker_data import personal_web_methods
from agent_gateway.channel_manager.web.app_web_handlers import WebHandlersBindParams, _register_web_handlers
from tests.unit_tests.gateway.test_personal_settings import Channel as _Channel


class Channel(_Channel):
    """The handler fixture, with the southbound client the real WebChannel exposes."""

    def __init__(self):
        super().__init__()
        self.agent_client = object()

ORIGIN = {"Origin": "http://127.0.0.1:5173"}
HEARTBEAT_WRITES = {
    "heartbeat.job.get", "heartbeat.job.preview", "heartbeat.job.create", "heartbeat.job.update",
    "heartbeat.job.toggle", "heartbeat.job.run_now", "heartbeat.job.cancel", "heartbeat.job.delete",
}


def bind(personal):
    channel = Channel()
    _register_web_handlers(WebHandlersBindParams(
        channel=app_web._PersonalSettingsRegistration(channel) if personal else channel, agent_client=None,
    ))
    return channel


def test_every_personal_web_operation_has_a_regular_handler():
    assert PERSONAL_WEB_OPERATIONS == HEARTBEAT_WRITES | {"project.create", "project.rename", "project.pin"}
    assert PERSONAL_WEB_OPERATIONS <= personal_web_methods()
    # Removal, restoration and batch lifecycle need Cron coordination and are not here.
    assert not {"project.remove", "project.restore", "project.delete", "project.sessions.archive"} & PERSONAL_WEB_OPERATIONS
    guarded = bind(personal=True)
    assert PERSONAL_WEB_OPERATIONS <= guarded.local_only
    plain = bind(personal=False)
    assert plain.local_only.isdisjoint(PERSONAL_WEB_OPERATIONS)
    # Only the personal settings and operations become local-only; the regular set is unchanged.
    assert guarded.local_only == plain.local_only | PERSONAL_SETTINGS_METHODS | PERSONAL_WEB_OPERATIONS
    for method in PERSONAL_WEB_OPERATIONS:
        assert getattr(plain.methods[method], "__name__", "") != "guarded", method
        assert guarded.methods[method].__name__ == "guarded", method


@pytest.mark.parametrize("headers, code", [
    (Headers(), "FORBIDDEN"),
    (Headers({"Origin": "https://untrusted.example"}), "FORBIDDEN"),
    (Headers({"Origin": "http://localhost:5173"}), None),
])
async def test_operation_requires_an_allowed_origin(monkeypatch, headers, code):
    fetch, calls = [], []

    async def fetch_agent_unary(**kwargs):
        fetch.append(kwargs)
        return True, {"user_id": "alice", "runtime": {"is_processing": False}}

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    seen = []
    channel = Channel()

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, session_id, user_id))

    await protect_personal_web_operation(channel, "project.create", handler)(
        SimpleNamespace(request_headers=headers), "create", {"name": "p"}, "synthetic", "alice",
    )
    if code:
        assert seen == [] and fetch == []
        assert channel.responses[-1]["code"] == code
    else:
        # A project write has no Session scope: the handler sees the request as it is.
        assert seen == [({"name": "p"}, "synthetic", "alice")] and fetch == []
        assert channel.responses == []


@pytest.mark.parametrize("params", ["text", None, [], 1])
async def test_operation_requires_an_object(params):
    channel = Channel()

    async def handler(*args, **kwargs):
        raise AssertionError("must not run")

    await protect_personal_web_operation(channel, "project.pin", handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "pin", params, "session", "alice",
    )
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, "BAD_REQUEST")


@pytest.mark.parametrize("session_id", [None, "", " padded ", "../escape", 5, "s" * 300])
async def test_heartbeat_write_needs_a_well_formed_session(monkeypatch, session_id):
    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", None)
    channel = Channel()

    async def handler(*args, **kwargs):
        raise AssertionError("must not run")

    await protect_personal_web_operation(channel, "heartbeat.job.delete", handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "delete", {"id": "job", "session_id": session_id},
        "synthetic-session", "alice",
    )
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, "BAD_REQUEST")


@pytest.mark.parametrize("snapshot, code", [
    ({"user_id": "alice", "runtime": {"is_processing": False}}, None),
    ({"user_id": "bob"}, "SESSION_IDENTITY_REQUIRED"),
    ({"user_id": None}, "SESSION_IDENTITY_REQUIRED"),
    ({}, "SERVICE_UNAVAILABLE"),
    ("not a dict", "SERVICE_UNAVAILABLE"),
])
async def test_heartbeat_write_is_scoped_to_the_connection_user(monkeypatch, snapshot, code):
    fetch = []

    async def fetch_agent_unary(**kwargs):
        fetch.append(kwargs)
        return True, snapshot

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    channel = Channel()
    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, session_id, user_id))

    # A caller-supplied user_id is dropped: the connection identity is the only scope.
    params = {"id": "job", "session_id": "real-session", "user_id": "bob"}
    await protect_personal_web_operation(channel, "heartbeat.job.delete", handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "delete", params, "synthetic-session", "alice",
    )
    assert len(fetch) == 1
    assert fetch[0]["params"] == {"session_id": "real-session", "enable_writeback": False}
    assert fetch[0]["session_id"] == "real-session" and fetch[0]["user_id"] == "alice"
    assert fetch[0]["req_method"].value == "session.get_metadata"
    if code:
        assert seen == [] and channel.responses[-1]["code"] == code
    else:
        assert seen == [({"id": "job", "session_id": "real-session"}, "real-session", "alice")]
        assert channel.responses == []


async def test_anonymous_session_belongs_to_an_anonymous_connection(monkeypatch):
    async def fetch_agent_unary(**kwargs):
        return True, {"user_id": None}

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    seen = []

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append(user_id)

    guard = protect_personal_web_operation(Channel(), "heartbeat.job.run_now", handler)
    await guard(SimpleNamespace(request_headers=Headers(ORIGIN)), "run", {"session_id": "s"}, "synthetic", "")
    await guard(SimpleNamespace(request_headers=Headers(ORIGIN)), "run", {"session_id": "s"}, "synthetic", "alice")
    assert seen == [""]


@pytest.mark.parametrize("failure, forwarded", [
    ({"error": "no such session", "code": "NOT_FOUND"}, ("no such session", "NOT_FOUND")),
    ({"error": "This AgentServer does not support the requested operation", "code": "UNSUPPORTED_DATA_PLANE"},
     ("This AgentServer does not support the requested operation", "UNSUPPORTED_DATA_PLANE")),
])
async def test_heartbeat_write_passes_the_metadata_failure_through(monkeypatch, failure, forwarded):
    async def fetch_agent_unary(**kwargs):
        return False, failure

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    channel = Channel()

    async def handler(*args, **kwargs):
        raise AssertionError("must not run")

    await protect_personal_web_operation(channel, "heartbeat.job.update", handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "update", {"session_id": "s", "id": "j"}, "x", "alice",
    )
    response = channel.responses[-1]
    assert (response["ok"], response["error"], response["code"]) == (False, *forwarded)


async def test_registered_heartbeat_and_project_handlers_are_wrapped(monkeypatch):
    channel = bind(personal=True)
    monkeyed = []

    async def fetch_agent_unary(**kwargs):
        monkeyed.append(kwargs)
        return False, {"error": "gone", "code": "NOT_FOUND"}

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    await channel.methods["heartbeat.job.get"](
        SimpleNamespace(request_headers=Headers(ORIGIN)), "get", {"session_id": "s", "id": "j"}, "x", "alice",
    )
    assert channel.responses[-1]["code"] == "NOT_FOUND" and len(monkeyed) == 1
    assert monkeyed[0]["params"] == {"session_id": "s", "enable_writeback": False}
