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
    """The handler fixture, with what a project lifecycle handler needs from a channel."""

    def __init__(self):
        super().__init__()
        self.agent_client = object()
        self.clients: list = []
        self.events: list = []
        self.cleared: list = []

    async def send_event(self, ws, event, payload):
        self.events.append((ws, event, payload))

    def clear_session_busy(self, session_id):
        self.cleared.append(session_id)

ORIGIN = {"Origin": "http://127.0.0.1:5173"}
HEARTBEAT_WRITES = {
    "heartbeat.job.get", "heartbeat.job.preview", "heartbeat.job.create", "heartbeat.job.update",
    "heartbeat.job.toggle", "heartbeat.job.run_now", "heartbeat.job.cancel", "heartbeat.job.delete",
}


def bind(personal, *, agent_client=None, cron_controller=None):
    channel = Channel()
    _register_web_handlers(WebHandlersBindParams(
        channel=app_web._PersonalSettingsRegistration(channel) if personal else channel,
        agent_client=agent_client, personal_settings=personal, cron_controller=cron_controller,
    ))
    return channel


def test_every_personal_web_operation_has_a_regular_handler():
    assert PERSONAL_WEB_OPERATIONS == HEARTBEAT_WRITES | {
        "project.create", "project.rename", "project.pin", "project.remove", "project.restore",
    } | {
        "project.git.probe", "project.git.init", "project.git.create_branch", "project.git.switch_branch",
        "project.git.commit", "project.git.push",
    }
    assert PERSONAL_WEB_OPERATIONS <= personal_web_methods()
    # Project-wide batch lifecycle is not here.
    assert not {"project.delete", "project.sessions.archive"} & PERSONAL_WEB_OPERATIONS
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


# ---- Project lifecycle (27698ad73) ----

CAPABILITY = "personal_project_lifecycle_v1"


def client_that_reports_capabilities():
    """Stands in for a client that qualifies its live socket by connection.ack."""
    return SimpleNamespace(advertises_capability=lambda capability: True)


def proxy_capture(monkeypatch, reply):
    """Record what the regular proxy sends; ``reply`` is the AgentServer's answer."""
    sent = []

    async def send(client, envelope, *, label, timeout_seconds, required_capability=None):
        sent.append((envelope.method, envelope.params, envelope.user_id, required_capability))
        return SimpleNamespace(**reply)

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    return sent


@pytest.mark.parametrize("method", ["project.remove", "project.restore"])
@pytest.mark.parametrize("params, forwarded", [
    ({"project_id": "p-1", "name": "dropped", "user_id": "bob"}, {"project_id": "p-1"}),
    ({"project_id": "  p-1  "}, {"project_id": "p-1"}),
])
async def test_project_mutation_is_reduced_to_its_project_id(method, params, forwarded):
    seen = []
    channel = Channel()

    async def handler(ws, req_id, params, session_id, user_id=None):
        seen.append((params, user_id))

    await protect_personal_web_operation(channel, method, handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "call", params, "synthetic", "alice",
    )
    assert seen == [(forwarded, "alice")] and channel.responses == []


@pytest.mark.parametrize("project_id", [None, "", "   ", 5, "p\n1", "p" * 257, {"id": "p"}])
async def test_project_mutation_needs_a_bounded_project_id(project_id):
    channel = Channel()

    async def handler(*args, **kwargs):
        raise AssertionError("must not run")

    await protect_personal_web_operation(channel, "project.remove", handler)(
        SimpleNamespace(request_headers=Headers(ORIGIN)), "remove", {"project_id": project_id}, "x", "alice",
    )
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, "BAD_REQUEST")


async def test_personal_project_remove_needs_no_cron_and_keeps_the_busy_cache(monkeypatch):
    sent = proxy_capture(monkeypatch, {"ok": True, "payload": {"hidden": True, "stopped_sessions": ["s-1"]}})

    async def hide_project_jobs(*args, **kwargs):
        raise AssertionError("the personal Worker has no Cron to coordinate with")

    channel = bind(personal=True, agent_client=client_that_reports_capabilities(),
                   cron_controller=SimpleNamespace(hide_project_jobs=hide_project_jobs))
    ws = SimpleNamespace(request_headers=Headers(ORIGIN), _web_connection_user_id="alice")
    sibling = SimpleNamespace(_web_connection_user_id="alice")
    other = SimpleNamespace(_web_connection_user_id="bob")
    channel.clients = [ws, sibling, other]
    await channel.methods["project.remove"](ws, "remove", {"project_id": "p-1", "name": "dropped"}, "synthetic", "alice")
    # One request, with the connection's identity, on a socket that advertises the capability.
    assert sent == [("project.remove", {"project_id": "p-1"}, "alice", CAPABILITY)]
    assert channel.responses == [{
        "id": "remove", "ok": True, "payload": {"hidden": True, "stopped_sessions": ["s-1"]}, "error": None, "code": None,
    }]
    # The owner's other tabs refresh; another user's socket is not told.
    assert channel.events == [(ws, "project.removed", {"project_id": "p-1"}), (sibling, "project.removed", {"project_id": "p-1"})]
    # The AgentServer stops those Sessions in the background. Their activity is
    # left to the events that report it, not forced idle from the answer's list.
    assert channel.cleared == []


@pytest.mark.parametrize("params, code", [({"project_id": "default"}, "FORBIDDEN"), ({"project_id": "default_code"}, "FORBIDDEN"),
                                          ({}, "BAD_REQUEST")])
async def test_personal_project_remove_refuses_the_default_project_before_the_agentserver(monkeypatch, params, code):
    sent = proxy_capture(monkeypatch, {"ok": True, "payload": {}})
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    await channel.methods["project.remove"](
        SimpleNamespace(request_headers=Headers(ORIGIN)), "remove", params, "synthetic", "alice",
    )
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, code) and sent == []


async def test_personal_project_mutations_stay_closed_before_an_agentserver_without_the_capability(monkeypatch):
    sent = proxy_capture(monkeypatch, {"ok": True, "payload": {}})
    # A socket that does not advertise the capability is covered by the
    # Worker integration test: the check is made under the client's send lock.
    for method in ("project.remove", "project.restore"):
        channel = bind(personal=True, agent_client=SimpleNamespace())
        await channel.methods[method](SimpleNamespace(request_headers=Headers(ORIGIN)), method, {"project_id": "p-1"}, "s", "alice")
        assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (
            False, "UNSUPPORTED_DATA_PLANE",
        ), method
        assert channel.events == []
    assert sent == []


async def test_personal_project_remove_passes_the_agentserver_refusal_through(monkeypatch):
    proxy_capture(monkeypatch, {"ok": False, "payload": {"error": "no such project", "code": "PROJECT_NOT_FOUND"}})
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    ws = SimpleNamespace(request_headers=Headers(ORIGIN), _web_connection_user_id="alice")
    channel.clients = [ws]
    await channel.methods["project.remove"](ws, "remove", {"project_id": "missing"}, "synthetic", "alice")
    assert channel.responses == [{
        "id": "remove", "ok": False, "payload": None, "error": "no such project", "code": "PROJECT_NOT_FOUND",
    }]
    assert channel.events == []


async def test_project_restore_requires_the_capability_in_the_personal_worker_only(monkeypatch):
    seen = []
    replies = {"ok": True, "payload": {"restored": True}}

    async def send(client, envelope, *, label, timeout_seconds, required_capability=None):
        seen.append(required_capability)
        return SimpleNamespace(**replies)

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    for personal in (True, False):
        channel = bind(personal, agent_client=client_that_reports_capabilities())
        ws = SimpleNamespace(request_headers=Headers(ORIGIN), _web_connection_user_id="alice")
        channel.clients = [ws]
        await channel.methods["project.restore"](ws, "restore", {"project_id": "p-1"}, "synthetic", "alice")
        assert channel.responses[-1]["ok"] is True
        assert [event for _, event, _ in channel.events] == ["project.restored"]
    assert seen == [CAPABILITY, None]


async def test_single_process_project_remove_keeps_cron_coordination_and_clears_its_busy_cache(monkeypatch):
    """The path the single process uses is the one main had."""
    fetched, hidden = [], []

    async def fetch_agent_unary(**kwargs):
        fetched.append(kwargs)
        return True, {"hidden": True, "stopped_sessions": ["s-1"]}

    class Cron:
        async def hide_project_jobs(self, project_id, *, commit):
            hidden.append(project_id)
            await commit()
            return {"stopped_cron_jobs": 2}

    monkeypatch.setattr(personal_web_operations, "fetch_agent_unary", fetch_agent_unary)
    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fetch_agent_unary)
    channel = bind(personal=False, cron_controller=Cron())
    ws = SimpleNamespace(request_headers=Headers(), _web_connection_user_id="alice")
    channel.clients = [ws]
    await channel.methods["project.remove"](ws, "remove", {"project_id": "p-1", "anything": "else"}, "synthetic", "alice")
    assert hidden == ["p-1"]
    assert [request["req_method"].value for request in fetched] == ["project.remove"]
    # No capability is asked of the single process's socket, and the request is not reduced.
    assert fetched[0].get("required_capability") is None
    assert fetched[0]["params"] == {"project_id": "p-1", "anything": "else"}
    assert channel.cleared == ["s-1"]
    assert channel.responses == [{
        "id": "remove", "ok": True, "payload": {"hidden": True, "stopped_sessions": ["s-1"], "stopped_cron_jobs": 2},
        "error": None, "code": None,
    }]
