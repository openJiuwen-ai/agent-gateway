# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The personal Web Git panel: the regular proxy handlers behind the local guard."""

from types import SimpleNamespace

import pytest
from starlette.datastructures import Headers

from agent_gateway.application.personal_web_operations import PERSONAL_WEB_OPERATIONS
from agent_gateway.routing.agent_client import AgentCapabilityUnavailable
from agent_gateway.routing.agent_request_timeout import AgentRequestTimeoutError
from tests.unit_tests.gateway.test_personal_web_operations import (
    ORIGIN, bind, client_that_reports_capabilities,
)

CAPABILITY = "personal_project_git_v1"
MARKER = "_personal_project_git_v1"
# The write, and how long the personal Worker waits for its answer.
GIT_WRITES = {
    "project.git.probe": 12.0,
    "project.git.init": 12.0,
    "project.git.create_branch": 12.0,
    "project.git.switch_branch": 12.0,
    "project.git.commit": 55.0,
    "project.git.push": 85.0,
}
GIT_READS = ("project.git.status", "project.git.diff_status", "project.git.turn_diff_list", "project.git.turn_diff")
RESTRICTED = {"project.git.commit": ("amend", "no_verify"), "project.git.push": ("force", "delete")}
PARAMS = {"project_id": "p-1", "branch": "main", "message": "Update", "user_id": "mallory"}
WITHOUT_USER = {key: value for key, value in PARAMS.items() if key != "user_id"}


def capture(monkeypatch, reply=None):
    """Record what the regular proxy sends; ``reply`` is the AgentServer's answer."""
    sent = []

    async def send(client, envelope, *, label, timeout_seconds, required_capability=None):
        sent.append(SimpleNamespace(
            method=envelope.method, params=envelope.params, user_id=envelope.user_id,
            required_capability=required_capability, timeout_seconds=timeout_seconds,
        ))
        return SimpleNamespace(**(reply or {"ok": True, "payload": {"answered": True}}))

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    return sent


def ws(headers=ORIGIN):
    return SimpleNamespace(request_headers=Headers(headers))


def test_git_writes_are_guarded_operations_and_git_reads_are_not():
    assert set(GIT_WRITES) <= PERSONAL_WEB_OPERATIONS
    assert set(GIT_READS).isdisjoint(PERSONAL_WEB_OPERATIONS)
    guarded, plain = bind(personal=True), bind(personal=False)
    for method in GIT_WRITES:
        assert guarded.methods[method].__name__ == "guarded", method
        assert getattr(plain.methods[method], "__name__", "") != "guarded", method
    for method in GIT_READS:
        assert getattr(guarded.methods[method], "__name__", "") != "guarded", method
        assert getattr(plain.methods[method], "__name__", "") != "guarded", method


@pytest.mark.parametrize("method", sorted(GIT_WRITES))
async def test_git_write_is_marked_and_qualified_in_the_personal_worker(monkeypatch, method):
    sent = capture(monkeypatch)
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    await channel.methods[method](ws(), "g", dict(PARAMS), None, "alice")
    # One request with the connection's identity. The page cannot name a user,
    # and the AgentServer that takes the write sees which panel sent it.
    assert [(request.method, request.user_id) for request in sent] == [(method, "alice")]
    assert sent[0].params == WITHOUT_USER | {MARKER: True}
    assert sent[0].required_capability == CAPABILITY
    assert sent[0].timeout_seconds == GIT_WRITES[method]
    assert channel.responses == [{
        "id": "g", "ok": True, "payload": {"answered": True}, "error": None, "code": None,
    }]


@pytest.mark.parametrize("method", sorted(GIT_WRITES))
async def test_single_process_git_writes_are_sent_as_main_sends_them(monkeypatch, method):
    """The path the single process uses is the one main had."""
    sent = capture(monkeypatch)
    channel = bind(personal=False, agent_client=client_that_reports_capabilities())
    await channel.methods[method](ws({}), "g", dict(PARAMS), None, "alice")
    assert [(request.method, request.user_id) for request in sent] == [(method, "alice")]
    # No marker, no capability and no timeout of its own.
    assert sent[0].params == WITHOUT_USER
    assert (sent[0].required_capability, sent[0].timeout_seconds) == (None, None)
    assert MARKER not in sent[0].params
    assert channel.responses[-1]["ok"] is True


async def test_anonymous_git_write_cannot_name_a_user(monkeypatch):
    sent = capture(monkeypatch)
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    await channel.methods["project.git.commit"](ws(), "g", dict(PARAMS), None, "")
    assert sent[0].user_id is None
    assert sent[0].params == WITHOUT_USER | {MARKER: True}


@pytest.mark.parametrize("method", sorted(RESTRICTED))
@pytest.mark.parametrize("value, code", [
    (True, "UNSUPPORTED_REQUEST"), ("true", "BAD_REQUEST"), (1, "BAD_REQUEST"), (None, "BAD_REQUEST"), ("", "BAD_REQUEST"),
])
async def test_restricted_git_flag_is_refused_before_the_write(monkeypatch, method, value, code):
    sent = capture(monkeypatch)
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    for flag in RESTRICTED[method]:
        await channel.methods[method](ws(), flag, {**PARAMS, flag: value}, None, "alice")
        response = channel.responses[-1]
        assert (response["id"], response["ok"], response["code"], response["error"]) == (
            flag, False, code, f"{flag} is not supported",
        ), (flag, response)
    assert sent == []


@pytest.mark.parametrize("method", sorted(RESTRICTED))
async def test_false_or_absent_restricted_flags_go_through(monkeypatch, method):
    for values in ({}, {flag: False for flag in RESTRICTED[method]}):
        sent = capture(monkeypatch)
        channel = bind(personal=True, agent_client=client_that_reports_capabilities())
        await channel.methods[method](ws(), "g", {**PARAMS, **values}, None, "alice")
        assert len(sent) == 1 and channel.responses[-1]["ok"] is True, (method, values)
        assert sent[0].params == WITHOUT_USER | values | {MARKER: True}


@pytest.mark.parametrize("method", sorted(RESTRICTED))
async def test_single_process_git_writes_keep_their_flags(monkeypatch, method):
    """What the single process accepts is not the personal panel's business."""
    flags = {flag: True for flag in RESTRICTED[method]}
    sent = capture(monkeypatch)
    channel = bind(personal=False, agent_client=client_that_reports_capabilities())
    await channel.methods[method](ws({}), "g", {**PARAMS, **flags}, None, "alice")
    assert len(sent) == 1 and channel.responses[-1]["ok"] is True
    assert sent[0].params == WITHOUT_USER | flags


@pytest.mark.parametrize("method", sorted(GIT_WRITES))
async def test_git_write_needs_a_browser_origin(monkeypatch, method):
    sent = capture(monkeypatch)
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    await channel.methods[method](ws({}), method, dict(PARAMS), None, "alice")
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, "FORBIDDEN"), method
    assert sent == []


@pytest.mark.parametrize("exception, code", [
    (AgentCapabilityUnavailable(), "UNSUPPORTED_DATA_PLANE"),
    (AgentRequestTimeoutError(), "AGENT_SERVER_TIMEOUT"),
])
async def test_git_write_fails_closed_and_is_never_replayed(monkeypatch, exception, code):
    """A write is never retried: the AgentServer may have done it and not answered."""
    calls = []

    async def send(*args, **kwargs):
        calls.append(kwargs)
        raise exception

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    dirty = []
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    channel.git_watcher_registry = SimpleNamespace(mark_dirty=dirty.append)
    await channel.methods["project.git.commit"](ws(), "g", dict(PARAMS), None, "alice")
    assert len(calls) == 1 and calls[0]["required_capability"] == CAPABILITY
    assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (False, code)
    assert dirty == []


async def test_git_writes_stay_closed_before_a_client_that_cannot_qualify_its_socket(monkeypatch):
    sent = capture(monkeypatch)
    channel = bind(personal=True, agent_client=SimpleNamespace())
    for method in GIT_WRITES:
        await channel.methods[method](ws(), method, dict(PARAMS), None, "alice")
        assert (channel.responses[-1]["ok"], channel.responses[-1]["code"]) == (
            False, "UNSUPPORTED_DATA_PLANE",
        ), method
    assert sent == []


@pytest.mark.parametrize("method, payload, wakes", [
    ("project.git.commit", {"error": "worktree busy", "code": "GIT_BUSY"}, True),
    ("project.git.push", {"error": "remote rejected", "code": "GIT_PUSH_FAILED"}, False),
])
async def test_agentserver_refusal_reaches_the_page_as_it_is(monkeypatch, method, payload, wakes):
    """A commit that failed may still have changed the index; a failed push did not."""
    sent = capture(monkeypatch, {"ok": False, "payload": dict(payload)})
    dirty = []
    channel = bind(personal=True, agent_client=client_that_reports_capabilities())
    channel.git_watcher_registry = SimpleNamespace(mark_dirty=dirty.append)
    await channel.methods[method](ws(), "g", dict(PARAMS), None, "alice")
    assert channel.responses == [{
        "id": "g", "ok": False, "payload": payload, "error": payload["error"], "code": payload["code"],
    }]
    assert dirty == (["p-1"] if wakes else [])
