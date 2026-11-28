# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Personal compatibility of real Worker processes behind the public listener.

These are black-box tests: the Worker group is started the way the public
``--mode multiprocess`` launcher starts it, and the AgentServer is a stub that
records what the Workers actually forwarded.
"""

import asyncio
from contextlib import asynccontextmanager, closing
import json
import socket
import time
from types import SimpleNamespace

import pytest
from e2a.agent_models import AgentResponse
from e2a.wire_codec import encode_agent_response_for_wire
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.processes.capabilities import management_preflight
from testcases_gateway.stub_agent_server import StubAgentServer

pytestmark = pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")

USER = "alice"


class RecordingAgentServer(StubAgentServer):
    """Stub with an observable connection.ack, per-method replies and headers."""

    def __init__(self, *, capabilities=()) -> None:
        super().__init__()
        self.capabilities = tuple(capabilities)
        self.handshakes: list[dict[str, str]] = []
        self.replies: dict[str, object] = {}

    def _handle(self, connection) -> None:
        self._connections.add(connection)
        self.handshakes.append(dict(getattr(getattr(connection, "request", None), "headers", None) or {}))
        connection.send(json.dumps({
            "type": "event", "event": "connection.ack",
            "payload": {"status": "ready", "readiness": "AGENT_READY",
                        "capabilities": list(self.capabilities)},
        }))
        try:
            for raw in connection:
                envelope = json.loads(raw)
                self.requests.append(envelope)
                request_id = str(envelope.get("request_id") or "")
                method = str(envelope.get("method") or "")
                if envelope.get("is_stream"):
                    self._send_stream(connection, envelope, request_id, method)
                    continue
                reply = self.replies.get(method)
                payload = reply(envelope) if callable(reply) else (
                    reply if reply is not None else {"accepted": True}
                )
                failed = isinstance(payload, dict) and isinstance(payload.get("error"), str)
                connection.send(json.dumps(encode_agent_response_for_wire(
                    AgentResponse(
                        request_id=request_id, channel_id=str(envelope.get("channel") or "web"),
                        payload=payload, ok=not failed,
                    ),
                    response_id=request_id,
                )))
        except Exception:  # noqa: BLE001 - closed by the Worker
            pass
        finally:
            self._connections.discard(connection)


async def worker_slot(group, user):
    """The Worker slot that owns ``user`` for both listeners."""
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    identity = resolve_local_identity(f"user_id={user}".encode(), (), entry="web", policy=LocalIdentityPolicy())
    return FixedUserHash(WorkerManifest(group.pool.slots), LocalIdentityPolicy()).select(identity)


async def answer(ws, request_id, *, timeout=10):
    """The one ``res`` frame for ``request_id``; a closed socket is reported as such."""
    async with asyncio.timeout(timeout):
        while True:
            try:
                frame = json.loads(await ws.recv())
            except ConnectionClosed as error:
                return {"id": request_id, "closed": error.rcvd.code if error.rcvd else None}
            if frame.get("id") == request_id and frame.get("type") == "res":
                return frame


def prepare_environment(tmp_path, monkeypatch):
    for name, directory in (("HOME", "home"), ("JIUWENSWARM_HOME", "home"),
                            ("JIUWENSWARM_DATA_DIR", "data"), ("AGENT_GATEWAY_DATA_DIR", "data"),
                            ("AGENTOS_GATEWAY_LOG_DIR", "logs")):
        path = tmp_path / directory
        path.mkdir(exist_ok=True)
        monkeypatch.setenv(name, str(path))
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")


@asynccontextmanager
async def running(stub, **options):
    group = WorkerGroup(stub.url, workers=1, web_port=0, protocol_port=0, handoff_timeout=2, **options)
    try:
        await group.start()
        yield group
    finally:
        try:
            await group.stop()
        finally:
            await asyncio.to_thread(stub.stop)
    assert all(process.returncode is not None for process in group.pool.processes)
    for address in group.addresses:
        with closing(socket.socket()) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(address)


@pytest.fixture
async def personal(tmp_path, monkeypatch):
    prepare_environment(tmp_path, monkeypatch)
    stub = RecordingAgentServer().start()
    async with running(stub) as group:
        yield group, stub


@asynccontextmanager
async def personal_with(tmp_path, monkeypatch, *capabilities):
    """The personal Worker in front of an AgentServer that advertises ``capabilities``."""
    prepare_environment(tmp_path, monkeypatch)
    stub = RecordingAgentServer(capabilities=capabilities).start()
    async with running(stub) as group:
        yield group, stub


def tui_url(group, *, user=USER):
    host, port = group.addresses[1]
    return f"ws://{host}:{port}/tui?user_id={user}"


def web_url(group, *, user=USER):
    host, port = group.addresses[0]
    return f"ws://{host}:{port}/ws?user_id={user}"


async def test_native_tui_queries_are_answered_without_a_session_binding(personal):
    group, stub = personal
    stub.replies["config.get"] = {"preferred_language": "zh", "model": "visible", "model_api_key": "do-not-project"}
    stub.replies["models.list"] = {
        "active_model": "m1",
        "models": [{"model_id": "m1", "is_current": True, "api_key": "do-not-project"}],
        "available_models": ["m1", "m2"],
        "vendors_token": "do-not-project",
    }
    async with connect(tui_url(group)) as tui:
        await tui.send(json.dumps({"type": "req", "id": "display", "method": "config.get", "params": {
            "session_id": "s-1", "cwd": "/tmp", "project_dir": "/tmp", "trusted_dirs": ["/tmp"], "user_id": "mallory",
        }}))
        display = await answer(tui, "display")
        assert display["ok"] is True, display
        # Only display fields are projected; connection context and secrets are not echoed.
        assert display["payload"] == {"preferred_language": "zh", "model": "visible"}
        await tui.send(json.dumps({"type": "req", "id": "models", "method": "models.list", "params": {}}))
        models = await answer(tui, "models")
        assert models["payload"] == {
            "active_model": "m1", "models": [{"model_id": "m1", "is_current": True}], "available_models": ["m1", "m2"],
        }
        await tui.send(json.dumps({"type": "req", "id": "secret", "method": "config.get",
                                   "params": {"key": "model_api_key"}}))
        assert (await answer(tui, "secret"))["code"] == "UNSUPPORTED_REQUEST"
        # The socket stays usable for a regular data-plane request afterwards.
        await tui.send(json.dumps({"type": "req", "id": "history", "method": "history.get",
                                   "params": {"session_id": "test-session"}}))
        async with asyncio.timeout(10):
            while True:
                frame = json.loads(await tui.recv())
                if frame.get("id") == "history":
                    assert frame["ok"] is True, frame
                    break
    forwarded = [request for request in stub.requests if request["method"] == "config.get"]
    assert len(forwarded) == 1
    assert forwarded[0]["channel"] == "tui"
    assert forwarded[0]["params"] == {}
    assert forwarded[0]["session_id"] == "s-1"
    assert forwarded[0]["user_id"] == USER
    assert [request["params"] for request in stub.requests if request["method"] == "models.list"] == [{}]


async def test_tui_compatibility_requests_require_a_native_loopback_peer(personal):
    group, stub = personal
    for headers in ({"Origin": "http://127.0.0.1:5173"}, {"origin": "null"}):
        async with connect(tui_url(group), additional_headers=headers) as tui:
            await tui.send(json.dumps({"type": "req", "id": "browser", "method": "models.list", "params": {}}))
            assert (await answer(tui, "browser"))["code"] == "FORBIDDEN"
    assert not any(request["method"] in {"models.list", "config.get"} for request in stub.requests)


async def test_tui_still_refuses_requests_outside_the_compatibility_set(personal):
    group, stub = personal
    for method in ("session.create", "config.set", "cron.job.list", "command.chrome"):
        async with connect(tui_url(group)) as tui:
            await tui.send(json.dumps({"type": "req", "id": method, "method": method, "params": {}}))
            closed = await answer(tui, method)
            assert closed == {"id": method, "closed": 1008}, closed
    assert not any(request["method"] in {"session.create", "config.set", "cron.job.list"} for request in stub.requests)


async def test_tui_session_writes_check_the_owner_and_activity_before_the_write(personal):
    group, stub = personal

    foreign = {"user_id": "bob", "runtime": {"is_processing": False}}

    def session_metadata(envelope):
        session_id = envelope["params"]["session_id"]
        if session_id == "no-such-id":
            return {"error": "AgentServer request timed out", "code": "AGENT_SERVER_TIMEOUT"}
        if session_id == "s-foreign":
            return foreign
        return {"user_id": envelope.get("user_id") or "",
                "runtime": {"is_processing": session_id == "s-busy"}}

    stub.replies["session.get_metadata"] = session_metadata
    stub.replies["session.rewind"] = {"turns_removed": 1}
    stub.replies["session.restore_files"] = {"turns_removed": 1}
    async with connect(tui_url(group)) as tui:
        for request_id, session_id, code in (("busy", "s-busy", "SESSION_BUSY"),
                                             ("foreign", "s-foreign", "SESSION_IDENTITY_REQUIRED"),
                                             ("missing", "no-such-id", "AGENT_SERVER_TIMEOUT")):
            await tui.send(json.dumps({"type": "req", "id": request_id, "method": "session.rewind", "params": {
                "session_id": session_id, "turn_index": 1, "user_id": USER,
            }}))
            result = await answer(tui, request_id)
            assert (result["ok"], result["code"]) == (False, code), result
        await tui.send(json.dumps({"type": "req", "id": "idle", "method": "session.restore_files", "params": {
            "session_id": "s-idle", "turn_index": 2, "user_id": "mallory",
        }}))
        assert await answer(tui, "idle") == {"type": "res", "id": "idle", "ok": True, "payload": {"turns_removed": 1}}
        await tui.send(json.dumps({"type": "req", "id": "exit", "method": "tui.disconnect", "params": {"session_id": "s-idle"}}))
        assert (await answer(tui, "exit"))["ok"] is True
    writes = [request for request in stub.requests if request["method"] in {"session.rewind", "session.restore_files"}]
    assert [(request["method"], request["params"], request["user_id"]) for request in writes] == [
        ("session.restore_files", {"session_id": "s-idle", "turn_index": 2}, USER),
    ]
    checks = [request for request in stub.requests if request["method"] == "session.get_metadata"]
    assert all(request["params"] == {"session_id": request["session_id"], "enable_writeback": False}
               for request in checks)
    assert [request["user_id"] for request in checks] == [USER] * len(checks)


async def test_default_model_switch_needs_the_agentserver_capability(tmp_path, monkeypatch):
    prepare_environment(tmp_path, monkeypatch)
    seen = {}
    for name, capabilities in (("legacy", ()), ("capable", ("personal_tui_model_switch_v1",))):
        stub = RecordingAgentServer(capabilities=capabilities).start()
        stub.replies["command.model"] = {"type": "switched", "applied": True, "current": "m2", "requested": "m2",
                                         "api_key": "do-not-project"}
        async with running(stub) as group:
            async with connect(tui_url(group)) as tui:
                await tui.send(json.dumps({"type": "req", "id": "switch", "method": "command.model", "params": {
                    "model": "m2", "index": 1, "session_id": "s1", "cwd": "/tmp",
                }}))
                seen[name] = await answer(tui, "switch")
        seen[f"{name}-sent"] = [(request["method"], request["params"])
                                for request in stub.requests if request["method"] == "command.model"]
    # An AgentServer without the capability is answered without any write: the unmarked
    # command.model of the display read path would be a different operation.
    assert seen["legacy"]["code"] == "UNSUPPORTED_DATA_PLANE" and seen["legacy"]["ok"] is False
    assert seen["legacy-sent"] == []
    assert seen["capable"] == {"type": "res", "id": "switch", "ok": True, "payload": {
        "type": "switched", "applied": True, "current": "m2", "requested": "m2",
    }}
    assert seen["capable-sent"] == [
        ("command.model", {"model": "m2", "index": 1, "_personal_tui_model_switch_v1": True}),
    ]


async def test_web_listener_does_not_take_tui_compatibility_requests(personal):
    group, stub = personal
    for method in ("command.diff", "command.rewind_compact", "session.rewind"):
        async with connect(web_url(group), additional_headers=ORIGIN) as web:
            await web.send(request(method, method, {}))
            assert await answer(web, method) == {"id": method, "closed": 1008}
    assert not any(request["method"] in {"command.diff", "session.rewind"} for request in stub.requests)


ORIGIN = {"Origin": "http://127.0.0.1:5173"}


def request(request_id, method, params):
    return json.dumps({"type": "req", "id": request_id, "method": method, "params": params})


def sent(stub, *methods):
    return [request for request in stub.requests if request["method"] in methods]


# What the Worker sends on its own behalf when a Web socket closes. It follows
# the socket's cleanup, so it can reach the AgentServer just after a test's
# sockets are gone; it is never part of what a client call forwarded.
SOCKET_CLOSE_NOTICES = frozenset({"webui.connection.closed"})


def requests_sent(stub, *methods):
    """The requests that client calls forwarded, in order."""
    return [
        request for request in stub.requests
        if request["method"] not in SOCKET_CLOSE_NOTICES and (not methods or request["method"] in methods)
    ]


async def event(ws, name, *, timeout=10):
    """The first ``event`` frame named ``name``; a closed socket is reported as such."""
    async with asyncio.timeout(timeout):
        while True:
            try:
                frame = json.loads(await ws.recv())
            except ConnectionClosed as error:
                return {"event": name, "closed": error.rcvd.code if error.rcvd else None}
            if frame.get("type") == "event" and frame.get("event") == name:
                return frame


async def frames_until(ws, request_id, *, timeout=10):
    """Every frame read up to the response to ``request_id``, that response included."""
    frames = []
    async with asyncio.timeout(timeout):
        while True:
            frames.append(json.loads(await ws.recv()))
            if frames[-1].get("id") == request_id and frames[-1].get("type") == "res":
                return frames


async def test_web_settings_are_forwarded_once_for_a_browser_origin(personal):
    group, stub = personal
    stub.replies["config.get"] = {"preferred_language": "zh", "model_api_key": "synthetic-settings-readback"}
    stub.replies["config.save_all"] = {"updated": ["free_search_ddg_enabled"], "applied_without_restart": True}
    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        # The Web page gets the AgentServer's answer as it is; the TUI's display
        # projection is a different, route-local contract.
        await web.send(request("read", "config.get", {}))
        assert await answer(web, "read") == {"type": "res", "id": "read", "ok": True, "payload": {
            "preferred_language": "zh", "model_api_key": "synthetic-settings-readback",
        }}
        await web.send(request("save", "config.save_all", {"config": {"free_search_ddg_enabled": "true"}}))
        assert await answer(web, "save") == {"type": "res", "id": "save", "ok": True, "payload": {
            "updated": ["free_search_ddg_enabled"], "applied_without_restart": True,
        }}
        # A request the Worker cannot save is refused as a unit, before a handler runs.
        for request_id, method, params in (
            ("unknown", "config.save_all", {"config": {"agents": {"codex": {"enabled": True}}}}),
            ("cli", "config.set", {"external_cli_agent_codex_enabled": True}),
            ("empty", "config.set", {}),
            ("account", "models.upsert", {"model": {"model_client_config": {"client_provider": "OpenAIAccount"}}}),
            ("argument", "config.get", {"key": "model_api_key"}),
            # The channel reads a non-object params value as {}, so the same
            # checks apply: a save still needs its config object.
            ("array", "config.save_all", ["config"]),
        ):
            await web.send(request(request_id, method, params))
            refused = await answer(web, request_id)
            assert (refused["ok"], refused["code"]) == (False, "BAD_REQUEST"), (request_id, refused)
        # Connection status is the Worker's own answer and still works on the same socket.
        await web.send(request("status", "connection.status", {}))
        assert (await answer(web, "status"))["ok"] is True
    assert [(request["method"], request["params"], request["channel"], request["user_id"])
            for request in requests_sent(stub)] == [
        ("config.get", {}, "web", USER),
        ("config.save_all", {"config": {"free_search_ddg_enabled": "true"}}, "web", USER),
    ]


@pytest.mark.parametrize("headers", [{}, {"Origin": "https://untrusted.example"}, {"Origin": "null"},
                                     {"Origin": "http://127.0.0.1:0"}, {"Origin": "http://user:pw@localhost:5173"}])
async def test_web_settings_refuse_a_foreign_or_missing_origin(personal, headers):
    group, stub = personal
    async with connect(web_url(group), additional_headers=headers) as web:
        await web.send(request("denied", "config.get", {}))
        assert await answer(web, "denied") == {
            "type": "res", "id": "denied", "ok": False, "payload": {}, "code": "FORBIDDEN",
            "error": "Settings require an allowed Web Origin",
        }
        # The refusal is answered; the socket itself stays open for the data plane.
        await web.send(request("status", "connection.status", {}))
        assert (await answer(web, "status"))["ok"] is True
    assert not sent(stub, "config.get")


async def test_web_listener_rejects_an_ambiguous_origin_at_the_handshake(personal):
    # Two Origin headers never reach a settings handler: the listener refuses the upgrade.
    group, stub = personal
    with pytest.raises(InvalidStatus) as refused:
        async with connect(web_url(group), additional_headers={
            "origin": "http://127.0.0.1:5173", "Origin": "http://localhost:5173",
        }):
            pass
    assert refused.value.response.status_code == 400
    # The WebSocket library answers 400 for two Origin headers, after the channel
    # has already accepted the socket and run its connect hooks. A settings
    # handler never runs, and nothing is forwarded for the refused socket beyond
    # its own close notice (see SOCKET_CLOSE_NOTICES).
    assert requests_sent(stub) == []


async def test_personal_web_reads_use_the_regular_handlers(personal):
    group, stub = personal
    stub.replies["project.list"] = {"projects": [{"project_id": "default"}]}
    stub.replies["mcp.list"] = {"servers": []}
    stub.replies["skills.list"] = {"skills": []}
    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        # Static Gateway metadata: answered locally, like the single process does.
        await web.send(request("commands", "commands.list", {}))
        commands = await answer(web, "commands")
        assert commands["ok"] is True and commands["payload"], commands
        # Registered proxy handlers and the generic forward path both end at the AgentServer.
        for request_id, method, params in (("projects", "project.list", {"work_mode": "work"}),
                                           ("mcp", "mcp.list", {}),
                                           ("skills", "skills.list", {})):
            await web.send(request(request_id, method, params))
            assert await answer(web, request_id) == {
                "type": "res", "id": request_id, "ok": True, "payload": stub.replies[method],
            }, method
        # Everything outside the personal Web set still closes the socket, unread methods included.
        await web.send(request("write", "project.delete", {"project_id": "default"}))
        assert await answer(web, "write") == {"id": "write", "closed": 1008}
    # project.list also starts the regular lifecycle event poll of the Web channel.
    assert sorted(request["method"] for request in requests_sent(stub)) == [
        "mcp.list", "project.lifecycle", "project.list", "skills.list",
    ], stub.requests
    assert sorted((request["method"], request["params"]) for request in sent(stub, "mcp.list", "project.list", "skills.list")) == [
        ("mcp.list", {}), ("project.list", {"work_mode": "work"}), ("skills.list", {}),
    ]
    assert all(request["channel"] == "web" and request["user_id"] == USER for request in requests_sent(stub))


@pytest.mark.parametrize("method", ["cron.job.list", "skills.install", "path.set", "models.replace_all",
                                    "permissions.tools.set", "openai_account.auth.status", "project.delete",
                                    "project.sessions.archive"])
async def test_personal_web_listener_keeps_ungated_writes_out(personal, method):
    group, stub = personal
    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        await web.send(request(method, method, {}))
        assert await answer(web, method) == {"id": method, "closed": 1008}
    assert not sent(stub, method)


async def test_web_language_and_tool_permission_settings(personal):
    group, stub = personal
    stub.replies["locale.set_conf"] = {"preferred_language": "en"}
    stub.replies["permissions.tools.update"] = {"write_file": "deny"}
    stub.replies["permissions.tools.delete"] = {}
    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        for request_id, method, params in (
            ("language", "locale.set_conf", {"preferred_language": "en"}),
            ("deny", "permissions.tools.update", {"tool": "write_file", "level": "deny"}),
            ("forget", "permissions.tools.delete", {"tool": "write_file"}),
        ):
            await web.send(request(request_id, method, params))
            assert await answer(web, request_id) == {
                "type": "res", "id": request_id, "ok": True, "payload": stub.replies[method],
            }, method
        for request_id, method, params in (
            ("language-code", "locale.set_conf", {"preferred_language": "fr"}),
            ("language-user", "locale.set_conf", {"preferred_language": "en", "user_id": "bob"}),
            ("level-shape", "permissions.tools.update", {"tool": "write_file", "level": {"*": "allow", "p": "deny"}}),
            ("level-value", "permissions.tools.update", {"tool": "write_file", "level": "always"}),
            ("two-tools", "permissions.tools.update", {"tool": "a", "name": "b", "level": "allow"}),
            ("no-level", "permissions.tools.update", {"tool": "write_file"}),
            ("delete-user", "permissions.tools.delete", {"tool": "write_file", "user_id": "bob"}),
        ):
            await web.send(request(request_id, method, params))
            refused = await answer(web, request_id)
            assert (refused["ok"], refused["code"]) == (False, "BAD_REQUEST"), (request_id, refused)
        # Replacing the whole tool table or another user's row stays closed.
        await web.send(request("replace", "permissions.tools.set", {"tools": {}}))
        assert await answer(web, "replace") == {"id": "replace", "closed": 1008}
    assert sorted((request["method"], request["params"], request["user_id"]) for request in requests_sent(stub)) == [
        ("locale.set_conf", {"preferred_language": "en"}, USER),
        ("permissions.tools.delete", {"tool": "write_file"}, USER),
        ("permissions.tools.update", {"tool": "write_file", "level": "deny"}, USER),
    ]


async def test_web_project_writes_need_a_browser_origin(personal):
    group, stub = personal
    for method, params in (("project.create", {"name": "p", "project_dir": "", "work_mode": "work"}),
                           ("project.rename", {"project_id": "p", "name": "n"}),
                           ("project.pin", {"project_id": "p", "pinned": True}),
                           ("project.remove", {"project_id": "p"}),
                           ("project.restore", {"project_id": "p"}),
                           ("project.git.init", {"project_id": "p"}),
                           ("project.git.commit", {"project_id": "p", "message": "m"}),
                           ("heartbeat.job.delete", {"session_id": "s-mine", "id": "job"})):
        async with connect(web_url(group)) as web:
            await web.send(request(method, method, params))
            assert await answer(web, method) == {
                "type": "res", "id": method, "ok": False, "payload": {}, "code": "FORBIDDEN",
                "error": "This operation requires an allowed Web Origin",
            }, method
    # Refused before any handler: neither the write nor its Session ownership check is sent.
    assert not sent(stub, "project.create", "project.rename", "project.pin", "project.remove",
                    "project.restore", "project.git", "session.get_metadata", "heartbeat.job")


async def test_web_project_writes_are_forwarded_once_with_the_connection_identity(personal):
    group, stub = personal
    stub.replies["project.create"] = {"project_id": "p-new", "project_dir": "/tmp/p-new"}
    stub.replies["project.rename"] = {"project_id": "p-new", "name": "Renamed Project"}
    stub.replies["project.pin"] = {"project_id": "p-new", "pinned": True}
    async with (connect(web_url(group), additional_headers=ORIGIN) as web,
                connect(web_url(group), additional_headers=ORIGIN) as sibling,
                connect(web_url(group, user="bob"), additional_headers=ORIGIN) as other):
        for request_id, method, params in (
            ("create", "project.create", {"name": "Blank Project", "project_dir": "", "work_mode": "work"}),
            # A caller-chosen user_id never reaches the AgentServer.
            ("rename", "project.rename", {"project_id": "p-new", "name": "Renamed Project", "user_id": "bob"}),
            ("pin", "project.pin", {"project_id": "p-new", "pinned": True}),
        ):
            await web.send(request(request_id, method, params))
            assert await answer(web, request_id) == {
                "type": "res", "id": request_id, "ok": True, "payload": stub.replies[method],
            }, method
        # The AgentServer's structured refusal reaches the page as it is.
        stub.replies["project.rename"] = {"error": "name already used", "code": "CONFLICT"}
        await web.send(request("duplicate", "project.rename", {"project_id": "p-new", "name": "default"}))
        duplicate = await answer(web, "duplicate")
        assert (duplicate["ok"], duplicate["code"], duplicate["error"]) == (
            False, "CONFLICT", "name already used",
        ), duplicate
        # An implicit restore notifies the owner's other tabs, and nobody else's.
        stub.replies["project.create"] = {"project_id": "p-old", "restored": True}
        await web.send(request("restore", "project.create", {"name": "Existing", "project_dir": "", "work_mode": "work"}))
        restored = await answer(web, "restore")
        assert restored["ok"] is True and restored["payload"]["restored"] is True, restored
        assert await event(sibling, "project.restored") == {
            "type": "event", "event": "project.restored", "payload": {"project_id": "p-old"},
        }
        # The broadcast finished before the write was answered, so another user's
        # socket has nothing queued before its next response.
        await other.send(request("status", "connection.status", {}))
        frames = await frames_until(other, "status")
        assert frames[-1]["ok"] is True and not [f for f in frames if f.get("event") == "project.restored"]
    # Each write went out once, with the connection's identity, and nothing else
    # was forwarded for the three sockets.
    assert [(request["method"], request["params"], request["channel"], request["user_id"])
            for request in requests_sent(stub)] == [
        ("project.create", {"name": "Blank Project", "project_dir": "", "work_mode": "work"}, "web", USER),
        ("project.rename", {"project_id": "p-new", "name": "Renamed Project"}, "web", USER),
        ("project.pin", {"project_id": "p-new", "pinned": True}, "web", USER),
        ("project.rename", {"project_id": "p-new", "name": "default"}, "web", USER),
        ("project.create", {"name": "Existing", "project_dir": "", "work_mode": "work"}, "web", USER),
    ]


async def test_web_project_lifecycle_is_refused_before_the_agentserver(personal):
    group, stub = personal
    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        for request_id, method, params, code in (
            ("default", "project.remove", {"project_id": "default"}, "FORBIDDEN"),
            ("default-code", "project.remove", {"project_id": "default_code"}, "FORBIDDEN"),
            ("missing", "project.remove", {}, "BAD_REQUEST"),
            ("blank", "project.restore", {"project_id": "   "}, "BAD_REQUEST"),
            ("control", "project.restore", {"project_id": "p\n1"}, "BAD_REQUEST"),
        ):
            await web.send(request(request_id, method, params))
            refused = await answer(web, request_id)
            assert (refused["ok"], refused["code"], refused["payload"]) == (False, code, {}), (request_id, refused)
    # Project-wide batch lifecycle is not a personal Web operation.
    for method in ("project.delete", "project.sessions.archive"):
        async with connect(web_url(group), additional_headers=ORIGIN) as web:
            await web.send(request(method, method, {"project_id": "p-1"}))
            assert await answer(web, method) == {"id": method, "closed": 1008}, method
    assert requests_sent(stub) == []


async def test_web_project_removal_and_restore_need_the_agentserver_capability(tmp_path, monkeypatch):
    seen = {}
    for name, capabilities in (("legacy", ()), ("capable", ("personal_project_lifecycle_v1",))):
        async with personal_with(tmp_path, monkeypatch, *capabilities) as (group, stub):
            stub.replies["project.remove"] = {"hidden": True, "stopped_sessions": ["s-1"]}
            stub.replies["project.restore"] = {"restored": True}
            async with connect(web_url(group), additional_headers=ORIGIN) as web:
                for request_id, method, params in (
                    ("remove", "project.remove", {"project_id": " p-1 ", "name": "dropped", "user_id": "bob"}),
                    ("restore", "project.restore", {"project_id": "p-1", "user_id": "bob"}),
                ):
                    await web.send(request(request_id, method, params))
                    seen[(name, method)] = await answer(web, request_id)
            seen[(name, "sent")] = [(request["method"], request["params"], request["user_id"])
                                    for request in requests_sent(stub)]
    # An AgentServer that does not say it stops the project's Sessions and jobs
    # is answered without a write: there is no Gateway Cron to stop them here.
    for method in ("project.remove", "project.restore"):
        legacy = seen[("legacy", method)]
        assert (legacy["ok"], legacy["code"], legacy["payload"]) == (False, "UNSUPPORTED_DATA_PLANE", {}), legacy
    assert seen[("legacy", "sent")] == []
    assert seen[("capable", "project.remove")] == {
        "type": "res", "id": "remove", "ok": True, "payload": {"hidden": True, "stopped_sessions": ["s-1"]},
    }
    assert seen[("capable", "project.restore")] == {"type": "res", "id": "restore", "ok": True, "payload": {"restored": True}}
    # One request each, reduced to the project ID and carrying the connection's identity.
    assert seen[("capable", "sent")] == [
        ("project.remove", {"project_id": "p-1"}, USER),
        ("project.restore", {"project_id": "p-1"}, USER),
    ]


async def test_web_project_removal_and_restore_events_reach_the_owner_only(tmp_path, monkeypatch):
    async with personal_with(tmp_path, monkeypatch, "personal_project_lifecycle_v1") as (group, stub):
        stub.replies["project.remove"] = {"hidden": True, "stopped_sessions": ["s-1"]}
        stub.replies["project.restore"] = {"restored": True}
        async with (connect(web_url(group), additional_headers=ORIGIN) as web,
                    connect(web_url(group), additional_headers=ORIGIN) as sibling,
                    connect(web_url(group, user="bob"), additional_headers=ORIGIN) as other):
            await web.send(request("remove", "project.remove", {"project_id": "p-1"}))
            assert (await answer(web, "remove"))["ok"] is True
            assert await event(sibling, "project.removed") == {
                "type": "event", "event": "project.removed", "payload": {"project_id": "p-1"},
            }
            await web.send(request("restore", "project.restore", {"project_id": "p-1"}))
            assert (await answer(web, "restore"))["ok"] is True
            assert await event(sibling, "project.restored") == {
                "type": "event", "event": "project.restored", "payload": {"project_id": "p-1"},
            }
            # The AgentServer's structured refusal reaches the page as it is, and notifies nobody.
            stub.replies["project.restore"] = {"error": "no such project", "code": "PROJECT_NOT_FOUND"}
            await web.send(request("missing", "project.restore", {"project_id": "missing"}))
            refused = await answer(web, "missing")
            assert (refused["ok"], refused["code"], refused["error"]) == (False, "PROJECT_NOT_FOUND", "no such project")
            # The broadcasts finished before each write was answered, so the other
            # sockets have nothing queued before their next response.
            for name, ws in (("sibling", sibling), ("other", other)):
                await ws.send(request("status", "connection.status", {}))
                frames = await frames_until(ws, "status")
                assert frames[-1]["ok"] is True, (name, frames)
                assert not [f for f in frames if f.get("event") in {"project.removed", "project.restored"}], (name, frames)
        assert [(request["method"], request["params"], request["user_id"]) for request in requests_sent(stub)] == [
            ("project.remove", {"project_id": "p-1"}, USER),
            ("project.restore", {"project_id": "p-1"}, USER),
            ("project.restore", {"project_id": "missing"}, USER),
        ]


GIT_CAPABILITY = "personal_project_git_v1"


async def test_web_git_writes_need_the_agentserver_capability(tmp_path, monkeypatch):
    seen = {}
    for name, capabilities in (("legacy", ()), ("capable", (GIT_CAPABILITY,))):
        async with personal_with(tmp_path, monkeypatch, *capabilities) as (group, stub):
            for method in ("project.git.status", "project.git.probe", "project.git.init", "project.git.commit",
                           "project.git.push"):
                stub.replies[method] = {"answered": method}
            async with connect(web_url(group), additional_headers=ORIGIN) as web:
                # Status is a read of the regular handler set, whatever the AgentServer says.
                await web.send(request("status", "project.git.status", {"project_id": "p-1"}))
                seen[(name, "project.git.status")] = await answer(web, "status")
                for method in ("project.git.probe", "project.git.init", "project.git.commit", "project.git.push"):
                    await web.send(request(method, method, {"project_id": "p-1", "user_id": "bob"}))
                    seen[(name, method)] = await answer(web, method)
            seen[(name, "sent")] = [(request["method"], request["params"], request["user_id"])
                                    for request in requests_sent(stub)]
    writes = ("project.git.probe", "project.git.init", "project.git.commit", "project.git.push")
    for method in writes:
        # An AgentServer that does not take the personal panel's Git writes is answered without one.
        legacy = seen[("legacy", method)]
        assert (legacy["ok"], legacy["code"], legacy["payload"]) == (False, "UNSUPPORTED_DATA_PLANE", {}), legacy
    assert seen[("legacy", "sent")] == [("project.git.status", {"project_id": "p-1"}, USER)]
    for method in writes:
        assert seen[("capable", method)] == {"type": "res", "id": method, "ok": True, "payload": {"answered": method}}
    # Each write went out once: marked for the AgentServer, reduced to the connection's identity.
    assert seen[("capable", "sent")] == [
        ("project.git.status", {"project_id": "p-1"}, USER),
    ] + [(method, {"project_id": "p-1", "_personal_project_git_v1": True}, USER) for method in writes]


async def test_web_git_panel_refuses_what_it_never_asks_for(tmp_path, monkeypatch):
    async with personal_with(tmp_path, monkeypatch, GIT_CAPABILITY) as (group, stub):
        stub.replies["project.git.commit"] = {"committed": True}
        stub.replies["project.git.push"] = {"pushed": True}
        async with connect(web_url(group), additional_headers=ORIGIN) as web:
            for request_id, method, params, code in (
                ("amend", "project.git.commit", {"project_id": "p-1", "amend": True}, "UNSUPPORTED_REQUEST"),
                ("no-verify", "project.git.commit", {"project_id": "p-1", "no_verify": True}, "UNSUPPORTED_REQUEST"),
                ("force", "project.git.push", {"project_id": "p-1", "force": True}, "UNSUPPORTED_REQUEST"),
                ("delete", "project.git.push", {"project_id": "p-1", "delete": True}, "UNSUPPORTED_REQUEST"),
                ("force-text", "project.git.push", {"project_id": "p-1", "force": "yes"}, "BAD_REQUEST"),
            ):
                await web.send(request(request_id, method, params))
                refused = await answer(web, request_id)
                assert (refused["ok"], refused["code"], refused["payload"]) == (False, code, {}), (request_id, refused)
            # The flags the panel does send, set to false, go through.
            await web.send(request("commit", "project.git.commit", {
                "project_id": "p-1", "message": "Update", "amend": False, "no_verify": False, "user_id": "bob",
            }))
            assert await answer(web, "commit") == {"type": "res", "id": "commit", "ok": True, "payload": {"committed": True}}
            await web.send(request("push", "project.git.push", {"project_id": "p-1", "force": False, "delete": False}))
            assert await answer(web, "push") == {"type": "res", "id": "push", "ok": True, "payload": {"pushed": True}}
        assert [(request["method"], request["params"], request["user_id"]) for request in requests_sent(stub)] == [
            ("project.git.commit", {
                "project_id": "p-1", "message": "Update", "amend": False, "no_verify": False, "_personal_project_git_v1": True,
            }, USER),
            ("project.git.push", {
                "project_id": "p-1", "force": False, "delete": False, "_personal_project_git_v1": True,
            }, USER),
        ]


def heartbeat_answers(stub):
    """AgentServer answers for heartbeat jobs: Session ownership and the job relay."""
    relayed = []

    def metadata(envelope):
        session_id = envelope["params"]["session_id"]
        if session_id == "s-gone":
            return {"error": "session not found", "code": "NOT_FOUND"}
        return {"user_id": {"s-mine": USER, "s-anon": None}.get(session_id, "bob")}

    def job(envelope):
        relayed.append(envelope)
        action = envelope["params"]["action"]
        return {"result": {"delete": {"deleted": True}, "list": {"jobs": []}}.get(action, {"job": {"id": "job"}})}

    stub.replies["session.get_metadata"] = metadata
    stub.replies["heartbeat.job"] = job
    return relayed


async def test_web_heartbeat_writes_are_scoped_to_the_connection_user(personal):
    group, stub = personal
    relayed = heartbeat_answers(stub)
    job = {"session_id": "s-mine", "id": "job"}

    async def refused(ws, request_id, params, code):
        await ws.send(request(request_id, "heartbeat.job.delete", params))
        frame = await answer(ws, request_id)
        assert (frame["ok"], frame["code"], frame.get("payload")) == (False, code, {}), (request_id, frame)

    async with connect(web_url(group), additional_headers=ORIGIN) as web:
        # The caller's user_id is dropped: only the connection identity scopes the job.
        await web.send(request("mine", "heartbeat.job.delete", {**job, "user_id": "bob"}))
        assert await answer(web, "mine") == {"type": "res", "id": "mine", "ok": True, "payload": {"deleted": True}}
        await refused(web, "foreign", {"session_id": "s-foreign", "id": "job"}, "SESSION_IDENTITY_REQUIRED")
        # The channel reads a non-object params value as {}, so it needs no case of its own.
        await refused(web, "no-session", {"id": "job"}, "BAD_REQUEST")
        await refused(web, "array", ["s-mine", "job"], "BAD_REQUEST")
        # The ownership answer is passed through before any job operation is relayed.
        await refused(web, "gone", {"session_id": "s-gone", "id": "job"}, "NOT_FOUND")
        # An owned Session with a malformed job is the regular handler's refusal.
        await refused(web, "no-id", {"session_id": "s-mine"}, "BAD_REQUEST")
        # Listing and metadata have no Session scope of their own.
        await web.send(request("jobs", "heartbeat.job.list", {"session_id": "s-mine"}))
        listed = await answer(web, "jobs")
        assert (listed["ok"], listed["payload"]) == (True, {"jobs": []}), listed
        await web.send(request("meta", "heartbeat.job.meta", {}))
        assert (await answer(web, "meta"))["ok"] is True
    async with connect(web_url(group, user="bob"), additional_headers=ORIGIN) as other:
        await refused(other, "steal", {**job, "user_id": USER}, "SESSION_IDENTITY_REQUIRED")
    # An anonymous connection owns anonymous Sessions, and only those.
    async with connect(web_url(group, user=""), additional_headers=ORIGIN) as anonymous:
        await refused(anonymous, "named", job, "SESSION_IDENTITY_REQUIRED")
        await anonymous.send(request("own", "heartbeat.job.delete", {"session_id": "s-anon", "id": "job"}))
        assert (await answer(anonymous, "own"))["ok"] is True
    assert [(envelope["params"]["action"], envelope["params"]["data"], envelope.get("user_id")) for envelope in relayed] == [
        ("delete", {"job_id": "job"}, USER),
        ("list", {"session_id": "s-mine"}, USER),
        ("meta", {}, USER),
        ("delete", {"job_id": "job"}, None),
    ]
    # The unguarded list takes the Session from its params, as the regular handler does.
    assert [envelope["session_id"] for envelope in relayed] == ["s-mine", "s-mine", "", "s-anon"]
    assert all(envelope["channel"] == "web" for envelope in relayed)
    # One ownership answer per well-formed write; none for a request refused before the check.
    assert len(sent(stub, "session.get_metadata")) == 7


async def test_protocol_listener_keeps_web_settings_out(personal):
    group, stub = personal
    stub.replies["config.get"] = {"preferred_language": "zh", "model_api_key": "synthetic-settings-readback"}
    host, port = group.addresses[1]
    for route, method in (("/tui", "config.set"), ("/tui", "config.save_all"), ("/tui", "models.upsert"),
                          ("/tui", "locale.set_conf"), ("/tui", "permissions.tools.update"),
                          ("/tui", "project.create"), ("/tui", "heartbeat.job.delete"),
                          ("/acp", "config.get"), ("/acp", "models.list")):
        async with connect(f"ws://{host}:{port}{route}?user_id={USER}", additional_headers=ORIGIN) as protocol:
            await protocol.send(request(method, method, {}))
            assert await answer(protocol, method) == {"id": method, "closed": 1008}, (route, method)
    # config.get on the TUI route is the display projection, never the Web readback.
    async with connect(tui_url(group)) as tui:
        await tui.send(request("display", "config.get", {}))
        assert await answer(tui, "display") == {
            "type": "res", "id": "display", "ok": True, "payload": {"preferred_language": "zh"},
        }
    assert [(request["channel"], request["params"]) for request in sent(stub, "config.get")] == [("tui", {})]


async def test_account_worker_keeps_the_tui_compatibility_requests_out(tmp_path, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    monkeypatch.setenv("JIUWENSWARM_CONFIG_URL", "off")
    monkeypatch.setattr("agent_gateway.auth.remote_config.warm_up_in_background", lambda: None)
    from agent_gateway.auth import apig, login_credentials, model_catalog
    from agent_gateway.auth.account_kit import Credential
    from agent_gateway.auth.service import AuthService
    from agent_gateway.auth.session_store import AuthSessionStore

    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"c" * 32)
    monkeypatch.setattr(model_catalog, "get_models", lambda *a, **k: [model_catalog.LoginModel("remote-model", "Remote")])
    monkeypatch.setattr(apig, "resolve_apig_config", lambda **k: SimpleNamespace(invoke_base_url="https://test.invalid"))
    store = AuthSessionStore(persist=False)
    store.create(SimpleNamespace(user_id="account-a", user_name="Account A", credential=Credential(
        id_token="synthetic-token", refresh_token="synthetic-refresh", expires_at=time.time() + 3600,
    )))
    for name in ("HOME", "JIUWENSWARM_HOME"):
        monkeypatch.setenv(name, str(tmp_path))
    stub = RecordingAgentServer().start()
    group = WorkerGroup(stub.url, workers=1, web_port=0, protocol_port=0, auth_service_provider=lambda: AuthService(store=store))
    try:
        await group.start()
        async with connect(tui_url(group)) as tui:
            await tui.send(json.dumps({"type": "req", "id": "display", "method": "config.get", "params": {}}))
            assert (await answer(tui, "display")) == {"id": "display", "closed": 1008}
        # The account Worker's Web listener keeps the data-plane admission it had.
        for method in ("config.get", "config.save_all", "models.list", "commands.list", "project.list",
                       "project.create", "project.rename", "heartbeat.job.list", "heartbeat.job.delete"):
            async with connect(web_url(group), additional_headers=ORIGIN) as web:
                await web.send(request(method, method, {}))
                assert await answer(web, method) == {"id": method, "closed": 1008}, method
    finally:
        try:
            await group.stop()
        finally:
            await asyncio.to_thread(stub.stop)
    assert not any(request["method"] in {"config.get", "project.create", "project.rename", "heartbeat.job",
                                         "session.get_metadata"} for request in stub.requests)
