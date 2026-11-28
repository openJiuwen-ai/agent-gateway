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
from websockets.exceptions import ConnectionClosed

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
    async with connect(web_url(group)) as web:
        await web.send(json.dumps({"type": "req", "id": "display", "method": "config.get", "params": {}}))
        assert (await answer(web, "display")) == {"id": "display", "closed": 1008}
    assert not any(request["method"] == "config.get" for request in stub.requests)


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
    finally:
        try:
            await group.stop()
        finally:
            await asyncio.to_thread(stub.stop)
    assert not any(request["method"] == "config.get" for request in stub.requests)
