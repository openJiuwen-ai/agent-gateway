# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real cold Workers: activation barrier, failure containment and panel ownership.

The AgentServer is a stub. These checks cover the Gateway side only: the
management phases, Control routing, owned store and panel handlers.
"""

import asyncio
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import socket
import threading

import pytest
from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.wire_codec import encode_agent_chunk_for_wire, encode_agent_response_for_wire
from websockets.asyncio.client import connect

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.control_commands import ControlCronCommands
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.processes import pool as pool_module
from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.processes.control_service import ControlServices
from agent_gateway.processes.pool import WorkerPool
from agent_gateway.transport.handoff import HandoffRejected
from testcases_gateway.stub_agent_server import StubAgentServer

pytestmark = pytest.mark.skipif(
    not pool_module.management_preflight().supported, reason="requires Linux/macOS fd passing",
)

MODEL = "st-model"


class _RequestLog(list):
    """The stub's request log; every request also names its connection."""

    def __init__(self, stub: "CronStubAgentServer") -> None:
        super().__init__()
        self._stub = stub

    def append(self, envelope) -> None:
        with self._stub.lock:
            super().append(envelope)
            self._stub.connections[str(envelope.get("request_id") or "")] = (
                self._stub._serving.get(threading.get_ident()))


class CronStubAgentServer(StubAgentServer):
    """The plain stub plus the three answers an owned Cron run needs.

    ``cron_push_for`` maps a streaming ``request_id`` to ``(owner, push id)``:
    that stream then carries an AgentServer Cron tool push for the owner, as
    the real ``cron.command.ack`` contract does. Each Worker has its own
    connection, so ``connection_of`` tells which Worker sent a request.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cron_push_for: dict[str, tuple[str, str]] = {}
        self._serving: dict[int, object] = {}
        self.connections: dict[str, object] = {}
        super().__init__()
        self.requests = _RequestLog(self)

    def _handle(self, connection) -> None:
        # One thread serves one connection: a request can name its connection.
        self._serving[threading.get_ident()] = connection
        try:
            super()._handle(connection)
        finally:
            self._serving.pop(threading.get_ident(), None)

    def connection_of(self, request_id):
        with self.lock:
            return self.connections.get(request_id)

    def _unary_wire(self, envelope, request_id, method):
        params = envelope.get("params") or {}
        payload = None
        if method == "models.list":
            payload = {"models": [{"model_id": "mdl_st_default", "model_name": MODEL,
                                   "alias": "ST Model", "is_free": False, "is_agentos": False}],
                       "model_groups": [], "active_model": MODEL}
        elif method == "project.cron.resolve_binding":
            payload = {"project_id": "default", "work_mode": "code"}
        elif method == "session.create":
            payload = {"session_id": "exec-" + str(params.get("cron_id") or "session")}
        if payload is None:
            return super()._unary_wire(envelope, request_id, method)
        response = AgentResponse(request_id=request_id, channel_id=str(envelope.get("channel") or "web"),
                                 payload=payload, ok=True)
        return encode_agent_response_for_wire(response, response_id=request_id)

    def _send_stream(self, connection, envelope, request_id, method):
        channel_id = str(envelope.get("channel") or "web")
        with self.lock:
            push = self.cron_push_for.pop(request_id, None)
        frames = []
        for payload in ({"event_type": "chat.delta", "content": "stub-hello"},
                        {"event_type": "chat.final", "content": "stub-hello"}):
            chunk = AgentResponseChunk(
                request_id=request_id, channel_id=channel_id, payload=payload,
                is_complete=payload["event_type"] == "chat.final")
            frames.append(encode_agent_chunk_for_wire(
                chunk, response_id=request_id, sequence=len(frames)))
        if push is not None:
            # Like the real AgentServer, the Cron tool result leaves as its
            # own server-push frame in the middle of the chat, never as a
            # chunk of the stream.
            owner, push_id = push
            tool = AgentResponseChunk(
                request_id=push_id, channel_id=channel_id, is_complete=False,
                payload={"event_type": "cron.response", "action": "list",
                         "command_id": "ack-" + push_id, "data": {}})
            wire = encode_agent_chunk_for_wire(tool, response_id=push_id, sequence=0)
            wire["metadata"] = {E2A_WIRE_SERVER_PUSH_KEY: True,
                                "_jiuwenswarm_cron_owner_user_id": owner}
            wire["session_id"] = envelope.get("session_id")
            frames.insert(1, wire)
        for frame in frames:
            connection.send(json.dumps(frame))

    def requests_for(self, method):
        with self.lock:
            return [request for request in self.requests if request.get("method") == method]


@pytest.fixture
async def cron_workspace(tmp_path, monkeypatch):
    for name, directory in (("HOME", "home"), ("JIUWENSWARM_HOME", "home"),
                            ("JIUWENSWARM_DATA_DIR", "data"), ("AGENT_GATEWAY_DATA_DIR", "data"),
                            ("AGENTOS_GATEWAY_LOG_DIR", "logs")):
        path = tmp_path / directory
        path.mkdir(exist_ok=True)
        monkeypatch.setenv(name, str(path))
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    return tmp_path


def new_group(stub, **options):
    return WorkerGroup(stub.url, workers=2, web_port=0, protocol_port=0, handoff_timeout=2,
                       cron=True, **options)


async def stop_group(group, stub):
    try:
        await group.stop()
    finally:
        await asyncio.to_thread(stub.stop)
    assert all(process.returncode is not None for process in group.pool.processes)
    for address in group.addresses:
        with closing(socket.socket()) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(address)


async def rpc(ws, request_id, method, params):
    await ws.send(json.dumps({"type": "req", "id": request_id, "method": method, "params": params}))
    async with asyncio.timeout(12):
        while True:
            frame = json.loads(await ws.recv())
            if frame.get("id") == request_id and frame.get("type") == "res":
                return frame


async def frames_until(ws, predicate, *, timeout=20):
    """Consume frames until one satisfies ``predicate``; return that frame."""
    async with asyncio.timeout(timeout):
        while True:
            frame = json.loads(await ws.recv())
            if predicate(frame):
                return frame


async def chat(ws, request_id, content="hi"):
    """One streamed turn; returns once its final has been delivered."""
    await ws.send(json.dumps({"type": "req", "id": request_id, "method": "chat.send", "is_stream": True,
                              "params": {"session_id": "chat-session", "content": content}}))
    await frames_until(ws, lambda frame: frame.get("event") == "chat.final")


def job_params(name, expression, *, targets="web", **extra):
    return {"name": name, "description": "private description of " + name, "cron_expr": expression,
            "timezone": "UTC", "targets": targets, "mode": "agent", "model_name": MODEL,
            "work_mode": "code", "wake_offset_seconds": 0, **extra}


def due_in(seconds):
    due = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return f"{due.second} {due.minute} {due.hour} {due.day} {due.month} ? {due.year}"


async def test_listeners_open_only_after_every_slot_is_active(cron_workspace, monkeypatch):
    stub = CronStubAgentServer().start()
    gate, activating = asyncio.Event(), asyncio.Event()
    original = WorkerPool.activate_cron

    async def held(pool):
        activating.set()
        await gate.wait()
        await original(pool)

    monkeypatch.setattr(WorkerPool, "activate_cron", held)
    group = new_group(stub)
    starting = asyncio.create_task(group.start())
    try:
        await asyncio.wait_for(activating.wait(), 30)
        # Every slot is prepared, none is active: nothing listens yet.
        assert group.addresses == () and group._acceptors == []
        assert all(worker.cron_activate_sent is False for worker in group.pool._workers)
        gate.set()
        await asyncio.wait_for(starting, 30)
        assert len(group.addresses) == 2
        assert all(worker.cron_active.is_set() for worker in group.pool._workers)
        assert all(process.returncode is None for process in group.pool.processes)
    finally:
        gate.set()
        await asyncio.gather(starting, return_exceptions=True)
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)


async def test_pool_refuses_handoff_until_the_slot_is_active(cron_workspace):
    stub = CronStubAgentServer().start()
    ownership = CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy())
    commands = ControlCronCommands(ownership, stub.url)
    pool = WorkerPool(
        stub.url, 2, cron_manifest=ownership.manifest,
        control_services=ControlServices(handlers_factory=commands.handlers_for,
                                         on_connect=commands.endpoint_connected,
                                         on_disconnect=commands.endpoint_disconnected),
    )
    parent = child = None
    try:
        await pool.start()
        parent, child = socket.socketpair()
        request = b"GET /healthz?user_id=alice HTTP/1.1\r\nHost: localhost\r\n\r\n"
        with pytest.raises(HandoffRejected):
            await pool.handoff(0, child, "web-http", request, timeout=2)
        assert child.fileno() >= 0 and sum(pool.adopted_counts) == 0
        assert sorted(commands._endpoints) == [0, 1]
        await pool.activate_cron()
        with pytest.raises(RuntimeError, match="already active"):
            await pool.activate_cron()
        await pool.handoff(0, child, "web-http", request, timeout=5)
        child.close()
        async with asyncio.timeout(5):
            response = await asyncio.get_running_loop().sock_recv(parent, 4096)
        assert response.startswith(b"HTTP/1.1 200 OK") and b"connection: close" in response.lower()
        assert pool.adopted_counts == (1, 0)
    finally:
        for sock in (parent, child):
            if sock is not None and sock.fileno() >= 0:
                sock.close()
        await pool.stop()
        await asyncio.to_thread(stub.stop)
    assert all(process.returncode == 0 for process in pool.processes)


async def test_failed_activation_fails_the_group_and_reaps_every_worker(cron_workspace, monkeypatch):
    stub = CronStubAgentServer().start()
    original = ManagementChannel.send

    async def duplicated(self, message, **options):
        await original(self, message, **options)
        if message["type"] == "CRON_ACTIVATE" and message["slot_id"] == 1:
            # A parent must never ask twice; the slot treats it as a protocol
            # failure and the whole generation is stopped.
            await original(self, message, **options)

    monkeypatch.setattr(ManagementChannel, "send", duplicated)
    group = new_group(stub)
    with pytest.raises(RuntimeError, match=r"Worker 1: .*FAILED \(MANAGEMENT_PROTOCOL\)"):
        await group.start()
    assert group.addresses == ()
    await stop_group(group, stub)
    # The healthy slot stops cleanly; the failing one is reaped non-zero.
    assert [process.returncode == 0 for process in group.pool.processes] == [True, False]


async def test_activation_timeout_fails_the_group_and_reaps_every_worker(cron_workspace, monkeypatch):
    stub = CronStubAgentServer().start()
    original = ManagementChannel.send

    async def dropped(self, message, **options):
        if message["type"] == "CRON_ACTIVATE" and message["slot_id"] == 1:
            return
        await original(self, message, **options)

    monkeypatch.setattr(ManagementChannel, "send", dropped)
    group = new_group(stub)
    pool_start = group.pool.start

    async def start_then_bound_activation():
        await pool_start()
        group.pool._startup_timeout = 1.0

    monkeypatch.setattr(group.pool, "start", start_then_bound_activation)
    with pytest.raises(RuntimeError, match="activation timed out"):
        await asyncio.wait_for(group.start(), 60)
    assert group.addresses == ()
    await stop_group(group, stub)
    assert all(process.returncode is not None for process in group.pool.processes)


async def test_panel_jobs_run_once_for_their_owner_and_users_stay_isolated(cron_workspace):
    stub = CronStubAgentServer().start()
    group = new_group(stub)
    try:
        await group.start()
        host, port = group.addresses[0]
        ownership = group._cron_ownership
        # One user on each configured slot; a third shares Alice's slot.
        first = "alice"
        second = next(name for name in ("bob", "carol", "dave", "erin", "frank")
                      if ownership.owner_slot(name) != ownership.owner_slot(first))
        assert {ownership.owner_slot(first), ownership.owner_slot(second)} == {0, 1}
        async with connect(f"ws://{host}:{port}/ws?user_id={first}", proxy=None, open_timeout=5) as alice, \
                connect(f"ws://{host}:{port}/ws?user_id={second}", proxy=None, open_timeout=5) as other, \
                connect(f"ws://{host}:{port}/ws", proxy=None, open_timeout=5) as anonymous:
            users = {first: alice, second: other}
            for ws in users.values():
                assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            jobs = {}
            for user, ws in users.items():
                # The payload identity is replaced by the connection's user.
                created = await rpc(ws, "create-" + user, "cron.job.create", job_params(
                    "task-" + user, "0 9 * * *", user_id="forged", group_id="forged", bot_id="forged"))
                assert created["ok"], created
                job = created["payload"]["job"]
                assert job["user_id"] == user and job["model_name"] == MODEL
                jobs[user] = job["id"]
            assert len(set(jobs.values())) == 2

            # Every slot sees only its own users' records.
            for user, ws in users.items():
                listed = await rpc(ws, "list-" + user, "cron.job.list", {"user_id": "forged"})
                assert listed["ok"] and [item["id"] for item in listed["payload"]["jobs"]] == [jobs[user]]

            # Cross-owner reads and forged updates are refused without change.
            foreign = jobs[second]
            denied = await rpc(alice, "foreign", "cron.job.get", {"id": foreign})
            assert denied["ok"] is False and denied["code"] == "NOT_FOUND"
            forged = await rpc(alice, "forged-toggle", "cron.job.toggle", {"id": foreign, "enabled": False})
            assert forged["ok"] is False
            untouched = await rpc(other, "untouched", "cron.job.get", {"id": foreign})
            assert untouched["payload"]["job"]["enabled"] is True
            anonymous_list = await rpc(anonymous, "anonymous", "cron.job.list", {"user_id": first})
            assert anonymous_list["ok"] is False and anonymous_list["code"] == "CRON_IDENTITY_REQUIRED"
            project = await rpc(alice, "project", "cron.job.create", job_params(
                "private", "0 9 * * *", project_id="private-project"))
            assert project["ok"] is False and project["code"] == "CRON_PROJECT_DISABLED"

            # A manual run allocates one execution session and one chat.
            for user, ws in users.items():
                run = await rpc(ws, "run-" + user, "cron.job.run_now", {"id": jobs[user]})
                assert run["ok"], run
                frame = await frames_until(
                    ws, lambda frame, user=user: (frame.get("payload") or {}).get("cron", {}).get("job_id")
                    == jobs[user] and frame.get("event") == "chat.final",
                )
                assert "private description" not in json.dumps(frame)
            for user, job_id in jobs.items():
                created_sessions = [request for request in stub.requests_for("session.create")
                                    if request["params"].get("cron_id") == job_id]
                chats = [request for request in stub.requests_for("chat.send")
                         if request.get("session_id") == "exec-" + job_id]
                assert len(created_sessions) == 1 and len(chats) == 1
                assert created_sessions[0]["user_id"] == user and chats[0]["user_id"] == user
                saved = await rpc(users[user], "saved-" + user, "cron.job.get", {"id": job_id})
                assert saved["payload"]["job"]["last_session_id"] == "exec-" + job_id
    finally:
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)


def users_on_both_slots(ownership):
    """One name per configured slot, in slot order."""
    found = {}
    for name in ("alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"):
        found.setdefault(ownership.owner_slot(name), name)
        if len(found) == len(ownership.manifest.slots):
            break
    assert len(found) == len(ownership.manifest.slots)
    return [found[slot] for slot in sorted(found)]


def routed_command(command_id, owner, action="list", data=None):
    return {"request_id": "routed-request", "session_id": "routed-session", "channel_id": "web",
            "owner_user_id": owner, "owner_group_id": "default", "owner_bot_id": "default",
            "payload": {"event_type": "cron.response", "action": action, "command_id": command_id,
                        "data": data or {}}}


async def test_control_routes_a_command_to_the_owner_slot_worker(cron_workspace):
    stub = CronStubAgentServer().start()
    group = new_group(stub)
    try:
        await group.start()
        host, port = group.addresses[0]
        ownership = group._cron_ownership
        here, there = users_on_both_slots(ownership)
        async with connect(f"ws://{host}:{port}/ws?user_id={here}", proxy=None, open_timeout=5) as local, \
                connect(f"ws://{host}:{port}/ws?user_id={there}", proxy=None, open_timeout=5) as owner:
            for ws in (local, owner):
                assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            created = await rpc(owner, "create-there", "cron.job.create", job_params("owners-task", "0 9 * * *"))
            assert created["ok"], created
            job_id = created["payload"]["job"]["id"]

            router = group._cron_commands
            # The Control plane reaches the Worker that owns the user, even
            # when another slot's endpoint asks. Only bound endpoints may ask.
            source = router._endpoints[ownership.owner_slot(here)][0]
            assert await router._route_command(source, routed_command("unbound-owner", "default")) == {
                "status": "rejected", "code": "INVALID_COMMAND"}
            forged = dict(router._endpoints[ownership.owner_slot(here)][0].__dict__)
            forged["worker_pid"] += 1
            # Another process incarnation of the same slot is not a source.
            assert await router._route_command(type(source)(**forged), routed_command("forged-source", there)) == {
                "status": "rejected", "code": "SOURCE_ENDPOINT_UNAVAILABLE"}
            assert await router._route_command(source, routed_command("routed-list-1", there)) == {
                "status": "accepted", "code": "DELIVERED"}
            # The same command id is answered from history, not executed twice.
            assert await router._route_command(source, routed_command("routed-list-1", there)) == {
                "status": "accepted", "code": "DELIVERED"}
            async with asyncio.timeout(10):
                while True:
                    acks = [request for request in stub.requests_for("cron.command.ack")
                            if request["params"].get("command_id") == "routed-list-1"]
                    if acks:
                        break
                    await asyncio.sleep(0.02)
            assert len(acks) == 1
            # The owner's slot executed it: another slot would have refused.
            assert acks[0]["user_id"] == there
            assert [job["id"] for job in acks[0]["params"]["data"]] == [job_id]
            assert not [request for request in stub.requests_for("cron.command.ack")
                        if request["params"].get("command_id") in {"unbound-owner", "forged-source"}]
    finally:
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)


async def test_cron_tool_push_in_a_chat_stream_is_answered_to_the_agentserver(cron_workspace):
    stub = CronStubAgentServer().start()
    group = new_group(stub)
    try:
        await group.start()
        host, port = group.addresses[0]
        ownership = group._cron_ownership
        here, there = users_on_both_slots(ownership)
        async with connect(f"ws://{host}:{port}/ws?user_id={here}", proxy=None, open_timeout=5) as ws:
            assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            created = await rpc(ws, "create-here", "cron.job.create", job_params("chats-task", "0 9 * * *"))
            assert created["ok"], created
            job_id = created["payload"]["job"]["id"]

            # The AgentServer tool push rides a chat of its own user.
            stub.cron_push_for["chat-owned"] = (here, "chat-owned")
            await chat(ws, "chat-owned", "list my tasks")
            # A push naming another user inside this chat is refused with a
            # fixed error: it is neither run here nor routed to that owner.
            stub.cron_push_for["chat-forged"] = (there, "chat-forged")
            await chat(ws, "chat-forged", "list my tasks")
            async with asyncio.timeout(10):
                while len(stub.requests_for("cron.command.ack")) < 2:
                    await asyncio.sleep(0.02)
            acks = {request["params"]["command_id"]: request
                    for request in stub.requests_for("cron.command.ack")}
            assert set(acks) == {"ack-chat-owned", "ack-chat-forged"}
            assert acks["ack-chat-owned"]["user_id"] == here
            assert [job["id"] for job in acks["ack-chat-owned"]["params"]["data"]] == [job_id]
            assert acks["ack-chat-forged"]["params"]["data"] == {
                "error": "Cron command owner conflicts with the original chat context",
                "code": "CRON_CONTEXT_CONFLICT"}
    finally:
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)


async def test_cron_tool_push_for_a_remote_owner_is_answered_from_the_owner_worker(cron_workspace):
    stub = CronStubAgentServer().start()
    group = new_group(stub)
    try:
        await group.start()
        host, port = group.addresses[0]
        ownership = group._cron_ownership
        here, there = users_on_both_slots(ownership)
        async with connect(f"ws://{host}:{port}/ws?user_id={here}", proxy=None, open_timeout=5) as local, \
                connect(f"ws://{host}:{port}/ws?user_id={there}", proxy=None, open_timeout=5) as owner:
            for ws in (local, owner):
                assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            created = await rpc(owner, "create-there", "cron.job.create", job_params("owners-task", "0 9 * * *"))
            assert created["ok"], created
            job_id = created["payload"]["job"]["id"]
            # The owner Worker is the one whose AgentServer connection carries
            # ``there``'s own chat; the other Worker has no chat of ``there``.
            await chat(owner, "chat-there")
            owner_worker = stub.connection_of("chat-there")
            assert owner_worker is not None

            # A Cron tool frame for that owner reaches the other Worker all
            # the same, on a request id of its own, and is answered there.
            stub.cron_push_for["chat-here"] = (there, "standalone-push")
            await chat(local, "chat-here")
            assert stub.connection_of("chat-here") is not owner_worker
            async with asyncio.timeout(10):
                while True:
                    acks = [request for request in stub.requests_for("cron.command.ack")
                            if request["params"].get("command_id") == "ack-standalone-push"]
                    if acks:
                        break
                    await asyncio.sleep(0.02)
            await asyncio.sleep(0.3)
            # Run once, for that owner only, over the owner Worker's own
            # AgentServer connection: the chatting Worker executed nothing.
            assert [request["params"]["command_id"]
                    for request in stub.requests_for("cron.command.ack")] == ["ack-standalone-push"]
            assert acks[0]["user_id"] == there
            assert [job["id"] for job in acks[0]["params"]["data"]] == [job_id]
            assert stub.connection_of("cron-command-ack-ack-standalone-push") is owner_worker
    finally:
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)


async def test_tui_panel_binds_the_connection_user_and_delivers_results_on_tui(cron_workspace):
    stub = CronStubAgentServer().start()
    group = new_group(stub)
    try:
        await group.start()
        web_host, web_port = group.addresses[0]
        tui_host, tui_port = group.addresses[1]
        ownership = group._cron_ownership
        alice = "alice"
        charlie = next(name for name in ("carol", "dave", "erin", "frank", "grace", "heidi", "ivan")
                       if name != alice and ownership.owner_slot(name) == ownership.owner_slot(alice))
        async with connect(f"ws://{tui_host}:{tui_port}/tui?user_id={alice}", proxy=None, open_timeout=5) as tui, \
                connect(f"ws://{tui_host}:{tui_port}/tui?user_id={charlie}", proxy=None, open_timeout=5) as other, \
                connect(f"ws://{tui_host}:{tui_port}/tui", proxy=None, open_timeout=5) as anonymous:
            for ws in (tui, other, anonymous):
                assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            # Charlie never chats: the two users only share one Worker.
            anonymous_list = await rpc(anonymous, "tui-anonymous", "cron.job.list", {"user_id": alice})
            assert anonymous_list["ok"] is False and anonymous_list["code"] == "CRON_IDENTITY_REQUIRED"
            created = await rpc(tui, "tui-create", "cron.job.create", job_params(
                "tui-task", "0 9 * * *", targets="tui", user_id=charlie, group_id="forged"))
            assert created["ok"], created
            job_id = created["payload"]["job"]["id"]
            assert created["payload"]["job"]["user_id"] == alice
            listed = await rpc(other, "tui-list", "cron.job.list", {"user_id": alice})
            assert listed["ok"] and listed["payload"]["jobs"] == []
            foreign = await rpc(other, "tui-foreign", "cron.job.run_now", {"id": job_id})
            assert foreign["ok"] is False

            run = await rpc(tui, "tui-run", "cron.job.run_now", {"id": job_id})
            assert run["ok"], run
            frame = await frames_until(
                tui, lambda frame: frame.get("event") == "chat.final"
                and (frame.get("payload") or {}).get("cron", {}).get("job_id") == job_id,
            )
            assert "private description" not in json.dumps(frame)
            # Nothing of Alice's run reaches the same-slot neighbour.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(other.recv(), 0.5)
            # The web panel of the same user reads the same owned record.
            async with connect(f"ws://{web_host}:{web_port}/ws?user_id={alice}", proxy=None, open_timeout=5) as web:
                assert json.loads(await asyncio.wait_for(web.recv(), 5))["event"] == "connection.ack"
                saved = await rpc(web, "web-read", "cron.job.get", {"id": job_id})
                assert saved["payload"]["job"]["last_session_id"] == "exec-" + job_id
    finally:
        await stop_group(group, stub)
    assert all(process.returncode == 0 for process in group.pool.processes)
