# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Black-box tests of real child processes and the existing northbound handlers."""

import asyncio
from contextlib import closing
import json
import os
import signal
import socket

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from agent_gateway.processes.capabilities import management_preflight
from testcases_gateway.stub_agent_server import StubAgentServer

pytestmark = pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")


@pytest.fixture
async def group(tmp_path, monkeypatch):
    for name, directory in (("HOME", "home"), ("JIUWENSWARM_HOME", "home"),
                            ("JIUWENSWARM_DATA_DIR", "data"), ("AGENT_GATEWAY_DATA_DIR", "data"),
                            ("AGENTOS_GATEWAY_LOG_DIR", "logs")):
        path = tmp_path / directory
        path.mkdir(exist_ok=True)
        monkeypatch.setenv(name, str(path))
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    stub = StubAgentServer().start()
    group = WorkerGroup(stub.url, workers=2, web_port=0, protocol_port=0, handoff_timeout=2)
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


async def test_web_stream_and_tui_acp_user_affinity(group):
    web_addr, protocol_addr = (f"{host}:{port}" for host, port in group.addresses)
    async with (
        connect(f"ws://{web_addr}/ws?user_id=alice") as web,
        connect(f"ws://{protocol_addr}/tui", additional_headers={"X-User-Id": "alice"}) as tui,
        connect(f"ws://{protocol_addr}/acp?user_id=alice", additional_headers={"X-User-Id": "alice"}) as acp,
    ):
        assert json.loads(await asyncio.wait_for(acp.recv(), 5))["event"] == "connection.ack"
        await acp.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
        assert "result" in json.loads(await asyncio.wait_for(acp.recv(), 5))
        await tui.send(json.dumps({"type": "req", "id": "tui-history", "method": "history.get",
                                   "params": {"session_id": "test-session"}}))
        async with asyncio.timeout(5):
            while True:
                frame = json.loads(await tui.recv())
                if frame.get("id") == "tui-history":
                    assert frame["ok"], frame
                    break
        await web.send(json.dumps({"type": "req", "id": "web-stream", "method": "chat.send", "is_stream": True,
                                   "params": {"session_id": "test-session", "content": "hello"}}))
        events = []
        async with asyncio.timeout(5):
            while "chat.final" not in events:
                events.append(json.loads(await web.recv()).get("event"))
        assert "chat.delta" in events
    assert sorted(group.pool.adopted_counts) == [0, 3]


async def test_http_connection_is_not_reused_and_conflicting_identity_is_rejected(group):
    web_addr, protocol_addr = (f"{host}:{port}" for host, port in group.addresses)
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.get(f"http://{web_addr}/healthz?user_id=alice")
        assert response.status_code == 200
        assert response.headers["connection"] == "close"
        assert response.headers["content-type"].startswith("application/json")
        assert response.json() == {"status": "ok"}
        rejected = await client.get(f"http://{web_addr}/healthz?user_id=bob",
                                    headers={"X-User-Id": "alice"})
        assert rejected.status_code == 400
    with pytest.raises(InvalidStatus) as error:
        async with connect(f"ws://{protocol_addr}/acp?user_id=bob", additional_headers={"X-User-Id": "alice"}):
            pytest.fail("conflicting identity is not admissible")
    assert error.value.response.status_code == 400
    assert sum(group.pool.adopted_counts) == 1


async def test_pipelined_http_finishes_only_the_first_response(group):
    reader, writer = await asyncio.open_connection(*group.addresses[0])
    try:
        for user in (b"alice", b"bob"):
            writer.write(b"GET /healthz?user_id=" + user + b" HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        output = await asyncio.wait_for(reader.read(), 5)
        headers, body = output.split(b"\r\n\r\n", 1)
        assert headers.startswith(b"HTTP/1.1 200 OK")
        assert b"connection: close" in headers.lower()
        assert json.loads(body) == {"status": "ok"}
    finally:
        writer.close()
        await writer.wait_closed()


async def test_user_burst_stays_on_owner_and_both_slots_are_reachable(group):
    host, port = group.addresses[0]
    policy = LocalIdentityPolicy()
    affinity = FixedUserHash(WorkerManifest(group.pool.slots), policy)
    users = {}
    for index in range(50):
        user = f"user-{index}"
        identity = resolve_local_identity(f"user_id={user}".encode(), (), entry="web", policy=policy)
        users.setdefault(affinity.select(identity), user)
    assert set(users) == {0, 1}

    async def open_and_confirm(user):
        async with connect(f"ws://{host}:{port}/ws?user_id={user}", proxy=None, open_timeout=5) as ws:
            assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"

    await asyncio.gather(*(open_and_confirm(users[0]) for _ in range(8)), open_and_confirm(users[1]))
    assert group.pool.adopted_counts == (8, 1)


async def test_stopping_during_spawn_reaps_the_child(tmp_path, monkeypatch):
    spawned = asyncio.Event()
    hold_spawn = asyncio.Event()
    real_spawn = asyncio.create_subprocess_exec

    async def delayed_spawn(*args, **kwargs):
        process = await real_spawn(*args, **kwargs)
        spawned.set()
        await hold_spawn.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_spawn)
    monkeypatch.setenv("HOME", str(tmp_path))
    group = WorkerGroup("ws://127.0.0.1:1", workers=2, web_port=0, protocol_port=0)
    start = asyncio.create_task(group.start())
    stop = None
    try:
        await asyncio.wait_for(spawned.wait(), 5)
        stop = asyncio.create_task(group.stop())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    finally:
        hold_spawn.set()
        if stop is not None:
            await asyncio.wait_for(stop, 20)
        else:
            await group.stop()
        result, = await asyncio.gather(start, return_exceptions=True)
    assert isinstance(result, asyncio.CancelledError)
    assert len(group.pool.processes) == 1
    assert group.pool.processes[0].returncode is not None
    assert not group.addresses


async def test_missing_ack_stops_group_and_does_not_retry_another_slot(group):
    policy = LocalIdentityPolicy()
    affinity = FixedUserHash(WorkerManifest(group.pool.slots), policy)
    identity = resolve_local_identity(b"user_id=alice", (), entry="http", policy=policy)
    worker = group.pool.processes[affinity.select(identity)]
    worker.send_signal(signal.SIGSTOP)
    reader, writer = await asyncio.open_connection(*group.addresses[0])
    try:
        writer.write(b"GET /healthz?user_id=alice HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        # Pool failure initiates shutdown. Resume only after the ACK deadline so
        # the test process can be reaped without waiting for the kill fallback.
        with pytest.raises(RuntimeError, match="handoff channel closed"):
            await asyncio.wait_for(group.pool.wait(), 5)
    finally:
        if worker.returncode is None:
            worker.send_signal(signal.SIGCONT)
        writer.close()
        await writer.wait_closed()
    with pytest.raises(RuntimeError, match="handoff channel closed"):
        await asyncio.wait_for(group.wait(), 10)
    assert reader.at_eof()
    assert group.pool.adopted_counts == (0, 0)


async def test_client_eof_and_worker_exit_release_listeners_and_workers(group):
    _, writer = await asyncio.open_connection(*group.addresses[0])
    writer.write(b"GET /ws HTTP/1.1\r\n")
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    os.kill(group.pool.processes[0].pid, signal.SIGKILL)
    with pytest.raises(RuntimeError, match="Worker 0"):
        await asyncio.wait_for(group.wait(), 10)
