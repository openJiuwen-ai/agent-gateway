# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""The public ``agent-gateway`` entry point with sharded Cron explicitly enabled.

Regression: ``--enable-cron`` once crashed inside ``validate_workspace``
because the workspace gate was handed a Worker manifest instead of a
Cron ownership. Only the real entry point exercises that path.
"""

import asyncio
from contextlib import closing
import json
import os
import re
import socket
import sys

import pytest
from websockets.asyncio.client import connect

from agent_gateway.processes.capabilities import management_preflight
from testcases_gateway.stub_agent_server import StubAgentServer

pytestmark = pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")


def environment(tmp_path, **extra):
    return {
        **os.environ,
        "HOME": str(tmp_path),
        "AGENT_GATEWAY_HOME": str(tmp_path),
        "AGENT_GATEWAY_DATA_DIR": str(tmp_path / "data"),
        "JIUWENSWARM_DATA_DIR": str(tmp_path / "data"),
        "JIUWENSWARM_HOME": str(tmp_path),
        "JIUWENSWARM_EDITION": "personal",
        "GATEWAY_HOST": "127.0.0.1",
        "GATEWAY_PORT": "0",
        "WEB_WS_PERMESSAGE_DEFLATE": "0",
        **extra,
    }


async def start_gateway(arguments, env, *, merged_output=True):
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "agent_gateway.app_gateway", *arguments,
        env=env, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT if merged_output else asyncio.subprocess.PIPE,
    )


async def read_until(proc, pattern, *, timeout=40):
    """Return (match, lines) once a stdout line matches; fail with the tail."""
    lines = []
    async with asyncio.timeout(timeout):
        while line := await proc.stdout.readline():
            lines.append(line.decode(errors="replace"))
            ready = re.search(pattern, lines[-1])
            if ready:
                return ready, lines
    pytest.fail("Gateway did not report the expected line:\n" + "".join(lines[-25:]))


async def stop_gateway(proc, worker_pids, ports):
    proc.terminate()
    await asyncio.wait_for(proc.communicate(), 15)
    assert proc.returncode == 0
    for pid in worker_pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    for port in ports:
        with closing(socket.socket()) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))


async def rpc(ws, request_id, method, params):
    await ws.send(json.dumps({"type": "req", "id": request_id, "method": method, "params": params}))
    async with asyncio.timeout(10):
        while True:
            frame = json.loads(await ws.recv())
            if frame.get("id") == request_id and frame.get("type") == "res":
                return frame


async def test_cli_enable_cron_starts_prepared_workers_and_serves_the_panel(tmp_path):
    stub = StubAgentServer().start()
    proc = None
    try:
        proc = await start_gateway(
            ["--mode", "multiprocess", "--workers", "2", "--host", "127.0.0.1", "--port", "0",
             "--web-path", "/ws", "--agent-server-url", stub.url, "--enable-cron"],
            environment(tmp_path),
        )
        # Cron is reported ready only after every slot was activated; the
        # group then opens its listeners and the launcher reports them.
        ready, lines = await read_until(proc, r"multiprocess_cron_ready workers=2")
        assert not any("multiprocess_ready workers=2 web_port=" in line for line in lines)
        ready, _lines = await read_until(
            proc, r"multiprocess_ready workers=2 web_port=(\d+) protocol_port=(\d+) worker_pids=(\d+),(\d+)")
        web_port, protocol_port, *worker_pids = map(int, ready.groups())
        async with connect(f"ws://127.0.0.1:{web_port}/ws?user_id=alice", proxy=None, open_timeout=5) as alice, \
                connect(f"ws://127.0.0.1:{web_port}/ws?user_id=bob", proxy=None, open_timeout=5) as bob, \
                connect(f"ws://127.0.0.1:{web_port}/ws", proxy=None, open_timeout=5) as anonymous:
            for ws in (alice, bob, anonymous):
                assert json.loads(await asyncio.wait_for(ws.recv(), 5))["event"] == "connection.ack"
            created = await rpc(alice, "create-1", "cron.job.create", {
                "name": "cli-task", "description": "answer", "cron_expr": "0 9 * * *",
                "timezone": "UTC", "targets": "web", "mode": "agent", "model_name": "st-model",
                "work_mode": "code", "user_id": "bob",
            })
            assert created["ok"], created
            assert created["payload"]["job"]["user_id"] == "alice"
            listed = await rpc(alice, "list-1", "cron.job.list", {})
            assert listed["ok"] and [job["id"] for job in listed["payload"]["jobs"]] == [
                created["payload"]["job"]["id"]]
            # Another user on the same or another slot never sees Alice's job.
            assert (await rpc(bob, "list-2", "cron.job.list", {"user_id": "alice"}))["payload"]["jobs"] == []
            denied = await rpc(anonymous, "list-3", "cron.job.list", {"user_id": "alice"})
            assert denied["ok"] is False and denied["code"] == "CRON_IDENTITY_REQUIRED"
        await stop_gateway(proc, worker_pids, (web_port, protocol_port))
    finally:
        try:
            if proc is not None and proc.returncode is None:
                proc.terminate()
                try:
                    output, _ = await asyncio.wait_for(proc.communicate(), 15)
                except TimeoutError:
                    proc.kill()
                    output, _ = await proc.communicate()
                pytest.fail("Gateway was still running:\n" + output.decode(errors="replace")[-3000:])
        finally:
            await asyncio.to_thread(stub.stop)


@pytest.mark.parametrize("enabled,expected", [(False, 0), (True, 1)])
async def test_cron_check_is_read_only_and_reports_compatibility(tmp_path, enabled, expected):
    data = tmp_path / "data"
    # The personal file layout the Worker reads: nothing else is consulted.
    path = data / "profile/agent/home/cron_jobs.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"version": 1, "jobs": [
        {"id": "review-job", "name": "task", "enabled": enabled, "cron_expr": "0 0 8 * * ? *",
         "timezone": "UTC", "description": "private task text", "targets": "web", "user_id": "alice",
         "credential_ref": "do-not-print"},
        {"id": "project-job", "name": "task", "enabled": enabled, "cron_expr": "0 0 8 * * ? *",
         "timezone": "UTC", "description": "private task text", "targets": "web", "user_id": "alice",
         "project_id": "private-project"},
    ]}))
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    proc = await start_gateway(
        ["--mode", "multiprocess", "--enable-cron", "--check-cron", "--workers", "2"],
        environment(tmp_path, AGENTOS_GATEWAY_LOG_DIR=str(tmp_path / "logs")), merged_output=False,
    )
    out, err = await asyncio.wait_for(proc.communicate(), 20)
    assert proc.returncode == expected, err.decode()
    assert err == b"", err
    report = json.loads(out)
    assert report["scope"] == "static_file_only"
    assert report["ready"] is not enabled
    from agent_gateway.common.ingress_identity import LocalIdentityPolicy
    from agent_gateway.common.worker_affinity import WorkerManifest
    from agent_gateway.cron.ownership import CronOwnership

    slot = CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy()).owner_slot("alice")
    assert report["issues"] == [
        {"job_id": "review-job", "slot": slot, "reason": "credential_ref", "blocking": enabled},
        {"job_id": "project-job", "slot": slot, "reason": "non_default_project", "blocking": enabled},
    ]
    assert report["agentserver_checked"] is False
    assert b"do-not-print" not in out + err and b"private task text" not in out + err
    assert before == {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}


@pytest.mark.parametrize("configuration", ["[private-marker]", "gateway: {cron: private-marker}", "broken: [private-marker"])
async def test_cron_check_reports_invalid_config_without_echoing_values(tmp_path, configuration):
    data = tmp_path / "data"
    config = data / "config/config-gateway.yaml"
    config.parent.mkdir(parents=True)
    config.write_text(configuration)
    proc = await start_gateway(
        ["--mode", "multiprocess", "--enable-cron", "--check-cron"],
        environment(tmp_path, HOME=str(tmp_path), JIUWENSWARM_RUNTIME_PROFILE="",
                    AGENTOS_GATEWAY_LOG_DIR=str(tmp_path / "logs")), merged_output=False,
    )
    out, err = await asyncio.wait_for(proc.communicate(), 20)
    assert proc.returncode == 1
    assert json.loads(out) == {
        "scope": "static_file_only", "agentserver_checked": False, "ready": False,
        "error": "invalid_or_unreadable_cron_configuration_or_snapshot",
    }
    assert b"private-marker" not in out + err and b"Traceback" not in err
    assert sorted(path for path in tmp_path.rglob("*") if path.is_file()) == [config]
