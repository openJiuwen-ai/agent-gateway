# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from contextlib import closing
import os
import re
import socket
import sys

import httpx
import pytest

from agent_gateway.processes.capabilities import management_preflight
from testcases_gateway.stub_agent_server import StubAgentServer


@pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")
async def test_cli_starts_two_workers_and_releases_listeners_on_sigterm(tmp_path):
    env = {
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
    }
    stub = StubAgentServer().start()
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "agent_gateway.app_gateway", "--mode", "multiprocess",
            "--workers", "2", "--host", "127.0.0.1", "--port", "0", "--web-path", "/ws",
            "--agent-server-url", stub.url,
            env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        lines = []
        async with asyncio.timeout(40):
            while line := await proc.stdout.readline():
                lines.append(line.decode(errors="replace"))
                ready = re.search(r"multiprocess_ready workers=2 web_port=(\d+) protocol_port=(\d+) "
                                  r"worker_pids=(\d+),(\d+)", lines[-1])
                if ready:
                    break
            else:
                pytest.fail("Gateway failed to become ready:\n" + "".join(lines[-25:]))
        web_port, protocol_port, *worker_pids = map(int, ready.groups())
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.get(f"http://127.0.0.1:{web_port}/healthz")
            assert response.json() == {"status": "ok"}
            assert response.headers["connection"] == "close"
        proc.terminate()
        await asyncio.wait_for(proc.communicate(), 15)
        assert proc.returncode == 0
        for pid in worker_pids:
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        for port in (web_port, protocol_port):
            with closing(socket.socket()) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", port))
    finally:
        try:
            if proc is not None and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.communicate(), 15)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
        finally:
            await asyncio.to_thread(stub.stop)
