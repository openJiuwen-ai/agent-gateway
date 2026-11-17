# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import json
from pathlib import Path
import shlex
import signal
from subprocess import Popen
from subprocess import TimeoutExpired
import sys
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection, connect

from testcases_support.process import (
    _seed_gateway_config,
    free_port,
    gateway_cmd,
    service_env,
    start_process,
    stop_process,
    wait_tcp,
)


@pytest.fixture(autouse=True)
def use_current_python_and_repository_config(monkeypatch: pytest.MonkeyPatch) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv(
        "ST_GATEWAY_CONFIG_SRC",
        str(repository_root / "agent_gateway" / "resources" / "config.yaml"),
    )
    monkeypatch.setenv(
        "AGENT_GATEWAY_BIN",
        f"{shlex.quote(sys.executable)} -m agent_gateway.app_gateway",
    )


def _start_gateway(
    home: Path,
    web_port: int,
    gateway_port: int,
    log_path: Path,
    agent_server_url: str,
) -> Popen:
    _seed_gateway_config(home)
    env = service_env(home)
    env["GATEWAY_PORT"] = str(gateway_port)
    command = [
        *gateway_cmd(),
        "--host",
        "127.0.0.1",
        "--port",
        str(web_port),
        "--agent-server-url",
        agent_server_url,
    ]
    proc = start_process(command, env, log_path)
    try:
        wait_tcp(web_port, proc, log_path, 40.0)
        wait_tcp(gateway_port, proc, log_path, 40.0)
    except BaseException:
        stop_process(proc)
        raise
    return proc


def _receive_connection_ack(
    ws: ClientConnection,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return json.loads(ws.recv(timeout=timeout))


def _close_connection(ws: ClientConnection) -> None:
    try:
        ws.close()
    except Exception:
        pass


def _assert_graceful_close(ws: ClientConnection) -> None:
    try:
        ws.recv(timeout=2)
    except ConnectionClosed:
        pass
    assert ws.close_code == 1001


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="SIGTERM graceful shutdown is not a Windows process contract",
)
def test_gateway_listener_lifecycle_with_live_connections_and_restart(
    tmp_path: Path,
    stub_agent_server,
) -> None:
    home = tmp_path / "gateway-p2-a"
    home.mkdir()
    web_port = free_port()
    gateway_port = free_port()
    first_log = home / f"gateway-first-{web_port}-{gateway_port}.log"
    second_log = home / f"gateway-second-{web_port}-{gateway_port}.log"

    first = _start_gateway(
        home,
        web_port,
        gateway_port,
        first_log,
        stub_agent_server.url,
    )
    second = None
    live_connections = []
    try:
        assert first.poll() is None
        for path in ("/ws", "/tui", "/acp"):
            live_connections.append(
                connect(
                    f"ws://127.0.0.1:{web_port if path == '/ws' else gateway_port}{path}",
                    open_timeout=10,
                    close_timeout=2,
                )
            )

        first_acks = [
            _receive_connection_ack(connection) for connection in live_connections
        ]
        assert len(first_acks) == 3
        for ack in first_acks:
            assert ack["type"] == "event"
            assert ack["event"] == "connection.ack"
            assert ack["payload"]["protocol_version"] == "1.0"
        assert first_acks[1]["payload"]["transport"] == "tui"
        assert first_acks[2]["payload"]["transport"] == "acp"

        first.send_signal(signal.SIGTERM)
        try:
            first.wait(timeout=20)
        except TimeoutExpired:
            pytest.fail(
                f"gateway did not exit after SIGTERM\n{first_log.read_text(encoding='utf-8')}"
            )
        assert first.returncode == 0

        for connection in live_connections:
            _assert_graceful_close(connection)
            _close_connection(connection)
        live_connections.clear()

        second = _start_gateway(
            home,
            web_port,
            gateway_port,
            second_log,
            stub_agent_server.url,
        )
        assert second.poll() is None
        for path, port in (
            ("/ws", web_port),
            ("/tui", gateway_port),
            ("/acp", gateway_port),
        ):
            connection = connect(
                f"ws://127.0.0.1:{port}{path}",
                open_timeout=10,
                close_timeout=2,
            )
            try:
                ack = _receive_connection_ack(connection)
                assert ack["type"] == "event"
                assert ack["event"] == "connection.ack"
                assert ack["payload"]["protocol_version"] == "1.0"
            finally:
                _close_connection(connection)
    finally:
        for connection in live_connections:
            _close_connection(connection)
        if first.poll() is None:
            stop_process(first)
        if second is not None and second.poll() is None:
            stop_process(second)
