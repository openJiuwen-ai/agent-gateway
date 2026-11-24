from __future__ import annotations

from pathlib import Path
import shlex
from subprocess import TimeoutExpired
import sys

import pytest

from testcases_support.process import (
    free_port,
    gateway_cmd,
    service_env,
    start_gateway,
    start_process,
    stop_process,
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


def test_single_gateway_can_restart_in_same_workspace(
    gateway_factory,
    stub_agent_server,
) -> None:
    first = gateway_factory(stub_agent_server.url)
    home = first.home
    first.stop()

    second = start_gateway(home, stub_agent_server.url)
    try:
        assert second.proc.poll() is None
    finally:
        second.stop()


def test_second_gateway_process_is_rejected_while_first_stays_alive(
    gateway_factory,
    stub_agent_server,
) -> None:
    first = gateway_factory(stub_agent_server.url)
    home = first.home
    second_port = free_port()
    log_path = home / f"gateway-reject-{second_port}.log"
    env = service_env(home)
    env["GATEWAY_PORT"] = str(free_port())
    command = [
        *gateway_cmd(),
        "--host",
        "127.0.0.1",
        "--port",
        str(second_port),
        "--agent-server-url",
        stub_agent_server.url,
    ]
    second = start_process(command, env, log_path)

    try:
        second.communicate(timeout=40)
    except TimeoutExpired:
        stop_process(second)
        pytest.fail(f"second Gateway did not reject within 40 seconds\n{log_path.read_text()}")
    finally:
        if second.poll() is None:
            stop_process(second)

    assert second.returncode == 1
    assert "refusing duplicate instance" in log_path.read_text(encoding="utf-8")
    assert first.proc.poll() is None
