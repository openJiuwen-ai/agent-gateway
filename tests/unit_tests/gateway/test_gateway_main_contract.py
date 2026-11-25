from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import os
from pathlib import Path
import time
import subprocess
import sys
from typing import Any
from unittest.mock import Mock

import pytest


_LOCK_PROBE_SCRIPT = """
import sys
from pathlib import Path

from agent_gateway.gateway_lock import GatewayLock

lock = GatewayLock(Path(sys.argv[1]))
if not lock.acquire(timeout=0):
    print("busy")
    raise SystemExit(1)
print("acquired")
lock.release()
"""

_LOCK_HOLDER_SCRIPT = """
import sys
from pathlib import Path

from agent_gateway.gateway_lock import GatewayLock

lock = GatewayLock(Path(sys.argv[1]))
if not lock.acquire(timeout=0):
    ready_path = Path(sys.argv[2])
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_text("busy", encoding="utf-8")
    raise SystemExit(1)
ready_path = Path(sys.argv[2])
ready_path.parent.mkdir(parents=True, exist_ok=True)
ready_path.write_text("acquired", encoding="utf-8")
sys.stdin.readline()
lock.release()
"""


@dataclass(frozen=True)
class _RunCall:
    agent_server_url: str
    web_host: str
    web_port: int
    web_path: str
    web_ws_deflate: bool


def _isolated_python_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["JIUWENSWARM_HOME"] = str(tmp_path)
    env["JIUWENSWARM_DATA_DIR"] = str(tmp_path)
    return env


def _wait_for_ready_file(path: Path, timeout: float = 10.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        time.sleep(0.05)
    raise TimeoutError(f"lock holder did not report readiness within {timeout:.0f}s")


def _stop_lock_holder(holder: subprocess.Popen[str]) -> None:
    if holder.poll() is not None:
        return
    if holder.stdin is not None:
        holder.stdin.close()
    try:
        holder.wait(timeout=10)
    except subprocess.TimeoutExpired:
        holder.kill()
        holder.wait(timeout=5)


def _assert_lock_is_released(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", _LOCK_PROBE_SCRIPT, str(tmp_path)],
        capture_output=True,
        text=True,
        env=_isolated_python_env(tmp_path),
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "acquired"


def _patch_main_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    run: Callable[..., Awaitable[None]],
) -> None:
    from agent_gateway import app_gateway
    from agent_gateway.routing import mcp_remote, session_metadata_remote

    monkeypatch.setenv("WEB_WS_PERMESSAGE_DEFLATE", "0")
    monkeypatch.setattr(app_gateway, "get_infra_data_dir", lambda: tmp_path)
    monkeypatch.setattr(app_gateway, "_run_with_shutdown_signals", run)
    monkeypatch.setattr(app_gateway, "install_crash_exit_handler", Mock())
    monkeypatch.setattr(app_gateway, "install_async_dump_handler", Mock())
    monkeypatch.setattr(session_metadata_remote, "install", Mock())
    monkeypatch.setattr(mcp_remote, "install", Mock())


def test_main_passes_explicit_cli_options_and_releases_workspace_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from agent_gateway import app_gateway

    calls: list[_RunCall] = []

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        web_ws_deflate: bool,
    ) -> None:
        calls.append(
            _RunCall(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )

    _patch_main_boundaries(monkeypatch, tmp_path, run)
    monkeypatch.setattr(
        app_gateway.sys,
        "argv",
        [
            "agent-gateway",
            "--agent-server-url",
            "ws://127.0.0.1:18092",
            "--host",
            "127.0.0.2",
            "--port",
            "19001",
            "--web-path",
            "/gateway-ws",
        ],
    )
    monkeypatch.setenv("AGENT_SERVER_URL", "ws://ignored:1")
    monkeypatch.setenv("WEB_HOST", "ignored")
    monkeypatch.setenv("WEB_PORT", "1")
    monkeypatch.setenv("WEB_PATH", "/ignored")

    app_gateway.main()

    assert calls == [
        _RunCall(
            agent_server_url="ws://127.0.0.1:18092",
            web_host="127.0.0.2",
            web_port=19001,
            web_path="/gateway-ws",
            web_ws_deflate=False,
        )
    ]
    _assert_lock_is_released(tmp_path)


def test_main_passes_environment_defaults_and_releases_workspace_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from agent_gateway import app_gateway

    calls: list[_RunCall] = []

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        web_ws_deflate: bool,
    ) -> None:
        calls.append(
            _RunCall(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )

    _patch_main_boundaries(monkeypatch, tmp_path, run)
    monkeypatch.setattr(app_gateway.sys, "argv", ["agent-gateway"])
    monkeypatch.setenv("AGENT_SERVER_URL", "ws://127.0.0.1:18092")
    monkeypatch.setenv("WEB_HOST", "127.0.0.2")
    monkeypatch.setenv("WEB_PORT", "19001")
    monkeypatch.setenv("WEB_PATH", "/gateway-ws")

    app_gateway.main()

    assert calls == [
        _RunCall(
            agent_server_url="ws://127.0.0.1:18092",
            web_host="127.0.0.2",
            web_port=19001,
            web_path="/gateway-ws",
            web_ws_deflate=False,
        )
    ]
    _assert_lock_is_released(tmp_path)


def test_main_uses_fallback_defaults_when_environment_is_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from agent_gateway import app_gateway

    calls: list[_RunCall] = []

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        web_ws_deflate: bool,
    ) -> None:
        calls.append(
            _RunCall(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )

    _patch_main_boundaries(monkeypatch, tmp_path, run)
    monkeypatch.setattr(app_gateway.sys, "argv", ["agent-gateway"])
    for name in (
        "AGENT_SERVER_URL",
        "AGENT_SERVER_HOST",
        "AGENT_SERVER_PORT",
        "AGENT_PORT",
        "WEB_HOST",
        "WEB_PORT",
        "WEB_PATH",
    ):
        monkeypatch.delenv(name, raising=False)

    app_gateway.main()

    assert calls == [
        _RunCall(
            agent_server_url="ws://127.0.0.1:18092",
            web_host="127.0.0.1",
            web_port=19000,
            web_path="/ws",
            web_ws_deflate=False,
        )
    ]
    _assert_lock_is_released(tmp_path)


def test_main_releases_workspace_lock_when_startup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from agent_gateway import app_gateway

    calls: list[_RunCall] = []

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        web_ws_deflate: bool,
    ) -> None:
        calls.append(
            _RunCall(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )
        raise RuntimeError("startup failed")

    _patch_main_boundaries(monkeypatch, tmp_path, run)
    monkeypatch.setattr(
        app_gateway.sys,
        "argv",
        [
            "agent-gateway",
            "--agent-server-url",
            "ws://127.0.0.1:18092",
        ],
    )

    with pytest.raises(RuntimeError, match="startup failed"):
        app_gateway.main()

    assert len(calls) == 1
    _assert_lock_is_released(tmp_path)


def test_main_rejects_a_lock_held_by_another_process_without_releasing_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from agent_gateway import app_gateway
    from agent_gateway.gateway_lock import GatewayLock

    calls: list[_RunCall] = []

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        web_ws_deflate: bool,
    ) -> None:
        calls.append(
            _RunCall(
                agent_server_url=agent_server_url,
                web_host=web_host,
                web_port=web_port,
                web_path=web_path,
                web_ws_deflate=web_ws_deflate,
            )
        )
        pytest.fail("a second Gateway must not start while another process holds the lock")

    _patch_main_boundaries(monkeypatch, tmp_path, run)
    monkeypatch.setattr(
        app_gateway.sys,
        "argv",
        [
            "agent-gateway",
            "--agent-server-url",
            "ws://127.0.0.1:18092",
        ],
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", _LOCK_HOLDER_SCRIPT, str(tmp_path), str(tmp_path / "holder.ready")],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        env=_isolated_python_env(tmp_path),
    )
    try:
        assert _wait_for_ready_file(tmp_path / "holder.ready") == "acquired"
        assert holder.poll() is None
        try:
            with pytest.raises(SystemExit) as excinfo:
                app_gateway.main()

            assert excinfo.value.code == 1
            assert calls == []
            assert holder.poll() is None
            holder_info = GatewayLock.find_holder(tmp_path)
            assert holder_info is not None
            assert holder_info["pid"] == holder.pid
        finally:
            _stop_lock_holder(holder)
    finally:
        _stop_lock_holder(holder)

    _assert_lock_is_released(tmp_path)
