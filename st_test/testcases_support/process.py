# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ST 公共进程工具：端口分配、gateway 入口解析、子进程启动与就绪探测。

契约 ST（``testcases_gateway``）与 workswarm 兼容 ST（``testcases_integration``）
共用，避免两套用例各写一份启动逻辑。
"""

from __future__ import annotations

import importlib.util
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

START_TIMEOUT = 30.0
WS_PATH = "/ws"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def service_env(home: Path) -> dict[str, str]:
    """子进程环境：隔离工作区，并去掉可能切换版本形态的企业版开关。"""
    env = dict(os.environ)
    env.setdefault("PYTHONUNBUFFERED", "1")
    env["JIUWENSWARM_HOME"] = str(home)
    env["JIUWENSWARM_DATA_DIR"] = str(home / ".jiuwenswarm")
    env.pop("JIUWENSWARM_EDITION", None)
    return env


def _importable(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def gateway_cmd() -> list[str]:
    """定位 gateway 启动命令。

    优先 ``AGENT_GATEWAY_BIN``（支持带参数，如 "python -m ..."），其次
    ``agent-gateway`` 入口，最后回退到 ``python -m agent_gateway.app_gateway``。
    不用 ``jiuwenswarm-gateway``：拆分前的旧安装指向已不存在的
    ``jiuwenswarm_gateway`` 包，命中后会在启动阶段直接 ImportError。
    """
    override = os.environ.get("AGENT_GATEWAY_BIN", "").strip()
    if override:
        return shlex.split(override)
    found = shutil.which("agent-gateway")
    if found:
        return [found]
    if _importable("agent_gateway.app_gateway"):
        return [sys.executable, "-m", "agent_gateway.app_gateway"]
    raise RuntimeError("agent-gateway 未安装（需要 pip install -e . 后执行 ST）")


def start_process(cmd: list[str], env: dict[str, str], log_path: Path) -> subprocess.Popen:
    log_file = log_path.open("w", encoding="utf-8")
    try:
        return subprocess.Popen(
            cmd,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log_file.close()


def _log_tail(log_path: Path) -> str:
    return log_path.read_text(encoding="utf-8", errors="replace")[-3000:]


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def wait_tcp(port: int, proc: subprocess.Popen, log_path: Path, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"进程启动失败（exit={proc.returncode}）\n{_log_tail(log_path)}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise AssertionError(f"端口 {port} 在 {timeout:.0f}s 内未就绪\n{_log_tail(log_path)}")


def wait_ws_ready(port: int, proc: subprocess.Popen, log_path: Path, timeout: float) -> None:
    """以 /ws 握手就绪作为启动完成的判据。

    个人版只在 WebChannel 端口暴露 /ws，``/api/v1`` HTTP 路由仅企业版挂载，
    因此不能用 ``/api/v1/health`` 探活。首帧 ``connection.ack`` 由 AgentServer
    就绪后下发，所以该探测同时覆盖“上游监听”和“南向已连通”两件事。
    """
    from websockets.sync.client import connect
    from websockets.exceptions import WebSocketException

    deadline = time.monotonic() + timeout
    uri = f"ws://127.0.0.1:{port}{WS_PATH}"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"gateway 启动失败（exit={proc.returncode}）\n{_log_tail(log_path)}")
        try:
            with connect(uri, open_timeout=2, close_timeout=1) as ws:
                ws.recv(timeout=2)
                return
        except (OSError, TimeoutError, WebSocketException):
            pass
        time.sleep(0.2)
    raise AssertionError(f"gateway 在 {timeout:.0f}s 内未就绪: {uri}\n{_log_tail(log_path)}")


class GatewayProcess:
    """已启动的 gateway 进程句柄。"""

    def __init__(self, proc: subprocess.Popen, port: int, home: Path, log_path: Path) -> None:
        self.proc = proc
        self.port = port
        self.home = home
        self.log_path = log_path

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}{WS_PATH}"

    def stop(self) -> None:
        stop_process(self.proc)


def start_gateway(
    home: Path,
    agent_server_url: str,
    *,
    timeout: float = START_TIMEOUT,
) -> GatewayProcess:
    port = free_port()
    log_path = home / f"gateway-{port}.log"
    env = service_env(home)
    # ACP/TUI 侧固定端口，默认 19001；同一轮里多实例并行会互相抢占。
    env["GATEWAY_PORT"] = str(free_port())
    cmd = [
        *gateway_cmd(),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--agent-server-url",
        agent_server_url,
    ]
    proc = start_process(cmd, env, log_path)
    try:
        wait_ws_ready(port, proc, log_path, timeout)
    except Exception:
        stop_process(proc)
        raise
    return GatewayProcess(proc, port, home, log_path)
