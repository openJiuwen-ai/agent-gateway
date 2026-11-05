# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""与真实 jiuwenswarm 结合的兼容 ST（黑盒）。

启动两个真实进程：

- ``jiuwenswarm-agentserver``（jiuwenswarm develop）
- ``agent-gateway``（本仓），``--agent-server-url`` 指向上面这个 AgentServer

用例只通过 gateway 对外接口（``/ws`` 最小帧）验证功能，不引用 jiuwenswarm
内部模块，也不 mock 南向。

运行（仓库根目录，环境需同时安装本仓与 workswarm）：

    python -m pytest st_test/testcases_integration --no-cov

可选环境变量：

- ``ST_WORKSWARM_HOME``：复用已初始化的工作区；默认每轮建临时目录。
- ``ST_WORKSWARM_CHAT=1``：启用真实模型对话用例；临时工作区会从
  ``ST_WORKSWARM_CONFIG_SRC``（默认 ``~/.jiuwenswarm/config``）复制模型配置。
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

from testcases_support.process import (
    GatewayProcess,
    free_port,
    service_env,
    start_gateway,
    start_process,
    stop_process,
    wait_tcp,
)

_AS_BIN = "jiuwenswarm-agentserver"
_AS_READY_TIMEOUT = 120.0
_GATEWAY_READY_TIMEOUT = 180.0


def chat_enabled() -> bool:
    return os.environ.get("ST_WORKSWARM_CHAT", "").strip().lower() in {"1", "true", "yes"}


@dataclass
class WorkswarmService:
    """真实 AgentServer + gateway 进程组。"""

    home: Path
    agent_server_url: str
    agentserver_proc: object
    gateway: GatewayProcess


def _seed_model_config(home: Path) -> None:
    default_src = Path.home() / ".jiuwenswarm" / "config"
    src = Path(os.environ.get("ST_WORKSWARM_CONFIG_SRC", "") or default_src).expanduser()
    if not src.is_dir():
        return
    dst = home / ".jiuwenswarm" / "config"
    dst.mkdir(parents=True, exist_ok=True)
    for name in ("config.yaml", ".env"):
        if (src / name).exists():
            shutil.copy2(src / name, dst / name)


@pytest.fixture(scope="session")
def workswarm_home(tmp_path_factory) -> Path:
    override = os.environ.get("ST_WORKSWARM_HOME", "").strip()
    if override:
        home = Path(override).expanduser()
        home.mkdir(parents=True, exist_ok=True)
        return home
    home = tmp_path_factory.mktemp("workswarm-st")
    if chat_enabled():
        _seed_model_config(home)
    return home


@pytest.fixture(scope="session")
def workswarm_service(workswarm_home) -> WorkswarmService:
    as_bin = shutil.which(_AS_BIN)
    if not as_bin:
        pytest.skip(f"{_AS_BIN} 未安装（需要先安装 workswarm develop）")

    as_port = free_port()
    as_log = workswarm_home / "agentserver.log"
    env = service_env(workswarm_home)
    as_proc = start_process([as_bin, "--port", str(as_port)], env, as_log)
    url = f"ws://127.0.0.1:{as_port}"
    try:
        wait_tcp(as_port, as_proc, as_log, _AS_READY_TIMEOUT)
        gateway = start_gateway(workswarm_home, url, timeout=_GATEWAY_READY_TIMEOUT)
    except Exception:
        stop_process(as_proc)
        raise
    service = WorkswarmService(
        home=workswarm_home,
        agent_server_url=url,
        agentserver_proc=as_proc,
        gateway=gateway,
    )
    try:
        yield service
    finally:
        gateway.stop()
        stop_process(as_proc)


@pytest.fixture
def workswarm_ws(workswarm_service):
    from testcases_gateway.gateway_client import GatewayWsClient

    client = GatewayWsClient(workswarm_service.gateway.ws_url)
    yield client
    client.close()
