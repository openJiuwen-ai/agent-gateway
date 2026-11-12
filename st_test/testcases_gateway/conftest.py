# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway contract ST with a stub AgentServer.

Standalone tests boot with Gateway's bundled config. The host upgrade test
also runs when JiuwenSwarm is installed.

运行（仓库根目录）：

    python -m pytest st_test/testcases_gateway --no-cov
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from testcases_gateway.stub_agent_server import StubAgentServer
from testcases_support.process import GatewayProcess, start_gateway


@pytest.fixture(scope="session")
def stub_agent_server() -> StubAgentServer:
    stub = StubAgentServer().start()
    yield stub
    stub.stop()


@pytest.fixture(scope="session")
def gateway(tmp_path_factory, stub_agent_server) -> GatewayProcess:
    home = tmp_path_factory.mktemp("gateway-st")
    instance = start_gateway(home, stub_agent_server.url, use_host_config=False)
    yield instance
    instance.stop()


@pytest.fixture
def gateway_factory(tmp_path):
    """按需启动额外的 gateway（如南向不可用场景）。"""

    created: list[GatewayProcess] = []

    def _make(agent_server_url: str) -> GatewayProcess:
        home = tmp_path / f"gateway-{len(created)}"
        home.mkdir()
        instance = start_gateway(home, agent_server_url, use_host_config=False)
        created.append(instance)
        return instance

    yield _make
    for instance in created:
        instance.stop()


@pytest.fixture
def gateway_ws(gateway) -> SimpleNamespace:
    from testcases_gateway.gateway_client import GatewayWsClient

    client = GatewayWsClient(gateway.ws_url)
    yield client
    client.close()
