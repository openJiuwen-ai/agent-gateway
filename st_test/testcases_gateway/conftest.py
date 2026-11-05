# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""agent-gateway 独立契约 ST：不依赖 jiuwenswarm / AgentServer / 沙箱 / 元戎。

启动方式：``agent-gateway --agent-server-url <stub>``，南向由
``StubAgentServer`` 提供固定应答；用例只验证 gateway 自身的传输与转发契约。

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
    instance = start_gateway(home, stub_agent_server.url)
    yield instance
    instance.stop()


@pytest.fixture
def gateway_factory(tmp_path):
    """按需启动额外的 gateway（如南向不可用场景）。"""

    created: list[GatewayProcess] = []

    def _make(agent_server_url: str) -> GatewayProcess:
        home = tmp_path / f"gateway-{len(created)}"
        home.mkdir()
        instance = start_gateway(home, agent_server_url)
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
