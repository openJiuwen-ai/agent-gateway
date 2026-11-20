# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Runtime 客户端的 ready 语义。

``server_ready`` 表示传输可用，``agent_ready`` 表示 Agent 真能执行。两者混用
会让前端在 Pod 重启/预热时被告知"可聊"，首条 chat 才失败。这里钉住区别。
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agent_gateway.extensions.runtime_management_extension import runtime_routed_client as routed
from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import (
    RuntimeRoutedAgentClient,
)


def _client() -> RuntimeRoutedAgentClient:
    client = RuntimeRoutedAgentClient(route_client=AsyncMock(), http_client=AsyncMock())
    return client


def test_not_connected_means_neither_ready():
    client = _client()
    assert client.server_ready is False
    assert client.agent_ready is False


def test_connected_without_pod_is_server_ready_but_not_agent_ready():
    """connect() 成功只代表能发 route，一个 Pod 都没连上就不算可执行。"""
    client = _client()
    client._connected = True

    assert client.server_ready is True
    assert client.agent_ready is False


def test_agent_ready_when_a_pod_push_channel_exists():
    client = _client()
    client._connected = True
    client._pod_clients["http://pod1"] = AsyncMock()

    assert client.agent_ready is True


def test_agent_ready_returns_false_after_disconnect():
    client = _client()
    client._connected = True
    client._pod_clients["http://pod1"] = AsyncMock()
    assert client.agent_ready is True

    client._connected = False
    assert client.agent_ready is False


def test_connection_status_prefers_agent_ready():
    """北向 connection.status 必须读 agent_ready，不能用 server_ready 冒充。"""
    import inspect

    from agent_gateway.channel_manager.web import app_web_handlers

    source = inspect.getsource(app_web_handlers)
    assert '"agent_ready"' in source
    # 不能出现「agent_ready 字段直接取 server_ready」的旧写法
    assert 'bool(getattr(ac, "server_ready", False))' not in source


@pytest.mark.parametrize(
    ("pod_clients", "expected"),
    [({}, False), ({"http://pod": object()}, True)],
)
def test_agent_ready_matrix(pod_clients, expected):
    client = _client()
    client._connected = True
    client._pod_clients.update(pod_clients)
    assert client.agent_ready is expected
