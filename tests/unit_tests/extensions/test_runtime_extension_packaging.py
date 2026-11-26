# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业 Runtime 扩展的部署契约：routeless 目标、传输标记、打包元数据。

这三个都是「配错了才发现」的问题：独立 Pod 部署时回环地址连不上、
Agent worker 误加载南向客户端扩展、扩展包声明了用不到的依赖。
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest
from unittest.mock import AsyncMock
from e2a.models import E2AEnvelope
from agent_gateway.enterprise.team.route import TeamRouteResolver
from agent_gateway.enterprise.team.route_binding_store import InMemoryTeamRouteBindingStore

_EXT_DIR = (
    Path(__file__).resolve().parents[3]
    / "agent_gateway"
    / "extensions"
    / "runtime_management_extension"
)


@pytest.fixture()
def clean_env(monkeypatch):
    for name in (
        "GATEWAY_RUNTIME_DEFAULT_HTTP_BASE",
        "AGENT_SERVER_HOST",
        "AGENT_HTTP_PORT",
        "GATEWAY_RUNTIME_COLOCATED_AGENT",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _reload_routed():
    import agent_gateway.extensions.runtime_management_extension.runtime_routed_client as module

    return importlib.reload(module)


# --------------------------------------------------------------------------- #
# 11：routeless 目标解析
# --------------------------------------------------------------------------- #


def test_no_target_configured_returns_none(clean_env):
    """独立 Pod 部署且未配置目标时，不返回回环地址。"""
    module = _reload_routed()
    assert module._default_http_base() is None


def test_routeless_without_target_fails_fast(clean_env):
    """缺目标时抛明确的配置错误，而不是等 HTTP 连接超时。"""
    module = _reload_routed()
    with pytest.raises(module.RoutelessTargetMissing) as caught:
        module._routeless_base_or_raise("agent.reload_config")
    assert caught.value.code == "SERVICE_UNAVAILABLE"
    assert "GATEWAY_RUNTIME_DEFAULT_HTTP_BASE" in str(caught.value)


def test_explicit_base_wins_and_is_normalised(clean_env):
    clean_env.setenv("GATEWAY_RUNTIME_DEFAULT_HTTP_BASE", "http://agent:8766/")
    module = _reload_routed()
    assert module._default_http_base() == "http://agent:8766"


def test_explicit_host_or_port_builds_target(clean_env):
    clean_env.setenv("AGENT_SERVER_HOST", "agent-host")
    module = _reload_routed()
    assert module._default_http_base() == "http://agent-host:8766"

    clean_env.setenv("AGENT_HTTP_PORT", "9000")
    module = _reload_routed()
    assert module._default_http_base() == "http://agent-host:9000"


def test_colocated_flag_allows_loopback(clean_env):
    """同机部署需显式确认，而不是默认假设。"""
    clean_env.setenv("GATEWAY_RUNTIME_COLOCATED_AGENT", "1")
    module = _reload_routed()
    assert module._default_http_base() == "http://127.0.0.1:8766"


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_missing_target_error_survives_request_dispatch(clean_env, stream):
    module = _reload_routed()
    clean_env.setenv("JIUWENSWARM_EDITION", "enterprise")
    http = AsyncMock()
    client = module.RuntimeRoutedAgentClient(
        route_client=AsyncMock(),
        http_client=http,
        team_route_resolver=TeamRouteResolver(store=InMemoryTeamRouteBindingStore()),
    )
    client._connected = True
    envelope = E2AEnvelope(method="initialize", request_id="r", channel="web")
    with pytest.raises(module.RoutelessTargetMissing) as caught:
        if stream:
            async for _ in client.send_request_stream(envelope):
                pytest.fail("no stream events without a target")
        else:
            await client.send_request(envelope)
    assert caught.value.code == "SERVICE_UNAVAILABLE"
    http.send_request.assert_not_called()
    http.send_request_stream.assert_not_called()


@pytest.mark.asyncio
async def test_configured_target_keeps_http_failure_response(clean_env):
    module = _reload_routed()
    clean_env.setenv("JIUWENSWARM_EDITION", "enterprise")
    clean_env.setenv("GATEWAY_RUNTIME_DEFAULT_HTTP_BASE", "http://agent:8766")
    http = AsyncMock()
    http.send_request.side_effect = RuntimeError("connection failed")
    client = module.RuntimeRoutedAgentClient(
        route_client=AsyncMock(),
        http_client=http,
        team_route_resolver=TeamRouteResolver(store=InMemoryTeamRouteBindingStore()),
    )
    client._connected = True
    response = await client.send_request(E2AEnvelope(method="initialize", request_id="r"))
    assert response.ok is False
    assert response.payload["code"] == "ROUTELESS_HTTP_ERROR"


# --------------------------------------------------------------------------- #
# 17：传输扩展标记
# --------------------------------------------------------------------------- #


def test_extension_is_marked_as_transport_extension():
    """必须与 agentos/adx 一致标 requires_transport，Agent worker 才会跳过它。"""
    yaml = pytest.importorskip("yaml")
    manifest = yaml.safe_load((_EXT_DIR / "extension.yaml").read_text(encoding="utf-8"))
    assert manifest.get("requires_transport") is True, (
        "runtime_management_extension 是南向客户端扩展，未标 requires_transport 时"
        "Agent worker（include_transport_extensions=false）会误加载它并抢注 client 槽位"
    )


# --------------------------------------------------------------------------- #
# 16：打包元数据
# --------------------------------------------------------------------------- #


def test_packaging_declares_no_missing_module():
    """py-modules 不得指向仓内不存在的文件（原 _ee_deps_* 占位缺失）。"""
    import tomllib

    data = tomllib.loads((_EXT_DIR / "pyproject.toml").read_text(encoding="utf-8"))
    modules = data.get("tool", {}).get("setuptools", {}).get("py-modules", [])
    for name in modules:
        assert (_EXT_DIR / f"{name}.py").exists(), (
            f"py-modules 声明了 {name}，但目录里没有 {name}.py，构建期会告警"
        )


def test_no_unused_runtime_sdk_dependency():
    """扩展源码不 import agent-runtime 管理 SDK，就不该声明这个依赖。"""
    import tomllib

    data = tomllib.loads((_EXT_DIR / "pyproject.toml").read_text(encoding="utf-8"))
    deps = data.get("project", {}).get("dependencies", [])
    assert not any("openjiuwen_runtime_management" in dep for dep in deps), (
        "本扩展不 import openjiuwen_runtime_management，声明该 git 依赖会让"
        "打包/安装引入无用依赖"
    )


def test_extension_sources_do_not_import_runtime_sdk():
    """守住上一条测试的前提：源码里确实没有该 import。"""
    for path in _EXT_DIR.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "import openjiuwen_runtime_management" not in text, path
