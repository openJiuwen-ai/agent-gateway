# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Southbound client assembly errors must survive optional extension loading."""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.agent_client_mode import (
    ROUTER_CLIENT_TYPES,
    AgentClientModeError,
    is_runtime_agent_client,
    resolve_agent_server_client,
    validate_agent_client_configuration,
)
from agent_gateway.extensions.manager import ExtensionManager
from agent_gateway.extensions.registry import ExtensionRegistry


@pytest.mark.parametrize("selected", ["agentos_router", "adx_router"])
def test_runtime_conflicting_configuration_fails(selected):
    with pytest.raises(AgentClientModeError, match=f"type={selected}"):
        validate_agent_client_configuration(
            {"gateway": {"agent_client": {"type": selected}}}, mode="runtime",
        )


@pytest.mark.parametrize("selected", ["agentos_router", "adx_router"])
def test_router_client_types_cover_both_router_extensions(selected):
    """冲突校验与 SSH 渠道准入共用 ROUTER_CLIENT_TYPES，避免两处口径漂移。"""
    assert selected in ROUTER_CLIENT_TYPES


@pytest.mark.parametrize("configured", ["AGENTOS_ROUTER", " adx_router "])
def test_runtime_conflict_check_normalizes_configured_type(configured):
    with pytest.raises(AgentClientModeError):
        validate_agent_client_configuration(
            {"gateway": {"agent_client": {"type": configured}}}, mode="runtime",
        )


@pytest.mark.parametrize("mode", ["", "websocket", "unknown"])
@pytest.mark.parametrize("selected", ["agentos_router", "adx_router", "websocket"])
def test_existing_router_configuration_is_unchanged(mode, selected):
    validate_agent_client_configuration(
        {"gateway": {"agent_client": {"type": selected}}}, mode=mode,
    )
    client = object()
    assert resolve_agent_server_client(
        SimpleNamespace(get_client=lambda: client), mode=mode,
        websocket_factory=lambda: pytest.fail("registered extension must win"),
    ) is client


def test_a_class_name_is_not_a_runtime_capability():
    client_type = type("RuntimeRoutedAgentClient", (), {})
    assert not is_runtime_agent_client(client_type())
    with pytest.raises(AgentClientModeError):
        resolve_agent_server_client(
            SimpleNamespace(get_client=client_type), mode="runtime",
            websocket_factory=lambda: pytest.fail("must not fall back"),
        )


@pytest.mark.parametrize("first,second", [
    ("runtime", "agentos_router"), ("adx_router", "runtime"),
    ("agentos_router", "adx_router"), ("runtime", "runtime"),
])
def test_registry_rejects_second_registration_without_overwriting(first, second):
    registry = ExtensionRegistry(callback_framework=Mock(), config={}, logger=Mock())
    original = SimpleNamespace(metadata=SimpleNamespace(id=first))
    registry.register_agent_server_client(original)
    with pytest.raises(AgentClientModeError, match="already registered"):
        registry.register_agent_server_client(SimpleNamespace(metadata=SimpleNamespace(id=second)))
    assert registry.get_agent_server_client_extension() is original


def test_registry_accepts_same_instance_registration_twice():
    """同一实例重复注册保持幂等（多扩展根/复用 registry 的进程）。"""
    registry = ExtensionRegistry(callback_framework=Mock(), config={}, logger=Mock())
    original = SimpleNamespace(metadata=SimpleNamespace(id="runtime"))
    registry.register_agent_server_client(original)
    registry.register_agent_server_client(original)
    assert registry.get_agent_server_client_extension() is original


@pytest.mark.asyncio
async def test_manager_does_not_swallow_client_assembly_error(monkeypatch):
    monkeypatch.setattr("agent_gateway.extensions.manager.get_config", lambda: {})
    manager = ExtensionManager.__new__(ExtensionManager)
    manager._loaded_extensions = []
    manager.loader = SimpleNamespace(
        discover_extension_roots=lambda: [Path("runtime")],
        load_manifest=lambda path: {},
        load_extension=AsyncMock(side_effect=AgentClientModeError("duplicate client")),
    )
    with pytest.raises(AgentClientModeError, match="duplicate client"):
        await manager.load_all_extensions()


@pytest.mark.asyncio
async def test_optional_extension_failure_retains_legacy_behavior(monkeypatch):
    monkeypatch.setattr("agent_gateway.extensions.manager.get_config", lambda: {})
    manager = ExtensionManager.__new__(ExtensionManager)
    manager._loaded_extensions = []
    manager.loader = SimpleNamespace(
        discover_extension_roots=lambda: [Path("optional")],
        load_manifest=lambda path: {},
        load_extension=AsyncMock(side_effect=ImportError("optional missing")),
    )
    await manager.load_all_extensions()
    assert manager._loaded_extensions == []


@pytest.mark.asyncio
async def test_selected_runtime_import_failure_is_fatal(monkeypatch):
    monkeypatch.setenv("GATEWAY_AGENT_CLIENT_MODE", "runtime")
    monkeypatch.setattr("agent_gateway.extensions.manager.get_config", lambda: {})
    manager = ExtensionManager.__new__(ExtensionManager)
    manager._loaded_extensions = []
    error = ImportError("runtime dependency missing")
    manager.loader = SimpleNamespace(
        discover_extension_roots=lambda: [Path("runtime_management_extension")],
        load_manifest=lambda path: {"id": "openjiuwen_runtime_management"},
        load_extension=AsyncMock(side_effect=error),
    )
    with pytest.raises(AgentClientModeError, match="runtime extension failed") as exc:
        await manager.load_all_extensions()
    assert exc.value.__cause__ is error


@pytest.mark.asyncio
async def test_app_runtime_load_failure_never_constructs_websocket(monkeypatch):
    from agent_gateway import app_gateway
    from agent_gateway.routing import agent_client

    monkeypatch.setenv("GATEWAY_AGENT_CLIENT_MODE", "runtime")
    monkeypatch.setattr("agent_gateway.enterprise.is_enterprise", lambda: False)
    monkeypatch.setattr("agent_gateway.common.config.get_config", lambda: {})
    registry = SimpleNamespace(get_agent_server_client_extension=lambda: None)
    monkeypatch.setattr(ExtensionRegistry, "create_instance", lambda **kwargs: registry)
    monkeypatch.setattr(ExtensionManager, "_setup_search_paths", lambda self: None)
    monkeypatch.setattr(ExtensionManager, "load_all_extensions", AsyncMock())
    monkeypatch.setattr(ExtensionManager, "list_extensions", lambda self: [])
    monkeypatch.setattr(app_gateway, "configure_gateway_telemetry", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        agent_client, "WebSocketAgentServerClient",
        lambda **kwargs: pytest.fail("explicit runtime must not construct WebSocket"),
    )
    with pytest.raises(AgentClientModeError, match="未加载"):
        await app_gateway._run("ws://unused", "127.0.0.1", 0, "/ws", asyncio.Event())
