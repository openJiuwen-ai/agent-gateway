"""Explicit (bundled) app-extension loading used by parent launchers.

``--app-extension`` 让随包扩展（如 Swarm 的应用更新扩展）不依赖
extension_dirs / extensions.enabled 装配：这里覆盖注册、initialize 生命周
期、失败仅记录不阻断启动，以及 AgentClientModeError 仍必须上抛。
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

from agent_gateway.agent_client_mode import AgentClientModeError
from agent_gateway.extensions.manager import ExtensionManager
from agent_gateway.extensions.sdk.application_plugin import ApplicationPluginExtension


def _make_manager(monkeypatch, registry):
    manager = ExtensionManager.__new__(ExtensionManager)
    manager._loaded_extensions = []
    manager.registry = registry
    return manager


def _make_registry(plugins):
    return SimpleNamespace(
        config=object(),
        register_application_plugin=lambda ext: plugins.append(ext),
        get_application_plugin=lambda plugin_id: None,
    )


class _FakePlugin(ApplicationPluginExtension):
    plugin_id = "explicit-test-plugin"

    def __init__(self):
        self.initialized_with = None

    async def initialize(self, config):
        self.initialized_with = config

    async def shutdown(self):
        return None


def _make_module(register_extensions):
    module = types.ModuleType("explicit_ext_test_module")
    module.register_extensions = register_extensions
    return module


@pytest.mark.asyncio
async def test_explicit_extension_registers_and_initializes(monkeypatch):
    plugins = []
    registry = _make_registry(plugins)

    async def register_extensions(reg):
        plugin = _FakePlugin()
        reg.register_application_plugin(plugin)
        return [plugin]

    monkeypatch.setitem(sys.modules, "explicit_ext_test_module", _make_module(register_extensions))
    manager = _make_manager(monkeypatch, registry)

    await manager.load_explicit_extensions(["explicit_ext_test_module"])

    assert len(plugins) == 1
    assert manager._loaded_extensions == [plugins[0]]
    assert plugins[0].initialized_with is registry.config


@pytest.mark.asyncio
async def test_explicit_extension_bypasses_enabled_allowlist(monkeypatch):
    # 显式注入不读取 extensions.enabled / extension_dirs：manager 不接触
    # get_config 与 loader 搜索路径，仅按模块路径装配。
    plugins = []
    registry = _make_registry(plugins)
    called = {"get_config": False}

    def _fail_get_config():
        called["get_config"] = True
        raise AssertionError("explicit loading must not read extension config")

    import agent_gateway.extensions.manager as manager_module

    monkeypatch.setattr(manager_module, "get_config", _fail_get_config)

    async def register_extensions(reg):
        reg.register_application_plugin(_FakePlugin())
        return []

    monkeypatch.setitem(sys.modules, "explicit_ext_test_module", _make_module(register_extensions))
    manager = _make_manager(monkeypatch, registry)

    await manager.load_explicit_extensions(["explicit_ext_test_module"])

    assert called["get_config"] is False
    assert manager._loaded_extensions == []


@pytest.mark.asyncio
async def test_explicit_extension_missing_module_does_not_block_startup():
    plugins = []
    registry = _make_registry(plugins)
    manager = _make_manager(None, registry)

    # 不存在的模块：记录明确原因后继续启动，绝不抛出。
    await manager.load_explicit_extensions(["explicit_ext_test_missing_module_xyz"])

    assert plugins == []
    assert manager._loaded_extensions == []


@pytest.mark.asyncio
async def test_explicit_extension_registration_error_does_not_block_startup(monkeypatch):
    plugins = []
    registry = _make_registry(plugins)

    async def register_extensions(reg):
        raise RuntimeError("bundled extension broken")

    monkeypatch.setitem(sys.modules, "explicit_ext_test_module", _make_module(register_extensions))
    manager = _make_manager(monkeypatch, registry)

    await manager.load_explicit_extensions(["explicit_ext_test_module"])

    assert plugins == []
    assert manager._loaded_extensions == []


@pytest.mark.asyncio
async def test_explicit_extension_client_conflict_still_raises(monkeypatch):
    registry = _make_registry([])

    async def register_extensions(reg):
        raise AgentClientModeError("duplicate client")

    monkeypatch.setitem(sys.modules, "explicit_ext_test_module", _make_module(register_extensions))
    manager = _make_manager(monkeypatch, registry)

    with pytest.raises(AgentClientModeError, match="duplicate client"):
        await manager.load_explicit_extensions(["explicit_ext_test_module"])
