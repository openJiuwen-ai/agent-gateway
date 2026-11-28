# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.router_client import AdxRouterClient
from pathlib import Path
from agent_gateway.extensions.agentos.agentos_router import extension
from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSRouterClient
from agent_gateway.extensions.loader import ExtensionLoader


def _client(monkeypatch):
    monkeypatch.setenv("ADX_TEST_PLATFORM", "platform-key")
    return AdxRouterClient(load_router_config({"gateway": {"adx": {
        "ingress_url": "https://adx.example", "tenant": "appliance",
        "token_env": "ADX_TEST_PLATFORM",
    }}}))


@pytest.mark.asyncio
async def test_extension_registers_only_when_adx_backend_is_selected(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router import extension

    settings = {"gateway": {"agent_client": {"type": "agentos_router"}}}
    monkeypatch.setenv("ADX_TEST_PLATFORM", "platform-key")
    settings["gateway"]["adx"] = {
        "ingress_url": "https://adx.example", "tenant": "appliance", "token_env": "ADX_TEST_PLATFORM",
    }
    monkeypatch.setattr(extension, "get_config", lambda: settings)
    registered = []
    third_agents = []
    registry = SimpleNamespace(register_agent_server_client=registered.append,register_third_agent=third_agents.append)
    extensions = await extension.register_extensions(registry)
    assert len(extensions) == 1 and extensions == registered == third_agents
    from pathlib import Path
    extensions[0].set_extension_dir(Path(extension.__file__).parents[1])
    assert extensions[0].metadata.id == "agentos_router"
    assert isinstance(extensions[0].get_client(), AdxRouterClient)
    await extensions[0].shutdown()




@pytest.mark.asyncio
async def test_extension_reuses_agentos_business_authenticator(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router import extension

    monkeypatch.setenv("ADX_TEST_PLATFORM", "platform-key")
    created = []

    class FakeAuthenticator:
        def __init__(self, url, timeout):
            created.append((url, timeout, self))

    monkeypatch.setattr(extension, "AgentOSAuthenticator", FakeAuthenticator)
    monkeypatch.setattr(extension, "get_config", lambda: {"gateway": {
        "agent_client": {"type": "agentos_router"},
        "agentos": {"auth_enabled": "true", "auth_service_url": "http://agentos-iam", "timeout": 7},
        "adx": {"ingress_url": "https://adx.example", "tenant": "appliance",
                "token_env": "ADX_TEST_PLATFORM"},
    }})
    registered = []
    extensions = await extension.register_extensions(SimpleNamespace(
        register_agent_server_client=registered.append,
        register_third_agent=lambda extension: None,
    ))
    try:
        assert created[:1] == [("http://agentos-iam", 7.0, created[0][2])]
        assert extensions[0].get_client()._auth_client is created[0][2]
    finally:
        await extensions[0].shutdown()


@pytest.mark.asyncio
async def test_loader_registers_one_canonical_router_for_adx_backend(monkeypatch):
    monkeypatch.setenv('ROUTER_BACKEND_TEST_TOKEN', 'test-only-token')
    settings = {'gateway': {'agent_client': {'type': 'agentos_router'}, 'adx': {
        'ingress_url': 'https://node.example', 'tenant': 'test', 'token_env': 'ROUTER_BACKEND_TEST_TOKEN',
    }}}
    monkeypatch.setattr(extension, 'get_config', lambda: settings)
    clients = []; third = []
    registry = SimpleNamespace(config={}, register_agent_server_client=clients.append,
                               register_third_agent=third.append)
    loader = ExtensionLoader(registry)
    package = Path(extension.__file__).parents[2]
    loader.add_search_path(package)
    roots = loader.discover_extension_roots()
    assert package / 'adx' not in roots
    loaded = await loader.load_extension(package / 'agentos')
    try:
        assert len(loaded) == 1 and loaded == clients == third
        assert loaded[0].metadata.id == 'agentos_router'
        assert isinstance(loaded[0].get_client(), AdxRouterClient)
    finally:
        await loaded[0].shutdown()


@pytest.mark.asyncio
async def test_router_requires_adx_settings_instead_of_selecting_a_retired_backend(monkeypatch):
    monkeypatch.setattr(extension, 'get_config', lambda: {'gateway': {'agent_client': {
        'type': 'agentos_router', 'frontend_endpoint': 'http://retired.example:22767',
        'function_version_urn': 'test-function:latest',
    }}})
    registered = []
    with pytest.raises(ValueError, match="ingress_url"):
        await extension.register_extensions(SimpleNamespace(
            register_agent_server_client=registered.append, register_third_agent=registered.append))
    assert registered == []


def test_managed_launch_delegates_node_selection_to_agentserver(monkeypatch):
    from agent_gateway.extensions.adx.config import managed_sandbox_settings

    monkeypatch.setenv('JIUWENSWARM_ADX_NODE_INGRESS_URL', 'https://gateway-node.example')
    settings = managed_sandbox_settings()
    assert 'url' not in settings
    assert settings['managed_connection']['url_env'] == 'JIUWENSWARM_ADX_NODE_INGRESS_URL'
    assert settings['managed_connection']['context_file_env'] == 'JIUWENSWARM_ADX_NODE_CONTEXT_FILE'


@pytest.mark.asyncio
async def test_selected_adx_cannot_fall_back_when_credentials_are_missing(monkeypatch):
    monkeypatch.delenv('MISSING_ROUTER_CREDENTIAL', raising=False)
    monkeypatch.setattr(extension, 'get_config', lambda: {'gateway': {
        'agent_client': {'type': 'agentos_router'},
        'adx': {'ingress_url': 'https://node.example', 'tenant': 'test',
                'token_env': 'MISSING_ROUTER_CREDENTIAL'},
    }})
    calls = []
    with pytest.raises(ValueError):
        await extension.register_extensions(SimpleNamespace(register_agent_server_client=calls.append))
    assert calls == []


@pytest.mark.asyncio
async def test_removed_router_type_does_not_register_an_extension(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router.config import agentos_router_selected

    settings = {'gateway': {'agent_client': {'type': 'adx_router'}, 'adx': {}}}
    assert not agentos_router_selected(settings)
    monkeypatch.setattr(extension, 'get_config', lambda: settings)
    registered = []
    registry = SimpleNamespace(register_agent_server_client=registered.append,
                               register_third_agent=registered.append)
    assert await extension.register_extensions(registry) == []
    assert registered == []
