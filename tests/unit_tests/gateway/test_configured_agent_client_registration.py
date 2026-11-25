"""Gateway startup validates configuration without depending on a backend class."""
import asyncio
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.agent_client_mode import _validate_agent_client_registration


def extension(kind, client=object()):
    return SimpleNamespace(metadata=SimpleNamespace(id=kind), get_client=lambda: client)


@pytest.mark.parametrize('config', [None, {}, {'gateway': {'agent_client': {'type': 'websocket'}}}])
def test_default_websocket_keeps_implicit_registration(config):
    _validate_agent_client_registration(config, None)


@pytest.mark.parametrize('kind', ['agentos_router', 'custom_extension'])
def test_configured_extension_uses_the_registered_interface(kind):
    _validate_agent_client_registration({'gateway': {'agent_client': {'type': kind}}}, extension(kind))


@pytest.mark.parametrize('registered', [None, extension('other'), extension('agentos_router', None)])
def test_missing_or_wrong_configured_extension_stops_startup(registered):
    with pytest.raises(RuntimeError, match='not registered'):
        _validate_agent_client_registration({'gateway': {'agent_client': {'type': 'agentos_router'}}}, registered)


def test_retained_service_requirement_cannot_select_a_different_client():
    with pytest.raises(RuntimeError, match='not registered'):
        _validate_agent_client_registration({}, extension('agentos_router'), required_type='agentos_router')


@pytest.mark.parametrize("kind", ["agentos_router", "adx_router"])
def test_retained_service_argument_accepts_either_registered_router(kind):
    _validate_agent_client_registration(
        {"gateway": {"agent_client": {"type": kind}}}, extension(kind),
        required_type=("agentos_router", "adx_router"),
    )


def test_registered_runtime_override_keeps_an_explicit_router_requirement():
    config = {"gateway": {"agent_client": {"type": "agentos_router"}}}
    runtime = extension("openjiuwen_runtime_management")
    _validate_agent_client_registration(config, runtime)
    with pytest.raises(RuntimeError, match="not registered"):
        _validate_agent_client_registration(config, runtime, required_type="agentos_router")


@pytest.mark.asyncio
async def test_standalone_startup_rejects_websocket_fallback_when_router_is_required(monkeypatch):
    from agent_gateway import app_gateway, enterprise
    from agent_gateway.common import config
    from agent_gateway.extensions import manager, registry
    from agent_gateway.routing import agent_client

    monkeypatch.delenv("GATEWAY_AGENT_CLIENT_MODE", raising=False)
    monkeypatch.setattr(config, "get_config", lambda: {})
    monkeypatch.setattr(enterprise, "is_enterprise", lambda: False)
    extensions = SimpleNamespace(get_agent_server_client_extension=lambda: None)
    monkeypatch.setattr(registry.ExtensionRegistry, "create_instance", lambda **kwargs: extensions)
    extension_manager = SimpleNamespace(load_all_extensions=AsyncMock(), list_extensions=lambda: [])
    monkeypatch.setattr(manager, "ExtensionManager", lambda **kwargs: extension_manager)
    monkeypatch.setattr(
        agent_client, "WebSocketAgentServerClient",
        lambda **kwargs: pytest.fail("router-required startup must not construct WebSocket"),
    )

    with pytest.raises(RuntimeError, match=r"not registered \(selected: websocket"):
        await app_gateway._run(
            "ws://unused", "127.0.0.1", 0, "/ws", asyncio.Event(),
            required_client_type=("agentos_router", "adx_router"),
        )


def test_previous_service_argument_is_hidden_from_help():
    result = subprocess.run([sys.executable, '-m', 'agent_gateway.app_gateway',
                             '--require-adx-router', '--help'], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert '--require-adx-router' not in result.stdout
