"""Live AgentServer network policy validation and updates."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.control_client import AdxControlClient
from tests.unit_tests.extensions.test_adx_router import _config


@pytest.mark.parametrize('network', [{}, {'ingress_url': 'invalid'}])
def test_builtin_network_requires_ingress_url(monkeypatch, network):
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    with pytest.raises(ValueError, match=r'builtin_launch\.network\.ingress_url'):
        load_router_config({'gateway': {'adx': {
            'ingress_url': 'https://adx.example', 'tenant': 'tenant', 'token_env': 'TEST_ADX_KEY',
            'builtin_launch': {'process': {'commands': [['true']]}, 'network': network},
        }}})


@pytest.mark.asyncio
async def test_model_policy_refresh_serializes_changes_and_does_not_reuse_old_operation(monkeypatch):
    import asyncio
    import httpx
    from tests.unit_tests.extensions.test_adx_router import _config
    from agent_gateway.extensions.adx.control_client import AdxControlClient
    origins = ['https://one.example/v1']
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: list(origins))
    requests = []
    def handle(request):
        assert request.headers['x-adx-request-id'] == request.headers['x-request-id']
        assert request.headers['x-adx-request-id'].startswith('network-')
        requests.append((request.headers['x-request-id'], json.loads(request.content)))
        return httpx.Response(200, json={'code': 200, 'data': None})
    launch = {'process': {'commands': [['true']]}, 'network': {'ingress_url': 'http://10.88.4.1:8080'}}
    client = AdxControlClient(_config(monkeypatch, builtin_launch=launch), transport=httpx.MockTransport(handle))
    try:
        await asyncio.gather(client.ensure_builtin_network('adx-alice', '/alice'), client.ensure_builtin_network('adx-alice', '/alice'))
        origins[:] = ['https://two.example/v1']
        await client.ensure_builtin_network('adx-alice', '/alice')
        origins[:] = ['https://one.example/v1']
        await client.ensure_builtin_network('adx-alice', '/alice')
        assert len(requests) == 3
        assert len({operation for operation, _ in requests}) == 3
        assert requests[0][1] == requests[2][1]
    finally:
        await client.close()

@pytest.mark.asyncio
async def test_applied_network_cache_reclaims_missing_and_least_recently_used_environments(monkeypatch):
    from agent_gateway.extensions.adx import control_client

    monkeypatch.setattr(control_client, '_MAX_APPLIED_NETWORK_POLICIES', 2)
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: [])
    applied = []

    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        applied.append(request.url.path.split('/')[-2])
        return httpx.Response(200, json={})

    launch = {'process': {'commands': [['true']]}, 'network': {'ingress_url': 'http://10.88.4.1:8080'}}
    client = AdxControlClient(_config(monkeypatch, builtin_launch=launch), transport=httpx.MockTransport(handle))
    try:
        for instance in ('adx-alice', 'adx-bob', 'adx-alice', 'adx-carol', 'adx-bob'):
            await client.ensure_builtin_network(instance, '/workspace')
        assert applied == ['adx-alice', 'adx-bob', 'adx-carol', 'adx-bob']
        assert list(client._network_applied) == ['adx-carol', 'adx-bob']
        with pytest.raises(control_client.AdxEnvironmentNotFoundError):
            await client.get_agent_info('adx-bob')
        assert list(client._network_applied) == ['adx-carol']
    finally:
        await client.close()

@pytest.mark.asyncio
async def test_unknown_policy_replacement_is_reconciled_before_a_different_model(monkeypatch):
    import httpx
    from tests.unit_tests.extensions.test_adx_router import _config
    from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError
    origins = ['https://one.example/v1']
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: list(origins))
    requests = []
    def handle(request):
        assert request.headers['x-adx-request-id'] == request.headers['x-request-id']
        assert request.headers['x-adx-request-id'].startswith('network-')
        requests.append((request.headers['x-request-id'], json.loads(request.content)))
        if len(requests) == 1:
            raise httpx.ReadTimeout('unknown outcome', request=request)
        return httpx.Response(200, json={'code': 200, 'data': None})
    launch = {'process': {'commands': [['true']]}, 'network': {'ingress_url': 'http://10.88.4.1:8080'}}
    client = AdxControlClient(_config(monkeypatch, builtin_launch=launch), transport=httpx.MockTransport(handle))
    try:
        with pytest.raises(AdxControlError):
            await client.ensure_builtin_network('adx-alice', '/alice')
        origins[:] = ['https://two.example/v1']
        await client.ensure_builtin_network('adx-alice', '/alice')
        assert len(requests) == 3
        assert requests[0] == requests[1]
        assert requests[2][0] != requests[1][0]
        assert requests[2][1] != requests[1][1]
    finally:
        await client.close()

@pytest.mark.asyncio
async def test_policy_refresh_failure_releases_the_business_activity_hold(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from tests.unit_tests.extensions.test_adx_router import _config, _router
    from agent_gateway.extensions.adx.control_client import AdxControlError
    from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSRouterClient
    from agent_gateway.extensions.agentos.agentos_router.agent_manager import AgentCreateFailed
    runtime = SimpleNamespace(info=SimpleNamespace(agent_type='jiuwenswarm', user_id='alice', sandbox_id='adx-alice', agent_id='generation-1'), key=('alice', 'jiuwenswarm'))
    monkeypatch.setattr(AgentOSRouterClient, '_resolve_agent', AsyncMock(return_value=runtime))
    launch = {'process': {'commands': [['true']]}, 'network': {'ingress_url': 'http://10.88.4.1:8080'}}
    router, _ = _router(_config(monkeypatch, builtin_launch=launch))
    monkeypatch.setattr(router._control, 'ensure_builtin_network', AsyncMock(side_effect=AdxControlError('policy rejected')))
    release = AsyncMock()
    monkeypatch.setattr(router._agent_manager, 'release', release)
    with pytest.raises(AgentCreateFailed, match='policy rejected'):
        await router._resolve_agent(None, acquire=True)
    release.assert_awaited_once_with(runtime.key, expected_agent_id=runtime.info.agent_id)

@pytest.mark.asyncio
async def test_delete_waits_for_policy_update_and_clears_generation_cache(monkeypatch):
    import asyncio
    import httpx
    from tests.unit_tests.extensions.test_adx_router import _config
    from agent_gateway.extensions.adx.control_client import AdxControlClient
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: [])
    updating = asyncio.Event()
    finish_update = asyncio.Event()
    requests = []

    async def handle(request):
        requests.append(request.method)
        if request.method == 'PUT' and requests.count('PUT') == 1:
            updating.set()
            await finish_update.wait()
        return httpx.Response(200, json={'code': 200, 'data': None})

    launch = {'process': {'commands': [['true']]}, 'network': {'ingress_url': 'http://10.88.4.1:8080'}}
    client = AdxControlClient(_config(monkeypatch, builtin_launch=launch), transport=httpx.MockTransport(handle))
    update = asyncio.create_task(client.ensure_builtin_network('adx-alice', '/alice'))
    deletion = None
    try:
        await asyncio.wait_for(updating.wait(), 2)
        deletion = asyncio.create_task(client.delete_sandbox('adx-alice'))
        await asyncio.sleep(0)
        assert requests == ['PUT'], 'Deletion must follow the in-flight policy replacement'
        finish_update.set()
        await asyncio.gather(update, deletion)
        assert 'adx-alice' not in client._environment_locks
        await client.ensure_builtin_network('adx-alice', '/alice')
        assert requests == ['PUT', 'DELETE', 'PUT']
    finally:
        finish_update.set()
        await asyncio.gather(update, *([deletion] if deletion else []), return_exceptions=True)
        await client.close()
