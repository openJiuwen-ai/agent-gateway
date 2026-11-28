"""Runtime network replacements, activity holds and eviction."""
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_gateway.extensions.adx import control_client
from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError
from agent_gateway.extensions.agentos.agentos_router.agent_manager import AgentCreateFailed
from tests.unit_tests.extensions.test_adx_router import _config, _request, _router


@pytest.fixture(autouse=True)
def workspace_without_models(monkeypatch):
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: [])


def network_launch(**network):
    return {'process': {'commands': [['true']]}, 'network': network}


@pytest.mark.parametrize('network', [{}, {'ingress_url': 'invalid'}])
def test_builtin_network_requires_ingress_url(monkeypatch, network):
    with pytest.raises(ValueError, match=r'builtin_launch\.network\.ingress_url'):
        _config(monkeypatch, builtin_launch=network_launch(**network))


@asynccontextmanager
async def live_control(monkeypatch, handle):
    config = _config(monkeypatch, builtin_launch=network_launch(ingress_url='http://node-ingress.test:8080'))
    client = AdxControlClient(config, transport=httpx.MockTransport(handle))
    try:
        yield client
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_model_policy_refresh_reconciles_and_deduplicates(monkeypatch):
    origins = ['https://one.example/v1']
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: origins)
    requests = []

    def handle(request):
        operation = request.headers['x-request-id']
        assert operation.startswith('network-') and request.headers['x-adx-request-id'] == operation
        requests.append((operation, request.content))
        if len(requests) == 2:
            raise httpx.ReadTimeout('unknown outcome', request=request)
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        async def update():
            await client.ensure_builtin_network('adx-alice', '/alice')

        await asyncio.gather(update(), update())
        assert len(requests) == 1
        origins[:] = ['https://two.example/v1']
        with pytest.raises(AdxControlError):
            await update()
        origins[:] = ['https://one.example/v1']
        await update()
        initial, timed_out, replayed, latest = requests
        assert timed_out == replayed
        assert initial[1] == latest[1] != replayed[1]
        assert len({initial[0], timed_out[0], latest[0]}) == 3


@pytest.mark.asyncio
async def test_eviction_on_cache_limit_and_missing_environment(monkeypatch):
    monkeypatch.setattr(control_client, '_MAX_APPLIED_NETWORK_POLICIES', 2)
    applied = []

    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        applied.append(request.url.path.split('/')[-2])
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        for instance in ('alice', 'bob', 'alice', 'carol', 'bob'):
            await client.ensure_builtin_network(instance, '/workspace')
        assert applied == ['alice', 'bob', 'carol', 'bob']
        assert list(client._network_applied) == ['carol', 'bob']
        with pytest.raises(control_client.AdxEnvironmentNotFoundError):
            await client.get_agent_info('bob')
        assert list(client._network_applied) == ['carol']


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['backend', 'release', 'cancel'])
async def test_policy_failure_releases_business_activity(monkeypatch, failure):
    launch = network_launch(ingress_url='http://node-ingress.test:8080')
    router, _ = _router(_config(monkeypatch, builtin_launch=launch))
    updating, releasing, finish, released = (asyncio.Event() for _ in range(4))

    async def update(*_args):
        updating.set()
        if failure == 'cancel':
            await asyncio.Future()
        raise AdxControlError('policy rejected')

    async def release(*_args, **_kwargs):
        releasing.set()
        if failure == 'release':
            raise RuntimeError('release failed')
        if failure == 'cancel':
            await finish.wait()
        released.set()

    monkeypatch.setattr(router._control, 'ensure_builtin_network', update)
    release_call = AsyncMock(side_effect=release)
    monkeypatch.setattr(router._agent_manager, 'release', release_call)
    await router.connect('')
    try:
        task = asyncio.create_task(router._resolve_agent(_request(), acquire=True))
        if failure == 'cancel':
            await asyncio.wait_for(updating.wait(), 2)
            task.cancel()
            await asyncio.wait_for(releasing.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            finish.set()
            await asyncio.wait_for(released.wait(), 2)
        else:
            with pytest.raises(AgentCreateFailed, match='AGENT_NETWORK_UPDATE_FAILED: policy rejected'):
                await task
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        release_call.assert_awaited_once_with(runtime.key, expected_agent_id=runtime.info.agent_id)
    finally:
        finish.set()
        await router.shutdown()


@pytest.mark.asyncio
async def test_delete_waits_for_update_and_clears_cached_generation(monkeypatch):
    updating, finish = asyncio.Event(), asyncio.Event()
    methods = []

    async def handle(request):
        methods.append(request.method)
        if methods == ['PUT']:
            updating.set()
            await finish.wait()
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(client.ensure_builtin_network('alice', '/alice'))
            await asyncio.wait_for(updating.wait(), 2)
            tasks.create_task(client.delete_sandbox('alice'))
            await asyncio.sleep(0)
            assert methods == ['PUT']
            finish.set()
        assert 'alice' not in client._environment_locks
        await client.ensure_builtin_network('alice', '/alice')
        assert methods == ['PUT', 'DELETE', 'PUT']
