"""ADX-owned restart and reuse through the business router."""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from agent_gateway.extensions.adx.control_client import AdxControlError
from agent_gateway.extensions.agentos.agentos_router.registry_client import ImageInfo, InstanceRecord
from tests.unit_tests.extensions.test_adx_router import _config, _request, _router

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def connected(monkeypatch, **config):
    router, clients = _router(_config(monkeypatch, **config))
    await router.connect('')
    try:
        yield router, clients
    finally:
        await router.shutdown()


def register_existing(router, instance, status, framework='jiuwenswarm', *, registered=True):
    router.test_api.environments[instance] = {'id': instance, 'tenant_id': 'appliance', 'status': status}
    record = InstanceRecord(service_id='existing', instance_id=instance, user='alice', framework=framework)
    router._registry.list_instances = AsyncMock(return_value=[record] if registered else [])


@pytest.mark.parametrize('runtime_class,instance,status,registered', [
    ('runsc', 'test-alice+jiuwenswarm', 'running', False),
    ('supervisor', 'test-gateway-existing-generation', 'paused', True),
    ('runsc', 'test-alice+jiuwenswarm-0123456789ab', 'paused', True),
])
async def test_first_request_reuses_native_environment(monkeypatch, runtime_class, instance, status, registered):
    async with connected(monkeypatch, runtime_class=runtime_class) as (router, clients):
        register_existing(router, instance, status, registered=registered)
        assert (await router.send_request(_request())).ok
        assert not router.test_api.creates
        assert unquote(parse_qs(urlsplit(clients[0].uri).query)['target'][0]) == 'urn:adx:instance:' + instance


async def test_custom_runsc_reuse_and_disconnect(monkeypatch):
    async with connected(monkeypatch, runtime_class='supervisor', restart_policy=None) as (router, _):
        instance = 'test-alice+dsh'
        register_existing(router, instance, 'running', 'dsh')
        spec = {'sandbox_type': 'runsc', 'rootfs': {'imageurl': 'dsh:1'}}
        router._registry.get_image_info = AsyncMock(return_value=ImageInfo(
            image_name='dsh', image_uri='dsh:1', metadata={'runtime_spec': spec}))
        runtime = await router._agent_manager.get_or_create_agent('alice', 'dsh', creator=router._create_agent)
        assert runtime.info.sandbox_id == instance and not router.test_api.creates
        await router._run_network_failure_delete(runtime)
        assert instance in router.test_api.environments
        assert all(req.method != 'DELETE' for req in router.test_api.requests)


@pytest.mark.parametrize('status', ['running', 'pausing', 'paused', 'resuming', 'failed', 'lookup_error'])
async def test_disconnect_retains_recoverable_environment(monkeypatch, status):
    async with connected(monkeypatch, runtime_class='runsc') as (router, clients):
        await router.send_request(_request())
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        instance = runtime.info.sandbox_id
        if status == 'lookup_error':
            monkeypatch.setattr(router._control, 'get_agent_info', AsyncMock(side_effect=AdxControlError('timeout')))
        else:
            router.test_api.environments[instance].update(status=status, restart_pending=status == 'failed')
        await router._run_network_failure_delete(runtime)
        clients[0].disconnect.assert_awaited()
        retained = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert retained.info.sandbox_id == instance and retained.info.agent_id == runtime.info.agent_id
        assert instance in router.test_api.environments
        assert all(req.method != 'DELETE' for req in router.test_api.requests)


@pytest.mark.parametrize('options,expected', [
    ({}, {'maxAttempts': 3, 'initialBackoffSeconds': 1, 'maxBackoffSeconds': 30}),
    ({'restart_policy': None}, None),
    ({'restart_policy': {'maxAttempts': 5, 'initialBackoffSeconds': 2, 'maxBackoffSeconds': 10}},
     {'maxAttempts': 5, 'initialBackoffSeconds': 2, 'maxBackoffSeconds': 10}),
])
async def test_restart_policy_on_create(monkeypatch, options, expected):
    async with connected(monkeypatch, **options) as (router, _):
        assert (await router.send_request(_request())).ok
        body, = router.test_api.creates
        assert ('restartPolicy' in body) == (expected is not None)
        assert body.get('restartPolicy') == expected


@pytest.mark.parametrize('terminal', [False, True])
async def test_wait_observes_restart_until_running_or_exhausted(monkeypatch, terminal):
    async with connected(monkeypatch) as (router, _):
        final = {'status': 'failed' if terminal else 'running', 'restart_pending': False}
        observe = AsyncMock(side_effect=[{'status': 'failed', 'restart_pending': True}, final])
        monkeypatch.setattr(router._control, 'get_agent_info', observe)
        sleep = AsyncMock()
        monkeypatch.setattr('agent_gateway.extensions.adx.control_client.asyncio.sleep', sleep)
        if terminal:
            with pytest.raises(AdxControlError, match='not available'):
                await router._control.wait_until_running('test-agent')
        else:
            assert await router._control.wait_until_running('test-agent') == final
        assert [call.args for call in observe.await_args_list] == [('test-agent',)] * 2
        sleep.assert_awaited_once()
