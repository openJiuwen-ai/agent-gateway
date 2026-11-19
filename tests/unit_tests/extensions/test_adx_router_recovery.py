"""ADX-owned restart and reuse through the business router."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from agent_gateway.extensions.adx.control_client import AdxControlError
from agent_gateway.extensions.agentos.agentos_router.models import AgentInfo
from agent_gateway.extensions.agentos.agentos_router.registry_client import InstanceRecord
from tests.unit_tests.extensions.test_adx_router import _config, _router, _request
from tests.unit_tests.extensions.test_adx_router_network import _network


@pytest.mark.asyncio
@pytest.mark.parametrize('instance', [
    'test-alice+jiuwenswarm', 'test-gateway-existing-generation',
    'test-alice+jiuwenswarm-0123456789ab',
])
@pytest.mark.parametrize('status', ['running', 'paused'])
@pytest.mark.parametrize('runtime_class', ['runsc', 'supervisor'])
async def test_first_request_after_restart_reuses_native_registered_environment(monkeypatch, instance, status, runtime_class):
    router, clients = _router(_config(monkeypatch, runtime_class=runtime_class))
    router.test_api.environments[instance] = {'id': instance, 'tenant_id': 'appliance', 'status': status}
    router._registry.list_instances = AsyncMock(return_value=[InstanceRecord(
        service_id='persistent', instance_id=instance, user='alice', framework='jiuwenswarm')])
    await router.connect('')
    try:
        assert (await router.send_request(_request())).ok
        assert router.test_api.creates == []
        assert unquote(parse_qs(urlsplit(clients[0].uri).query)['target'][0]) == 'urn:adx:instance:' + instance
    finally:
        await router.shutdown()

@pytest.mark.asyncio
async def test_custom_runsc_reuse_and_disconnect_follow_its_declared_runtime(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router.registry_client import ImageInfo

    router, _ = _router(_config(monkeypatch, runtime_class='supervisor', restart_policy=None))
    spec = {'sandbox_type': 'runsc', 'rootfs': {'imageurl': 'dsh:1'}}
    router._registry.get_image_info = AsyncMock(return_value=ImageInfo(
        image_name='dsh', image_uri='dsh:1', metadata={'runtime_spec': spec}))
    instance = 'test-alice+dsh'
    router._registry.list_instances = AsyncMock(return_value=[InstanceRecord(
        service_id='old-service', instance_id=instance, user='alice', framework='dsh')])
    try:
        router.test_api.environments[instance] = {'id': instance, 'tenant_id': 'appliance', 'status': 'running'}
        runtime = await router._agent_manager.get_or_create_agent('alice', 'dsh', creator=router._create_agent)
        assert runtime.info.sandbox_id == instance
        assert not router.test_api.creates
        await router._run_network_failure_delete(runtime, reason='connection_closed')
        assert instance in router.test_api.environments
        assert not any(request.method == 'DELETE' for request in router.test_api.requests)
    finally:
        await router.shutdown()

@pytest.mark.asyncio
@pytest.mark.parametrize('state,restart_pending', [('running',False),('pausing',False),('paused',False),('resuming',False),('failed',True)])
@pytest.mark.parametrize('runtime_class', ['runsc', 'supervisor'])
async def test_transport_failure_keeps_adx_lifecycle_and_checkpoint(monkeypatch, state, restart_pending, runtime_class):
    router, clients = _router(_config(monkeypatch, runtime_class=runtime_class))
    await router.connect('')
    try:
        await router.send_request(_request())
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        row = router.test_api.environments[runtime.info.sandbox_id]
        row.update(status=state, restart_pending=restart_pending)
        await router._run_network_failure_delete(runtime, reason='connection_closed')
        assert runtime.info.sandbox_id in router.test_api.environments
        assert not any(r.method == 'DELETE' for r in router.test_api.requests)
        clients[0].disconnect.assert_awaited()
        assert await router._agent_manager.get_agent('alice', 'jiuwenswarm') is not None
    finally:
        await router.shutdown()

@pytest.mark.asyncio
async def test_uncertain_lifecycle_observation_does_not_delete(monkeypatch):
    from agent_gateway.extensions.adx.control_client import AdxControlError
    router, _ = _router(_config(monkeypatch, runtime_class='runsc'))
    await router.connect('')
    try:
        await router.send_request(_request())
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        monkeypatch.setattr(router._control, 'get_agent_info', AsyncMock(side_effect=AdxControlError('lookup timed out', outcome='unknown')))
        await router._run_network_failure_delete(runtime, reason='timeout')
        assert not any(r.method == 'DELETE' for r in router.test_api.requests)
    finally:
        await router.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize('restart_policy', ['default'])
async def test_real_stream_disconnect_is_not_replayed_and_next_call_reauthenticates(tmp_path, monkeypatch, restart_policy):
    async with _network(tmp_path, monkeypatch, restart_policy=restart_policy) as (router, requests, handshakes, targets):
        stream = router.send_request_stream(_request(rid="lost", action="drop"))
        assert (await anext(stream)).payload["index"] == 0
        failure = await asyncio.wait_for(anext(stream), timeout=3)
        assert failure.is_complete and failure.payload.get("error")
        await stream.aclose()
        assert [request["request_id"] for request in requests] == ["lost"]
        current = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert (current is None) == (restart_policy is None)
        assert (await router.send_request(_request(rid="next"))).ok
        assert len(router.test_api.creates) == (2 if restart_policy is None else 1)
        assert len(handshakes) == 2
        assert (targets[0] != targets[1]) == (restart_policy is None)
        if restart_policy is None:
            assert router.test_api.creates[0]['mounts'] == router.test_api.creates[1]['mounts']
        assert all(headers["authorization"] == "Bearer platform-secret" for _, headers in handshakes)
