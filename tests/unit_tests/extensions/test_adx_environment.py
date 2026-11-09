"""Environment-backed routing retains Gateway-owned launch specifications."""
import base64
import json
from ipaddress import IPv4Address

import httpx
import pytest

from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError, AdxCreateTimeout


def config(monkeypatch, tmp_path):
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    return load_router_config({'gateway': {
        'agent_client': {'type': 'adx_router', 'agent_namespace': 'test'},
        'adx': {'ingress_url': 'https://adx.example', 'tenant': 'tenant',
                'token_env': 'TEST_ADX_KEY', 'runtime_class': 'runc'},
        'agentos': {'workspace_root': str(tmp_path), 'connect_warmup_enabled': False},
    }})


def envelope(data):
    return {'code': 200, 'message': '', 'data': base64.b64encode(json.dumps(data).encode()).decode()}


@pytest.mark.asyncio
@pytest.mark.parametrize(('name', 'image', 'from_image'), [
    ('alice+jiuwenswarm', 'jiuwenswarm-agent-runtime:latest', True),
    ('alice+custom', 'jiuwenswarm-agent-runtime:latest', False),
    ('alice+jiuwenswarm', 'custom-agent:1', False),
])
async def test_builtin_can_use_deployed_image_process(monkeypatch, name, image, from_image):
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    cfg = load_router_config({'gateway': {
        'adx': {'ingress_url': 'https://adx.example', 'tenant': 'tenant',
                'token_env': 'TEST_ADX_KEY', 'builtin_process_from_image': True},
    }})
    exists = False

    def handle(request):
        nonlocal exists
        if request.method == 'GET':
            return httpx.Response(200, json=[{'id': 'default-' + name,
                'tenant_id': 'tenant', 'status': 'running'}]) if exists else httpx.Response(404)
        body = json.loads(request.content)
        assert body['inheritEntrypoint'] is True
        if from_image:
            assert 'process' not in body
        else:
            assert body['process'] == {'commands': [['/bin/app']], 'user': 'agentos'}
        exists = True
        return httpx.Response(200, json=envelope({'environmentId': 'default-' + name}))

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    try:
        await client.create_sandbox(namespace='default', name=name, workspace='/mnt/alice',
            runtime_spec={'rootfs': {'imageurl': image, 'user': 'agentos'}, 'cmds': [['/bin/app']]})
    finally:
        await client.close()


async def test_environment_create_query_delete_uses_original_spec(monkeypatch, tmp_path):
    cfg = config(monkeypatch, tmp_path)
    calls = []
    exists = False

    def handle(request):
        nonlocal exists
        calls.append(request)
        assert request.headers['authorization'] == 'Bearer platform-secret'
        if request.method == 'GET':
            return httpx.Response(200, json=[{'id': 'test-alice+jiuwenswarm', 'tenant_id': 'tenant',
                'status': 'running'}]) if exists else httpx.Response(404)
        if request.method == 'POST':
            exists = True
            return httpx.Response(200, json=envelope({'environmentId': 'test-alice+jiuwenswarm', 'status': 'running'}))
        return httpx.Response(200, json={'code': 200, 'message': '', 'data': None})

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    spec = {'sandbox_type': 'supervisor', 'runtime': 'python3.11',
            'rootfs': {'imageurl': 'agent-runtime:1', 'user': '10001:10001', 'ports': ['tcp:18092']},
            'cpu': 2000, 'memory': 4096, 'cmds': [['/bin/app', '--port', '18092']]}
    args = dict(namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
                runtime_spec=spec, env_vars={'MODEL_API_BASE': 'http://model/v1'})
    first = await client.create_sandbox(**args)
    second = await client.create_sandbox(**args)
    assert first.sandbox_id == second.sandbox_id == 'test-alice+jiuwenswarm'
    creates = [request for request in calls if request.method == 'POST']
    assert len(creates) == 1
    body = json.loads(creates[0].content)
    assert body['namespace'] == cfg.router.agent_namespace != cfg.tenant
    assert second.metadata['tenant_id'] == cfg.tenant
    assert body['rootfs'] == {'type': 'image', 'imageurl': 'agent-runtime:1', 'runtime': 'runc'}
    assert body['process'] == {'commands': spec['cmds'], 'user': '10001:10001'}
    assert body['cpu'] == 2000 and body['memory'] == 4096
    assert body['env']['MODEL_API_BASE'] == 'http://model/v1'
    assert body['mounts'][0]['host_path'] == '/mnt/users/alice'
    assert body['mounts'][0]['target'] == '/home/agentos'
    assert body['mounts'][0]['options'] == ['rbind', 'rw']
    await client.delete_sandbox(first.sandbox_id)
    assert calls[-1].url.path == '/api/environment/v1/environments/test-alice+jiuwenswarm'
    assert all('/api/agent/' not in request.url.path for request in calls)
    await client.close()


async def test_create_timeout_retries_same_operation_and_never_changes_instance(monkeypatch, tmp_path):
    ids = []
    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        ids.append(request.headers['x-request-id'])
        raise httpx.ReadTimeout('response lost', request=request)
    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    args = dict(namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}, 'cpu': 1000, 'memory': 512})
    for _ in range(2):
        with pytest.raises(AdxControlError):
            await client.create_sandbox(**args)
    assert len(ids) == 2 and ids[0] == ids[1]
    await client.close()


@pytest.mark.parametrize('result,expected_error', [
    ({'environmentId': 'test-other'}, 'unexpected'),
    ({'environmentId': ''}, 'control'),
    ({'environmentId': 123}, 'control'),
    ({}, 'control'),
    (None, 'control'),
])
async def test_create_response_identity_mismatch(monkeypatch, tmp_path, result, expected_error):
    from agent_gateway.extensions.yuanrong_frontend_client import UnexpectedSandboxIdError

    calls = []
    def handle(request):
        calls.append(request.method)
        return httpx.Response(404) if request.method == 'GET' else httpx.Response(200, json=envelope(result))

    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    try:
        error = UnexpectedSandboxIdError if expected_error == 'unexpected' else AdxControlError
        with pytest.raises(error) as exc:
            await client.create_sandbox(namespace='test', name='alice', workspace='/mnt/alice',
                                        runtime_spec={'rootfs': {'imageurl': 'app:1'}})
        if expected_error == 'unexpected':
            assert exc.value.instance_id == 'test-other'
        assert calls == ['GET', 'POST']
    finally:
        await client.close()


async def test_forget_abandoned_create_releases_only_its_request_tokens(monkeypatch, tmp_path):
    client = AdxControlClient(config(monkeypatch, tmp_path))
    try:
        client._creates.update({'test-alice': ('create-a', 'digest'), 'test-bob': ('create-b', 'digest')})
        client._deletes.update({'test-alice': 'delete-a', 'test-bob': 'delete-b'})
        client.forget_abandoned_create('test-alice')
        assert client._creates == {'test-bob': ('create-b', 'digest')}
        assert client._deletes == {'test-bob': 'delete-b'}
    finally:
        await client.close()


async def test_environment_query_rejects_mismatched_tenant_and_identity(monkeypatch, tmp_path):
    cfg = config(monkeypatch, tmp_path)
    for view in [
        {'id': 'test-alice', 'tenant_id': 'other', 'status': 'running'},
        {'id': 'test-alice', 'tenant_id': cfg.router.agent_namespace, 'status': 'running'},
        {'id': 'test-bob', 'tenant_id': 'tenant', 'status': 'running'},
    ]:
        client = AdxControlClient(cfg, transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[view])))
        with pytest.raises(AdxControlError):
            await client.get_agent_info('test-alice')
        await client.close()


async def test_wait_running_timeout_uses_the_existing_lifecycle_error(monkeypatch, tmp_path):
    from dataclasses import replace
    from agent_gateway.extensions.yuanrong_frontend_client import YuanrongAgentTimeoutError

    cfg = replace(config(monkeypatch, tmp_path), connect_timeout_seconds=0.01)
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json=[{
            'id': 'test-alice', 'tenant_id': cfg.tenant, 'status': 'creating',
        }])

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    try:
        with pytest.raises(AdxCreateTimeout) as error:
            await client.wait_until_running('test-alice')
        assert isinstance(error.value, YuanrongAgentTimeoutError)
        assert error.value.code == 'SERVICE_UNAVAILABLE'
        assert isinstance(error.value.__cause__, TimeoutError)
        assert calls and all(request.method == 'GET' for request in calls)
    finally:
        await client.close()


@pytest.mark.parametrize('committed', [True, False])
async def test_router_recovers_unknown_adx_create_with_original_identity(monkeypatch, tmp_path, committed):
    from agent_gateway.extensions.adx.router_client import AdxRouterClient
    from agent_gateway.extensions.agentos.agentos_router.router_client import (
        AgentRebuildPending, _SandboxAttempt, _SandboxLifecycle,
    )

    cfg = config(monkeypatch, tmp_path)
    instance = 'test-alice+agent-gen1'
    # Use two distinct addresses from TEST-NET-1 for the mocked placement.
    node_ip = str(IPv4Address(bytes((192, 0, 2, 1))))
    sandbox_ip = str(IPv4Address(bytes((192, 0, 2, 2))))
    exists = False
    posts = []
    queries = []

    def handle(request):
        nonlocal exists
        if request.method == 'GET':
            queries.append(request.url.params['instance_id'])
            if not exists:
                return httpx.Response(404)
            return httpx.Response(200, json=[{'id': instance, 'tenant_id': cfg.tenant,
                'status': 'running', 'node_ip': node_ip, 'sandbox_ip': sandbox_ip}])
        posts.append(request)
        if len(posts) == 1:
            exists = committed
            raise httpx.ReadTimeout('response lost', request=request)
        exists = True
        return httpx.Response(200, json=envelope({'environmentId': instance}))

    control = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    router = AdxRouterClient(cfg, control_client=control)
    args = dict(namespace='test', name='alice+agent-gen1', workspace='/mnt/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}})
    attempt = _SandboxAttempt('gen1', router._sandbox_instance_id('test', args['name']),
                              lambda: control.create_sandbox(**args))
    lifecycle = _SandboxLifecycle()
    try:
        with pytest.raises(AgentRebuildPending):
            await router._provision_sandbox(lifecycle, attempt)
        recovered = await router._provision_sandbox(lifecycle, attempt)
        assert recovered.sandbox_id == instance
        assert recovered.metadata['node'] == node_ip
        assert recovered.metadata['address'] == sandbox_ip
        assert set(queries) == {instance}
        assert len(posts) == (1 if committed else 2)
        if not committed:
            assert posts[0].headers['x-request-id'] == posts[1].headers['x-request-id']
            assert posts[0].content == posts[1].content
    finally:
        await control.close()


@pytest.mark.parametrize('response', ['server_error', 'failed', 'pending_then_absent'])
async def test_router_never_replays_after_uncertain_adx_observation(monkeypatch, tmp_path, response):
    from agent_gateway.extensions.adx.router_client import AdxRouterClient
    from agent_gateway.extensions.agentos.agentos_router.router_client import (
        AgentRebuildPending, _SandboxAttempt, _SandboxLifecycle,
    )

    cfg = config(monkeypatch, tmp_path)
    instance = 'test-alice+agent-gen1'
    posts = []
    gets = 0

    def handle(request):
        nonlocal gets
        if request.method == 'POST':
            posts.append(request)
            if len(posts) > 1:
                return httpx.Response(200, json=envelope({'environmentId': instance}))
            raise httpx.ReadTimeout('response lost', request=request)
        gets += 1
        if gets == 1:
            return httpx.Response(404)
        if response == 'server_error':
            if gets == 2:
                return httpx.Response(503, json={'error': {'code': 'SERVICE_UNAVAILABLE'}})
            if len(posts) == 1:
                return httpx.Response(404)
            return httpx.Response(200, json=[{'id': instance, 'tenant_id': cfg.tenant, 'status': 'running'}])
        if response == 'pending_then_absent' and gets >= 3:
            return httpx.Response(404)
        status = 'failed' if response == 'failed' else 'creating'
        return httpx.Response(200, json=[{'id': instance, 'tenant_id': cfg.tenant, 'status': status}])

    control = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    router = AdxRouterClient(cfg, control_client=control)
    args = dict(namespace='test', name='alice+agent-gen1', workspace='/mnt/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}})
    attempt = _SandboxAttempt('gen1', router._sandbox_instance_id('test', args['name']),
                              lambda: control.create_sandbox(**args))
    lifecycle = _SandboxLifecycle()
    try:
        # The initial POST and the following observation each remain pending.
        for _ in range(2):
            with pytest.raises(AgentRebuildPending):
                await router._provision_sandbox(lifecycle, attempt)
        assert len(posts) == 1
        if response == 'server_error':
            recovered = await router._provision_sandbox(lifecycle, attempt)
            assert recovered.sandbox_id == instance
            assert len(posts) == 2
            assert posts[0].headers['x-request-id'] == posts[1].headers['x-request-id']
    finally:
        await control.close()


async def test_late_adx_create_cleanup_requires_exact_id_and_tenant(monkeypatch, tmp_path):
    from agent_gateway.extensions.adx.router_client import AdxRouterClient
    from agent_gateway.extensions.agentos.agentos_router.router_client import (
        AgentRebuildPending, _SandboxAttempt, _SandboxLifecycle,
    )

    cfg = config(monkeypatch, tmp_path)
    instance = 'test-gateway-gen1'
    visible_tenant = None
    deleted = []

    def handle(request):
        if request.method == 'POST':
            raise httpx.ReadTimeout('response lost', request=request)
        if request.method == 'DELETE':
            deleted.append(request.url.path.rsplit('/', 1)[-1])
            return httpx.Response(200, json={'code': 200, 'data': None})
        assert request.url.params['instance_id'] == instance
        if visible_tenant is None:
            return httpx.Response(404)
        return httpx.Response(200, json=[{'id': instance, 'tenant_id': visible_tenant, 'status': 'running'}])

    control = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    router = AdxRouterClient(cfg, control_client=control)
    args = dict(namespace='test', name='gateway-gen1', workspace='/mnt/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}})
    attempt = _SandboxAttempt('gen1', instance, lambda: control.create_sandbox(**args))
    lifecycle = _SandboxLifecycle()
    try:
        with pytest.raises(AgentRebuildPending):
            await router._provision_sandbox(lifecycle, attempt)
        with pytest.raises(AgentRebuildPending, match='replay budget exhausted'):
            await router._provision_sandbox(lifecycle, attempt)
        assert lifecycle.attempt is None
        record = lifecycle.retirements[(instance, 'gen1')]
        assert not await router._observe_retired_sandbox(lifecycle, record)
        visible_tenant = 'foreign'
        assert not await router._observe_retired_sandbox(lifecycle, record)
        assert not deleted
        visible_tenant = cfg.tenant
        await router._observe_retired_sandbox(lifecycle, record)
        assert deleted == [instance]
    finally:
        await control.close()


async def test_expired_adx_create_retirement_releases_request_tokens(monkeypatch, tmp_path):
    import asyncio
    from agent_gateway.extensions.adx.router_client import AdxRouterClient
    from agent_gateway.extensions.agentos.agentos_router.router_client import (
        AgentRebuildPending, _SandboxAttempt, _SandboxLifecycle,
    )

    calls = []
    def handle(request):
        calls.append(request.method)
        if request.method == 'POST':
            raise httpx.ReadTimeout('response lost', request=request)
        return httpx.Response(404)

    cfg = config(monkeypatch, tmp_path)
    control = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    router = AdxRouterClient(cfg, control_client=control)
    instance = 'test-gateway-gen1'
    args = dict(namespace='test', name='gateway-gen1', workspace='/mnt/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}})
    attempt = _SandboxAttempt('gen1', instance, lambda: control.create_sandbox(**args))
    lifecycle = _SandboxLifecycle()
    try:
        for _ in range(2):
            with pytest.raises(AgentRebuildPending):
                await router._provision_sandbox(lifecycle, attempt)
        assert instance in control._creates
        lifecycle.retirements[(instance, 'gen1')].expires_at = asyncio.get_running_loop().time()
        router._sandbox_lifecycles['gateway'] = lifecycle
        router._closed = False
        await router._retry_retired_sandbox_cleanup('gateway', lifecycle)
        assert instance not in control._creates
        assert instance not in control._deletes
        assert 'DELETE' not in calls
    finally:
        await control.close()
