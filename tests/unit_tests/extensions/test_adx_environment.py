"""Environment-backed routing retains Gateway-owned launch specifications."""
import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.control_client import (
    AdxControlClient, AdxControlError, AdxCreateTimeout, AdxCreateUncertain, AdxGenerationMismatch,
)
from agent_gateway.extensions.agentos.agentos_router.backend import AGENT_GENERATION_ENV


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
@pytest.mark.parametrize(('stored_generation', 'probed_generation', 'accepted'), [
    (None, None, False),
    (None, '0123456789ab' + '0' * 20, False),
    (None, '0123456789ab' + '1' * 20, True),
    ('0123456789ab' + '0' * 20, None, False),
    ('0123456789ab' + '1' * 20, None, True),
])
async def test_short_name_reuse_requires_the_full_generation(
    monkeypatch, tmp_path, stored_generation, probed_generation, accepted,
):
    generation = '0123456789ab' + '1' * 20
    name = 'alice+jiuwenswarm-' + generation[:12]
    identifier = 'test-' + name
    requests = []

    def handle(request):
        requests.append(request)
        if request.method == 'POST':
            assert request.url.path == f'/direct/{identifier}/invoke'
            body = json.loads(request.content)
            assert body['action'] == 'cmd.run'
            assert body['args'] == {'cmd': f'printf %s "${AGENT_GENERATION_ENV}"', 'timeout': 5}
            return httpx.Response(200, json={'exit_code': 0, 'stdout': probed_generation or ''})
        assert request.method == 'GET' and request.url.path == '/api/instances'
        return httpx.Response(200, json=[{
            'id': identifier, 'tenant_id': 'tenant', 'status': 'running',
            'env_vars': {} if stored_generation is None else {AGENT_GENERATION_ENV: stored_generation},
        }])

    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    try:
        args = dict(namespace='test', name=name, workspace='/mnt/users/alice', runtime_spec={},
                    env_vars={AGENT_GENERATION_ENV: generation})
        if accepted:
            assert (await client.create_sandbox(**args)).sandbox_id == identifier
        elif stored_generation is None and probed_generation is None:
            with pytest.raises(AdxCreateUncertain, match='generation is not visible') as error:
                await client.create_sandbox(**args)
            assert error.value.outcome == 'unknown'
        else:
            with pytest.raises(AdxGenerationMismatch, match='another Agent generation') as error:
                await client.create_sandbox(**args)
            assert error.value.code == 'CONFLICT'
        # Missing metadata permits only the exact read-only marker probe, never
        # an Environment creation/deletion or arbitrary command on a collision.
        assert sum(request.method == 'POST' for request in requests) == int(stored_generation is None)
    finally:
        await client.close()


@pytest.mark.parametrize('name', ['config.yaml', 'adx-migration-error.json'])
def test_workspace_preflight_reports_invalid_utf8(tmp_path, name):
    from agent_gateway.extensions.adx.workspace import check_workspace_config

    (tmp_path / name).write_bytes(b'\xff')
    with pytest.raises(ValueError, match='WORKSPACE_CONFIG_INVALID: .*cannot read workspace deployment status'):
        check_workspace_config(tmp_path / 'config.yaml', require_prepared=True)


@pytest.mark.asyncio
async def test_replayed_create_never_deletes_a_different_generation():
    from agent_gateway.extensions.agentos.agentos_router.agent_manager import AgentRebuildPending
    from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSRouterClient

    router = SimpleNamespace(_request_sandbox_delete=AsyncMock())
    attempt = SimpleNamespace(
        deterministic_id='test-alice',
        create=AsyncMock(side_effect=AdxGenerationMismatch('another Agent generation')),
    )
    lifecycle = SimpleNamespace(attempt=attempt, retired_instance='previous-instance')
    with pytest.raises(AgentRebuildPending, match='generation did not match'):
        await AgentOSRouterClient._post_sandbox_attempt(
            router, lifecycle, attempt, asyncio.get_running_loop().time() + 1,
        )
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == 'previous-instance'
    router._request_sandbox_delete.assert_not_awaited()


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
                'token_env': 'TEST_ADX_KEY', 'builtin_process_from_image': True,
                'builtin_image': 'jiuwenswarm-agent-runtime:latest'},
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


@pytest.mark.asyncio
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
    assert body['rootfs'] == {'type': 'image', 'imageurl': 'agent-runtime:1', 'runtime': 'supervisor'}
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


@pytest.mark.asyncio
@pytest.mark.parametrize('lost_first_reply', [False, True])
async def test_recreate_after_confirmed_removal_uses_a_new_operation(monkeypatch, tmp_path, lost_first_reply):
    exists = False
    operations = []
    identifier = 'test-alice+jiuwenswarm'

    def handle(request):
        nonlocal exists
        if request.method == 'GET':
            return httpx.Response(200, json=[{
                'id': identifier, 'tenant_id': 'tenant', 'status': 'running',
            }]) if exists else httpx.Response(404)
        operation = request.headers['x-request-id']
        if operation not in operations:
            operations.append(operation)
            exists = True
            if lost_first_reply and len(operations) == 1:
                raise httpx.ReadTimeout('reply lost', request=request)
        # Replaying a completed create does not recreate a removed resource.
        return httpx.Response(200, json=envelope({'environmentId': identifier, 'status': 'running'}))

    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    args = dict(namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}, 'cpu': 1000, 'memory': 512})
    try:
        if lost_first_reply:
            with pytest.raises(AdxControlError):
                await client.create_sandbox(**args)
        first = await client.create_sandbox(**args)
        assert first.sandbox_id == identifier
        assert identifier not in client._creates
        exists = False

        second = await client.create_sandbox(**args)

        assert second.sandbox_id == identifier
        assert len(operations) == 2 and operations[0] != operations[1]
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['terminal', 'unknown', None])
async def test_terminal_create_can_restart_only_after_confirmed_absence(monkeypatch, tmp_path, outcome):
    operations = []
    exists = False
    identifier = 'test-alice+jiuwenswarm'

    def handle(request):
        nonlocal exists
        if request.method == 'GET':
            return httpx.Response(200, json=[{
                'id': identifier, 'tenant_id': 'tenant', 'status': 'running',
            }]) if exists else httpx.Response(404)
        operation = request.headers['x-request-id']
        operations.append(operation)
        if operation == operations[0]:
            error = {'code': 'UNAVAILABLE', 'retry': 'never'}
            if outcome is not None:
                error['outcome'] = outcome
            return httpx.Response(409, json={'error': error})
        exists = True
        return httpx.Response(200, json=envelope({'environmentId': identifier, 'status': 'running'}))

    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    args = dict(namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}})
    try:
        with pytest.raises(AdxControlError):
            await client.create_sandbox(**args)
        if outcome == 'terminal':
            assert identifier not in client._creates
            assert (await client.create_sandbox(**args)).sandbox_id == identifier
            assert operations[0] != operations[1]
        else:
            assert client._creates[identifier][0] == operations[0]
            with pytest.raises(AdxControlError):
                await client.create_sandbox(**args)
            assert operations[0] == operations[1]
    finally:
        await client.close()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
@pytest.mark.parametrize(('status', 'code'), [(429, 'RATE_LIMITED'), (503, 'UNAVAILABLE')])
async def test_create_http_error_preserves_status(monkeypatch, tmp_path, status, code):
    cfg = config(monkeypatch, tmp_path)
    instance = 'test-alice+jiuwenswarm'

    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        return httpx.Response(status, json={'error': {'code': code}})

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    try:
        with pytest.raises(AdxControlError) as caught:
            await client.create_sandbox(
                namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
                runtime_spec={'rootfs': {'imageurl': 'app:1'}, 'cpu': 1000, 'memory': 512},
            )
        assert caught.value.status_code == status
        if status == 429:
            assert instance not in client._creates
        else:
            assert instance in client._creates
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_rate_limit_on_replay_keeps_create_operation(monkeypatch, tmp_path):
    cfg = config(monkeypatch, tmp_path)
    instance = 'test-alice+jiuwenswarm'
    posts = {'n': 0}

    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        posts['n'] += 1
        if posts['n'] == 1:
            raise httpx.ReadTimeout('response lost', request=request)
        return httpx.Response(429, json={'error': {'code': 'RATE_LIMITED'}})

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    args = dict(
        namespace='test', name='alice+jiuwenswarm', workspace='/mnt/users/alice',
        runtime_spec={'rootfs': {'imageurl': 'app:1'}, 'cpu': 1000, 'memory': 512},
    )
    try:
        with pytest.raises(AdxControlError):
            await client.create_sandbox(**args)
        operation = client._creates[instance]

        with pytest.raises(AdxControlError):
            await client.create_sandbox(**args)

        assert client._creates[instance] == operation
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


@pytest.mark.asyncio
@pytest.mark.parametrize('confirm_generation', [False, True])
async def test_wait_running_timeout_uses_the_existing_lifecycle_error(monkeypatch, tmp_path, confirm_generation):
    from dataclasses import replace
    from agent_gateway.extensions.agentos.agentos_router.backend import AgentTimeoutError

    cfg = replace(config(monkeypatch, tmp_path), connect_timeout_seconds=0.01)
    calls = []

    def handle(request):
        calls.append(request)
        if request.method == 'POST':
            return httpx.Response(200, json={'exit_code': 0, 'stdout': ''})
        return httpx.Response(200, json=[{
            'id': 'test-alice', 'tenant_id': cfg.tenant,
            'status': 'running' if confirm_generation else 'creating',
        }])

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    try:
        with pytest.raises(AdxCreateTimeout) as error:
            await client.wait_until_running('test-alice', expected_generation='pending' if confirm_generation else None)
        assert isinstance(error.value, AgentTimeoutError)
        assert error.value.code == 'SERVICE_UNAVAILABLE'
        assert isinstance(error.value.__cause__, TimeoutError)
        if confirm_generation:
            assert 'confirming the ADX Environment generation' in str(error.value)
            assert calls[1].url.path == '/direct/test-alice/invoke'
        else:
            assert 'become available' in str(error.value)
            assert calls and all(request.method == 'GET' for request in calls)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_wait_running_honors_probe_deadline_beyond_connect_timeout(monkeypatch, tmp_path):
    from dataclasses import replace

    cfg = replace(config(monkeypatch, tmp_path), connect_timeout_seconds=0.01)

    async def handle(_request):
        await asyncio.sleep(0.03)
        return httpx.Response(200, json=[{'id': 'test-alice', 'tenant_id': cfg.tenant, 'status': 'running'}])

    client = AdxControlClient(cfg, transport=httpx.MockTransport(handle))
    try:
        deadline = asyncio.get_running_loop().time() + 1
        assert (await client.wait_until_running('test-alice', deadline=deadline))['status'] == 'running'
        assert (await client._ready_sandbox_info('test-alice')).sandbox_id == 'test-alice'
    finally:
        await client.close()


def test_built_in_prepared_local_rootfs_and_image_environment(monkeypatch, tmp_path):
    from dataclasses import replace

    from agent_gateway.common.config import resolve_env_vars

    monkeypatch.setenv("USER_VALUE", "host-value")
    launch = {
        "rootfs": {"type": "local", "path": "/prepared/rootfs"},
        "process": {"commands": [["/app/server"]]},
        "image_env": {"VALUE": "${USER_VALUE}", "PROFILE": "image"},
        "env": {"PROFILE": "${USER_VALUE}"},
    }
    resolved = resolve_env_vars({
        "gateway": {"adx": {"builtin_launch": launch}},
        "sandbox": {"image_env": launch["image_env"], "env": launch["env"]},
    })
    client = AdxControlClient(replace(config(monkeypatch, tmp_path), runtime_class="runsc",
                                      builtin_launch=resolved["gateway"]["adx"]["builtin_launch"]))
    body = client._creation_body("test", "alice", str(tmp_path), {}, {}, [], builtin=True)
    assert body["rootfs"] == {"type": "local", "path": "/prepared/rootfs", "runtime": "runsc"}
    assert body["process"] == launch["process"]
    assert body["env"]["VALUE"] == "${USER_VALUE}"
    assert body["env"]["PROFILE"] == "host-value"
    assert "inheritEntrypoint" not in body
    assert resolved["sandbox"] == {
        "image_env": launch["image_env"], "env": {"PROFILE": "host-value"},
    }


def test_declared_runtime_credential_requires_a_launch_value(monkeypatch):
    from agent_gateway.common import runtime_settings

    monkeypatch.setattr(runtime_settings, "runtime_settings",
                        lambda: {"credentials": {"EXAMPLE_SOURCE": "EXAMPLE_TARGET"}})
    monkeypatch.delenv("EXAMPLE_SOURCE", raising=False)

    with pytest.raises(ValueError, match="EXAMPLE_SOURCE is missing for EXAMPLE_TARGET"):
        runtime_settings.prepare_runtime_environment()


def test_third_party_web_port_is_published_only_when_requested(monkeypatch, tmp_path):
    client = AdxControlClient(config(monkeypatch, tmp_path))
    spec = {"sandbox_type": "runsc", "rootfs": {"type": "local", "path": "/node/rootfs"},
            "cmds": [["/app/agent"]]}
    body = client._creation_body("test", "alice", str(tmp_path), spec, {}, [], builtin=False, web_port=8080)
    assert body["ports"] == ["8080"]
    assert body["dataPlane"] == {"portForwardSecurityMode": "tls"}
    assert body["rootfs"] == {"type": "local", "path": "/node/rootfs", "runtime": "runsc"}


@pytest.mark.asyncio
async def test_deployment_default_rootfs_is_reserved_for_builtin_agents(monkeypatch, tmp_path):
    from dataclasses import replace

    cfg = replace(config(monkeypatch, tmp_path), builtin_image=None)
    client = AdxControlClient(cfg)
    try:
        args = ('test', 'alice', str(tmp_path), {}, {}, [])
        assert 'rootfs' not in client._creation_body(*args, builtin=True)
        with pytest.raises(ValueError, match='requires an image or Local rootfs'):
            client._creation_body(*args, builtin=False)
    finally:
        await client.close()
