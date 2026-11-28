# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway business routing over public ADX Environment lifecycle APIs."""
import asyncio
import base64
import json
import tempfile
from types import SimpleNamespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, unquote, urlsplit

import httpx
import pytest

from e2a.models import E2AEnvelope
from e2a.agent_models import AgentResponse
from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.runtime_config import managed_sandbox_settings
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.router_client import AdxRouterClient
from agent_gateway.extensions.agentos.agentos_router.registry_client import InstanceRecord
from agent_gateway.extensions.agentos.agentos_router.backend import AGENT_GENERATION_ENV


def _config(monkeypatch, *, sandbox=None, **overrides):
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    root = Path(tempfile.mkdtemp(prefix='adx-env-gateway-'))
    for user in ('alice', 'bob'):
        (root / user).mkdir()
    settings = {'ingress_url': 'https://adx.example', 'tenant': 'appliance', 'token_env': 'TEST_ADX_KEY',
                'connect_timeout_seconds': 2}
    settings.update(overrides)
    return load_router_config({'gateway': {
        'agent_client': {'type': 'agentos_router', 'agent_namespace': 'test'},
        'adx': settings, 'agentos': {'workspace_root': str(root)},
    }, 'sandbox': sandbox})


def test_explicit_http_origin_controls_business_http_and_websocket_schemes(monkeypatch):
    config = _config(monkeypatch, ingress_url='http://127.0.0.1:8080', allow_loopback_http=True)
    router = AdxRouterClient(config)
    local_url = urlsplit(router._agent_ws_url('test-alice+jiuwenswarm', 18092))
    assert local_url.scheme == 'ws'
    assert local_url.path == '/test-alice%2Bjiuwenswarm/18092'
    assert urlsplit(router._agent_http_url('test-third', 8080)).path == '/test-third/8080'
    assert urlsplit(router._agent_http_url('test-alice+jiuwenswarm', 18092)).scheme == 'http'
    secure = AdxRouterClient(_config(monkeypatch))
    assert urlsplit(secure._agent_ws_url('test-alice+jiuwenswarm', 18092)).scheme == 'wss'
    for protocol, url in [
        ('ws', secure._agent_ws_url('test-alice+jiuwenswarm', 18092)),
        ('http', secure._agent_http_url('test-alice+jiuwenswarm', 18092)),
    ]:
        parsed = urlsplit(url)
        target = parse_qs(parsed.query)['target'][0]
        assert parsed.path == f'/agent/{protocol}'
        assert target == 'urn:adx:instance:test-alice%2Bjiuwenswarm'
        assert unquote(target.removeprefix('urn:adx:instance:')) == 'test-alice+jiuwenswarm'


class EnvironmentAPI:
    def __init__(self, tenant='appliance'):
        self.tenant = tenant
        self.environments = {}
        self.creates = []
        self.requests = []
        self.delete_error = False
        self.delete_committed = False

    def handle(self, request):
        self.requests.append(request)
        assert request.headers['authorization'] == 'Bearer platform-secret'
        if request.method == 'GET' and request.url.path == '/api/instances':
            instance = request.url.params['instance_id']
            return httpx.Response(200, json=[self.environments[instance]]) if instance in self.environments else httpx.Response(404)
        if request.method == 'POST' and request.url.path == '/api/environment/v1/environments':
            body = json.loads(request.content)
            self.creates.append(body)
            instance = body['namespace'] + '-' + body['name']
            self.environments[instance] = {'id': instance, 'tenant_id': self.tenant, 'status': 'running',
                                           'env_vars': dict(body.get('env') or {})}
            result = {'code': 200, 'data': base64.b64encode(json.dumps({'environmentId': instance}).encode()).decode()}
            return httpx.Response(200, json=result)
        if request.method == 'DELETE':
            instance = request.url.path.rsplit('/', 1)[1]
            if self.delete_error:
                if self.delete_committed:
                    self.environments.pop(instance, None)
                raise httpx.ReadTimeout('unknown delete', request=request)
            self.environments.pop(instance, None)
            return httpx.Response(200, json={'success': True})
        raise AssertionError(f'unexpected ADX request: {request.method} {request.url}')


class FakeClient:
    def __init__(self):
        self.server_ready = False
        self.disconnect = AsyncMock(side_effect=self._disconnect)
        self.calls = []
        self.push = None
        self.closed = None

    async def _disconnect(self):
        self.server_ready = False

    def set_server_push_handler(self, handler):
        self.push = handler

    def set_disconnect_handler(self, handler):
        self.closed = handler

    async def connect(self, uri, extra_headers=None):
        await asyncio.sleep(0)
        self.uri = uri
        self.headers = extra_headers
        self.server_ready = True

    async def send_request(self, envelope, *, timeout=None):
        self.calls.append(envelope)
        return AgentResponse(envelope.request_id, envelope.channel, payload={'user': envelope.user_id})

def _router(config, *, auth_client=None):
    clients = []
    api = EnvironmentAPI(config.tenant)
    control = AdxControlClient(config, transport=httpx.MockTransport(api.handle))
    def create():
        client = FakeClient()
        clients.append(client)
        return client
    router = AdxRouterClient(
        config, ws_client_factory=create, control_client=control, auth_client=auth_client,
    )
    router.set_or_update_server_config(config={})
    router.test_api = api
    return router, clients


@pytest.mark.parametrize('failure', ['migration', 'uninitialized'])
async def test_workspace_failure_blocks_only_its_user_and_is_retryable(monkeypatch, failure):
    from agent_gateway.extensions.adx.workspace import MIGRATION_ERROR_FILE
    monkeypatch.setenv('JIUWENSWARM_RUNTIME_PROFILE', 'agentos')
    config = _config(monkeypatch)
    healthy_folder = Path(config.router.workspace_root) / 'bob/.jiuwenswarm/config'
    healthy_folder.mkdir(parents=True)
    (healthy_folder / 'config.yaml').write_text('sandbox: {type: adx-environment}\n')
    folder = Path(config.router.workspace_root) / 'alice/.jiuwenswarm/config'
    folder.mkdir(parents=True)
    marker = folder / MIGRATION_ERROR_FILE
    if failure == 'migration':
        (folder / 'config.yaml').write_text('sandbox: {type: adx-environment}\n')
        marker.write_text(json.dumps({'reason': 'sandbox.files requires explicit migration to ADX policy/mounts'}))
    router, _ = _router(config)
    await router.connect('')
    try:
        failed, healthy = await asyncio.gather(router.send_request(_request('alice')),
                                             router.send_request(_request('bob')))
        assert not failed.ok
        assert 'WORKSPACE_CONFIG_INVALID' in failed.payload['error']
        reason = 'sandbox.files' if failure == 'migration' else 'application workspace initialization required'
        assert reason in failed.payload['error']
        assert healthy.ok
        assert len(router.test_api.creates) == 1
        assert 'bob' in router.test_api.creates[0]['name']
        marker.unlink(missing_ok=True)
        (folder / 'config.yaml').write_text('sandbox: {type: adx-environment}\n')
        assert (await router.send_request(_request('alice', rid='after-migration'))).ok
        assert len(router.test_api.creates) == 2
    finally:
        await router.shutdown()


@pytest.mark.parametrize('runtime,status', [
    ('runsc', 'running'), ('runsc', 'paused'),
    ('supervisor', 'running'), ('conch', 'running'),
])
async def test_startup_cleanup_retires_untracked_environments_for_all_runtimes(
    monkeypatch, runtime, status,
):
    router, _ = _router(_config(monkeypatch, runtime_class=runtime))
    instance = 'test-alice+jiuwenswarm'
    router.test_api.environments[instance] = {'id': instance, 'status': status}
    registry = SimpleNamespace(
        enabled=True,
        list_instances=AsyncMock(return_value=[InstanceRecord(
            service_id='existing-service', instance_id=instance, user='alice', framework='jiuwenswarm',
        )]),
        unregister_instance=AsyncMock(return_value={'deleted': True}),
        close=AsyncMock(),
    )
    router._router._registry = registry
    try:
        await router.connect('')
        if router._stale_cleanup_task is not None:
            await asyncio.wait_for(router._stale_cleanup_task, timeout=2)
        assert instance not in router.test_api.environments
        registry.unregister_instance.assert_awaited_once()
        registry.list_instances.assert_awaited()
    finally:
        await router.shutdown()


def _request(user='alice', rid='same-id', **params):
    return E2AEnvelope(request_id=rid, user_id=user, channel='web', method='chat.send', params=params)


async def test_connect_warmup_survives_a_timed_out_foreground_waiter(monkeypatch):
    router, clients = _router(_config(monkeypatch))
    started = asyncio.Event()
    finish = asyncio.Event()
    original_create = router._create_agent

    async def delayed_create(info):
        started.set()
        await finish.wait()
        return await original_create(info)

    router._router._create_agent = delayed_create
    await router.connect('')
    try:
        router._schedule_connect_warmup('alice')
        warmup = router._warmup_tasks.get('alice')
        assert warmup is not None
        await asyncio.wait_for(started.wait(), timeout=3)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(router.send_request(_request()), timeout=0.01)
        assert not warmup.done()
        finish.set()
        await asyncio.wait_for(warmup, timeout=3)
        response = await router.send_request(_request(rid='after-timeout'))
        assert response.ok
        assert len(router.test_api.creates) == len(clients) == 1
    finally:
        finish.set()
        await router.shutdown()


async def test_connect_warmup_can_be_disabled_by_router_settings(monkeypatch):
    config = _config(monkeypatch)
    config = replace(config, router=replace(config.router, connect_warmup_enabled=False))
    router, clients = _router(config)
    await router.connect('')
    try:
        router._schedule_connect_warmup('alice')
        assert not router._warmup_tasks
        assert not router.test_api.creates
        assert not clients
    finally:
        await router.shutdown()


async def test_users_and_first_creates_are_isolated_and_coalesced(monkeypatch):
    router, clients = _router(_config(monkeypatch, builtin_image='registry.example/agentserver:1'))
    await router.connect('')
    try:
        results = await asyncio.gather(*(router.send_request(_request(user, str(index)))
            for index, user in enumerate(['alice', 'alice', 'bob', 'alice'])))
        assert len(clients) == len(router.test_api.creates) == 2
        assert [result.payload['user'] for result in results] == ['alice', 'alice', 'bob', 'alice']
        for client in clients:
            target = parse_qs(urlsplit(client.uri).query)['target'][0]
            assert target.startswith('urn:adx:instance:test-')
            assert client.headers['Authorization'] == 'Bearer platform-secret'
            assert 'X-ADX-Parameters' not in client.headers
        assert all(body['rootfs']['imageurl'] == 'registry.example/agentserver:1' for body in router.test_api.creates)
        for body in router.test_api.creates:
            user = Path(body['mounts'][0]['host_path']).name
            generation = body['env'][AGENT_GENERATION_ENV]
            assert len(generation) == 32
            assert body['name'] == f'{user}+jiuwenswarm-{generation[:12]}'
        mounts = [body['mounts'][0]['host_path'] for body in router.test_api.creates]
        assert len(set(mounts)) == 2 and all(Path(path).is_dir() for path in mounts)
        assert all(body['process']['commands'][0][-1].startswith('exec jiuwenswarm-agentserver')
                   for body in router.test_api.creates)
    finally:
        await router.shutdown()


@pytest.mark.parametrize('remote_deleted', [False, True])
async def test_unknown_delete_retains_operation_until_lookup_confirms_result(monkeypatch, remote_deleted):
    router, clients = _router(_config(monkeypatch))
    router._router._probe_settings = replace(router._probe_settings,
        wait_running_timeout_seconds=0.2, wait_running_interval_seconds=0.01)
    await router.connect('')
    try:
        await router.send_request(_request())
        await asyncio.gather(*tuple(router._background_tasks))
        router._registry.unregister_agent = AsyncMock()
        first_url = clients[0].uri
        router.test_api.delete_error = True
        router.test_api.delete_committed = remote_deleted
        assert await router.delete_agent('alice', 'jiuwenswarm')
        instance = next(iter(router._control._deletes))
        delete_operation = router._control._deletes[instance]
        clients[0].disconnect.assert_awaited_once()
        assert await router._agent_manager.get_agent('alice', 'jiuwenswarm') is None
        router._registry.unregister_agent.assert_awaited_once()
        # An unknown DELETE must never hand its old target back to a caller.
        response = await router.send_request(_request(rid='next'))
        assert response.ok is remote_deleted
        assert len(router.test_api.creates) == (2 if remote_deleted else 1)
        if remote_deleted:
            assert instance not in router._control._deletes
        if not remote_deleted:
            assert 'AGENT_REBUILD_PENDING' in response.payload['error']
            assert router._control._deletes[instance] == delete_operation
        router.test_api.delete_error = False
        if not remote_deleted:
            for lifecycle in router._sandbox_lifecycles.values():
                lifecycle.delete_retry_at = 0
            assert (await router.send_request(_request(rid='delete-confirmed'))).ok
            retries = [request for request in router.test_api.requests if request.method == 'DELETE'
                       and request.url.path.endswith('/' + instance)]
            assert all(request.headers['x-request-id'] == delete_operation for request in retries)
        assert len(router.test_api.creates) == 2
        assert clients[-1].uri != first_url
        assert router.test_api.creates[0]['mounts'] == router.test_api.creates[1]['mounts']
        assert await router.delete_agent('alice', 'jiuwenswarm')
        assert (await router.send_request(_request(rid='after-delete'))).ok
        assert len(router.test_api.creates) == 3
        create_ids = [request.headers['x-request-id'] for request in router.test_api.requests
                      if request.method == 'POST']
        assert len(set(create_ids)) == 3
    finally:
        await router.shutdown()


@pytest.mark.parametrize('status', ['failed', 'deleted', 'deleting', 'missing'])
async def test_terminal_environment_is_cleaned_and_next_request_recreates(monkeypatch, status):
    router, clients = _router(_config(monkeypatch))
    await router.connect('')
    try:
        assert (await router.send_request(_request())).ok
        await asyncio.gather(*tuple(router._background_tasks))
        instance = next(iter(router.test_api.environments))
        await router._close_ws_client(instance)
        if status == 'missing':
            router.test_api.environments.pop(instance)
        else:
            router.test_api.environments[instance]['status'] = status
        router._registry.unregister_agent = AsyncMock()
        assert not (await router.send_request(_request(rid='failed'))).ok
        assert await router._agent_manager.get_agent('alice', 'jiuwenswarm') is None
        router._registry.unregister_agent.assert_awaited_once()
        assert (await router.send_request(_request(rid='recovered'))).ok
        assert len(clients) == 2 and len(router.test_api.creates) == 2
        assert all(
            body['mounts'] == router.test_api.creates[0]['mounts'] for body in router.test_api.creates
        )
    finally:
        await router.shutdown()


@pytest.mark.parametrize('url', ['ftp://adx.example', 'https://user:secret@adx.example', 'https://adx.example/path',
                                  'https://adx.example?token=x', 'https://adx.example/#fragment',
                                  'http://127.0.0.1', 'http://adx.example'])
def test_invalid_ingress_origins_fail_at_startup(monkeypatch, url):
    with pytest.raises(ValueError):
        _config(monkeypatch, ingress_url=url)


def test_loopback_http_opt_in_excludes_remote_origins(monkeypatch):
    with pytest.raises(ValueError, match='requires HTTPS'):
        _config(monkeypatch, ingress_url='http://adx.example', allow_loopback_http=True)
    assert _config(monkeypatch, ingress_url='http://[::1]', allow_loopback_http=True).ingress_url == 'http://[::1]'


@pytest.mark.parametrize('ingress_url', ['https://adx.example', 'http://127.0.0.1:8080'])
async def test_registry_launch_spec_is_forwarded_to_environment_without_template_registration(monkeypatch, ingress_url):
    from agent_gateway.extensions.agentos.agentos_router.registry_client import ImageInfo

    router, _clients = _router(_config(monkeypatch, ingress_url=ingress_url, allow_loopback_http=True,
                                       builtin_image='registry.example/builtin:1'))
    spec = {'rootfs': {'imageurl': 'registry.example/third:2', 'user': '10001:10001', 'ports': ['tcp:2222']},
            'cmds': [['/app/worker', '--serve']], 'code_path': '/work', 'cpu': 500, 'memory': 1024,
            'probes': {'startup': {'tcpSocket': {'port': 2222}, 'failureThreshold': 3}}}
    router._registry.get_image_info = AsyncMock(return_value=ImageInfo(
        image_name='third', image_uri='registry.example/third:2',
        metadata={'runtime_spec': spec, 'web_port': 8080, 'env_vars': {'MODEL_ENDPOINT': 'http://model/v1'}}))
    await router.connect('')
    try:
        runtime = await router._agent_manager.get_or_create_agent('alice', 'third', creator=router._create_agent)
        body = router.test_api.creates[0]
        assert runtime.info.sandbox_id == f"{body['namespace']}-{body['name']}"
        assert body['rootfs']['imageurl'] == 'registry.example/third:2'
        assert body['process'] == {'commands': spec['cmds'], 'user': '10001:10001', 'cwd': '/work'}
        assert body['cpu'] == 500 and body['memory'] == 1024
        env = dict(body['env'])
        generation = env.pop(AGENT_GENERATION_ENV)
        assert body['name'] == f'alice+third-{generation[:12]}'
        assert env == {'MODEL_ENDPOINT': 'http://model/v1'}
        assert Path(body['mounts'][0]['host_path']).name == 'alice'
        assert body['readinessProbe']['tcpSocket']['port'] == 2222
        assert {'2222', '8080'} <= set(body['ports'])
        assert body['dataPlane']['portForwardSecurityMode'] == 'tls'
        assert all('/api/agent/' not in request.url.path for request in router.test_api.requests)
    finally:
        await router.shutdown()


@pytest.mark.parametrize('existing', [False, True])
async def test_native_placement_is_available_to_strict_registry_on_first_post(monkeypatch, existing):
    from agent_gateway.extensions.agentos.agentos_router.models import AgentInfo
    from agent_gateway.extensions.agentos.agentos_router.registry_client import RegistryClient, RegistryConfig

    api = EnvironmentAPI()
    identifier = 'test-alice+jiuwenswarm'
    placement = {'id': identifier, 'tenant_id': 'appliance', 'status': 'running',
                 'node_ip': '172.30.58.2', 'sandbox_ip': '100.96.201.131'}
    if existing:
        api.environments[identifier] = dict(placement)

    def environment(request):
        response = api.handle(request)
        if request.method == 'POST':
            api.environments[identifier].update(placement)
        return response

    registered = []

    def strict_registry(request):
        body = json.loads(request.content)
        assert body['node'] == placement['node_ip'], 'Registry requires the real node at initial registration'
        assert body['address'] == placement['sandbox_ip']
        assert body['instance_id'] == identifier
        registered.append(body)
        return httpx.Response(200, json={**body, 'status': '运行'})

    control = AdxControlClient(_config(monkeypatch), transport=httpx.MockTransport(environment))
    registry = RegistryClient(RegistryConfig(endpoint='http://registry.example'))
    registry._http = httpx.AsyncClient(base_url='http://registry.example', transport=httpx.MockTransport(strict_registry))
    try:
        sandbox = await control.create_sandbox(namespace='test', name='alice+jiuwenswarm',
            workspace='/home/agentos/alice', runtime_spec={'rootfs': {'imageurl': 'agentos-supervisor:v1'}})
        await registry.register_agent(AgentInfo(user_id='alice', agent_type='jiuwenswarm',
            sandbox_id=sandbox.sandbox_id, metadata={'sandbox': sandbox.metadata}))
        assert len(registered) == 1
    finally:
        await control.close()
        await registry.close()


async def test_config_request_retains_adx_warmup_failure(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router.agent_manager import AgentRuntime

    router, _ = _router(_config(monkeypatch))
    await router.connect('')
    started = asyncio.Event()
    release_create = asyncio.Event()
    joined = asyncio.Event()
    release_waiter = asyncio.Event()
    wait = AgentRuntime.wait_until_settled
    request = router._control.http.request
    detail = 'requested cgroup v1 backend is unavailable'

    async def create(method, url, **kwargs):
        if method == 'POST' and url.endswith('/environments'):
            started.set()
            await release_create.wait()
            return httpx.Response(409, json={
                'message': detail,
                'error': {'code': 'CONFLICT', 'outcome': 'terminal', 'requestId': 'create-test'},
            })
        return await request(method, url, **kwargs)

    async def wait_for_create(runtime, timeout):
        joined.set()
        await wait(runtime, timeout)
        await release_waiter.wait()

    monkeypatch.setattr(router._control.http, 'request', create)
    monkeypatch.setattr(AgentRuntime, 'wait_until_settled', wait_for_create)
    warmup = asyncio.create_task(router._warmup_user_agent('alice'))
    envelope = _request()
    envelope.method = 'config.get'
    try:
        await asyncio.wait_for(started.wait(), 2)
        pending = asyncio.create_task(router.send_request(envelope))
        await asyncio.wait_for(joined.wait(), 2)
        release_create.set()
        await warmup
        release_waiter.set()
        response = await pending
        assert not response.ok
        assert 'AGENT_CREATE_FAILED' in response.payload['error']
        assert detail in response.payload['error']
        assert 'create-test' in response.payload['error']
    finally:
        release_create.set()
        release_waiter.set()
        await router.shutdown()


async def test_adx_gateway_does_not_broadcast_its_tool_ingress_to_another_node(monkeypatch):
    router, _ = _router(_config(monkeypatch))
    router.set_or_update_server_config(config={"sandbox": {
        "enabled": True, "type": "adx-environment", "url": "https://master:8443",
        "startup_mode": "external", "image": "workspace:v1", "runtime": "conch",
        "rootfs": {"type": "local", "path": "/objects/workspace/rootfs"},
        "mounts": [{"host_path": "/opt/python", "target": "/opt/python", "options": ["ro"]}],
        "env": {"PATH": "/opt/python/bin:/usr/bin", "ADX_TOKEN": "not-for-tools", "JIUWENSWARM_ADX_NODE_CONTEXT_FILE": "/private-node.json"},
    }})
    await router.connect("")
    try:
        assert (await router.send_request(_request())).ok
        env = router.test_api.creates[0]["env"]
        assert router._server_config["sandbox"]["type"] == "adx-environment"
        assert router._server_config["sandbox"]["enabled"]
        deployment = json.loads(env["JIUWENSWARM_AGENTOS_SANDBOX_CONFIG"])
        assert "url" not in deployment
        assert "startup_mode" not in deployment
        assert "JIUWENSWARM_ADX_NODE_INGRESS_URL" not in env
        assert deployment == managed_sandbox_settings()
    finally:
        await router.shutdown()


@pytest.mark.parametrize("status", ["failed", "missing"])
async def test_sandbox_probe_reclaims_terminal_environment(monkeypatch, status):
    router, _ = _router(_config(monkeypatch))
    await router.connect("")
    try:
        assert (await router.send_request(_request())).ok
        instance = next(iter(router.test_api.environments))
        router._agent_manager.increment_user_connections("alice")
        if status == "missing":
            router.test_api.environments.pop(instance)
        else:
            router.test_api.environments[instance]["status"] = status

        scheduled = []
        monkeypatch.setattr(
            router._router, "_schedule_connect_warmup",
            lambda user_id, *, require_online=False: scheduled.append(
                (user_id, require_online)
            ),
        )

        # The probe uses the same 1s idle grace as disconnect cleanup.
        await asyncio.sleep(1.1)
        assert await router._router._probe_ready_sandboxes_once() == 1
        assert await router._agent_manager.get_agent("alice", "jiuwenswarm") is None
        assert scheduled == [("alice", True)]

        assert (await router.send_request(_request(rid="after-probe"))).ok
        assert len(router.test_api.creates) == 2
    finally:
        await router.shutdown()


async def test_loader_registers_one_canonical_router_for_adx_backend(monkeypatch):
    from agent_gateway.extensions.agentos.agentos_router import extension
    from agent_gateway.extensions.loader import ExtensionLoader

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
