# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Worker-owned dynamic instances over the public ADX Environment lifecycle API.

The control plane under test is the real ``AdxControlClient``/``AdxControl``
on an in-process ``httpx.MockTransport`` that speaks the Environment wire
format. Only the registry is replaced, to gate and fail registration.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx
import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.instance_routes import InstanceProviderSpec
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.extensions.adx import instance_backend as module
from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.instance_backend import WorkerInstanceBackend
from agent_gateway.extensions.adx.router_client import _AdxManagedRouter, adx_sandbox_name
from agent_gateway.extensions.agentos.agentos_router.agent_manager import (
    AgentCreateFailed, AgentPreCreateError, AgentRebuildPending,
)
from agent_gateway.extensions.agentos.agentos_router.backend import (
    AGENT_GENERATION_ENV, TRACE_ID_HEADER, deterministic_agent_instance_id,
)
from agent_gateway.extensions.agentos.agentos_router.models import AgentStatus
from agent_gateway.extensions.agentos.agentos_router.registry_client import RegistryConfig
from agent_gateway.southbound.orchestration.instance_provider import TargetLease
from agent_gateway.southbound.orchestration.agent_manager import AgentCreateFailed
from tests.unit_tests.gateway.instance_fakes import BrokenRegistry, EnvironmentAPI, FakeRegistry


# Environment lifecycle values a leaf supplies itself; ADX adds its own binding.
_LEAF_ENV = (
    'JIUWENSWARM_USER_DIRECTORY', 'JIUWENSWARM_RUNTIME_PROFILE', 'TOOL_SANDBOX_IDLE_TIMEOUT',
    'TOOL_SANDBOX_CPU', 'TOOL_SANDBOX_MEMORY', AGENT_GENERATION_ENV,
)


@pytest.fixture(autouse=True)
def _deployment_env(monkeypatch):
    """Env seeds for probes and resources must not change what a test observes."""
    from agent_gateway.common.agentos_runtime import RUNTIME_PROFILE_ENV
    from agent_gateway.extensions.agentos.agentos_router import config as router_config

    names = [RUNTIME_PROFILE_ENV, *(getattr(router_config, name) for name in dir(router_config)
                                    if name.endswith('_ENV'))]
    for name in names:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def managed_root(tmp_path: Path) -> Path:
    root = tmp_path / 'managed'
    root.mkdir()
    for user in ('alice', 'bob'):
        (root / user).mkdir()
    return root


def _config(monkeypatch, root: Path, *, adx: dict | None = None, agentos: dict | None = None) -> Any:
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    settings = {'ingress_url': 'https://adx.example', 'tenant': 'appliance', 'token_env': 'TEST_ADX_KEY',
                'connect_timeout_seconds': 2, **(adx or {})}
    worker = {'workspace_root': str(root), 'connect_warmup_enabled': False, **(agentos or {})}
    return load_router_config({'gateway': {
        'agent_client': {'type': 'agentos_router', 'agent_namespace': 'test'},
        'adx': settings, 'agentos': worker,
    }})


def _spec(root: Path, **overrides) -> InstanceProviderSpec:
    value: dict[str, Any] = dict(
        identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37),
        admitted_users=('alice', 'bob'),
        frontend_endpoint='https://adx.example',
        namespace='test',
        workspace_root=str(root),
        cpu=4,
        memory=8192,
        agent_port=18092,
    )
    value.update(overrides)
    return InstanceProviderSpec(**value)


@pytest.fixture
async def leaf(monkeypatch):
    """Build leaves over the real ADX control plane; every backend is closed afterwards."""
    monkeypatch.setattr(module, 'RegistryClient', FakeRegistry)
    built: list[WorkerInstanceBackend] = []

    def build(root: Path, *, spec: InstanceProviderSpec | None = None, adx: dict | None = None,
              agentos: dict | None = None, ingress: str = 'https://adx.example',
              ) -> tuple[WorkerInstanceBackend, EnvironmentAPI]:
        api = EnvironmentAPI()
        config = _config(monkeypatch, root, adx={'ingress_url': ingress, **(adx or {})}, agentos=agentos)
        value = _spec(root, frontend_endpoint=ingress) if spec is None else spec
        backend = WorkerInstanceBackend(
            value, config, slot_id=0, transport=httpx.MockTransport(api.handle),
        )
        built.append(backend)
        return backend, api

    yield build
    for backend in built:
        await backend.close()


@pytest.fixture
def control_clients(monkeypatch) -> list[AdxControlClient]:
    created: list[AdxControlClient] = []

    class RecordingClient(AdxControlClient):
        def __init__(self, config, *, transport=None) -> None:
            super().__init__(config, transport=transport)
            created.append(self)

    monkeypatch.setattr(module, 'AdxControlClient', RecordingClient)
    return created


def _posts(api: EnvironmentAPI) -> list[tuple[str, str]]:
    return [(request.url.path, request.headers['x-request-id']) for request in api.posts]


# --------------------------------------------------------------------------- creation


@pytest.mark.parametrize('ingress', [
    'https://adx.example',
    pytest.param('http://127.0.0.1:8080', id='loopback'),
])
async def test_backend_creates_waits_registers_and_closes_without_delete(leaf, monkeypatch, managed_root, ingress):
    adx = {'allow_loopback_http': True} if ingress.startswith('http://') else {}
    backend, api = leaf(managed_root, adx=adx, ingress=ingress)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    # Management readiness is verified against the exact generation before READY.
    verified: list[tuple[str, str]] = []
    original = backend.control.verify_generation

    async def spy(sandbox, *, generation, deadline):
        verified.append((sandbox.sandbox_id, generation))
        return await original(sandbox, generation=generation, deadline=deadline)

    monkeypatch.setattr(backend.control, 'verify_generation', spy)
    issued: list[Any] = []
    create_request = backend.control.create

    async def record(request):
        issued.append(request)
        return await create_request(request)

    monkeypatch.setattr(backend.control, 'create', record)
    lease = await provider.acquire(route)

    generation = lease.agent_id
    instance_id = f'test-alice+jiuwenswarm-{generation[:12]}'
    assert instance_id == deterministic_agent_instance_id(
        'test', adx_sandbox_name('alice', 'jiuwenswarm', generation),
    )
    assert lease.target.generation == generation
    assert lease.target.physical_id == f'test:{instance_id}'
    # The data endpoint comes from the ADX control, never from the provider or the spec.
    assert lease.target.ws_url == backend.control.ws_url(instance_id, 18092)
    endpoint = urlsplit(lease.target.ws_url)
    if ingress.startswith('https://'):
        assert (endpoint.scheme, endpoint.netloc, endpoint.path) == ('wss', 'adx.example', '/agent/ws')
        # ADX decodes the query value and the instance segment of its target URN separately.
        target = quote('urn:adx:instance:' + quote(instance_id, safe=''), safe='')
        assert endpoint.query == f'target={target}&port=18092'
    else:
        assert (endpoint.scheme, endpoint.netloc, endpoint.path) == (
            'ws', '127.0.0.1:8080', f'/{quote(instance_id, safe="")}/18092',
        )
        assert not endpoint.query
    runtime = await provider.manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.info.status is AgentStatus.READY
    assert runtime.info.sandbox_id == instance_id
    assert runtime.info.metadata['physical_generation'] == generation
    assert runtime.info.metadata['agent_port'] == 18092

    # A later request reuses the READY runtime without provisioning or verifying again.
    second = await provider.acquire(route)
    await provider.release(second, outcome='finished')
    assert verified == [(instance_id, generation)]
    assert len(api.creates) == 1

    (create,) = api.creates
    workspace = str(Path(backend.spec.workspace_root).resolve() / 'alice')
    assert create['namespace'] == 'test'
    assert create['name'] == f'alice+jiuwenswarm-{generation[:12]}'
    assert create['cpu'] == 4 and create['memory'] == 8192
    assert create['mounts'] == [{
        'type': 'bind', 'host_path': workspace, 'target': '/home/agentos', 'options': ['rbind', 'rw'],
    }]
    assert create['ports'] == ['18092']
    assert create['readinessProbe']['tcpSocket'] == {'port': 18092}
    assert create['healthcheckProbe']['tcpSocket'] == {'port': 18092}
    assert create['process'] == {
        'commands': [['sh', '-c', 'exec jiuwenswarm-agentserver --port 18092']], 'user': 'agentos',
    }
    leaf_env = {name: create['env'][name] for name in _LEAF_ENV}
    assert leaf_env == {
        'JIUWENSWARM_USER_DIRECTORY': workspace,
        'JIUWENSWARM_RUNTIME_PROFILE': 'agentos',
        'TOOL_SANDBOX_IDLE_TIMEOUT': '600',
        'TOOL_SANDBOX_CPU': '2000',
        'TOOL_SANDBOX_MEMORY': '4096',
        AGENT_GENERATION_ENV: generation,
    }
    # The recorded launch specification is exactly what was sent, as a builtin Environment.
    recorded = runtime.info.metadata['runtime_spec']
    assert recorded == {
        'sandbox_type': 'supervisor',
        'runtime': 'python3.11',
        'rootfs': {'imageurl': 'jiuwenswarm-agent-runtime:latest', 'user': 'agentos'},
        'cmds': [['sh', '-c', 'exec jiuwenswarm-agentserver --port 18092']],
        'probes': {
            'startup': {
                'tcpSocket': {'port': 18092},
                'initialDelaySeconds': 3,
                'periodSeconds': 3,
                'timeoutSeconds': 2,
                'failureThreshold': 6,
            },
            'liveness': {'tcpSocket': {'port': 18092}, 'timeoutSeconds': 2, 'failureThreshold': 3},
        },
        'cpu': 4,
        'memory': 8192,
    }
    assert create == backend.client._creation_body(
        'test', create['name'], workspace, recorded, leaf_env, None, builtin=True,
    )
    assert create['rootfs']['runtime'] == 'runc' and 'ADX_TOKEN' in create['env']
    # The leaf declares a builtin Environment and asks for nothing it did not admit.
    (request,) = issued
    assert (request.namespace, request.name, request.workspace) == ('test', create['name'], workspace)
    assert request.builtin is True and request.web_port is None
    assert request.runtime_spec == recorded
    assert request.env_vars == leaf_env

    assert backend.registry.config == RegistryConfig(endpoint='')
    (registered,) = backend.registry.registered
    assert registered.sandbox_id == instance_id
    assert registered.user_id == 'alice' and registered.agent_type == 'jiuwenswarm'
    assert registered.metadata['sandbox'] == {'node': '10.0.0.5', 'address': '10.0.0.6'}

    await provider.release(lease, outcome='finished')
    registry, client = backend.registry, backend.client
    await backend.close()
    assert registry.closed and client.http.is_closed
    assert api.deletes == []


async def test_environment_uses_the_deployment_builtin_binding(leaf, managed_root):
    launch = {
        'process': {'commands': [['/opt/agent/server', '--port', '18092']]},
        'image_env': {'DEPLOY_PROFILE': 'image'},
    }
    backend, api = leaf(
        managed_root, adx={'builtin_image': 'registry.example/agentserver:1', 'builtin_launch': launch},
    )
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))

    (create,) = api.creates
    assert create['rootfs']['imageurl'] == 'registry.example/agentserver:1'
    assert create['process'] == launch['process']
    assert create['env']['DEPLOY_PROFILE'] == 'image'
    assert create['env'][AGENT_GENERATION_ENV] == lease.agent_id
    assert create['mounts'] == [{
        'type': 'bind', 'host_path': str(Path(backend.spec.workspace_root).resolve() / 'alice'),
        'target': '/home/agentos', 'options': ['rbind', 'rw'],
    }]
    assert parse_qs(urlsplit(lease.target.ws_url).query)['port'] == ['18092']
    assert lease.target.physical_id == f"test:{create['namespace']}-{create['name']}"


async def test_unset_spec_resources_follow_the_worker_deployment_settings(leaf, monkeypatch, managed_root):
    monkeypatch.setenv('AGENTOS_BUILTIN_AGENT_CPU', '1500')
    monkeypatch.setenv('AGENTOS_BUILTIN_AGENT_MEMORY', '3000')
    monkeypatch.setenv('TOOL_SANDBOX_CPU', '777')
    backend, api = leaf(
        managed_root, spec=_spec(managed_root, cpu=None, memory=None, agent_port=None),
    )
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))

    (create,) = api.creates
    assert create['cpu'] == 1500 and create['memory'] == 3000
    assert create['env']['TOOL_SANDBOX_CPU'] == '777'
    assert create['ports'] == ['18092']
    assert create['process']['commands'] == [['sh', '-c', 'exec jiuwenswarm-agentserver --port 18092']]
    assert parse_qs(urlsplit(lease.target.ws_url).query)['port'] == ['18092']
    runtime = await provider.manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.info.metadata['agent_port'] == 18092
    assert (runtime.info.metadata['runtime_spec']['cpu'], runtime.info.metadata['runtime_spec']['memory']) == (
        1500, 3000,
    )


async def test_platform_credential_is_sent_to_the_ingress_only(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    assert lease is not None
    (create,) = api.creates
    # ADX binds the node credential into its builtin Environment; that is the request's business.
    assert create['env']['ADX_TOKEN'] == 'platform-secret'
    assert {request.url.host for request in api.requests} == {'adx.example'}
    # The leaf never copies it into its spec, leases, targets, runtime metadata or registry record.
    assert 'platform-secret' not in backend.spec.to_json()
    assert 'platform-secret' not in repr(lease)
    assert 'platform-secret' not in repr(lease.target)
    assert 'platform-secret' not in repr(lease.runtime.info.metadata)
    assert 'ADX_TOKEN' not in repr(lease.runtime.info.metadata)
    assert 'platform-secret' not in repr(backend.registry.registered[0].metadata)


def test_business_and_worker_routers_name_a_generation_identically():
    generation = '0123456789abcdef0123456789abcdef'
    router = object.__new__(_AdxManagedRouter)
    assert router._sandbox_physical_name(
        SimpleNamespace(user_id='alice', agent_type='jiuwenswarm', agent_id=generation),
    ) == adx_sandbox_name('alice', 'jiuwenswarm', generation) == f'alice+jiuwenswarm-{generation[:12]}'


# --------------------------------------------------------------------------- admission


async def test_backend_rejects_missing_workspace_before_create(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    (Path(backend.spec.workspace_root) / 'alice').rmdir()

    with pytest.raises(AgentPreCreateError):
        await provider.acquire(provider.validate_owner('alice'))

    assert api.requests == [] and backend.registry.registered == []
    await backend.close()


async def test_backend_rejects_workspace_symlink_escape(leaf, monkeypatch, tmp_path):
    managed_root, outside = tmp_path / 'managed', tmp_path / 'outside'
    managed_root.mkdir()
    outside.mkdir()
    (managed_root / 'bob').mkdir()
    (managed_root / 'alice').symlink_to(outside, target_is_directory=True)
    backend, api = leaf(managed_root)

    provider = await backend.start()
    with pytest.raises(AgentPreCreateError, match='not a direct managed child'):
        await provider.acquire(provider.validate_owner('alice'))

    assert api.requests == []
    assert outside.is_dir()


async def test_backend_does_not_create_managed_root(leaf, tmp_path):
    missing_root = tmp_path / 'missing'
    backend, api = leaf(missing_root)

    provider = await backend.start()
    with pytest.raises(AgentPreCreateError, match='does not exist'):
        await provider.acquire(provider.validate_owner('alice'))

    assert not missing_root.exists()
    assert api.requests == []


@pytest.mark.parametrize('failure', ['uninitialized', 'migration'])
async def test_workspace_deployment_admission_precedes_any_request_and_recovers(
    leaf, monkeypatch, managed_root, failure,
):
    from agent_gateway.extensions.adx.workspace import MIGRATION_ERROR_FILE

    monkeypatch.setenv('JIUWENSWARM_RUNTIME_PROFILE', 'agentos')
    backend, api = leaf(managed_root)
    provider = await backend.start()
    folder = managed_root / 'alice/.jiuwenswarm/config'
    folder.mkdir(parents=True)
    (folder / 'config.yaml').write_text('sandbox: {type: adx-environment}\n')
    if failure == 'migration':
        (folder / MIGRATION_ERROR_FILE).write_text(
            json.dumps({'reason': 'sandbox.files requires explicit migration'}),
        )
    else:
        (folder / 'config.yaml').unlink()
    route = provider.validate_owner('alice')

    with pytest.raises(AgentPreCreateError, match='WORKSPACE_CONFIG_INVALID') as error:
        await provider.acquire(route)

    assert ('sandbox.files' if failure == 'migration'
            else 'application workspace initialization required') in str(error.value)
    assert api.requests == []
    assert await provider.manager.get_agent('alice', 'jiuwenswarm') is None
    # A later successful deployment admits the user without restarting this Worker.
    (folder / 'config.yaml').write_text('sandbox: {type: adx-environment}\n')
    (folder / MIGRATION_ERROR_FILE).unlink(missing_ok=True)
    lease = await provider.acquire(route)
    assert lease.target.physical_id == f"test:{api.creates[0]['namespace']}-{api.creates[0]['name']}"


# --------------------------------------------------------------------------- uncertain outcomes


@pytest.mark.parametrize('created_before_loss', [False, True], ids=['not-created', 'created'])
async def test_uncertain_create_replays_same_generation_and_physical_name(leaf, managed_root, created_before_loss):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.append('lost_created' if created_before_loss else 'lost')

    with pytest.raises(AgentRebuildPending, match='create result is unknown'):
        await provider.acquire(route)
    with pytest.raises(AgentRebuildPending, match='logical identity was recovered'):
        await provider.acquire(route)
    lease = await provider.acquire(route)

    (first, *replays) = api.creates
    # An Environment that may already exist is looked up, never created twice.
    assert [body['name'] for body in replays] == [first['name']] * (0 if created_before_loss else 1)
    operations = [request.headers['x-request-id'] for request in api.posts]
    assert len(operations) == len(api.creates) and len(set(operations)) == 1
    assert first['name'] == adx_sandbox_name('alice', 'jiuwenswarm', lease.target.generation)
    assert lease.target.generation != lease.agent_id
    assert lease.target.physical_id == f"test:{first['namespace']}-{first['name']}"
    assert backend._lifecycles['alice+jiuwenswarm'].attempt is None
    runtime = await provider.manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.info.agent_id == lease.agent_id
    assert runtime.info.metadata['physical_generation'] == lease.target.generation
    assert api.deletes == []
    assert len(backend.registry.registered) == 1


async def test_replayed_name_owned_by_another_generation_is_never_deleted(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.append('lost_created')

    with pytest.raises(AgentRebuildPending, match='create result is unknown'):
        await provider.acquire(route)
    (first,) = api.creates
    foreign = api.environments[f"test-{first['name']}"]
    foreign['env_vars'][AGENT_GENERATION_ENV] = '0' * 32

    with pytest.raises(AgentRebuildPending, match='generation did not match'):
        await provider.acquire(route)
    assert len(api.posts) == 1 and api.deletes == []
    lease = await provider.acquire(route)

    # Ownership could not be established, so the next request minted its own generation.
    second, = api.creates[1:]
    assert second['name'] != first['name']
    assert lease.target.generation == second['env'][AGENT_GENERATION_ENV]
    assert lease.target.generation != first['env'][AGENT_GENERATION_ENV]
    assert api.environments[f"test-{first['name']}"] is foreign
    assert foreign['env_vars'][AGENT_GENERATION_ENV] == '0' * 32
    assert api.deletes == []


async def test_concurrent_waiter_recovers_uncertain_attempt_without_duplicate_post(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')

    async def attempt_with_waiter(outcome: str) -> list[Any]:
        api.arm(outcome)
        owner = asyncio.create_task(provider.acquire(route))
        await api.post_started.wait()
        waiter = asyncio.create_task(provider.acquire(route))
        await asyncio.sleep(0)
        api.post_gate.set()
        return await asyncio.gather(owner, waiter, return_exceptions=True)

    assert all(isinstance(result, AgentRebuildPending) for result in await attempt_with_waiter('lost'))
    owner, recovered = await attempt_with_waiter('commit')
    assert isinstance(owner, AgentRebuildPending)
    assert type(recovered) is TargetLease
    waiter_lease = await provider.acquire(route)

    generations = [body['env'][AGENT_GENERATION_ENV] for body in api.creates]
    assert len(generations) == 2 and generations[0] == generations[1]
    assert recovered.agent_id != generations[0]
    assert waiter_lease.agent_id == recovered.agent_id
    assert {recovered.target.generation, waiter_lease.target.generation} == {generations[0]}
    assert len(backend.registry.registered) == 1
    await provider.release(recovered, outcome='finished')
    await provider.release(waiter_lease, outcome='finished')
    assert api.deletes == []


async def test_registry_failure_does_not_install_ready_and_keeps_attempt(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.append('lost')
    with pytest.raises(AgentRebuildPending):
        await provider.acquire(route)

    backend.registry.fail_registration = RuntimeError('registry registration failed')
    with pytest.raises(AgentRebuildPending, match='registry registration failed'):
        await provider.acquire(route)
    assert await provider.manager.get_agent('alice', 'jiuwenswarm') is None

    backend.registry.fail_registration = None
    with pytest.raises(AgentRebuildPending, match='logical identity was recovered'):
        await provider.acquire(route)
    lease = await provider.acquire(route)
    assert len(api.creates) == 2
    assert len(backend.registry.registered) == 1
    assert lease.target.generation != lease.agent_id
    assert api.deletes == []


async def test_rate_limited_create_keeps_its_generation_and_deletes_nothing(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.append((429, 'RATE_LIMITED'))

    with pytest.raises(AgentRebuildPending, match='rate limited'):
        await provider.acquire(route)
    with pytest.raises(AgentRebuildPending, match='logical identity was recovered'):
        await provider.acquire(route)
    lease = await provider.acquire(route)

    assert [body['name'] for body in api.creates] == [api.creates[0]['name']] * 2
    assert lease.target.generation != lease.agent_id
    assert api.deletes == []


async def test_failed_environment_is_retired_by_its_exact_id_before_a_new_generation(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.create_status = 'failed'

    with pytest.raises(AgentRebuildPending, match='creation was rejected'):
        await provider.acquire(route)
    (failed,) = api.creates
    failed_id = f"test-{failed['name']}"
    (delete,) = [request for request in api.requests if request.method == 'DELETE']
    assert delete.url.raw_path == f'/api/environment/v1/environments/{quote(failed_id, safe="")}'.encode()
    assert delete.headers['x-request-id'].startswith('delete-')
    assert api.deletes == [(failed_id, delete.headers['x-request-id'])]

    api.create_status = 'running'
    lease = await provider.acquire(route)
    assert lease.target.physical_id != f'test:{failed_id}'
    assert list(api.environments) == [lease.target.physical_id.removeprefix('test:')]
    assert [instance for instance, _ in api.deletes] == [failed_id]
    assert len(backend.registry.registered) == 1


async def test_repeated_unknown_outcomes_retire_the_instance_once_and_fail_closed(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.extend([(503, 'UNAVAILABLE')] * 4)

    for _ in range(3):
        with pytest.raises(AgentRebuildPending, match='create result is unknown'):
            await provider.acquire(route)
    with pytest.raises(AgentCreateFailed, match='stayed uncertain after 4 attempts'):
        await provider.acquire(route)
    (first, *replays) = api.creates
    assert {body['name'] for body in replays} == {first['name']}
    operations = [request.headers['x-request-id'] for request in api.posts]
    assert len(operations) == 4 and len(set(operations)) == 1
    assert [instance for instance, _ in api.deletes] == [f"test-{first['name']}"]

    # A permanently uncertain name is never retried by a later request.
    with pytest.raises(AgentCreateFailed):
        await provider.acquire(route)
    assert len(api.creates) == 4 and len(api.deletes) == 1


# --------------------------------------------------------------------------- shutdown


async def test_close_during_registration_cancels_without_ready_or_registry(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.post_plan.append('lost')
    with pytest.raises(AgentRebuildPending):
        await provider.acquire(route)

    backend.registry.register_gate.clear()
    acquisition = asyncio.create_task(provider.acquire(route))
    await backend.registry.register_started.wait()
    registry = backend.registry
    await backend.close()
    with pytest.raises(asyncio.CancelledError):
        await acquisition

    assert await provider.manager.get_agent('alice', 'jiuwenswarm') is None
    assert registry.registered == []
    assert api.deletes == []


@pytest.mark.parametrize('created_before_reply', [False, True], ids=['not-created', 'created'])
async def test_startup_deadline_during_create_keeps_its_attempt_for_an_idempotent_replay(
    leaf, managed_root, created_before_reply,
):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    route = provider.validate_owner('alice')
    api.arm(created_before_reply=created_before_reply)

    # The routed caller bounds acquisition; a slow Environment start must not fail the user.
    with pytest.raises(TimeoutError):
        await provider.acquire(route, deadline=asyncio.get_running_loop().time() + 0.05)

    lifecycle = backend._lifecycles['alice+jiuwenswarm']
    attempt = lifecycle.attempt
    assert attempt is not None
    physical = adx_sandbox_name('alice', 'jiuwenswarm', attempt.generation)
    instance_id = deterministic_agent_instance_id('test', physical)
    assert attempt.deterministic_id == instance_id
    assert [body['name'] for body in api.creates] == [physical]
    assert (instance_id in api.environments) is created_before_reply
    assert await provider.manager.get_agent('alice', 'jiuwenswarm') is None

    # The next request replays that name: the same operation, or the Environment it finds.
    api.post_gate.set()
    with pytest.raises(AgentRebuildPending, match='logical identity was recovered'):
        await provider.acquire(route)
    lease = await provider.acquire(route)
    assert [body['name'] for body in api.creates] == [physical] * (1 if created_before_reply else 2)
    assert len({request.headers['x-request-id'] for request in api.posts}) == 1
    assert lease.target.generation == attempt.generation != lease.agent_id
    assert lease.target.physical_id == f'test:{instance_id}'
    assert list(api.environments) == [instance_id]
    assert len(backend.registry.registered) == 1
    assert api.deletes == []


async def test_backend_constructor_failure_closes_control_client_and_allows_retry(
    leaf, monkeypatch, managed_root, control_clients,
):
    backend, _ = leaf(managed_root)
    monkeypatch.setattr(module, 'RegistryClient', BrokenRegistry)

    with pytest.raises(RuntimeError, match='registry constructor failed'):
        await backend.start()

    assert len(control_clients) == 1 and control_clients[0].http.is_closed
    assert backend.registry is None and backend.manager is None and backend._provider is None

    monkeypatch.setattr(module, 'RegistryClient', FakeRegistry)
    provider = await backend.start()
    assert len(control_clients) == 2 and not control_clients[1].http.is_closed
    assert provider is backend._provider


async def test_backend_refuses_a_non_default_agent_key_model(leaf, managed_root, control_clients):
    backend, api = leaf(managed_root, agentos={'agent_key_fields': ['user_id', 'agent_type', 'session_id']})

    with pytest.raises(ValueError, match='default-key manager'):
        await backend.start()

    assert len(control_clients) == 1 and control_clients[0].http.is_closed
    assert api.requests == []


async def test_start_is_single_shot_and_close_is_idempotent(leaf, managed_root):
    backend, _ = leaf(managed_root)
    await backend.close()
    with pytest.raises(RuntimeError, match='not startable'):
        await backend.start()

    backend, _ = leaf(managed_root)
    provider = await backend.start()
    with pytest.raises(RuntimeError, match='not startable'):
        await backend.start()
    await backend.close()
    await backend.close()
    with pytest.raises(RuntimeError, match='closed'):
        await provider.acquire(provider.validate_owner('alice'))


def test_backend_requires_exact_spec_config_and_manifest_slot(monkeypatch, managed_root):
    config = _config(monkeypatch, managed_root)
    spec = _spec(managed_root)
    for bad_spec, bad_config, slot in [
        (object(), config, 0), (spec, object(), 0), (spec, config, True),
        (spec, config, 1), (spec, config, '0'),
    ]:
        with pytest.raises(ValueError):
            WorkerInstanceBackend(bad_spec, bad_config, slot_id=slot)


@pytest.mark.parametrize('field', ['frontend_endpoint', 'namespace', 'workspace_root', 'registry_endpoint'])
def test_backend_refuses_a_spec_that_disagrees_with_the_worker_config(monkeypatch, tmp_path, field):
    root = tmp_path / 'managed'
    root.mkdir()
    agentos = {'registry': {'endpoint': 'http://registry.example'}} if field == 'registry_endpoint' else {}
    config = _config(monkeypatch, root, agentos=agentos)
    other = {
        'frontend_endpoint': 'https://other.example',
        'namespace': 'other',
        'workspace_root': str(tmp_path / 'elsewhere'),
        'registry_endpoint': '',
    }[field]

    with pytest.raises(ValueError, match=f'configuration: {field}'):
        WorkerInstanceBackend(_spec(root, **{field: other}), config, slot_id=0)

    agreeing = {'registry_endpoint': 'http://registry.example'} if field == 'registry_endpoint' else {}
    WorkerInstanceBackend(_spec(root, frontend_endpoint='https://adx.example/', **agreeing), config, slot_id=0)


def test_spec_may_name_the_configured_root_through_a_symlink(monkeypatch, tmp_path):
    root, alias = tmp_path / 'managed', tmp_path / 'users'
    root.mkdir()
    alias.symlink_to(root, target_is_directory=True)

    # The same directory, however it is spelled, manages the same workspaces.
    WorkerInstanceBackend(_spec(alias), _config(monkeypatch, root), slot_id=0)


# --------------------------------------------------------------------------- data-path hooks


async def test_each_dial_takes_a_fresh_ingress_credential(leaf, managed_root):
    backend, api = leaf(managed_root)
    with pytest.raises(RuntimeError, match='not started'):
        backend.dial_headers()
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    first, second = backend.dial_headers(), backend.dial_headers()
    assert first is not second and first['Authorization'] == 'Bearer platform-secret'
    # The trace carrier the control plane binds, so a dial can be followed in ADX.
    assert first[TRACE_ID_HEADER]
    first['Authorization'] = 'changed by the caller'
    assert backend.dial_headers()['Authorization'] == 'Bearer platform-secret'
    # It reaches the control plane and the ingress only: no lease, target, record or spec carries it.
    assert 'platform-secret' not in repr(lease) + repr(lease.target) + repr(lease.runtime.info.metadata)
    assert 'platform-secret' not in backend.spec.to_json()
    assert 'platform-secret' not in repr(backend.registry.registered[0].metadata)
    assert {request.url.host for request in api.requests} == {'adx.example'}
    await backend.close()
    with pytest.raises(RuntimeError, match='not started'):
        backend.dial_headers()


def _write_model_config(user_root: Path, api_base: str) -> None:
    config = user_root / '.jiuwenswarm' / 'config'
    config.mkdir(parents=True, exist_ok=True)
    (config / 'config.yaml').write_text(f'models:\n  primary:\n    api_base: {api_base}\n')


def _network_deployment(root: Path) -> dict:
    """A deployment whose builtin Environments get a network policy from the workspace."""
    for user in ('alice', 'bob'):
        _write_model_config(root / user, 'https://model.example/v1')
    return {
        'builtin_image': 'registry.example/agentserver:1',
        'builtin_launch': {
            'process': {'commands': [['/opt/agent/server', '--port', '18092']]},
            'network': {'ingress_url': 'https://ingress.example'},
        },
    }


async def test_network_admission_is_skipped_unless_the_deployment_configures_one(leaf, managed_root):
    backend, api = leaf(managed_root)
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    before = len(api.requests)
    # No model configuration exists in this workspace: nothing is even looked up.
    await backend.admit_network(lease.target)
    assert len(api.requests) == before and api.networks == []


async def test_network_admission_reconciles_the_leased_environment_with_its_workspace(leaf, managed_root):
    backend, api = leaf(managed_root, adx=_network_deployment(managed_root))
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    # Creation carried the workspace's policy: an unchanged one is admitted without a request.
    await backend.admit_network(lease.target)
    assert api.networks == []
    (create,) = api.creates
    assert 'model.example' in json.dumps(create['network'])
    # The user's model configuration changed after the Environment started.
    _write_model_config(managed_root / 'alice', 'https://other-model.example/v1')
    await backend.admit_network(lease.target)
    ((instance, policy),) = api.networks
    assert instance == lease.runtime.info.sandbox_id == lease.target.physical_id.split(':', 1)[1]
    assert 'other-model.example' in json.dumps(policy) and 'model.example/' not in json.dumps(policy)
    # Once per policy: the next request of the same generation asks for nothing.
    await backend.admit_network(lease.target)
    assert len(api.networks) == 1
    # Another route reconciles its own Environment once its own configuration changes.
    bob = await provider.acquire(provider.validate_owner('bob'))
    await provider.release(bob, outcome='finished')
    await backend.admit_network(bob.target)
    assert [item[0] for item in api.networks] == [instance]
    _write_model_config(managed_root / 'bob', 'https://bob-model.example/v1')
    await backend.admit_network(bob.target)
    assert [item[0] for item in api.networks] == [instance, bob.runtime.info.sandbox_id]
    assert 'bob-model.example' in json.dumps(api.networks[1][1])
    assert 'other-model.example' not in json.dumps(api.networks[1][1])
    await provider.release(lease, outcome='finished')
    assert {request.url.host for request in api.requests} == {'adx.example'}
    assert api.deletes == []


async def test_network_admission_refuses_a_generation_that_is_no_longer_current(leaf, managed_root):
    backend, api = leaf(managed_root, adx=_network_deployment(managed_root))
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    await provider.manager.pop_agent('alice', 'jiuwenswarm', expected_agent_id=lease.agent_id)
    _write_model_config(managed_root / 'alice', 'https://other-model.example/v1')
    before = len(api.requests)
    with pytest.raises(RuntimeError, match='stale'):
        await backend.admit_network(lease.target)
    assert len(api.requests) == before and api.networks == []
    with pytest.raises(ValueError, match='instance target'):
        await backend.admit_network(lease.route)
    with pytest.raises(ValueError, match='instance target'):
        await backend.admit_network(object())
    await backend.close()
    with pytest.raises(RuntimeError, match='not started'):
        await backend.admit_network(lease.target)


async def test_a_refused_network_policy_is_reported_and_not_remembered_as_applied(leaf, managed_root):
    backend, api = leaf(managed_root, adx=_network_deployment(managed_root))
    provider = await backend.start()
    lease = await provider.acquire(provider.validate_owner('alice'))
    _write_model_config(managed_root / 'alice', 'https://other-model.example/v1')
    api.network_reply = 403
    with pytest.raises(AgentCreateFailed, match='AGENT_NETWORK_UPDATE_FAILED') as refused:
        await backend.admit_network(lease.target)
    assert type(refused.value.__cause__).__name__ == 'AdxControlError'
    assert refused.value.__cause__.code == 'PERMISSION_DENIED'
    assert len(api.networks) == 1
    # Nothing was applied, so the next admission asks again instead of assuming success.
    api.network_reply = None
    await backend.admit_network(lease.target)
    assert len(api.networks) == 2
