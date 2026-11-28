# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""One Worker's dynamic-instance southbound, assembled and used end to end.

The ADX control plane is the real ``AdxControlClient``/``AdxControl`` on an
in-process ``httpx.MockTransport``; the ingress is a loopback stand-in for the
real ``WebSocketConnection`` engine; only the registry is replaced. This is an
in-process assembly test: no ADX deployment takes part.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import httpx
from e2a.models import E2AEnvelope
import pytest

from agent_gateway.application import worker_instance_southbound as module
from agent_gateway.application.worker_instance_southbound import build_instance_southbound
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.instance_routes import InstanceProviderSpec
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.extensions.adx import instance_backend
from agent_gateway.extensions.adx.config import load_router_config
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.instance_backend import WorkerInstanceBackend
from agent_gateway.extensions.agentos.agentos_router.backend import AGENT_GENERATION_ENV
from agent_gateway.southbound.orchestration.agent_manager import AgentCreateFailed
from tests.unit_tests.gateway.instance_fakes import EnvironmentAPI, FakeRegistry, InstanceFrontend

PORT = 18092


@pytest.fixture(autouse=True)
def _deployment_env(monkeypatch):
    """Env seeds for probes and resources must not change what a test observes."""
    from agent_gateway.common.agentos_runtime import RUNTIME_PROFILE_ENV
    from agent_gateway.extensions.agentos.agentos_router import config as router_config

    names = [RUNTIME_PROFILE_ENV, *(getattr(router_config, name) for name in dir(router_config)
                                    if name.endswith('_ENV'))]
    for name in names:
        monkeypatch.delenv(name, raising=False)


def _request(rid: str, user: str, method: str = 'session.list') -> E2AEnvelope:
    return E2AEnvelope(request_id=rid, user_id=user, channel='web', method=method, params={})


def _write_model_config(user_root: Path, api_base: str) -> None:
    config = user_root / '.jiuwenswarm' / 'config'
    config.mkdir(parents=True, exist_ok=True)
    (config / 'config.yaml').write_text(f'models:\n  primary:\n    api_base: {api_base}\n')


@pytest.fixture
async def stack(monkeypatch, tmp_path):
    """Deployments over the real control plane; everything built is closed afterwards."""
    monkeypatch.setenv('TEST_ADX_KEY', 'platform-secret')
    registries: list[Any] = []
    controls: list[AdxControlClient] = []

    class RecordingRegistry(FakeRegistry):
        def __init__(self, config: Any) -> None:
            super().__init__(config)
            registries.append(self)

    class RecordingControlClient(AdxControlClient):
        def __init__(self, config, *, transport=None) -> None:
            super().__init__(config, transport=transport)
            controls.append(self)

    monkeypatch.setattr(instance_backend, 'RegistryClient', RecordingRegistry)
    monkeypatch.setattr(instance_backend, 'AdxControlClient', RecordingControlClient)
    ingress = await InstanceFrontend().start()
    built: list[module.InstanceSouthbound] = []
    root = tmp_path / 'managed'
    for user in ('alice', 'bob'):
        (root / user).mkdir(parents=True)

    def deployment(*, network: bool = False) -> SimpleNamespace:
        api = EnvironmentAPI()
        # The ingress learns of a generation when the control plane creates it.
        api.on_create = ingress.adopt
        adx: dict[str, Any] = {
            'ingress_url': f'http://{ingress.netloc}', 'allow_loopback_http': True,
            'tenant': 'appliance', 'token_env': 'TEST_ADX_KEY', 'connect_timeout_seconds': 2,
        }
        if network:
            adx['builtin_image'] = 'registry.example/agentserver:1'
            adx['builtin_launch'] = {
                'process': {'commands': [['/opt/agent/server', '--port', str(PORT)]]},
                'network': {'ingress_url': 'https://ingress.example'},
            }
            for user in ('alice', 'bob'):
                _write_model_config(root / user, 'https://model.example/v1')
        config = load_router_config({'gateway': {
            'agent_client': {'type': 'agentos_router', 'agent_namespace': 'test'},
            'adx': adx, 'agentos': {'workspace_root': str(root), 'connect_warmup_enabled': False},
        }})
        spec = InstanceProviderSpec(
            identity_policy=LocalIdentityPolicy(domain='instances'),
            manifest=WorkerManifest((0,), config_version=37), admitted_users=('alice', 'bob'),
            frontend_endpoint=adx['ingress_url'], namespace='test', workspace_root=str(root),
            agent_port=PORT,
        )
        return SimpleNamespace(config=config, spec=spec, api=api, root=root)

    async def build(*, network: bool = False, slot_id: int = 0) -> tuple[module.InstanceSouthbound, Any]:
        deployed = deployment(network=network)
        southbound = await build_instance_southbound(
            deployed.spec, deployed.config, slot_id=slot_id,
            transport=httpx.MockTransport(deployed.api.handle),
        )
        built.append(southbound)
        return southbound, deployed

    yield SimpleNamespace(build=build, ingress=ingress, registries=registries, controls=controls)
    for item in built:
        await item.close()
    await ingress.stop()


# --------------------------------------------------------------------------- construction


async def test_construction_starts_the_leaf_and_binds_the_client_without_any_request(stack) -> None:
    southbound, deployed = await stack.build()
    client = southbound.client
    # Nothing is created or dialled before a request, not even by activation.
    assert client.server_ready is False and southbound.required_tasks == ()
    assert deployed.api.requests == [] and stack.ingress.dials == []
    with pytest.raises(RuntimeError, match='push handler'):
        await client.activate()
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()
    assert client.server_ready and deployed.api.creates == [] and stack.ingress.dials == []
    # The client is bound to this leaf's provider, policy and hooks.
    assert client.identity_policy is deployed.spec.identity_policy
    assert southbound.backend.spec is deployed.spec and southbound.backend.slot_id == 0
    assert southbound.backend.manager is client._provider.manager


async def test_a_leaf_that_cannot_start_leaves_nothing_behind(stack, monkeypatch) -> None:
    class BrokenRegistry:
        def __init__(self, config: Any) -> None:
            raise RuntimeError('registry constructor failed')

    monkeypatch.setattr(instance_backend, 'RegistryClient', BrokenRegistry)
    with pytest.raises(RuntimeError, match='registry constructor failed'):
        await stack.build()
    # The control client the leaf had opened is closed; nothing was dialled.
    (control,) = stack.controls
    assert control.http.is_closed and stack.ingress.dials == []


async def test_a_client_that_cannot_be_built_closes_the_started_leaf(stack, monkeypatch) -> None:
    def refuse(provider, **hooks):
        raise RuntimeError('client construction failed')

    monkeypatch.setattr(
        'agent_gateway.application.worker_instance_routed_client.WorkerInstanceRoutedClient', refuse,
    )
    with pytest.raises(RuntimeError, match='client construction failed'):
        await stack.build()
    # The leaf had started: its control client and registry are closed again.
    (control,) = stack.controls
    (registry,) = stack.registries
    assert control.http.is_closed and registry.closed
    assert stack.ingress.dials == []


# --------------------------------------------------------------------------- a request, end to end


async def test_a_request_creates_one_environment_and_dials_the_ingress_with_its_credential(stack) -> None:
    southbound, deployed = await stack.build()
    client = southbound.client
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()

    first = await client.send_request(_request('one', 'alice'))
    assert first.ok and first.payload == {'marker': 'alice'}
    (create,) = deployed.api.creates
    instance = f"{create['namespace']}-{create['name']}"
    # The dial is the ADX loopback ingress shape for exactly that Environment.
    ((user, dialled, headers),) = stack.ingress.dials
    assert user == 'alice' and dialled == instance
    assert stack.ingress.paths == [f'/{quote(instance, safe="")}/{PORT}']
    lower = {key.lower(): value for key, value in headers.items()}
    assert lower['authorization'] == 'Bearer platform-secret'
    assert create['env'][AGENT_GENERATION_ENV]
    # The Environment is registered for its owner, and the credential is not in the record.
    (record,) = stack.registries[0].registered
    assert record.user_id == 'alice' and record.sandbox_id == instance
    assert 'platform-secret' not in repr(record.metadata)

    # A second request of the same generation reuses the socket; another user gets its own.
    second = await client.send_request(_request('two', 'alice'))
    other = await client.send_request(_request('three', 'bob'))
    assert second.payload == {'marker': 'alice'} and other.payload == {'marker': 'bob'}
    assert len(deployed.api.creates) == 2 and [user for user, _, _ in stack.ingress.dials] == ['alice', 'bob']
    assert {request.url.host for request in deployed.api.requests} == {'127.0.0.1'}
    assert deployed.api.deletes == []


async def test_network_admission_runs_through_the_assembly_before_each_request(stack) -> None:
    southbound, deployed = await stack.build(network=True)
    client = southbound.client
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()
    assert (await client.send_request(_request('one', 'alice'))).ok
    # Creation carried the workspace's policy: nothing was reconciled yet.
    assert deployed.api.networks == []

    alice = deployed.root / 'alice'
    _write_model_config(alice, 'https://other-model.example/v1')
    assert (await client.send_request(_request('two', 'alice'))).ok
    ((instance, policy),) = deployed.api.networks
    assert 'other-model.example' in repr(policy)
    assert stack.ingress.dials[0][1] == instance

    # A refused policy fails the request before anything reaches the instance.
    _write_model_config(alice, 'https://third-model.example/v1')
    deployed.api.network_reply = 403
    with pytest.raises(AgentCreateFailed, match='AGENT_NETWORK_UPDATE_FAILED'):
        await client.send_request(_request('three', 'alice'))
    assert len(stack.ingress.requests) == 2 and len(stack.ingress.dials) == 1
    # It was not remembered as applied: the next request asks ADX again.
    deployed.api.network_reply = None
    assert (await client.send_request(_request('four', 'alice'))).ok
    assert len(deployed.api.networks) == 3 and len(stack.ingress.requests) == 3
    assert deployed.api.deletes == []


# --------------------------------------------------------------------------- shutdown


async def test_close_closes_the_sockets_then_the_leaf_and_deletes_nothing(stack, monkeypatch) -> None:
    southbound, deployed = await stack.build()
    client = southbound.client
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()
    await client.send_request(_request('one', 'alice'))
    await client.send_request(_request('two', 'bob'))
    assert len(client._sockets) == 2

    original = WorkerInstanceBackend.close
    seen: list[tuple[int, tuple[tuple[str, int | None], ...]]] = []

    async def close_after_the_sockets(self, *, deadline=None) -> None:
        seen.append((len(client._sockets), tuple(sorted(stack.ingress.closed))))
        await original(self, deadline=deadline)

    monkeypatch.setattr(WorkerInstanceBackend, 'close', close_after_the_sockets)
    await southbound.close()
    # Both sockets were closed by their peers' bookkeeping before the leaf was touched.
    assert seen == [(0, (('alice', 1000), ('bob', 1000)))]
    assert not client.server_ready
    assert southbound.backend.client is None and southbound.backend.registry is None
    assert southbound.backend.manager is None
    assert stack.controls[0].http.is_closed and stack.registries[0].closed
    # The Environments stay with their platform.
    assert deployed.api.deletes == [] and len(deployed.api.environments) == 2
    assert deployed.api.requests and all(request.method != 'DELETE' for request in deployed.api.requests)


async def test_close_with_an_expired_deadline_still_closes_the_leaf(stack) -> None:
    southbound, deployed = await stack.build()
    client = southbound.client
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()
    await client.send_request(_request('one', 'alice'))
    await southbound.close(deadline=asyncio.get_running_loop().time() - 1)
    assert not client.server_ready and client._sockets == set()
    assert southbound.backend.client is None and stack.controls[0].http.is_closed
    assert stack.registries[0].closed and deployed.api.deletes == []
    # The peer sees its connection go, without a closing handshake to wait for.
    async with asyncio.timeout(5):
        while not stack.ingress.closed:
            await asyncio.sleep(0.01)
    # Closing again is harmless.
    await southbound.close()
