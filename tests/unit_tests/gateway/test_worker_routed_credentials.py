# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Credential operations are bound to trusted physical targets, never user input."""

import asyncio
from contextlib import AsyncExitStack

import pytest
from e2a.models import E2AEnvelope

from agent_gateway.application.worker_routed_client import WorkerRoutedClient
from tests.unit_tests.gateway.routed_agent_server import RoutedAgentServer, legacy_push_wire
from tests.unit_tests.gateway.test_worker_routed_client import directory, target


async def make_pool(stack):
    agents = [await RoutedAgentServer(user).start() for user in ('alice', 'bob')]
    for agent in agents:
        stack.push_async_callback(agent.stop)
    routes = directory(*(target(agent.name, i, agent.url) for i, agent in enumerate(agents)))
    client = WorkerRoutedClient(routes, slot_id=0)
    stack.push_async_callback(client.disconnect)
    return client, agents, routes


async def test_private_operation_ports_reach_only_their_exact_target() -> None:
    async with AsyncExitStack() as stack:
        client, agents, directory = await make_pool(stack)
        ports = client.credential_operation_clients()
        await client.activate()
        assert set(ports) == {client.server_id_for_user('alice'), client.server_id_for_user('bob')}
        for user, agent in zip(('alice', 'bob'), agents):
            written = []
            result = await ports[client.server_id_for_user(user)].send_request(
                E2AEnvelope(request_id='auth-' + user, method='auth.credentials.update', params={}),
                on_written=lambda: written.append(True),
            )
            assert result.ok and agent.has_request('auth-' + user)
            assert written == [True]
        assert all(len(agent.requests) == 1 for agent in agents)
        with pytest.raises(ValueError):
            client.server_id_for_user('unknown')


async def test_refresh_push_uses_trusted_source_and_requires_explicit_lifecycle_handler() -> None:
    async with AsyncExitStack() as stack:
        client, agents, directory = await make_pool(stack)
        received = []
        delivered = asyncio.Event()

        async def refresh(server_id, wire):
            received.append((server_id, wire))
            delivered.set()

        client.set_credential_lifecycle_handler(refresh)
        await client.activate()
        wire = legacy_push_wire('refresh', '', event='auth.credential.refresh', credential_ref='a' * 32)
        await agents[1].push(wire)
        await asyncio.wait_for(delivered.wait(), 1)
        assert received[0][0] == client.server_id_for_user('bob')
        assert received[0][1]['payload']['credential_ref'] == 'a' * 32


async def test_cross_target_credential_lease_is_rejected_before_network_send() -> None:
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.worker_credential_pool import WorkerCredentialPool
    from agent_gateway.processes.control_service import ControlServiceIdentity
    from tests.unit_tests.gateway.test_worker_routed_client import envelope

    async with AsyncExitStack() as stack:
        client, agents, directory = await make_pool(stack)
        pool = WorkerCredentialPool(
            identity=ControlServiceIdentity('group', 0, 1, 101, 100),
            operation_clients=client.credential_operation_clients(),
        )
        stack.push_async_callback(pool.stop)
        client.set_credential_runtime(pool)
        await client.activate()
        lease = pool.issue('a' * 32, 1, client.server_id_for_user('alice'))
        credential = ModelCredential('http://localhost', 'private-fixture-token', 'a' * 32, lease=lease)
        request = envelope('bob', 'cross-target')
        request.params['_model_auth'] = credential.to_params()
        with pytest.raises(ValueError, match='another physical target'):
            await client.send_request(request)
        assert not any(agent.requests for agent in agents)
        pool.release(request)
