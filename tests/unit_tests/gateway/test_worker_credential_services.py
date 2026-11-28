"""Explicit lifecycle composition does not enable production account ports."""

import asyncio
import os
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission, ControlServiceClient, ControlServiceIdentity, ControlServiceServer,
)


async def test_composition_binds_ports_event_and_report_to_one_real_endpoint():
    from agent_gateway.auth.worker_credential_services import WorkerCredentialServices

    identity = ControlServiceIdentity('group', 0, 1, os.getpid(), os.getpid())
    left, right = socket.socketpair()
    reports = []
    async def report(arguments):
        reports.append(arguments)
        return {'status': 'ok'}
    class Client:
        def set_credential_runtime(self, runtime):
            self.runtime = runtime

        async def send_request(self, envelope, *, on_written=None):
            on_written()
            return SimpleNamespace(ok=True, payload={'applied': True})
    client = Client()
    services = WorkerCredentialServices(ControlServiceChannel(left), identity,
                                        server_id='as', agent_client=client)
    server = ControlServiceServer(ControlServiceChannel(right), identity,
        handlers={'credentials.update_result': report}, admission=ControlServiceAdmission())
    try:
        await asyncio.gather(services.start(), server.start())
        assert client.runtime is services.runtime
        await server.notify('credentials.invalidate', {
            'operation_id': 'b' * 32, 'server_id': 'as',
            'credential_ref': 'a' * 32, 'cutoff_revision': 1}, timeout=1)
        assert await services.runtime.wait_for_operations(timeout=1)
        assert len(reports) == 1
        assert reports[0]['outcome'] == 'applied'
        assert reports[0]['written'] is True
    finally:
        await services.stop()
        await server.stop()


async def test_cancelled_active_write_barrier_leaves_no_waiters():
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    identity = ControlServiceIdentity('group', 0, 1, 101, 100)
    runtime = WorkerCredentialRuntime(identity=identity, server_id='as', client=SimpleNamespace())
    lease = runtime.issue('a' * 32, 1, 'as')
    envelope = SimpleNamespace(params={'_model_auth': ModelCredential(
        'https://test.invalid', 'synthetic-token', 'a' * 32, lease).to_params()})
    runtime.check(envelope)
    before = asyncio.all_tasks()
    waiting = asyncio.create_task(runtime.wait_for_writes('a' * 32, 'as', 1))
    await asyncio.sleep(0)
    waiting.cancel()
    await asyncio.gather(waiting, return_exceptions=True)
    leftovers = [task for task in asyncio.all_tasks() - before if not task.done()]
    try:
        assert not leftovers
    finally:
        runtime.release(envelope)
        for task in leftovers:
            task.cancel()
        await asyncio.gather(*leftovers, return_exceptions=True)
        await runtime.stop()


async def test_borrowed_services_do_not_stop_process_client():
    from agent_gateway.auth.worker_credential_services import WorkerCredentialServices

    client = SimpleNamespace(
        identity=ControlServiceIdentity('group', 0, 1, 101, 100),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    agent = SimpleNamespace(set_credential_runtime=Mock())
    services = WorkerCredentialServices.from_client(
        client, server_id='as-one', agent_client=agent,
    )

    await services.start()
    await services.stop()

    client.start.assert_not_awaited()
    client.stop.assert_not_awaited()
    assert agent.set_credential_runtime.call_args.args == (services.runtime,)


async def test_borrowed_services_bind_real_invalidation_and_report_to_process_client():
    from agent_gateway.auth.worker_credential_services import WorkerCredentialServices

    identity = ControlServiceIdentity('group', 0, 1, os.getpid(), os.getpid())
    left, right = socket.socketpair()
    services_holder = {}

    async def invalidate(arguments):
        selected = services_holder.get('services')
        if selected is None:
            raise ControlServiceBusy()
        await selected.handle_invalidation(arguments)

    client = ControlServiceClient(
        ControlServiceChannel(left), identity,
        event_handlers={'credentials.invalidate': invalidate},
    )
    reports = []

    async def report(arguments):
        reports.append(arguments)
        return {'status': 'ok'}

    class Agent:
        def __init__(self):
            self.runtime = None

        def set_credential_runtime(self, runtime):
            self.runtime = runtime

        async def send_request(self, envelope, *, on_written=None):
            on_written()
            return SimpleNamespace(ok=True, payload={'applied': True})

    agent = Agent()
    server = ControlServiceServer(
        ControlServiceChannel(right), identity,
        handlers={'credentials.update_result': report}, admission=ControlServiceAdmission(),
    )
    services = None
    try:
        await asyncio.gather(client.start(), server.start())
        services = WorkerCredentialServices.from_client(
            client, server_id='as', agent_client=agent,
        )
        services_holder['services'] = services
        assert agent.runtime is services.runtime

        await server.notify('credentials.invalidate', {
            'operation_id': 'b' * 32, 'server_id': 'as',
            'credential_ref': 'a' * 32, 'cutoff_revision': 1,
        }, timeout=1)
        assert await services.runtime.wait_for_operations(timeout=1)
        assert len(reports) == 1
        assert all(report['outcome'] == 'applied' for report in reports)

        await services.stop()
        assert not client.channel.closed
    finally:
        if services is not None:
            await services.stop()
        await client.stop()
        await server.stop()


async def test_invalidate_ack_waits_for_already_entered_wire_write(monkeypatch):
    from e2a.models import E2AEnvelope
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.worker_credential_services import WorkerCredentialServices
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    identity = ControlServiceIdentity('group', 0, 1, os.getpid(), os.getpid())
    left, right = socket.socketpair()
    target = WebSocketAgentServerClient()
    target._server_ready = True
    target._ws = object()
    entered, release = asyncio.Event(), asyncio.Event()
    written = []
    async def no_op(*args):
        pass
    async def write(payload):
        if payload['request_id'] == 'chat':
            entered.set()
            await release.wait()
        written.append(payload['request_id'])
        target._message_queues[payload['request_id']].put_nowait({})
    monkeypatch.setattr(target, '_ensure_connected_for_request', no_op)
    monkeypatch.setattr(target, '_send_wire_payload', write)
    monkeypatch.setattr('agent_gateway.routing.agent_client._stamp_expert_packages', no_op)
    monkeypatch.setattr('agent_gateway.southbound.data.websocket.parse_agent_server_wire_unary',
                        lambda data: SimpleNamespace(ok=True, payload={'applied': True}))
    services = WorkerCredentialServices(ControlServiceChannel(left), identity, server_id='as', agent_client=target)
    async def report(arguments):
        return {'status': 'ok'}
    server = ControlServiceServer(ControlServiceChannel(right), identity,
        handlers={'credentials.update_result': report}, admission=ControlServiceAdmission())
    sending = notifying = None
    try:
        await asyncio.gather(services.start(), server.start())
        lease = services.runtime.issue('a' * 32, 1, 'as')
        credential = ModelCredential('https://test.invalid', 'synthetic-token', 'a' * 32, lease)
        envelope = E2AEnvelope(request_id='chat', method='chat.send',
                               params={'_model_auth': credential.to_params()})
        sending = asyncio.create_task(target.send_request(envelope))
        await asyncio.wait_for(entered.wait(), 1)
        notifying = asyncio.create_task(server.notify('credentials.invalidate', {
            'operation_id': 'b' * 32, 'server_id': 'as', 'credential_ref': 'a' * 32,
            'cutoff_revision': 1}, timeout=2))
        async with asyncio.timeout(1):
            while not lease.revoked:
                await asyncio.sleep(0)
        # ACK must not claim the fence is settled while an old write can finish.
        await asyncio.sleep(0.01)
        assert not notifying.done()
        release.set()
        await asyncio.wait_for(sending, 1)
        await asyncio.wait_for(notifying, 1)
        assert written[0] == 'chat'
        assert await services.runtime.wait_for_operations(timeout=1)
        assert services.runtime.active_lease_count == 0
    finally:
        release.set()
        for task in (sending, notifying):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (sending, notifying) if task is not None), return_exceptions=True)
        await services.stop()
        await server.stop()
