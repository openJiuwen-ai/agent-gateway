# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Dynamic ownership runs through real per-generation WebSocket sockets.

The instance frontend is a loopback stand-in for the ADX ingress: it speaks the
Gateway WebSocket protocol with the real ``WebSocketConnection`` engine and
records every dial. The provider and manager under test are the real ones.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, aclosing
import json
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest
from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.models import E2AEnvelope
from e2a.wire_codec import encode_agent_chunk_for_wire, encode_agent_response_for_wire
from websockets.asyncio.server import serve

from agent_gateway.application.worker_instance_routed_client import WorkerInstanceRoutedClient, _InstanceSocket
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.southbound.orchestration.agent_manager import AgentCreateFailed, AgentManager, AgentStatus
from agent_gateway.southbound.orchestration.instance_provider import AgentOSInstanceProvider


class Frontend:
    """The control's endpoint derivation for a loopback ADX ingress."""

    def __init__(self, netloc: str) -> None:
        self.frontend_endpoint = f'http://{netloc}'
        self._netloc = netloc

    def ws_url(self, instance_id: str, port: int) -> str:
        return f'ws://{self._netloc}/{instance_id}/{port}'


class InstanceFrontend:
    """One loopback listener playing the ingress and every generation behind it."""

    def __init__(self) -> None:
        self.created: list[Any] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.dials: list[tuple[str, str, dict[str, str]]] = []
        self.events: list[str] = []
        self.closed: list[tuple[str, int | None]] = []
        self.users: dict[str, str] = {}
        self.received = asyncio.Event()
        self.reply_gate = asyncio.Event()
        self.reply_gate.set()
        # A stream's last chunk waits here, so a test can cut the stream.
        self.final_gate = asyncio.Event()
        self.final_gate.set()
        self._sockets: dict[str, Any] = {}
        self._server: Any = None

    async def start(self) -> InstanceFrontend:
        self._server = await serve(self._handle, '127.0.0.1', 0)
        return self

    @property
    def netloc(self) -> str:
        return f"127.0.0.1:{self._server.sockets[0].getsockname()[1]}"

    async def stop(self) -> None:
        for socket in list(self._sockets.values()):
            try:
                await socket.close(code=1001)
            except Exception:  # noqa: BLE001 - the peer may already be gone
                pass
        self._sockets.clear()
        self._server.close()
        await self._server.wait_closed()

    async def create(self, info):
        """The trusted creator: management readiness is attested for this generation."""
        self.created.append(info.copy())
        info.sandbox_id = 'instance-' + info.agent_id
        info.status = AgentStatus.READY
        info.metadata.update(physical_generation=info.agent_id, agent_port=18092)
        self.users[info.sandbox_id] = info.user_id
        return info

    def drop(self, user: str) -> None:
        socket = self._sockets.pop(user, None)
        if socket is not None:
            socket.transport.abort()

    def dial_count(self) -> int:
        return len(self.dials)

    async def wait_dials(self, count: int) -> None:
        async with asyncio.timeout(5):
            while len(self.dials) < count:
                await asyncio.sleep(0.01)

    async def push(self, user: str, wire: dict[str, Any]) -> None:
        await self._sockets[user].send(json.dumps(wire))

    def request_ids(self) -> list[str]:
        return [str(row.get('request_id') or '') for _, row in self.requests]

    async def _handle(self, websocket) -> None:
        # ADX loopback ingress shape: /{instance_id}/{port}.
        instance = unquote(urlsplit(websocket.request.path).path.split('/')[1])
        user = self.users[instance]
        headers = dict(websocket.request.headers)
        self.dials.append((user, instance, headers))
        self.events.append('dial')
        self._sockets[user] = websocket
        pending: set[asyncio.Task[None]] = set()
        try:
            await websocket.send(json.dumps({
                'type': 'event', 'event': 'connection.ack',
                'payload': {'status': 'ready', 'readiness': 'AGENT_READY'},
            }))
            async for raw in websocket:
                request = json.loads(raw)
                self.requests.append((user, request))
                self.received.set()
                # A real AgentServer answers concurrently; a held stream must not
                # stop this socket from reading the next request.
                task = asyncio.create_task(self._answer(websocket, user, request))
                pending.add(task)
                task.add_done_callback(pending.discard)
        except Exception:  # noqa: BLE001 - the Gateway side owns the close
            pass
        finally:
            self.closed.append((user, websocket.close_code))
            self.events.append('closed')
            if self._sockets.get(user) is websocket:
                self._sockets.pop(user, None)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def _answer(self, websocket, user: str, request: dict[str, Any]) -> None:
        rid = str(request.get('request_id') or '')
        channel = str(request.get('channel_id') or request.get('channel') or 'web')
        await self.received.wait()
        await self.reply_gate.wait()
        if request.get('is_stream'):
            for sequence, (event, complete) in enumerate((('chat.delta', False), ('chat.final', True))):
                if complete:
                    await self.final_gate.wait()
                chunk = AgentResponseChunk(
                    request_id=rid, channel_id=channel, payload={'event_type': event, 'marker': user},
                    is_complete=complete,
                )
                await websocket.send(json.dumps(encode_agent_chunk_for_wire(
                    chunk, response_id=rid, sequence=sequence,
                )))
            return
        payload: dict[str, Any] = {'marker': user}
        if request.get('method') == 'session.create':
            payload['result'] = {'session_id': 'session-' + user}
        response = AgentResponse(request_id=rid, channel_id=channel, payload=payload, ok=True)
        await websocket.send(json.dumps(encode_agent_response_for_wire(response, response_id=rid)))


def push_wire(request_id: str, session_id: str, *, user: str | None = None, **payload: Any) -> dict[str, Any]:
    wire: dict[str, Any] = {
        'request_id': request_id, 'channel_id': 'web', 'session_id': session_id,
        'metadata': {E2A_WIRE_SERVER_PUSH_KEY: True},
        'payload': {'event_type': 'chat.delta', **payload}, 'is_complete': False,
    }
    if user is not None:
        wire['user_id'] = user
    return wire


async def pool(stack: AsyncExitStack, **hooks: Any):
    frontend = await InstanceFrontend().start()
    stack.push_async_callback(frontend.stop)
    owner = AgentOSInstanceProvider(
        AgentManager(), creator=frontend.create, control=Frontend(frontend.netloc),
        namespace='private', identity_policy=LocalIdentityPolicy(),
        manifest=WorkerManifest((0,)), slot_id=0, admitted_users=('alice', 'bob'),
    )
    client = WorkerInstanceRoutedClient(owner, **hooks)
    pushes: list[dict[str, Any]] = []
    client.set_server_push_handler(pushes.append)
    stack.push_async_callback(client.disconnect)
    return client, frontend, owner, pushes


def request(
    user: str, rid: str = 'one', method: str = 'session.list', sid: str | None = None, **extra: Any,
):
    params = extra.pop('params', {})
    return E2AEnvelope(
        request_id=rid, user_id=user, channel='web', method=method, session_id=sid, params=params, **extra,
    )


async def current_generation(owner: AgentOSInstanceProvider, user: str = 'alice'):
    runtime = await owner.manager.get_agent(user, 'jiuwenswarm')
    assert runtime is not None
    return runtime.info.agent_id


async def pop_generation(owner: AgentOSInstanceProvider, user: str = 'alice') -> None:
    await owner.manager.pop_agent(user, 'jiuwenswarm', expected_agent_id=await current_generation(owner, user))


# --------------------------------------------------------------------------- routing


async def test_dynamic_activation_is_lazy_and_same_worker_targets_are_isolated() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        assert not frontend.created and not frontend.dials
        assert client.server_ready and not client.agent_ready and client.required_tasks == ()
        first = await client.send_request(request('alice', 'alice'))
        second = await client.send_request(request('bob', 'bob'))
        assert first.payload == {'marker': 'alice'} and second.payload == {'marker': 'bob'}
        assert [user for user, _ in frontend.requests] == ['alice', 'bob']
        assert len(frontend.created) == 2 and len(frontend.dials) == 2
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0


async def test_cancel_missing_user_instance_never_creates_a_sandbox() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        response = await client.send_request(request('alice', method='chat.interrupt'))
        assert response.ok and response.payload['success'] and response.payload['event_type'] == 'chat.interrupt_result'
        # Every other existing-only method reports the missing instance instead of creating one.
        for method in ('chat.user_answer', 'swarmflow.pause', 'acp.tool_response'):
            refused = await client.send_request(request('alice', method=method))
            assert refused.ok is False and refused.payload == {'error': 'no existing instance for this control request'}
        # A streamed control request is refused the same way: no instance is made for it.
        with pytest.raises(ValueError, match='existing instance'):
            async for _ in client.send_request_stream(request('alice', method='chat.interrupt', is_stream=True)):
                pass
        with pytest.raises(ValueError):
            await client.send_request(request('unknown'))
        # A foreign agent type is refused by ownership, never by a request.
        with pytest.raises(ValueError, match='owned instance route'):
            await client.send_request(request('alice', params={'agent_type': 'openclaw'}))
        with pytest.raises(ValueError, match='owned instance route'):
            await client.send_request(request('alice', channel_context={'agent_type': 'openclaw'}))
        assert not frontend.created and not frontend.requests and frontend.dials == []


async def test_dynamic_stream_and_unary_reuse_one_physical_connection() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        created = await client.send_request(request('alice', method='session.create'))
        assert created.payload['result'] == {'session_id': 'session-alice'}
        async with aclosing(client.send_request_stream(
            request('alice', 'stream', 'chat.send', 'session-alice', is_stream=True),
        )) as stream:
            chunks = [chunk async for chunk in stream]
        assert chunks[-1].is_complete and chunks[-1].payload['marker'] == 'alice'
        assert len(frontend.created) == 1 and len(frontend.dials) == 1
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0


async def test_old_session_is_not_silently_sent_to_replacement_generation() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        await client.send_request(request('alice', 'create-first', 'session.create'))
        await pop_generation(owner)
        await client.send_request(request('alice', 'new-target'))
        before = len(frontend.requests)
        with pytest.raises(ValueError, match='generation'):
            await client.send_request(request('alice', 'old-session', 'history.get', 'session-alice'))
        with pytest.raises(ValueError, match='generation'):
            async for _ in client.send_request_stream(
                request('alice', 'old-stream', 'chat.send', 'session-alice', is_stream=True),
            ):
                pass
        # Nothing was leased for the refused requests: the hold of each is released.
        assert len(frontend.requests) == before and owner.active_lease_count == 0


async def test_used_request_ids_and_foreign_sessions_are_refused_before_creation() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        client.validate_bound_session('alice', 'shared')
        with pytest.raises(ValueError, match='another instance route'):
            client.validate_bound_session('bob', 'shared')
        with pytest.raises(ValueError, match='request ID'):
            client.validate_bound_request('alice', '')
        async def consume(stream) -> None:
            async with aclosing(stream) as chunks:
                async for _ in chunks:
                    pass

        frontend.reply_gate.clear()
        held = asyncio.create_task(consume(client.send_request_stream(
            request('alice', 'held', 'chat.send', 's', is_stream=True),
        )))
        await frontend.received.wait()
        # One id indexes shared consumers: no route may take it while it is live.
        for user in ('alice', 'bob'):
            with pytest.raises(ValueError, match='unused request ID'):
                client.validate_bound_request(user, 'held')
            with pytest.raises(ValueError, match='unused request ID'):
                await client.send_request(request(user, 'held'))
        frontend.reply_gate.set()
        await held
        # A session pinned to alice's generation can not be claimed by bob's route.
        with pytest.raises(ValueError, match='another instance route'):
            client.validate_bound_session('bob', 's')
        assert frontend.request_ids() == ['held'] and len(frontend.created) == 1


# --------------------------------------------------------------------------- generations


async def test_late_unary_response_is_not_delivered_after_manager_epoch_changes() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        frontend.reply_gate.clear()
        task = asyncio.create_task(client.send_request(request('alice')))
        await frontend.received.wait()
        await pop_generation(owner)
        frontend.reply_gate.set()
        with pytest.raises(RuntimeError, match='stale'):
            await task
        # The instance answered, so the work finished; only its delivery was refused.
        # A write that never reached a known outcome is the case that keeps a hold.
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0


async def test_a_stream_of_a_replaced_generation_stops_before_its_next_chunk() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        frontend.final_gate.clear()
        delivered: list[str] = []

        async def consume() -> None:
            stream = client.send_request_stream(request('alice', 'stream', 'chat.send', 's', is_stream=True))
            async for chunk in stream:
                delivered.append(chunk.payload['event_type'])

        streaming = asyncio.create_task(consume())
        async with asyncio.timeout(5):
            while not delivered:
                await asyncio.sleep(0.01)
        assert delivered == ['chat.delta']
        # The generation is replaced while the old instance is still answering.
        await pop_generation(owner)
        frontend.final_gate.set()
        with pytest.raises(RuntimeError, match='stale'):
            await streaming
        # What the old instance said after that never reaches the consumer.
        assert delivered == ['chat.delta']
        # The stream did not say how it ended: its activity hold stays, flagged uncertain.
        assert owner.active_lease_count == 1 and owner.uncertain_lease_count == 1


async def test_uncertain_write_without_callback_keeps_activity_hold(monkeypatch) -> None:
    async def partial_write(self, payload):
        raise RuntimeError('connection lost during write')

    monkeypatch.setattr(_InstanceSocket, '_write_wire', partial_write)
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        with pytest.raises(RuntimeError, match='during write'):
            await client.send_request(request('alice'))
        assert owner.uncertain_lease_count == 1 and owner.active_lease_count == 1
        assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 1


async def test_generation_change_after_preflight_cannot_write_old_socket(monkeypatch) -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        await client.send_request(request('alice', 'warm'))
        original = _InstanceSocket._send_wire_payload
        changed = False

        async def change_before_wire(self, payload):
            nonlocal changed
            if not changed:
                changed = True
                await pop_generation(owner)
            await original(self, payload)

        monkeypatch.setattr(_InstanceSocket, '_send_wire_payload', change_before_wire)
        with pytest.raises(RuntimeError, match='stale'):
            await client.send_request(request('alice', 'must-not-write'))
        assert frontend.request_ids() == ['warm']


async def test_replacing_idle_generation_retires_old_connection_and_entry() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        await client.send_request(request('alice', 'first'))
        old_socket = next(iter(client._sockets))
        raw = old_socket._ws
        assert old_socket.live
        await pop_generation(owner)
        await client.send_request(request('alice', 'second'))
        assert len(client._entries) == 1 and frontend.dial_count() == 2
        # The receiver forgot the connection when it was retired; it is still closed.
        async with asyncio.timeout(5):
            while raw.close_code is None or old_socket in client._sockets:
                await asyncio.sleep(0.01)
        assert not old_socket.live and len(client._sockets) == 1
        assert ('alice', raw.close_code) in frontend.closed
        # The retired connection is closed before its replacement dials.
        assert frontend.events == ['dial', 'closed', 'dial']


async def test_old_active_generation_is_retired_when_its_last_call_finishes() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        frontend.reply_gate.clear()
        old_call = asyncio.create_task(client.send_request(request('alice', 'old-active', 'chat.send', 's')))
        await frontend.received.wait()
        old_socket = next(iter(client._sockets))
        await pop_generation(owner)
        new_call = asyncio.create_task(client.send_request(request('alice', 'new-generation')))
        await frontend.wait_dials(2)
        frontend.reply_gate.set()
        with pytest.raises(RuntimeError, match='stale'):
            await old_call
        await new_call
        async with asyncio.timeout(5):
            while len(client._entries) != 1 or old_socket in client._sockets:
                await asyncio.sleep(0.01)
        assert frontend.request_ids().count('new-generation') == 1


async def test_a_dropped_socket_is_replaced_by_the_client_not_by_the_engine() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        await client.send_request(request('alice', 'first'))
        old_socket = next(iter(client._sockets))
        frontend.drop('alice')
        async with asyncio.timeout(5):
            while old_socket.live:
                await asyncio.sleep(0.01)
        # The engine's own request-driven reconnect is disabled on a dead socket.
        with pytest.raises(Exception, match='unavailable'):
            await old_socket.send_request(request('alice', 'never'))
        assert (await client.send_request(request('alice', 'second'))).payload == {'marker': 'alice'}
        assert frontend.dial_count() == 2 and len(client._sockets) == 1


# --------------------------------------------------------------------------- ADX hooks


async def test_each_dial_takes_a_fresh_ingress_credential_that_never_enters_a_route() -> None:
    issued: list[int] = []

    def dial_headers() -> dict[str, str]:
        issued.append(len(issued))
        return {'Authorization': f'Bearer platform-secret-{len(issued)}', 'X-Trace-Id': f'trace-{len(issued)}'}

    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack, dial_headers=dial_headers)
        await client.activate()
        await client.send_request(request('alice', 'first'))
        await client.send_request(request('alice', 'second'))
        first = {key.lower(): value for key, value in frontend.dials[0][2].items()}
        assert len(issued) == 1 and first['authorization'] == 'Bearer platform-secret-1'
        assert first['x-trace-id'] == 'trace-1'
        await pop_generation(owner)
        await client.send_request(request('alice', 'third'))
        second = {key.lower(): value for key, value in frontend.dials[1][2].items()}
        assert len(issued) == 2 and second['authorization'] == 'Bearer platform-secret-2'
        assert second['x-trace-id'] == 'trace-2'
        runtime = await owner.manager.get_agent('alice', 'jiuwenswarm')
        assert 'platform-secret' not in repr(vars(runtime)) + repr(vars(client)) + repr(list(client._entries.values()))


async def test_network_admission_runs_for_each_leased_generation_before_its_dial() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        seen: list[Any] = []

        async def admit(target):
            frontend.events.append('admit')
            seen.append(target)

        client._admit_network = admit
        await client.activate()
        lease = await owner.acquire(owner.validate_owner('alice'))
        await owner.release(lease, outcome='finished')
        await client.send_request(request('alice', 'first'))
        await client.send_request(request('alice', 'second'))
        assert frontend.events == ['admit', 'dial', 'admit']
        assert [item.generation for item in seen] == [lease.target.generation] * 2


async def test_refused_network_admission_writes_nothing_and_releases_the_hold() -> None:
    async def refuse(target):
        raise AgentCreateFailed(f'AGENT_NETWORK_UPDATE_FAILED: denied {target.physical_id}')

    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack, admit_network=refuse)
        await client.activate()
        with pytest.raises(AgentCreateFailed, match='AGENT_NETWORK_UPDATE_FAILED'):
            await client.send_request(request('alice'))
        assert frontend.dials == [] and frontend.requests == []
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0
        assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


# --------------------------------------------------------------------------- pushes


async def test_a_push_reaches_the_consumer_only_through_the_generation_that_proved_it() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, pushes = await pool(stack)
        await client.activate()
        # Bob's generation holds a socket of its own for the whole test.
        await client.send_request(request('bob', 'bob'))
        bob = client._entries[await current_generation(owner, 'bob')].target
        # From here the signal means: alice's stream request has arrived.
        frontend.received.clear()

        async def consume(stream) -> None:
            async with aclosing(stream) as chunks:
                async for _ in chunks:
                    pass

        frontend.reply_gate.clear()
        held = asyncio.create_task(consume(client.send_request_stream(
            request('alice', 'live', 'chat.send', 's-alice', is_stream=True),
        )))
        await frontend.received.wait()
        alice = client._entries[await current_generation(owner)].target
        assert alice.user_id == 'alice'
        # Frames the consumers broadcast to everyone, behind a request this
        # generation really dispatched: the socket must drop them.
        await frontend.push('alice', push_wire(
            'live', 's-alice', event_type='chat.final', source='proactive_notification',
        ))
        await frontend.push('alice', push_wire('live', 's-alice', event_type='models.updated'))
        # A chat frame of the live request and a session notification keep their route.
        live = push_wire('live', 's-alice')
        usage = push_wire('', 's-alice', event_type='context.usage')
        await frontend.push('alice', live)
        await frontend.push('alice', usage)
        async with asyncio.timeout(5):
            while len(pushes) < 2:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        assert [user for user, _, _ in frontend.dials] == ['bob', 'alice']
        assert [item['request_id'] for item in pushes] == ['live', '']
        assert pushes == [
            {**live, 'user_id': 'alice', 'session_id': 's-alice'},
            {**usage, 'user_id': 'alice', 'session_id': 's-alice'},
        ]
        # Bob's generation may not speak for alice's request.
        assert client._push_guard.admit(bob, live) is None
        await frontend.push('bob', live)
        await asyncio.sleep(0.1)
        assert len(pushes) == 2
        frontend.reply_gate.set()
        await held
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0


async def test_pushes_of_a_retired_generation_are_dropped_at_the_socket() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, pushes = await pool(stack)
        await client.activate()
        await client.send_request(request('alice', 'first', sid='s'))
        retired = next(iter(client._entries.values())).target
        await pop_generation(owner)
        # The socket still exists until a request retires it; the id alone no
        # longer proves anything, so the frame is dropped before the consumers.
        assert not await owner.is_target_current(retired)
        await frontend.push('alice', push_wire('first', 's'))
        await asyncio.sleep(0.2)
        assert pushes == [] and len(client._sockets) == 1 and frontend.dial_count() == 1


# --------------------------------------------------------------------------- outcomes


async def test_a_request_the_engine_did_not_write_releases_its_hold(monkeypatch) -> None:
    live = _InstanceSocket.live
    seen = {'count': 0}

    @property
    def died_before_send(self):
        # Alive when the client picks the socket, gone when the engine writes.
        seen['count'] += 1
        return live.fget(self) if seen['count'] == 1 else False

    monkeypatch.setattr(_InstanceSocket, 'live', died_before_send)
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        with pytest.raises(Exception, match='unavailable'):
            await client.send_request(request('alice'))
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0
        assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
        assert frontend.requests == []


async def test_a_failed_dial_writes_nothing_and_releases_the_hold() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack, connect_timeout=0.5)

        async def refuse(target):
            raise RuntimeError(f'AGENT_NETWORK_UPDATE_FAILED: {target.physical_id}')

        client._admit_network = refuse
        await client.activate()
        # The first request reaches the network hook; the hook fails before any dial.
        with pytest.raises(RuntimeError, match='AGENT_NETWORK_UPDATE_FAILED'):
            await client.send_request(request('alice'))
        client._admit_network = None
        await frontend.stop()
        stack.pop_all()
        with pytest.raises(Exception):
            await client.send_request(request('alice', 'second'))
        assert owner.active_lease_count == 0 and owner.uncertain_lease_count == 0
        assert frontend.requests == []
        await client.disconnect()


# --------------------------------------------------------------------------- lifecycle


async def test_construction_activation_and_shutdown_are_explicit_and_bounded() -> None:
    async def creator(info):
        return info

    owner = AgentOSInstanceProvider(
        AgentManager(), creator=creator, control=Frontend('127.0.0.1:1'), namespace='private',
        identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest((0,)), slot_id=0,
        admitted_users=('alice',),
    )
    for changes in (
        {'provider': object()}, {'connect_timeout': 0}, {'startup_timeout': float('inf')},
        {'stop_timeout': True}, {'dial_headers': 'Bearer x'}, {'admit_network': 'no'},
    ):
        values = {'provider': owner, **changes}
        with pytest.raises(ValueError):
            WorkerInstanceRoutedClient(values.pop('provider'), **values)
    client = WorkerInstanceRoutedClient(owner)
    assert not client.server_ready
    with pytest.raises(RuntimeError, match='push handler'):
        await client.activate()
    client.set_server_push_handler(None)
    with pytest.raises(RuntimeError, match='not active'):
        await client.send_request(request('alice'))
    with pytest.raises(ValueError):
        await client.connect('ws://127.0.0.1:1/x')
    with pytest.raises(ValueError):
        client.set_or_update_server_config(config={})
    client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    await client.activate()
    await client.activate()
    assert client.server_ready
    with pytest.raises(RuntimeError, match='fixed at activation'):
        client.set_server_push_handler(lambda wire: asyncio.sleep(0))
    # A deadline already in the past still closes everything without raising.
    await client.disconnect(deadline=asyncio.get_running_loop().time() - 1)
    assert not client.server_ready and owner._closed
    with pytest.raises(RuntimeError, match='stopped'):
        await client.activate()
    with pytest.raises(RuntimeError, match='not active'):
        await client.send_request(request('alice'))
    await client.disconnect()


async def test_disconnect_closes_sockets_then_stops_local_acquisition_without_deleting() -> None:
    async with AsyncExitStack() as stack:
        client, frontend, owner, _ = await pool(stack)
        await client.activate()
        await client.send_request(request('alice'))
        await client.send_request(request('bob', 'bob'))
        assert len(client._sockets) == 2 and len(frontend.created) == 2
        await client.disconnect()
        assert client._sockets == set() and client._entries == {} and client._push_guard.counts == (0, 0, 0)
        assert owner._closed and frontend.dial_count() == 2
        # The instances stay registered with their platform: a Worker restart does not delete them.
        assert len(frontend.users) == 2 and frontend.created
