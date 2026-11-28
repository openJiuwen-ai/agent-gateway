# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fakes shared by the dynamic-instance tests.

``InstanceFrontend`` is a loopback stand-in for the ADX ingress: it speaks the
Gateway WebSocket protocol with the real ``WebSocketConnection`` engine and
records every dial. ``EnvironmentAPI`` is the ADX Environment control API on
its public wire format, served through an ``httpx.MockTransport`` to the real
``AdxControlClient``. Neither is a test module.
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any, Callable
from urllib.parse import unquote, urlsplit

import httpx
from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.wire_codec import encode_agent_chunk_for_wire, encode_agent_response_for_wire
from websockets.asyncio.server import serve

from agent_gateway.extensions.agentos.agentos_router.models import AgentStatus


class EnvironmentAPI:
    """In-process ADX Environment control API on the public wire format."""

    def __init__(self, tenant: str = 'appliance') -> None:
        self.tenant = tenant
        self.environments: dict[str, dict[str, Any]] = {}
        self.requests: list[httpx.Request] = []
        self.creates: list[dict[str, Any]] = []
        self.deletes: list[tuple[str, str | None]] = []
        self.create_status = 'running'
        self.create_before_gate = False
        self.post_plan: list[Any] = []
        self.post_started = asyncio.Event()
        self.post_gate = asyncio.Event()
        self.post_gate.set()
        self.networks: list[tuple[str, dict[str, Any]]] = []
        self.network_reply: int | None = None
        # Told about every Environment that comes to exist, by instance id.
        self.on_create: Callable[[str], None] | None = None

    def arm(self, outcome: Any = 'commit', *, created_before_reply: bool = False) -> None:
        """Hold the next create POST until it is released with ``outcome``."""
        self.post_plan.append(outcome)
        self.create_before_gate = created_before_reply
        self.post_started = asyncio.Event()
        self.post_gate.clear()

    def _create(self, body: dict[str, Any]) -> str:
        instance = f"{body['namespace']}-{body['name']}"
        self.environments[instance] = {
            'id': instance, 'tenant_id': self.tenant, 'status': self.create_status,
            'env_vars': dict(body['env']), 'node_ip': '10.0.0.5', 'sandbox_ip': '10.0.0.6',
            'runtime_id': 'runtime-' + instance,
        }
        if self.on_create is not None:
            self.on_create(instance)
        return instance

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.headers['authorization'] == 'Bearer platform-secret'
        if request.method == 'GET' and request.url.path == '/api/instances':
            entry = self.environments.get(request.url.params['instance_id'])
            return httpx.Response(200, json=[entry]) if entry is not None else httpx.Response(404)
        if request.method == 'POST' and request.url.path == '/api/environment/v1/environments':
            body = json.loads(request.content)
            self.creates.append(body)
            self.post_started.set()
            if self.create_before_gate:
                self._create(body)
            await self.post_gate.wait()
            outcome = self.post_plan.pop(0) if self.post_plan else 'commit'
            instance = f"{body['namespace']}-{body['name']}"
            if outcome in ('commit', 'lost_created') and instance not in self.environments:
                self._create(body)
            if outcome in ('lost', 'lost_created'):
                raise httpx.ReadTimeout('response lost', request=request)
            if isinstance(outcome, tuple):
                return httpx.Response(outcome[0], json={'error': {'code': outcome[1]}})
            data = base64.b64encode(json.dumps({'environmentId': instance}).encode()).decode()
            return httpx.Response(200, json={'code': 200, 'data': data})
        if request.method == 'DELETE':
            # httpx decodes the path: '%2B' is one character inside one segment.
            instance = request.url.path.rsplit('/', 1)[1]
            self.deletes.append((instance, request.headers['x-request-id']))
            if instance not in self.environments:
                return httpx.Response(404)
            del self.environments[instance]
            return httpx.Response(200, json={'success': True})
        if request.method == 'PUT' and request.url.path.endswith('/network'):
            # httpx decodes the path: the instance id is one segment.
            instance = request.url.path.rsplit('/', 2)[-2]
            self.networks.append((instance, json.loads(request.content)))
            if self.network_reply is not None:
                return httpx.Response(self.network_reply, json={
                    'error': {'code': 'PERMISSION_DENIED', 'outcome': 'terminal'},
                })
            return httpx.Response(200, json={'success': True})
        raise AssertionError(f'unexpected ADX request: {request.method} {request.url}')

    @property
    def posts(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.method == 'POST']


class FakeRegistry:
    def __init__(self, config: Any) -> None:
        self.config = config
        self.registered: list[Any] = []
        self.closed = False
        self.fail_registration: Exception | None = None
        self.register_started = asyncio.Event()
        self.register_gate = asyncio.Event()
        self.register_gate.set()

    async def register_agent(self, agent_info: Any) -> None:
        self.register_started.set()
        await self.register_gate.wait()
        if self.fail_registration is not None:
            raise self.fail_registration
        self.registered.append(agent_info.copy())

    async def close(self) -> None:
        self.closed = True


class BrokenRegistry:
    def __init__(self, config: Any) -> None:
        raise RuntimeError('registry constructor failed')


class InstanceFrontend:
    """One loopback listener playing the ingress and every generation behind it."""

    def __init__(self) -> None:
        self.created: list[Any] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.dials: list[tuple[str, str, dict[str, str]]] = []
        self.paths: list[str] = []
        self.events: list[str] = []
        self.closed: list[tuple[str, int | None]] = []
        self.users: dict[str, str] = {}
        self.received = asyncio.Event()
        self.reply_gate = asyncio.Event()
        self.reply_gate.set()
        # A stream's last chunk waits here, so a test can cut the stream.
        self.final_gate = asyncio.Event()
        self.final_gate.set()
        # Owners whose sockets stopped reading at the transport level.
        self.muted: set[str] = set()
        self._muted_sockets: dict[str, Any] = {}
        self._sockets: dict[str, Any] = {}
        self._server: Any = None

    async def start(self) -> InstanceFrontend:
        self._server = await serve(self._handle, '127.0.0.1', 0)
        return self

    @property
    def netloc(self) -> str:
        return f"127.0.0.1:{self._server.sockets[0].getsockname()[1]}"

    async def stop(self) -> None:
        # A muted generation reads its peer's close first, so stop() cannot wait on it.
        self.let_muted_read()
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

    def adopt(self, instance: str) -> None:
        """Let a generation created elsewhere dial in under its owner.

        ADX names it ``<namespace>-<user>+<agent type>-<generation>``.
        """
        self.users[unquote(instance)] = unquote(instance).split('-', 1)[1].split('+', 1)[0]

    def mute(self, *users: str) -> None:
        """Stop reading for these owners: nothing they receive is parsed again.

        websockets answers a close frame while parsing it, so a generation that
        only stopped calling ``recv()`` would still complete a closing handshake.
        """
        for user in users:
            self.muted.add(user)
            self._muted_sockets[user] = self._sockets[user]
            self._sockets[user].transport.pause_reading()

    def let_muted_read(self) -> None:
        for user, socket in tuple(self._muted_sockets.items()):
            del self._muted_sockets[user]
            transport = getattr(socket, 'transport', None)
            if transport is not None and not transport.is_closing():
                transport.resume_reading()

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

    async def wait_closed(self, count: int) -> None:
        """Let muted generations read again, then wait for ``count`` recorded closes."""
        self.let_muted_read()
        async with asyncio.timeout(5):
            while len(self.closed) < count:
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
        self.paths.append(websocket.request.path)
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
                    request_id=rid, channel_id=channel, payload={'event_type': event, 'marker': user, 'content': user},
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
