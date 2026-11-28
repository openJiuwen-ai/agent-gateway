"""Automatic reconnect must retain the last explicit connection's parameters."""
import asyncio
import json

import pytest
from websockets.exceptions import ConnectionClosedError

from e2a.models import E2AEnvelope
from e2a.wire_codec import encode_agent_response_for_wire
from e2a.agent_models import AgentResponse
from agent_gateway.routing.agent_client import AgentCapabilityUnavailable, WebSocketAgentServerClient
from agent_gateway.southbound.data.websocket import RequestNotSentError


class Socket:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.frames.put_nowait(json.dumps({
            'type': 'event', 'event': 'connection.ack', 'payload': {'status': 'ready'},
        }))
        self.sent = []

    async def recv(self):
        frame = await self.frames.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def send(self, raw):
        request = json.loads(raw)
        self.sent.append(request)
        self.frames.put_nowait(json.dumps(encode_agent_response_for_wire(
            AgentResponse(request['request_id'], request['channel'], payload={'received': True}),
            response_id=request['request_id'],
        )))

    async def close(self):
        pass


def transport(monkeypatch):
    calls, sockets = [], []

    async def connect(uri, **kwargs):
        calls.append((uri, kwargs))
        socket = Socket()
        sockets.append(socket)
        return socket

    monkeypatch.setattr('websockets.legacy.client.connect', connect)
    return calls, sockets


async def drop(client, socket):
    socket.frames.put_nowait(ConnectionClosedError(None, None))
    await asyncio.wait_for(client._receiver_task, 1)


async def test_reconnect_reuses_headers_without_replaying_business_request(monkeypatch):
    calls, sockets = transport(monkeypatch)
    client = WebSocketAgentServerClient()
    headers = {'Authorization': 'Bearer original', 'X-Session-Context': 'user-context'}
    try:
        await client.connect('wss://ingress/agent/ws?target=binding', extra_headers=headers)
        headers['Authorization'] = 'Bearer caller-mutated'
        await drop(client, sockets[0])
        response = await client.send_request(E2AEnvelope(
            request_id='new-request', channel='web', method='session.list', params={},
        ), timeout=1)
        assert response.ok
        assert calls[0] == calls[1]
        assert dict(calls[1][1]['extra_headers'])['Authorization'] == 'Bearer original'
        assert sum(len(socket.sent) for socket in sockets) == 1
    finally:
        await client.disconnect()


@pytest.mark.parametrize('replacement', [None, {'Authorization': 'Bearer rotated'}])
async def test_explicit_new_connection_replaces_saved_reconnect_headers(monkeypatch, replacement):
    calls, sockets = transport(monkeypatch)
    client = WebSocketAgentServerClient()
    try:
        await client.connect('wss://old-host/ws', extra_headers={'Authorization': 'Bearer old'})
        await client.connect('wss://new-host/ws', extra_headers=replacement)
        await drop(client, sockets[-1])
        await client.send_request(E2AEnvelope(
            request_id='new-host-request', channel='web', method='session.list', params={},
        ), timeout=1)
        assert calls[-1] == calls[-2]
        assert calls[-1][0] == 'wss://new-host/ws'
        assert dict(calls[-1][1].get('extra_headers', [])) == (replacement or {})
    finally:
        await client.disconnect()


async def test_real_authenticated_server_accepts_automatic_reconnect(monkeypatch):
    from http import HTTPStatus
    from websockets.exceptions import ConnectionClosed
    from websockets.legacy.server import serve

    headers, requests = [], []

    async def authenticate(path, received):
        headers.append(received.get('Authorization'))
        if received.get('Authorization') != 'Bearer authenticated':
            return HTTPStatus.UNAUTHORIZED, [], b'unauthorized'
        return None

    async def server(ws, path):
        await ws.send(json.dumps({
            'type': 'event', 'event': 'connection.ack', 'payload': {'status': 'ready'},
        }))
        try:
            async for raw in ws:
                request = json.loads(raw)
                requests.append(request['request_id'])
                await ws.send(json.dumps(encode_agent_response_for_wire(
                    AgentResponse(request['request_id'], request['channel'], payload={'received': True}),
                    response_id=request['request_id'],
                )))
                if request['request_id'] == 'first':
                    await ws.close(code=1011, reason='test connection interruption')
        except ConnectionClosed:
            pass

    async with serve(server, '127.0.0.1', 0, process_request=authenticate) as listener:
        port = listener.sockets[0].getsockname()[1]
        client = WebSocketAgentServerClient()
        try:
            await client.connect(f'ws://127.0.0.1:{port}/agent/ws', extra_headers={
                'Authorization': 'Bearer authenticated',
            })
            response = await client.send_request(E2AEnvelope(
                request_id='first', channel='web', method='session.list', params={},
            ), timeout=1)
            assert response.ok
            await asyncio.wait_for(client._receiver_task, 2)
            response = await client.send_request(E2AEnvelope(
                request_id='second', channel='web', method='session.list', params={},
            ), timeout=1)
            assert response.ok
            assert headers == ['Bearer authenticated', 'Bearer authenticated']
            assert requests == ['first', 'second']
        finally:
            await client.disconnect()


async def test_retired_socket_advertisement_is_never_inherited_by_a_waiter(monkeypatch):
    """A request waiting for the writer belongs to one socket and its capabilities."""
    calls, sockets = transport(monkeypatch)
    client = WebSocketAgentServerClient()
    try:
        await client.connect('ws://localhost/agent/ws')
        client._apply_connection_ack({'payload': {'capabilities': ['market_skill_install_v1']}})
        envelope = E2AEnvelope(
            request_id='market-install', channel='web', method='skills.install',
            params={'spec': 'fixture@market'},
        )
        assert (await client.send_request(
            envelope, timeout=1, required_capability='market_skill_install_v1',
        )).ok
        async with client._lock:
            task = asyncio.create_task(client.send_request(
                envelope, timeout=1, required_capability='market_skill_install_v1',
            ))
            await asyncio.sleep(0)
            assert not task.done()
            assert 'market-install' in client._message_queues
            await drop(client, sockets[0])
            await client._ensure_connected_for_request()
        # The waiter is not replayed onto the replacement socket.
        with pytest.raises(RequestNotSentError, match='closed before send'):
            await task
        assert len(calls) == 2
        assert len(sockets[0].sent) == 1 and sockets[1].sent == []
        # The replacement socket never advertised the capability.
        with pytest.raises(AgentCapabilityUnavailable, match='does not support'):
            await client.send_request(envelope, timeout=1, required_capability='market_skill_install_v1')
        assert sockets[1].sent == []
        assert 'market-install' not in client._message_queues
    finally:
        await client.disconnect()
