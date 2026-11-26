# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
import json
from contextlib import aclosing
from unittest.mock import AsyncMock

import pytest
from websockets.exceptions import ConnectionClosedError

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.message import ReqMethod
from e2a.models import E2AEnvelope
from e2a.wire_codec import encode_agent_chunk_for_wire, encode_agent_response_for_wire

from agent_gateway.routing.e2a_proxy import _agent_unavailable, fetch_agent_unary, is_legacy_shared_directory_client
from agent_gateway.southbound.assembly import build_static_client
from agent_gateway.southbound.contracts import ConnectionTarget, Source, TransportKind
from agent_gateway.southbound.control.local import FixedTargetControl
from agent_gateway.southbound.data.websocket import AgentServerUnaryTimeout, DuplicateRequestIdError
from agent_gateway.southbound.orchestration.client import SouthboundAgentClient
from agent_gateway.southbound.orchestration.interfaces import RouteRequest
from agent_gateway.southbound.orchestration.static_router import StaticRouter


URL = "ws://agentserver:18092"


class Socket:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.frames.put_nowait(json.dumps({
            "type": "event", "event": "connection.ack",
            "payload": {"status": "ready", "readiness": "AGENT_READY"},
        }))
        self.sent = []
        self.closed = False

    async def recv(self):
        frame = await self.frames.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def send(self, raw):
        request = json.loads(raw)
        self.sent.append(request)
        rid = request["request_id"]
        if request["is_stream"]:
            response = AgentResponseChunk(request_id=rid, channel_id="web", payload={"value": 2})
            wire = encode_agent_chunk_for_wire(response, response_id=rid, sequence=0)
        else:
            response = AgentResponse(request_id=rid, channel_id="web", payload={"value": 1})
            wire = encode_agent_response_for_wire(response, response_id=rid)
        self.frames.put_nowait(json.dumps(wire))

    async def close(self):
        self.closed = True


@pytest.fixture
def sockets(monkeypatch):
    created = []

    async def connect(uri, **kwargs):
        assert uri == URL
        assert kwargs["ping_interval"] == 20.0
        assert kwargs["ping_timeout"] == 600.0
        socket = Socket()
        created.append(socket)
        return socket

    monkeypatch.setattr("websockets.legacy.client.connect", connect)
    return created


@pytest.mark.parametrize("shared_directory_layout", [False, True])
async def test_static_client_preserves_unary_stream_push_and_reconnect(sockets, shared_directory_layout):
    client = build_static_client(URL, shared_directory_layout=shared_directory_layout)
    pushed = asyncio.Event()
    received = []

    async def handle(wire):
        received.append(wire)
        pushed.set()

    try:
        assert isinstance(client, SouthboundAgentClient)
        assert is_legacy_shared_directory_client(client) is shared_directory_layout
        assert _agent_unavailable(client)
        await client.connect(URL)
        assert client.server_ready and client.agent_ready
        client.set_server_push_handler(handle)
        response = await client.send_request(E2AEnvelope(method="session.list", request_id="r", channel="web"), timeout=1)
        assert response.payload == {"value": 1}
        async with aclosing(client.send_request_stream(E2AEnvelope(method="chat.send", request_id="s", channel="web"))) as stream:
            assert (await anext(stream)).payload == {"value": 2}
        assert not client._transport._connection._message_queues
        push = {"request_id": "push", "metadata": {E2A_WIRE_SERVER_PUSH_KEY: True}}
        sockets[0].frames.put_nowait(json.dumps(push))
        await asyncio.wait_for(pushed.wait(), 1)
        assert received == [push]
        sockets[0].frames.put_nowait(ConnectionClosedError(None, None))
        await asyncio.wait_for(client._transport._connection._receiver_task, 1)
        assert not client.server_ready and client.can_reconnect
        assert not _agent_unavailable(client)
        ok, payload = await fetch_agent_unary(agent_client=client, req_method=ReqMethod.SESSION_LIST,
                                             params={}, session_id=None, user_id=None, channel_id="web",
                                             timeout_seconds=1)
        assert ok and payload == {"value": 1}
        assert len(sockets) == 2
        assert [wire["request_id"] for wire in sockets[0].sent] == ["r", "s"]
        assert len(sockets[1].sent) == 1
        pushed.clear()
        sockets[1].frames.put_nowait(json.dumps(push))
        await asyncio.wait_for(pushed.wait(), 1)
        assert received == [push, push]
    finally:
        await client.disconnect()
    assert sockets[-1].closed
    assert _agent_unavailable(client)
    with pytest.raises(RuntimeError):
        await client.send_request(E2AEnvelope(method="session.list", request_id="closed"))


def test_shared_directory_layout_is_explicit_for_static_client():
    assert not is_legacy_shared_directory_client(build_static_client(URL))
    assert not is_legacy_shared_directory_client(build_static_client(URL, shared_directory_layout=False))
    assert is_legacy_shared_directory_client(build_static_client(URL, shared_directory_layout=True))


async def test_connect_retry_after_failed_handshake(monkeypatch, sockets):
    from websockets.legacy import client as websocket_client

    client = build_static_client(URL)
    connect = websocket_client.connect
    failed = AsyncMock(side_effect=OSError("unavailable"))
    monkeypatch.setattr("websockets.legacy.client.connect", failed)
    with pytest.raises(OSError, match="unavailable"):
        await client.connect(URL)
    assert not client.server_ready and not client.can_reconnect
    monkeypatch.setattr("websockets.legacy.client.connect", connect)
    await client.connect(URL)
    assert client.server_ready
    await client.disconnect()


async def test_static_route_preserves_origin_and_never_allocates():
    target = ConnectionTarget(TransportKind.WEBSOCKET, URL)
    router = StaticRouter(FixedTargetControl(target))
    envelope = E2AEnvelope(method="session.list", session_id="session")
    async with router.resolve(RouteRequest(envelope, allow_create=False, origin=Source(target))) as selection:
        assert selection.targets == (target,)
        assert selection.session_id == "session"
    other = ConnectionTarget(TransportKind.WEBSOCKET, "ws://other")
    with pytest.raises(ValueError, match="origin"):
        async with router.resolve(RouteRequest(envelope, allow_create=True, origin=Source(other))):
            pytest.fail("different origin accepted")


async def test_request_errors_cancellation_and_shutdown_are_compatible(sockets):
    client = build_static_client(URL)
    await client.connect(URL)
    socket = sockets[0]
    sent = asyncio.Event()

    async def send(raw):
        socket.sent.append(json.loads(raw))
        sent.set()

    socket.send = send
    connection = client._transport._connection
    first = asyncio.create_task(client.send_request(E2AEnvelope(method="session.list", request_id="pending")))
    try:
        await asyncio.wait_for(sent.wait(), 1)
        with pytest.raises(DuplicateRequestIdError):
            await client.send_request(E2AEnvelope(method="session.list", request_id="pending"))
        assert len(socket.sent) == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not connection._message_queues
        with pytest.raises(AgentServerUnaryTimeout) as timeout:
            await client.send_request(E2AEnvelope(method="session.list", request_id="timeout"), timeout=0.01)
        assert isinstance(timeout.value.__cause__, TimeoutError)
        sent.clear()
        pending = asyncio.create_task(client.send_request(E2AEnvelope(method="session.list", request_id="shutdown")))
        await asyncio.wait_for(sent.wait(), 1)
        await client.disconnect()
        with pytest.raises(RuntimeError, match="connection closed"):
            await asyncio.wait_for(pending, 1)
        assert not connection._message_queues
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await client.disconnect()
