# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from contextlib import aclosing
from unittest.mock import AsyncMock

import httpx
import pytest

from e2a.models import E2AEnvelope
from agent_gateway.southbound.contracts import ConnectionTarget, DeliveryState, TransportError, TransportKind
from agent_gateway.southbound.data.connection_pool import ConnectionPool
from agent_gateway.southbound.data.http_sse import HttpSseConnection
from agent_gateway.southbound.data.push_hub import EndpointPushPool, sourced_push_handler
from agent_gateway.southbound.data.transport import AgentDataTransport


@pytest.mark.asyncio
async def test_pool_coalesces_connect_but_not_different_generations():
    first = ConnectionTarget(TransportKind.WEBSOCKET, "ws://agent", "id", "1")
    second = ConnectionTarget(TransportKind.WEBSOCKET, "ws://agent", "id", "2")
    factory = AsyncMock(side_effect=lambda _: object())
    close = AsyncMock()
    pool = ConnectionPool(factory, close)
    await pool.start()
    a, b, c = await asyncio.gather(pool.get(first), pool.get(first), pool.get(second))
    assert a is b and a is not c
    assert factory.await_count == 2
    await pool.close()
    await pool.close()
    assert close.await_count == 2
    with pytest.raises(RuntimeError):
        await pool.get(first)


@pytest.mark.asyncio
async def test_cancelled_pool_waiter_does_not_cancel_other_waiter():
    target = ConnectionTarget(TransportKind.WEBSOCKET, "ws://agent")
    started, release = asyncio.Event(), asyncio.Event()
    async def factory(_):
        started.set()
        await release.wait()
        return object()
    close = AsyncMock()
    pool = ConnectionPool(factory, close)
    await pool.start()
    first = asyncio.create_task(pool.get(target))
    await started.wait()
    second = asyncio.create_task(pool.get(target))
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    assert await second is pool.peek(target)
    await pool.close()
    close.assert_awaited_once()


@pytest.mark.asyncio
async def test_http_transport_unary_and_stream_preserve_wire_and_source(monkeypatch):
    target = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod", "pod")
    async def respond(request):
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"ok": True})
        if request.method == "GET":
            return httpx.Response(200, json={"ok": True, "data": {"value": 1}})
        return httpx.Response(200, text='data: {"ok": true, "request_id": "r", "payload": {"value": 2}}\n\n')
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        connection = HttpSseConnection(http_client=http)
        await connection.connect(target.endpoint)
        transport = AgentDataTransport()
        monkeypatch.setattr(transport._pool, "_connect", AsyncMock(return_value=connection))
        await transport.start()
        result = await transport.send_request(target, E2AEnvelope(method="session.list", request_id="r"))
        assert result.payload.payload == {"value": 1}
        assert result.source.target is target
        async with aclosing(transport.send_request_stream(target, E2AEnvelope(
            method="chat.send", request_id="r", session_id="s",
        ))) as stream:
            chunk = await anext(stream)
            assert chunk.payload.payload == {"value": 2}
            assert chunk.source.target is target
        await transport.close()
        assert not http.is_closed


@pytest.mark.asyncio
async def test_connection_failure_not_sent_and_push_origin_preserved(monkeypatch):
    target = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod", "pod")
    transport = AgentDataTransport()
    monkeypatch.setattr(transport._pool, "_connect", AsyncMock(side_effect=OSError("unavailable")))
    await transport.start()
    with pytest.raises(TransportError) as caught:
        await transport.send_request(target, E2AEnvelope(method="session.list", request_id="r"))
    assert caught.value.delivery is DeliveryState.NOT_SENT
    assert isinstance(caught.value.__cause__, OSError)
    await transport.close()
    handler = AsyncMock()
    wire = {"request_id": "rpc", "session_id": "s"}
    await sourced_push_handler(target, handler)(wire)
    pushed = handler.await_args.args[0]
    assert pushed.payload is wire
    assert pushed.source.target is target
    assert pushed.source.request_id == "rpc"


@pytest.mark.asyncio
async def test_runtime_push_pool_drops_only_matching_connection():
    first, second = AsyncMock(), AsyncMock()
    first._running = second._running = True
    clients = iter((first, second))
    pool = EndpointPushPool(lambda: next(clients))
    pool.start()
    handler = AsyncMock()
    await asyncio.gather(pool.ensure("http://pod", handler), pool.ensure("http://pod", handler))
    first.connect.assert_awaited_once()
    first._running = False
    await pool.ensure("http://pod", handler)
    await pool._drop("http://pod", first)
    assert pool.clients["http://pod"] is second
    first.disconnect.assert_awaited_once()
    await pool.close()
    second.disconnect.assert_awaited_once()
    assert await pool.endpoints() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("stream", [False, True])
async def test_http_target_status_preserves_transport_failure(status, stream, monkeypatch):
    target = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod", "pod")

    async def respond(request):
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(status, json={"ok": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        connection = HttpSseConnection(http_client=http)
        await connection.connect(target.endpoint)
        transport = AgentDataTransport()
        monkeypatch.setattr(transport._pool, "_connect", AsyncMock(return_value=connection))
        await transport.start()
        envelope = E2AEnvelope(method="chat.send" if stream else "session.list", request_id="r", session_id="s")
        with pytest.raises(TransportError) as caught:
            if stream:
                async for _ in transport.send_request_stream(target, envelope):
                    pass
            else:
                await transport.send_request(target, envelope)
        assert isinstance(caught.value.__cause__, httpx.HTTPStatusError)
        assert caught.value.__cause__.response.status_code == status
        await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", [False, True])
async def test_pool_retirement_reports_not_sent_to_waiter(retire, monkeypatch):
    target = ConnectionTarget(TransportKind.WEBSOCKET, "ws://agent")
    started = asyncio.Event()

    async def connect(_):
        started.set()
        await asyncio.Event().wait()

    transport = AgentDataTransport()
    monkeypatch.setattr(transport._pool, "_connect", connect)
    await transport.start()
    task = asyncio.create_task(transport.send_request(target, E2AEnvelope(method="session.list", request_id="r")))
    await asyncio.wait_for(started.wait(), timeout=2)
    if retire:
        await transport.disconnect(target)
    else:
        await transport.close()
    with pytest.raises(TransportError) as caught:
        await asyncio.wait_for(task, timeout=2)
    assert caught.value.delivery is DeliveryState.NOT_SENT
    await transport.close()
