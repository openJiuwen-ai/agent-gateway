# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Target-oriented data-plane adapter for the extracted connection engines."""

from __future__ import annotations

from contextlib import aclosing
from typing import AsyncIterator

from e2a.models import E2AEnvelope
from agent_gateway.common.security.link_mtls import LinkMTLSConfig
from agent_gateway.southbound.contracts import (
    ConnectionTarget, DeliveryState, Source, Sourced, StreamResult,
    TransportError, TransportKind, UnaryResult,
)
from agent_gateway.southbound.data.connection_pool import ConnectionPool
from agent_gateway.southbound.data.http_sse import HttpSseConnection
from agent_gateway.southbound.data.interfaces import PushHandler
from agent_gateway.southbound.data.push_hub import sourced_push_handler
from agent_gateway.southbound.data.websocket import DuplicateRequestIdError, WebSocketConnection

Connection = WebSocketConnection | HttpSseConnection


class AgentDataTransport:
    """No platform lookup, request replay or business retry decisions."""

    def __init__(self) -> None:
        self._pool = ConnectionPool(self._open, self._disconnect)

    async def start(self) -> None:
        await self._pool.start()

    async def _open(self, target: ConnectionTarget) -> Connection:
        if target.transport is TransportKind.WEBSOCKET:
            if target.binding is not None:
                raise ValueError("WebSocket binding is not supported by this connection engine")
            client = WebSocketConnection()
        elif target.transport is TransportKind.HTTP_SSE:
            config = LinkMTLSConfig.from_env()
            if target.binding is not None and target.binding != config.identity:
                raise ValueError("target binding does not match configured identity")
            client = HttpSseConnection(link_mtls_config=config, extra_headers=dict(target.headers))
        else:
            raise ValueError("unsupported agent transport")
        try:
            if isinstance(client, WebSocketConnection):
                await client.connect(target.endpoint, extra_headers=dict(target.headers))
            else:
                await client.connect(target.endpoint)
            return client
        except BaseException:
            await client.disconnect()
            raise

    @staticmethod
    async def _disconnect(client: Connection) -> None:
        await client.disconnect()

    async def connect(self, target: ConnectionTarget) -> None:
        await self._pool.get(target)

    async def disconnect(self, target: ConnectionTarget) -> None:
        await self._pool.disconnect(target)

    async def close(self) -> None:
        await self._pool.close()

    @staticmethod
    def _source(target: ConnectionTarget, envelope: E2AEnvelope) -> Source:
        return Source(target, str(envelope.request_id or "") or None,
                      str(envelope.session_id or "") or None)

    async def _get_for_request(self, target: ConnectionTarget, source: Source) -> Connection:
        try:
            return await self._pool.get(target)
        except Exception as exc:
            raise TransportError("connection setup failed", delivery=DeliveryState.NOT_SENT,
                                 source=source) from exc

    @staticmethod
    def _failure(exc: Exception, source: Source) -> TransportError:
        state = DeliveryState.MAY_HAVE_EXECUTED
        if isinstance(exc, DuplicateRequestIdError):
            state = DeliveryState.NOT_SENT
        return TransportError("request transport failed", delivery=state, source=source)

    async def send_request(self, target: ConnectionTarget, envelope: E2AEnvelope,
                           *, timeout: float | None = None) -> UnaryResult:
        source = self._source(target, envelope)
        client = await self._get_for_request(target, source)
        try:
            payload = await client.send_request(envelope, timeout=timeout)
        except Exception as exc:
            raise self._failure(exc, source) from exc
        return Sourced(payload, source)

    async def send_request_stream(self, target: ConnectionTarget,
                                  envelope: E2AEnvelope) -> AsyncIterator[StreamResult]:
        source = self._source(target, envelope)
        client = await self._get_for_request(target, source)
        try:
            async with aclosing(client.send_request_stream(envelope)) as stream:
                async for chunk in stream:
                    yield Sourced(chunk, Source(target, str(chunk.request_id or "") or source.request_id,
                                                source.session_id))
        except Exception as exc:
            raise self._failure(exc, source) from exc

    async def subscribe(self, target: ConnectionTarget, handler: PushHandler) -> None:
        client = await self._pool.get(target)
        client.set_server_push_handler(sourced_push_handler(target, handler))
        if isinstance(client, HttpSseConnection):
            try:
                await client.wait_push_ready()
            except BaseException:
                await client.stop_push()
                raise

    async def unsubscribe(self, target: ConnectionTarget) -> None:
        client = self._pool.peek(target)
        if client is not None:
            await client.stop_push()
