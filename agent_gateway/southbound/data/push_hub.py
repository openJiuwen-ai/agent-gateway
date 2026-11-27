# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Push provenance and Runtime-compatible per-endpoint subscriptions."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from weakref import WeakValueDictionary

from agent_gateway.southbound.contracts import ConnectionTarget, Source, Sourced
from agent_gateway.southbound.data.interfaces import PushHandler
from agent_gateway.southbound.data.http_sse import HttpSseConnection

logger = logging.getLogger(__name__)


def sourced_push_handler(target: ConnectionTarget, handler: PushHandler):
    async def receive(wire: dict[str, object]) -> None:
        await handler(Sourced(wire, Source(
            target,
            request_id=str(wire.get("request_id") or "") or None,
            session_id=str(wire.get("session_id") or "") or None,
        )))

    return receive


class EndpointPushPool:
    """Legacy Runtime's URL-keyed push subscriptions.

    The caller selects endpoints and handles business correlation. Target-keyed
    clients use ConnectionPool instead; this preserves the existing Runtime API.
    """

    def __init__(self, factory: Callable[[], HttpSseConnection]) -> None:
        self._factory = factory
        self.clients: dict[str, HttpSseConnection] = {}
        self._locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._guard = asyncio.Lock()
        self._drop_tasks: set[asyncio.Task[None]] = set()
        self._pending: set[asyncio.Task] = set()
        self._running = False

    def start(self) -> None:
        self._running = True

    async def _lock_for(self, endpoint: str) -> asyncio.Lock:
        async with self._guard:
            lock = self._locks.get(endpoint)
            if lock is None:
                lock = self._locks[endpoint] = asyncio.Lock()
            return lock

    async def ensure(self, endpoint: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        if not self._running:
            raise RuntimeError("client not connected")
        task = asyncio.current_task()
        if task is not None:
            self._pending.add(task)
        stale = None
        try:
            async with await self._lock_for(endpoint):
                if not self._running:
                    raise RuntimeError("client not connected")
                client = self.clients.get(endpoint)
                if client is not None and not getattr(client, "_running", True):
                    stale = self.clients.pop(endpoint)
                    client = None
                if client is None:
                    client = self._factory()
                    client.set_server_push_handler(handler)
                    try:
                        await client.connect(endpoint)
                        await client.wait_push_ready()
                        if not self._running:
                            raise RuntimeError("client not connected")
                    except BaseException:
                        await client.disconnect()
                        raise
                    self.clients[endpoint] = client
                    client.add_push_done_callback(
                        lambda _task: self._schedule_drop(endpoint, client))
            await client.wait_push_ready()
        finally:
            try:
                if stale is not None:
                    await self._close_client(stale)
            finally:
                self._pending.discard(task)

    def _schedule_drop(self, endpoint: str, client: HttpSseConnection) -> None:
        if self._running:
            task = asyncio.create_task(self._drop(endpoint, client), name="runtime-drop-pod-push")
            self._drop_tasks.add(task)
            task.add_done_callback(self._drop_tasks.discard)

    async def _drop(self, endpoint: str, client: HttpSseConnection) -> None:
        async with await self._lock_for(endpoint):
            if self.clients.get(endpoint) is not client:
                return
            self.clients.pop(endpoint)
        await self._close_client(client)

    @staticmethod
    async def _close_client(client: HttpSseConnection) -> None:
        try:
            await client.disconnect()
        except Exception:
            logger.warning("Failed to disconnect routed push client", exc_info=True)

    async def endpoints(self) -> list[str]:
        async with self._guard:
            return list(self.clients)

    async def close(self) -> None:
        self._running = False
        pending = [task for task in self._pending if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        async with self._guard:
            clients = list(self.clients.values())
            self.clients.clear()
            self._locks.clear()
        await asyncio.gather(*list(self._drop_tasks), return_exceptions=True)
        await asyncio.gather(*(self._close_client(client) for client in clients))
