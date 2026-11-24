# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Owned connections keyed by complete, resolved target snapshots."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Generic, TypeVar

from agent_gateway.southbound.contracts import ConnectionTarget

C = TypeVar("C")


class ConnectionPool(Generic[C]):
    """Coalesce concurrent connects; different targets connect independently.

    The factory owns partial connection cleanup on failure. Cancelled waiters
    do not cancel shared connection setup. No routing or resource allocation.
    """

    def __init__(self, connect: Callable[[ConnectionTarget], Awaitable[C]],
                 disconnect: Callable[[C], Awaitable[None]]) -> None:
        self._connect = connect
        self._disconnect = disconnect
        self._entries: dict[ConnectionTarget, asyncio.Task[C]] = {}
        self._running = False
        self._closing = asyncio.Lock()

    async def start(self) -> None:
        async with self._closing:
            self._running = True

    async def get(self, target: ConnectionTarget) -> C:
        async with self._closing:
            if not self._running:
                raise RuntimeError("transport is not started")
            task = self._entries.get(target)
            if task is None:
                task = asyncio.create_task(self._connect(target))
                self._entries[target] = task
                task.add_done_callback(lambda done: self._on_done(target, done))
        result = await asyncio.shield(task)
        if self._entries.get(target) is not task:
            raise RuntimeError("connection was retired during setup")
        return result

    def _on_done(self, target: ConnectionTarget, task: asyncio.Task[C]) -> None:
        if task.cancelled() or task.exception() is not None:
            if self._entries.get(target) is task:
                self._entries.pop(target, None)

    def peek(self, target: ConnectionTarget) -> C | None:
        task = self._entries.get(target)
        if task is not None and task.done() and not task.cancelled() and task.exception() is None:
            return task.result()
        return None

    async def _dispose(self, task: asyncio.Task[C]) -> None:
        if not task.done():
            task.cancel()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        if not isinstance(result, BaseException):
            await self._disconnect(result)

    async def disconnect(self, target: ConnectionTarget) -> None:
        async with self._closing:
            task = self._entries.pop(target, None)
            if task is not None:
                await self._dispose(task)

    async def close(self) -> None:
        async with self._closing:
            self._running = False
            tasks = list(self._entries.values())
            self._entries.clear()
            results = await asyncio.gather(*(self._dispose(task) for task in tasks),
                                           return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
