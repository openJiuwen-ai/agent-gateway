# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bound actual Control blocking work, including work outliving its caller."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
import math
import threading
from typing import Any, TypeVar

from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceError

_Result = TypeVar('_Result')


def _consume_result(future: asyncio.Future[Any]) -> None:
    """Retrieve abandoned exceptions without logging their possibly secret text."""
    if not future.cancelled():
        future.exception()


class ControlBlockingWork:
    """Same-loop submissions to a private pool with no admission waiting queue.

    Caller cancellation never returns an occupied slot. Only completion of the
    concurrent future does so. Construction starts no threads; the owner must
    call ``stop`` before closing its event loop. A timed-out stop leaves actual
    work observable through ``pending_count`` and does not claim to kill it.
    """

    def __init__(self, limit: int = 4) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError('limit must be a positive integer')
        self._limit = limit
        self._executor = ThreadPoolExecutor(max_workers=limit, thread_name_prefix='control-blocking')
        self._lock = threading.Lock()
        self._pending: set[Future[Any]] = set()
        self._count = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._idle = asyncio.Event()
        self._idle.set()
        self._closed = False
        self._stop_task: asyncio.Task[None] | None = None

    @property
    def pending_count(self) -> int:
        """Number of actual occupied slots, even after a caller or loop exits."""
        with self._lock:
            return self._count

    def _owner_loop(self) -> asyncio.AbstractEventLoop:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise ControlServiceError() from None
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise ControlServiceError()
        return loop

    def submit(self, fn: Callable[..., _Result], *args: Any) -> Future[_Result]:
        """Reserve a real slot synchronously or fail immediately without queuing."""
        self._owner_loop()
        if self._closed:
            raise ControlServiceError()
        with self._lock:
            if self._count >= self._limit:
                raise ControlServiceBusy()
            self._count += 1
        self._idle.clear()
        try:
            future = self._executor.submit(fn, *args)
        except Exception:
            with self._lock:
                self._count -= 1
            self._notify_completion()
            raise ControlServiceError() from None
        with self._lock:
            self._pending.add(future)
        # Register outside the lock: add_done_callback may execute immediately.
        future.add_done_callback(self._work_finished)
        return future

    async def run(self, fn: Callable[..., _Result], *args: Any) -> _Result:
        """Await a submitted call without cancelling its actual work on timeout."""
        future = asyncio.wrap_future(self.submit(fn, *args))
        future.add_done_callback(_consume_result)
        return await asyncio.shield(future)

    def _work_finished(self, future: Future[Any]) -> None:
        # This can run on a pool thread, including after the owner loop closes.
        with self._lock:
            self._pending.remove(future)
            self._count -= 1
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._notify_completion)
            except RuntimeError:
                # A closed loop cannot be notified; actual-work tracking is done.
                pass

    def _notify_completion(self) -> None:
        if self.pending_count == 0:
            self._idle.set()

    async def _wait_for_work(self, timeout: float) -> None:
        try:
            async with asyncio.timeout(timeout):
                while self.pending_count:
                    await self._idle.wait()
        except TimeoutError:
            if self.pending_count:
                raise ControlServiceError() from None

    async def stop(self, timeout: float = 1.0) -> None:
        """Reject new calls and finish owned, bounded cleanup despite cancellation.

        Concurrent stop callers share the current cleanup deadline. After a
        timeout a later stop can wait again, but submissions remain closed.
        Residual work raises only a fixed error and remains counted until done.
        """
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout < 0:
            raise ValueError('timeout must be a non-negative finite number')
        loop = self._owner_loop()
        if not self._closed:
            self._closed = True
            self._executor.shutdown(wait=False, cancel_futures=False)
        if self._stop_task is None or self._stop_task.done():
            self._stop_task = loop.create_task(self._wait_for_work(float(timeout)))
            self._stop_task.add_done_callback(_consume_result)
        task = self._stop_task
        cancelled = False
        while True:
            try:
                await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.cancelled():
                    raise
                cancelled = True
            except ControlServiceError:
                if cancelled:
                    raise asyncio.CancelledError from None
                raise
        if cancelled:
            raise asyncio.CancelledError
