"""Workspace ownership and graceful shutdown for Gateway entrypoints."""

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import signal
from types import FrameType
from typing import Any, NoReturn

from agent_gateway.gateway_lock import GatewayLock


def _force_exit(status_code: int) -> NoReturn:
    os._exit(status_code)


@contextmanager
def gateway_workspace_lock(workspace: Path) -> Iterator[GatewayLock]:
    """Hold the existing workspace lock for one Gateway lifetime."""
    lock = GatewayLock(workspace)
    if not lock.acquire():
        logging.getLogger("gateway").error(
            "[App] Another Gateway is already serving this workspace; "
            "refusing duplicate instance (lock: %s)",
            lock.lock_path,
        )
        raise SystemExit(1)
    try:
        yield lock
    finally:
        lock.release()


async def run_with_shutdown_signals(
    run: Callable[[asyncio.Event], Awaitable[None]],
    cleanup: Callable[[], Awaitable[None]],
) -> None:
    """Run on the main thread with idempotent signal-driven shutdown."""
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    previous_handlers: dict[signal.Signals, Any] = {}
    shutdown_requested = False

    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        nonlocal shutdown_requested
        if shutdown_requested:
            logging.getLogger("gateway").warning(
                "[App] Second shutdown signal received; forcing exit with status %s",
                signum + 128,
            )
            _force_exit(signum + 128)
        shutdown_requested = True
        loop.call_soon_threadsafe(shutdown_event.set)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, request_shutdown)
        await run(shutdown_event)
    finally:
        try:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
        finally:
            await cleanup()
