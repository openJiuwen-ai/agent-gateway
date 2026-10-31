# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Gateway process restart and task-waiting lifecycle."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field

from agent_gateway.common.process_supervision import (
    GATEWAY_RESTART_EXIT_CODE,
    SUPERVISOR_PID_ENV,
)

logger = logging.getLogger("gateway")


def running_under_supervisor() -> bool:
    """Whether our jiuwenswarm.app supervisor is alive right now and really is our parent.

    On Windows os.getppid() keeps reporting the original PID even after the
    supervisor dies (no reparenting), so a stale env match alone would make us
    os._exit(GATEWAY_RESTART_EXIT_CODE) with nobody left to respawn. A recycled
    PID necessarily belongs to a process younger than us, so requiring the
    parent to be live and not created after us rejects both cases.
    """
    if os.environ.get(SUPERVISOR_PID_ENV) != str(os.getppid()):
        return False
    import psutil

    try:
        return psutil.Process(os.getppid()).create_time() <= psutil.Process().create_time()
    except psutil.Error:
        return False


def exec_gateway_restart() -> None:
    if running_under_supervisor():
        # Under jiuwenswarm.app, let the supervisor respawn us. os.execv on
        # Windows starts a new PID and exits this one with 0, which the
        # supervisor reads as the Gateway stopping and tears the service down.
        # os._exit keeps execv's no-cleanup semantics; the OS frees the ports
        # and the gateway lock.
        logger.info("[App] requesting Gateway restart from supervisor")
        os._exit(GATEWAY_RESTART_EXIT_CODE)  # pylint: disable=protected-access  # public API; underscore is historical
    if os.environ.get(SUPERVISOR_PID_ENV):
        # Env says we were spawned by a supervisor, yet the parent check in
        # running_under_supervisor failed. execv exits 0, which that live
        # supervisor reads as "Gateway stopped" and tears the WHOLE stack
        # down (AgentServer included) — log the deciding values so a later
        # post-mortem does not have to guess.
        import psutil

        try:
            parent = psutil.Process(os.getppid())
            parent_desc = f"alive, created {parent.create_time():.0f}"
        except psutil.Error:
            parent_desc = "not running"
        logger.warning(
            "[App] %s=%s but parent pid %s is %s; execv restart will be read "
            "as a stop by the supervisor",
            SUPERVISOR_PID_ENV,
            os.environ.get(SUPERVISOR_PID_ENV),
            os.getppid(),
            parent_desc,
        )
    logger.info("[App] .env updated, restarting Gateway...")
    os.execv(sys.executable, [sys.executable, *sys.argv])


@dataclass
class GatewayRestartRequest:
    requested: bool = False
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)


def schedule_gateway_restart(
        restart_request: GatewayRestartRequest | None = None,
        *,
        delay: float = 2.0,
) -> None:
    if restart_request is not None:
        restart_request.requested = True

    def _request_restart() -> None:
        if restart_request is not None:
            restart_request.ready_event.set()
            return
        exec_gateway_restart()

    try:
        loop = asyncio.get_running_loop()
        loop.call_later(delay, _request_restart)
    except RuntimeError:
        _request_restart()


async def wait_for_gateway_tasks_or_restart(
    tasks_to_wait: list[asyncio.Task],
    restart_request: GatewayRestartRequest,
    shutdown_event: asyncio.Event,
) -> bool:
    restart_task = asyncio.create_task(restart_request.ready_event.wait(), name="gateway-restart")
    shutdown_task = asyncio.create_task(shutdown_event.wait(), name="gateway-shutdown")
    service_tasks = set(tasks_to_wait)
    wait_tasks = set(service_tasks)
    wait_tasks.update((restart_task, shutdown_task))
    try:
        while wait_tasks:
            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
            if shutdown_task in done:
                return False
            if restart_task in done:
                return restart_request.requested or restart_request.ready_event.is_set()
            for task in done:
                wait_tasks.discard(task)
                service_tasks.discard(task)
                if task.cancelled():
                    raise asyncio.CancelledError
                exc = task.exception()
                if exc is not None:
                    if restart_request.requested:
                        logger.warning("[App] service task failed while Gateway restart is pending: %s", exc)
                        return True
                    raise exc
            if not service_tasks:
                return restart_request.requested or restart_request.ready_event.is_set()
        return restart_request.requested or restart_request.ready_event.is_set()
    finally:
        if not restart_task.done():
            restart_task.cancel()
            try:
                await restart_task
            except asyncio.CancelledError:
                pass
        if not shutdown_task.done():
            shutdown_task.cancel()
            try:
                await shutdown_task
            except asyncio.CancelledError:
                pass
