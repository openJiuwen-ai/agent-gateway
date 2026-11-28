# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Private subprocess entry point for one data Worker."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys

from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import ControlServiceClient, ControlServiceIdentity
from agent_gateway.processes.management_protocol import validate_message
from agent_gateway.processes.pool import management_frame
from agent_gateway.transport.handoff import HandoffReceiver
from agent_gateway.transport.handoff_channel import HandoffChannel
from agent_gateway.transport.handoff_protocol import HandoffIdentity

logger = logging.getLogger("gateway")


async def _run(management_fd: int, transfer_fd: int, agent_server_url: str, control_fd: int | None = None) -> None:
    from agent_gateway.application.worker_data import data_worker

    management = ManagementChannel(socket.socket(fileno=management_fd))
    transfer = HandoffChannel(socket.socket(fileno=transfer_fd))
    control_channel = ControlServiceChannel(socket.socket(fileno=control_fd)) if control_fd is not None else None
    control = credentials = None
    identity = None
    try:
        init = await management.receive(timeout=10)
        if init["type"] != "INIT":
            raise RuntimeError("expected Worker INIT")
        identity = HandoffIdentity(init["group_id"], init["slot_id"], init["generation"])
        await management.send(management_frame("READY", identity, pid=os.getpid(), stage="management_ready"))
        stop = asyncio.create_task(management.receive(), name="gateway-worker-stop-frame")
        try:
            if control_channel is not None:
                async def invalidate(arguments):
                    if credentials is None:
                        raise RuntimeError("Worker credential services not ready")
                    await credentials.handle_invalidation(arguments)

                def attach_credentials(value):
                    nonlocal credentials
                    credentials = value

                control = ControlServiceClient(
                    control_channel,
                    ControlServiceIdentity(identity.group_id, identity.slot_id, identity.generation,
                                           os.getpid(), os.getppid()),
                    event_handlers={"credentials.invalidate": invalidate},
                )
                await control.start()
            async with data_worker(agent_server_url, control_client=control,
                                   on_credentials=attach_credentials if control is not None else None) as (
                runtimes, web_runtime,
            ):
                await management.send(management_frame("RUNTIME_READY", identity, pid=os.getpid(),
                                                       runtime_ids=list(runtimes)))
                receiver = HandoffReceiver(transfer, identity, runtimes)

                async def receive_connections() -> None:
                    while True:
                        await receiver.receive_one()

                handoff = asyncio.create_task(receive_connections(), name="gateway-worker-handoff")
                web_closed = asyncio.create_task(web_runtime.wait_closed(), name="gateway-worker-web-runtime")
                tasks = [stop, handoff, web_closed]
                if control is not None:
                    tasks.append(asyncio.create_task(control.wait()))
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    if stop not in done:
                        for task in done:
                            task.result()
                        raise RuntimeError("Worker runtime closed unexpectedly")
                    frame = validate_message(stop.result(), expected=vars(identity))
                    if frame["type"] != "STOP":
                        raise RuntimeError("expected Worker STOP")
                finally:
                    for task in tasks[1:]:
                        task.cancel()
                    await asyncio.gather(*tasks[1:], return_exceptions=True)
        finally:
            stop.cancel()
            await asyncio.gather(stop, return_exceptions=True)
        await management.send(management_frame("STOPPED", identity, pid=os.getpid()), timeout=2)
    except Exception:
        logger.exception("Gateway Worker failed")
        if identity is not None:
            try:
                await management.send(management_frame("FAILED", identity, pid=os.getpid(),
                                                       error_code="WORKER_FAILURE"), timeout=2)
            except (ConnectionError, OSError, TimeoutError):
                pass
        raise
    finally:
        try:
            if control is not None:
                await control.stop()
        finally:
            management.close()
            transfer.close()
            if control_channel is not None:
                control_channel.close()


if __name__ == "__main__":
    asyncio.run(_run(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3],
                     int(sys.argv[4]) if len(sys.argv) == 5 else None))
