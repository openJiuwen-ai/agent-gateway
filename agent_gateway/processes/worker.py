# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Private subprocess entry point for one data Worker."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import logging
import os
import socket
import sys
from typing import Any

from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.common.southbound_routes import StaticRouteDirectory
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceBusy, ControlServiceClient, ControlServiceIdentity,
)
from agent_gateway.processes.management_protocol import validate_message
from agent_gateway.processes.pool import STATIC_SOUTHBOUND, management_frame
from agent_gateway.transport.handoff import HandoffReceiver
from agent_gateway.transport.handoff_channel import HandoffChannel
from agent_gateway.transport.handoff_protocol import HandoffIdentity

logger = logging.getLogger("gateway")


class _WorkerFailure(RuntimeError):
    """A failure that has a specific machine-readable management code."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


async def _await_stop(management: ManagementChannel, identity: HandoffIdentity, cron, receive: list) -> dict:
    """Resolve to the STOP frame, activating the owned Cron shard on CRON_ACTIVATE.

    ``receive[0]`` is the Worker's single pending management read. It is
    replaced here and never cancelled: a cancelled read closes the channel
    that still has to report ``CRON_ACTIVE`` or a failure code.

    Preparation and activation are separate phases: no Cron work starts until
    every sibling slot has reported ``CRON_PREPARED`` and the parent says so.
    A repeated or premature activation is a protocol failure. A STOP that
    arrives mid-activation cancels the activation.
    """
    activation = None
    activated = False
    try:
        while True:
            waiting = (receive[0],) if activation is None else (receive[0], activation)
            done, _pending = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            if activation is not None and activation in done:
                task, activation = activation, None
                try:
                    await task
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    raise _WorkerFailure("CRON_ACTIVATION") from error
                activated = True
                await management.send(management_frame("CRON_ACTIVE", identity, pid=os.getpid()))
                continue
            frame = validate_message(receive[0].result(), expected=vars(identity))
            if frame["type"] == "STOP":
                return frame
            if (frame["type"] != "CRON_ACTIVATE" or cron is None or activated
                    or activation is not None):
                # Only the protocol's own phase name is reported, never the
                # frame content.
                raise _WorkerFailure("MANAGEMENT_PROTOCOL")
            activation = asyncio.create_task(cron[0].activate(), name="gateway-worker-cron-activation")
            receive[0] = asyncio.create_task(management.receive(), name="gateway-worker-stop-frame")
    finally:
        if activation is not None:
            activation.cancel()
            await asyncio.gather(activation, return_exceptions=True)


def _parse_role_arguments(arguments: list[str]):
    """Return ``(control_fd, accounts, cron_slot, cron_manifest)`` for this slot.

    With Cron flags the Control client is Cron-only unless ``--accounts`` is
    explicit, so a Cron slot never inherits account services by accident.
    """
    return _parse_worker_role(arguments)[:4]


def _parse_worker_role(arguments: list[str]):
    """Return the role of this slot and its optional static route directory."""
    control_fd = accounts = cron_slot = cron_manifest = southbound_routes = None
    positionals: list[str] = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--accounts":
            accounts = True
        elif argument == "--cron-slot":
            index += 1
            if index >= len(arguments) or not arguments[index].isdigit():
                raise ValueError("--cron-slot requires a slot number")
            cron_slot = int(arguments[index])
        elif argument == "--cron-slots":
            index += 1
            if index >= len(arguments):
                raise ValueError("--cron-slots requires a slot list")
            try:
                cron_manifest = WorkerManifest(tuple(int(item) for item in arguments[index].split(",")))
            except ValueError as error:
                raise ValueError("--cron-slots requires a slot list") from error
        elif argument == "--southbound-routes-json":
            index += 1
            if index >= len(arguments) or southbound_routes is not None:
                raise ValueError("--southbound-routes-json requires one directory")
            southbound_routes = StaticRouteDirectory.from_json(arguments[index])
        elif argument.startswith("--"):
            raise ValueError(f"unknown Worker argument: {argument}")
        else:
            positionals.append(argument)
        index += 1
    if len(positionals) > 1:
        raise ValueError("unexpected positional Worker arguments")
    if positionals:
        if not positionals[0].isdigit():
            raise ValueError("the Control fd must be a number")
        control_fd = int(positionals[0])
    if accounts and control_fd is None:
        raise ValueError("Worker accounts require a Control fd")
    if (cron_slot is None) != (cron_manifest is None):
        raise ValueError("sharded Cron requires --cron-slot and --cron-slots")
    if cron_slot is not None and (control_fd is None or cron_slot not in cron_manifest.slots):
        raise ValueError("Worker Cron requires a Control fd and a slot of the manifest")
    if cron_slot is None:
        # The original contract: a Control fd alone serves the account services.
        accounts = control_fd is not None
    return control_fd, bool(accounts), cron_slot, cron_manifest, southbound_routes


async def _run(
    management_fd: int, transfer_fd: int, agent_server_url: str, control_fd: int | None = None, *,
    accounts: bool = True, cron_slot: int | None = None, cron_manifest: WorkerManifest | None = None,
    southbound_routes: StaticRouteDirectory | None = None,
) -> None:
    from agent_gateway.application.worker_data import data_worker
    from agent_gateway.application.worker_routed_client import RoutedTargetsUnreachable

    management = ManagementChannel(socket.socket(fileno=management_fd))
    transfer = HandoffChannel(socket.socket(fileno=transfer_fd))
    control_channel = ControlServiceChannel(socket.socket(fileno=control_fd)) if control_fd is not None else None
    control = credentials = None
    cron_enabled = cron_slot is not None
    owned: dict[str, Any] = {}
    identity = None
    # The Worker's single pending management read, once it exists. Cancelling
    # a read closes the channel, so it is released in the outer ``finally``:
    # after STOPPED, or FAILED with its code, has been sent.
    receive: list = []
    try:
        init = await management.receive(timeout=10)
        if init["type"] != "INIT":
            raise RuntimeError("expected Worker INIT")
        identity = HandoffIdentity(init["group_id"], init["slot_id"], init["generation"])
        if cron_enabled and identity.slot_id != cron_slot:
            raise RuntimeError("Worker Cron slot does not match the Worker identity")
        if southbound_routes is not None or agent_server_url == STATIC_SOUTHBOUND:
            # A directory or role this Worker cannot serve is a configuration
            # failure, before anything is built or any target is contacted.
            try:
                if southbound_routes is None:
                    raise ValueError("the static Worker marker requires a route directory")
                if agent_server_url != STATIC_SOUTHBOUND:
                    raise ValueError("a static Worker takes its targets from the directory only")
                southbound_routes.targets_for_slot(identity.slot_id)
                if cron_manifest is not None:
                    if cron_manifest.slots != southbound_routes.manifest.slots:
                        raise ValueError("Cron manifest disagrees with the static directory")
                    cron_manifest = southbound_routes.manifest
            except ValueError as error:
                raise _WorkerFailure("SOUTHBOUND_CONFIGURATION") from error
            agent_server_url = None
        await management.send(management_frame("READY", identity, pid=os.getpid(), stage="management_ready"))
        receive.append(asyncio.create_task(management.receive(), name="gateway-worker-stop-frame"))
        if control_channel is not None:
            async def invalidate(arguments):
                if credentials is None:
                    raise RuntimeError("Worker credential services not ready")
                await credentials.handle_invalidation(arguments)

            async def deliver_cron_command(arguments: dict[str, Any]) -> None:
                cron = owned.get("cron")
                if cron is None:
                    raise ControlServiceBusy()
                await cron[1].accept(arguments)

            def attach_credentials(value):
                nonlocal credentials
                credentials = value

            event_handlers = ({"cron.command_deliver": deliver_cron_command}
                              if cron_enabled and southbound_routes is None else {})
            if accounts:
                event_handlers["credentials.invalidate"] = invalidate
            control = ControlServiceClient(
                control_channel,
                ControlServiceIdentity(identity.group_id, identity.slot_id, identity.generation,
                                       os.getpid(), os.getppid()),
                event_handlers=event_handlers,
            )
            await control.start()
        async with AsyncExitStack() as worker_scope:
            # A single-AgentServer Worker keeps its original keyword contract.
            static = ({"southbound_routes": southbound_routes, "slot_id": identity.slot_id}
                      if southbound_routes is not None else {})
            try:
                runtimes, web_runtime, cron = await worker_scope.enter_async_context(data_worker(
                    agent_server_url, control_client=control, accounts=accounts,
                    on_credentials=attach_credentials if control is not None and accounts else None,
                    cron_slot=cron_slot, cron_manifest=cron_manifest, **static,
                ))
            except RoutedTargetsUnreachable as error:
                raise _WorkerFailure("SOUTHBOUND_UNREACHABLE") from error
            except ValueError as error:
                if southbound_routes is None:
                    raise
                # A directory, slot or role the static Worker cannot serve.
                raise _WorkerFailure("SOUTHBOUND_CONFIGURATION") from error
            owned["cron"] = cron
            await management.send(management_frame("RUNTIME_READY", identity, pid=os.getpid(),
                                                   runtime_ids=list(runtimes)))
            if cron is not None:
                await management.send(management_frame("CRON_PREPARED", identity, pid=os.getpid()))
            receiver = HandoffReceiver(transfer, identity, runtimes)

            async def receive_connections() -> None:
                while True:
                    await receiver.receive_one()

            handoff = asyncio.create_task(receive_connections(), name="gateway-worker-handoff")
            web_closed = asyncio.create_task(web_runtime.wait_closed(), name="gateway-worker-web-runtime")
            # ``command`` resolves to the STOP frame. With Cron it also
            # answers CRON_ACTIVATE between preparation and stop.
            command = (asyncio.create_task(_await_stop(management, identity, cron, receive),
                                           name="gateway-worker-commands")
                       if cron is not None else receive[0])
            tasks = [command, handoff, web_closed]
            if control is not None:
                tasks.append(asyncio.create_task(control.wait(), name="gateway-worker-control"))
            if cron is not None:
                tasks.append(asyncio.create_task(cron[0].wait(), name="gateway-worker-cron"))
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if command not in done:
                    for task in done:
                        task.result()
                    raise RuntimeError("Worker runtime closed unexpectedly")
                frame = command.result()
                if frame["type"] != "STOP":
                    raise RuntimeError("expected Worker STOP")
            finally:
                # The management read itself is left to the outer block.
                pending = tasks[1:] + ([command] if command is not receive[0] else [])
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        await management.send(management_frame("STOPPED", identity, pid=os.getpid()), timeout=2)
    except _WorkerFailure as failure:
        logger.exception("Gateway Worker failed: %s", failure.error_code)
        await _report_failure(management, identity, failure.error_code)
        raise
    except Exception:
        logger.exception("Gateway Worker failed")
        await _report_failure(management, identity, "WORKER_FAILURE")
        raise
    finally:
        try:
            for pending_read in receive:
                pending_read.cancel()
                await asyncio.gather(pending_read, return_exceptions=True)
        finally:
            try:
                if control is not None:
                    await control.stop()
            finally:
                management.close()
                transfer.close()
                if control_channel is not None:
                    control_channel.close()


async def _report_failure(management: ManagementChannel, identity, error_code: str) -> None:
    if identity is None:
        return
    try:
        await management.send(management_frame("FAILED", identity, pid=os.getpid(),
                                               error_code=error_code), timeout=2)
    except (ConnectionError, OSError, TimeoutError):
        pass


if __name__ == "__main__":
    # Positional arguments keep the original contract; role options follow them.
    control_fd, accounts, cron_slot, cron_manifest, southbound_routes = _parse_worker_role(sys.argv[4:])
    asyncio.run(_run(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], control_fd, accounts=accounts,
                     cron_slot=cron_slot, cron_manifest=cron_manifest, southbound_routes=southbound_routes))
