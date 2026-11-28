# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Manage a fixed generation of data Workers. A lost slot stops the group."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from dataclasses import dataclass, field
import logging
import os
import socket
import sys
import uuid

from agent_gateway.common.southbound_routes import StaticRouteDirectory
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.processes.capabilities import management_preflight
from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission, ControlServiceIdentity, ControlServices, ControlServiceServer,
)
from agent_gateway.processes.management_protocol import PROTOCOL_VERSION, validate_message
from agent_gateway.transport.handoff import HandoffRejected, HandoffSender
from agent_gateway.transport.handoff_channel import HandoffChannel
from agent_gateway.transport.handoff_protocol import HandoffIdentity

logger = logging.getLogger("gateway")

RUNTIME_IDS = ("web-http", "web-ws", "protocol")
# The Worker positional that names one AgentServer carries this marker when a
# static route directory follows as an option instead.
STATIC_SOUTHBOUND = "static"


class _WorkerFrameError(RuntimeError):
    """A Worker reported a management phase that was not expected."""


def management_frame(kind: str, identity: HandoffIdentity, **fields) -> dict:
    return dict(type=kind, protocol_version=PROTOCOL_VERSION, group_id=identity.group_id,
                slot_id=identity.slot_id, generation=identity.generation, **fields)


@dataclass
class _Worker:
    process: asyncio.subprocess.Process
    management: ManagementChannel
    sender: HandoffSender
    adopted: int = 0
    watcher: asyncio.Task | None = None
    handoff_waiters: int = 0
    handoff_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    control_socket: socket.socket | None = None
    control: ControlServiceServer | None = None
    control_watch: asyncio.Task | None = None
    cron_active: asyncio.Event = field(default_factory=asyncio.Event)
    cron_activate_sent: bool = False


class WorkerPool:
    def __init__(
        self, agent_server_url: str | None, count: int, *, startup_timeout: float = 30,
        control_services: ControlServices | None = None, cron_manifest: WorkerManifest | None = None,
        account_services: bool = False, southbound_routes: StaticRouteDirectory | None = None,
    ) -> None:
        """Spawn ``count`` Workers over one AgentServer or over ``southbound_routes``.

        A static pool hands every Worker the same directory. Each Worker keeps
        the whole manifest and serves the targets of its own slot.
        """
        if type(count) is not int or not 1 <= count <= 32:
            raise ValueError("number of Workers must be between 1 and 32")
        self.slots = tuple(range(count))
        if southbound_routes is not None:
            if type(southbound_routes) is not StaticRouteDirectory:
                raise ValueError("static routes must be an explicit static route directory")
            if southbound_routes.manifest.slots != self.slots:
                raise ValueError("the static route manifest must cover the complete Worker range")
            if agent_server_url is not None:
                raise ValueError("a static Worker pool takes its targets from the directory only")
        elif agent_server_url is None:
            raise ValueError("a Worker pool requires an AgentServer URL or a static route directory")
        self.southbound_routes = southbound_routes
        if cron_manifest is not None:
            if type(cron_manifest) is not WorkerManifest or cron_manifest.slots != self.slots:
                raise ValueError("Worker Cron manifest must cover the complete Worker range")
            if control_services is None:
                raise ValueError("Worker Cron requires control services")
        if type(account_services) is not bool:
            raise ValueError("account services must be an explicit boolean")
        if account_services and control_services is None:
            raise ValueError("Worker account services require control services")
        self.cron_manifest = cron_manifest
        # A Control fd alone keeps the original account contract. It is stated
        # explicitly for a Cron slot, whose Control client is Cron-only otherwise.
        self._account_services = account_services
        self._cron_activation_in_progress = False
        self._agent_server_url = agent_server_url
        self._startup_timeout = startup_timeout
        self._group_id = uuid.uuid4().hex
        self._control_services = control_services
        self._control_admission = (ControlServiceAdmission(control_services.max_group_handlers)
                                   if control_services is not None else None)
        self._workers: list[_Worker] = []
        self._failure = asyncio.get_running_loop().create_future()
        self._startup: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None
        self._started = False
        self._ready = False

    @property
    def processes(self) -> tuple[asyncio.subprocess.Process, ...]:
        return tuple(worker.process for worker in self._workers)

    @property
    def adopted_counts(self) -> tuple[int, ...]:
        return tuple(worker.adopted for worker in self._workers)

    async def start(self) -> None:
        if self._started or self._stop_task is not None:
            raise RuntimeError("Worker pool is single-use")
        preflight = management_preflight()
        if not preflight.supported or not hasattr(socket, "SCM_RIGHTS"):
            raise RuntimeError(f"Worker pool platform unsupported: {preflight.reasons}")
        self._started = True
        startup = self._startup = asyncio.create_task(self._start(), name="gateway-start-workers")
        try:
            await asyncio.shield(startup)
            if self._stop_task is not None:
                raise RuntimeError("Worker pool stopped during startup")
            self._ready = True
        except BaseException:
            await self.stop()
            raise
        finally:
            self._startup = None

    async def _start(self) -> None:
        for slot in self.slots:
            await self._spawn(slot)
        async with asyncio.timeout(self._startup_timeout):
            await self._wait_ready()

    async def _spawn(self, slot: int) -> None:
        identity = HandoffIdentity(self._group_id, slot, 1)
        with ExitStack() as children, ExitStack() as parents:
            def pair() -> tuple[socket.socket, int]:
                left, right = socket.socketpair()
                return parents.enter_context(left), children.enter_context(right).fileno()

            manager, manager_fd = pair()
            transfer, transfer_fd = pair()
            args = [
                str(manager_fd), str(transfer_fd),
                STATIC_SOUTHBOUND if self.southbound_routes is not None else self._agent_server_url,
            ]
            pass_fds = [manager_fd, transfer_fd]
            control = None
            if self._control_services is not None:
                control, control_fd = pair()
                args.append(str(control_fd))
                pass_fds.append(control_fd)
            if self._account_services:
                args.append("--accounts")
            if self.cron_manifest is not None:
                args += ["--cron-slot", str(slot), "--cron-slots", ",".join(map(str, self.slots))]
            if self.southbound_routes is not None:
                # A bounded, non-secret snapshot: at most 16 KiB per Worker.
                args += ["--southbound-routes-json", self.southbound_routes.to_json()]

            # Keep the spawn task owned if start is cancelled during fork/exec.
            spawn = asyncio.create_task(asyncio.create_subprocess_exec(
                sys.executable, "-m", "agent_gateway.processes.worker", *args,
                pass_fds=pass_fds, stdin=asyncio.subprocess.DEVNULL,
            ))
            try:
                process = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                process = await spawn
                self._register(process, manager, transfer, identity, control)
                parents.pop_all()
                raise
            self._register(process, manager, transfer, identity, control)
            parents.pop_all()

    def _register(self, process, manager, transfer, identity, control=None) -> None:
        self._workers.append(_Worker(
            process, ManagementChannel(manager), HandoffSender(HandoffChannel(transfer), identity),
            control_socket=control,
        ))

    async def _wait_ready(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for worker in self._workers:
                tasks.create_task(self._initialize(worker))
        for worker in self._workers:
            worker.watcher = asyncio.create_task(self._watch(worker), name="gateway-watch-worker")

    async def _initialize(self, worker: _Worker) -> None:
        await worker.management.send(management_frame("INIT", worker.sender.identity))
        await self._expect(worker, "READY")
        if worker.control_socket is not None:
            services = self._control_services
            identity = worker.sender.identity
            control_identity = ControlServiceIdentity(
                identity.group_id, identity.slot_id, identity.generation, worker.process.pid, os.getpid(),
            )
            handlers = (services.handlers_factory(control_identity) if services.handlers_factory
                        else services.handlers)
            control = worker.control = ControlServiceServer(
                ControlServiceChannel(worker.control_socket), control_identity, handlers=handlers,
                admission=self._control_admission, max_handlers=services.max_handlers,
            )
            await control.start()
            if services.on_connect is not None:
                services.on_connect(control)
            worker.control_watch = asyncio.create_task(self._watch_control(worker))
        frame = await self._expect(worker, "RUNTIME_READY")
        if set(frame["runtime_ids"]) != set(RUNTIME_IDS):
            raise RuntimeError("Worker runtime manifest does not match")
        if self.cron_manifest is not None:
            # Prepared, not started: the slot has validated its owned Cron
            # shard and now waits for the common CRON_ACTIVATE below.
            await self._expect(worker, "CRON_PREPARED")

    async def _watch_control(self, worker: _Worker) -> None:
        try:
            await worker.control.wait()
        except Exception:
            pass
        if self._stop_task is None:
            self._fail(RuntimeError(f"Worker {worker.sender.identity.slot_id} Control channel closed"))

    async def _expect(self, worker: _Worker, kinds: str | tuple[str, ...]) -> dict:
        expected = (kinds,) if isinstance(kinds, str) else kinds
        frame = validate_message(await worker.management.receive(), expected=vars(worker.sender.identity))
        if frame["type"] not in expected or frame["pid"] != worker.process.pid:
            slot = worker.sender.identity.slot_id
            received = frame["type"]
            if received == "FAILED":
                received += f" ({frame['error_code']})"
            raise _WorkerFrameError(f"Worker {slot}: expected {' or '.join(expected)}, received {received}")
        return frame

    async def _watch(self, worker: _Worker) -> None:
        slot = worker.sender.identity.slot_id
        try:
            while True:
                frame = await self._expect(worker, ("CRON_ACTIVE", "STOPPED"))
                if frame["type"] == "STOPPED":
                    break
                if (self.cron_manifest is None or not worker.cron_activate_sent
                        or worker.cron_active.is_set()):
                    raise _WorkerFrameError(f"Worker {slot}: unexpected CRON_ACTIVE")
                worker.cron_active.set()
        except Exception as error:
            if self._stop_task is None:
                # Only the protocol's own frame names and error codes are
                # echoed; a decode failure keeps its type name alone.
                failure = error if isinstance(error, _WorkerFrameError) else RuntimeError(
                    f"Worker {slot} management channel failed: {type(error).__name__}"
                )
                self._fail(failure)
            return
        if self._stop_task is None:
            self._fail(RuntimeError(f"Worker {slot} stopped unexpectedly"))

    async def activate_cron(self) -> None:
        """Start every owned Cron shard once all fixed slots are prepared.

        A slot starts only after every sibling reported ``CRON_PREPARED``, and
        the group admits no connection until every slot reported
        ``CRON_ACTIVE``. A failed or cancelled activation stops the pool: a
        partly activated group must not keep serving Cron traffic.
        """
        if self.cron_manifest is None:
            raise RuntimeError("Worker Cron activation is not enabled")
        if self._stop_task is not None or not self._ready or not self._workers:
            raise RuntimeError("Worker Cron activation requires a ready Worker pool")
        if self._cron_activation_in_progress:
            raise RuntimeError("Worker Cron activation is already in progress")
        if all(worker.cron_active.is_set() for worker in self._workers):
            raise RuntimeError("Worker Cron is already active")

        async def activated() -> None:
            await asyncio.gather(*(worker.cron_active.wait() for worker in self._workers))

        async def pool_failed(self) -> RuntimeError | None:
            # The first Worker failure, or None once a stop finished first.
            return await asyncio.shield(self._failure)

        self._cron_activation_in_progress = True
        activation = asyncio.create_task(activated(), name="gateway-activate-cron")
        failure = asyncio.create_task(pool_failed(self), name="gateway-cron-pool-failure")
        cancelled = False
        try:
            async with asyncio.timeout(self._startup_timeout):
                for worker in self._workers:
                    worker.cron_activate_sent = True
                await asyncio.gather(*(
                    worker.management.send(management_frame("CRON_ACTIVATE", worker.sender.identity))
                    for worker in self._workers
                ))
                done, _pending = await asyncio.wait(
                    (activation, failure), return_when=asyncio.FIRST_COMPLETED,
                )
                if failure in done:
                    raise failure.result() or RuntimeError(
                        "Worker pool stopped during Cron activation"
                    )
        except TimeoutError as error:
            await self._abort_cron_activation()
            raise RuntimeError("Worker Cron activation timed out") from error
        except asyncio.CancelledError:
            cancelled = True
        except RuntimeError:
            # The pool already holds the first Worker failure; report it as is.
            await self._abort_cron_activation()
            raise
        except BaseException as error:
            await self._abort_cron_activation()
            raise RuntimeError(f"Worker Cron activation failed: {type(error).__name__}") from error
        finally:
            for task in (activation, failure):
                task.cancel()
            await asyncio.gather(activation, failure, return_exceptions=True)
            self._cron_activation_in_progress = False
        if cancelled:
            # Some slots may already run their shard; the group cannot recover.
            self._fail(RuntimeError("Worker Cron activation was cancelled"))
            await self.stop()
            raise asyncio.CancelledError

    async def _abort_cron_activation(self) -> None:
        """Keep the first observed Worker failure and reap the whole pool."""
        if not self._failure.done():
            self._fail(RuntimeError("Worker Cron activation did not complete"))
        await self.stop()

    def _fail(self, error: RuntimeError) -> None:
        if not self._failure.done():
            self._ready = False
            self._failure.set_result(error)

    async def wait(self) -> None:
        failure = await asyncio.shield(self._failure)
        if failure is not None:
            raise failure

    async def handoff(self, slot: int, client: socket.socket, runtime: str, prefix: bytes, *, timeout: float) -> None:
        worker = self._workers[slot]
        if (not self._ready or worker.handoff_waiters >= 16 or worker.process.returncode is not None
                or (self.cron_manifest is not None and not worker.cron_active.is_set())):
            raise HandoffRejected("Worker not available for handoff")
        worker.handoff_waiters += 1
        try:
            async with asyncio.timeout(timeout):
                async with worker.handoff_lock:
                    if not self._ready:
                        raise HandoffRejected("Worker pool stopped admission")
                    await worker.sender.handoff(client, runtime, prefix, timeout=timeout)
                    worker.adopted += 1
        finally:
            worker.handoff_waiters -= 1
            if worker.sender.channel.closed and self._stop_task is None:
                self._fail(RuntimeError(f"Worker {slot} handoff channel closed"))

    async def stop(self) -> None:
        if self._stop_task is None:
            self._ready = False
            self._stop_task = asyncio.create_task(self._stop(), name="gateway-stop-workers")
        await asyncio.shield(self._stop_task)

    async def _stop(self) -> None:
        try:
            startup = self._startup
            if startup is not None and not startup.done():
                startup.cancel()
                await asyncio.gather(startup, return_exceptions=True)
            await asyncio.gather(*(self._stop_worker(worker) for worker in self._workers))
        finally:
            if not self._failure.done():
                self._failure.set_result(None)

    async def _stop_worker(self, worker: _Worker) -> None:
        process = worker.process
        watcher = worker.watcher
        try:
            if process.returncode is None:
                try:
                    async with asyncio.timeout(12):
                        await worker.management.send(management_frame("STOP", worker.sender.identity))
                        if watcher is not None:
                            await asyncio.shield(watcher)
                        await process.wait()
                except (ConnectionError, OSError, TimeoutError):
                    pass
        finally:
            worker.management.close()
            worker.sender.channel.close()
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except TimeoutError:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
            if process.returncode not in (None, 0):
                # A non-zero stop is how a Worker reports unconfirmed cleanup,
                # for example an AgentServer run it could not confirm stopped.
                logger.warning("[App] Worker %s exited with status %s during stop",
                               worker.sender.identity.slot_id, process.returncode)
            if watcher is not None:
                await asyncio.gather(watcher, return_exceptions=True)
            if worker.control_watch is not None:
                worker.control_watch.cancel()
                await asyncio.gather(worker.control_watch, return_exceptions=True)
            if worker.control is not None:
                try:
                    await worker.control.stop()
                finally:
                    if self._control_services.on_disconnect is not None:
                        self._control_services.on_disconnect(worker.control.identity)
            elif worker.control_socket is not None:
                worker.control_socket.close()
