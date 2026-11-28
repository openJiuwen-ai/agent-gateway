# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Manage a fixed generation of data Workers. A lost slot stops the group."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import socket
import sys
import uuid

from agent_gateway.processes.capabilities import management_preflight
from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.processes.management_protocol import PROTOCOL_VERSION, validate_message
from agent_gateway.transport.handoff import HandoffRejected, HandoffSender
from agent_gateway.transport.handoff_channel import HandoffChannel
from agent_gateway.transport.handoff_protocol import HandoffIdentity

RUNTIME_IDS = ("web-http", "web-ws", "protocol")


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


class WorkerPool:
    def __init__(self, agent_server_url: str, count: int, *, startup_timeout: float = 30) -> None:
        if type(count) is not int or not 1 <= count <= 32:
            raise ValueError("number of Workers must be between 1 and 32")
        self.slots = tuple(range(count))
        self._agent_server_url = agent_server_url
        self._startup_timeout = startup_timeout
        self._group_id = uuid.uuid4().hex
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
        manager, child_manager = socket.socketpair()
        try:
            transfer, child_transfer = socket.socketpair()
            try:
                # Keep the spawn task owned if start is cancelled during fork/exec.
                spawn = asyncio.create_task(asyncio.create_subprocess_exec(
                    sys.executable, "-m", "agent_gateway.processes.worker",
                    str(child_manager.fileno()), str(child_transfer.fileno()), self._agent_server_url,
                    pass_fds=(child_manager.fileno(), child_transfer.fileno()),
                    stdin=asyncio.subprocess.DEVNULL,
                ))
                try:
                    process = await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    process = await spawn
                    self._register(process, manager, transfer, identity)
                    raise
                self._register(process, manager, transfer, identity)
            except BaseException:
                if len(self._workers) == slot:
                    transfer.close()
                raise
            finally:
                child_transfer.close()
        except BaseException:
            if len(self._workers) == slot:
                manager.close()
            raise
        finally:
            child_manager.close()

    def _register(self, process, manager, transfer, identity) -> None:
        self._workers.append(_Worker(
            process, ManagementChannel(manager), HandoffSender(HandoffChannel(transfer), identity),
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
        frame = await self._expect(worker, "RUNTIME_READY")
        if set(frame["runtime_ids"]) != set(RUNTIME_IDS):
            raise RuntimeError("Worker runtime manifest does not match")

    async def _expect(self, worker: _Worker, kind: str) -> dict:
        frame = validate_message(await worker.management.receive(), expected=vars(worker.sender.identity))
        if frame["type"] != kind or frame["pid"] != worker.process.pid:
            slot = worker.sender.identity.slot_id
            raise RuntimeError(f"Worker {slot}: expected {kind}, received {frame['type']}")
        return frame

    async def _watch(self, worker: _Worker) -> None:
        slot = worker.sender.identity.slot_id
        try:
            await self._expect(worker, "STOPPED")
        except Exception as error:
            if self._stop_task is None:
                self._fail(RuntimeError(f"Worker {slot} management channel failed: {type(error).__name__}"))
            return
        if self._stop_task is None:
            self._fail(RuntimeError(f"Worker {slot} stopped unexpectedly"))

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
        if not self._ready or worker.handoff_waiters >= 16 or worker.process.returncode is not None:
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
            if watcher is not None:
                await asyncio.gather(watcher, return_exceptions=True)
