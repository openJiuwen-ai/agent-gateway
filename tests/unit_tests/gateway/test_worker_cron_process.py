# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Cron preparation and activation phases of the private Worker entry point.

The Worker is driven in-process over real socket pairs; only the data assembly
is replaced, so every management and Control frame below is the wire contract.
"""

import asyncio
from contextlib import asynccontextmanager
import os
import socket
import sys
import types

import pytest

from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.processes import worker
from agent_gateway.processes.channel import ManagementChannel
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission, ControlServiceBusy, ControlServiceIdentity, ControlServiceServer,
)

GROUP = "cron-group"
SLOT = 2
MANIFEST = WorkerManifest((0, 1, 2))


def frame(kind: str, pid: int, **fields) -> dict:
    return {"protocol_version": 1, "group_id": GROUP, "slot_id": SLOT, "generation": 1,
            "type": kind, "pid": pid, **fields}


def parent_frame(kind: str, **fields) -> dict:
    return {"protocol_version": 1, "group_id": GROUP, "slot_id": SLOT, "generation": 1,
            "type": kind, **fields}


class CronRuntime:
    def __init__(self, *, fail: bool = False, block: bool = False, wait_fails: bool = False) -> None:
        self.fail = fail
        self.block = block
        self.wait_fails = wait_fails
        self.activate_called = asyncio.Event()
        self.release = asyncio.Event()
        self.stop_called = asyncio.Event()
        self.activation_cancelled = False
        self.activations = 0

    async def activate(self) -> None:
        self.activations += 1
        self.activate_called.set()
        try:
            if self.block:
                await self.release.wait()
        except asyncio.CancelledError:
            self.activation_cancelled = True
            raise
        if self.fail:
            raise RuntimeError("cron activation failed")

    async def wait(self) -> None:
        if self.wait_fails:
            await self.activate_called.wait()
            raise RuntimeError("cron scheduler exited")
        await self.stop_called.wait()

    async def stop(self) -> None:
        self.stop_called.set()


class WebRuntime:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def wait_closed(self) -> None:
        await asyncio.Future()

    async def stop(self) -> None:
        self._log.append("web.stop")


def install_data_worker(monkeypatch, cron_runtime, log, builds, shards, *, gate=None):
    @asynccontextmanager
    async def data_worker(agent_server_url, *, control_client=None, on_credentials=None,
                          accounts=True, cron_slot=None, cron_manifest=None):
        builds.append({"url": agent_server_url, "control": control_client, "accounts": accounts,
                       "cron_slot": cron_slot, "cron_manifest": cron_manifest})
        if gate is not None:
            await gate.wait()
        runtimes = {"web-http": object(), "web-ws": object(), "protocol": object()}
        cron = None
        if cron_slot is not None:
            commands = types.SimpleNamespace(accepted=[])
            commands.accept = commands.accepted.append
            # ``accept`` is awaited by the Worker and must run its argument check.
            async def accept(arguments):
                if not isinstance(arguments, dict):
                    raise TypeError("command must be an object")
                commands.accepted.append(arguments)
            commands.accept = accept
            cron = (cron_runtime, commands)
            shards.append(commands)
        web_runtime = WebRuntime(log)
        log.append("enter")
        try:
            yield runtimes, web_runtime, cron
        finally:
            # Same order as the real assembly: northbound runtimes, then the
            # shard, while the southbound connection is still open.
            await web_runtime.stop()
            if cron is not None:
                log.append("cron.stop")
                await cron_runtime.stop()

    module = types.ModuleType("agent_gateway.application.worker_data")
    module.data_worker = data_worker
    monkeypatch.setitem(sys.modules, "agent_gateway.application.worker_data", module)


@asynccontextmanager
async def running_worker(cron_runtime, *, cron_slot=SLOT, init_slot=SLOT, manifest=MANIFEST):
    management_parent, management_child = socket.socketpair()
    transfer_parent, transfer_child = socket.socketpair()
    service_parent, service_child = socket.socketpair()
    channel = ManagementChannel(management_parent)
    server = ControlServiceServer(
        ControlServiceChannel(service_parent),
        ControlServiceIdentity(GROUP, init_slot, 1, os.getpid(), os.getppid()),
        handlers={}, admission=ControlServiceAdmission(),
    )
    server_task = asyncio.create_task(server.start(timeout=5))
    task = asyncio.create_task(worker._run(
        management_child.detach(), transfer_child.detach(), "ws://127.0.0.1:8000/agent",
        service_child.detach(), accounts=False, cron_slot=cron_slot, cron_manifest=manifest,
    ))
    try:
        yield channel, task, server
    finally:
        channel.close()
        transfer_parent.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(server_task, return_exceptions=True)
        await server.stop()


async def initialize(channel, pid=os.getpid()):
    await channel.send(parent_frame("INIT"), timeout=2)
    assert await channel.receive(timeout=5) == frame("READY", pid, stage="management_ready")
    assert await channel.receive(timeout=5) == frame(
        "RUNTIME_READY", pid, runtime_ids=["web-http", "web-ws", "protocol"])
    return await channel.receive(timeout=5)


@pytest.mark.parametrize(
    "arguments,expected",
    [
        ([], (None, False, None, None)),
        (["7"], (7, True, None, None)),
        (["--accounts", "7"], (7, True, None, None)),
        (["7", "--cron-slot", "2", "--cron-slots", "0,1,2"], (7, False, 2, WorkerManifest((0, 1, 2)))),
        (["--cron-slot", "2", "--cron-slots", "2,0", "--accounts", "7"], (7, True, 2, WorkerManifest((2, 0)))),
    ],
)
def test_role_arguments_keep_the_original_account_contract(arguments, expected):
    assert worker._parse_role_arguments(arguments) == expected


@pytest.mark.parametrize(
    "arguments",
    [
        ["--accounts"],
        ["7", "--accounts", "8"],
        ["seven"],
        ["--cron-slot", "2"],
        ["--cron-slots", "0,1"],
        ["--cron-slot", "2", "--cron-slots", "0,1"],
        ["--cron-slot", "x", "--cron-slots", "0,1"],
        ["--cron-slot", "2", "--cron-slots", "0,x"],
        ["--cron-slot", "2", "--cron-slots", ""],
        ["--cron-slot", "2", "--cron-slots"],
        ["--cron-slot"],
        ["7", "--unknown"],
        ["7", "8", "9"],
    ],
)
def test_role_arguments_refuse_incomplete_or_inconsistent_roles(arguments):
    with pytest.raises(ValueError):
        worker._parse_role_arguments(arguments)


async def test_cron_slot_is_prepared_but_not_started_until_activate(monkeypatch):
    runtime, log, builds = CronRuntime(), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        assert builds[0]["cron_slot"] == SLOT and builds[0]["cron_manifest"] == MANIFEST
        assert builds[0]["accounts"] is False and builds[0]["control"] is not None
        await asyncio.sleep(0.05)
        assert not runtime.activate_called.is_set() and not task.done()

        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame("CRON_ACTIVE", os.getpid())
        assert runtime.activations == 1 and not runtime.stop_called.is_set()

        await channel.send(parent_frame("STOP"), timeout=2)
        assert await channel.receive(timeout=5) == frame("STOPPED", os.getpid())
        assert await asyncio.wait_for(task, 5) is None
    # Northbound runtimes stop first; the shard still sees a live southbound.
    assert log == ["enter", "web.stop", "cron.stop"] and runtime.stop_called.is_set()


async def test_stop_after_prepare_never_activates_the_shard(monkeypatch):
    runtime, log, builds = CronRuntime(), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("STOP"), timeout=2)
        assert await channel.receive(timeout=5) == frame("STOPPED", os.getpid())
        assert await asyncio.wait_for(task, 5) is None
    assert runtime.activations == 0 and runtime.stop_called.is_set()


async def test_stop_during_activation_cancels_it_and_still_stops_the_runtime(monkeypatch):
    runtime, log, builds = CronRuntime(block=True), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        await asyncio.wait_for(runtime.activate_called.wait(), 5)
        await channel.send(parent_frame("STOP"), timeout=2)
        assert await channel.receive(timeout=5) == frame("STOPPED", os.getpid())
        assert await asyncio.wait_for(task, 5) is None
    assert runtime.activation_cancelled and runtime.stop_called.is_set()


async def test_activation_failure_reports_its_code_and_stops_the_runtime(monkeypatch):
    runtime, log, builds = CronRuntime(fail=True), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame("FAILED", os.getpid(), error_code="CRON_ACTIVATION")
        with pytest.raises(worker._WorkerFailure) as failure:
            await asyncio.wait_for(task, 5)
        # The upstream text is neither the code nor the management message.
        assert failure.value.error_code == "CRON_ACTIVATION"
    assert runtime.stop_called.is_set()


async def test_duplicate_activation_is_a_protocol_failure(monkeypatch):
    runtime, log, builds = CronRuntime(), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame("CRON_ACTIVE", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame(
            "FAILED", os.getpid(), error_code="MANAGEMENT_PROTOCOL")
        with pytest.raises(worker._WorkerFailure):
            await asyncio.wait_for(task, 5)
    assert runtime.activations == 1 and runtime.stop_called.is_set()


async def test_second_activation_while_the_first_is_running_is_refused(monkeypatch):
    runtime, log, builds = CronRuntime(block=True), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        await asyncio.wait_for(runtime.activate_called.wait(), 5)
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame(
            "FAILED", os.getpid(), error_code="MANAGEMENT_PROTOCOL")
        with pytest.raises(worker._WorkerFailure):
            await asyncio.wait_for(task, 5)
    assert runtime.activations == 1 and runtime.activation_cancelled and runtime.stop_called.is_set()


async def test_scheduler_failure_after_activation_stops_the_worker(monkeypatch):
    runtime, log, builds = CronRuntime(wait_fails=True), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        await channel.send(parent_frame("CRON_ACTIVATE"), timeout=2)
        assert await channel.receive(timeout=5) == frame("CRON_ACTIVE", os.getpid())
        with pytest.raises(RuntimeError, match="cron scheduler exited"):
            await asyncio.wait_for(task, 5)
    assert runtime.stop_called.is_set()


async def test_management_eof_after_prepare_stops_the_runtime(monkeypatch):
    runtime, log, builds = CronRuntime(), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime) as (channel, task, _server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        channel.close()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(task, 5)
    assert runtime.activations == 0 and runtime.stop_called.is_set()


async def test_cron_slot_must_match_the_worker_identity(monkeypatch):
    runtime, log, builds = CronRuntime(), [], []
    install_data_worker(monkeypatch, runtime, log, builds, [])
    async with running_worker(runtime, cron_slot=1) as (channel, task, _server):
        await channel.send(parent_frame("INIT"), timeout=2)
        assert await channel.receive(timeout=5) == frame(
            "FAILED", os.getpid(), error_code="WORKER_FAILURE")
        with pytest.raises(RuntimeError, match="slot does not match"):
            await asyncio.wait_for(task, 5)
    assert builds == []


async def test_delivered_cron_commands_reach_the_command_runtime(monkeypatch):
    runtime, log, builds, shards = CronRuntime(), [], [], []
    install_data_worker(monkeypatch, runtime, log, builds, shards)
    async with running_worker(runtime) as (channel, task, server):
        assert await initialize(channel) == frame("CRON_PREPARED", os.getpid())
        assert shards[0].accepted == []
        await server.notify("cron.command_deliver", {"command_id": "first"}, timeout=3)
        await server.notify("cron.command_deliver", {"command_id": "second"}, timeout=3)
        async with asyncio.timeout(5):
            while len(shards[0].accepted) < 2:
                await asyncio.sleep(0.01)
        # The Worker forwards the arguments object it received, unchanged.
        assert shards[0].accepted == [{"command_id": "first"}, {"command_id": "second"}]
        await channel.send(parent_frame("STOP"), timeout=2)
        assert await channel.receive(timeout=5) == frame("STOPPED", os.getpid())
        assert await asyncio.wait_for(task, 5) is None
    assert runtime.activations == 0 and runtime.stop_called.is_set()


async def test_delivery_before_the_shard_exists_is_refused_as_busy(monkeypatch):
    runtime, log, builds, shards = CronRuntime(), [], [], []
    gate = asyncio.Event()
    install_data_worker(monkeypatch, runtime, log, builds, shards, gate=gate)
    async with running_worker(runtime) as (channel, task, server):
        await channel.send(parent_frame("INIT"), timeout=2)
        assert await channel.receive(timeout=5) == frame("READY", os.getpid(), stage="management_ready")
        with pytest.raises(ControlServiceBusy):
            await server.notify("cron.command_deliver", {"command_id": "early"}, timeout=3)
        gate.set()
        assert await channel.receive(timeout=5) == frame(
            "RUNTIME_READY", os.getpid(), runtime_ids=["web-http", "web-ws", "protocol"])
        assert await channel.receive(timeout=5) == frame("CRON_PREPARED", os.getpid())
        assert shards[0].accepted == []
        await channel.send(parent_frame("STOP"), timeout=2)
        assert await channel.receive(timeout=5) == frame("STOPPED", os.getpid())
        assert await asyncio.wait_for(task, 5) is None
