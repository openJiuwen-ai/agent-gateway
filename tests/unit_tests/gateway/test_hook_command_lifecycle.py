# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
import sys
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.hooks import executor
from agent_gateway.hooks.executor import HookExecutor, HookOutcome


@pytest.fixture
def child(monkeypatch):
    proc = Mock(spec=asyncio.subprocess.Process, returncode=None)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))
    return proc


@pytest.mark.asyncio
async def test_completed_command_keeps_output_and_needs_no_cleanup(child):
    child.returncode = 0
    child.communicate.return_value = (b'{"additionalContext":"ready"}', b"")

    (result,) = await HookExecutor().run_all([{"command": "hook"}], {})

    assert result.outcome == HookOutcome.SUCCESS
    assert result.additional_context == "ready"
    child.kill.assert_not_called()
    child.wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_during_command_reaps_child_and_propagates(child):
    started = asyncio.Event()

    async def communicate(input):
        started.set()
        await asyncio.Future()

    child.communicate.side_effect = communicate
    task = asyncio.create_task(HookExecutor().run_all([{"command": "hook"}], {}))
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    child.kill.assert_called_once_with()
    child.wait.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_command_timeout_reaps_child_and_returns_non_blocking_error(child):
    (result,) = await HookExecutor().run_all([{"command": "hook", "timeout": 0}], {})

    assert result.outcome == HookOutcome.NON_BLOCKING_ERROR
    assert result.error == "hook timeout after 0s"
    child.kill.assert_called_once_with()
    child.wait.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_cleanup_does_not_mask_communication_error_or_wait_forever(child, monkeypatch):
    child.communicate.side_effect = OSError("pipe closed")
    child.kill.side_effect = ProcessLookupError()
    waiting = asyncio.Event()
    child.wait.side_effect = waiting.wait
    monkeypatch.setattr(executor, "_PROCESS_EXIT_GRACE_SECONDS", 0.01)

    (result,) = await asyncio.wait_for(HookExecutor().run_all([{"command": "hook"}], {}), timeout=5)

    assert result.outcome == HookOutcome.NON_BLOCKING_ERROR
    assert result.error == "pipe closed"
    child.wait.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_cancel_stops_real_direct_child(monkeypatch):
    loop = asyncio.get_running_loop()
    started = loop.create_future()
    communicate = asyncio.subprocess.Process.communicate

    async def record_child(proc, **kwargs):
        started.set_result(proc)
        return await communicate(proc, **kwargs)

    monkeypatch.setattr(asyncio.subprocess.Process, "communicate", record_child)
    config = {"shell": sys.executable, "command": "import time; time.sleep(30)"}
    task = asyncio.create_task(HookExecutor().run_all([config], {}))
    proc = await asyncio.wait_for(started, timeout=5)
    try:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert proc.returncode is not None
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
