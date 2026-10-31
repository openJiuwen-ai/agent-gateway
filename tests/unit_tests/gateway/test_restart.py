# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio

import pytest

from agent_gateway.restart import (
    GatewayRestartRequest,
    schedule_gateway_restart,
    wait_for_gateway_tasks_or_restart,
)


def test_schedule_gateway_restart_without_request_executes_restart(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "agent_gateway.restart.exec_gateway_restart",
        lambda: calls.append(True),
    )

    schedule_gateway_restart(None, delay=0.0)

    assert calls == [True]


async def test_schedule_gateway_restart_sets_ready_event():
    request = GatewayRestartRequest()

    schedule_gateway_restart(request, delay=0.0)

    assert request.requested is True
    await request.ready_event.wait()


async def test_wait_for_gateway_tasks_or_restart_returns_restart_state():
    request = GatewayRestartRequest()
    request.requested = True
    request.ready_event.set()
    service_task = asyncio.create_task(asyncio.sleep(0))

    result = await wait_for_gateway_tasks_or_restart(
        [service_task],
        request,
        asyncio.Event(),
    )

    assert result is True
    await service_task


async def test_wait_for_gateway_tasks_or_restart_returns_shutdown_state():
    request = GatewayRestartRequest()
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    service_task = asyncio.create_task(asyncio.sleep(0))

    result = await wait_for_gateway_tasks_or_restart(
        [service_task],
        request,
        shutdown_event,
    )

    assert result is False
    await service_task


async def test_wait_for_gateway_tasks_or_restart_suppresses_failure_when_restart_requested():
    request = GatewayRestartRequest()
    request.requested = True

    async def fail():
        raise RuntimeError("service failed")

    service_task = asyncio.create_task(fail())

    result = await wait_for_gateway_tasks_or_restart(
        [service_task],
        request,
        asyncio.Event(),
    )

    assert result is True


async def test_wait_for_gateway_tasks_or_restart_propagates_service_failure():
    request = GatewayRestartRequest()

    async def fail():
        raise RuntimeError("service failed")

    service_task = asyncio.create_task(fail())

    with pytest.raises(RuntimeError, match="service failed"):
        await wait_for_gateway_tasks_or_restart(
            [service_task],
            request,
            asyncio.Event(),
        )
