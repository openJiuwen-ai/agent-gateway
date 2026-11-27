# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.southbound.data.managed_websocket import ManagedWebSocketPool


async def test_connection_pool_coalesces_waiters_and_releases_retired_instance():
    pool = ManagedWebSocketPool()
    started, finish = asyncio.Event(), asyncio.Event()
    socket = Mock(disconnect=AsyncMock())
    validate = AsyncMock()

    async def connect(check):
        started.set()
        await finish.wait()
        await check()
        return socket

    async def get():
        return await pool.get('instance', validate=validate, connect=connect,
                              retired_error=lambda: RuntimeError('retired'), on_failure=Mock())

    leader = asyncio.create_task(get())
    await started.wait()
    follower = asyncio.create_task(get())
    await asyncio.sleep(0)
    follower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await follower
    finish.set()
    assert await leader is socket
    assert await get() is socket
    await pool.close('instance')
    socket.disconnect.assert_awaited_once()
    assert not pool.clients and not pool.connecting


async def test_retired_instance_cannot_publish_connection():
    pool = ManagedWebSocketPool()
    started, finish = asyncio.Event(), asyncio.Event()
    socket = Mock(disconnect=AsyncMock())
    on_failure = Mock()

    async def connect(check):
        started.set()
        await finish.wait()
        return socket

    leader = asyncio.create_task(pool.get(
        'instance', validate=AsyncMock(), connect=connect,
        retired_error=lambda: RuntimeError('retired'), on_failure=on_failure,
    ))
    await started.wait()
    await pool.close('instance')
    finish.set()
    with pytest.raises(RuntimeError, match='retired'):
        await leader
    socket.disconnect.assert_awaited_once()
    on_failure.assert_called_once()
    assert not pool.clients and not pool.connecting
