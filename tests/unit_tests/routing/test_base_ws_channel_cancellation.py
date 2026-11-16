# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""BaseWsChannel writer 收尾：等子任务退出，但不吞掉本协程的取消信号.

历史实现用 ``except (asyncio.CancelledError, Exception): pass`` 包住 ``await writer``：
writer 自身的取消被吞掉是对的，但调用方（断连/关停路径）被取消时也会一起被吞掉，
导致取消不再向上传播，收尾流程继续执行。
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib

import pytest

pytest.importorskip("e2a")

# 先加载包，保证 base_ws_channel 的导入顺序与运行时一致（见 test_base_ws_channel_register）。
importlib.import_module("agent_gateway.channel_manager")
from agent_gateway.routing.base_ws_channel import BaseWsChannel


class _FakeWs:
    closed = False


class _Channel(BaseWsChannel):
    channel_id = "test"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def _serialize_frame(self, msg, routing_target, *, member_names=None):
        return {}


def _install_writer(channel: _Channel, ws_id: str, writer: asyncio.Task) -> None:
    channel._writers[ws_id] = writer
    channel._send_queues[ws_id] = asyncio.Queue(maxsize=4)


async def test_shutdown_all_writers_waits_for_cancelled_writer() -> None:
    """writer 被取消时正常收尾，不把子任务的取消抛给调用方。"""

    channel = _Channel(None, None)
    writer = asyncio.create_task(asyncio.sleep(3600))
    _install_writer(channel, "ws-1", writer)

    await channel._shutdown_all_writers()

    assert writer.cancelled() or writer.done()
    assert "ws-1" not in channel._writers
    assert "ws-1" not in channel._send_queues


async def test_shutdown_all_writers_propagates_caller_cancellation() -> None:
    """writer 不响应取消时，调用方的取消必须继续向上传播。"""

    channel = _Channel(None, None)
    release = asyncio.Event()

    async def _stubborn() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await release.wait()

    writer = asyncio.create_task(_stubborn())
    _install_writer(channel, "ws-1", writer)

    shutdown = asyncio.create_task(channel._shutdown_all_writers())
    await asyncio.sleep(0.05)
    shutdown.cancel()

    with pytest.raises(asyncio.CancelledError):
        await shutdown

    release.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(writer, timeout=5)


async def test_drain_and_cleanup_writer_pops_state() -> None:
    """正常路径：flush 完成后清理 writer 与队列。"""

    channel = _Channel(None, None)
    ws = _FakeWs()
    writer = asyncio.create_task(asyncio.sleep(0.01))
    _install_writer(channel, "ws-1", writer)

    await channel._drain_and_cleanup_writer(ws, "ws-1")

    assert "ws-1" not in channel._writers
    assert "ws-1" not in channel._send_queues
