"""Feishu lifecycle ACK waits for the host, instead of losing async failures.

mingo007 #200679986 迁移件：本文件曾随 PR 整体删除，其中包装层
（_on_persistent_change_sync）契约用例与本 PR 未变更行为对应，全部迁回；
仅「REJECTED 抛出」旧分支与新语义（P2③：REJECTED/UNCLAIMED=永久结局 ACK
不重投）冲突——剔除并反向锁定新语义。
"""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from lark_oapi.api.im.v1 import P2ImChatMemberBotAddedV1

from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import FeishuChannel
from agent_gateway.channel_manager.persistent_events import InboundDisposition


def channel(callback):
    result = object.__new__(FeishuChannel)
    result.config = SimpleNamespace(app_id="app", agent_id="agent")
    result._main_loop = asyncio.get_running_loop()
    result._persistent_group_fanout_callback = callback
    return result


def joined():
    return P2ImChatMemberBotAddedV1({
        "schema": "2.0",
        "header": {"event_id": "join-event", "create_time": str(int(time.time() * 1000)),
                   "event_type": "im.chat.member.bot.added_v1"},
        "event": {"chat_id": "silent-chat", "name": "Silent group"},
    })


@pytest.mark.asyncio
async def test_sync_callback_waits_until_host_confirms_join():
    entered, release = asyncio.Event(), asyncio.Event()

    async def accept(event):
        entered.set()
        await release.wait()
        return InboundDisposition.HANDLED

    c = channel(accept)
    task = asyncio.create_task(asyncio.to_thread(c._on_persistent_change_sync, joined(), "joined"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.02)
        assert not task.done()
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_host_exception_reaches_sdk_for_retry():
    """包装层：宿主回调异常必须传到 SDK（不 ACK→重投），旧 parametrize 的
    exception 分支原样迁回；rejected 分支与 P2③ 新语义冲突，由下方
    test_rejected_acks_without_raise 反向锁定取代。"""
    c = channel(AsyncMock(side_effect=RuntimeError("host unavailable")))
    with pytest.raises(RuntimeError):
        await asyncio.to_thread(c._on_persistent_change_sync, joined(), "joined")


@pytest.mark.asyncio
async def test_rejected_acks_without_raise():
    """P2③ 新语义包装层反证：REJECTED＝永久结局——ACK 不抛出（旧用例的
    rejected 分支已废止，此处锁定新契约防回退）。"""
    c = channel(AsyncMock(return_value=InboundDisposition.REJECTED))
    await asyncio.to_thread(c._on_persistent_change_sync, joined(), "joined")


@pytest.mark.asyncio
async def test_missing_main_loop_is_not_acknowledged_as_success():
    c = channel(AsyncMock())
    c._main_loop = None
    with pytest.raises(RuntimeError):
        c._on_persistent_change_sync(joined(), "joined")


@pytest.mark.asyncio
async def test_timeout_cancels_unconfirmed_delivery(monkeypatch):
    import agent_gateway.channel_manager.im_platforms.feishu.feishu_connect as module

    future = MagicMock()
    future.result.side_effect = TimeoutError()

    def schedule(coroutine, loop):
        coroutine.close()
        return future

    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", schedule)
    c = channel(AsyncMock())
    with pytest.raises(RuntimeError):
        c._on_persistent_change_sync(joined(), "joined")
    future.cancel.assert_called_once()
    assert future.result.call_args.kwargs["timeout"] == 3


@pytest.mark.asyncio
async def test_unbound_channel_lifecycle_is_ignored_without_a_host():
    c = channel(None)
    c.config.agent_id = ""
    c._main_loop = None
    c._on_persistent_change_sync(joined(), "joined")


@pytest.mark.asyncio
async def test_dissolution_is_forwarded_as_lifecycle_not_chat():
    callback = AsyncMock(return_value=InboundDisposition.HANDLED)
    c = channel(callback)
    await asyncio.to_thread(c._on_persistent_change_sync, joined(), "dissolved")
    event = callback.await_args.args[0]
    assert event.event_kind == "dissolved"
    assert event.chat_id == "silent-chat"
    assert event.text is None
