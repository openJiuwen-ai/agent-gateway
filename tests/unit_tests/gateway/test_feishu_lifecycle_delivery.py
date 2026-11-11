"""Feishu lifecycle ACK waits for the host, instead of losing async failures."""

import asyncio
import logging
import time
import traceback
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
@pytest.mark.parametrize("failure", ["rejected", "exception"])
async def test_host_failure_reaches_sdk_for_retry(failure):
    callback = AsyncMock(return_value=InboundDisposition.REJECTED)
    if failure == "exception":
        callback.side_effect = RuntimeError("host unavailable")
    c = channel(callback)
    with pytest.raises(RuntimeError):
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


@pytest.mark.asyncio
@pytest.mark.parametrize("failure, reason, error_type", [
    ("missing", "callback_unavailable", "_FeishuLifecycleError"),
    ("invalid_time", "invalid_event_time", "_FeishuLifecycleError"),
    ("rejected", "host_not_accepted", "_FeishuLifecycleError"),
    ("exception", "host_failed", "RuntimeError"),
    ("host_timeout", "host_failed", "TimeoutError"),
])
async def test_lifecycle_failure_logs_safe_reason_and_event_identity(caplog, failure, reason, error_type):
    callback = AsyncMock(return_value=InboundDisposition.REJECTED)
    if failure in ("exception", "host_timeout"):
        exception = TimeoutError if failure == "host_timeout" else RuntimeError
        callback.side_effect = exception("private host response")
    c = channel(None if failure == "missing" else callback)
    data = joined()
    if failure == "invalid_time":
        data.header.create_time = "invalid"
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError) as caught:
        await asyncio.to_thread(c._on_persistent_change_sync, data, "joined")
    record, = [entry.getMessage() for entry in caplog.records if "Lifecycle not confirmed" in entry.getMessage()]
    assert f"reason={reason}" in record
    assert f"error_type={error_type}" in record
    assert "event_id=join-event" in record
    assert "private host response" not in caplog.text
    assert "private host response" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.asyncio
async def test_confirmation_timeout_logs_identity_and_safe_reason(monkeypatch, caplog):
    import agent_gateway.channel_manager.im_platforms.feishu.feishu_connect as module

    future = MagicMock()
    future.result.side_effect = TimeoutError("private timeout details")

    def schedule(coroutine, loop):
        coroutine.close()
        return future

    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", schedule)
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError):
        channel(AsyncMock())._on_persistent_change_sync(joined(), "joined")
    assert "reason=confirmation_timeout" in caplog.text
    assert "event_id=join-event" in caplog.text
    assert "private timeout details" not in caplog.text
    future.cancel.assert_called_once()
