from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.web_connect import (
    WebChannel,
    WebChannelConfig,
)
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.routing.keys import ChannelKey


def test_message_handler_user_queue_is_bounded_and_reports_full():
    handler = object.__new__(MessageHandler)
    handler._user_messages = asyncio.Queue(maxsize=1)

    assert handler.is_user_queue_full() is False
    handler._user_messages.put_nowait(object())
    assert handler.is_user_queue_full() is True


async def test_channel_manager_rejects_with_busy_when_queue_full():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: True,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(channel_id="web", reject_inbound=AsyncMock())
    manager._channels[ChannelKey("web", "default")] = channel
    msg = SimpleNamespace(id="req-1", channel_id="web", metadata={})

    manager._on_channel_message(msg)
    await asyncio.sleep(0)

    handler.handle_message.assert_not_awaited()
    channel.reject_inbound.assert_awaited_once()
    assert channel.reject_inbound.await_args.kwargs["code"] == "SERVER_BUSY"


async def test_channel_manager_enqueues_when_queue_has_room():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: False,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(channel_id="web", reject_inbound=AsyncMock())
    manager._channels[ChannelKey("web", "default")] = channel
    msg = SimpleNamespace(id="req-1", channel_id="web", metadata={})

    manager._on_channel_message(msg)
    await asyncio.sleep(0)

    handler.handle_message.assert_awaited_once_with(msg)
    channel.reject_inbound.assert_not_awaited()


async def test_web_channel_reject_inbound_sends_busy_response(monkeypatch):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    ws = SimpleNamespace(closed=False)
    channel._ws_by_id["ws-1"] = ws
    enqueued: list[tuple[object, dict]] = []
    monkeypatch.setattr(
        channel,
        "_enqueue_send",
        lambda peer, frame: enqueued.append((peer, frame)),
    )
    msg = SimpleNamespace(
        id="req-1",
        channel_id="web",
        metadata={"ws_id": "ws-1"},
    )

    await channel.reject_inbound(msg)

    assert len(enqueued) == 1
    peer, frame = enqueued[0]
    assert peer is ws
    assert frame["type"] == "res"
    assert frame["id"] == "req-1"
    assert frame["ok"] is False
    assert frame["code"] == "SERVER_BUSY"
    json.dumps(frame)
