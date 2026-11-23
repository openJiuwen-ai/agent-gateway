from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.web_connect import (
    WebChannel,
    WebChannelConfig,
)
from agent_gateway.message_handler import message_handler as message_handler_module
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.routing.keys import ChannelKey


def test_message_handler_user_queue_is_bounded_and_reports_full():
    handler = object.__new__(MessageHandler)
    handler._user_messages = asyncio.Queue(maxsize=1)

    assert handler.is_user_queue_full() is False
    handler._user_messages.put_nowait(object())
    assert handler.is_user_queue_full() is True


async def test_web_inbound_rejects_with_busy_when_queue_full():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: True,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(
        channel_id="web",
        on_message=Mock(),
        reject_inbound=AsyncMock(),
    )
    manager.register_channel_with_inbound(
        channel, manager.deliver_to_message_handler,
    )
    msg = SimpleNamespace(id="req-1", channel_id="web", metadata={})

    await manager.deliver_to_message_handler(msg)

    handler.handle_message.assert_not_awaited()
    channel.reject_inbound.assert_awaited_once()
    assert channel.reject_inbound.await_args.kwargs["code"] == "SERVER_BUSY"


async def test_web_inbound_falls_back_when_reject_cannot_notify():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: True,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(
        channel_id="web",
        on_message=Mock(),
        reject_inbound=AsyncMock(return_value=False),
    )
    manager.register_channel_with_inbound(
        channel, manager.deliver_to_message_handler,
    )
    msg = SimpleNamespace(id="req-1", channel_id="web", metadata={})

    await manager.deliver_to_message_handler(msg)

    channel.reject_inbound.assert_awaited_once()
    handler.handle_message.assert_awaited_once_with(msg)


async def test_web_busy_warning_logs_once_per_overload(caplog):
    handler = SimpleNamespace(
        is_user_queue_full=lambda: True,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(
        channel_id="web",
        on_message=Mock(),
        reject_inbound=AsyncMock(return_value=True),
    )
    manager.register_channel_with_inbound(
        channel, manager.deliver_to_message_handler,
    )

    with caplog.at_level(logging.WARNING):
        await manager.deliver_to_message_handler(
            SimpleNamespace(id="req-1", channel_id="web", metadata={}),
        )
        await manager.deliver_to_message_handler(
            SimpleNamespace(id="req-2", channel_id="web", metadata={}),
        )

    assert caplog.text.count("inbound queue full") == 1


async def test_web_inbound_enqueues_when_queue_has_room():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: False,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(
        channel_id="web",
        on_message=Mock(),
        reject_inbound=AsyncMock(),
    )
    manager.register_channel_with_inbound(
        channel, manager.deliver_to_message_handler,
    )
    msg = SimpleNamespace(id="req-1", channel_id="web", metadata={})

    await manager.deliver_to_message_handler(msg)

    handler.handle_message.assert_awaited_once_with(msg)
    channel.reject_inbound.assert_not_awaited()


async def test_im_inbound_is_not_dropped_when_queue_full():
    handler = SimpleNamespace(
        is_user_queue_full=lambda: True,
        handle_message=AsyncMock(),
    )
    manager = ChannelManager(handler)
    channel = SimpleNamespace(channel_id="feishu", on_message=Mock())
    manager.register_channel(channel)
    msg = SimpleNamespace(id="im-1", channel_id="feishu", metadata={})

    manager._on_channel_message(msg)
    await asyncio.sleep(0)

    handler.handle_message.assert_awaited_once_with(msg)


def test_queue_maxsize_invalid_value_uses_default(monkeypatch):
    monkeypatch.setenv("GATEWAY_USER_MESSAGE_QUEUE_MAXSIZE", "0")
    assert message_handler_module._queue_maxsize_from_env(
        "GATEWAY_USER_MESSAGE_QUEUE_MAXSIZE", 1000,
    ) == 1000

    monkeypatch.setenv("GATEWAY_USER_MESSAGE_QUEUE_MAXSIZE", "-5")
    assert message_handler_module._queue_maxsize_from_env(
        "GATEWAY_USER_MESSAGE_QUEUE_MAXSIZE", 1000,
    ) == 1000


async def test_nowait_publish_defers_when_queue_full(monkeypatch):
    handler = object.__new__(MessageHandler)
    handler._user_messages = asyncio.Queue(maxsize=1)
    handler._deferred_put_tasks = set()
    handler._user_messages.put_nowait("first")
    monkeypatch.setattr(message_handler_module, "queue_request_trace", lambda msg: None)
    msg = SimpleNamespace(id="req-2")

    handler.publish_user_messages_nowait(msg)

    assert handler._user_messages.get_nowait() == "first"
    assert await asyncio.wait_for(handler._user_messages.get(), timeout=1) is msg


async def test_robot_nowait_publish_defers_when_queue_full(monkeypatch):
    handler = object.__new__(MessageHandler)
    handler._robot_messages = asyncio.Queue(maxsize=1)
    handler._deferred_put_tasks = set()
    handler._robot_messages.put_nowait("first")
    monkeypatch.setattr(message_handler_module, "queue_response_trace", lambda msg: msg)
    monkeypatch.setattr(handler, "_attach_derived_response_owner", lambda msg: None)
    msg = SimpleNamespace(id="res-2")

    handler.publish_robot_messages_nowait(msg)

    assert handler._robot_messages.get_nowait() == "first"
    assert await asyncio.wait_for(handler._robot_messages.get(), timeout=1) is msg


async def test_web_channel_reject_inbound_without_peer_returns_false(caplog):
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    msg = SimpleNamespace(
        id="req-1",
        channel_id="web",
        metadata={"ws_id": "missing"},
    )

    with caplog.at_level(logging.WARNING):
        assert await channel.reject_inbound(msg) is False

    assert "peer gone" in caplog.text
    assert "ws_id=missing" in caplog.text


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

    assert await channel.reject_inbound(msg) is True

    assert len(enqueued) == 1
    peer, frame = enqueued[0]
    assert peer is ws
    assert frame["type"] == "res"
    assert frame["id"] == "req-1"
    assert frame["ok"] is False
    assert frame["code"] == "SERVER_BUSY"
    json.dumps(frame)
