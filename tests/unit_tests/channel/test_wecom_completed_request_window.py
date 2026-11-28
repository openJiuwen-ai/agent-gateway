# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""WeCom completed-stream markers must survive repeated late events and expire by time."""

import time
from typing import Any

import pytest

from e2a.message import EventType, Message
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.wecom import wecom_connect
from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import (
    WecomChannel,
    WecomConfig,
)


class _FakeWecomClient:
    is_connected = True

    def __init__(self) -> None:
        self.reply_calls: list[tuple[Any, str, str, bool]] = []
        self.send_message_calls: list[tuple[str, dict[str, Any]]] = []

    async def reply_stream(
        self,
        frame: Any,
        stream_id: str,
        content: str,
        *,
        finish: bool,
    ) -> None:
        self.reply_calls.append((frame, stream_id, content, finish))

    async def send_message(self, chatid: str, body: dict[str, Any]) -> None:
        self.send_message_calls.append((chatid, body))


def _channel() -> tuple[WecomChannel, _FakeWecomClient]:
    channel = WecomChannel(
        WecomConfig(enabled=True, enable_streaming=True),
        RobotMessageRouter(),
    )
    client = _FakeWecomClient()
    channel._ws_client = client
    return channel, client


def _message(
    content: str,
    *,
    event_type: EventType = EventType.CHAT_FINAL,
    request_id: str = "request-1",
) -> Message:
    return Message(
        id=request_id,
        type="event",
        channel_id="wecom",
        session_id="session-1",
        params={},
        payload={"event_type": event_type.value, "content": content},
        metadata={"wecom_req_id": request_id, "wecom_chat_id": "chat-1"},
        timestamp=0,
        ok=True,
        event_type=event_type,
    )


@pytest.mark.asyncio
async def test_repeated_late_final_events_are_suppressed_within_window() -> None:
    channel, client = _channel()
    channel._pending_streams["request-1"] = {
        "frame": {"headers": {}},
        "stream_id": "stream-1",
        "accumulated": "",
    }

    await channel.send(_message("Hello"))
    assert client.reply_calls == [({"headers": {}}, "stream-1", "Hello", True)]
    assert isinstance(channel._stream_completed_requests["request-1"], float)

    await channel.send(_message("Hello"))
    await channel.send(_message("Hello"))

    assert client.reply_calls == [({"headers": {}}, "stream-1", "Hello", True)]
    assert client.send_message_calls == []
    assert "request-1" in channel._stream_completed_requests


@pytest.mark.asyncio
async def test_completed_marker_is_released_after_window() -> None:
    channel, _client = _channel()
    channel._stream_completed_requests["request-1"] = (
        time.monotonic() - wecom_connect._STREAM_COMPLETED_WINDOW_SECONDS - 1.0
    )

    await channel.send(_message("Hello"))

    assert "request-1" not in channel._stream_completed_requests


@pytest.mark.asyncio
async def test_chat_error_does_not_mark_request_completed() -> None:
    channel, _client = _channel()
    channel._pending_streams["request-1"] = {
        "frame": {"headers": {}},
        "stream_id": "stream-1",
        "accumulated": "",
    }

    await channel.send(_message("boom", event_type=EventType.CHAT_ERROR))

    assert channel._stream_completed_requests == {}
