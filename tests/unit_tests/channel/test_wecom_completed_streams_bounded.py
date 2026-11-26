# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""企业微信 ``_stream_completed_requests`` 必须有界。

该集合记录已经收尾的流式请求，用来吞掉同一 req_id 的迟到事件，正常路径下
只有在迟到事件真的再次出现时才会被丢弃。没有迟到事件的请求会永久留在集合
里，长跑网关按流式请求数线性增长。
"""
from __future__ import annotations

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

    async def reply_stream(
        self,
        frame: Any,
        stream_id: str,
        content: str,
        *,
        finish: bool,
    ) -> None:
        self.reply_calls.append((frame, stream_id, content, finish))


def _message(
    req_id: str,
    content: str,
    *,
    event_type: EventType = EventType.CHAT_FINAL,
) -> Message:
    return Message(
        id="request-1",
        type="event",
        channel_id="wecom",
        session_id="session-1",
        params={},
        payload={"event_type": event_type.value, "content": content},
        metadata={"wecom_req_id": req_id},
        timestamp=0,
        ok=True,
        event_type=event_type,
    )


def _build_channel() -> tuple[WecomChannel, _FakeWecomClient]:
    channel = WecomChannel(
        WecomConfig(enabled=True, enable_streaming=True),
        RobotMessageRouter(),
    )
    client = _FakeWecomClient()
    channel._ws_client = client
    return channel, client


async def _complete_stream(channel: WecomChannel, req_id: str) -> None:
    channel._pending_streams[req_id] = {
        "frame": {"headers": {}},
        "stream_id": f"stream-{req_id}",
        "accumulated": "",
    }
    await channel.send(_message(req_id, f"reply for {req_id}"))


@pytest.mark.asyncio
async def test_completed_stream_markers_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(wecom_connect, "_STREAM_COMPLETED_LIMIT", 3)
    channel, _client = _build_channel()

    for index in range(5):
        await _complete_stream(channel, f"req-{index}")

    assert list(channel._stream_completed_requests) == ["req-2", "req-3", "req-4"]


@pytest.mark.asyncio
async def test_late_event_for_recent_stream_is_still_swallowed() -> None:
    channel, client = _build_channel()
    await _complete_stream(channel, "req-late")
    calls_after_final = len(client.reply_calls)

    await channel.send(
        _message("req-late", "duplicate tail", event_type=EventType.CHAT_DELTA)
    )

    assert len(client.reply_calls) == calls_after_final
    assert "req-late" not in channel._stream_completed_requests
