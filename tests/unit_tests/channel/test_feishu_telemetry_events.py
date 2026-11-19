# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Feishu must not render Web/TUI-only runtime telemetry events."""

import asyncio
from unittest.mock import MagicMock

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.base import (
    WEB_ONLY_TELEMETRY_EVENTS,
    RobotMessageRouter,
)
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)


def _make_channel() -> FeishuChannel:
    config = FeishuConfig(
        enabled=True,
        app_id="cli_test_app",
        app_secret="test_app_secret",
        enable_streaming=False,
    )
    channel = FeishuChannel(config, MagicMock(spec=RobotMessageRouter))
    channel._api_client = MagicMock()
    channel._file_service = None
    return channel


def _message(event_name: str, content: str = "duplicate answer") -> Message:
    return Message(
        id="hb_run-1",
        type="event",
        channel_id="feishu",
        session_id="feishu_session",
        params={},
        timestamp=0.0,
        ok=True,
        payload={"event_type": event_name, "content": content},
        event_type=None,
        metadata={"feishu_chat_id": "oc_chat"},
        enable_streaming=False,
    )


@pytest.mark.parametrize("event_name", sorted(WEB_ONLY_TELEMETRY_EVENTS))
def test_telemetry_events_are_dropped(event_name: str) -> None:
    channel = _make_channel()
    sent: list[tuple] = []

    async def _capture(*args, **kwargs):
        sent.append(args)

    channel._send_feishu_message = _capture
    asyncio.run(channel.send(_message(event_name)))
    assert sent == []


def test_chat_final_is_still_sent() -> None:
    channel = _make_channel()
    sent: list[tuple] = []

    async def _capture(*args, **kwargs):
        sent.append(args)

    channel._send_feishu_message = _capture
    asyncio.run(channel.send(_message("chat.final", "the answer")))
    assert len(sent) == 1
    assert "the answer" in sent[0][2]
