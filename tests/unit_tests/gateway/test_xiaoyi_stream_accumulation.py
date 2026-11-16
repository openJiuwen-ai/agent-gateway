# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Regression tests for Xiaoyi streaming text accumulation.

``payload.content`` 在 chat.delta 上是文本片段（与 ``_send_team``、飞书
``_cardkit_buffers`` 的增量拼接语义一致），``_send_legacy`` 必须以片段累积的方式
维护 ``_accumulated_texts``，否则后台任务完成推送取到的只是最后一段增量。
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

from e2a.message import EventType, Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XiaoyiChannel,
    XiaoyiChannelConfig,
)

_AGENT_ID = "agent-user-1"
_PHYSICAL_SESSION_ID = "physical-session-1"
_LOGICAL_SESSION_ID = "logical-session-1"
_TASK_ID = "task-1"
_PUSH_ID = "push-1"


def _channel(*, enable_streaming: bool = True) -> XiaoyiChannel:
    channel = XiaoyiChannel(
        XiaoyiChannelConfig(
            channel_id="xiaoyi",
            agent_id=_AGENT_ID,
            api_id="api-1",
            push_id=_PUSH_ID,
            enable_streaming=enable_streaming,
        ),
        RobotMessageRouter(),
    )
    channel._ws_connections = {"primary": object()}
    channel._send_text_response = AsyncMock()
    channel._send_push_notification = AsyncMock()
    channel._stop_session_heartbeat = AsyncMock()
    channel._active_push_sessions[_AGENT_ID] = (
        _PHYSICAL_SESSION_ID,
        _TASK_ID,
        _PUSH_ID,
        time.time(),
    )
    return channel


def _event(
    content: str,
    *,
    msg_id: str,
    event_type: EventType = EventType.CHAT_DELTA,
    is_complete: bool = False,
) -> Message:
    return Message(
        id=msg_id,
        type="event",
        channel_id="xiaoyi",
        session_id=_LOGICAL_SESSION_ID,
        params={},
        timestamp=time.time(),
        ok=True,
        payload={"content": content, "is_complete": is_complete},
        event_type=event_type,
        metadata={
            "xiaoyi_session_id": _PHYSICAL_SESSION_ID,
            "xiaoyi_task_id": _TASK_ID,
            "xiaoyi_conversation_id": _LOGICAL_SESSION_ID,
            "xiaoyi_push_id": _PUSH_ID,
            "im_sender_user_id": _AGENT_ID,
        },
    )


async def test_delta_chunks_accumulate_into_push_summary() -> None:
    channel = _channel()
    channel._mark_session_waiting_for_push(_PHYSICAL_SESSION_ID, _TASK_ID)

    await channel._send_legacy(_event("你好", msg_id="delta-1"))
    await channel._send_legacy(_event("，我是", msg_id="delta-2"))
    await channel._send_legacy(
        _event("小易", msg_id="final-1", event_type=EventType.CHAT_FINAL, is_complete=True)
    )

    channel._send_push_notification.assert_awaited_once()
    text, push_text = channel._send_push_notification.await_args.args
    assert text == "你好，我是小易"
    assert push_text == "后台任务已完成：你好，我是小易"


async def test_non_streaming_message_is_not_accumulated() -> None:
    channel = _channel(enable_streaming=False)
    channel._mark_session_waiting_for_push(_PHYSICAL_SESSION_ID, _TASK_ID)

    await channel._send_legacy(
        _event("完整回答", msg_id="single-1", event_type=EventType.CHAT_FINAL, is_complete=True)
    )

    channel._send_push_notification.assert_awaited_once()
    text, _push_text = channel._send_push_notification.await_args.args
    assert text == "完整回答"


async def test_accumulated_buffer_is_cleared_after_final() -> None:
    channel = _channel()

    await channel._send_legacy(_event("abc", msg_id="delta-1"))
    assert channel._accumulated_texts[_PHYSICAL_SESSION_ID] == "abc"

    await channel._send_legacy(
        _event("def", msg_id="final-1", event_type=EventType.CHAT_FINAL, is_complete=True)
    )
    assert _PHYSICAL_SESSION_ID not in channel._accumulated_texts


async def test_next_turn_does_not_inherit_previous_text() -> None:
    channel = _channel()

    await channel._send_legacy(_event("第一轮", msg_id="delta-1"))
    await channel._send_legacy(
        _event("结束", msg_id="final-1", event_type=EventType.CHAT_FINAL, is_complete=True)
    )
    await channel._send_legacy(_event("第二轮", msg_id="delta-2"))

    assert channel._accumulated_texts[_PHYSICAL_SESSION_ID] == "第二轮"
