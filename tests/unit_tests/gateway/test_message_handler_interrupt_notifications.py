# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from unittest.mock import AsyncMock
from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler
from openjiuwen_protocol.message import EventType


def _handler():
    handler = object.__new__(MessageHandler)
    handler.publish_robot_messages = AsyncMock()
    return handler


async def test_send_interrupt_result_notification_builds_default_success_message():
    handler = _handler()

    await handler._send_interrupt_result_notification(
        "req-1", "web", "sess-1", "pause", has_active_task=True
    )

    assert handler.publish_robot_messages.await_count == 1
    msg = handler.publish_robot_messages.await_args.args[0]
    assert msg.id == "req-1"
    assert msg.type == "event"
    assert msg.channel_id == "web"
    assert msg.session_id == "sess-1"
    assert msg.ok is True
    assert msg.event_type is EventType.CHAT_INTERRUPT_RESULT
    assert msg.payload == {
        "event_type": "chat.interrupt_result",
        "intent": "pause",
        "success": True,
        "message": "任务已暂停",
        "has_active_task": True,
    }


async def test_send_interrupt_result_notification_failure_message():
    handler = _handler()

    await handler._send_interrupt_result_notification(
        "req-2", "web", None, "resume", success=False
    )

    msg = handler.publish_robot_messages.await_args.args[0]
    assert msg.payload["message"] == "任务恢复失败"
    assert "has_active_task" not in msg.payload


async def test_send_interrupt_to_agent_sends_cancelled_tool_results():
    handler = _handler()
    handler._send_non_stream_agent_request = AsyncMock(
        return_value=SimpleNamespace(
            request_id="req-3",
            ok=True,
            payload={"cancelled_tools": [{"tool_name": "search"}]},
        )
    )
    handler._send_cancelled_tool_results = AsyncMock()
    env = SimpleNamespace(
        channel="web",
        session_id="sess-1",
    )

    await handler._send_interrupt_to_agent(env, metadata={"mode": "plan"})

    handler._send_cancelled_tool_results.assert_awaited_once_with(
        "web",
        "sess-1",
        {"cancelled_tools": [{"tool_name": "search"}]},
        {"mode": "plan"},
    )


async def test_send_cancelled_tool_results_publishes_each_tool():
    handler = _handler()
    handler._build_tool_result_message = lambda *args: ("tool", args)

    await handler._send_cancelled_tool_results(
        "web",
        "sess-1",
        {"cancelled_tools": [{"tool_name": "search"}, {"tool_name": "browser"}]},
        {"mode": "plan"},
    )

    assert handler.publish_robot_messages.await_count == 2
    assert handler.publish_robot_messages.await_args_list[0].args[0] == (
        "tool",
        ("web", "sess-1", {"tool_name": "search"}, {"mode": "plan"}),
    )
    assert handler.publish_robot_messages.await_args_list[1].args[0] == (
        "tool",
        ("web", "sess-1", {"tool_name": "browser"}, {"mode": "plan"}),
    )
