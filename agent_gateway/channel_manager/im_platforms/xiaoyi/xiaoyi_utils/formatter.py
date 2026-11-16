# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""XiaoYi Formatter - 状态事件格式化辅助。"""

from e2a.message import EventType


def should_send_as_status_update(event_type: EventType | None) -> bool:
    """
    判断是否应该作为 status update 发送。

    Args:
        event_type: 事件类型

    Returns:
        bool: 是否应该作为 status update 发送
    """
    if event_type is None:
        return False

    # Status update 用于以下事件：
    # - CHAT_TOOL_CALL: 工具调用
    # - CHAT_TOOL_RESULT: 工具结果
    status_events = {
        EventType.CHAT_TOOL_CALL,
        EventType.CHAT_TOOL_RESULT,
    }

    return event_type in status_events


def get_status_state_for_event(event_type: EventType | None, payload: dict | None = None) -> str:
    """
    根据事件类型获取状态值。

    Args:
        event_type: 事件类型
        payload: 消息载荷

    Returns:
        str: 状态值
    """
    if event_type is None:
        return "unknown"

    if event_type == EventType.CHAT_PROCESSING_STATUS:
        if payload and isinstance(payload, dict):
            is_processing = payload.get("is_processing", True)
            return "working" if is_processing else "completed"
        return "working"

    status_map = {
        EventType.CHAT_TOOL_CALL: "working",
        EventType.CHAT_TOOL_RESULT: "working",
        EventType.CHAT_FINAL: "completed",
        EventType.CHAT_ERROR: "failed",
    }

    return status_map.get(event_type, "unknown")


def get_status_text_for_event(event_type: EventType | None, payload: dict | None = None) -> str:
    """
    根据事件类型获取状态文本。

    Args:
        event_type: 事件类型
        payload: 消息载荷

    Returns:
        str: 状态文本
    """
    if event_type is None:
        return "处理中"

    if payload and isinstance(payload, dict):
        if event_type == EventType.CHAT_TOOL_CALL:
            tool_call = payload.get("tool_call", {})
            if isinstance(tool_call, dict):
                tool_name = tool_call.get("name", "")
                if tool_name:
                    return f"正在使用工具：{tool_name}"
            return "正在使用工具..."
        if event_type == EventType.CHAT_TOOL_RESULT:
            tool_name = payload.get("tool_name", "")
            if tool_name:
                return f"工具 {tool_name} 执行完成"
            return "工具执行完成"
        if event_type == EventType.CHAT_PROCESSING_STATUS:
            is_processing = payload.get("is_processing", True)
            if is_processing:
                return "任务正在处理中，请稍后~"
            return "任务处理已完成~"
        content = payload.get("content", "")
        if isinstance(content, str) and content:
            return content
        if isinstance(content, dict):
            return content.get("output", "处理中")

    status_text_map = {
        EventType.CHAT_TOOL_CALL: "正在使用工具...",
        EventType.CHAT_TOOL_RESULT: "工具执行完成",
        EventType.CHAT_PROCESSING_STATUS: "任务正在处理中",
        EventType.CHAT_FINAL: "任务已完成",
        EventType.CHAT_ERROR: "处理失败，请稍后重试",
        EventType.CHAT_INTERRUPT_RESULT: "任务已中断",
    }

    return status_text_map.get(event_type, "处理中")
