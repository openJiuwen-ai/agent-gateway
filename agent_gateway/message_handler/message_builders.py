# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure outbound message construction helpers extracted from MessageHandler."""

from __future__ import annotations

import secrets
import time
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.message import EventType, Message

from agent_gateway.common.mode_matrix import (
    DEPRECATION_MAP,
    MODE_ALIASES,
    NEW_CANONICAL_MODES,
    is_team_mode,
)

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


_VALID_MODE_INPUTS: frozenset[str] = frozenset(
    NEW_CANONICAL_MODES | set(DEPRECATION_MAP.keys()) | set(MODE_ALIASES.keys())
)
_KNOWN_TEAM_MODE_INPUTS: tuple[str, ...] = tuple(
    sorted(mode for mode in _VALID_MODE_INPUTS if is_team_mode(mode))
)


_UNKNOWN_TEAM_MODE_NOTICE = (
    "不支持的模式 '{mode}'：以 team 开头的 mode 必须是已知的 Team 模式"
    "（{valid}）。请修正 mode 后重试。"
)
_NON_STREAM_TEAM_NOTICE = (
    "集群模式需要开启流式输出才能运行（成员协作事件是流式下发的）。"
    "请在该通道配置中开启 enable_streaming，或改用单 Agent 模式。"
)


def build_supplement_continuation_query(
    new_input: str,
    original_request: str = "",
) -> str:
    trimmed = str(new_input or "").strip()
    original = str(original_request or "").strip()
    original_section = (
        f"\n\n原始任务请求如下，请以它作为继续执行 todo 时的上下文，尤其要保留其中的文件路径、目录、约束和目标：\n{original[:8000]}"
        if original
        else ""
    )
    return (
        "用户在当前任务执行中追加了补充/调整请求：\n"
        f"{trimmed}\n\n"
        "请先处理这个补充/调整请求，然后检查并继续执行当前会话 todo 列表中仍未完成的 "
        "in_progress 或 pending 任务。不要因为补充请求本身处理完成就询问用户下一步；"
        "只有在确认 todo 列表没有未完成任务时，才可以总结或询问后续方向。\n\n"
        "注意：追加补充请求会中断上一轮流式输出，用户界面上上一轮正在输出的任务结果可能只展示了一部分。"
        "如果补充请求发生时某个 todo 正在输出结果，或者 todo 状态已经前进但该任务结果可能没有完整展示，"
        "继续执行时请先补全或简要重述这个被中断任务的完整结果，再推进后续 todo；"
        "不要仅因为 todo 状态已经变为 completed 就跳过用户尚未完整看到的任务结果。"
        f"{original_section}"
    )


def build_queued_chat_send_message(
    msg: "Message",
    new_input: str,
    attachments: list[dict[str, Any]] | None = None,
    original_request: str = "",
) -> "Message":
    from openjiuwen_protocol.message import Message, ReqMethod

    new_req_id = f"req_{int(time.time() * 1000):x}_{msg.id}"
    params: dict[str, Any] = {
        "query": build_supplement_continuation_query(
            new_input,
            original_request,
        ),
        "supplement_input": new_input,
        "original_request": original_request,
        "session_id": msg.session_id,
        "is_supplement": True,
    }
    if attachments:
        params["attachments"] = attachments
    return Message(
        id=new_req_id,
        type="req",
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params=params,
        timestamp=time.time(),
        ok=True,
        req_method=ReqMethod.CHAT_SEND,
        is_stream=True,
        user_id=getattr(msg, "user_id", None),
    )


def build_error_out_message(
    msg: "Message",
    error: Exception,
) -> "Message":
    from openjiuwen_protocol.message import Message

    payload: dict[str, Any] = {"error": str(error)}
    code = getattr(error, "code", None)
    if isinstance(code, str) and code:
        payload["code"] = code

    return Message(
        id=msg.id,
        type="res",
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params={},
        timestamp=time.time(),
        ok=False,
        payload=payload,
        metadata=msg.metadata,
        app_id=msg.app_id or "",
    )


def build_tool_result_message(
    channel_id: str,
    session_id: str,
    tool_info: dict,
    metadata: dict | None,
) -> "Message":
    """Build a tool_result message for cancelled tool execution."""
    from openjiuwen_protocol.message import EventType, Message

    return Message(
        id=f"tool_result_{int(time.time() * 1000):x}_{secrets.token_hex(3)}",
        type="event",
        channel_id=channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        payload={
            "tool_result": {
                "tool_name": tool_info.get("tool_name", ""),
                "tool_call_id": tool_info.get("tool_call_id", ""),
                "result": tool_info.get("result", ""),
                "status": tool_info.get("status", "error"),
            },
        },
        event_type=EventType.CHAT_TOOL_RESULT,
        metadata=metadata,
    )


def build_unknown_team_mode_error_message(msg: "Message") -> "Message":
    """Build the rejection notice for an unknown team.* mode."""
    params = msg.params if isinstance(msg.params, dict) else {}
    mode = str(params.get("mode") or "").strip()
    metadata = msg.metadata if isinstance(msg.metadata, dict) else None

    return Message(
        id=msg.id,
        type="event",
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params={},
        timestamp=time.time(),
        ok=False,
        payload={
            "event_type": EventType.CHAT_ERROR.value,
            "error": _UNKNOWN_TEAM_MODE_NOTICE.format(
                mode=mode,
                valid=", ".join(_KNOWN_TEAM_MODE_INPUTS),
            ),
            "code": "UNKNOWN_TEAM_MODE",
            "is_complete": True,
        },
        event_type=EventType.CHAT_ERROR,
        metadata=metadata,
        enable_streaming=False,
    )


def build_non_stream_team_notice_message(msg: "Message") -> "Message":
    """Build the notice for non-streaming team requests."""
    metadata = msg.metadata if isinstance(msg.metadata, dict) else None

    return Message(
        id=msg.id,
        type="event",
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        payload={
            "event_type": EventType.CHAT_FINAL.value,
            "content": _NON_STREAM_TEAM_NOTICE,
            "is_complete": True,
        },
        event_type=EventType.CHAT_FINAL,
        metadata=metadata,
        enable_streaming=False,
    )
