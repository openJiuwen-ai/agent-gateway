# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure message/request classification helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agent_gateway.common.mode_matrix import is_team_mode
from agent_gateway.message_handler.evolution_approval import (
    is_interrupt_evolution_approval_answer_payload,
)
from agent_gateway.runtime.session_input import resolve_session_input_mode

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


_INTERRUPT_RESUME_SOURCES = frozenset({
    "ask_user_interrupt",
    "confirm_interrupt",
    "permission_interrupt",
    "evolution_interrupt",
})


def is_chat_send_message(msg: "Message") -> bool:
    """Return whether the message uses the chat.send request method."""
    method = getattr(msg, "req_method", None)
    value = getattr(method, "value", method)
    return value == "chat.send" or str(value) == "ReqMethod.CHAT_SEND"


def is_team_chat_send(msg: "Message") -> bool:
    """Return whether the message selects a team mode."""
    params = msg.params if isinstance(msg.params, dict) else {}
    return is_team_mode(str(params.get("mode") or ""))


def is_unknown_team_mode_send(msg: "Message") -> bool:
    """Return whether a team-prefixed mode is not a known team mode."""
    if not is_chat_send_message(msg):
        return False
    params = msg.params if isinstance(msg.params, dict) else {}
    mode = str(params.get("mode") or "").strip().lower()
    if not mode or mode.split(".", 1)[0] != "team":
        return False
    return not is_team_mode(mode)


def is_unsupported_non_stream_team_send(msg: "Message") -> bool:
    """Return whether team chat cannot run on a non-streaming channel."""
    if getattr(msg, "enable_streaming", True):
        return False
    return is_chat_send_message(msg) and is_team_chat_send(msg)


def is_interrupt_resume_chat_send(msg: "Message") -> bool:
    """Return whether this chat.send resumes a pending interrupt."""
    params = msg.params if isinstance(msg.params, dict) else {}
    source = str(params.get("source") or "").strip()
    answers = params.get("answers")
    request_id = str(params.get("request_id") or "").strip()
    if (
        bool(request_id)
        and isinstance(answers, list)
        and source in _INTERRUPT_RESUME_SOURCES
    ):
        return True
    return is_interrupt_evolution_approval_answer_payload(params)


def should_cancel_existing_stream_before_chat_send(msg: "Message") -> bool:
    """Return whether an ordinary chat request may replace the current stream."""
    params = msg.params if isinstance(msg.params, dict) else {}
    if params.get("source") == "proactive_recommendation":
        return False
    return (
        is_chat_send_message(msg)
        and not is_team_chat_send(msg)
        and not is_interrupt_resume_chat_send(msg)
        and not is_interaction_managed_chat_send(msg)
    )


def is_interaction_managed_chat_send(msg: "Message") -> bool:
    """Return whether the host must not replace the current stream."""
    params = msg.params if isinstance(msg.params, dict) else {}
    if params.get("attach_goal") is True:
        return True
    return resolve_session_input_mode(params) is not None


def is_session_input_message(msg: "Message") -> bool:
    """Return whether this is a normal session-input chat request."""
    return (
        is_chat_send_message(msg)
        and not is_interrupt_resume_chat_send(msg)
        and resolve_session_input_mode(msg.params) is not None
    )
