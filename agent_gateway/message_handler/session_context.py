# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure session query/originator context helpers extracted from MessageHandler."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from collections.abc import Mapping

from agent_gateway.message_handler.request_state import is_chat_send_message
from agent_gateway.routing.session_sharing import SubRole

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


def remember_user_query_context(
    msg: "Message",
    *,
    user_queries: dict[str, str],
    originators: dict[str, tuple[str, str]],
) -> None:
    """Remember the latest user query and human originator for a session."""
    if not is_chat_send_message(msg):
        return
    params = msg.params if isinstance(msg.params, dict) else {}
    if params.get("is_supplement") is True:
        return
    session_id = str(msg.session_id or "").strip()
    if not session_id:
        return
    query = str(params.get("query") or params.get("content") or "").strip()
    if not query:
        return
    user_queries[session_id] = query[:8000]
    # 记录最近人类发起者身份（member_name 由 resolve_member_by_user 注入 msg.metadata），
    # 供 send_file 在 file msg 不携带发起者时按 session_id 反查定向。
    # 排除 GodView：它是通道级自动注册的伪 member，非真实 /join 人类席位，不应作为发起者定向。
    # 无 member_name 的入站（如 web：web 不 /join，resolve_member_by_user 不命中）→ 清空
    # last-originator，避免 web 发起时残留上一次 feishu 用户导致文件误投到 feishu。
    origin_member = str((msg.metadata or {}).get("member_name") or "").strip()
    if origin_member and origin_member != SubRole.GODVIEW:
        originators[session_id] = (
            str(msg.channel_id or "").strip(),
            origin_member,
        )
    else:
        originators.pop(session_id, None)


def get_last_user_query(
    user_queries: Mapping[str, str],
    session_id: str | None,
) -> str:
    """Return the latest remembered query for a session."""
    if not session_id:
        return ""
    return user_queries.get(str(session_id), "")


def get_last_originator(
    originators: Mapping[str, tuple[str, str]],
    session_id: str | None,
) -> tuple[str, str] | None:
    """Return the latest human originator for a session."""
    if not session_id:
        return None
    return originators.get(str(session_id))


def attach_original_request_to_ask_user_answer(
    msg: "Message",
    *,
    user_queries: Mapping[str, str],
) -> "Message":
    """Attach the remembered original request to an interrupt answer."""
    params = msg.params if isinstance(msg.params, dict) else {}
    if str(params.get("source") or "").strip() != "ask_user_interrupt":
        return msg
    if params.get("original_request"):
        return msg
    original_request = get_last_user_query(user_queries, msg.session_id)
    if not original_request:
        return msg

    answer_params = dict(msg.params)
    answer_params["original_request"] = original_request
    return replace(msg, params=answer_params)
