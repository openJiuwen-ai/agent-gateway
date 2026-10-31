# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""/new_session and /persist command handlers extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.models import NewSessionCancelParams


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


logger = logging.getLogger(__name__)


async def handle_new_session_command(
    handler: Any,
    msg: "Message",
    state: Any,
    user_infos: dict[str, Any],
    channel_id: str,
    channel_type: str,
) -> bool:
    """Allocate a new controlled-channel session and cancel the old one."""
    old_sid = state.session_id
    try:
        state.session_id = None
        new_sid = await handler._allocate_channel_session(msg, state)
    except Exception:  # noqa: BLE001
        state.session_id = old_sid
        logger.exception(
            "[MessageHandler] 创建新会话失败 channel=%s message_id=%s",
            channel_type,
            msg.id,
        )
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                channel_id,
                msg.session_id,
                {"error": "创建新会话失败，请稍后重试"},
            )
        )
        return True

    # 触发 SessionStart hook
    if handler._gateway_hook_handler:
        asyncio.create_task(
            handler._gateway_hook_handler.on_session_start(
                new_sid, source=channel_type
            )
        )
    asyncio.create_task(
        handler._new_session_cancel_and_notice(
            NewSessionCancelParams(
                user_infos=user_infos,
                channel_id=channel_id,
                reply_session_id=msg.session_id,
                new_sid=new_sid,
                old_sid=old_sid,
            ),
            msg,
        )
    )
    return True


async def handle_persist_command(
    handler: Any,
    msg: "Message",
    parsed: Any,
    state: Any,
    user_infos: dict[str, Any],
    channel_id: str,
    channel_type: str,
) -> bool:
    """Allocate a persistent session, then forward the supplied first task."""
    old_sid = state.session_id
    try:
        state.session_id = None
        new_sid = await handler._allocate_channel_session(
            msg,
            state,
            persist_session=True,
        )
    except Exception:  # noqa: BLE001
        state.session_id = old_sid
        logger.exception(
            "[MessageHandler] 创建永续会话失败 channel=%s message_id=%s",
            channel_type,
            msg.id,
        )
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                channel_id,
                msg.session_id,
                {"error": "创建永续会话失败，请稍后重试"},
            )
        )
        return True

    task = parsed.persist_task or ""
    msg.session_id = new_sid
    msg.params = dict(msg.params or {})
    msg.params["query"] = task
    if "content" in msg.params:
        msg.params["content"] = task
    msg.metadata = dict(msg.metadata or {})
    msg.metadata["persist_session_first_task"] = True

    if handler._gateway_hook_handler:
        asyncio.create_task(
            handler._gateway_hook_handler.on_session_start(
                new_sid, source=channel_type
            )
        )
    asyncio.create_task(
        handler._new_session_cancel_and_notice(
            NewSessionCancelParams(
                user_infos=user_infos,
                channel_id=channel_id,
                reply_session_id=msg.session_id,
                new_sid=new_sid,
                old_sid=old_sid,
            ),
            msg,
        )
    )
    # 与 /review 相同：控制动作已经完成，改写后的正文继续作为首条任务。
    return False
