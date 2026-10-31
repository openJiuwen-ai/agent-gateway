# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Controlled-channel slash command dispatch extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from agent_gateway.message_handler.command_parser.slash_command import (
    ParsedControlAction,
    parse_channel_control_text,
)
from agent_gateway.message_handler.review_prompt_injection import (
    inject_review_prompt,
    inject_security_review_prompt,
)
from agent_gateway.message_handler.session_commands import (
    handle_new_session_command,
    handle_persist_command,
)
from agent_gateway.message_handler.switch_command import handle_switch_command


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message
    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)


async def handle_channel_control(
    handler: "MessageHandler",
    msg: "Message",
) -> bool:
    r"""处理 \new_session / \mode / \skills 指令.

    Returns:
        True: 该消息是控制指令，已处理完毕，不需要转发给 Agent。
        False: 非控制指令，继续正常处理。
    """
    user_infos = {
        "id": msg.id,
        "meta_data": msg.metadata,
        "app_id": getattr(msg, "app_id", None) or getattr(msg, "bot_id", None) or "",
        "bot_id": getattr(msg, "bot_id", None) or "",
    }

    ch = msg.channel_id
    channel_type = handler._resolve_control_channel_type(msg)
    if channel_type not in handler._control_channel_types:
        return False

    params = msg.params or {}
    text = str(params.get("query") or params.get("content") or "").strip()
    if not text:
        return False

    parsed = parse_channel_control_text(text)
    if parsed.action is ParsedControlAction.NONE:
        return False

    logger.info(
        "[MessageHandler] _handle_channel_control channel=%s text=%s action=%s",
        channel_type,
        text,
        parsed.action.value,
    )

    if parsed.action is ParsedControlAction.SKILLS_OK:
        asyncio.create_task(
            handler._skills_slash_notice(user_infos, ch, msg.session_id, msg)
        )
        return True

    # 获取当前会话的状态（使用复合键）
    state = handler.get_or_create_channel_state(msg)

    # /join 已认领 team 席位后，除白名单（/exit、只读 /skills list 等）外的控制指令
    # 一律拒绝——执行类指令会改 session/mode/分支/历史或注入 query，破坏席位绑定
    # 或扰乱 team 流程。必须先 /exit。白名单与判定收敛在 JoinExitHandlers，见该类。
    if (
        not handler._join_exit.is_allowed_when_joined(parsed.action)
        and handler._join_exit.sender_has_joined(msg)
    ):
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "⚠️ 当前已加入 team session，不能执行该指令。请先执行 **/exit** 退出后再试。",
            )
        )
        return True

    if parsed.action is ParsedControlAction.NEW_SESSION_OK:
        return await handle_new_session_command(
            handler,
            msg,
            state,
            user_infos,
            ch,
            channel_type,
        )
    if parsed.action is ParsedControlAction.PERSIST_OK:
        return await handle_persist_command(
            handler,
            msg,
            parsed,
            state,
            user_infos,
            ch,
            channel_type,
        )
    if parsed.action is ParsedControlAction.PERSIST_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "指令格式错误。正确格式：/persist <第一条任务>",
            )
        )
        return True
    if parsed.action is ParsedControlAction.NEW_SESSION_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "非法指令",
            )
        )
        return True

    if parsed.action is ParsedControlAction.MODE_OK:
        mode_str = parsed.mode_subcommand or ""
        return handler.handle_mode_switch(
            mode_str,
            state=state,
            user_infos=user_infos,
            channel_id=ch,
            reply_session_id=msg.session_id,
            msg=msg,
        )
    if parsed.action is ParsedControlAction.SWITCH_OK:
        return await handle_switch_command(
            handler,
            msg,
            parsed,
            state,
            user_infos,
            ch,
        )
    if parsed.action in (ParsedControlAction.MODE_BAD, ParsedControlAction.SWITCH_BAD):
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "非法指令",
            )
        )
        return True

    if parsed.action is ParsedControlAction.BRANCH_OK:
        asyncio.create_task(
            handler._branch_slash_notice(
                user_infos, ch, msg.session_id, msg,
                branch_name=parsed.branch_name or "",
            )
        )
        return True

    if parsed.action is ParsedControlAction.REWIND_OK:
        # 两步确认：先发送确认提示，不立即执行
        turn_index = parsed.rewind_turn or 1
        asyncio.create_task(
            handler._rewind_slash_confirm_prompt(
                user_infos, ch, msg.session_id, msg,
                turn_index=turn_index,
            )
        )
        return True

    if parsed.action is ParsedControlAction.REWIND_CONFIRM:
        # 用户确认执行 rewind
        asyncio.create_task(
            handler._rewind_slash_notice(
                user_infos, ch, msg.session_id, msg,
                turn_index=parsed.rewind_turn or 1,
            )
        )
        return True

    if parsed.action is ParsedControlAction.REWIND_CANCEL:
        # 用户取消 rewind
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos, ch, msg.session_id,
                "[收到 /rewind cancel] 已取消回退操作。",
            )
        )
        return True

    if parsed.action is ParsedControlAction.REWIND_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "非法指令，/rewind 须带正整数轮次编号，如 /rewind 2",
            )
        )
        return True

    # ── origin/develop: /review / /security-review 指令 ──
    if parsed.action is ParsedControlAction.REVIEW_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "非法指令，/review 参数过长或含有非法控制字符",
            )
        )
        return True

    # ── V2: /join / /exit 共享会话指令 ──
    if parsed.action is ParsedControlAction.JOIN_OK:
        asyncio.create_task(
            handler._join_exit.join_slash_handler(user_infos, ch, msg, parsed)
        )
        return True
    if parsed.action is ParsedControlAction.JOIN_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos, ch, msg.session_id,
                "⚠️ join 指令格式错误",
            )
        )
        return True
    if parsed.action is ParsedControlAction.EXIT_OK:
        asyncio.create_task(
            handler._join_exit.exit_slash_handler(user_infos, ch, msg, parsed)
        )
        return True
    if parsed.action is ParsedControlAction.EXIT_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos, ch, msg.session_id,
                "exit 指令格式错误。正确格式：/exit <session_id>",
            )
        )
        return True

    # ── origin/develop: /review / /security-review 指令 ──
    if parsed.action is ParsedControlAction.REVIEW_OK:
        return await inject_review_prompt(
            handler,
            msg,
            parsed,
            channel_type=channel_type,
        )

    if parsed.action is ParsedControlAction.SECURITY_REVIEW_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                ch,
                msg.session_id,
                "非法指令，/security-review 参数过长或含有非法控制字符",
            )
        )
        return True

    if parsed.action is ParsedControlAction.SECURITY_REVIEW_OK:
        return await inject_security_review_prompt(
            handler,
            msg,
            parsed,
            channel_type=channel_type,
        )

    return False
