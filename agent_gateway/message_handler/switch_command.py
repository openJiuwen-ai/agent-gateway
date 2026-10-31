# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""/switch dispatch extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.models import (
    ChannelMode,
    ModeChangeCancelParams,
    _SWITCH_AGENT_WORK_MODES,
    _SWITCH_CODE_MODES,
)


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


logger = logging.getLogger(__name__)


async def handle_switch_command(
    handler: Any,
    msg: "Message",
    parsed: Any,
    state: "ChannelControlState",
    user_infos: dict[str, Any],
    channel_id: str,
) -> bool:
    """Apply a valid ``/switch`` target and dispatch the same notices."""
    switch_str = parsed.switch_subcommand or ""
    target_mode: ChannelMode | None = None
    # state.mode 经 handle_mode_switch 落定后必为新 canonical，故判据查新
    # canonical 字符串集合（与旧 ``in (ChannelMode.X, ...)`` 等价，见
    # ``_SWITCH_AGENT_WORK_MODES`` / ``_SWITCH_CODE_MODES``）。
    if switch_str == "plan":
        # agent 下 plan / fast 已合并：/switch plan 保持 agent.work.normal。
        if state.mode.value in _SWITCH_AGENT_WORK_MODES:
            target_mode = ChannelMode.AGENT_WORK_NORMAL
        elif state.mode.value in _SWITCH_CODE_MODES:
            target_mode = ChannelMode.AGENT_CODE_PLAN
    elif switch_str == "fast":
        # agent 下 plan / fast 已合并：/switch fast 保持 agent.work.normal。
        if state.mode.value in _SWITCH_AGENT_WORK_MODES:
            target_mode = ChannelMode.AGENT_WORK_NORMAL
    elif switch_str == "normal":
        if state.mode.value in _SWITCH_CODE_MODES:
            target_mode = ChannelMode.AGENT_CODE_NORMAL
    elif switch_str == "team":
        if state.mode.value in _SWITCH_CODE_MODES:
            target_mode = ChannelMode.TEAM_CODE_NORMAL

    if target_mode is None:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                channel_id,
                msg.session_id,
                "非法指令",
            )
        )
        return True

    old_mode = state.mode
    old_sid = state.session_id
    state.mode = target_mode
    new_label = state.mode.value
    if old_mode != state.mode:
        asyncio.create_task(
            handler._mode_change_cancel_and_notice(
                ModeChangeCancelParams(
                    user_infos=user_infos,
                    channel_id=channel_id,
                    reply_session_id=msg.session_id,
                    old_sid=old_sid,
                    new_mode_label=new_label,
                ),
                msg,
            )
        )
    else:
        asyncio.create_task(
            handler.send_channel_notice(
                user_infos,
                channel_id,
                msg.session_id,
                handler._build_mode_change_notice_text(new_label),
            )
        )
    return True
