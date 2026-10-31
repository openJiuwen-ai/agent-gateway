# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure session and delivery identity helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import Any

from agent_gateway.channel_manager.base import ChannelType


_KNOWN_JIUWENSWARM_SESSION_PREFIXES = (
    "sess_",
    "web_",
    "tui_",
    "acp_",
    "cron_",
    "feishu_",
    "wechat_",
    "xiaoyi_",
    "dingtalk_",
    "wecom_",
    "telegram_",
    "discord_",
    "slack_",
    "whatsapp_",
)
_SINGLE_USER_CHANNEL_IDS = frozenset({
    ChannelType.ACP.value,
})


def extract_session_id_from_ref(
    session_ref: str | None,
) -> str | None:
    """Extract session ID from a full team ref or a simple session ID."""
    if not session_ref:
        return None
    parts = session_ref.split("_session_")
    if len(parts) == 2:
        return parts[1]
    return session_ref


def extract_team_name_from_ref(session_ref: str | None) -> str:
    """Extract team name from a full team session reference."""
    if not session_ref:
        return "unknown"
    prefix = session_ref.replace("team_", "", 1)
    return prefix.split("_session_")[0]


def resolve_app_id(msg: Any) -> str:
    """Resolve the effective app ID from a V2 message."""
    return (
        getattr(msg, "app_id", None)
        or getattr(msg, "bot_id", None)
        or "default"
    )


def is_session_map_style_session_id(session_id: str) -> bool:
    """Return whether an ID uses the five/six-part session-map format."""
    parts = [
        part.strip()
        for part in str(session_id or "").split("::")
    ]
    if len(parts) not in (5, 6):
        return False
    return all(parts)


def is_known_jiuwenswarm_session_id(session_id: str | None) -> bool:
    """Return whether an ID has a known gateway session prefix or map shape."""
    sid = str(session_id or "").strip()
    if not sid:
        return False
    if sid.startswith(_KNOWN_JIUWENSWARM_SESSION_PREFIXES):
        return True
    return is_session_map_style_session_id(sid)


def is_single_user_channel(channel_id: str) -> bool:
    """Return whether a new chat.send replaces work on the whole channel."""
    return channel_id in _SINGLE_USER_CHANNEL_IDS
