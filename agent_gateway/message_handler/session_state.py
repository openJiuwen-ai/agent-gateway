# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure channel state helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import Any

from agent_gateway.message_handler.models import (
    ChannelControlState,
    channel_mode_from_str,
)


def make_channel_state_key(
    channel_id: str, conversation_id: str | None
) -> str:
    """Build the compound state key used by channel control state."""
    if conversation_id:
        return f"{channel_id}:{conversation_id}"
    return channel_id


def channel_state_from_config(
    channel_id: str, config: dict[str, Any] | None
) -> ChannelControlState:
    """Build a channel state from persisted channel configuration."""
    channels_cfg = (config or {}).get("channels") or {}
    ch_cfg = channels_cfg.get(channel_id) or {}
    sid_raw = ch_cfg.get("default_session_id") or ""
    sid = str(sid_raw).strip() or None
    mode_raw = str(ch_cfg.get("default_mode") or "agent").strip().lower()
    mode = channel_mode_from_str(mode_raw)
    return ChannelControlState(session_id=sid, mode=mode)
