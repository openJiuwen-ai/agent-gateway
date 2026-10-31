# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure channel state helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.models import (
    ChannelControlState,
    channel_mode_from_str,
)

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


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


def get_channel_default_state(
    handler: Any, channel_id: str
) -> ChannelControlState:
    """Read a channel's default session and mode from config."""
    try:
        cfg: dict[str, Any] = handler._get_config_raw()
    except Exception:  # noqa: BLE001
        cfg = {}
    return channel_state_from_config(channel_id, cfg)


def get_or_create_channel_state(
    handler: Any, msg: "Message"
) -> ChannelControlState:
    """Get, load, or create the channel state for an inbound message."""
    ch = msg.channel_id
    key = handler._get_channel_state_key(ch, msg.session_id)
    state = handler._channel_states.get(key)
    if state is not None:
        return state

    state = handler._get_channel_default_state(ch)
    identity_key = handler._extract_identity_tuple(msg)
    if identity_key and handler._channel_id_matches_session_map_types(str(ch or "")):
        state.session_id = handler._session_map.find_session_id(*identity_key)
    handler._channel_states[key] = state
    return state


def save_channel_state_to_config(
    handler: Any, channel_id: str
) -> None:
    """Persist a channel's default session and mode back to config."""
    state = handler._channel_states.get(channel_id)
    if not state:
        return
    handler._update_channel_in_config(
        channel_id,
        {
            "default_session_id": state.session_id or "",
            "default_mode": state.mode.value if hasattr(state.mode, "value") else str(state.mode),
        },
    )


def remember_alloc_state_key(
    handler: Any, msg: "Message", channel_id: str
) -> None:
    """Remember the original state key for sticky channel session allocation."""
    if str(channel_id or "") not in handler._sticky_channel_state_types:
        return
    handler._pending_alloc_state_key[msg.id] = handler._get_channel_state_key(
        str(channel_id or ""), msg.session_id
    )
