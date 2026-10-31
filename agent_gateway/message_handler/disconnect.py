# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure disconnect cancel helpers extracted from MessageHandler."""

from __future__ import annotations

import secrets
import asyncio
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.message import Message, ReqMethod

from agent_gateway.message_handler.session_identity import is_single_user_channel
from agent_gateway.message_handler.session_state import make_channel_state_key


def collect_stream_tasks_to_cancel(
    *,
    stream_tasks: Mapping[str, asyncio.Task],
    stream_sessions: Mapping[str, str | None],
    stream_channels: Mapping[str, str],
    non_stream_chat_messages: Mapping[str, Any],
    old_sid: str | None,
    channel_id: str | None,
) -> tuple[list[asyncio.Task], list[str], bool]:
    """Collect in-flight tasks matching a session (and optional channel) filter."""
    tasks_to_cancel: list[asyncio.Task] = []
    rids_cancelled: list[str] = []

    for rid, task in list(stream_tasks.items()):
        if stream_sessions.get(rid) != old_sid:
            continue
        if channel_id is not None and stream_channels.get(rid) != channel_id:
            continue
        if not task.done():
            rids_cancelled.append(rid)
            tasks_to_cancel.append(task)

    has_unary = any(
        item.session_id == old_sid and (channel_id is None or item.channel_id == channel_id)
        for item in non_stream_chat_messages.values()
    )
    return tasks_to_cancel, rids_cancelled, has_unary


def resolve_cancel_params(
    msg_params: dict[str, Any] | None,
    old_sid: str | None,
    msg_channel_id: str,
    *,
    channel_states: Mapping[str, Any],
) -> dict[str, Any]:
    """Build cancel request params from message params and channel state."""
    sid_for_agent = (old_sid or "").strip()
    cancel_params: dict[str, Any] = {
        "intent": "cancel",
        "session_id": sid_for_agent,
    }
    cancel_mode = None
    if isinstance(msg_params, dict) and msg_params.get("mode"):
        cancel_mode = msg_params["mode"]
    else:
        state = channel_states.get(
            make_channel_state_key(msg_channel_id, sid_for_agent)
        ) or channel_states.get(msg_channel_id)
        if state is not None:
            cancel_mode = state.mode.value if hasattr(state.mode, "value") else str(state.mode)
    if cancel_mode:
        cancel_params["mode"] = cancel_mode
    if isinstance(msg_params, dict) and msg_params.get("team"):
        cancel_params["team"] = msg_params["team"]
    if isinstance(msg_params, dict) and msg_params.get("trusted_dirs"):
        cancel_params["trusted_dirs"] = msg_params["trusted_dirs"]
    if isinstance(msg_params, dict):
        for key in ("work_mode", "project_dir"):
            value = msg_params.get(key)
            if value is not None and str(value).strip():
                cancel_params[key] = value
    return cancel_params


def clone_message_for_session_cancel(
    msg: "Message",
    session_id: str,
    *,
    mode: str | None = None,
) -> "Message":
    """Build a Message suitable for ``_cancel_agent_work_for_session``."""
    params = dict(msg.params) if isinstance(msg.params, dict) else {}
    if mode:
        params["mode"] = mode
    return replace(msg, session_id=session_id, params=params)


def should_cancel_stream_on_channel(
    channel_id: str,
    new_session_id: str | None,
    task_session: str | None,
) -> bool:
    """Whether an ordinary chat.send replaces this in-flight host stream."""
    if new_session_id and task_session == new_session_id:
        return True
    return is_single_user_channel(channel_id)


def resolve_stream_cancel_session_id(
    channel_id: str,
    *,
    stream_sessions: Mapping[str, str | None],
    stream_channels: Mapping[str, str],
    channel_states: Mapping[str, Any],
) -> str | None:
    """Best-effort session id when a stream task lacks ``_stream_sessions``."""
    for rid, sess in stream_sessions.items():
        if stream_channels.get(rid) != channel_id:
            continue
        candidate = (sess or "").strip()
        if candidate:
            return candidate
    state_key_prefix = f"{channel_id}:"
    for key in channel_states:
        if key == channel_id:
            continue
        if key.startswith(state_key_prefix):
            suffix = key[len(state_key_prefix):].strip()
            if suffix:
                return suffix
    return None


def merge_disconnect_session_keys(
    session_keys: list[tuple[str, ...]],
    *,
    stale_request_keys: list[tuple[str, ...]] | None = None,
    stream_sessions: Mapping[str, str | None],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Merge direct session keys with request-recovered session keys."""
    merged: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(channel_id: str, session_id: str) -> bool:
        sid = (session_id or "").strip()
        if not channel_id or not sid:
            return False
        entry = (channel_id, sid)
        if entry in seen:
            return False
        seen.add(entry)
        merged.append(entry)
        return True

    for route_key in session_keys or []:
        if len(route_key) >= 2:
            add(route_key[0], route_key[1])

    recovered_via_requests: list[tuple[str, str]] = []
    for route_key in stale_request_keys or []:
        if len(route_key) < 2:
            continue
        channel_id, request_id = route_key[:2]
        task_session = (stream_sessions.get(request_id) or "").strip()
        if add(channel_id, task_session):
            recovered_via_requests.append((channel_id, task_session))

    return merged, recovered_via_requests


def build_disconnect_cancel_message(
    channel_id: str,
    session_id: str,
    *,
    user_id: str | None = None,
    mode: str | None = None,
    channel_states: Mapping[str, Any],
) -> "Message":
    """Build a cancel message for a client disconnect."""
    disconnect_params = {
        "intent": "cancel",
        "session_id": session_id,
    }
    # mode decides whether AgentServer routes the cancel into the team
    # interrupt path (workflow_disposition=pause for disconnects). An
    # explicit mode (the TUI's tui.disconnect request carries the live
    # client mode) wins; the channel-state table is the fallback for
    # passive transport-close cancels - for TUI/web the table is never
    # populated (they are not control channels), and a mode-less cancel
    # falls into the non-team terminate path, hard-stopping running
    # workflows instead of pausing them.
    cancel_mode = (mode or "").strip()
    if not cancel_mode:
        disconnect_state = channel_states.get(
            make_channel_state_key(channel_id, session_id)
        ) or channel_states.get(channel_id)
        if disconnect_state is not None:
            cancel_mode = (
                disconnect_state.mode.value
                if hasattr(disconnect_state.mode, "value")
                else str(disconnect_state.mode)
            )
    if cancel_mode:
        disconnect_params["mode"] = cancel_mode
    return Message(
        id=f"ws_drop_{int(time.time() * 1000):x}_{secrets.token_hex(4)}",
        type="req",
        channel_id=channel_id,
        session_id=session_id,
        params=disconnect_params,
        timestamp=time.time(),
        ok=True,
        req_method=ReqMethod.CHAT_CANCEL,
        is_stream=False,
        user_id=user_id,
    )
