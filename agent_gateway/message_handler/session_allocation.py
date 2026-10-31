# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Channel session allocation and external session mapping helpers."""

from __future__ import annotations

import secrets
import time
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.message_handler.models import (
    ChannelControlState,
    channel_mode_from_str,
)

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


async def allocate_channel_session(
    handler: Any,
    msg: "Message",
    state: ChannelControlState,
    *,
    persist_session: bool = False,
) -> str:
    """Allocate and persist a real AgentServer-owned session for a channel."""
    channel_type = handler._resolve_control_channel_type(msg)
    mode = state.mode.value
    params = dict(msg.params or {})
    create_params = {
        "create_token": secrets.token_hex(16),
        "mode": mode,
        "is_swarm": ChannelMode.is_team_mode(mode),
    }
    if persist_session:
        create_params["persist_session"] = True
    for name in ("project_id", "project_dir", "work_mode", "model_name"):
        if params.get(name) is not None:
            create_params[name] = params[name]
    env = e2a_from_agent_fields(
        request_id=f"session-create-{int(time.time() * 1000):x}-{secrets.token_hex(3)}",
        channel_id=channel_type,
        req_method=ReqMethod.SESSION_CREATE,
        params=create_params,
        is_stream=False,
        timestamp=time.time(),
    )
    resp = await handler._send_non_stream_agent_request(env)
    payload = dict(resp.payload or {}) if isinstance(resp.payload, dict) else {}
    if not resp.ok:
        raise RuntimeError(str(payload.get("error") or "session.create failed"))
    sid = str(payload.get("session_id") or payload.get("sessionId") or "").strip()
    if not sid:
        raise RuntimeError("session.create returned empty session_id")
    state.session_id = sid
    identity_key = handler._extract_identity_tuple(msg)
    if identity_key and handler._channel_id_matches_session_map_types(
        str(msg.channel_id or "")
    ):
        handler._session_map.set_session_id(*identity_key, sid)
    return sid


async def ensure_inbound_session(
    handler: Any,
    msg: "Message",
) -> None:
    """Allocate an inbound controlled-channel session when needed."""
    channel_type = handler._resolve_control_channel_type(msg)
    if channel_type not in handler._control_channel_types:
        return
    if str(msg.session_id or "").strip():
        return
    key = handler._pending_alloc_state_key.pop(msg.id, "")
    state = handler._channel_states.get(key) if key else None
    if state is None:
        state = handler.get_or_create_channel_state(msg)
    msg.session_id = await handler._allocate_channel_session(msg, state)
    # 兼容旧行为：同一次分配也留一份 channel 级状态（cancel/disconnect 的
    # mode 兜底按 channel_id 键读取）。
    handler._channel_states.setdefault(channel_type, state)


async def resolve_external_channel_session(
    handler: Any,
    msg: "Message",
) -> None:
    """Map A2A protocol IDs onto AgentServer-owned product Sessions."""
    channel_id = str(msg.channel_id or "").strip()
    external_id = str(msg.session_id or "").strip()
    if channel_id != "a2a" or not external_id:
        return
    key = (channel_id, external_id)
    resolved = handler._external_session_aliases.get(key)
    if resolved is None:
        async with handler._external_session_alias_lock:
            resolved = handler._external_session_aliases.get(key)
            if resolved is None:
                raw_mode = str((msg.params or {}).get("mode") or "agent")
                # 与 /mode 分发同源：旧 canonical 先经 deprecate_mode 映射到
                # 新 canonical 再构造 ChannelMode，否则 a2a 通道的旧值（如
                # agent.fast / team.plan）会被原样解析成 ValueError 兜底为
                # AGENT，丢失原有语义。
                mode = channel_mode_from_str(raw_mode)
                resolved = await handler._allocate_channel_session(
                    msg, ChannelControlState(mode=mode)
                )
                handler._external_session_aliases[key] = resolved
    metadata = dict(msg.metadata or {})
    metadata.setdefault("external_session_id", external_id)
    msg.metadata = metadata
    msg.session_id = resolved
