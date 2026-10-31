# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""ACP session allocation and alias helpers extracted from MessageHandler."""

from __future__ import annotations

import secrets
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import ReqMethod


ACP_CHANNEL_ID = "acp"
ACP_ORIGINAL_SESSION_ID_KEY = "acp_original_session_id"

if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


async def ensure_acp_agent_session(handler: Any) -> str:
    """Allocate a real AgentServer-owned session for ACP."""
    env = e2a_from_agent_fields(
        request_id=(
            f"acp-session-create-{int(time.time() * 1000):x}-{secrets.token_hex(3)}"
        ),
        channel_id=ACP_CHANNEL_ID,
        req_method=ReqMethod.SESSION_CREATE,
        params={
            "create_token": secrets.token_hex(16),
            "mode": "agent",
            "is_swarm": False,
        },
        is_stream=False,
        timestamp=time.time(),
    )
    resp = await handler._send_non_stream_agent_request(env)
    if not resp.ok:
        payload = dict(resp.payload or {}) if isinstance(resp.payload, dict) else {}
        raise RuntimeError(str(payload.get("error") or "acp session.create failed"))
    payload = dict(resp.payload or {}) if isinstance(resp.payload, dict) else {}
    resolved = payload.get("sessionId") or payload.get("session_id")
    resolved_str = str(resolved or "").strip()
    if not resolved_str:
        raise RuntimeError("acp session.create returned empty session_id")
    return resolved_str


async def resolve_acp_internal_session_id(
    handler: Any,
    external_session_id: str | None,
) -> tuple[str | None, bool]:
    """Resolve an external ACP session ID to an internal AgentServer session."""
    external = str(external_session_id or "").strip()
    if not external:
        return None, False

    cached = handler._acp_session_aliases.get(external)
    if cached:
        return cached, cached != external

    async with handler._acp_session_alias_lock:
        cached = handler._acp_session_aliases.get(external)
        if cached:
            return cached, cached != external

        ensured = await handler._ensure_acp_agent_session()
        handler._acp_session_aliases[external] = ensured
        return ensured, ensured != external


async def prepare_agent_dispatch_message(
    handler: Any,
    msg: "Message",
) -> "Message":
    """Prepare a message for AgentServer dispatch."""
    msg = handler._attach_original_request_to_ask_user_answer(msg)
    if msg.channel_id != ACP_CHANNEL_ID:
        return msg
    if msg.req_method in (ReqMethod.INITIALIZE, ReqMethod.SESSION_CREATE):
        return msg

    internal_session_id, aliased = await handler._resolve_acp_internal_session_id(
        msg.session_id
    )
    if not internal_session_id:
        return msg

    params = dict(msg.params or {})
    params["session_id"] = internal_session_id

    metadata = dict(msg.metadata or {})
    if aliased:
        metadata.setdefault(
            ACP_ORIGINAL_SESSION_ID_KEY, str(msg.session_id or "")
        )

    return replace(
        msg,
        session_id=internal_session_id,
        params=params,
        metadata=metadata or None,
    )


def resolve_acp_external_session_id(
    handler: Any,
    session_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    """Resolve an internal session ID back to the external ACP ID."""
    sid = str(session_id or "").strip()
    if not sid:
        return None

    original = ""
    if isinstance(metadata, dict):
        original = str(metadata.get(ACP_ORIGINAL_SESSION_ID_KEY) or "").strip()
    if original:
        return original

    for external, internal in handler._acp_session_aliases.items():
        if internal == sid:
            return external
    return sid
