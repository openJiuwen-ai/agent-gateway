# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure runtime wire-state helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import Any

from openjiuwen_protocol.message import ReqMethod, TEAM_ATTACH_ONLY_PARAM

from agent_gateway.message_handler.models import ChannelMode


_BEFORE_CHAT_HOOK_WATCH_PARAM_KEYS = ("query", "model_name")


def is_terminal_stream_chunk(chunk: Any) -> bool:
    """Identify a sentinel chunk that ends the stream but carries no event."""
    if not bool(getattr(chunk, "is_complete", False)):
        return False
    payload = getattr(chunk, "payload", None)
    if not payload:
        return True
    if not isinstance(payload, dict):
        return False
    if payload.get("event_type"):
        return False
    if payload.get("content") not in (None, ""):
        return False
    if payload.get("error") not in (None, ""):
        return False
    return payload.get("is_complete") is True and set(payload.keys()) <= {"is_complete"}


def non_stream_rpc_may_run_parallel(env: Any) -> bool:
    """Return whether a non-streaming RPC can run concurrently."""
    m = getattr(env.method, "value", env.method)
    if not m:
        return False
    return m not in (
        ReqMethod.CHAT_SEND.value,
        ReqMethod.CHAT_RESUME.value,
        ReqMethod.CHAT_CANCEL.value,
        ReqMethod.CHAT_ANSWER.value,
    )


def should_trigger_before_chat_request_hook(msg: Any) -> bool:
    """Return whether the before-chat hook applies to this message."""
    return msg.req_method in (
        ReqMethod.CHAT_SEND,
        ReqMethod.CHAT_RESUME,
        ReqMethod.CHAT_ANSWER,
    )


def should_emit_processing_status_for_stream(msg: Any) -> bool:
    """Return whether a stream should emit processing status."""
    if msg.req_method != ReqMethod.CHAT_SEND:
        return False
    # An attach-only stream is a pure observation subscription: it must
    # neither raise the Session's processing state on start nor block the
    # authoritative is_processing=False of the streams that actually own
    # the turn (a blocked terminal leaves the busy map stuck on True).
    params = msg.params if isinstance(msg.params, dict) else {}
    return params.get(TEAM_ATTACH_ONLY_PARAM) is not True


def expert_stream_route(params: dict[str, Any] | None) -> dict[str, str]:
    """Derive expert routing from the request, independent of UI selection."""
    params = params if isinstance(params, dict) else {}
    team_id = str(params.get("target_team_id") or "").strip()
    if not team_id or not ChannelMode.is_team_mode(params.get("mode")):
        return {}
    return {"team_id": team_id, "team_name": team_id, "source": "org_expert_direct"}


def stream_method_value(msg: Any) -> str:
    """Normalize ``msg.req_method`` to its wire value string."""
    req_method = getattr(msg, "req_method", None)
    if req_method is None:
        return ""
    return str(getattr(req_method, "value", req_method) or "")


def snapshot_hook_watch_params(msg: Any) -> dict[str, Any]:
    """Snapshot user-visible params before before-chat hooks mutate them."""
    params = msg.params if isinstance(msg.params, dict) else {}
    return {k: params.get(k) for k in _BEFORE_CHAT_HOOK_WATCH_PARAM_KEYS}
