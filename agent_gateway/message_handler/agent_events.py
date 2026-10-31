# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure agent event helpers extracted from MessageHandler."""

from __future__ import annotations

from typing import Any

from openjiuwen_protocol.message import ReqMethod


_DELIVERY_IDENTITY_METADATA_KEYS = frozenset({
    "app_id",
    "chat_type",
    "im_chat_type",
    "feishu_chat_id",
    "feishu_open_id",
    "open_id",
    "im_sender_user_id",
    "im_thread_id",
})


def merge_agent_metadata(
    request_metadata: dict[str, Any] | None,
    response_metadata: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Merge agent response metadata with gateway request metadata."""
    req_md = request_metadata or {}
    resp_md = response_metadata or {}
    if not req_md and not resp_md:
        return None
    merged: dict[str, Any] = {**req_md, **resp_md}
    for key in _DELIVERY_IDENTITY_METADATA_KEYS:
        if key in req_md:
            merged[key] = req_md[key]
    return merged


def normalize_agent_error_event(
    payload: dict[str, Any],
    *,
    legacy_failed: bool,
) -> str | None:
    """Normalize runtime and legacy agent errors for channel delivery."""
    source_event = payload.get("event_type")
    runtime_error = source_event in ("runtime.error", "chat.error")
    legacy_error = (
        legacy_failed
        and source_event in (None, "")
        and "error" in payload
    )
    if runtime_error or legacy_error:
        payload["event_type"] = "chat.error"
        payload["is_complete"] = True
        return "chat.error"
    return source_event if isinstance(source_event, str) else None


def uses_legacy_chat_error_protocol(
    request_method: Any,
    request_params: dict[str, Any] | None = None,
) -> bool:
    """Whether a source-less unary failure belongs to the chat event flow."""
    method = getattr(request_method, "value", request_method)
    if method in {
        ReqMethod.CHAT_SEND.value,
        ReqMethod.CHAT_RESUME.value,
        ReqMethod.CHAT_ANSWER.value,
        ReqMethod.CHAT_SWARMFLOW_REPLY.value,
    }:
        return True
    return (
        method == ReqMethod.CHAT_CANCEL.value
        and isinstance(request_params, dict)
        and request_params.get("intent") == "resume"
    )
