# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Wire-message conversion helpers extracted from MessageHandler."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.agent_events import (
    merge_agent_metadata,
    normalize_agent_error_event,
    uses_legacy_chat_error_protocol,
)
from agent_gateway.message_handler.models import (
    apply_a2ui_text_fallback_to_gateway_payload,
    normalize_legacy_health_check_relay_payload,
)

if TYPE_CHECKING:
    from openjiuwen_protocol.e2a.agent_models import AgentResponse, AgentResponseChunk
    from openjiuwen_protocol.e2a.models import E2AEnvelope
    from openjiuwen_protocol.message import Message


logger = logging.getLogger(__name__)


def message_to_e2a(msg: "Message") -> "E2AEnvelope":
    from openjiuwen_protocol.e2a.gateway_normalize import message_to_e2a_or_fallback

    return message_to_e2a_or_fallback(msg)


def response_to_message(
    resp: "AgentResponse",
    session_id: str | None,
    *,
    request_metadata: dict[str, Any] | None = None,
    app_id: str | None = None,
    request_method: Any = None,
    request_params: dict[str, Any] | None = None,
) -> "Message":
    from openjiuwen_protocol.message import EventType, Message

    metadata = merge_agent_metadata(request_metadata, resp.metadata)
    resp_agent_ref = getattr(resp, "agent_ref", None)
    group_digital_avatar = (
        bool(metadata.get("group_digital_avatar", False)) if metadata else False
    )
    enable_memory = bool(metadata.get("enable_memory", True)) if metadata else True

    event_type = None
    payload = resp.payload
    if resp.payload and isinstance(resp.payload, dict):
        payload = apply_a2ui_text_fallback_to_gateway_payload(
            dict(resp.payload),
            channel_id=resp.channel_id,
        )
        payload = normalize_legacy_health_check_relay_payload(payload)
        event_type_str = normalize_agent_error_event(
            payload,
            legacy_failed=(
                not bool(resp.ok)
                and uses_legacy_chat_error_protocol(
                    request_method,
                    request_params,
                )
            ),
        )
        if isinstance(event_type_str, str):
            try:
                event_type = EventType(event_type_str)
                return Message(
                    id=resp.request_id,
                    type="event",
                    channel_id=resp.channel_id,
                    session_id=session_id,
                    params={},
                    timestamp=time.time(),
                    ok=event_type != EventType.CHAT_ERROR,
                    payload=payload,
                    event_type=event_type,
                    metadata=metadata,
                    app_id=app_id,
                    agent_ref=resp_agent_ref,
                    group_digital_avatar=group_digital_avatar,
                    enable_memory=enable_memory,
                )
            except ValueError:
                pass

    return Message(
        id=resp.request_id,
        type="res",
        channel_id=resp.channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=resp.ok,
        payload=payload,
        event_type=EventType.CHAT_FINAL,
        metadata=metadata,
        app_id=app_id,
        agent_ref=resp_agent_ref,
        group_digital_avatar=group_digital_avatar,
        enable_memory=enable_memory,
    )


def chunk_to_message(
    chunk: "AgentResponseChunk",
    session_id: str | None,
    *,
    metadata: dict[str, Any] | None = None,
    app_id: str | None = None,
) -> "Message":
    from openjiuwen_protocol.message import EventType, Message

    group_digital_avatar = (
        bool(metadata.get("group_digital_avatar", False)) if metadata else False
    )
    enable_memory = bool(metadata.get("enable_memory", True)) if metadata else True
    chunk_md = getattr(chunk, "metadata", None) or {}
    merged_metadata: dict[str, Any] | None = None
    if metadata or chunk_md:
        merged_metadata = {**(metadata or {}), **chunk_md}

    chunk_agent_ref = getattr(chunk, "agent_ref", None)
    event_type = None
    payload = chunk.payload
    if chunk.payload and isinstance(chunk.payload, dict):
        payload = apply_a2ui_text_fallback_to_gateway_payload(
            dict(chunk.payload),
            channel_id=chunk.channel_id,
        )
        payload = normalize_legacy_health_check_relay_payload(payload)
        event_type_str = normalize_agent_error_event(
            payload,
            legacy_failed=bool(getattr(chunk, "is_complete", False)),
        )
        if isinstance(event_type_str, str):
            try:
                event_type = EventType(event_type_str)
            except ValueError:
                logger.debug("未知的 event_type: %s", event_type_str)

    return Message(
        id=chunk.request_id,
        type="event",
        channel_id=chunk.channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=event_type != EventType.CHAT_ERROR,
        payload=payload,
        event_type=event_type,
        metadata=merged_metadata,
        app_id=app_id,
        agent_ref=chunk_agent_ref,
        group_digital_avatar=group_digital_avatar,
        enable_memory=enable_memory,
    )
