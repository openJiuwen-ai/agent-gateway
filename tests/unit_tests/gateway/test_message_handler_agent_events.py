# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from agent_gateway.message_handler.message_handler import MessageHandler
from openjiuwen_protocol.message import ReqMethod


def test_merge_agent_metadata_prefers_response_and_keeps_delivery_identity():
    metadata = MessageHandler._merge_agent_metadata(
        {"open_id": "request-open-id", "custom": "request"},
        {"open_id": "response-open-id", "model": "qwen"},
    )

    assert metadata == {
        "open_id": "request-open-id",
        "custom": "request",
        "model": "qwen",
    }


def test_merge_agent_metadata_without_metadata_is_none():
    assert MessageHandler._merge_agent_metadata(None, None) is None


def test_normalize_agent_error_event_normalizes_runtime_and_legacy_errors():
    runtime_payload = {"event_type": "runtime.error"}
    assert MessageHandler._normalize_agent_error_event(
        runtime_payload,
        legacy_failed=False,
    ) == "chat.error"
    assert runtime_payload == {"event_type": "chat.error", "is_complete": True}

    legacy_payload = {"error": "boom"}
    assert MessageHandler._normalize_agent_error_event(
        legacy_payload,
        legacy_failed=True,
    ) == "chat.error"
    assert legacy_payload == {"error": "boom", "event_type": "chat.error", "is_complete": True}


def test_normalize_agent_error_event_passes_through_other_events():
    payload = {"event_type": "heartbeat.relay"}
    assert MessageHandler._normalize_agent_error_event(
        payload,
        legacy_failed=True,
    ) == "heartbeat.relay"
    assert payload == {"event_type": "heartbeat.relay"}


def test_uses_legacy_chat_error_protocol_detects_chat_methods():
    assert MessageHandler._uses_legacy_chat_error_protocol(
        ReqMethod.CHAT_SEND,
    ) is True
    assert MessageHandler._uses_legacy_chat_error_protocol(
        ReqMethod.CHAT_CANCEL,
        {"intent": "resume"},
    ) is True
    assert MessageHandler._uses_legacy_chat_error_protocol(
        ReqMethod.CHAT_CANCEL,
    ) is False
