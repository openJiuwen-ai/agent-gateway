# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler


def test_response_to_message_normalizes_agent_error_event():
    resp = SimpleNamespace(
        request_id="resp-1",
        channel_id="feishu",
        ok=False,
        payload={"error": "boom"},
        metadata=None,
        agent_ref="agent-1",
    )

    out = MessageHandler._response_to_message(
        resp,
        "sess-1",
        request_metadata={"mode": "agent"},
        app_id="app-1",
        request_method="chat.send",
        request_params={},
    )

    assert out.type == "event"
    assert out.ok is False
    assert out.payload["event_type"] == "chat.error"
    assert out.payload["is_complete"] is True
    assert out.metadata["mode"] == "agent"


def test_chunk_to_message_merges_request_and_chunk_metadata():
    chunk = SimpleNamespace(
        request_id="chunk-1",
        channel_id="feishu",
        payload={"text": "part"},
        is_complete=False,
        metadata={"fan_out_targets": ["web"]},
        agent_ref="agent-1",
    )

    out = MessageHandler._chunk_to_message(
        chunk,
        "sess-1",
        metadata={"app_id": "app-1"},
        app_id="app-1",
    )

    assert out.id == "chunk-1"
    assert out.type == "event"
    assert out.ok is True
    assert out.payload["text"] == "part"
    assert out.metadata["fan_out_targets"] == ["web"]
    assert out.metadata["app_id"] == "app-1"
    assert out.agent_ref == "agent-1"
