# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

import agent_gateway.auth.passthrough as auth_passthrough
from agent_gateway.message_normalization import (
    _auth_session_id,
    build_event_frame,
    normalize_gateway_message,
)
from openjiuwen_protocol.message import EventType, ReqMethod


def test_build_event_frame_defaults_dict_payload_session_id():
    msg = SimpleNamespace(
        event_type=EventType.CHAT_FINAL,
        payload={"text": "done"},
        session_id="sess-1",
    )

    assert build_event_frame(msg) == {
        "type": "event",
        "event": "chat.final",
        "payload": {"text": "done", "session_id": "sess-1"},
    }


def test_build_event_frame_wraps_non_dict_payload():
    msg = SimpleNamespace(
        event_type=None,
        payload=None,
        session_id="sess-2",
    )

    assert build_event_frame(msg) == {
        "type": "event",
        "event": "chat.final",
        "payload": {"session_id": "sess-2", "content": ""},
    }


def test_auth_session_id_reads_login_session_metadata():
    msg = SimpleNamespace(metadata={"auth_session": " login-session "})

    assert _auth_session_id(msg) == "login-session"


def test_normalize_gateway_message_maps_content_and_stream_method(
    monkeypatch,
):
    seen = []

    def normalize_model_auth(params, auth_session_id):
        seen.append((params, auth_session_id))

    monkeypatch.setattr(
        auth_passthrough,
        "normalize_model_auth",
        normalize_model_auth,
    )
    msg = SimpleNamespace(
        id="req-1",
        type="req",
        channel_id="web",
        session_id="sess-1",
        params={"content": "hello"},
        timestamp=1.0,
        ok=True,
        req_method=ReqMethod.CHAT_SEND,
        mode="agent",
        is_stream=False,
        stream_seq=None,
        stream_id=None,
        metadata={"auth_session": "login-1"},
        user_id="user-1",
    )

    normalized = normalize_gateway_message(msg)

    assert normalized.params["query"] == "hello"
    assert normalized.is_stream is True
    assert seen == [(normalized.params, "login-1")]


def test_normalize_gateway_message_maps_resume_to_cancel(monkeypatch):
    monkeypatch.setattr(
        auth_passthrough,
        "normalize_model_auth",
        lambda params, auth_session_id: None,
    )
    msg = SimpleNamespace(
        id="req-2",
        type="req",
        channel_id="web",
        session_id="sess-2",
        params={},
        timestamp=2.0,
        ok=True,
        req_method=ReqMethod.CHAT_RESUME,
        mode="agent",
        is_stream=False,
        stream_seq=None,
        stream_id=None,
        metadata=None,
        user_id=None,
    )

    normalized = normalize_gateway_message(msg)

    assert normalized.req_method == ReqMethod.CHAT_CANCEL
    assert normalized.params["intent"] == "resume"
    assert normalized.is_stream is False
