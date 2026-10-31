# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.disconnect import (
    build_disconnect_cancel_message,
    clone_message_for_session_cancel,
    merge_disconnect_session_keys,
    resolve_stream_cancel_session_id,
    should_cancel_stream_on_channel,
)
from agent_gateway.message_handler.models import ChannelMode
from openjiuwen_protocol.message import Message, ReqMethod


def _make_message(session_id="sess-1"):
    return Message(
        id="req-1",
        type="req",
        channel_id="web",
        session_id=session_id,
        params={"query": "continue"},
        timestamp=1.0,
        ok=True,
        req_method=ReqMethod.CHAT_SEND,
        is_stream=True,
        user_id="user-1",
    )


def test_clone_message_for_session_cancel_preserves_params_and_overrides_mode():
    msg = _make_message()
    msg.params["mode"] = "agent.code.plan"

    cloned = clone_message_for_session_cancel(
        msg,
        "sess-2",
        mode="team.work.plan",
    )

    assert cloned.session_id == "sess-2"
    assert cloned.params["query"] == "continue"
    assert cloned.params["mode"] == "team.work.plan"
    assert msg.session_id == "sess-1"
    assert msg.params["mode"] == "agent.code.plan"


def test_should_cancel_stream_on_channel_uses_same_session_or_acp():
    assert should_cancel_stream_on_channel("web", "sess-1", "sess-1") is True
    assert should_cancel_stream_on_channel("web", "sess-1", "sess-2") is False
    assert should_cancel_stream_on_channel("acp", "sess-1", "sess-2") is True


def test_resolve_stream_cancel_session_id_prefers_stream_sessions():
    stream_sessions = {"r1": " sid-1 ", "r2": " sid-2 "}
    stream_channels = {"r1": "web", "r2": "web"}
    channel_states = {"web:sid-2": SimpleNamespace(), "web:sid-1": SimpleNamespace()}

    assert resolve_stream_cancel_session_id(
        "web",
        stream_sessions=stream_sessions,
        stream_channels=stream_channels,
        channel_states=channel_states,
    ) == "sid-1"


def test_resolve_stream_cancel_session_id_recovers_from_channel_states():
    stream_sessions = {}
    stream_channels = {}
    channel_states = {"web:sid-2": SimpleNamespace(), "web:sid-1": SimpleNamespace()}

    assert resolve_stream_cancel_session_id(
        "web",
        stream_sessions=stream_sessions,
        stream_channels=stream_channels,
        channel_states=channel_states,
    ) == "sid-2"


def test_merge_disconnect_session_keys_deduplicates_and_recovers_requests():
    session_keys = [
        ("web", " sess-1 "),
        ("web", ""),
        ("web", "sess-1", "agent"),
        ("acp", "acp-sid"),
    ]

    merged, recovered = merge_disconnect_session_keys(
        session_keys,
        stream_sessions={},
    )

    assert merged == [
        ("web", "sess-1"),
        ("acp", "acp-sid"),
    ]
    assert recovered == []


def test_merge_disconnect_session_keys_recovers_from_request_routes():
    stale_request_keys = [
        ("web", "r1"),
        ("acp", "r2"),
        ("acp", "r3"),
    ]
    stream_sessions = {
        "r1": " sess-1 ",
        "r2": "acp-sid",
        "r3": "acp-sid",
        "r4": "other",
    }

    merged, recovered = merge_disconnect_session_keys(
        [],
        stale_request_keys=stale_request_keys,
        stream_sessions=stream_sessions,
    )

    assert merged == [
        ("web", "sess-1"),
        ("acp", "acp-sid"),
    ]
    assert recovered == [
        ("web", "sess-1"),
        ("acp", "acp-sid"),
    ]


def test_build_disconnect_cancel_message_prefers_explicit_mode():
    channel_states = {
        "web:sess-1": SimpleNamespace(
            mode=SimpleNamespace(value="team.plan.normal"),
        ),
        "web": SimpleNamespace(mode="agent.code.plan"),
    }

    msg = build_disconnect_cancel_message(
        "web",
        "sess-1",
        user_id="user-1",
        mode=" agent.work.plan ",
        channel_states=channel_states,
    )

    assert msg.id.startswith("ws_drop_")
    assert msg.type == "req"
    assert msg.channel_id == "web"
    assert msg.session_id == "sess-1"
    assert msg.ok is True
    assert msg.req_method == ReqMethod.CHAT_CANCEL
    assert msg.is_stream is False
    assert msg.user_id == "user-1"
    assert msg.params == {
        "intent": "cancel",
        "session_id": "sess-1",
        "mode": "agent.work.plan",
    }


def test_build_disconnect_cancel_message_falls_back_to_channel_state():
    channel_states = {
        "web:sess-1": SimpleNamespace(mode=ChannelMode.AGENT_CODE_PLAN),
    }

    msg = build_disconnect_cancel_message(
        "web",
        "sess-1",
        channel_states=channel_states,
    )

    assert msg.params["mode"] == "agent.code.plan"
    assert msg.user_id is None
