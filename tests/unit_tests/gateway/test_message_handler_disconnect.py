# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.disconnect import (
    build_disconnect_cancel_message,
    collect_stream_tasks_to_cancel,
    clone_message_for_session_cancel,
    merge_disconnect_session_keys,
    resolve_cancel_params,
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


def test_collect_stream_tasks_to_cancel_filters_by_session_and_channel():
    active_task = SimpleNamespace(done=lambda: False)
    done_task = SimpleNamespace(done=lambda: True)
    other_task = SimpleNamespace(done=lambda: False)

    stream_tasks = {"r1": active_task, "r2": done_task, "r3": other_task}
    stream_sessions = {"r1": "sid-1", "r2": "sid-1", "r3": "sid-2"}
    stream_channels = {"r1": "web", "r2": "web", "r3": "web"}
    non_stream_chat_messages: dict[str, Any] = {}

    tasks, rids, has_unary = collect_stream_tasks_to_cancel(
        stream_tasks=stream_tasks,
        stream_sessions=stream_sessions,
        stream_channels=stream_channels,
        non_stream_chat_messages=non_stream_chat_messages,
        old_sid="sid-1",
        channel_id="web",
    )

    assert rids == ["r1"]
    assert tasks == [active_task]
    assert has_unary is False


def test_collect_stream_tasks_to_cancel_detects_unary_messages():
    active_msg = SimpleNamespace(session_id="sid-1", channel_id="web")
    non_stream_chat_messages = {"r1": active_msg}

    _, _, has_unary = collect_stream_tasks_to_cancel(
        stream_tasks={},
        stream_sessions={},
        stream_channels={},
        non_stream_chat_messages=non_stream_chat_messages,
        old_sid="sid-1",
        channel_id=None,
    )

    assert has_unary is True


def test_resolve_cancel_params_prefers_explicit_mode_and_merges_params():
    channel_states = {
        "web:sess-1": SimpleNamespace(mode="fallback.mode"),
    }

    params = resolve_cancel_params(
        {"mode": "explicit.mode", "team": "t1", "trusted_dirs": ["/a"], "work_mode": "code", "project_dir": "/p"},
        "sess-1",
        "web",
        channel_states=channel_states,
    )

    assert params["mode"] == "explicit.mode"
    assert params["team"] == "t1"
    assert params["trusted_dirs"] == ["/a"]
    assert params["work_mode"] == "code"
    assert params["project_dir"] == "/p"


def test_resolve_cancel_params_falls_back_to_channel_state():
    channel_states = {
        "web:sess-1": SimpleNamespace(mode="fallback.mode"),
    }

    params = resolve_cancel_params(
        None,
        "sess-1",
        "web",
        channel_states=channel_states,
    )

    assert params["mode"] == "fallback.mode"
