# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace
from unittest.mock import AsyncMock

from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.message_handler.stream_state import build_global_running_event
from openjiuwen_protocol.message import ReqMethod


def _make_handler():
    handler = object.__new__(MessageHandler)
    handler._stream_modes = {}
    handler._stream_emits_processing_status = {}
    handler._active_chat_tasks = {}
    return handler


def test_is_chat_stream_requires_processing_status_flag():
    handler = _make_handler()
    handler._stream_emits_processing_status["rid-1"] = True
    handler._stream_emits_processing_status["rid-2"] = False

    assert MessageHandler._is_chat_stream(handler, "rid-1") is True
    assert MessageHandler._is_chat_stream(handler, "rid-2") is False
    assert MessageHandler._is_chat_stream(handler, "missing") is False


def test_iter_active_stream_modes_preserves_order():
    handler = _make_handler()
    handler._stream_modes = {
        "r1": "agent.code.normal",
        "r2": "team.plan.normal",
    }
    handler._stream_emits_processing_status = {"r1": True, "r2": True}
    handler._active_chat_tasks = {"r3": "agent.work.plan"}

    assert MessageHandler._iter_active_stream_modes(handler) == [
        "agent.code.normal",
        "team.plan.normal",
        "agent.work.plan",
    ]


def test_has_active_streams_ignores_team_modes():
    handler = _make_handler()
    handler._stream_modes = {"r1": "team.plan.normal"}
    handler._stream_emits_processing_status = {"r1": True}
    handler._active_chat_tasks = {"r2": "team.code.plan"}

    assert MessageHandler.has_active_streams(handler) is False
    handler._active_chat_tasks["r3"] = "agent.work.plan"
    assert MessageHandler.has_active_streams(handler) is True


def test_active_chat_request_ids_and_non_team_modes_match_running_chats():
    handler = _make_handler()
    handler._stream_modes = {
        "r1": "agent.code.normal",
        "r2": "team.plan.normal",
        "r3": "agent.work.plan",
    }
    handler._stream_emits_processing_status = {
        "r1": True,
        "r2": True,
        "r3": False,
    }
    handler._active_chat_tasks = {"r4": "team.work.normal"}

    assert MessageHandler.active_chat_request_ids(handler) == frozenset({"r1", "r2", "r4"})
    assert MessageHandler.active_non_team_modes(handler) == [
        ("r1", "agent.code.normal")
    ]


def test_build_global_running_event_counts_only_non_team_tasks():
    event = build_global_running_event(
        stream_modes={"r1": "agent.code.normal", "r2": "team.plan.normal"},
        stream_emits_processing_status={"r1": True, "r2": True},
        active_chat_tasks={"r3": "team.work.normal"},
    )

    assert event == {
        "event_type": "task.global_running",
        "running": True,
        "count": 1,
    }


async def test_broadcast_task_global_running_uses_event_payload():
    handler = _make_handler()
    web_channel = SimpleNamespace(broadcast_event=AsyncMock())
    handler._channel_manager = SimpleNamespace(
        get_channel=lambda channel_id: web_channel,
    )
    handler._stream_modes = {"r1": "agent.code.normal"}
    handler._stream_emits_processing_status = {"r1": True}
    handler._active_chat_tasks = {"r2": "agent.work.plan"}

    await handler._broadcast_task_global_running()

    web_channel.broadcast_event.assert_awaited_once_with(
        "task.global_running",
        {
            "event_type": "task.global_running",
            "running": True,
            "count": 2,
        },
    )


async def test_broadcast_task_global_running_without_web_channel_is_quiet():
    handler = _make_handler()
    handler._channel_manager = SimpleNamespace(get_channel=lambda channel_id: None)

    await handler._broadcast_task_global_running()


def _session_stream_state_handler():
    handler = object.__new__(MessageHandler)
    handler._non_stream_chat_messages = {}
    handler._stream_sessions = {}
    handler._stream_team_ids = {}
    handler._stream_emits_processing_status = {}
    handler._stream_methods = {}
    return handler


def test_non_stream_chat_blocks_processing_false_for_same_session():
    handler = _session_stream_state_handler()
    handler._non_stream_chat_messages["req-1"] = SimpleNamespace(
        session_id="sess-1", params={}
    )

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is True
    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-2"
    ) is False


def test_non_stream_team_chat_does_not_block_processing_false():
    handler = _session_stream_state_handler()
    handler._non_stream_chat_messages["req-1"] = SimpleNamespace(
        session_id="sess-1",
        params={"mode": "team.plan.normal", "target_team_id": "team-1"},
    )

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is False


def test_stream_with_processing_status_blocks_processing_false():
    handler = _session_stream_state_handler()
    rid = "req-1"
    handler._stream_sessions[rid] = "sess-1"
    handler._stream_team_ids[rid] = ""
    handler._stream_emits_processing_status[rid] = True
    handler._stream_methods[rid] = ReqMethod.CHAT_SEND.value

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is True


def test_command_goal_stream_blocks_processing_false():
    handler = _session_stream_state_handler()
    rid = "req-1"
    handler._stream_sessions[rid] = "sess-1"
    handler._stream_team_ids[rid] = ""
    handler._stream_emits_processing_status[rid] = False
    handler._stream_methods[rid] = ReqMethod.COMMAND_GOAL.value

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is True


def test_low_status_read_stream_does_not_block_processing_false():
    handler = _session_stream_state_handler()
    rid = "req-1"
    handler._stream_sessions[rid] = "sess-1"
    handler._stream_team_ids[rid] = ""
    handler._stream_emits_processing_status[rid] = False
    handler._stream_methods[rid] = "history.get"

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is False


def test_team_stream_does_not_block_processing_false():
    handler = _session_stream_state_handler()
    rid = "req-1"
    handler._stream_sessions[rid] = "sess-1"
    handler._stream_team_ids[rid] = "team-1"
    handler._stream_emits_processing_status[rid] = True
    handler._stream_methods[rid] = ReqMethod.CHAT_SEND.value

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1"
    ) is False


def test_exclude_rid_skips_matching_stream():
    handler = _session_stream_state_handler()
    rid = "req-1"
    handler._stream_sessions[rid] = "sess-1"
    handler._stream_team_ids[rid] = ""
    handler._stream_emits_processing_status[rid] = True
    handler._stream_methods[rid] = ReqMethod.CHAT_SEND.value

    assert MessageHandler._session_has_streams_blocking_processing_false(
        handler, "sess-1", exclude_rid=rid
    ) is False
