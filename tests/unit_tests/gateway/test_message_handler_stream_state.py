# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler


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
