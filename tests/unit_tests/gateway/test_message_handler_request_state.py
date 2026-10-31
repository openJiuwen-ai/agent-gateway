# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler


def _message(*, req_method="chat.send", params=None, enable_streaming=True):
    return SimpleNamespace(
        req_method=req_method,
        params=params,
        enable_streaming=enable_streaming,
    )


def test_is_chat_send_message_accepts_enum_and_wire_value():
    enum_like = SimpleNamespace(value="chat.send")
    assert MessageHandler._is_chat_send_message(_message(req_method=enum_like)) is True
    assert MessageHandler._is_chat_send_message(_message(req_method="chat.send")) is True
    assert MessageHandler._is_chat_send_message(_message(req_method="history.get")) is False


def test_team_send_and_unknown_mode_detection():
    assert MessageHandler._is_team_chat_send(_message(params={"mode": "team.plan.normal"})) is True
    assert MessageHandler._is_team_chat_send(_message(params={"mode": "agent.work.normal"})) is False
    assert MessageHandler._is_team_chat_send(_message(params=None)) is False

    unknown = _message(params={"mode": "team.unknown"})
    assert MessageHandler._is_unknown_team_mode_send(unknown) is True

    ordinary = _message(params={"mode": "work"})
    assert MessageHandler._is_unknown_team_mode_send(ordinary) is False


def test_non_stream_team_send_requires_streaming():
    team = _message(params={"mode": "team.plan.normal"}, enable_streaming=False)
    assert MessageHandler._is_unsupported_non_stream_team_send(team) is True

    streaming = _message(params={"mode": "team.plan.normal"}, enable_streaming=True)
    assert MessageHandler._is_unsupported_non_stream_team_send(streaming) is False


def test_interrupt_resume_detection_covers_direct_and_evolution_answers():
    direct = _message(
        params={
            "source": "ask_user_interrupt",
            "answers": [],
            "request_id": "req-1",
        }
    )
    assert MessageHandler._is_interrupt_resume_chat_send(direct) is True

    evolution = _message(
        params={"source": "evolution_interrupt", "request_id": "call_1"}
    )
    assert MessageHandler._is_interrupt_resume_chat_send(evolution) is True


def test_cancel_existing_stream_excludes_special_inputs():
    ordinary = _message(params={})
    assert MessageHandler._should_cancel_existing_stream_before_chat_send(ordinary) is True

    proactive = _message(params={"source": "proactive_recommendation"})
    assert MessageHandler._should_cancel_existing_stream_before_chat_send(proactive) is False

    team = _message(params={"mode": "team.plan.normal"})
    assert MessageHandler._should_cancel_existing_stream_before_chat_send(team) is False


def test_interaction_managed_and_session_input_flags():
    goal = _message(params={"attach_goal": True})
    assert MessageHandler._is_interaction_managed_chat_send(goal) is True
    assert MessageHandler._is_session_input_message(goal) is False

    steer = _message(params={"input_mode": "steer"})
    assert MessageHandler._is_interaction_managed_chat_send(steer) is True
    assert MessageHandler._is_session_input_message(steer) is True
