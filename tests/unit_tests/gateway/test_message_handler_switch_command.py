# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""/switch dispatch characterization tests."""

import asyncio
from types import SimpleNamespace

from agent_gateway.message_handler.models import (
    ChannelControlState,
    ChannelMode,
    ModeChangeCancelParams,
)
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.message_handler.switch_command import handle_switch_command


def _make_handler():
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []
    handler.cancel_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    async def mode_change(params, msg):
        handler.cancel_calls.append((params, msg))

    handler.send_channel_notice = send_notice
    handler._mode_change_cancel_and_notice = mode_change
    return handler


def test_handle_switch_command_rejects_unknown_target():
    handler = _make_handler()
    state = ChannelControlState("sid-1", ChannelMode.TEAM_CODE_NORMAL)
    parsed = SimpleNamespace(switch_subcommand="fast")
    msg = SimpleNamespace(channel_id="web", session_id="sid-1")

    result = asyncio.run(handle_switch_command(handler, msg, parsed, state, {}, "web"))

    assert result is True
    assert state.mode is ChannelMode.TEAM_CODE_NORMAL
    assert len(handler.notice_calls) == 1
    assert handler.cancel_calls == []


def test_handle_switch_command_maps_plan_from_code_mode():
    handler = _make_handler()
    state = ChannelControlState("sid-1", ChannelMode.AGENT_CODE_NORMAL)
    parsed = SimpleNamespace(switch_subcommand="plan")
    msg = SimpleNamespace(channel_id="web", session_id="sid-1")

    result = asyncio.run(handle_switch_command(handler, msg, parsed, state, {}, "web"))

    assert result is True
    assert state.mode is ChannelMode.AGENT_CODE_PLAN
    assert len(handler.cancel_calls) == 1
    params = handler.cancel_calls[0][0]
    assert isinstance(params, ModeChangeCancelParams)
    assert params.old_sid == "sid-1"
    assert params.new_mode_label == "agent.code.plan"


def test_handle_switch_command_maps_normal_from_code_mode():
    handler = _make_handler()
    state = ChannelControlState("sid-1", ChannelMode.AGENT_CODE_PLAN)
    parsed = SimpleNamespace(switch_subcommand="normal")
    msg = SimpleNamespace(channel_id="web", session_id="sid-1")

    result = asyncio.run(handle_switch_command(handler, msg, parsed, state, {}, "web"))

    assert result is True
    assert state.mode is ChannelMode.AGENT_CODE_NORMAL
    assert len(handler.cancel_calls) == 1


def test_handle_switch_command_maps_team_from_code_mode():
    handler = _make_handler()
    state = ChannelControlState("sid-1", ChannelMode.AGENT_CODE_NORMAL)
    parsed = SimpleNamespace(switch_subcommand="team")
    msg = SimpleNamespace(channel_id="web", session_id="sid-1")

    result = asyncio.run(handle_switch_command(handler, msg, parsed, state, {}, "web"))

    assert result is True
    assert state.mode is ChannelMode.TEAM_CODE_NORMAL
    assert len(handler.cancel_calls) == 1


def test_handle_switch_command_keeps_same_mode_notice():
    handler = _make_handler()
    state = ChannelControlState("sid-1", ChannelMode.AGENT_WORK_NORMAL)
    parsed = SimpleNamespace(switch_subcommand="plan")
    msg = SimpleNamespace(channel_id="web", session_id="sid-1")

    result = asyncio.run(handle_switch_command(handler, msg, parsed, state, {}, "web"))

    assert result is True
    assert state.mode is ChannelMode.AGENT_WORK_NORMAL
    assert handler.notice_calls == [
        ({}, "web", "sid-1", "[收到 CLI 指令], mode 已变更为 agent.work.normal")
    ]
