# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Session command handler characterization tests."""

import asyncio
from types import SimpleNamespace

from agent_gateway.message_handler.models import (
    ChannelControlState,
    NewSessionCancelParams,
)
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.message_handler.session_commands import (
    handle_new_session_command,
    handle_persist_command,
)


def _make_handler():
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []
    handler.cancel_calls = []
    handler.hook_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    async def on_session_start(sid, *, source):
        handler.hook_calls.append((sid, source))

    async def cancel_and_notice(params, msg):
        handler.cancel_calls.append((params, msg))

    handler.send_channel_notice = send_notice
    handler._new_session_cancel_and_notice = cancel_and_notice
    handler._gateway_hook_handler = SimpleNamespace(
        on_session_start=on_session_start
    )
    return handler


def test_handle_new_session_command_allocates_and_schedules_notices():
    handler = _make_handler()
    state = ChannelControlState("old-sid", )
    msg = SimpleNamespace(
        id="msg-1", channel_id="feishu", session_id="old-sid"
    )
    allocation_calls = []

    async def allocate(msg, state):
        allocation_calls.append((state.session_id,))
        state.session_id = "new-sid"
        return state.session_id

    handler._allocate_channel_session = allocate

    result = asyncio.run(handle_new_session_command(
        handler, msg, state, {}, "feishu", "feishu"
    ))

    assert result is True
    assert state.session_id == "new-sid"
    assert allocation_calls == [(None,)]
    assert handler.hook_calls == [("new-sid", "feishu")]
    assert len(handler.cancel_calls) == 1
    params = handler.cancel_calls[0][0]
    assert params.new_sid == "new-sid"
    assert params.old_sid == "old-sid"


def test_handle_new_session_command_restores_sid_on_failure():
    handler = _make_handler()
    state = ChannelControlState("old-sid", )
    msg = SimpleNamespace(id="msg-1", channel_id="feishu", session_id="old-sid")

    async def allocate(msg, state):
        raise RuntimeError("allocation failed")

    handler._allocate_channel_session = allocate

    result = asyncio.run(handle_new_session_command(
        handler, msg, state, {}, "feishu", "feishu"
    ))

    assert result is True
    assert state.session_id == "old-sid"
    assert handler.notice_calls[-1][3] == {"error": "创建新会话失败，请稍后重试"}


def test_handle_persist_command_allocates_and_rewrites_task():
    handler = _make_handler()
    state = ChannelControlState("old-sid", )
    parsed = SimpleNamespace(persist_task="new task")
    msg = SimpleNamespace(
        id="msg-1",
        channel_id="feishu",
        session_id="old-sid",
        params={"content": "old task", "attachments": []},
        metadata={},
    )

    async def allocate(msg, state, *, persist_session):
        assert persist_session is True
        state.session_id = "persist-sid"
        return state.session_id

    handler._allocate_channel_session = allocate

    result = asyncio.run(handle_persist_command(
        handler, msg, parsed, state, {}, "feishu", "feishu"
    ))

    assert result is False
    assert msg.session_id == "persist-sid"
    assert state.session_id == "persist-sid"
    assert msg.params == {
        "content": "new task",
        "attachments": [],
        "query": "new task",
    }
    assert msg.metadata == {"persist_session_first_task": True}
    assert handler.hook_calls == [("persist-sid", "feishu")]
    assert len(handler.cancel_calls) == 1
    params = handler.cancel_calls[0][0]
    assert params.reply_session_id == "persist-sid"
    assert params.old_sid == "old-sid"


def test_handle_persist_command_restores_sid_on_failure():
    handler = _make_handler()
    state = ChannelControlState("old-sid", )
    parsed = SimpleNamespace(persist_task="new task")
    msg = SimpleNamespace(id="msg-1", channel_id="feishu", session_id="old-sid")

    async def allocate(msg, state, *, persist_session):
        raise RuntimeError("allocation failed")

    handler._allocate_channel_session = allocate

    result = asyncio.run(handle_persist_command(
        handler, msg, parsed, state, {}, "feishu", "feishu"
    ))

    assert result is True
    assert state.session_id == "old-sid"
    assert handler.notice_calls[-1][3] == {"error": "创建永续会话失败，请稍后重试"}
