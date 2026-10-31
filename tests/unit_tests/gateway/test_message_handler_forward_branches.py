# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio

import pytest

from types import SimpleNamespace

from openjiuwen_protocol.message import Message, ReqMethod


def _make_msg(req_method=ReqMethod.CHAT_SEND, params=None, is_stream=True):
    return Message(
        id="req-1",
        type="req",
        channel_id="web",
        session_id="sess-1",
        params=params if params is not None else {},
        timestamp=1.0,
        ok=True,
        req_method=req_method,
        is_stream=is_stream,
        user_id="user-1",
    )


@pytest.mark.asyncio
async def test_handle_swarmflow_reply_dispatches_non_stream():
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    calls = []

    async def prepare_agent_dispatch_message(m):
        calls.append(("prepare", m))
        return m

    def message_to_e2a(m):
        calls.append(("to_e2a", m))
        return SimpleNamespace(is_stream=False, request_id="r1", method="chat.swarmflow_reply")

    async def process_non_stream_request(m, env):
        calls.append(("process", env))

    handler._prepare_agent_dispatch_message = prepare_agent_dispatch_message
    handler.message_to_e2a = message_to_e2a
    handler._process_non_stream_request = process_non_stream_request

    msg = _make_msg(req_method=ReqMethod.CHAT_SWARMFLOW_REPLY)
    await handler._handle_swarmflow_reply(msg)

    assert len(calls) == 3
    assert calls[0][0] == "prepare"
    assert calls[1][0] == "to_e2a"
    assert calls[2][0] == "process"


@pytest.mark.asyncio
async def test_enrich_chat_send_content_skips_on_review_command():
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.message_handler.review_prompt_injection import InboundReviewPromptResult

    handler = object.__new__(MessageHandler)
    msg = _make_msg(params={"query": "/review fix the bug"})

    async def apply_inbound_review_prompt(handler, msg, parsed):
        return InboundReviewPromptResult(skip=True, prompt=None)

    import agent_gateway.message_handler.message_handler as mh
    original = mh.parse_channel_control_text
    original_apply = mh.apply_inbound_review_prompt
    mh.parse_channel_control_text = lambda text: SimpleNamespace(text="review fix the bug")
    mh.apply_inbound_review_prompt = apply_inbound_review_prompt
    try:
        result = await handler._enrich_chat_send_content(msg, is_session_input=False)
    finally:
        mh.parse_channel_control_text = original
        mh.apply_inbound_review_prompt = original_apply

    assert result is True


@pytest.mark.asyncio
async def test_enrich_chat_send_content_returns_false_for_non_string_content():
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    msg = _make_msg(params={"query": 12345})

    result = await handler._enrich_chat_send_content(msg, is_session_input=False)

    assert result is False
