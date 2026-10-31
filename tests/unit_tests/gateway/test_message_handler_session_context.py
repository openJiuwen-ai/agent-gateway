# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from agent_gateway.message_handler.session_context import (
    attach_original_request_to_ask_user_answer,
    get_last_originator,
    get_last_user_query,
    remember_user_query_context,
)
from openjiuwen_protocol.message import Message, ReqMethod


def _chat_message(**params):
    return Message(
        id="req-1",
        type="req",
        channel_id="web",
        session_id="sess-1",
        params=params,
        timestamp=1.0,
        ok=True,
        req_method=ReqMethod.CHAT_SEND,
        is_stream=True,
        user_id="user-1",
    )


def test_remember_user_query_context_updates_query_and_originator():
    user_queries = {}
    originators = {}
    msg = _chat_message(query=" continue the task ")
    msg.metadata = {"member_name": " member-1 "}

    remember_user_query_context(
        msg,
        user_queries=user_queries,
        originators=originators,
    )

    assert user_queries == {"sess-1": "continue the task"}
    assert originators == {"sess-1": ("web", "member-1")}


def test_remember_user_query_context_ignores_supplement_and_empty_query():
    user_queries = {}
    originators = {}
    supplement = _chat_message(
        query="follow-up",
        is_supplement=True,
    )

    remember_user_query_context(
        supplement,
        user_queries=user_queries,
        originators=originators,
    )

    assert user_queries == {}
    assert originators == {}

    blank = _chat_message(query="   ")
    remember_user_query_context(
        blank,
        user_queries=user_queries,
        originators=originators,
    )

    assert user_queries == {}
    assert originators == {}


def test_remember_user_query_context_clears_originator_without_member_name():
    originators = {"sess-1": ("feishu", "old-member")}
    msg = _chat_message(query="continue")
    msg.metadata = None

    remember_user_query_context(
        msg,
        user_queries={},
        originators=originators,
    )

    assert originators == {}


def test_get_last_user_query_and_originator_handle_missing_sessions():
    user_queries = {"sess-1": "continue"}
    originators = {"sess-1": ("web", "member-1")}

    assert get_last_user_query(user_queries, "sess-1") == "continue"
    assert get_last_user_query(user_queries, None) == ""
    assert get_last_user_query(user_queries, "missing") == ""
    assert get_last_originator(originators, "sess-1") == ("web", "member-1")
    assert get_last_originator(originators, None) is None
    assert get_last_originator(originators, "missing") is None


def test_attach_original_request_to_ask_user_answer():
    user_queries = {"sess-1": "finish the PR"}
    msg = _chat_message(source="ask_user_interrupt", answers=[])

    attached = attach_original_request_to_ask_user_answer(
        msg,
        user_queries=user_queries,
    )

    assert attached.params["original_request"] == "finish the PR"
    assert msg.params.get("original_request") is None


def test_attach_original_request_preserves_existing_answer():
    user_queries = {"sess-1": "finish the PR"}
    msg = _chat_message(
        source="ask_user_interrupt",
        original_request="already provided",
    )

    attached = attach_original_request_to_ask_user_answer(
        msg,
        user_queries=user_queries,
    )

    assert attached.params["original_request"] == "already provided"
