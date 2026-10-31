# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio
from types import SimpleNamespace

from openjiuwen_protocol.message import Message, ReqMethod

from agent_gateway.message_handler.message_handler import MessageHandler


def _make_message(**overrides):
    values = {
        "id": "msg-1",
        "type": "req",
        "channel_id": "acp",
        "session_id": None,
        "params": {},
        "timestamp": 0.0,
        "ok": True,
    }
    values.update(overrides)
    return Message(**values)


def _make_acp_handler():
    handler = object.__new__(MessageHandler)
    handler._acp_session_aliases = {}
    handler._acp_session_alias_lock = asyncio.Lock()
    return handler


def test_resolve_acp_internal_session_id_creates_session_once():
    handler = _make_acp_handler()
    creation_calls = []

    async def send_request(_env):
        creation_calls.append(1)
        return SimpleNamespace(ok=True, payload={"sessionId": "acp-sid"})

    handler._send_non_stream_agent_request = send_request

    async def exercise():
        first = await MessageHandler._resolve_acp_internal_session_id(
            handler, "external-1"
        )
        second = await MessageHandler._resolve_acp_internal_session_id(
            handler, "external-1"
        )
        return first, second

    first, second = asyncio.run(exercise())

    assert first == ("acp-sid", True)
    assert second == ("acp-sid", True)
    assert creation_calls == [1]
    assert handler._acp_session_aliases == {"external-1": "acp-sid"}


def test_prepare_agent_dispatch_message_maps_acp_session():
    handler = _make_acp_handler()
    handler._attach_original_request_to_ask_user_answer = lambda msg: msg

    async def resolve_session_id(session_id):
        return "acp-sid", session_id != "acp-sid"

    handler._resolve_acp_internal_session_id = resolve_session_id
    msg = _make_message(
        session_id="external-1",
        req_method=ReqMethod.CHAT_SEND,
        metadata={},
    )

    result = asyncio.run(
        MessageHandler._prepare_agent_dispatch_message(handler, msg)
    )

    assert result.session_id == "acp-sid"
    assert result.params == {"session_id": "acp-sid"}
    assert result.metadata == {"acp_original_session_id": "external-1"}


def test_resolve_acp_external_session_id_prefers_metadata():
    handler = _make_acp_handler()
    handler._acp_session_aliases = {"external-1": "acp-sid"}

    assert MessageHandler._resolve_acp_external_session_id(
        handler, None, None
    ) is None
    assert MessageHandler._resolve_acp_external_session_id(
        handler, "acp-sid", {"acp_original_session_id": "external-1"}
    ) == "external-1"
    assert MessageHandler._resolve_acp_external_session_id(
        handler, "acp-sid", None
    ) == "external-1"
    assert MessageHandler._resolve_acp_external_session_id(
        handler, "unknown", None
    ) == "unknown"
