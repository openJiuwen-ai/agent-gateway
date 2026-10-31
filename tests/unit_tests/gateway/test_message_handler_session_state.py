# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio
from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import (
    ChannelMode,
    MessageHandler,
)


def _make_handler(**attrs):
    handler = object.__new__(MessageHandler)
    defaults = {
        "_channel_states": {},
        "_pending_alloc_state_key": {},
        "_external_session_aliases": {},
        "_external_session_alias_lock": asyncio.Lock(),
        "_sticky_channel_state_types": {"xiaoyi"},
        "_control_channel_types": {"feishu", "xiaoyi"},
        "_session_map_channel_types": {"feishu"},
        "_session_map": SimpleNamespace(find_session_id=lambda *parts: ""),
    }
    for key, value in {**defaults, **attrs}.items():
        setattr(handler, key, value)
    return handler


def test_channel_state_key_includes_conversation_id():
    assert MessageHandler._get_channel_state_key(
        None, "feishu", "chat-1"
    ) == "feishu:chat-1"
    assert MessageHandler._get_channel_state_key(None, "feishu", None) == "feishu"


def test_channel_default_state_reads_config_and_normalizes_mode():
    handler = _make_handler(
        _get_config_raw=lambda: {
            "channels": {
                "feishu": {
                    "default_session_id": " sid ",
                    "default_mode": "team.plan",
                }
            }
        }
    )

    state = MessageHandler._get_channel_default_state(handler, "feishu")

    assert state.session_id == "sid"
    assert state.mode is ChannelMode.TEAM_WORK_PLAN


def test_get_or_create_channel_state_reuses_session_map_and_cache():
    handler = _make_handler(
        _get_config_raw=lambda: {"channels": {"feishu": {"default_mode": "agent.code.plan"}}},
        _extract_identity_tuple=lambda msg: ("feishu", "chat-1", "bot-1", "user-1"),
    )
    handler._session_map.find_session_id = lambda *parts: "mapped-sid"
    msg = SimpleNamespace(
        channel_id="feishu",
        session_id="conversation-1",
        provider="feishu",
        chat_id="chat-1",
        bot_id="bot-1",
        user_id="user-1",
    )

    state = MessageHandler.get_or_create_channel_state(handler, msg)
    again = MessageHandler.get_or_create_channel_state(handler, msg)

    assert state is again
    assert state.session_id == "mapped-sid"
    assert state.mode is ChannelMode.AGENT_CODE_PLAN


def test_save_channel_state_writes_channel_level_state():
    handler = _make_handler()
    handler._channel_states["feishu"] = SimpleNamespace(
        session_id="sid-1", mode=ChannelMode.AGENT_CODE_NORMAL
    )
    handler._update_channel_in_config = lambda cid, payload: captured.update(
        {"channel_id": cid, **payload}
    )
    captured = {}

    MessageHandler._save_channel_state_to_config(handler, "feishu")

    assert captured == {
        "channel_id": "feishu",
        "default_session_id": "sid-1",
        "default_mode": "agent.code.normal",
    }


def test_remember_alloc_state_key_only_tracks_sticky_channels():
    handler = _make_handler()
    xiaoyi = SimpleNamespace(id="msg-1", session_id="conversation-1")
    web = SimpleNamespace(id="msg-2", session_id=None)

    MessageHandler._remember_alloc_state_key(handler, xiaoyi, "xiaoyi")
    MessageHandler._remember_alloc_state_key(handler, web, "web")

    assert handler._pending_alloc_state_key == {"msg-1": "xiaoyi:conversation-1"}


def test_ensure_inbound_session_reuses_pending_state_key():
    handler = _make_handler(_resolve_control_channel_type=lambda msg: "xiaoyi")
    state = SimpleNamespace(session_id=None, mode=ChannelMode.AGENT)
    handler._channel_states["xiaoyi:conversation-1"] = state
    handler._pending_alloc_state_key["msg-1"] = "xiaoyi:conversation-1"
    allocation_calls = []

    async def _allocate(msg, control_state):
        allocation_calls.append((msg.id, control_state is state))
        return "allocated-sid"

    handler._allocate_channel_session = _allocate
    msg = SimpleNamespace(id="msg-1", channel_id="xiaoyi", session_id=None)

    asyncio.run(MessageHandler._ensure_inbound_session(handler, msg))

    assert msg.session_id == "allocated-sid"
    assert handler._pending_alloc_state_key == {}
    assert allocation_calls == [("msg-1", True)]
    assert handler._channel_states["xiaoyi"] is state


def test_resolve_external_channel_session_reuses_alias():
    handler = _make_handler()
    allocation_calls = []

    async def _allocate(msg, state):
        allocation_calls.append(state.mode)
        return "real-sid"

    handler._allocate_channel_session = _allocate
    def make_message():
        return SimpleNamespace(
            channel_id="a2a",
            session_id="external-1",
            params={"mode": "agent.code.plan"},
            metadata={},
        )

    first_msg = make_message()
    second_msg = make_message()

    async def exercise():
        await MessageHandler._resolve_external_channel_session(handler, first_msg)
        await MessageHandler._resolve_external_channel_session(handler, second_msg)

    asyncio.run(exercise())

    assert first_msg.session_id == "real-sid"
    assert second_msg.session_id == "real-sid"
    assert second_msg.metadata["external_session_id"] == "external-1"
    assert allocation_calls == [ChannelMode.AGENT_CODE_PLAN]
    assert handler._external_session_aliases == {("a2a", "external-1"): "real-sid"}
