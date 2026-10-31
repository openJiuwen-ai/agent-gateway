# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler


def test_extract_session_id_from_ref_supports_full_and_simple_refs():
    assert MessageHandler.extract_session_id_from_ref(
        "team_alpha_session_sess_1"
    ) == "sess_1"
    assert MessageHandler.extract_session_id_from_ref("sess_1") == "sess_1"
    assert MessageHandler.extract_session_id_from_ref(None) is None


def test_extract_team_name_from_ref_falls_back_to_unknown():
    assert MessageHandler.extract_team_name_from_ref(
        "team_alpha_session_sess_1"
    ) == "alpha"
    assert MessageHandler.extract_team_name_from_ref(None) == "unknown"


def test_resolve_app_id_prefers_v2_field_then_bot_id():
    msg = SimpleNamespace(app_id="app-1", bot_id="bot-1")
    assert MessageHandler.resolve_app_id(msg) == "app-1"

    legacy = SimpleNamespace(app_id=None, bot_id="bot-1")
    assert MessageHandler.resolve_app_id(legacy) == "bot-1"
    assert MessageHandler.resolve_app_id(SimpleNamespace()) == "default"


def test_session_map_style_session_id_requires_all_parts():
    assert MessageHandler._is_session_map_style_session_id(
        "a::b::c::d::e"
    ) is True
    assert MessageHandler._is_session_map_style_session_id(
        "a::b::c::d::e::f"
    ) is True
    assert MessageHandler._is_session_map_style_session_id(
        "a::b::c::d::"
    ) is False
    assert MessageHandler._is_session_map_style_session_id("") is False


def test_known_jiuwenswarm_session_id_accepts_prefixes_and_map_shape():
    assert MessageHandler._is_known_jiuwenswarm_session_id(
        "feishu_chat_1"
    ) is True
    assert MessageHandler._is_known_jiuwenswarm_session_id(
        "a::b::c::d::e"
    ) is True
    assert MessageHandler._is_known_jiuwenswarm_session_id(None) is False
    assert MessageHandler._is_known_jiuwenswarm_session_id("") is False
    assert MessageHandler._is_known_jiuwenswarm_session_id("other") is False


def test_single_user_channel_is_restricted_to_acp():
    assert MessageHandler._is_single_user_channel("acp") is True
    assert MessageHandler._is_single_user_channel("web") is False
