# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from agent_gateway.message_handler.message_handler import (
    ChannelControlState,
    ChannelMode,
    ModeChangeCancelParams,
    NewSessionCancelParams,
    apply_a2ui_text_fallback_to_gateway_payload,
    channel_mode_from_str,
    normalize_legacy_health_check_relay_payload,
)


def test_channel_mode_recognizes_team_modes():
    assert ChannelMode.is_team_mode("team.plan.normal")
    assert ChannelMode.is_team_mode("team.code.plan")
    assert not ChannelMode.is_team_mode("agent.work.normal")


def test_channel_mode_normalizes_legacy_and_unknown_modes():
    assert channel_mode_from_str("agent.plan") is ChannelMode.AGENT_WORK_PLAN
    assert channel_mode_from_str("team.plan") is ChannelMode.TEAM_WORK_PLAN
    assert channel_mode_from_str("not-a-mode") is ChannelMode.AGENT


def test_channel_control_state_defaults():
    state = ChannelControlState()
    assert state.session_id is None
    assert state.mode is ChannelMode.AGENT


def test_cancel_parameter_models_keep_required_fields():
    new_session = NewSessionCancelParams(
        user_infos={},
        channel_id="web",
        reply_session_id=None,
        new_sid="new",
        old_sid="old",
    )
    mode_change = ModeChangeCancelParams(
        user_infos={},
        channel_id="web",
        reply_session_id=None,
        old_sid="old",
        new_mode_label="code",
    )
    assert new_session.new_sid == "new"
    assert mode_change.new_mode_label == "code"


def test_a2ui_payload_without_marker_is_unchanged():
    payload = {"text": "hello"}
    assert apply_a2ui_text_fallback_to_gateway_payload(payload, channel_id="web") is payload


def test_legacy_health_check_relay_payload_is_normalized():
    payload = {"event_type": "heartbeat.relay", "heartbeat": {"ok": True}}
    normalized = normalize_legacy_health_check_relay_payload(payload)
    assert normalized["event_type"] == "health_check.relay"
    assert normalized["health_check"] == {"ok": True}
