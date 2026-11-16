"""The standalone Gateway serves a public, read-only enterprise config view."""

from unittest.mock import AsyncMock

import pytest

from agent_gateway.common.config_panel import config_set_handlers


async def _read_panel(monkeypatch, config):
    monkeypatch.setattr(config_set_handlers, "get_config_raw", lambda: config)
    channel = AsyncMock()
    outbound = object()
    await config_set_handlers.config_get_handler(channel, outbound, "config-id")
    channel.send_response.assert_awaited_once()
    args = channel.send_response.await_args
    assert args.args == (outbound, "config-id")
    assert args.kwargs["ok"] is True
    return args.kwargs["payload"]


@pytest.mark.asyncio
async def test_enterprise_config_disables_setup_guide_and_keeps_web_flags(monkeypatch):
    monkeypatch.setenv("API_KEY", "fixture-model-key")
    payload = await _read_panel(monkeypatch, {
        "setup_guide": {"enabled": True},
        "a2ui": {"enabled": True},
        "recommendation": {"prompt_enabled": False},
        "modes": {"team": {"jiuwen_team": {"dispatch_mode": " SCHEDULED "}}},
        "experimental": {
            "comfyui_workflow_enabled": "false",
            "multi_model_collaboration_enabled": True,
            "multi_model_collaboration": {
                "jev_enabled": True,
                "verifier_interval": 7,
                "jev_api_key": "fixture-moa-key",
            },
        },
    })
    assert payload["setup_guide_enabled"] == "false"
    assert payload["a2ui_enabled"] == "true"
    assert payload["prompt_suggestion_enabled"] == "false"
    assert payload["comfyui_workflow_enabled"] == "false"
    assert payload["team_scheduled_dispatch_enabled"] == "true"
    assert payload["multi_model_collaboration_enabled"] == "true"
    assert payload["moa_jev_enabled"] == "true"
    assert payload["moa_verifier_interval"] == "7"
    assert "api_key" not in payload
    assert "fixture-model-key" not in str(payload)
    assert "fixture-moa-key" not in str(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("config", [None, [], {}])
async def test_enterprise_config_defaults_for_empty_or_non_mapping_config(monkeypatch, config):
    payload = await _read_panel(monkeypatch, config)
    assert payload["setup_guide_enabled"] == "false"
    assert payload["team_scheduled_dispatch_enabled"] == "false"
    assert payload["multi_model_collaboration_enabled"] == "false"
    assert payload["moa_verifier_interval"] == "3"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [{"api_key": "fixture-moa-key"}, "fixture-moa-key", 0])
async def test_enterprise_config_ignores_invalid_moa_interval(monkeypatch, value):
    payload = await _read_panel(monkeypatch, {
        "experimental": {"multi_model_collaboration": {"verifier_interval": value}},
    })
    assert payload["moa_verifier_interval"] == "3"
