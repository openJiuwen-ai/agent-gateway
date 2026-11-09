"""The standalone Gateway serves a public, read-only enterprise config view."""

from unittest.mock import AsyncMock

import pytest

from agent_gateway.common.config_panel import config_set_handlers


@pytest.mark.asyncio
async def test_enterprise_config_disables_setup_guide_and_keeps_web_flags(monkeypatch):
    monkeypatch.setattr(config_set_handlers, "get_config_raw", lambda: {
        "setup_guide": {"enabled": True},
        "a2ui": {"enabled": True},
        "recommendation": {"prompt_enabled": False},
        "experimental": {"comfyui_workflow_enabled": "false"},
        "models": {"default": {"api_key": "fixture-secret"}},
    })
    channel = AsyncMock()
    outbound = object()
    await config_set_handlers.config_get_handler(channel, outbound, "config-id")
    args = channel.send_response.await_args
    assert args.args == (outbound, "config-id")
    assert args.kwargs["ok"] is True
    payload = args.kwargs["payload"]
    assert payload["setup_guide_enabled"] == "false"
    assert payload["a2ui_enabled"] == "true"
    assert payload["prompt_suggestion_enabled"] == "false"
    assert payload["comfyui_workflow_enabled"] == "false"
    assert "fixture-secret" not in str(payload)
