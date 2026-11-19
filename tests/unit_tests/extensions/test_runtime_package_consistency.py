# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""HTTP and WebSocket use the same bot-bound expert package source."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from e2a.models import E2AEnvelope

from agent_gateway.common.expert_packages import HEADER_NAME, METADATA_KEY, decode_expert_packages_header
from agent_gateway.enterprise.capabilities.experts import bound_packages
from agent_gateway.enterprise.capabilities.experts.outbound_packages import outbound_packages
from agent_gateway.extensions.runtime_management_extension.pod_http_client import _assemble_with_allowlist
from agent_gateway.routing.agent_client import _stamp_expert_packages


def _envelope() -> E2AEnvelope:
    return E2AEnvelope(
        request_id="req-1", channel="web", method="chat.send", session_id="session-1",
        user_id="trusted-user",
        params={"session_id": "session-1", "bot_id": "forged-bot"},
        channel_context={
            "user_id": "trusted-user",
            "routing": {"group_id": "trusted-group", "bot_id": "trusted-bot"},
            METADATA_KEY: [{"kind": "agent_template", "id": "forged-package"}],
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("packages", [[], [
    {"kind": "agent_template", "id": "expert-1"},
    {"kind": "agent_group", "id": "team-1"},
]])
async def test_http_and_websocket_use_same_trusted_bot_packages(monkeypatch, packages):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    lookup = AsyncMock(return_value=packages)
    monkeypatch.setattr(bound_packages, "packages_for_bot", lookup)
    http_envelope, ws_envelope = _envelope(), _envelope()

    request = await _assemble_with_allowlist(http_envelope, "http://agent/api/v1")
    await _stamp_expert_packages(ws_envelope)

    assert decode_expert_packages_header(request.headers[HEADER_NAME]) == packages
    assert ws_envelope.channel_context[METADATA_KEY] == packages
    assert [call.args for call in lookup.await_args_list] == [("trusted-bot",), ("trusted-bot",)]
    assert outbound_packages() is None


@pytest.mark.asyncio
async def test_http_missing_trusted_bot_does_not_use_params(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    lookup = AsyncMock(return_value=[])
    monkeypatch.setattr(bound_packages, "packages_for_bot", lookup)
    envelope = _envelope()
    envelope.channel_context["routing"].pop("bot_id")
    request = await _assemble_with_allowlist(envelope, "http://agent/api/v1")
    lookup.assert_awaited_once_with("")
    assert decode_expert_packages_header(request.headers[HEADER_NAME]) == []


@pytest.mark.asyncio
async def test_http_missing_trusted_bot_is_logged_not_silent(monkeypatch, caplog):
    """空 bot 出站仍受下游空名单约束，但必须留下可定位的告警。"""
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr(bound_packages, "packages_for_bot", AsyncMock(return_value=[]))
    envelope = _envelope()
    envelope.channel_context["routing"].pop("bot_id")
    with caplog.at_level("WARNING"):
        await _assemble_with_allowlist(envelope, "http://agent/api/v1")
    assert "no trusted bot_id" in caplog.text


@pytest.mark.asyncio
async def test_personal_http_skips_enterprise_package_lookup(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    lookup = AsyncMock(side_effect=AssertionError("personal request read enterprise packages"))
    monkeypatch.setattr(bound_packages, "packages_for_bot", lookup)
    request = await _assemble_with_allowlist(_envelope(), "http://agent/api/v1")
    lookup.assert_not_awaited()
    assert HEADER_NAME not in request.headers
