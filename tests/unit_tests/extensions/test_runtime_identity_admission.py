# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Runtime routing uses trusted identity and fails closed on missing admission."""
from __future__ import annotations

import builtins
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

import pytest
from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension import agent_authorization as admission
from agent_gateway.extensions.runtime_management_extension.invoke_ids import routing_triple_from_envelope
from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import identity_from_envelope
from agent_gateway.extensions.runtime_management_extension.session_route_client import FatalRouteError


def _envelope(method: str = "chat.send") -> E2AEnvelope:
    return E2AEnvelope(
        request_id="request-1", channel="web", method=method,
        user_id="trusted-user", session_id="session-1",
        channel_context={
            "user_id": "trusted-user",
            "routing": {"group_id": "trusted-group", "bot_id": "trusted-bot"},
        },
        params={"group_id": "forged-group", "bot_id": "forged-bot", "user_id": "forged-user"},
    )


@pytest.mark.parametrize("method", ["session.create", "history.get", "command.sandbox", "chat.send"])
def test_enterprise_identity_ignores_business_params(monkeypatch, method):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    envelope = _envelope(method)
    assert routing_triple_from_envelope(envelope) == ("trusted-group", "trusted-bot", "trusted-user")
    assert identity_from_envelope(envelope) == (
        "session-1", "trusted-group", "trusted-bot", "request-1", "trusted-user",
    )


@pytest.mark.parametrize("missing", ["group_id", "bot_id", "user_id"])
def test_enterprise_identity_does_not_fall_back_to_params(monkeypatch, missing):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    envelope = _envelope("session.create")
    if missing == "user_id":
        envelope.user_id = None
        envelope.channel_context.pop("user_id")
    else:
        envelope.channel_context["routing"].pop(missing)
    with pytest.raises(FatalRouteError) as exc:
        identity_from_envelope(envelope)
    assert exc.value.code == "FORBIDDEN"


def test_authorized_identity_is_used_before_session_key(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    envelope = _envelope()
    envelope.session_id = None
    assert identity_from_envelope(envelope, authorized_identity=("g", "b", "u")) == (
        "g:b:u", "g", "b", "request-1", "u",
    )


def test_personal_identity_keeps_params_fallback(monkeypatch):
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    envelope = _envelope("session.create")
    envelope.channel_context = {}
    envelope.user_id = None
    assert identity_from_envelope(envelope) == (
        "session-1", "forged-group", "forged-bot", "request-1", "forged-user",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", sorted(admission.CHAT_METHODS))
async def test_enterprise_chat_missing_repository_is_unavailable(monkeypatch, method):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    getter, matches = Mock(return_value=None), Mock()
    monkeypatch.setattr(admission, "_enterprise_checkers", lambda: (getter, matches))
    with pytest.raises(FatalRouteError) as exc:
        await admission.authorize_agent(_envelope(method))
    assert exc.value.code == "SERVICE_UNAVAILABLE"
    getter.assert_called_once_with("instance_agent_resource")
    matches.assert_not_called()


@pytest.mark.asyncio
async def test_enterprise_chat_missing_module_is_unavailable(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    original_import = builtins.__import__

    def import_without_admission(name, *args, **kwargs):
        if name == "agent_gateway.config.enterprise.access":
            raise ModuleNotFoundError("admission dependency unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_admission)
    with pytest.raises(FatalRouteError) as exc:
        await admission.authorize_agent(_envelope())
    assert exc.value.code == "SERVICE_UNAVAILABLE"


@pytest.mark.asyncio
@pytest.mark.parametrize("enterprise,method", [(False, "chat.send"), (True, "session.create")])
async def test_personal_and_non_chat_skip_resource_admission(monkeypatch, enterprise, method):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise" if enterprise else "personal")
    checkers = Mock(side_effect=AssertionError("unexpected enterprise admission dependency"))
    monkeypatch.setattr(admission, "_enterprise_checkers", checkers)
    assert await admission.authorize_agent(_envelope(method)) is None
    checkers.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("row,allowed", [
    ({"enabled": True, "expires_at": None, "match_expr": "policy"}, True),
    ({"enabled": False, "match_expr": "policy"}, False),
    ({"enabled": True, "expires_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()}, False),
    ({"enabled": True, "expires_at": "invalid-date", "match_expr": "policy"}, False),
    (None, False),
])
async def test_enterprise_admission_preserves_resource_checks(monkeypatch, row, allowed):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    repo = Mock(get=AsyncMock(return_value=row))
    matches = Mock(return_value=True)
    monkeypatch.setattr(admission, "_enterprise_checkers", lambda: (Mock(return_value=repo), matches))
    if allowed:
        assert await admission.authorize_agent(_envelope()) == ("trusted-group", "trusted-bot", "trusted-user")
        matches.assert_called_once_with("policy", {
            "group_id": "trusted-group", "bot_id": "trusted-bot", "user_id": "trusted-user",
        })
    else:
        with pytest.raises(FatalRouteError) as exc:
            await admission.authorize_agent(_envelope())
        assert exc.value.code == "FORBIDDEN"
    repo.get.assert_awaited_once_with(resource_id="trusted-bot")


@pytest.mark.asyncio
async def test_enterprise_admission_policy_denial_remains_forbidden(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    repo = Mock(get=AsyncMock(return_value={"enabled": True, "match_expr": "deny"}))
    monkeypatch.setattr(admission, "_enterprise_checkers", lambda: (Mock(return_value=repo), Mock(return_value=False)))
    with pytest.raises(FatalRouteError) as exc:
        await admission.authorize_agent(_envelope())
    assert exc.value.code == "FORBIDDEN"
