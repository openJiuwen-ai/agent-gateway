# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.routing.session_metadata_remote import SessionMetadataRemoteUnavailable


def _team_message():
    return SimpleNamespace(
        session_id="sess-1",
        user_id="user-1",
        params={
            "mode": "team.plan.normal",
            "target_team_id": "team-1",
        },
    )


def _handler():
    return object.__new__(MessageHandler)


def _install_provider(monkeypatch, metadata=None, error=None):
    if error:
        get_metadata = AsyncMock(side_effect=error)
    else:
        get_metadata = AsyncMock(return_value=metadata)
    provider = SimpleNamespace(get_session_metadata_async=get_metadata)
    monkeypatch.setattr(
        "agent_gateway.common.external_providers.provider",
        lambda domain: provider,
    )
    return get_metadata


@pytest.mark.asyncio
async def test_targeted_expert_cancel_requires_route_and_session(monkeypatch):
    get_metadata = _install_provider(monkeypatch, {})
    msg = _team_message()
    msg.session_id = None

    assert await MessageHandler._is_targeted_expert_cancel(_handler(), msg) is False
    get_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_targeted_expert_cancel_returns_true_on_remote_unavailable(monkeypatch):
    get_metadata = _install_provider(
        monkeypatch,
        error=SessionMetadataRemoteUnavailable("unavailable"),
    )

    assert await MessageHandler._is_targeted_expert_cancel(
        _handler(), _team_message()
    ) is True
    get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_targeted_expert_cancel_returns_false_on_other_lookup_failure(monkeypatch):
    get_metadata = _install_provider(
        monkeypatch,
        error=RuntimeError("lookup failed"),
    )

    assert await MessageHandler._is_targeted_expert_cancel(
        _handler(), _team_message()
    ) is False
    get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_targeted_expert_cancel_unknown_owner_is_targeted(monkeypatch):
    get_metadata = _install_provider(monkeypatch, {})

    assert await MessageHandler._is_targeted_expert_cancel(
        _handler(), _team_message()
    ) is True
    get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_targeted_expert_cancel_owner_match_is_not_targeted(monkeypatch):
    get_metadata = _install_provider(monkeypatch, {"team_name": "team-1"})

    assert await MessageHandler._is_targeted_expert_cancel(
        _handler(), _team_message()
    ) is False
    get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_targeted_expert_cancel_owner_mismatch_is_targeted(monkeypatch):
    get_metadata = _install_provider(monkeypatch, {"team_name": "team-2"})

    assert await MessageHandler._is_targeted_expert_cancel(
        _handler(), _team_message()
    ) is True
    get_metadata.assert_awaited_once()
