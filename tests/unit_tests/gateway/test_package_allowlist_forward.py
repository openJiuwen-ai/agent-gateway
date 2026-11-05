# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""WebSocket 出站请求使用企业 repository 中的专家包允许名单。"""

from __future__ import annotations

import pytest
from openjiuwen_protocol.e2a.agent_compat import e2a_to_agent_request
from openjiuwen_protocol.e2a.models import E2AEnvelope
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.enterprise.config.manager_config.core.application_config.package_allowlist import (
    PackageAllowlistService,
    read_package_allowlist_packages,
)
from agent_gateway.routing.agent_client import _stamp_package_allowlist

_KEEP = [{"kind": "agent_template", "id": "keep"}]
_EVIL = [{"kind": "agent_template", "id": "evil"}]
_REPO = (
    "agent_gateway.enterprise.config.manager_config.core.application_config."
    "package_allowlist.require_enterprise_repository"
)
_READ = (
    "agent_gateway.enterprise.config.manager_config.core.application_config."
    "package_allowlist.read_package_allowlist_packages"
)


def _envelope() -> E2AEnvelope:
    return E2AEnvelope(
        request_id="req-1",
        channel="web",
        method=ReqMethod.CHAT_SEND.value,
        session_id="sess-1",
        params={"session_id": "sess-1"},
        user_id="alice",
        channel_context={
            "package_allowlist": _EVIL,
            "routing": {"group_id": "finance", "bot_id": "bot-1"},
        },
    )


class _Repo:
    def __init__(self, row):
        self.row = row
        self.created = False

    async def get(self):
        return self.row

    async def create(self, row):
        self.created = True
        return row


@pytest.mark.asyncio
async def test_websocket_stamp_overwrites_client_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    async def _read() -> list[dict]:
        return list(_KEEP)

    monkeypatch.setattr(_READ, _read)
    envelope = _envelope()

    await _stamp_package_allowlist(envelope)

    assert envelope.channel_context["package_allowlist"] == _KEEP
    assert envelope.channel_context["routing"]["group_id"] == "finance"
    request = e2a_to_agent_request(envelope)
    assert request.metadata["package_allowlist"] == _KEEP


@pytest.mark.asyncio
async def test_personal_websocket_stamp_skips_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)

    async def _read() -> list[dict]:
        raise AssertionError("personal edition read the allowlist")

    monkeypatch.setattr(_READ, _read)
    envelope = _envelope()

    await _stamp_package_allowlist(envelope)

    assert envelope.channel_context["package_allowlist"] == _EVIL


@pytest.mark.asyncio
async def test_read_returns_stored_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr(_REPO, lambda _name: _Repo({"packages": _KEEP}))

    assert await read_package_allowlist_packages() == _KEEP


@pytest.mark.asyncio
async def test_read_failure_sends_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    def _missing(_name: str):
        raise RuntimeError("not wired")

    monkeypatch.setattr(_REPO, _missing)
    assert await read_package_allowlist_packages() == []


@pytest.mark.asyncio
async def test_personal_read_skips_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)

    def _missing(_name: str):
        raise AssertionError("personal edition read the repository")

    monkeypatch.setattr(_REPO, _missing)
    assert await read_package_allowlist_packages() == []


@pytest.mark.asyncio
async def test_upsert_rejects_oversized_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    repo = _Repo(None)
    monkeypatch.setattr(_REPO, lambda _name: repo)
    huge = [
        {"kind": "agent_template", "id": f"{index:04d}{'x' * 124}"}
        for index in range(200)
    ]

    with pytest.raises(ValueError, match="16384"):
        await PackageAllowlistService().upsert({"packages": huge})

    assert repo.created is False
