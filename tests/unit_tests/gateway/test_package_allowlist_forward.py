"""Expert packages follow the bot template rather than client-provided data."""
from __future__ import annotations

import pytest
from e2a.agent_compat import e2a_to_agent_request
from e2a.models import E2AEnvelope
from e2a.message import ReqMethod
from agent_gateway.common.expert_packages import METADATA_KEY, HEADER_NAME, encode_expert_packages_header, decode_expert_packages_header
from agent_gateway.enterprise.capabilities.experts.bound_packages import packages_for_bot, packages_from_template_rows
from agent_gateway.routing.agent_client import _stamp_expert_packages

_KEEP = [{"kind": "agent_template", "id": "keep"}]
_EVIL = [{"kind": "agent_template", "id": "evil"}]
_PACKAGES = "agent_gateway.enterprise.capabilities.experts.bound_packages.packages_for_bot"

def _envelope() -> E2AEnvelope:
    return E2AEnvelope(
        request_id="req-1",
        channel="web",
        method=ReqMethod.CHAT_SEND.value,
        session_id="sess-1",
        params={"session_id": "sess-1"},
        user_id="alice",
        channel_context={
            METADATA_KEY: _EVIL,
            "routing": {"group_id": "finance", "bot_id": "bot-1"},
        },
    )


@pytest.mark.asyncio
async def test_websocket_stamp_overwrites_client_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    async def _read(_bot_id: str) -> list[dict]:
        assert _bot_id == "bot-1"
        return list(_KEEP)

    monkeypatch.setattr(_PACKAGES, _read)
    envelope = _envelope()
    await _stamp_expert_packages(envelope)
    assert envelope.channel_context[METADATA_KEY] == _KEEP
    assert envelope.channel_context["routing"]["group_id"] == "finance"
    request = e2a_to_agent_request(envelope)
    assert request.metadata[METADATA_KEY] == _KEEP


@pytest.mark.asyncio
async def test_personal_websocket_stamp_skips_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)

    async def _read() -> list[dict]:
        raise AssertionError("personal edition read the allowlist")

    monkeypatch.setattr(_PACKAGES, _read)
    envelope = _envelope()
    await _stamp_expert_packages(envelope)
    assert envelope.channel_context[METADATA_KEY] == _EVIL


def test_allowlist_header_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    encoded = encode_expert_packages_header(_KEEP)
    assert decode_expert_packages_header(encoded) == _KEEP
    assert decode_expert_packages_header("corrupt header") == []


def test_template_rows_become_packages_without_match_expr() -> None:
    packages = packages_from_template_rows(
        ["tpl-keep", "tpl-keep", "tpl-gone", "tpl-off"],
        [
            {
                "template_id": "tpl-keep",
                "enabled": True,
                "packages": [
                    {"kind": "agent_template", "asset_id": "keep", "version": "1"},
                    {"kind": "agent_group", "asset_id": "team-1", "version": ""},
                    {"kind": "agent_template", "asset_id": "keep", "version": "2"},
                    {"kind": "other", "asset_id": "skip-me"},
                ],
            },
            {
                "template_id": "tpl-off",
                "enabled": False,
                "packages": [{"kind": "agent_template", "asset_id": "off"}],
            },
        ],
    )
    assert packages == [
        {"kind": "agent_template", "id": "keep"},
        {"kind": "agent_group", "id": "team-1"},
    ]
    assert "match_expr" not in packages[0]


@pytest.mark.asyncio
async def test_packages_for_bot_reads_template_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    async def _list(table: str, *, filters=None, order_by: str = ""):
        del order_by
        if table == "instance_agent_resource" and filters["resource_id"] == "bot-1":
            return [{"ref_template_id": "tpl-1", "enabled": True}]
        if table == "agent_template" and filters["template_id"] == "tpl-1":
            return [{
                "template_ref": {
                    "expert_package": ["tpl-keep"],
                },
            }]
        if table == "expert_package_template":
            return [
                {
                    "template_id": "tpl-keep",
                    "enabled": True,
                    "packages": [
                        {"kind": "agent_template", "asset_id": "keep"},
                        {"kind": "agent_group", "asset_id": "team-1"},
                    ],
                },
            ]
        return []

    monkeypatch.setattr(
        "agent_gateway.enterprise.config.effective.db_queries.list_records",
        _list,
    )
    assert await packages_for_bot("bot-1") == [
        {"kind": "agent_template", "id": "keep"},
        {"kind": "agent_group", "id": "team-1"},
    ]


@pytest.mark.asyncio
async def test_packages_for_missing_bot_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    async def _list(table: str, *, filters=None, order_by: str = ""):
        del table, filters, order_by
        return []

    monkeypatch.setattr(
        "agent_gateway.enterprise.config.effective.db_queries.list_records",
        _list,
    )
    assert await packages_for_bot("missing") == []
    assert await packages_for_bot("") == []


@pytest.mark.asyncio
async def test_personal_packages_for_bot_skips_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)

    def _missing(*_args, **_kwargs):
        raise AssertionError("personal edition read the gateway database")

    monkeypatch.setattr(
        "agent_gateway.enterprise.config.effective.db_queries.list_records",
        _missing,
    )
    assert await packages_for_bot("bot-1") == []


def test_allowlist_module_forwards_outbound_names() -> None:
    from agent_gateway.enterprise.capabilities.experts import allowlist, outbound_packages

    assert allowlist.bind_outbound_allowlist is outbound_packages.bind_outbound_packages
    assert allowlist.outbound_allowlist_packages is outbound_packages.outbound_packages
    assert allowlist.apply_allowlist_to_envelope is outbound_packages.apply_packages_to_envelope
    assert allowlist.attach_expert_packages_header is outbound_packages.attach_expert_packages_header


