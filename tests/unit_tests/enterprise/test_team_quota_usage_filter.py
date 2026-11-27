# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Team quota usage ignores leases whose team is no longer active."""

from __future__ import annotations

import pytest

from agent_gateway.enterprise.config.manager_config.core.quota import team_quota


class _Actor:
    handler = object()

    def call(self, fn):
        return fn()


def _lease(team_id: str, *, status: str = "committed") -> dict:
    return {
        "lease_id": f"lease-{team_id or 'empty'}",
        "kind": team_quota.KIND_TEAM_CREATE,
        "user_id": "u1",
        "group_id": "g1",
        "bot_id": "b1",
        "team_id": team_id,
        "member_name": "team-leader",
        "status": status,
        "expires_at": None,
    }


def _install(
    monkeypatch: pytest.MonkeyPatch, leases: list[dict], fetch
) -> list[tuple[str, dict]]:
    actor = _Actor()
    calls: list[tuple[str, dict]] = []

    def _record_fetch(handler, sql, params):
        calls.append((str(sql), dict(params)))
        return fetch(handler, sql, params)

    def _list(handler, table_name, *, filters=None, **kwargs):
        filters = filters or {}
        if table_name == team_quota._LEASE_TABLE and "team_id" not in filters:
            return leases
        return []

    monkeypatch.setattr(team_quota, "_require_mysql", lambda: actor)
    monkeypatch.setattr(team_quota, "list_record_dicts", _list)
    monkeypatch.setattr(team_quota, "fetch_all", _record_fetch)
    return calls


def test_usage_drops_lease_of_stopped_team(monkeypatch: pytest.MonkeyPatch) -> None:
    leases = [_lease("team-live"), _lease("team-stale"), _lease("")]
    calls = _install(
        monkeypatch, leases, lambda *args, **kwargs: [{"team_id": "team-live"}]
    )

    usage = team_quota.TeamQuotaPolicyService()._list_usage_sync()

    assert usage["items"][0]["used"] == 2
    assert [t["team_id"] for t in usage["items"][0]["teams"]] == ["team-live"]
    assert usage["orgs"] == [{"group_id": "g1", "used": 2, "limit": 0}]

    # The status whitelist is the core of the fix; it must be in the query itself.
    sql, params = calls[0]
    assert "status IN" in sql
    assert set(params.values()) == {"creating", "running", "paused", "stopping"}
    assert "stopped" not in params.values()
    assert "deleted" not in params.values()


def test_usage_drops_every_stale_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(
        monkeypatch,
        [_lease("team-stale")],
        lambda *args, **kwargs: [],
    )

    usage = team_quota.TeamQuotaPolicyService()._list_usage_sync()

    assert usage == {"items": [], "count": 0, "orgs": []}


def test_usage_keeps_leases_when_control_table_unreadable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """读表失败时不过滤，避免把活跃团队误判成残留。"""

    def _boom(*args, **kwargs):
        raise RuntimeError("team_entity unavailable")

    _install(monkeypatch, [_lease("team-stale")], _boom)

    usage = team_quota.TeamQuotaPolicyService()._list_usage_sync()

    assert usage["items"][0]["used"] == 1
