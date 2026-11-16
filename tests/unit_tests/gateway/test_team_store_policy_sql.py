# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared team-store SQL dialect and occupancy after a hold is released."""

from __future__ import annotations

import pytest

import agent_gateway.enterprise.db.team_store_actor as actor
from agent_gateway.enterprise.config.manager_config.core.quota.team_quota import (
    _row_occupies as manager_row_occupies,
)
from agent_gateway.enterprise.team.quota_policy import (
    _row_occupies as policy_row_occupies,
)


def test_upsert_policy_sql_uses_postgres_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actor, "team_store_db_type", lambda: "postgres")
    sql = actor.upsert_policy_sql()
    assert "ON CONFLICT (policy_id) DO UPDATE" in sql
    assert "EXCLUDED.policy_name" in sql
    assert "CAST(:match_expr AS json)" in sql
    assert "CAST(:data AS json)" in sql
    assert "ON DUPLICATE KEY" not in sql
    assert actor.policy_enabled_value(True) is True
    assert actor.policy_enabled_value(False) is False


def test_upsert_policy_sql_keeps_mysql_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actor, "team_store_db_type", lambda: "mysql")
    sql = actor.upsert_policy_sql()
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "VALUES(policy_name)" in sql
    assert "CAST(" not in sql
    assert actor.policy_enabled_value(True) == 1
    assert actor.policy_enabled_value(False) == 0


def test_develop_mysql_names_stay_importable(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent_gateway.enterprise.db.team_mysql_actor as old
    from agent_gateway.enterprise.team.route_binding_mysql import MysqlTeamRouteBindingStore
    from agent_gateway.enterprise.team.route_binding_shared import SharedTeamRouteBindingStore

    monkeypatch.setattr(old, "_gateway_db_kind", lambda: "mysql")
    assert old.use_team_mysql() is True
    monkeypatch.setattr(old, "_gateway_db_kind", lambda: "postgresql")
    assert old.use_team_mysql() is False
    assert old.get_team_mysql_actor is old.get_team_store_actor
    assert old.TeamMysqlActor is old.TeamStoreActor
    assert MysqlTeamRouteBindingStore is SharedTeamRouteBindingStore


@pytest.mark.parametrize("occupies", [manager_row_occupies, policy_row_occupies])
def test_released_hold_does_not_occupy(occupies) -> None:
    now = 1_000.0
    assert occupies({"status": "released", "expires_at": None}, now) is False
    assert occupies({"status": "reserved", "expires_at": None}, now) is True
    assert occupies({"status": "committed", "expires_at": None}, now) is True
    assert occupies({"status": "reserved", "expires_at": now - 1}, now) is False
