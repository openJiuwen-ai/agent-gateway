# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Team store follows Gateway DB type, including MariaDB and PostgreSQL."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.enterprise.db import team_mysql_actor as actor
from agent_gateway.enterprise.team.abandoned_hold import RESERVED_HOLD_MAX_SEC


def _settings(kind: str):
    return SimpleNamespace(gateway_db_type=kind)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("mysql", "mysql"),
        ("MariaDB", "mysql"),
        ("postgresql", "postgres"),
        ("pg", "postgres"),
    ],
)
def test_team_store_db_type_accepts_mysql_family_and_postgres(monkeypatch, kind, expected):
    monkeypatch.setattr(actor, "_gateway_db_kind", lambda: kind)
    assert actor.team_store_db_type() == expected


def test_json_value_expr_casts_only_on_postgres(monkeypatch):
    monkeypatch.setattr(actor, "team_store_db_type", lambda: "mysql")
    assert actor.json_value_expr("match_expr") == ":match_expr"
    monkeypatch.setattr(actor, "team_store_db_type", lambda: "postgres")
    assert actor.json_value_expr("data") == "CAST(:data AS json)"


def test_use_shared_team_store_rejects_unregistered_enterprise_type(monkeypatch):
    monkeypatch.setattr(actor, "_gateway_db_kind", lambda: "sqlite")
    monkeypatch.setattr(
        "agent_gateway.enterprise.edition.is_enterprise",
        lambda: True,
    )
    with pytest.raises(RuntimeError, match="GATEWAY_DB_TYPE=sqlite"):
        actor.use_shared_team_store()


def test_use_shared_team_store_keeps_personal_sqlite(monkeypatch):
    monkeypatch.setattr(actor, "_gateway_db_kind", lambda: "sqlite")
    monkeypatch.setattr(
        "agent_gateway.enterprise.edition.is_enterprise",
        lambda: False,
    )
    assert actor.use_shared_team_store() is False


def test_abandoned_hold_cutoff_is_thirty_minutes():
    assert RESERVED_HOLD_MAX_SEC == 30 * 60
