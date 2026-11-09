# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Team store follows Gateway DB type, including MariaDB and PostgreSQL."""

from __future__ import annotations

import asyncio
import sys
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def _import_quota_subjects():
    from agent_gateway.enterprise.config.manager_config.core.quota.team_quota import (
        TeamQuotaPolicyService,
        _row_occupies as management_occupies,
    )
    from agent_gateway.enterprise.db import team_mysql_actor as actor
    from agent_gateway.enterprise.team.abandoned_hold import RESERVED_HOLD_MAX_SEC
    from agent_gateway.enterprise.team.quota_policy import (
        TeamQuotaPolicyStore,
        _row_occupies as policy_occupies,
    )

    return (
        TeamQuotaPolicyService,
        management_occupies,
        actor,
        RESERVED_HOLD_MAX_SEC,
        TeamQuotaPolicyStore,
        policy_occupies,
    )


def _load_without_init(name: str, path: Path) -> None:
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    module.__package__ = name
    sys.modules[name] = module


try:
    (
        TeamQuotaPolicyService,
        management_occupies,
        actor,
        RESERVED_HOLD_MAX_SEC,
        TeamQuotaPolicyStore,
        policy_occupies,
    ) = _import_quota_subjects()
except ModuleNotFoundError:
    # 部署节点上的协议包还没有 match_expressions / template_slots。
    # 只在正式导入失败时跳过 manager_config 的路由初始化，避免污染已加载的包。
    for key in list(sys.modules):
        if key == "agent_gateway.enterprise.config.manager_config" or key.startswith(
            "agent_gateway.enterprise.config.manager_config."
        ):
            del sys.modules[key]
    if "openjiuwen_protocol.match_expressions" not in sys.modules:
        match_expressions = types.ModuleType("openjiuwen_protocol.match_expressions")
        match_expressions.validate_match_expr = lambda expr: None
        match_expressions.matches = lambda expr, identity: True
        sys.modules["openjiuwen_protocol.match_expressions"] = match_expressions
    _manager_config = (
        Path(__file__).resolve().parents[3]
        / "agent_gateway"
        / "enterprise"
        / "config"
        / "manager_config"
    )
    _load_without_init(
        "agent_gateway.enterprise.config.manager_config",
        _manager_config,
    )
    _load_without_init(
        "agent_gateway.enterprise.config.manager_config.core",
        _manager_config / "core",
    )
    _load_without_init(
        "agent_gateway.enterprise.config.manager_config.core.quota",
        _manager_config / "core" / "quota",
    )
    _load_without_init(
        "agent_gateway.enterprise.config.manager_config.schemas",
        _manager_config / "schemas",
    )
    (
        TeamQuotaPolicyService,
        management_occupies,
        actor,
        RESERVED_HOLD_MAX_SEC,
        TeamQuotaPolicyStore,
        policy_occupies,
    ) = _import_quota_subjects()


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


def test_upsert_policy_sql_matches_dialect(monkeypatch):
    monkeypatch.setattr(actor, "team_store_db_type", lambda: "postgres")
    postgres_sql = actor.upsert_team_quota_policy_sql()
    assert "ON CONFLICT (policy_id) DO UPDATE" in postgres_sql

    monkeypatch.setattr(actor, "team_store_db_type", lambda: "mysql")
    mysql_sql = actor.upsert_team_quota_policy_sql()
    assert "ON DUPLICATE KEY UPDATE" in mysql_sql


@pytest.mark.parametrize("occupies", [policy_occupies, management_occupies])
@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"status": "released", "expires_at": None}, False),
        ({"status": "reserved", "expires_at": None}, True),
        ({"status": "committed", "expires_at": None}, True),
        ({"status": "reserved", "expires_at": 1.0}, False),
    ],
)
def test_row_occupies_follows_lease_status(occupies, row, expected):
    assert occupies(row, now=10.0) is expected


class _RecordingConn:
    def __init__(self, captured: list[object]) -> None:
        self._captured = captured

    async def execute(self, _statement, params) -> None:
        self._captured.append(params["enabled"])


class _FakeActor:
    """Run the store coroutine the way ``TeamMysqlActor.call`` does: sync in."""

    def __init__(self, captured: list[object]) -> None:
        self.handler = object()
        self._captured = captured

    def call(self, fn):
        async def _run() -> None:
            await fn()

        def _on_thread() -> None:
            asyncio.run(_run())

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            _on_thread()
            return None
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(_on_thread).result()
        return None


def test_shared_store_upsert_binds_enabled_as_bool(monkeypatch):
    captured: list[object] = []

    async def _run_tx(_handler, fn) -> None:
        await fn(_RecordingConn(captured))

    monkeypatch.setattr(actor, "team_store_db_type", lambda: "mysql")
    monkeypatch.setattr(actor, "run_tx", _run_tx)

    service = TeamQuotaPolicyStore()
    monkeypatch.setattr(service, "_use_mysql", lambda: True)
    monkeypatch.setattr(service, "_mysql", lambda: _FakeActor(captured))
    service.upsert_policy(
        {"policy_id": "p-false", "policy_name": "bool-check", "enabled": False}
    )

    team_quota = sys.modules[
        "agent_gateway.enterprise.config.manager_config.core.quota.team_quota"
    ]

    monkeypatch.setattr(team_quota, "run_tx", _run_tx)
    monkeypatch.setattr(team_quota, "_require_mysql", lambda: _FakeActor(captured))
    asyncio.run(
        TeamQuotaPolicyService().upsert(
            {"policy_id": "p-true", "policy_name": "bool-check", "enabled": True}
        )
    )

    assert captured == [False, True]
    assert all(type(item) is bool for item in captured)
