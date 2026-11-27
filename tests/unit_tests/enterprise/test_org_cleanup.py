# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Identity-change cascade: stop active teams and release their quota."""

from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from agent_gateway.enterprise.team import org_cleanup


def _entity(
    team_id: str,
    group_id: str,
    *,
    user_id: str = "u1",
    team_session_id: str = "",
) -> dict[str, str]:
    return {
        "team_id": team_id,
        "team_session_id": team_session_id or f"sess-{team_id}",
        "owner_user_id": user_id,
        "owner_group_id": group_id,
        "owner_bot_id": "b1",
        "status": "running",
    }


def _async_return(value: bool):
    async def _fn(team_id: str, **kwargs: str) -> bool:
        return value

    return _fn


@pytest.fixture(autouse=True)
def _direct_store(monkeypatch: pytest.MonkeyPatch):
    """Run the sync store helpers inline instead of through the actor thread."""

    async def _run(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(org_cleanup, "run_team_store", _run)


@pytest.mark.asyncio
async def test_stop_teams_for_org_falls_back_when_runtime_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entities = [_entity("t1", "g1"), _entity("t4", "g1", user_id="u2")]
    marked: list[str] = []
    monkeypatch.setattr(
        org_cleanup, "_list_active_sync", lambda column, value: entities
    )
    monkeypatch.setattr(org_cleanup, "_mark_stopped_sync", marked.append)
    monkeypatch.setattr(org_cleanup, "stop_team_runtime", _async_return(False))

    stopped = await org_cleanup.stop_teams_for_org("g1")

    assert stopped == ["t1", "t4"]
    assert marked == ["t1", "t4"]


@pytest.mark.asyncio
async def test_stop_teams_for_org_leaves_state_to_agentserver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """转发成功时由 AgentServer 落状态、释放租约，不在 Gateway 重复处理。"""
    entities = [_entity("t1", "g1")]
    calls: list[tuple[str, str]] = []
    marked: list[str] = []

    async def _delivered(team_id: str, **kwargs: str) -> bool:
        calls.append((team_id, kwargs.get("team_session_id", "")))
        return True

    monkeypatch.setattr(
        org_cleanup, "_list_active_sync", lambda column, value: entities
    )
    monkeypatch.setattr(org_cleanup, "_mark_stopped_sync", marked.append)
    monkeypatch.setattr(org_cleanup, "stop_team_runtime", _delivered)

    stopped = await org_cleanup.stop_teams_for_org("g1")

    assert stopped == ["t1"]
    assert calls == [("t1", "sess-t1")]
    assert marked == []


@pytest.mark.asyncio
async def test_stop_teams_for_org_ignores_empty_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        org_cleanup,
        "_list_active_sync",
        lambda column, value: pytest.fail("must not read store"),
    )

    assert await org_cleanup.stop_teams_for_org("  ") == []


@pytest.mark.asyncio
async def test_stop_teams_for_user_stops_orgs_outside_keep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """用户当前仍在 g2，只停他不在的 g1 组织下的团队。"""
    entities = [_entity("t1", "g1"), _entity("t3", "g2")]
    marked: list[str] = []
    monkeypatch.setattr(
        org_cleanup, "_list_active_sync", lambda column, value: entities
    )
    monkeypatch.setattr(org_cleanup, "_mark_stopped_sync", marked.append)
    monkeypatch.setattr(org_cleanup, "stop_team_runtime", _async_return(False))

    stopped = await org_cleanup.stop_teams_for_user("u1", ["g2"])

    assert stopped == ["t1"]
    assert marked == ["t1"]


@pytest.mark.asyncio
async def test_stop_teams_for_user_with_empty_keep_stops_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """删除用户：只停该用户的团队，空保留集合表示全部。"""
    entities = [_entity("t1", "g1"), _entity("t3", "g2")]
    marked: list[str] = []
    monkeypatch.setattr(
        org_cleanup, "_list_active_sync", lambda column, value: entities
    )
    monkeypatch.setattr(org_cleanup, "_mark_stopped_sync", marked.append)
    monkeypatch.setattr(org_cleanup, "stop_team_runtime", _async_return(False))

    stopped = await org_cleanup.stop_teams_for_user("u1")

    assert stopped == ["t1", "t3"]
    assert marked == ["t1", "t3"]


@pytest.mark.asyncio
async def test_stop_teams_for_user_ignores_empty_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        org_cleanup,
        "_list_active_sync",
        lambda column, value: pytest.fail("must not read store"),
    )

    assert await org_cleanup.stop_teams_for_user("") == []


def test_mark_stopped_sync_stops_entity_and_releases_leases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The database-only fallback must really flip status and release leases."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(
            """
            CREATE TABLE team_entity (
                team_id TEXT PRIMARY KEY, status TEXT, updated_at REAL
            );
            CREATE TABLE team_quota_lease (
                lease_id TEXT PRIMARY KEY, team_id TEXT, status TEXT, updated_at REAL
            );
            INSERT INTO team_entity VALUES ('t1', 'running', 0);
            INSERT INTO team_quota_lease VALUES ('l1', 't1', 'committed', 0);
            INSERT INTO team_quota_lease VALUES ('l2', 't1', 'released', 0);
            INSERT INTO team_quota_lease VALUES ('l3', 't2', 'committed', 0);
            """
        )

        class _Actor:
            handler = object()

            def call(self, fn):
                return asyncio.run(fn())

        class _AsyncConn:
            async def execute(self, statement, params=None):
                conn.execute(str(statement), params or {})

        async def _run_tx(_handler, fn):
            await fn(_AsyncConn())

        monkeypatch.setattr(org_cleanup, "_require_actor", lambda: _Actor())
        monkeypatch.setattr(org_cleanup, "run_tx", _run_tx)

        org_cleanup._mark_stopped_sync("t1")

        assert conn.execute(
            "SELECT status FROM team_entity WHERE team_id='t1'"
        ).fetchone() == ("stopped",)
        assert conn.execute(
            "SELECT lease_id, status FROM team_quota_lease ORDER BY lease_id"
        ).fetchall() == [
            ("l1", "released"),
            ("l2", "released"),
            ("l3", "committed"),
        ]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_stop_team_runtime_sends_team_stop_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The forwarded stop must carry the method, ids and owner triple."""
    captured: dict[str, object] = {}

    class _Client:
        async def send_request(self, envelope):
            captured["envelope"] = envelope
            return SimpleNamespace(ok=True)

    class _Registry:
        def get_agent_server_client(self):
            return _Client()

    monkeypatch.setattr(
        "agent_gateway.extensions.registry.ExtensionRegistry.get_instance",
        lambda: _Registry(),
    )

    ok = await org_cleanup.stop_team_runtime(
        "t1",
        team_session_id="s1",
        owner_user_id="u1",
        owner_group_id="g1",
        owner_bot_id="b1",
    )

    assert ok is True
    envelope = captured["envelope"]
    assert envelope.method == "team.stop"
    assert envelope.params == {"team_id": "t1", "session_id": "s1"}
    assert envelope.session_id == "s1"
    assert envelope.channel_context == {
        "user_id": "u1",
        "routing": {"group_id": "g1", "bot_id": "b1"},
    }
