# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Abandoned team-create holds: reserved crash holds and orphaned commits."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_gateway.enterprise import edition
from agent_gateway.enterprise.db import team_store_actor
from agent_gateway.enterprise.team import abandoned_hold

_LEASE_COLUMNS = (
    "lease_id, kind, idempotency_key, team_id, status, expires_at, updated_at"
)


class _FakeConnection:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def execute(self, statement, params=None):
        self._conn.execute(str(statement), dict(params or {}))
        return None


class _FakeTransaction:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None

    async def __aenter__(self) -> _FakeConnection:
        self._conn = sqlite3.connect(self._path)
        return _FakeConnection(self._conn)

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        assert self._conn is not None
        try:
            if exc_type is None:
                self._conn.commit()
        finally:
            self._conn.close()
        return False


class _FakeEngine:
    def __init__(self, path: Path) -> None:
        self._path = path

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction(self._path)


class _FakeActor:
    def __init__(self, path: Path) -> None:
        self.handler = SimpleNamespace(get_engine=lambda: _FakeEngine(path))

    def call(self, coro_fn):
        return asyncio.run(coro_fn())


@pytest.fixture
def quota_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "team_store.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE team_entity (
                team_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'running'
            );
            CREATE TABLE team_quota_lease (
                lease_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                team_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                expires_at REAL,
                updated_at REAL NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(edition, "is_enterprise", lambda: True)
    monkeypatch.setattr(team_store_actor, "use_shared_team_store", lambda: True)
    monkeypatch.setattr(
        team_store_actor, "get_team_store_actor", lambda: _FakeActor(path)
    )
    return path


def _seed(path: Path, entities: tuple[str, ...], leases: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executemany(
            "INSERT INTO team_entity (team_id) VALUES (?)",
            [(team_id,) for team_id in entities],
        )
        conn.executemany(
            f"INSERT INTO team_quota_lease ({_LEASE_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            leases,
        )
        conn.commit()
    finally:
        conn.close()


def _seed_entities(path: Path, rows: list[tuple[str, str]]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executemany(
            "INSERT INTO team_entity (team_id, status) VALUES (?, ?)", rows
        )
        conn.commit()
    finally:
        conn.close()


def _statuses(path: Path) -> dict[str, str]:
    conn = sqlite3.connect(path)
    try:
        return {
            str(row[0]): str(row[1])
            for row in conn.execute("SELECT lease_id, status FROM team_quota_lease")
        }
    finally:
        conn.close()


def test_releases_committed_lease_whose_team_entity_is_gone(quota_db: Path) -> None:
    now = time.time()
    old = now - abandoned_hold.COMMITTED_ORPHAN_MIN_AGE_SEC - 60
    _seed(
        quota_db,
        entities=("live-team",),
        leases=[
            ("orphan", "team_create", "session:s1", "deleted-team", "committed", None, old),
            ("live", "team_create", "session:s2", "live-team", "committed", None, old),
            ("no-team-id", "team_create", "session:s3", "", "committed", None, old),
            ("teammate", "teammate_spawn", "member:m1", "deleted-team", "committed", None, old),
        ],
    )

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {
        "orphan": "released",
        "live": "committed",
        # A committed row without a team id has no entity to compare against,
        # so it is left for the in-band owner rather than released blindly.
        "no-team-id": "committed",
        "teammate": "committed",
    }


def test_keeps_recently_committed_lease_without_entity(quota_db: Path) -> None:
    now = time.time()
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("young-orphan", "team_create", "session:s1", "deleted-team", "committed", None, now - 60),
        ],
    )

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {"young-orphan": "committed"}


def test_keeps_reserved_hold_inside_the_crash_window(quota_db: Path) -> None:
    now = time.time()
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("stale-hold", "team_create", "session:s1", "", "reserved", None, now - 31 * 60),
            ("fresh-hold", "team_create", "session:s2", "", "reserved", None, now - 60),
            (
                "expiring-hold",
                "team_create",
                "session:s3",
                "",
                "reserved",
                now + 60,
                now,
            ),
        ],
    )

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {
        "stale-hold": "released",
        "fresh-hold": "reserved",
        "expiring-hold": "reserved",
    }


def test_release_is_idempotent(quota_db: Path) -> None:
    now = time.time() - abandoned_hold.COMMITTED_ORPHAN_MIN_AGE_SEC - 60
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("orphan", "team_create", "session:s1", "deleted-team", "committed", None, now),
        ],
    )

    abandoned_hold.release_abandoned_holds()
    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {"orphan": "released"}


def test_personal_edition_skips_the_database(
    quota_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = time.time() - abandoned_hold.COMMITTED_ORPHAN_MIN_AGE_SEC - 60
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("orphan", "team_create", "session:s1", "deleted-team", "committed", None, now),
        ],
    )
    monkeypatch.setattr(edition, "is_enterprise", lambda: False)

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {"orphan": "committed"}


def test_releases_committed_lease_of_stopped_team(quota_db: Path) -> None:
    """``team.stop`` keeps the entity row with status=stopped; it owns no quota."""
    now = time.time()
    old = now - abandoned_hold.COMMITTED_ORPHAN_MIN_AGE_SEC - 60
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("stopped-team", "team_create", "session:s1", "t1", "committed", None, old),
            ("deleted-team", "team_create", "session:s2", "t2", "committed", None, old),
            ("live-team", "team_create", "session:s3", "t3", "committed", None, old),
        ],
    )
    _seed_entities(quota_db, [("t1", "stopped"), ("t2", "deleted"), ("t3", "running")])

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {
        "stopped-team": "released",
        "deleted-team": "released",
        "live-team": "committed",
    }


def test_committed_sweep_failure_keeps_the_reserved_release(quota_db: Path) -> None:
    """An older shared DB without team_entity must not break reserved recovery."""
    now = time.time()
    _seed(
        quota_db,
        entities=(),
        leases=[
            ("stale-hold", "team_create", "session:s1", "", "reserved", None, now - 31 * 60),
            (
                "orphan",
                "team_create",
                "session:s2",
                "gone",
                "committed",
                None,
                now - abandoned_hold.COMMITTED_ORPHAN_MIN_AGE_SEC - 60,
            ),
        ],
    )
    conn = sqlite3.connect(quota_db)
    try:
        conn.execute("DROP TABLE team_entity")
        conn.commit()
    finally:
        conn.close()

    abandoned_hold.release_abandoned_holds()

    assert _statuses(quota_db) == {
        "stale-hold": "released",
        "orphan": "committed",
    }
