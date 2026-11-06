# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Team quota policy and usage over the shared quota database."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_gateway.common.paths import get_infra_data_dir
from agent_gateway.config.enterprise.expressions import matches, validate_match_expr

@dataclass(frozen=True)
class OwnerIdentity:
    """Routing identity of the user who created the team.

    ``group_id`` / ``bot_id`` are the enterprise routing scope; together with
    ``user_id`` they are what the gateway derives ``service_id`` / ``agent_id`` /
    ``workspace_key`` from. Personal edition leaves group/bot empty.
    """

    user_id: str = ""
    group_id: str = ""
    bot_id: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"user_id": self.user_id, "group_id": self.group_id, "bot_id": self.bot_id}

    def same_scope(self, other: "OwnerIdentity") -> bool:
        """Phase-1 "same tenant" check: same routing scope (group + bot)."""
        return self.group_id == other.group_id and self.bot_id == other.bot_id

DEFAULT_MAX_TEAMMATES = 5

def resolve_max_teammates(config_base: dict[str, Any] | None, template_id: str | None = None) -> int:
    """Read ``modes.team.<template>.max_teammates`` (fallback: any team / default)."""
    cfg = config_base if isinstance(config_base, dict) else {}
    modes = cfg.get("modes") if isinstance(cfg.get("modes"), dict) else {}
    teams = modes.get("team") if isinstance(modes.get("team"), dict) else {}
    candidates: list[dict[str, Any]] = []
    if template_id and isinstance(teams.get(template_id), dict):
        candidates.append(teams[template_id])
    candidates.extend(v for v in teams.values() if isinstance(v, dict))
    legacy = cfg.get("team")
    if isinstance(legacy, dict):
        candidates.append(legacy)
    for item in candidates:
        raw = item.get("max_teammates")
        if raw is None:
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return DEFAULT_MAX_TEAMMATES

KIND_TEAM_CREATE = "team_create"

KIND_TEAMMATE_SPAWN = "teammate_spawn"

STATUS_RESERVED = "reserved"

STATUS_COMMITTED = "committed"

DEFAULT_MAX_TEAMS_PER_USER = 5

DEFAULT_MAX_TEAMMATES_PER_TEAM = 5

@dataclass
class QuotaLimits:
    max_teams_per_user: int
    max_teammates_per_team: int
    default_teams: int = DEFAULT_MAX_TEAMS_PER_USER
    default_teammates: int = DEFAULT_MAX_TEAMMATES_PER_TEAM
    policy_id: str = ""
    # 0 means the organization has no team cap.
    max_teams_per_org: int = 0
    default_org_teams: int = 0

def _now() -> float:
    return time.time()

def _clean(value: Any) -> str:
    return str(value or "").strip()

def _lease_still_held(status: Any, expires_at: Any, now: float) -> bool:
    """Committed, or reserved and not yet expired."""
    if status == STATUS_COMMITTED:
        return True
    if status != STATUS_RESERVED:
        return False
    if expires_at is None:
        return True
    return float(expires_at) > now

def _row_occupies(row: dict[str, Any], now: float) -> bool:
    if row.get("status") == STATUS_COMMITTED:
        return True
    expires_at = row.get("expires_at")
    if expires_at is None:
        return True
    return float(expires_at) > now

DEFAULT_LEADER_MEMBER_NAME = "team-leader"

def leader_spawn_names(leader_member_name: str = "") -> tuple[str, ...]:
    """Member names that must not occupy a Y (teammate_spawn) slot."""
    names = {_clean(leader_member_name), DEFAULT_LEADER_MEMBER_NAME}
    return tuple(n for n in names if n)

def _parse_match_expr(raw: Any) -> Any:
    if raw is None or raw == "":
        return []
    if isinstance(raw, (list, dict)):
        return raw
    text = str(raw).strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

def _updated_at_rank(raw: Any) -> str:
    """Normalize ``updated_at`` so a later save sorts after an earlier one."""
    if raw is None or raw == "":
        return ""
    if isinstance(raw, datetime):
        return raw.strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(raw).strip().replace("T", " ").replace("Z", "")

def _policy_rank(row: dict[str, Any]) -> tuple[int, str]:
    """Higher priority wins. The same priority keeps the later ``updated_at``."""
    return (int(row.get("priority") or 0), _updated_at_rank(row.get("updated_at")))

def _is_unconditional(expr: Any) -> bool:
    canon = _parse_match_expr(expr)
    if canon in (None, [], ""):
        return True
    if isinstance(canon, str):
        return not any(marker in canon for marker in ("==", "!=", " in "))
    return False

def _select_limits(policies: list[dict[str, Any]], identity: OwnerIdentity) -> QuotaLimits:
    ident = {
        "user_id": identity.user_id,
        "group_id": identity.group_id,
        "bot_id": identity.bot_id,
    }
    hits: list[dict[str, Any]] = []
    for row in policies:
        if not bool(row.get("enabled", True)):
            continue
        expr = _parse_match_expr(row.get("match_expr"))
        try:
            if matches(expr, ident):
                hits.append(row)
        except Exception:  # noqa: BLE001
            logger.debug("[TeamQuota] skip invalid match_expr policy=%s", row.get("policy_id"))
    default_teams = DEFAULT_MAX_TEAMS_PER_USER
    default_teammates = DEFAULT_MAX_TEAMMATES_PER_TEAM
    for row in policies:
        if bool(row.get("enabled", True)) and _is_unconditional(row.get("match_expr")):
            default_teams = max(1, int(row.get("max_teams_per_user") or default_teams))
            default_teammates = max(1, int(row.get("max_teammates_per_team") or default_teammates))
            break
    if not hits:
        return QuotaLimits(
            max_teams_per_user=default_teams,
            max_teammates_per_team=default_teammates,
            default_teams=default_teams,
            default_teammates=default_teammates,
        )
    hits.sort(key=_policy_rank, reverse=True)
    top = hits[0]
    return QuotaLimits(
        max_teams_per_user=max(1, int(top.get("max_teams_per_user") or default_teams)),
        max_teammates_per_team=max(1, int(top.get("max_teammates_per_team") or default_teammates)),
        default_teams=default_teams,
        default_teammates=default_teammates,
        policy_id=_clean(top.get("policy_id")),
    )

def _expr_blob(expr: Any) -> str:
    canon = _parse_match_expr(expr)
    if canon in (None, [], ""):
        return ""
    if isinstance(canon, list):
        return "\n".join(str(item) for item in canon)
    return str(canon)

def _is_org_policy(expr: Any) -> bool:
    """Z ignores policies that name a user or a bot."""
    blob = _expr_blob(expr)
    return "user_id" not in blob and "bot_id" not in blob

def _org_limit_value(row: dict[str, Any]) -> int:
    raw = row.get("max_teams_per_org")
    if raw is None or raw == "":
        return 0
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    if value < 0:
        return 0
    return min(value, 1000)

def _select_org_limit(policies: list[dict[str, Any]], group_id: str) -> tuple[int, int]:
    """Return ``(limit, default)``. 0 means unlimited.

    Only unconditional policies and policies that do not mention ``user_id`` or
    ``bot_id`` participate. Highest priority among those that match this org wins.
    """
    default_org = 0
    for row in policies:
        match_expr = row.get("match_expr")
        if not bool(row.get("enabled", True)):
            continue
        if not _is_unconditional(match_expr):
            continue
        if not _is_org_policy(match_expr):
            continue
        default_org = _org_limit_value(row)
        break
    ident = {"user_id": "", "group_id": group_id, "bot_id": ""}
    hits: list[dict[str, Any]] = []
    for row in policies:
        if not bool(row.get("enabled", True)) or not _is_org_policy(row.get("match_expr")):
            continue
        expr = _parse_match_expr(row.get("match_expr"))
        try:
            if matches(expr, ident):
                hits.append(row)
        except Exception:  # noqa: BLE001
            logger.debug("[TeamQuota] skip invalid org match_expr policy=%s", row.get("policy_id"))
    if not hits:
        return default_org, default_org
    hits.sort(key=_policy_rank, reverse=True)
    return _org_limit_value(hits[0]), default_org

def _coerce_org_limit(body: dict[str, Any]) -> int:
    if "max_teams_per_org" not in body or body.get("max_teams_per_org") is None:
        return 0
    value = int(body.get("max_teams_per_org"))
    if value < 0 or value > 1000:
        raise ValueError("max_teams_per_org must be between 0 and 1000")
    return value

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS team_quota_policy (
    policy_id TEXT PRIMARY KEY,
    policy_name TEXT NOT NULL DEFAULT '',
    policy_desc TEXT,
    match_expr TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    max_teams_per_user INTEGER NOT NULL,
    max_teammates_per_team INTEGER NOT NULL,
    max_teams_per_org INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    source TEXT NOT NULL DEFAULT 'manual',
    data TEXT,
    created_at TEXT NOT NULL,
    created_by TEXT,
    updated_at TEXT NOT NULL,
    updated_by TEXT
);
CREATE TABLE IF NOT EXISTS team_quota_account (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL DEFAULT '',
    group_id TEXT NOT NULL DEFAULT '',
    bot_id TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (user_id, group_id, bot_id)
);
CREATE TABLE IF NOT EXISTS team_quota_lease (
    lease_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    group_id TEXT NOT NULL DEFAULT '',
    bot_id TEXT NOT NULL DEFAULT '',
    team_id TEXT NOT NULL DEFAULT '',
    member_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (kind, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_team_quota_lease_owner
    ON team_quota_lease (kind, user_id, group_id, bot_id, status);
CREATE INDEX IF NOT EXISTS idx_team_quota_lease_team
    ON team_quota_lease (kind, team_id, status);
CREATE INDEX IF NOT EXISTS idx_team_quota_lease_org
    ON team_quota_lease (kind, group_id, status);
CREATE TABLE IF NOT EXISTS team_quota_org_account (
    group_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS team_entity (
    team_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL DEFAULT '',
    owner_group_id TEXT NOT NULL DEFAULT '',
    owner_bot_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL
);
"""

def _ensure_sqlite_org_column(conn: sqlite3.Connection) -> None:
    """Existing files keep the old policy table. CREATE IF NOT EXISTS will not add a column."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(team_quota_policy)")}
    if "max_teams_per_org" not in cols:
        conn.execute(
            "ALTER TABLE team_quota_policy ADD COLUMN max_teams_per_org INTEGER NOT NULL DEFAULT 0"
        )

class TeamQuotaPolicyStore:
    """Manage policies and read usage without loading the team executor."""

    @staticmethod
    def _default_sqlite_path() -> Path:
        return get_infra_data_dir() / "agent" / "teams" / "team_control.db"

    @staticmethod
    def _quota_config() -> dict[str, Any]:
        from agent_gateway.common.config import get_config

        return get_config()

    def __init__(self, sqlite_path: Path | None = None) -> None:
        self._sqlite_path = sqlite_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    def _use_mysql(self) -> bool:
        if self._sqlite_path is not None:
            return False
        from agent_gateway.enterprise.db.team_mysql_actor import use_team_mysql

        return use_team_mysql()

    def use_mysql(self) -> bool:
        return self._use_mysql()

    def _sqlite(self) -> sqlite3.Connection:
        if self._conn is None:
            path = self._sqlite_path
            if path is None:
                path = self._default_sqlite_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(_SQLITE_SCHEMA)
            _ensure_sqlite_org_column(conn)
            self._conn = conn
        return self._conn

    @staticmethod
    def _mysql():
        from agent_gateway.enterprise.db.team_mysql_actor import get_team_mysql_actor

        return get_team_mysql_actor()

    def resolve_limits(self, identity: OwnerIdentity) -> QuotaLimits:
        return self._limits_for_rows(self.list_policies(), identity)

    @classmethod
    def _limits_for_rows(cls, rows: list[dict[str, Any]], identity: OwnerIdentity) -> QuotaLimits:
        limits = _select_limits(rows, identity)
        if not rows:
            y = DEFAULT_MAX_TEAMMATES_PER_TEAM
            try:
                y = max(1, int(resolve_max_teammates(cls._quota_config(), None)))
            except Exception:  # noqa: BLE001
                pass
            limits = QuotaLimits(
                max_teams_per_user=DEFAULT_MAX_TEAMS_PER_USER,
                max_teammates_per_team=y,
                default_teams=DEFAULT_MAX_TEAMS_PER_USER,
                default_teammates=DEFAULT_MAX_TEAMMATES_PER_TEAM,
            )
        org_limit, org_default = _select_org_limit(rows, identity.group_id)
        limits.max_teams_per_org = org_limit
        limits.default_org_teams = org_default
        return limits

    def list_policies(self) -> list[dict[str, Any]]:
        if self._use_mysql():
            from agent_gateway.enterprise.db.team_mysql_actor import list_record_dicts

            actor = self._mysql()
            return actor.call(
                lambda: list_record_dicts(actor.handler, "team_quota_policy")
            )
        with self._lock:
            rows = self._sqlite().execute("SELECT * FROM team_quota_policy").fetchall()
        return [dict(r) for r in rows]

    def upsert_policy(self, body: dict[str, Any]) -> dict[str, Any]:
        pid = _clean(body.get("policy_id")) or uuid.uuid4().hex
        name = _clean(body.get("policy_name")) or "team-quota"
        expr = body.get("match_expr")
        if expr is None:
            expr = []
        validate_match_expr(expr)
        x = max(1, min(1000, int(body.get("max_teams_per_user") or DEFAULT_MAX_TEAMS_PER_USER)))
        y = max(1, min(1000, int(body.get("max_teammates_per_team") or DEFAULT_MAX_TEAMMATES_PER_TEAM)))
        z = _coerce_org_limit(body)
        enabled = bool(body.get("enabled", True))
        priority = int(body.get("priority") or 0)
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # MySQL DATETIME rejects ISO-8601 with T/Z under strict mode.
        now_mysql = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        row = {
            "policy_id": pid,
            "policy_name": name,
            "policy_desc": _clean(body.get("policy_desc")) or None,
            "match_expr": expr,
            "priority": priority,
            "max_teams_per_user": x,
            "max_teammates_per_team": y,
            "max_teams_per_org": z,
            "enabled": enabled,
            "source": _clean(body.get("source")) or "manual",
            "data": body.get("data"),
            "created_at": now_iso,
            "created_by": _clean(body.get("created_by")) or None,
            "updated_at": now_iso,
            "updated_by": _clean(body.get("updated_by")) or None,
        }
        if self._use_mysql():
            from sqlalchemy import text

            from agent_gateway.enterprise.db.team_mysql_actor import run_tx

            actor = self._mysql()

            async def _op() -> None:
                payload = dict(row)
                payload["match_expr"] = json.dumps(expr, ensure_ascii=False)
                data = payload["data"]
                payload["data"] = None if data is None else json.dumps(data, ensure_ascii=False)
                payload["enabled"] = 1 if enabled else 0
                payload["created_at"] = now_mysql
                payload["updated_at"] = now_mysql

                async def _tx(conn: Any) -> None:
                    await conn.execute(
                        text(
                            "INSERT INTO team_quota_policy ("
                            "policy_id, policy_name, policy_desc, match_expr, priority, "
                            "max_teams_per_user, max_teammates_per_team, max_teams_per_org, enabled, source, data, "
                            "created_at, created_by, updated_at, updated_by"
                            ") VALUES ("
                            ":policy_id, :policy_name, :policy_desc, :match_expr, :priority, "
                            ":max_teams_per_user, :max_teammates_per_team, "
                            ":max_teams_per_org, :enabled, :source, :data, "
                            ":created_at, :created_by, :updated_at, :updated_by"
                            ") ON DUPLICATE KEY UPDATE "
                            "policy_name=VALUES(policy_name), policy_desc=VALUES(policy_desc), "
                            "match_expr=VALUES(match_expr), priority=VALUES(priority), "
                            "max_teams_per_user=VALUES(max_teams_per_user), "
                            "max_teammates_per_team=VALUES(max_teammates_per_team), "
                            "max_teams_per_org=VALUES(max_teams_per_org), "
                            "enabled=VALUES(enabled), source=VALUES(source), data=VALUES(data), "
                            "updated_at=VALUES(updated_at), updated_by=VALUES(updated_by)"
                        ),
                        payload,
                    )

                await run_tx(actor.handler, _tx)

            actor.call(_op)
            return row
        with self._lock:
            conn = self._sqlite()
            conn.execute(
                "INSERT INTO team_quota_policy ("
                "policy_id, policy_name, policy_desc, match_expr, priority, "
                "max_teams_per_user, max_teammates_per_team, max_teams_per_org, enabled, source, data, "
                "created_at, created_by, updated_at, updated_by"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(policy_id) DO UPDATE SET "
                "policy_name=excluded.policy_name, policy_desc=excluded.policy_desc, "
                "match_expr=excluded.match_expr, priority=excluded.priority, "
                "max_teams_per_user=excluded.max_teams_per_user, "
                "max_teammates_per_team=excluded.max_teammates_per_team, "
                "max_teams_per_org=excluded.max_teams_per_org, "
                "enabled=excluded.enabled, source=excluded.source, data=excluded.data, "
                "updated_at=excluded.updated_at, updated_by=excluded.updated_by",
                (
                    pid, name, row["policy_desc"], json.dumps(expr, ensure_ascii=False),
                    priority, x, y, z, 1 if enabled else 0, row["source"],
                    json.dumps(row["data"], ensure_ascii=False) if row["data"] is not None else None,
                    now_iso, row["created_by"], now_iso, row["updated_by"],
                ),
            )
        return row

    def delete_policy(self, policy_id: str) -> None:
        pid = _clean(policy_id)
        if not pid:
            return
        if self._use_mysql():
            from sqlalchemy import text

            from agent_gateway.enterprise.db.team_mysql_actor import run_tx

            actor = self._mysql()

            async def _op() -> None:
                async def _tx(conn: Any) -> None:
                    await conn.execute(
                        text("DELETE FROM team_quota_policy WHERE policy_id = :pid"),
                        {"pid": pid},
                    )

                await run_tx(actor.handler, _tx)

            actor.call(_op)
            return
        with self._lock:
            self._sqlite().execute("DELETE FROM team_quota_policy WHERE policy_id = ?", (pid,))

    def list_usage(self) -> dict[str, Any]:
        rows = self._list_occupying(KIND_TEAM_CREATE)
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (_clean(row.get("user_id")), _clean(row.get("group_id")), _clean(row.get("bot_id")))
            grouped.setdefault(key, []).append(row)
        items = []
        for (uid, gid, bid), leases in grouped.items():
            ident = OwnerIdentity(user_id=uid, group_id=gid, bot_id=bid)
            limits = self.resolve_limits(ident)
            team_ids = [_clean(r.get("team_id")) for r in leases if _clean(r.get("team_id"))]
            teams = []
            for tid in team_ids:
                teams.append(
                    {
                        "team_id": tid,
                        "used": self._count_team_members_including_leader(tid),
                        "limit": limits.max_teammates_per_team,
                    }
                )
            items.append(
                {
                    "user_id": uid,
                    "group_id": gid,
                    "bot_id": bid,
                    "used": len(leases),
                    "limit": limits.max_teams_per_user,
                    "default": limits.default_teams,
                    "teams": teams,
                }
            )
        by_org: dict[str, int] = {}
        for row in rows:
            gid = _clean(row.get("group_id"))
            if gid:
                by_org[gid] = by_org.get(gid, 0) + 1
        orgs = []
        for gid, used_n in sorted(by_org.items()):
            limits = self.resolve_limits(OwnerIdentity(user_id="", group_id=gid, bot_id=""))
            orgs.append({
                "group_id": gid,
                "used": used_n,
                "limit": limits.max_teams_per_org,
            })
        return {"items": items, "count": len(items), "orgs": orgs}

    def _count_team_teammates(self, team_id: str) -> int:
        skip = set(leader_spawn_names())
        return len(
            [
                row
                for row in self._list_occupying(KIND_TEAMMATE_SPAWN, team_id=team_id)
                if _clean(row.get("member_name")) not in skip
            ]
        )

    def _count_team_members_including_leader(self, team_id: str) -> int:
        tid = _clean(team_id)
        used_teammates = self._count_team_teammates(tid)
        if not tid:
            return used_teammates
        team_alive = bool(self._list_occupying(KIND_TEAM_CREATE, team_id=tid))
        return used_teammates + (1 if team_alive else 0)

    def _list_occupying(self, kind: str, team_id: str = "") -> list[dict[str, Any]]:
        now = _now()
        if self._use_mysql():
            from agent_gateway.enterprise.db.team_mysql_actor import list_record_dicts

            actor = self._mysql()
            filters: dict[str, Any] = {
                "kind": kind,
                "status": [STATUS_COMMITTED, STATUS_RESERVED],
            }
            if team_id:
                filters["team_id"] = team_id
            rows = actor.call(
                lambda: list_record_dicts(
                    actor.handler,
                    "team_quota_lease",
                    filters=filters,
                )
            )
            return [row for row in rows if _row_occupies(row, now)]
        with self._lock:
            conn = self._sqlite()
            if team_id:
                rows = conn.execute(
                    "SELECT * FROM team_quota_lease WHERE kind=? AND team_id=? AND "
                    "(status=? OR (status=? AND (expires_at IS NULL OR expires_at>?)))",
                    (kind, team_id, STATUS_COMMITTED, STATUS_RESERVED, now),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM team_quota_lease WHERE kind=? AND "
                    "(status=? OR (status=? AND (expires_at IS NULL OR expires_at>?)))",
                    (kind, STATUS_COMMITTED, STATUS_RESERVED, now),
                ).fetchall()
        return [dict(r) for r in rows]
