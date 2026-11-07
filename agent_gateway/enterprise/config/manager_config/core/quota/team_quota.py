# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""团队配额管理面投影：策略 upsert / 删除与本集群用量只读。

执行面（租约准入、团队创建限额）仍在 AgentServer；本模块只读写 Gateway
侧 ``team_quota_policy`` / ``team_quota_lease`` 表，不依赖 AgentServer 代码。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text

from agent_gateway.config.enterprise.expressions import matches, validate_match_expr
from agent_gateway.enterprise.db.team_mysql_actor import (
    get_team_mysql_actor,
    list_record_dicts,
    run_team_store,
    run_tx,
    upsert_team_quota_policy_sql,
    use_team_mysql,
)

from ...schemas.quota_schemas import TeamQuotaPolicyUpsertRequest

KIND_TEAM_CREATE = "team_create"
KIND_TEAMMATE_SPAWN = "teammate_spawn"
STATUS_COMMITTED = "committed"
STATUS_RESERVED = "reserved"
DEFAULT_MAX_TEAMS_PER_USER = 5
DEFAULT_MAX_TEAMMATES_PER_TEAM = 5
DEFAULT_LEADER_MEMBER_NAME = "team-leader"

_POLICY_TABLE = "team_quota_policy"
_LEASE_TABLE = "team_quota_lease"

_DELETE_POLICY_SQL = text("DELETE FROM team_quota_policy WHERE policy_id = :pid")


@dataclass(frozen=True)
class OwnerIdentity:
    user_id: str
    group_id: str
    bot_id: str


@dataclass
class QuotaLimits:
    max_teams_per_user: int
    max_teammates_per_team: int
    default_teams: int = DEFAULT_MAX_TEAMS_PER_USER
    default_teammates: int = DEFAULT_MAX_TEAMMATES_PER_TEAM
    policy_id: str = ""
    # 0 表示组织团队总数不设限。
    max_teams_per_org: int = 0
    default_org_teams: int = 0


def _clean(value: Any) -> str:
    return str(value or "").strip()


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
    if raw is None or raw == "":
        return ""
    if isinstance(raw, datetime):
        return raw.strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(raw).strip().replace("T", " ").replace("Z", "")


def _policy_rank(row: dict[str, Any]) -> tuple[int, str]:
    """priority 高者优先；同优先级取较晚 ``updated_at``。"""
    return (int(row.get("priority") or 0), _updated_at_rank(row.get("updated_at")))


def _is_unconditional(expr: Any) -> bool:
    canon = _parse_match_expr(expr)
    if canon in (None, [], ""):
        return True
    if isinstance(canon, str):
        return not any(marker in canon for marker in ("==", "!=", " in "))
    return False


def _expr_blob(expr: Any) -> str:
    canon = _parse_match_expr(expr)
    if canon in (None, [], ""):
        return ""
    if isinstance(canon, list):
        return "\n".join(str(item) for item in canon)
    return str(canon)


def _is_org_policy(expr: Any) -> bool:
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
    return 0 if value < 0 else min(value, 1000)


def _select_limits(policies: list[dict[str, Any]], identity: OwnerIdentity) -> QuotaLimits:
    ident = {
        "user_id": identity.user_id,
        "group_id": identity.group_id,
        "bot_id": identity.bot_id,
    }
    default_teams = DEFAULT_MAX_TEAMS_PER_USER
    default_teammates = DEFAULT_MAX_TEAMMATES_PER_TEAM
    for row in policies:
        if bool(row.get("enabled", True)) and _is_unconditional(row.get("match_expr")):
            default_teams = max(1, int(row.get("max_teams_per_user") or default_teams))
            default_teammates = max(
                1, int(row.get("max_teammates_per_team") or default_teammates)
            )
            break
    hits: list[dict[str, Any]] = []
    for row in policies:
        if not bool(row.get("enabled", True)):
            continue
        try:
            if matches(_parse_match_expr(row.get("match_expr")), ident):
                hits.append(row)
        except Exception:  # noqa: BLE001 - 单条策略非法不应影响整体用量
            continue
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
        max_teammates_per_team=max(
            1, int(top.get("max_teammates_per_team") or default_teammates)
        ),
        default_teams=default_teams,
        default_teammates=default_teammates,
        policy_id=_clean(top.get("policy_id")),
    )


def _select_org_limit(policies: list[dict[str, Any]], group_id: str) -> tuple[int, int]:
    """返回 ``(limit, default)``；0 表示不限制。"""
    default_org = 0
    for row in policies:
        if not bool(row.get("enabled", True)):
            continue
        expr = row.get("match_expr")
        if not _is_unconditional(expr) or not _is_org_policy(expr):
            continue
        default_org = _org_limit_value(row)
        break
    ident = {"user_id": "", "group_id": group_id, "bot_id": ""}
    hits: list[dict[str, Any]] = []
    for row in policies:
        if not bool(row.get("enabled", True)) or not _is_org_policy(row.get("match_expr")):
            continue
        try:
            if matches(_parse_match_expr(row.get("match_expr")), ident):
                hits.append(row)
        except Exception:  # noqa: BLE001 - 单条策略非法不应影响整体用量
            continue
    if not hits:
        return default_org, default_org
    hits.sort(key=_policy_rank, reverse=True)
    return _org_limit_value(hits[0]), default_org


def _row_occupies(row: dict[str, Any], now: float) -> bool:
    if row.get("status") == STATUS_COMMITTED:
        return True
    expires_at = row.get("expires_at")
    if expires_at is None:
        return True
    return float(expires_at) > now


def _require_mysql() -> Any:
    if not use_team_mysql():
        raise RuntimeError("team quota management requires the enterprise database")
    return get_team_mysql_actor()


class TeamQuotaPolicyService:
    """团队配额策略投影与本集群用量（只读）汇总。"""

    async def upsert(self, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise ValueError("team quota policy upsert requires an object")
        req = TeamQuotaPolicyUpsertRequest.model_validate(body)
        expr = req.match_expr if req.match_expr is not None else []
        validate_match_expr(expr)
        actor = _require_mysql()
        now_mysql = _utc_text()
        row = {
            "policy_id": req.policy_id,
            "policy_name": req.policy_name,
            "policy_desc": req.policy_desc,
            "match_expr": json.dumps(expr, ensure_ascii=False),
            "priority": int(req.priority),
            "max_teams_per_user": int(req.max_teams_per_user),
            "max_teammates_per_team": int(req.max_teammates_per_team),
            "max_teams_per_org": int(req.max_teams_per_org),
            "enabled": 1 if req.enabled else 0,
            "source": req.source,
            "data": None,
            "created_at": now_mysql,
            "created_by": None,
            "updated_at": now_mysql,
            "updated_by": None,
        }

        async def _op() -> None:
            async def _tx(conn: Any) -> None:
                await conn.execute(text(upsert_team_quota_policy_sql()), row)

            await run_tx(actor.handler, _tx)

        actor.call(_op)
        return {**row, "match_expr": expr, "enabled": bool(req.enabled)}

    async def delete(self, policy_id: str) -> None:
        pid = _clean(policy_id)
        if not pid:
            raise ValueError("policy_id is required")
        actor = _require_mysql()

        async def _op() -> None:
            async def _tx(conn: Any) -> None:
                await conn.execute(
                    _DELETE_POLICY_SQL, {"pid": pid}
                )

            await run_tx(actor.handler, _tx)

        actor.call(_op)

    async def list_usage(self) -> dict[str, Any]:
        return await run_team_store(self._list_usage_sync)

    def _list_usage_sync(self) -> dict[str, Any]:
        actor = _require_mysql()
        now = _now()
        policies = actor.call(
            lambda: list_record_dicts(actor.handler, _POLICY_TABLE)
        )
        rows = [
            row
            for row in actor.call(
                lambda: list_record_dicts(
                    actor.handler,
                    _LEASE_TABLE,
                    filters={"kind": KIND_TEAM_CREATE},
                )
            )
            if _row_occupies(row, now)
        ]
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (
                _clean(row.get("user_id")),
                _clean(row.get("group_id")),
                _clean(row.get("bot_id")),
            )
            grouped.setdefault(key, []).append(row)
        items = []
        for (uid, gid, bid), leases in grouped.items():
            limits = self._limits_for(policies, OwnerIdentity(uid, gid, bid))
            teams = [
                {
                    "team_id": tid,
                    "used": self._team_member_count(actor, tid, now),
                    "limit": limits.max_teammates_per_team,
                }
                for tid in (
                    _clean(r.get("team_id"))
                    for r in leases
                )
                if tid
            ]
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
            org_limit, _default = _select_org_limit(policies, gid)
            orgs.append({"group_id": gid, "used": used_n, "limit": org_limit})
        return {"items": items, "count": len(items), "orgs": orgs}

    @staticmethod
    def _limits_for(policies: list[dict[str, Any]], identity: OwnerIdentity) -> QuotaLimits:
        limits = _select_limits(policies, identity)
        org_limit, org_default = _select_org_limit(policies, identity.group_id)
        limits.max_teams_per_org = org_limit
        limits.default_org_teams = org_default
        return limits

    @staticmethod
    def _team_member_count(actor: Any, team_id: str, now: float) -> int:
        teammates = [
            row
            for row in actor.call(
                lambda: list_record_dicts(
                    actor.handler,
                    _LEASE_TABLE,
                    filters={"kind": KIND_TEAMMATE_SPAWN, "team_id": team_id},
                )
            )
            if _row_occupies(row, now)
            and _clean(row.get("member_name")) != DEFAULT_LEADER_MEMBER_NAME
        ]
        team_alive = any(
            _row_occupies(row, now)
            for row in actor.call(
                lambda: list_record_dicts(
                    actor.handler,
                    _LEASE_TABLE,
                    filters={"kind": KIND_TEAM_CREATE, "team_id": team_id},
                )
            )
        )
        return len(teammates) + (1 if team_alive else 0)


def _utc_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def _now() -> float:
    return time.time()


__all__ = ["QuotaLimits", "OwnerIdentity", "TeamQuotaPolicyService"]
