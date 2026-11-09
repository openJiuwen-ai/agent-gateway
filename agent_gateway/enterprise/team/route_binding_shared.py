# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""主库上的团队路由绑定。upsert 在一条事务里完成，代替进程锁。"""

from __future__ import annotations

import time
from dataclasses import replace
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from agent_gateway.enterprise.db.team_store_actor import (
    fetch_all,
    fetch_one,
    get_team_store_actor,
    is_unique_violation,
    run_tx,
)
from agent_gateway.enterprise.team.route_binding_store import (
    ROUTE_STATUS_ACTIVE,
    TeamRouteBinding,
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _binding_from_row(row: dict[str, Any]) -> TeamRouteBinding:
    return TeamRouteBinding(
        team_session_id=row["team_session_id"],
        team_id=row["team_id"] or "",
        endpoint=row["endpoint"],
        agent_service_id=row["agent_service_id"] or "",
        route_request_id=row["route_request_id"] or "",
        owner_user_id=row["owner_user_id"] or "",
        owner_group_id=row["owner_group_id"] or "",
        owner_bot_id=row["owner_bot_id"] or "",
        gateway_id=row["gateway_id"] or "",
        status=row["status"],
        route_version=int(row["route_version"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        last_heartbeat_at=float(row["last_heartbeat_at"]),
    )


_INSERT = text(
    "INSERT INTO team_route_binding ("
    "team_session_id, team_id, endpoint, agent_service_id, route_request_id, "
    "owner_user_id, owner_group_id, owner_bot_id, gateway_id, status, route_version, "
    "created_at, updated_at, last_heartbeat_at"
    ") VALUES ("
    ":team_session_id, :team_id, :endpoint, :agent_service_id, :route_request_id, "
    ":owner_user_id, :owner_group_id, :owner_bot_id, :gateway_id, :status, :route_version, "
    ":created_at, :updated_at, :last_heartbeat_at)"
)

_UPDATE = text(
    "UPDATE team_route_binding SET team_id=:team_id, endpoint=:endpoint, "
    "agent_service_id=:agent_service_id, route_request_id=:route_request_id, "
    "owner_user_id=:owner_user_id, owner_group_id=:owner_group_id, owner_bot_id=:owner_bot_id, "
    "gateway_id=:gateway_id, status=:status, route_version=:route_version, "
    "updated_at=:updated_at, last_heartbeat_at=:last_heartbeat_at "
    "WHERE team_session_id=:team_session_id"
)


def _params(binding: TeamRouteBinding) -> dict[str, Any]:
    return {
        "team_session_id": binding.team_session_id,
        "team_id": binding.team_id,
        "endpoint": binding.endpoint,
        "agent_service_id": binding.agent_service_id,
        "route_request_id": binding.route_request_id,
        "owner_user_id": binding.owner_user_id,
        "owner_group_id": binding.owner_group_id,
        "owner_bot_id": binding.owner_bot_id,
        "gateway_id": binding.gateway_id,
        "status": binding.status,
        "route_version": binding.route_version,
        "created_at": binding.created_at,
        "updated_at": binding.updated_at,
        "last_heartbeat_at": binding.last_heartbeat_at,
    }


class SharedTeamRouteBindingStore:
    """``TeamRouteBindingStore`` backed by the shared team store."""

    def __init__(self) -> None:
        get_team_store_actor()

    def _handler(self) -> Any:
        return get_team_store_actor().handler

    def _run(self, coro_fn: Any) -> Any:
        return get_team_store_actor().call(coro_fn)

    def get_by_session(self, team_session_id: str) -> TeamRouteBinding | None:
        sid = _clean(team_session_id)
        if not sid:
            return None

        async def _op() -> dict[str, Any] | None:
            return await fetch_one(
                self._handler(),
                "SELECT * FROM team_route_binding WHERE team_session_id = :sid",
                {"sid": sid},
            )

        row = self._run(_op)
        return _binding_from_row(row) if row else None

    def get_by_team_id(self, team_id: str) -> TeamRouteBinding | None:
        tid = _clean(team_id)
        if not tid:
            return None

        async def _op() -> dict[str, Any] | None:
            return await fetch_one(
                self._handler(),
                "SELECT * FROM team_route_binding WHERE team_id = :tid "
                "ORDER BY updated_at DESC LIMIT 1",
                {"tid": tid},
            )

        row = self._run(_op)
        return _binding_from_row(row) if row else None

    def upsert(
        self,
        *,
        team_session_id: str,
        endpoint: str,
        agent_service_id: str = "",
        route_request_id: str = "",
        team_id: str | None = None,
        owner: tuple[str, str, str] | None = None,
        gateway_id: str | None = None,
        status: str = ROUTE_STATUS_ACTIVE,
    ) -> TeamRouteBinding:
        sid = _clean(team_session_id)
        ep = _clean(endpoint)
        if not sid or not ep:
            raise ValueError("team_session_id and endpoint are required")
        last_error: IntegrityError | None = None
        for _attempt in range(2):
            try:
                return self._upsert_once(
                    sid=sid,
                    ep=ep,
                    agent_service_id=agent_service_id,
                    route_request_id=route_request_id,
                    team_id=team_id,
                    owner=owner,
                    gateway_id=gateway_id,
                    status=status,
                )
            except IntegrityError as exc:
                if not is_unique_violation(exc):
                    raise
                last_error = exc
        raise last_error  # type: ignore[misc]

    def _upsert_once(
        self,
        *,
        sid: str,
        ep: str,
        agent_service_id: str,
        route_request_id: str,
        team_id: str | None,
        owner: tuple[str, str, str] | None,
        gateway_id: str | None,
        status: str,
    ) -> TeamRouteBinding:
        now = time.time()

        async def _op() -> TeamRouteBinding:
            async def _tx(conn: Any) -> TeamRouteBinding:
                result = await conn.execute(
                    text(
                        "SELECT * FROM team_route_binding "
                        "WHERE team_session_id = :sid FOR UPDATE"
                    ),
                    {"sid": sid},
                )
                existing_row = result.mappings().first()
                if existing_row is None:
                    binding = TeamRouteBinding(
                        team_session_id=sid,
                        endpoint=ep,
                        agent_service_id=_clean(agent_service_id),
                        route_request_id=_clean(route_request_id),
                        team_id=_clean(team_id),
                        owner_group_id=_clean(owner[0]) if owner else "",
                        owner_bot_id=_clean(owner[1]) if owner else "",
                        owner_user_id=_clean(owner[2]) if owner else "",
                        gateway_id=_clean(gateway_id),
                        status=status,
                        route_version=1,
                        created_at=now,
                        updated_at=now,
                        last_heartbeat_at=now,
                    )
                    await conn.execute(_INSERT, _params(binding))
                    return binding

                existing = _binding_from_row(dict(existing_row))
                endpoint_changed = existing.endpoint != ep or (
                    _clean(agent_service_id)
                    and existing.agent_service_id != _clean(agent_service_id)
                )
                set_owner = owner is not None and _clean(owner[2]) and not existing.has_owner
                binding = replace(
                    existing,
                    endpoint=ep,
                    agent_service_id=_clean(agent_service_id) or existing.agent_service_id,
                    route_request_id=_clean(route_request_id) or existing.route_request_id,
                    team_id=(
                        _clean(team_id)
                        if team_id is not None and _clean(team_id)
                        else existing.team_id
                    ),
                    owner_group_id=_clean(owner[0]) if set_owner else existing.owner_group_id,
                    owner_bot_id=_clean(owner[1]) if set_owner else existing.owner_bot_id,
                    owner_user_id=_clean(owner[2]) if set_owner else existing.owner_user_id,
                    gateway_id=_clean(gateway_id) if gateway_id is not None else existing.gateway_id,
                    status=status,
                    route_version=existing.route_version + (1 if endpoint_changed else 0),
                    updated_at=now,
                    last_heartbeat_at=now,
                )
                await conn.execute(_UPDATE, _params(binding))
                return binding

            return await run_tx(self._handler(), _tx)

        return self._run(_op)

    def touch(self, team_session_id: str) -> None:
        sid = _clean(team_session_id)
        if not sid:
            return
        now = time.time()

        async def _op() -> None:
            await run_tx(
                self._handler(),
                lambda conn: conn.execute(
                    text(
                        "UPDATE team_route_binding SET last_heartbeat_at = :now "
                        "WHERE team_session_id = :sid"
                    ),
                    {"now": now, "sid": sid},
                ),
            )

        self._run(_op)

    def mark_status(self, team_session_id: str, status: str) -> TeamRouteBinding | None:
        sid = _clean(team_session_id)
        if not sid:
            return None
        now = time.time()

        async def _op() -> int:
            result = await run_tx(
                self._handler(),
                lambda conn: conn.execute(
                    text(
                        "UPDATE team_route_binding SET status = :status, updated_at = :now "
                        "WHERE team_session_id = :sid"
                    ),
                    {"status": status, "now": now, "sid": sid},
                ),
            )
            return int(result.rowcount or 0)

        if self._run(_op) == 0:
            return None
        return self.get_by_session(sid)

    def delete(self, team_session_id: str) -> bool:
        sid = _clean(team_session_id)
        if not sid:
            return False

        async def _op() -> bool:
            result = await run_tx(
                self._handler(),
                lambda conn: conn.execute(
                    text("DELETE FROM team_route_binding WHERE team_session_id = :sid"),
                    {"sid": sid},
                ),
            )
            return int(result.rowcount or 0) > 0

        return bool(self._run(_op))

    def list_all(self) -> list[TeamRouteBinding]:
        async def _op() -> list[dict[str, Any]]:
            return await fetch_all(
                self._handler(),
                "SELECT * FROM team_route_binding ORDER BY updated_at DESC",
                {},
            )

        return [_binding_from_row(row) for row in self._run(_op)]
