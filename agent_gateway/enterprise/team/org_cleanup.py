# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Identity-change cascade: stop teams that lost their owning org or user.

The Gateway owns the shared team tables and the quota projection, so the
cleanup runs here. The team runtime lives in an AgentServer; the Gateway only
forwards ``team.stop`` through the runtime-routed client and falls back to
marking the entity stopped and releasing its leases when delivery fails.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from sqlalchemy import text

from agent_gateway.enterprise.db.team_store_actor import (
    get_team_store_actor,
    list_record_dicts,
    run_team_store,
    run_tx,
    use_shared_team_store,
)

logger = logging.getLogger(__name__)

TEAM_ACTIVE_STATUSES: frozenset[str] = frozenset(
    {"creating", "running", "paused", "stopping"}
)
TEAM_STATUS_STOPPED = "stopped"
STATUS_RELEASED = "released"

#: Enterprise team-control method; registered by the startup flow but kept as a
#: literal so the cleanup does not depend on protocol registration order.
TEAM_STOP_METHOD = "team.stop"

_ENTITY_TABLE = "team_entity"
_LEASE_TABLE = "team_quota_lease"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _require_actor() -> Any:
    if not use_shared_team_store():
        raise RuntimeError("team cleanup requires the enterprise database")
    return get_team_store_actor()


def _list_active_sync(owner_column: str, owner_id: str) -> list[dict[str, Any]]:
    actor = _require_actor()
    rows = actor.call(
        lambda: list_record_dicts(
            actor.handler, _ENTITY_TABLE, filters={owner_column: owner_id}
        )
    )
    return [
        row
        for row in rows
        if _clean(row.get("status")) in TEAM_ACTIVE_STATUSES
        and _clean(row.get("team_id"))
    ]


def _mark_stopped_sync(team_id: str) -> None:
    """Fallback when the runtime cannot be reached: stop the row and free quota."""
    actor = _require_actor()
    now = time.time()

    async def _op() -> None:
        async def _tx(conn: Any) -> None:
            await conn.execute(
                text(
                    "UPDATE team_entity SET status=:status, updated_at=:now "
                    "WHERE team_id=:tid"
                ),
                {"status": TEAM_STATUS_STOPPED, "now": now, "tid": team_id},
            )
            await conn.execute(
                text(
                    "UPDATE team_quota_lease SET status=:rel, updated_at=:now "
                    "WHERE team_id=:tid AND status<>:rel"
                ),
                {"rel": STATUS_RELEASED, "now": now, "tid": team_id},
            )

        await run_tx(actor.handler, _tx)

    actor.call(_op)


async def stop_team_runtime(
    team_id: str,
    *,
    team_session_id: str = "",
    owner_user_id: str = "",
    owner_group_id: str = "",
    owner_bot_id: str = "",
) -> bool:
    """Ask the AgentServer hosting ``team_id`` to run ``team.stop``.

    ``session_id`` routes the request to the leader pod; the owner triple feeds
    the enterprise admission check. Any failure returns ``False`` so the caller
    can fall back to the database-only stop.
    """
    tid = _clean(team_id)
    if not tid:
        return False
    sid = _clean(team_session_id)
    user_id = _clean(owner_user_id)
    group_id = _clean(owner_group_id)
    bot_id = _clean(owner_bot_id)
    try:
        from e2a.models import E2AEnvelope

        from agent_gateway.extensions.registry import ExtensionRegistry

        client = ExtensionRegistry.get_instance().get_agent_server_client()
        if client is None:
            return False
        envelope = E2AEnvelope(
            request_id=f"org-stop-{uuid.uuid4().hex}",
            channel="web",
            session_id=sid or None,
            user_id=user_id or None,
            method=TEAM_STOP_METHOD,
            params={"team_id": tid, "session_id": sid},
            channel_context={
                "user_id": user_id,
                "routing": {"group_id": group_id, "bot_id": bot_id},
            },
        )
        response = await client.send_request(envelope)
        return bool(getattr(response, "ok", False))
    except Exception:  # noqa: BLE001 - caller falls back to the database stop
        logger.warning(
            "[OrgCleanup] runtime stop failed: team_id=%s", tid, exc_info=True
        )
        return False


async def _stop_entities(entities: list[dict[str, Any]]) -> list[str]:
    stopped: list[str] = []
    for entity in entities:
        team_id = _clean(entity.get("team_id"))
        delivered = await stop_team_runtime(
            team_id,
            team_session_id=_clean(entity.get("team_session_id")),
            owner_user_id=_clean(entity.get("owner_user_id")),
            owner_group_id=_clean(entity.get("owner_group_id")),
            owner_bot_id=_clean(entity.get("owner_bot_id")),
        )
        if not delivered:
            await run_team_store(_mark_stopped_sync, team_id)
        stopped.append(team_id)
    return stopped


async def stop_teams_for_org(group_id: str) -> list[str]:
    """Stop every active team owned by ``group_id`` and release its quota."""
    gid = _clean(group_id)
    if not gid:
        return []
    entities = await run_team_store(_list_active_sync, "owner_group_id", gid)
    return await _stop_entities(entities)


async def stop_teams_for_user(
    user_id: str, keep_group_ids: list[str] | None = None
) -> list[str]:
    """Stop the user's active teams that belong outside ``keep_group_ids``.

    ``keep_group_ids`` is the user's current org list. Teams whose owner org is
    not in it are stopped; an empty list means the user was deleted, so every
    active team of that user is stopped.
    """
    uid = _clean(user_id)
    if not uid:
        return []
    keep = {_clean(gid) for gid in (keep_group_ids or []) if _clean(gid)}
    entities = await run_team_store(_list_active_sync, "owner_user_id", uid)
    targets = [
        entity
        for entity in entities
        if _clean(entity.get("owner_group_id")) not in keep
    ]
    return await _stop_entities(targets)


__all__ = ["stop_team_runtime", "stop_teams_for_org", "stop_teams_for_user"]
