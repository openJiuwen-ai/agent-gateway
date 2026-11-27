# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Release team-create holds left behind when a process exits before ready."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

KIND_TEAM_CREATE = "team_create"
STATUS_RESERVED = "reserved"
STATUS_COMMITTED = "committed"
STATUS_RELEASED = "released"
# ``team_entity.status`` values that still own quota, mirroring AgentServer's
# ``TEAM_ACTIVE_STATUSES``. ``stopped`` and ``deleted`` rows must not keep an
# orphaned lease alive: ``team.stop`` intentionally leaves the entity row behind
# with a stopped status.
TEAM_ACTIVE_STATUSES = ("creating", "running", "paused", "stopping")
RESERVED_HOLD_MAX_SEC = 30 * 60
# Commit and entity insert happen together, but a maintenance transaction can
# still read a snapshot taken just before that commit. Ignoring young rows
# removes the race and keeps a freshly created team from losing its occupancy.
COMMITTED_ORPHAN_MIN_AGE_SEC = 5 * 60
ABANDONED_HOLD_RELEASE_INTERVAL_SEC = 60.0


def release_abandoned_holds() -> None:
    """Release abandoned team-create holds and orphaned committed leases.

    ``hold_reserved_create`` clears ``expires_at`` and stamps ``updated_at``.
    The resident stream refreshes that stamp every five minutes, so a live
    hold stays inside this window. A process that exits before
    ``team.runtime_ready`` leaves the row reserved with an empty expiry.
    Rows still reserved 30 minutes after the last stamp are released.

    A committed lease owns quota until something releases it. When the team
    entity is gone (Session deleted while another Session of the team failed,
    or an in-band release that never ran), the lease is orphaned. Such rows are
    released here so the user's occupancy converges without waiting for an
    AgentServer restart. A row still counts as owned while its entity exists
    with an active status; a stopped or deleted entity no longer occupies
    quota. Rows younger than ``COMMITTED_ORPHAN_MIN_AGE_SEC`` are left alone so
    a concurrent commit cannot be mistaken for an orphan.

    The two statements run in separate transactions: a database that has the
    lease table but not yet ``team_entity`` must not disable the reserved-hold
    recovery that has always worked.

    Personal edition returns before the shared-store check.
    """
    from agent_gateway.enterprise.db.team_store_actor import (
        get_team_store_actor,
        run_tx,
        use_shared_team_store,
    )
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return
    if not use_shared_team_store():
        return
    now = time.time()
    actor = get_team_store_actor()

    async def _release_reserved(conn: Any) -> None:
        await conn.execute(
            text(
                "UPDATE team_quota_lease SET status=:rel, updated_at=:now "
                "WHERE kind=:kind AND status=:res AND expires_at IS NULL "
                "AND updated_at<=:cutoff"
            ),
            {
                "rel": STATUS_RELEASED,
                "now": now,
                "kind": KIND_TEAM_CREATE,
                "res": STATUS_RESERVED,
                "cutoff": now - RESERVED_HOLD_MAX_SEC,
            },
        )

    async def _release_committed(conn: Any) -> None:
        # ``team_entity`` and ``team_quota_lease`` share the Gateway DB, so the
        # existence check reads the same control-plane state the AgentServer
        # writes.
        active_in = ", ".join(
            f":s{index}" for index in range(len(TEAM_ACTIVE_STATUSES))
        )
        params: dict[str, Any] = {
            "rel": STATUS_RELEASED,
            "now": now,
            "kind": KIND_TEAM_CREATE,
            "com": STATUS_COMMITTED,
            "age_cutoff": now - COMMITTED_ORPHAN_MIN_AGE_SEC,
        }
        params.update(
            {f"s{index}": status for index, status in enumerate(TEAM_ACTIVE_STATUSES)}
        )
        await conn.execute(
            text(
                "UPDATE team_quota_lease SET status=:rel, updated_at=:now "
                "WHERE kind=:kind AND status=:com AND team_id<>'' "
                "AND updated_at<=:age_cutoff "
                "AND NOT EXISTS ("
                "SELECT 1 FROM team_entity e "
                "WHERE e.team_id=team_quota_lease.team_id "
                f"AND e.status IN ({active_in})"
                ")"
            ),
            params,
        )

    async def _op() -> None:
        await run_tx(actor.handler, _release_reserved)
        try:
            await run_tx(actor.handler, _release_committed)
        except Exception:
            # An older shared DB may not have ``team_entity`` yet. Keep the
            # reserved recovery alive and retry the committed sweep next round.
            logger.warning(
                "[TeamQuota] committed orphan release skipped", exc_info=True
            )

    actor.call(_op)


async def run_abandoned_hold_release_loop() -> None:
    """Release crash-left holds immediately, then every 60 seconds.

    Personal edition returns before any database type check. An unsupported
    enterprise database type is logged once and the loop exits. A later
    database error is logged and the loop retries.
    """
    from agent_gateway.enterprise.db.team_store_actor import (
        run_team_store,
        use_shared_team_store,
    )
    from agent_gateway.enterprise.edition import is_enterprise

    try:
        if not is_enterprise():
            return
        if not use_shared_team_store():
            return
    except Exception:
        logger.exception("[TeamQuota] abandoned hold release disabled")
        return
    while True:
        try:
            await run_team_store(release_abandoned_holds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[TeamQuota] abandoned hold release failed")
        await asyncio.sleep(ABANDONED_HOLD_RELEASE_INTERVAL_SEC)
