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
STATUS_RELEASED = "released"
RESERVED_HOLD_MAX_SEC = 30 * 60
ABANDONED_HOLD_RELEASE_INTERVAL_SEC = 60.0


def release_abandoned_holds() -> None:
    """Release held team-create reservations left by a dead process.

    ``hold_reserved_create`` clears ``expires_at`` and stamps ``updated_at``.
    The resident stream refreshes that stamp every five minutes, so a live
    hold stays inside this window. A process that exits before
    ``team.runtime_ready`` leaves the row reserved with an empty expiry.
    Rows still reserved 30 minutes after the last stamp are released.
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

    async def _op() -> None:
        async def _tx(conn: Any) -> None:
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

        await run_tx(actor.handler, _tx)

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
