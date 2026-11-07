# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Release team-create reservations left behind when a process exits early."""

from __future__ import annotations

import asyncio
import logging
import time

from sqlalchemy import text

from agent_gateway.enterprise.db.team_mysql_actor import (
    get_team_mysql_actor,
    run_team_store,
    run_tx,
    use_shared_team_store,
)
from agent_gateway.enterprise.edition import is_enterprise
from agent_gateway.enterprise.team.quota_policy import KIND_TEAM_CREATE, STATUS_RESERVED

logger = logging.getLogger(__name__)

RESERVED_HOLD_MAX_SEC = 30 * 60
ABANDONED_HOLD_RELEASE_INTERVAL_SEC = 60.0
STATUS_RELEASED = "released"


def release_abandoned_holds() -> None:
    """Release held team-create reservations left by a dead process.

    A still-reserved ``team_create`` row with no expiry, last touched more
    than 30 minutes ago, is released. Personal edition returns first.
    """
    if not is_enterprise():
        return
    if not use_shared_team_store():
        return
    now = time.time()
    actor = get_team_mysql_actor()

    async def _op() -> None:
        async def _tx(conn) -> None:
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

    Personal edition returns before any database type check.
    """
    if not is_enterprise():
        return
    if not use_shared_team_store():
        return
    while True:
        try:
            await run_team_store(release_abandoned_holds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[TeamQuota] abandoned hold release failed")
        await asyncio.sleep(ABANDONED_HOLD_RELEASE_INTERVAL_SEC)
