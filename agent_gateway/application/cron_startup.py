# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A lifecycle-owned barrier for legacy Cron migration and scheduler startup."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("gateway")


class CronStartup:
    def __init__(self, store: Any, scheduler: Any, controller: Any, client: Any, *, migrate_file: bool):
        self.store = store
        self.scheduler = scheduler
        self.controller = controller
        self.client = client
        self.migrate_file = migrate_file
        self._task: asyncio.Task | None = None
        self._closed = False

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("Cron startup is closed")
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="cron-startup")

    async def wait_ready(self) -> None:
        self.start()
        await asyncio.shield(self._task)

    async def _migrate(self) -> None:
        from agent_gateway.common.model_migration import migrate_legacy_cron_selections
        from agent_gateway.common.remote_model_catalog import RemoteModelCatalog, fetch_agent_model_rows

        jobs = await self.store.list_jobs(strict=True)
        if not any(job.model_selection is None and job.model_name for job in jobs):
            return
        # No controller/store lock is held across the remote request.
        rows = await fetch_agent_model_rows(
            agent_client=self.client, channel_id="gateway-cron-migration",
        )
        if rows is None:
            raise RuntimeError("AgentServer model catalog is unavailable")
        async with self.controller.mutation_lock:
            # The existing store rechecks the legacy name atomically (CAS).
            await migrate_legacy_cron_selections(
                self.store, catalog=RemoteModelCatalog(rows),
                user_id=None, remote_validated=True,
            )

    async def _run(self) -> None:
        while True:
            try:
                if self.migrate_file:
                    await self._migrate()
                await self.scheduler.start()
                logger.info("[App] deferred Cron scheduler ready")
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("[App] Cron startup deferred; retrying in 5s", exc_info=True)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
