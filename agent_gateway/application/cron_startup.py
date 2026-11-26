# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A lifecycle-owned barrier for legacy Cron migration and scheduler startup."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agent_gateway.lifecycle_errors import LifecycleError

logger = logging.getLogger("gateway")
_MIGRATION_ATTEMPTS = 3
_CATALOG_TIMEOUT_SECONDS = 3.0
_RETRY_DELAY_SECONDS = 1.0
_READY_TIMEOUT_SECONDS = 5.0


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
            raise LifecycleError("SERVICE_UNAVAILABLE", "Cron startup is closed")
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="cron-startup")
            self._task.add_done_callback(self._observe_completion)

    @staticmethod
    def _observe_completion(task: asyncio.Task) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("[App] Cron scheduler startup failed", exc_info=(type(error), error, error.__traceback__))

    async def wait_ready(self) -> None:
        self.start()
        try:
            # A timed-out/disconnected caller must not cancel shared startup or
            # an atomic migration write that another caller is waiting for.
            await asyncio.wait_for(asyncio.shield(self._task), _READY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            raise LifecycleError("SERVICE_UNAVAILABLE", "Cron is still initializing; retry shortly") from exc
        except asyncio.CancelledError as exc:
            if self._closed:
                raise LifecycleError("SERVICE_UNAVAILABLE", "Cron is shutting down") from exc
            raise
        except Exception as exc:
            raise LifecycleError("SERVICE_UNAVAILABLE", "Cron startup failed; restart Gateway to retry") from exc
        if self._closed:
            raise LifecycleError("SERVICE_UNAVAILABLE", "Cron is shutting down")

    async def _migrate(self) -> None:
        from agent_gateway.common.model_migration import migrate_legacy_cron_selections
        from agent_gateway.common.remote_model_catalog import RemoteModelCatalog, fetch_agent_model_rows

        jobs = await self.store.list_jobs(strict=True)
        if not any(job.model_selection is None and job.model_name for job in jobs):
            return
        # No controller/store lock is held across the remote request.
        rows = await asyncio.wait_for(
            fetch_agent_model_rows(agent_client=self.client, channel_id="gateway-cron-migration"),
            _CATALOG_TIMEOUT_SECONDS,
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
        for attempt in range(_MIGRATION_ATTEMPTS if self.migrate_file else 0):
            try:
                await self._migrate()
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt + 1 == _MIGRATION_ATTEMPTS:
                    # Preserve develop's best-effort migration contract. The
                    # scheduler already forwards legacy model_name unchanged;
                    # failed migration must not disable all existing jobs.
                    logger.warning("[App] Cron model-selection migration deferred; retaining legacy model names", exc_info=True)
                else:
                    await asyncio.sleep(_RETRY_DELAY_SECONDS)
        # Scheduler initialization is never part of the migration retry loop.
        # A partial failure is reported to callers, not retried on a live graph.
        await self.scheduler.start()
        logger.info("[App] deferred Cron scheduler ready")

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
