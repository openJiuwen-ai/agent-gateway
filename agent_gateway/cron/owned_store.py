# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A slot-scoped personal file view, preserving the original shared file format."""

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from agent_gateway.common.work_mode import DEFAULT_WEB_WORK_MODE
from agent_gateway.cron.models import CronJob, is_valid_target_channel_id
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.preflight import require_supported_cron
from agent_gateway.cron.store import FileCronJobStore


class OwnedCronJobStore:
    """Limit scheduling ownership; callers must still enforce user permissions.

    Multiple users share a slot. This view is not an authentication boundary.
    Global project changes require the later explicit coordination service.
    """

    supports_watch = False

    def __init__(self, backend: FileCronJobStore, ownership: CronOwnership, *, slot: int, allow_login_credentials: bool = False) -> None:
        if not isinstance(backend, FileCronJobStore):
            raise ValueError('owned Cron requires the personal file backend')
        if type(ownership) is not CronOwnership:
            raise ValueError('explicit Cron ownership is required')
        if type(slot) is not int or slot not in ownership.manifest.slots:
            raise ValueError('Cron slot is absent from the configured manifest')
        self.backend = backend
        self.ownership = ownership
        self.slot = slot
        self.allow_login_credentials = allow_login_credentials

    @property
    def path(self) -> Path:
        return self.backend.path

    def _owns(self, job: CronJob) -> bool:
        if not job.user_id or job.user_id == 'default':
            return False
        return self.ownership.owner_slot(job.user_id) == self.slot

    def _require_owner(self, job: CronJob) -> None:
        if not self._owns(job):
            raise KeyError('job not found')

    async def list_jobs(self, *, strict: bool = False) -> list[CronJob]:
        # A malformed inventory must never become an apparently empty shard.
        return [job for job in await self.backend.read_snapshot() if self._owns(job)]

    async def get_job(self, job_id: str) -> CronJob | None:
        job_id = str(job_id or '').strip()
        return next((job for job in await self.list_jobs() if job.id == job_id), None)

    async def create_job(
        self,
        *,
        job_id: str | None = None,
        name: str,
        cron_expr: str,
        timezone: str,
        description: str,
        targets: str,
        enabled: bool = True,
        wake_offset_seconds: int | None = None,
        session_id: str | None = None,
        chat_type: str | None = None,
        mode: str | None = None,
        delete_after_run: bool | None = None,
        timeout_seconds: int | None = None,
        project_id: str = '',
        model_name: str | None = None,
        model_selection: dict[str, str] | None = None,
        mcp: list[str] | None = None,
        app_id: str = '',
        work_mode: str = DEFAULT_WEB_WORK_MODE,
        user_id: str = '',
        group_id: str = '',
        bot_id: str = '',
        credential_ref: str = '',
    ) -> CronJob:
        if self.ownership.owner_slot(user_id) != self.slot:
            raise ValueError('Cron owner belongs to a different slot')
        if not isinstance(targets, str) or not is_valid_target_channel_id(targets):
            raise ValueError('invalid Cron target')
        if type(enabled) is not bool:
            raise ValueError('Cron enabled must be boolean')
        job = self.backend.build_job(
            job_id=job_id, name=name, cron_expr=cron_expr, timezone=timezone,
            description=description, targets=targets, enabled=enabled,
            wake_offset_seconds=wake_offset_seconds, session_id=session_id,
            chat_type=chat_type, mode=mode, delete_after_run=delete_after_run,
            timeout_seconds=timeout_seconds, project_id=project_id,
            model_name=model_name, model_selection=model_selection, mcp=mcp,
            app_id=app_id, work_mode=work_mode, user_id=user_id, group_id=group_id,
            bot_id=bot_id, credential_ref=credential_ref,
        )
        if (job.group_id, job.bot_id, job.credential_ref) != (group_id, bot_id, credential_ref):
            raise ValueError('Cron identity and credential fields must be canonical')
        self.backend._parse_snapshot({'version': 1, 'jobs': [job.to_dict()]})
        if job.enabled:
            require_supported_cron(job, allow_login_credentials=self.allow_login_credentials)
        return await self.backend.insert_job(job)

    async def update_job(
        self,
        job_id: str,
        patch: dict[str, Any],
    ) -> CronJob:
        patch = dict(patch)
        if set(patch).intersection(('user_id', 'group_id', 'bot_id', 'id', 'job_id')):
            raise ValueError('Cron identity cannot change through ordinary update')
        if 'enabled' in patch and type(patch['enabled']) is not bool:
            raise ValueError('Cron enabled must be boolean')
        if 'targets' in patch and (
            not isinstance(patch['targets'], str) or not is_valid_target_channel_id(patch['targets'])
        ):
            raise ValueError('invalid Cron target')

        def guard(job: CronJob) -> None:
            self._require_owner(job)
            self.backend._parse_snapshot({'version': 1, 'jobs': [job.to_dict()]})
            # Disabling is allowed even for a legacy unsupported enabled job.
            if patch.get('enabled') is not False and job.enabled:
                require_supported_cron(job, allow_login_credentials=self.allow_login_credentials)

        return await self.backend.update_job(job_id, patch, guard=guard)

    async def delete_job(self, job_id: str, *, force: bool = False) -> bool:
        try:
            return await self.backend.delete_job(job_id, force=force, guard=self._require_owner)
        except KeyError:
            return False

    async def disable_project_jobs(self, project_id: str) -> None:
        raise NotImplementedError('global project changes require Cron coordination')

    async def get_revision(self) -> int:
        return await self.backend.get_revision()

    async def watch(self, callback: Callable[[], Awaitable[None]]) -> None:
        await self.backend.watch(callback)

    async def aclose(self) -> None:
        await self.backend.aclose()
