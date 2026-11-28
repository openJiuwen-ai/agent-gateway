# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Read-only inventory and first-batch Cron capability gates, without scheduling."""

from dataclasses import dataclass

from agent_gateway.cron.models import CronJob
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.store import FileCronJobStore


@dataclass(frozen=True)
class CronPreflightIssue:
    """An operational identifier and reason; never a task body or credential."""

    job_id: str | None
    slot: int | None
    reason: str
    blocking: bool


@dataclass(frozen=True)
class CronPreflightReport:
    total: int
    slot_counts: tuple[tuple[int, int], ...]
    issues: tuple[CronPreflightIssue, ...]

    @property
    def ready(self) -> bool:
        return not any(issue.blocking for issue in self.issues)


def unsupported_cron_reasons(job: CronJob, *, allow_login_credentials: bool = False) -> tuple[str, ...]:
    """Keep disabled records visible; recheck these gates before enabling/running.

    These are B1 capabilities, not a claim of model/project or AS validation.
    Later implementation batches must explicitly extend the execution contract.
    """
    reasons: list[str] = []
    if not job.user_id or job.user_id == 'default':
        reasons.append('legacy_owner')
    if job.id == 'proactive-tick-auto' or job.mode == 'proactive.tick':
        reasons.append('proactive')
    if job.credential_ref and not allow_login_credentials:
        reasons.append('credential_ref')
    if job.targets not in ('web', 'tui'):
        reasons.append('target')
    return tuple(reasons)


def require_supported_cron(job: CronJob, *, allow_login_credentials: bool = False) -> None:
    """Fail closed without echoing descriptions or credential references."""
    if unsupported_cron_reasons(job, allow_login_credentials=allow_login_credentials):
        raise ValueError('cron execution capability is not enabled')


async def inspect_cron_store(
    store: FileCronJobStore,
    ownership: CronOwnership,
    *,
    proactive_enabled: bool = False,
    allow_login_credentials: bool = False,
    allow_project_jobs: bool = True,
) -> CronPreflightReport:
    """Inspect the complete personal file without mutation or lock-file creation.

    Malformed records raise a sanitized snapshot error. Unsupported disabled
    records yield warnings, while enabled records block later Cron activation.
    The caller must also run model/project checks during lifecycle preparation.
    """
    if type(proactive_enabled) is not bool:
        raise ValueError('proactive_enabled must be boolean')
    jobs = await store.read_snapshot()
    counts = dict.fromkeys(ownership.manifest.slots, 0)
    issues: list[CronPreflightIssue] = []
    if proactive_enabled:
        issues.append(CronPreflightIssue(None, None, 'proactive_config', True))
    for job in jobs:
        slot = None
        if job.user_id and job.user_id != 'default':
            slot = ownership.owner_slot(job.user_id)
            counts[slot] += 1
        reasons = unsupported_cron_reasons(job, allow_login_credentials=allow_login_credentials)
        if not allow_project_jobs and job.project_id not in ("", "default", "default_code"):
            reasons += ('non_default_project',)
        issues.extend(CronPreflightIssue(job.id, slot, reason, job.enabled) for reason in reasons)
    return CronPreflightReport(len(jobs), tuple(counts.items()), tuple(issues))
