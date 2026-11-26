"""个人版 cron 认领的 run_id 必须有上限。

个人版每次唤醒都会把 run_id 记进 ``_claimed_run_ids``，而 ``_push_allowed``
在个人版直接放行、从不清理，长期运行的高频任务会让这个集合无界增长。
"""

import pytest

from agent_gateway.cron.scheduler import _CLAIMED_RUN_ID_LIMIT
from agent_gateway.cron.store import CronJobStore
from tests.unit_tests.gateway.test_cron_scheduler import (
    FakeAgentClient,
    FakeMessageHandler,
    _TestableScheduler,
    _create_one_job,
)


class _BoundedScheduler(_TestableScheduler):
    @property
    def claimed_run_ids(self):
        return self._claimed_run_ids


def _build(store):
    return _BoundedScheduler(
        store=store,
        agent_client=FakeAgentClient(),
        message_handler=FakeMessageHandler(),
    )


@pytest.mark.asyncio
async def test_personal_claims_stay_bounded(tmp_path, monkeypatch):
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    store = CronJobStore(path=tmp_path / "jobs.json")
    svc = _build(store)
    job = await _create_one_job(store, name="tick")

    total = _CLAIMED_RUN_ID_LIMIT * 3
    for i in range(total):
        assert await svc.claim_wake(job, f"{job.id}:{i}") is True

    assert len(svc.claimed_run_ids) == _CLAIMED_RUN_ID_LIMIT
    assert f"{job.id}:0" not in svc.claimed_run_ids
    assert f"{job.id}:{total - 1}" in svc.claimed_run_ids


@pytest.mark.asyncio
async def test_repeated_claim_refreshes_position(tmp_path, monkeypatch):
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    store = CronJobStore(path=tmp_path / "jobs.json")
    svc = _build(store)
    job = await _create_one_job(store, name="refresh")

    first = f"{job.id}:keep"
    await svc.claim_wake(job, first)
    for i in range(_CLAIMED_RUN_ID_LIMIT - 1):
        await svc.claim_wake(job, f"{job.id}:{i}")
    # 复用后应排到队尾，再挤掉一条也不会把它淘汰。
    await svc.claim_wake(job, first)
    await svc.claim_wake(job, f"{job.id}:extra")

    assert len(svc.claimed_run_ids) == _CLAIMED_RUN_ID_LIMIT
    assert first in svc.claimed_run_ids
    assert f"{job.id}:0" not in svc.claimed_run_ids


@pytest.mark.asyncio
async def test_enterprise_claim_success_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    async def _claim(job, fire_at):
        return True

    monkeypatch.setattr("agent_gateway.cron.db_schedule.claim_periodic_run", _claim)
    store = CronJobStore(path=tmp_path / "jobs.json")
    svc = _build(store)
    job = await _create_one_job(store, name="shared")
    run_id = f"{job.id}:1700000000"

    assert await svc.claim_wake(job, run_id) is True
    assert run_id in svc.claimed_run_ids
    assert svc.push_allowed(run_id) is True
