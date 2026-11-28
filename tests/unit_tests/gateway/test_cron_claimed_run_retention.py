"""Cron 认领集合必须按保留期回收，否则每个任务每次到点各留一条。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.cron.scheduler import CronSchedulerService
from agent_gateway.cron.store import CronJobStore


def _scheduler(tmp_path, now: list[float]) -> CronSchedulerService:
    return CronSchedulerService(
        store=CronJobStore(path=tmp_path / "cron_jobs.json"),
        agent_client=SimpleNamespace(),
        message_handler=SimpleNamespace(),
        now_fn=lambda: now[0],
    )


def test_same_run_stays_pushable_and_other_runs_are_not(tmp_path, monkeypatch) -> None:
    """认领只放行本机抢到的那一趟，行为与改前一致。"""
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    now = [1000.0]
    scheduler = _scheduler(tmp_path, now)

    scheduler._claim_run_id("job-1:1000")

    assert scheduler._push_allowed("job-1:1000") is True
    assert scheduler._push_allowed("job-1:1001") is False


def test_claims_past_retention_are_reclaimed(tmp_path, monkeypatch) -> None:
    """保留期之外再认领，旧 run_id 必须被丢掉，集合不随到点次数增长。"""
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    now = [1000.0]
    scheduler = _scheduler(tmp_path, now)
    scheduler._claim_run_id("job-1:1000")

    now[0] = 1000.0 + scheduler._CLAIM_RETENTION_SECONDS + 1
    scheduler._claim_run_id("job-1:2000")

    assert list(scheduler._claimed_run_ids) == ["job-1:2000"]
    assert scheduler._push_allowed("job-1:1000") is False


def test_claims_inside_retention_are_kept(tmp_path, monkeypatch) -> None:
    """保留期内的一趟仍要能推，补发晚一点也不丢。"""
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    now = [1000.0]
    scheduler = _scheduler(tmp_path, now)
    scheduler._claim_run_id("job-1:1000")

    now[0] = 1000.0 + scheduler._CLAIM_RETENTION_SECONDS - 1
    scheduler._claim_run_id("job-1:2000")

    assert scheduler._push_allowed("job-1:1000") is True


def test_repeated_claims_of_one_run_do_not_grow_the_table(tmp_path) -> None:
    """同一 run_id 重复认领只留一条。"""
    now = [1000.0]
    scheduler = _scheduler(tmp_path, now)

    for _ in range(5):
        scheduler._claim_run_id("job-1:1000")

    assert list(scheduler._claimed_run_ids) == ["job-1:1000"]


@pytest.mark.asyncio
async def test_personal_claim_wake_records_the_run(tmp_path, monkeypatch) -> None:
    """个人版不查库也要留下认领记录，行为与改前一致。"""
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    now = [1000.0]
    scheduler = _scheduler(tmp_path, now)
    job = await scheduler._store.create_job(
        name="home",
        cron_expr="0 0 9 * * ? *",
        timezone="Asia/Shanghai",
        description="reminder",
        targets="tui",
        user_id="",
    )
    run_id = f"{job.id}:1700000000"

    assert await scheduler._claim_wake(job, run_id) is True
    assert scheduler._push_allowed(run_id) is True
    assert len(scheduler._claimed_run_ids) == 1
