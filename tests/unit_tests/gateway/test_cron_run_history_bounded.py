"""cron 运行历史必须有界。

每次调度都会生成一条 ``CronRunState``（带完整 ``result_text``）和一个 asyncio
Task；两者原先都没有回收路径，长期运行会随调度次数线性增长。
"""

from agent_gateway.cron.models import CronRunState
from agent_gateway.cron.scheduler import CronSchedulerService
from agent_gateway.cron.store import CronJobStore


class _DoneTask:
    def done(self) -> bool:
        return True


class _Scheduler(CronSchedulerService):
    @property
    def runs(self):
        return self._runs

    @property
    def run_tasks(self):
        return self._run_tasks

    def prune(self) -> int:
        return self._prune_finished_runs()


def _build(tmp_path, now: float) -> _Scheduler:
    return _Scheduler(
        store=CronJobStore(path=tmp_path / "jobs.json"),
        agent_client=object(),
        message_handler=object(),
        now_fn=lambda: now,
    )


def _state(
    run_id: str,
    *,
    status: str = "succeeded",
    finished_at: float | None = 1000.0,
    pushed_final: bool = False,
    result_text: str | None = "done",
) -> CronRunState:
    return CronRunState(
        run_id=run_id,
        job_id="job",
        wake_at_iso="",
        push_at_iso="",
        status=status,
        finished_at=finished_at,
        pushed_final=pushed_final,
        result_text=result_text,
    )


def _fill(scheduler: _Scheduler, run_ids) -> None:
    for run_id in run_ids:
        scheduler.runs[run_id] = _state(run_id)
        scheduler.run_tasks[run_id] = _DoneTask()


def test_finished_runs_are_capped_and_oldest_dropped(tmp_path):
    now = 2000.0
    scheduler = _build(tmp_path, now)
    limit = scheduler._RUN_HISTORY_LIMIT
    run_ids = [f"run-{i}" for i in range(limit + 10)]
    _fill(scheduler, run_ids)

    assert scheduler.prune() == 10

    assert len(scheduler.runs) == limit
    assert run_ids[0] not in scheduler.runs
    assert run_ids[9] not in scheduler.runs
    assert run_ids[10] in scheduler.runs
    assert run_ids[-1] in scheduler.runs
    assert len(scheduler.run_tasks) == limit
    assert run_ids[0] not in scheduler.run_tasks


def test_active_runs_are_never_dropped(tmp_path):
    now = 2000.0
    scheduler = _build(tmp_path, now)
    limit = scheduler._RUN_HISTORY_LIMIT
    statuses = ("pending", "running", "awaiting_approval")
    for i in range(limit + 25):
        run_id = f"run-{i}"
        scheduler.runs[run_id] = _state(
            run_id,
            status=statuses[i % len(statuses)],
            finished_at=None,
            result_text=None,
        )
        scheduler.run_tasks[run_id] = _DoneTask()

    assert scheduler.prune() == 0
    assert len(scheduler.runs) == limit + 25
    assert len(scheduler.run_tasks) == limit + 25


def test_expired_unpushed_run_is_reclaimed(tmp_path):
    now = 100000.0
    scheduler = _build(tmp_path, now)
    ttl = scheduler._RUN_RETENTION_SECONDS
    scheduler.runs["stale"] = _state("stale", finished_at=now - ttl - 1)
    scheduler.run_tasks["stale"] = _DoneTask()

    assert scheduler.prune() == 1
    assert "stale" not in scheduler.runs
    assert "stale" not in scheduler.run_tasks


def test_recent_unpushed_run_keeps_result_for_push(tmp_path):
    now = 100000.0
    scheduler = _build(tmp_path, now)
    scheduler.runs["fresh"] = _state("fresh", finished_at=now - 5)

    assert scheduler.prune() == 0
    assert scheduler.runs["fresh"].result_text == "done"
