"""Cron run bookkeeping is released when a run reaches its lifecycle end.

每跑一轮 cron，调度器都会在 ``_runs`` / ``_run_tasks`` 里留一条记录（含整段
``result_text``）；企业版还会在 ``_claimed_run_ids`` 里留一个认领标记。这些
记录以前从不回收，进程跑得越久占用越高，所以按 run 的终态加保留窗口回收。
"""
from __future__ import annotations

import asyncio

import pytest

from agent_gateway.cron import scheduler as cron_scheduler_module
from agent_gateway.cron.models import CronRunState
from agent_gateway.cron.store import CronJobStore
from tests.unit_tests.gateway.test_cron_scheduler import (
    FakeAgentClient,
    FakeMessageHandler,
    _create_one_job,
    _TestableScheduler,
)


class _LifecycleScheduler(_TestableScheduler):
    def release_finished_runs(self):
        return self._release_finished_runs()

    async def claim_wake(self, job, run_id):
        return await self._claim_wake(job, run_id)

    async def on_push(self, job, run_id):
        return await self._on_push(job, run_id)

    @property
    def claimed_run_ids(self):
        return self._claimed_run_ids


def _make_scheduler(store, now_fn):
    return _LifecycleScheduler(
        store=store,
        agent_client=FakeAgentClient(),
        message_handler=FakeMessageHandler(),
        now_fn=now_fn,
    )


def _run_state(run_id: str, job_id: str, *, status: str, finished_at, result_text: str = "") -> CronRunState:
    return CronRunState(
        run_id=run_id,
        job_id=job_id,
        wake_at_iso="2026-06-09T08:55:00+08:00",
        push_at_iso="2026-06-09T09:00:00+08:00",
        job_name="job",
        targets="tui",
        session_id=None,
        chat_type=None,
        timezone="Asia/Shanghai",
        status=status,
        finished_at=finished_at,
        result_text=result_text,
    )


async def _done_task() -> asyncio.Task:
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    return task


def test_retention_window_covers_late_chat_file_delivery():
    # 迟到的 chat.file 靠 _find_active_run_by_exec_session 的 fallback 找回
    # 已结束的 run，所以终态记录不能立刻删，保留窗口至少几分钟。
    assert cron_scheduler_module._RUN_RETENTION_SECONDS >= 300.0


@pytest.mark.asyncio
async def test_finished_run_released_after_retention_window(tmp_path):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])

    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status="succeeded",
        finished_at=now["ts"] - cron_scheduler_module._RUN_RETENTION_SECONDS - 1,
        result_text="x" * 200,
    )
    svc.run_tasks[run_id] = await _done_task()
    svc.claimed_run_ids.add(run_id)

    svc.release_finished_runs()

    assert run_id not in svc.runs
    assert run_id not in svc.run_tasks
    assert run_id not in svc.claimed_run_ids


@pytest.mark.asyncio
async def test_finished_run_kept_inside_retention_window(tmp_path):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])

    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status="succeeded",
        finished_at=now["ts"] - 1.0,
        result_text="result",
    )
    svc.run_tasks[run_id] = await _done_task()
    svc.claimed_run_ids.add(run_id)

    svc.release_finished_runs()

    assert run_id in svc.runs
    assert run_id in svc.run_tasks
    assert run_id in svc.claimed_run_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "awaiting_approval", "pending"])
async def test_active_runs_are_never_released(tmp_path, status):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])

    run_id = f"{job.id}:1234"
    # 即使 finished_at 异常陈旧，非终态也不能被回收：审批暂停还可以续跑。
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status=status,
        finished_at=now["ts"] - cron_scheduler_module._RUN_RETENTION_SECONDS - 1,
    )

    svc.release_finished_runs()

    assert run_id in svc.runs


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "failed", "skipped", "cancelled"])
async def test_every_terminal_status_is_released(tmp_path, status):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])

    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status=status,
        finished_at=now["ts"] - cron_scheduler_module._RUN_RETENTION_SECONDS - 1,
    )

    svc.release_finished_runs()

    assert run_id not in svc.runs


@pytest.mark.asyncio
async def test_loop_releases_finished_runs_while_idle(tmp_path):
    # 没有事件时 _loop 每 5 秒轮询一次 store，回收挂在每轮开头，长时间空转
    # 的进程也不会把已结束的 run 一直留在内存里。
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])
    svc._store_poll_interval = 0.01
    svc._running = True

    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status="succeeded",
        finished_at=now["ts"] - cron_scheduler_module._RUN_RETENTION_SECONDS - 1,
    )

    loop_task = asyncio.create_task(svc._loop())
    try:
        for _ in range(50):
            await asyncio.sleep(0.01)
            if run_id not in svc.runs:
                break
    finally:
        svc._running = False
        loop_task.cancel()
        await asyncio.gather(loop_task, return_exceptions=True)

    assert run_id not in svc.runs


@pytest.mark.asyncio
async def test_repeated_runs_do_not_grow_bookkeeping(tmp_path):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])

    for turn in range(20):
        run_id = f"{job.id}:{1000 + turn}"
        svc.runs[run_id] = _run_state(
            run_id,
            job.id,
            status="succeeded",
            finished_at=now["ts"],
            result_text="result",
        )
        svc.run_tasks[run_id] = await _done_task()
        svc.claimed_run_ids.add(run_id)
        now["ts"] += 60.0

    now["ts"] += cron_scheduler_module._RUN_RETENTION_SECONDS
    svc.release_finished_runs()

    assert svc.runs == {}
    assert svc.run_tasks == {}
    assert svc.claimed_run_ids == set()


@pytest.mark.asyncio
async def test_personal_mode_does_not_record_run_claims(tmp_path):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    svc = _make_scheduler(store, lambda: now["ts"])
    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(run_id, job.id, status="pending", finished_at=None)

    assert await svc.claim_wake(job, run_id) is True

    # 个人版 _push_allowed 恒为 True，记录认领没有意义，只会随 run 数增长。
    assert run_id not in svc.claimed_run_ids


@pytest.mark.asyncio
async def test_final_push_releases_claim(tmp_path):
    now = {"ts": 1_000_000.0}
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_one_job(store)
    handler = FakeMessageHandler()
    svc = _LifecycleScheduler(
        store=store,
        agent_client=FakeAgentClient(),
        message_handler=handler,
        now_fn=lambda: now["ts"],
    )

    run_id = f"{job.id}:1234"
    svc.runs[run_id] = _run_state(
        run_id,
        job.id,
        status="succeeded",
        finished_at=now["ts"],
        result_text="result: 9am now",
    )
    svc.claimed_run_ids.add(run_id)

    await svc.on_push(job, run_id)

    # 终稿已经发出，这个 run 不会再推第二次，认领标记不必留到进程结束。
    assert run_id not in svc.claimed_run_ids
    assert len(handler.published) == 1
