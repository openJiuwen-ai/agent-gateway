"""Gateway-owned cron path, upgrade, and scheduler diagnostics."""

from __future__ import annotations

import contextlib
import json
import logging

import pytest

from agent_gateway.common.paths import get_cron_jobs_path, get_legacy_cron_jobs_path


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_user_workspace_dir", lambda: tmp_path
    )
    return tmp_path


def test_cron_uses_dedicated_directory(workspace):
    gateway_dir = workspace / "gateway"
    gateway_dir.mkdir(parents=True)
    (gateway_dir / "cron_jobs.json").write_text("{}", encoding="utf-8")
    assert get_cron_jobs_path() == gateway_dir / "cron" / "cron_jobs.json"


_JOB = {
    "id": "job-1",
    "name": "Example job",
    "enabled": True,
    "expired": False,
    "cron_expr": "0 0 8 * * ? *",
    "timezone": "UTC",
    "description": "Example scheduled job.",
    "targets": "web",
    "session_id": "web_session_1",
    "mode": "agent",
}


def _write_job(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 1, "jobs": [_JOB]}), encoding="utf-8")
    return path


@contextlib.contextmanager
def _scheduler_logs(level=logging.INFO):
    """Collect what the scheduler logs, from the logger that emits it.

    ``caplog`` attaches to the root logger, and ``setup_logger`` sets
    ``propagate = False`` on ``jiuwenswarm`` when it is imported, so these
    records only reach the root on pytest 9.1+, which also attaches to
    non-propagating loggers. Attaching here holds on every version.
    """
    logger = logging.getLogger("agent_gateway.cron.scheduler")
    records: list[logging.LogRecord] = []

    class _Collect(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Collect(level)
    original = logger.level
    logger.setLevel(level)
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original)


def _scheduler(store):
    """Minimal scheduler for the logging paths: reload() touches only the store
    and its own bookkeeping, so the client and handler are never called."""
    from agent_gateway.cron.scheduler import CronSchedulerService

    return CronSchedulerService(store=store, agent_client=None, message_handler=None)


@pytest.mark.asyncio
async def test_scheduler_only_uses_authoritative_file_after_upgrade(workspace):
    from agent_gateway.runtime.cron.factory import create_gateway_cron_store

    old = _write_job(get_legacy_cron_jobs_path())
    store = await create_gateway_cron_store({})
    scheduler = _scheduler(store)
    await scheduler.reload()
    assert set(scheduler._jobs) == {_JOB["id"]}

    old.write_text('{"jobs":[]}', encoding="utf-8")
    await scheduler.reload()
    assert set(scheduler._jobs) == {_JOB["id"]}


@pytest.mark.asyncio
async def test_reload_reports_how_many_jobs_it_loaded(workspace):
    """Say what was loaded and from where: "scheduler started" alone reads the
    same with one job or none."""
    from agent_gateway.cron.store import CronJobStore

    path = _write_job(workspace / "profile" / "agent" / "home" / "cron_jobs.json")
    with _scheduler_logs() as records:
        await _scheduler(CronJobStore(path=path)).reload()

    messages = [r.getMessage() for r in records]
    assert any("loaded 1 job(s)" in m and str(path) in m for m in messages), messages


@pytest.mark.asyncio
async def test_reload_warns_when_it_loads_nothing(workspace):
    """Zero jobs is a warning, not silence: it is the symptom of the bug."""
    from agent_gateway.cron.store import CronJobStore

    missing = workspace / "profile" / "agent" / "home" / "cron_jobs.json"
    with _scheduler_logs(logging.DEBUG) as records:
        await _scheduler(CronJobStore(path=missing)).reload()

    warnings = [r.getMessage() for r in records if r.levelname == "WARNING"]
    assert any("loaded 0 jobs" in m for m in warnings), warnings


@pytest.mark.asyncio
async def test_a_store_vanishing_under_us_warns_by_name(workspace):
    """Losing a populated store is not routine housekeeping: the old INFO line
    read the same whether the file was edited or had vanished with the schedules."""
    from agent_gateway.cron.store import CronJobStore

    path = _write_job(workspace / "profile" / "agent" / "home" / "cron_jobs.json")
    scheduler = _scheduler(CronJobStore(path=path))
    await scheduler.reload()
    await scheduler._sync_store_revision()

    path.unlink()
    with _scheduler_logs() as records:
        assert await scheduler._check_store_changed() is True

    warnings = [r.getMessage() for r in records if r.levelname == "WARNING"]
    assert any("disappeared while holding 1 job(s)" in m for m in warnings), warnings
