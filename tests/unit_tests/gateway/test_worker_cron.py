# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Controlled Worker Cron runtime preparation and execution gates."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from e2a.agent_models import AgentResponse
from e2a.message import ReqMethod

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.models import CronRunState
from agent_gateway.cron.store import FileCronJobStore


async def _job(store: FileCronJobStore, job_id: str, **fields: Any) -> None:
    fields.setdefault("user_id", "alice")
    await store.create_job(job_id=job_id, name=job_id, cron_expr="0 9 * * *", timezone="UTC",
                           description=job_id, targets="web", **fields)


class _ModelClient:
    """Answers models.list with the rows the tip controller resolves against."""

    def __init__(self) -> None:
        self.requests: list[Any] = []

    async def send_request(self, envelope: Any, **kwargs: Any) -> AgentResponse:
        self.requests.append(envelope)
        if envelope.method == ReqMethod.MODELS_LIST.value:
            payload: dict[str, Any] = {
                "models": [
                    {"model_id": "mdl_remote", "model_name": "remote-model", "alias": "remote-alias",
                     "model_provider": "OpenAI"},
                    {"model_id": "mdl_self", "model_name": "self-configured", "model_provider": "OpenAI"},
                ],
                "active_model": "remote-model",
            }
        else:
            payload = {"echo_method": envelope.method}
        return AgentResponse(request_id=envelope.request_id, channel_id=envelope.channel,
                             ok=True, payload=payload)


class _FakeResolver:
    instances: list["_FakeResolver"] = []

    def __init__(self, agent_client: Any) -> None:
        self.agent_client = agent_client
        self.validated: list[Any] = []
        self.fail = False
        self.resolved: list[Any] = []
        self.instances.append(self)

    async def __call__(
        self,
        raw: Any,
        *,
        model_selection: Any = None,
        user_id: str | None = None,
    ) -> tuple[str | None, str]:
        del user_id
        self.resolved.append((raw, model_selection))
        return "remote-model", "config"

    async def validate_job(self, job: Any) -> None:
        if self.fail:
            raise ValueError("model unavailable")
        self.validated.append(job)


async def _build_runtime(
    *,
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
    message_handler: Any | None = None,
) -> Any:
    from agent_gateway.application.worker_cron import build_worker_cron

    if message_handler is None:
        message_handler = SimpleNamespace(
            set_cron_controller=lambda controller: setattr(
                message_handler, "controller", controller
            ),
            set_cron_tool_push_enabled=lambda enabled: setattr(
                message_handler, "tool_push_enabled", enabled
            ),
        )
    return await build_worker_cron(
        manifest=WorkerManifest((0,)),
        slot_id=0,
        agent_client=_ModelClient(),
        message_handler=message_handler,
        config={"gateway": {"cron": {"store_backend": "file"}}},
    )


@pytest.fixture
def cron_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "agent" / "home" / "cron_jobs.json"
    monkeypatch.setattr(
        "agent_gateway.common.cron_settings.get_cron_jobs_path",
        lambda: path,
    )
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_cron_jobs_path",
        lambda: path,
    )
    return path


@pytest.fixture
def fake_resolver(monkeypatch: pytest.MonkeyPatch) -> type[_FakeResolver]:
    _FakeResolver.instances = []
    monkeypatch.setattr(
        "agent_gateway.application.worker_cron.WorkerCronModelResolver",
        _FakeResolver,
    )
    return _FakeResolver


async def test_build_prepares_owned_slot_without_starting_scheduler(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import build_worker_cron

    store = FileCronJobStore(path=cron_path)
    await _job(store, "enabled", model_name="model")
    await _job(store, "disabled", enabled=False, model_name="login-model", credential_ref="login:ref")
    message_handler = SimpleNamespace(
        set_cron_controller=lambda controller: setattr(
            message_handler, "controller", controller
        ),
        set_cron_tool_push_enabled=lambda enabled: setattr(
            message_handler, "tool_push_enabled", enabled
        ),
    )

    runtime = await build_worker_cron(
        manifest=WorkerManifest((0,)),
        slot_id=0,
        agent_client=_ModelClient(),
        message_handler=message_handler,
        config={"gateway": {"cron": {"store_backend": "file"}}},
    )

    assert runtime.controller is message_handler.controller
    assert runtime.scheduler is runtime.controller.scheduler
    assert runtime.controller.store.path == cron_path
    assert set(runtime.scheduler._jobs) == {"enabled", "disabled"}
    assert runtime.scheduler._running is False
    assert runtime.scheduler._task is None
    assert fake_resolver.instances[0].agent_client is not None
    assert runtime.controller._model_resolver is fake_resolver.instances[0]
    assert runtime.controller._agent_client is not None
    assert runtime.controller._credential_access is not None
    assert [job.id for job in fake_resolver.instances[0].validated] == ["enabled"]
    created = await runtime.controller.create_job(
        {
            "name": "remote-created",
            "cron_expr": "0 11 * * *",
            "timezone": "UTC",
            "description": "remote-created",
            "targets": "web",
            "user_id": "alice",
            "model_name": "remote-alias",
        }
    )
    assert created["model_name"] == "remote-model"
    assert fake_resolver.instances[0].resolved == [("remote-alias", None)]
    with pytest.raises(ValueError, match="cron execution capability"):
        await runtime.controller.toggle_job("disabled", True, user_id="alice")
    assert [job.id for job in fake_resolver.instances[0].validated] == ["enabled"]
    assert message_handler.tool_push_enabled is False


async def test_preflight_rejects_enabled_real_project_but_keeps_disabled_stock(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    store = FileCronJobStore(path=cron_path)
    await _job(store, "real-enabled", project_id="real-project")
    before = cron_path.read_bytes()
    with pytest.raises(ValueError, match="preflight|real-project"):
        await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    # The read-only preflight neither rewrites nor retires the record.
    assert cron_path.read_bytes() == before

    store = FileCronJobStore(path=cron_path)
    await store.delete_job("real-enabled")
    await _job(store, "real-disabled", enabled=False, project_id="real-project")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)

    listed = await runtime.controller.list_jobs(project_id="real-project", user_id="alice")
    assert listed == []
    owned = await runtime.controller.store.list_jobs()
    assert [job.id for job in owned] == ["real-disabled"]
    assert await runtime.controller.delete_job(
        "real-disabled", user_id="alice", delete_sessions=False
    ) is True
    assert await runtime.scheduler.project_execution_allowed("real-project") is False
    assert await runtime.scheduler.project_execution_allowed("default") is True
    runtime.scheduler.close_project_admission("default")
    assert await runtime.scheduler.project_execution_allowed("default") is False


async def test_runtime_rejects_implicit_manifest_and_foreign_slot(cron_path: Path) -> None:
    from agent_gateway.application.worker_cron import build_worker_cron

    with pytest.raises(ValueError, match="manifest"):
        await build_worker_cron(manifest=None, slot_id=0, agent_client=None, message_handler=None)
    with pytest.raises(ValueError, match="slot"):
        await build_worker_cron(
            manifest=WorkerManifest((0, 1)), slot_id=True, agent_client=None, message_handler=None,
        )
    with pytest.raises(ValueError, match="slot"):
        await build_worker_cron(
            manifest=WorkerManifest((0, 1)), slot_id=2, agent_client=None, message_handler=None,
        )


async def test_create_rejects_real_project_and_login_model_before_store(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronCredentialAccess
    from agent_gateway.auth.credential_access import CredentialAccessError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    for params in ({"project_id": "real-project"}, {"project_dir": "/tmp/project"}):
        with pytest.raises(ValueError, match="non-default project"):
            await runtime.controller.create_job({
                "name": "p", "cron_expr": "0 9 * * *", "user_id": "alice", **params,
            })
    assert await runtime.controller.store.list_jobs() == []
    assert fake_resolver.instances[0].resolved == []
    with pytest.raises(ValueError, match="login models are not enabled"):
        await runtime.controller._credential_ref_for_login_model("session")
    access = WorkerCronCredentialAccess()
    for name, arguments in (
        ("request_model_auth", (None, "model")), ("credential_update_for_ref", ("ref",)),
        ("credential_refresh_for_ref", ("ref", "server")), ("credential_ref_for_login_session", (None,)),
        ("model_auth_for_ref", ("ref",)),
    ):
        with pytest.raises(CredentialAccessError) as failure:
            await getattr(access, name)(*arguments)
        assert failure.value.reason == "not_enabled"
        assert "ref" not in str(failure.value)


async def test_run_now_rereads_owned_store_before_cache_and_permission_hold(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    runtime.scheduler._jobs["ghost"] = SimpleNamespace(id="ghost")

    with pytest.raises(KeyError, match="job not found"):
        await runtime.scheduler.trigger_run_now_info("ghost")


async def test_run_now_rejects_disabled_job_without_model_validation(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    store = FileCronJobStore(path=cron_path)
    await _job(store, "disabled", enabled=False, model_name="missing-model")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)

    with pytest.raises(ValueError, match="任务已暂停"):
        await runtime.scheduler.trigger_run_now_info("disabled")
    assert fake_resolver.instances[0].validated == []


async def test_model_gate_failure_is_request_error_not_worker_failure(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    store = FileCronJobStore(path=cron_path)
    await _job(store, "job")
    await _job(store, "disabled-job", enabled=False, model_name="invalid-model")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    await runtime.activate()
    fake_resolver.instances[0].fail = True
    await runtime.scheduler._on_wake(runtime.scheduler._jobs["job"], "job:run")

    with pytest.raises(ValueError, match="model unavailable"):
        await runtime.controller.toggle_job(
            "disabled-job", True, user_id="alice"
        )
    with pytest.raises(ValueError, match="model unavailable"):
        await runtime.scheduler.trigger_run_now_info("job")
    assert runtime.scheduler._fatal_error is None
    assert runtime.scheduler._running is True
    await runtime.stop()


async def test_wake_model_failure_marks_run_failed_for_final_push(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    store = FileCronJobStore(path=cron_path)
    await _job(store, "job")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    fake_resolver.instances[0].fail = True
    runtime.scheduler._push_to_targets = AsyncMock()

    await runtime.scheduler._on_wake(runtime.scheduler._jobs["job"], "job:run")
    state = runtime.scheduler._runs["job:run"]
    assert state.status == "failed"
    assert state.error == "model unavailable"
    assert state.result_text == "[cron] 任务执行失败: model unavailable"
    assert state.finished_at is not None

    await runtime.scheduler._on_push(runtime.scheduler._jobs["job"], "job:run")
    runtime.scheduler._push_to_targets.assert_awaited_once()
    assert runtime.scheduler._push_to_targets.call_args.kwargs["text"] == (
        "[cron] 任务执行失败: model unavailable"
    )
    assert state.pushed_final is True


async def test_update_job_checks_identity_before_model_fetch_and_allows_model_rebind(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    store = FileCronJobStore(path=cron_path)
    await _job(store, "login-stock", enabled=False, model_name="login-model", credential_ref="login:ref")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)

    with pytest.raises(KeyError, match="job not found"):
        await runtime.controller.update_job(
            "login-stock", {"enabled": True}, user_id="bob"
        )
    assert fake_resolver.instances[0].resolved == []
    assert fake_resolver.instances[0].validated == []

    with pytest.raises(ValueError, match="non-default project"):
        await runtime.controller.update_job(
            "login-stock", {"project_id": "real-project"}, user_id="alice"
        )
    # A name-only patch keeps the stored selection; re-enabling is still gated.
    with pytest.raises(ValueError, match="cron execution capability"):
        await runtime.controller.update_job("login-stock", {"enabled": True}, user_id="alice")

    updated = await runtime.controller.update_job(
        "login-stock",
        {"enabled": True, "model_name": "self-configured"},
        user_id="alice",
    )
    assert updated["enabled"] is True
    assert updated["model_name"] == "self-configured"
    assert updated.get("credential_ref", "") == ""
    stored_job = await runtime.controller.store.get_job("login-stock")
    assert stored_job is not None
    assert stored_job.credential_ref == ""


async def test_store_poll_failure_stops_scheduler_and_raises_from_wait(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStoreError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    runtime.scheduler._store_poll_interval = 0.01
    monkeypatch.setattr(
        runtime.controller.store,
        "get_revision",
        AsyncMock(side_effect=OSError("store gone")),
    )

    waiter = asyncio.create_task(runtime.wait())
    await runtime.activate()
    with pytest.raises(WorkerCronStoreError, match="owned Cron store is unavailable"):
        await asyncio.wait_for(waiter, 1)


async def test_event_store_hydration_failure_is_fatal(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStoreError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    monkeypatch.setattr(
        runtime.scheduler._store,
        "get_job",
        AsyncMock(side_effect=OSError("store gone")),
    )

    with pytest.raises(WorkerCronStoreError, match="owned Cron store is unavailable"):
        await runtime.scheduler._handle_event(
            SimpleNamespace(kind="wake", job_id="job", run_id="job:run")
        )
    assert isinstance(runtime.scheduler._fatal_error, WorkerCronStoreError)


async def build_race_runtime(cron_path: Path, fake_resolver: type[_FakeResolver]) -> Any:
    await _job(FileCronJobStore(path=cron_path), "job")
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    await runtime.activate()
    return runtime


async def test_run_now_is_rejected_when_stop_wins_during_model_validation(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    runtime = await build_race_runtime(cron_path, fake_resolver)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_validation(job: Any) -> None:
        entered.set()
        await release.wait()

    runtime.scheduler._validate_job_for_execution = blocked_validation
    requests = []

    async def send_request(envelope, **kwargs):
        requests.append(envelope)
        return AgentResponse(request_id=envelope.request_id, channel_id=envelope.channel,
                             ok=True, payload={"session_id": "unexpected-session"})

    runtime.scheduler._agent_client = SimpleNamespace(send_request=send_request)
    run_task = asyncio.create_task(runtime.scheduler.trigger_run_now_info("job"))
    await asyncio.wait_for(entered.wait(), 1)
    stop_task = asyncio.create_task(runtime.stop())
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(run_task, 1)
    await asyncio.wait_for(stop_task, 1)
    assert requests == []
    assert runtime.scheduler._runs == {}
    assert runtime.scheduler._events == []


async def test_run_now_session_create_is_cancelled_when_stop_waits_for_admission(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    class BlockingAgentClient:
        def __init__(self) -> None:
            self.session_entered = asyncio.Event()
            self.release_session = asyncio.Event()
            self.requests: list[Any] = []

        async def send_request(self, envelope: Any, **kwargs: Any) -> AgentResponse:
            del kwargs
            self.requests.append(envelope)
            if envelope.method == ReqMethod.SESSION_CREATE.value:
                self.session_entered.set()
                await self.release_session.wait()
                return AgentResponse(
                    request_id=envelope.request_id, channel_id=envelope.channel,
                    ok=True, payload={"session_id": "session-run-now"},
                )
            if envelope.method == ReqMethod.CHAT_CANCEL.value:
                return AgentResponse(
                    request_id=envelope.request_id, channel_id=envelope.channel,
                    ok=True, payload={"success": True, "stop_confirmed": True},
                )
            raise AssertionError(envelope.method)

    runtime = await build_race_runtime(cron_path, fake_resolver)
    client = BlockingAgentClient()
    runtime.scheduler._agent_client = client
    run_task = asyncio.create_task(runtime.scheduler.trigger_run_now_info("job"))
    await asyncio.wait_for(client.session_entered.wait(), 1)
    stop_task = asyncio.create_task(runtime.stop())
    await asyncio.sleep(0)
    assert not stop_task.done()
    client.release_session.set()

    run_info = await asyncio.wait_for(run_task, 1)
    await asyncio.wait_for(stop_task, 1)
    assert run_info["session_id"] == "session-run-now"
    assert [request.method for request in client.requests] == [
        ReqMethod.SESSION_CREATE.value, ReqMethod.CHAT_CANCEL.value,
    ]
    assert runtime.scheduler._runs[run_info["run_id"]].status == "failed"
    assert runtime.scheduler._events == []
    with pytest.raises(RuntimeError, match="stopping"):
        await runtime.scheduler.trigger_run_now_info("job")


async def test_stop_bounds_admission_that_delays_cancellation(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStopTimeout

    runtime = await build_race_runtime(cron_path, fake_resolver)
    runtime.scheduler.stop_timeout_seconds = .1
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def delayed_validation(job: Any) -> None:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    runtime.scheduler._validate_job_for_execution = delayed_validation
    run_task = asyncio.create_task(runtime.scheduler.trigger_run_now_info("job"))
    await asyncio.wait_for(entered.wait(), 1)
    try:
        with pytest.raises(WorkerCronStopTimeout):
            await asyncio.wait_for(runtime.stop(), .5)
        assert cancelled.is_set()
        assert not run_task.done()
    finally:
        release.set()
        result = await asyncio.gather(run_task, return_exceptions=True)
    assert isinstance(result[0], RuntimeError)
    assert "stopping" in str(result[0])
    assert runtime.scheduler._runs == {}
    assert runtime.scheduler._events == []


async def test_stop_bounds_remote_cancel_run_tasks_and_closes_store_last(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStopTimeout

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    await runtime.activate()
    runtime.scheduler.stop_timeout_seconds = 0.05

    async def uncancellable_run() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(10)

    run_task = asyncio.create_task(uncancellable_run())
    runtime.scheduler._runs["run"] = SimpleNamespace(
        exec_session_id="session-1", status="running"
    )
    runtime.scheduler._run_tasks["run"] = run_task

    async def uncancellable_cancel(*_args: Any, **_kwargs: Any) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(10)

    runtime.scheduler._cancel_agent_session = AsyncMock(side_effect=uncancellable_cancel)
    runtime.scheduler._store.aclose = AsyncMock()

    with pytest.raises(WorkerCronStopTimeout, match="deadline"):
        await asyncio.wait_for(runtime.stop(), 1)

    assert run_task.cancelling() >= 1
    assert runtime.scheduler._store.aclose.await_count == 1
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)


async def test_stop_awaits_real_as_cancel_before_cancelling_active_runs(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    class CaptureAgentClient:
        def __init__(self) -> None:
            self.requests: list[Any] = []

        async def send_request(
            self, envelope: Any, timeout: float | None = None
        ) -> AgentResponse:
            events.append("as_cancel_sent")
            self.requests.append(envelope)
            return AgentResponse(
                request_id=envelope.request_id,
                channel_id=envelope.channel,
                ok=True,
                payload={"success": True, "stop_confirmed": True},
            )

    events: list[str] = []
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    runtime.scheduler.stop_timeout_seconds = 1.0
    client = CaptureAgentClient()
    runtime.scheduler._agent_client = client
    runtime.scheduler._store.aclose = AsyncMock()

    async def active_run() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            events.append("local_run_cancelled")
            raise

    local_run = asyncio.create_task(active_run())
    done_run = asyncio.create_task(asyncio.sleep(0))
    await asyncio.sleep(0)
    runtime.scheduler._runs.update(
        {
            "running": CronRunState(
                run_id="running",
                job_id="job",
                wake_at_iso="2026-01-01T00:00:00+00:00",
                push_at_iso="2026-01-01T00:01:00+00:00",
                status="running",
                exec_channel_id="__cron__",
                exec_session_id="session-running",
                exec_mode="agent",
                exec_work_mode="work",
                exec_user_id="alice",
                exec_project_id="default",
            ),
            "approval": CronRunState(
                run_id="approval",
                job_id="job",
                wake_at_iso="2026-01-01T00:00:00+00:00",
                push_at_iso="2026-01-01T00:01:00+00:00",
                status="awaiting_approval",
                exec_channel_id="web",
                exec_session_id="session-approval",
                exec_mode="agent",
                exec_work_mode="work",
                exec_user_id="alice",
                exec_project_id="default",
            ),
            "finished": CronRunState(
                run_id="finished",
                job_id="job",
                wake_at_iso="2026-01-01T00:00:00+00:00",
                push_at_iso="2026-01-01T00:01:00+00:00",
                status="succeeded",
                exec_channel_id="__cron__",
                exec_session_id="session-finished",
                exec_mode="agent",
                exec_work_mode="work",
                exec_user_id="alice",
                exec_project_id="default",
            ),
        }
    )
    runtime.scheduler._run_tasks.update({"running": local_run, "approval": done_run})
    runtime.scheduler._permission_hold["job"] = (
        time.time() + 60,
        "session-approval",
    )
    runtime.scheduler._redis_clear_permission_hold = AsyncMock()

    await runtime.stop()

    assert [envelope.method for envelope in client.requests] == [
        ReqMethod.CHAT_CANCEL.value,
        ReqMethod.CHAT_CANCEL.value,
    ]
    cancelled_sessions = {
        envelope.params["session_id"] for envelope in client.requests
    }
    assert cancelled_sessions == {"session-running", "session-approval"}
    assert events == [
        "as_cancel_sent",
        "as_cancel_sent",
        "local_run_cancelled",
    ]
    assert local_run.cancelled()
    assert not done_run.cancelled()
    assert runtime.scheduler._permission_hold == {}
    runtime.scheduler._redis_clear_permission_hold.assert_awaited_once_with(
        "job", session_id="session-approval"
    )
    assert runtime.scheduler._store.aclose.await_count == 1


async def test_stop_cancels_only_active_runs_and_survives_fatal_scheduler_task(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)

    async def fatal_scheduler() -> None:
        raise RuntimeError("scheduler already failed")

    fatal_task = asyncio.create_task(fatal_scheduler())
    await asyncio.sleep(0)
    runtime.scheduler._task = fatal_task

    async def long_run() -> None:
        await asyncio.sleep(10)

    run_task = asyncio.create_task(long_run())
    runtime.scheduler._runs.update(
        {
            "finished": SimpleNamespace(
                exec_session_id="finished-session", status="succeeded"
            ),
            "active": SimpleNamespace(
                exec_session_id="active-session", status="running"
            ),
        }
    )
    runtime.scheduler._run_tasks["active"] = run_task
    runtime.scheduler._cancel_agent_session = AsyncMock()
    runtime.scheduler._store.aclose = AsyncMock()

    await runtime.stop()

    cancelled_sessions = [
        call.args[0].exec_session_id
        for call in runtime.scheduler._cancel_agent_session.call_args_list
    ]
    assert cancelled_sessions == ["active-session"]
    assert run_task.cancelled()
    assert runtime.scheduler._store.aclose.await_count == 1


async def test_stop_reports_failure_when_remote_cancel_is_not_confirmed(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStopError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    runtime.scheduler.stop_timeout_seconds = 0.2
    runtime.scheduler._runs["run"] = SimpleNamespace(
        exec_session_id="session-1", status="running"
    )
    runtime.scheduler._run_tasks["run"] = asyncio.create_task(asyncio.sleep(0.01))
    runtime.scheduler._cancel_agent_session = AsyncMock(return_value=False)
    runtime.scheduler._store.aclose = AsyncMock()

    with pytest.raises(WorkerCronStopError, match="remote cancellation was not confirmed"):
        await runtime.stop()

    runtime.scheduler._cancel_agent_session.assert_awaited_once()
    assert runtime.scheduler._store.aclose.await_count == 1


async def test_stop_reports_failure_when_remote_cancel_exceeds_its_share(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronStopError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    runtime.scheduler.stop_timeout_seconds = 0.08
    runtime.scheduler._runs["run"] = SimpleNamespace(
        exec_session_id="session-1", status="running"
    )
    run_task = asyncio.create_task(asyncio.sleep(0))
    await asyncio.sleep(0)
    runtime.scheduler._run_tasks["run"] = run_task

    async def slow_cancel(*_args: Any, **_kwargs: Any) -> bool:
        await asyncio.sleep(0.05)
        return True

    runtime.scheduler._cancel_agent_session = AsyncMock(side_effect=slow_cancel)
    runtime.scheduler._store.aclose = AsyncMock()

    with pytest.raises(WorkerCronStopError, match="remote cancellation was not confirmed"):
        await runtime.stop()

    assert runtime.scheduler._store.aclose.await_count == 1


async def test_wait_returns_on_normal_stop_and_raises_on_unexpected_exit(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    from agent_gateway.application.worker_cron import WorkerCronRuntimeError

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    waiter = asyncio.create_task(runtime.wait())
    await asyncio.sleep(0)
    await runtime.activate()
    await runtime.stop()
    await asyncio.wait_for(waiter, 1)

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    waiter = asyncio.create_task(runtime.wait())
    await runtime.activate()
    runtime.scheduler._running = False
    runtime.scheduler._reload_event.set()
    with pytest.raises(WorkerCronRuntimeError, match="exited unexpectedly"):
        await asyncio.wait_for(waiter, 1)


async def test_activate_cannot_restart_after_stop_and_race_uses_lifecycle_lock(
    cron_path: Path,
    fake_resolver: type[_FakeResolver],
) -> None:
    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    await runtime.stop()
    with pytest.raises(RuntimeError, match="Worker Cron runtime is stopped"):
        await runtime.activate()

    runtime = await _build_runtime(cron_path=cron_path, fake_resolver=fake_resolver)
    async with runtime._lifecycle_lock:
        stop_task = asyncio.create_task(runtime.stop())
        activate_task = asyncio.create_task(runtime.activate())
        await asyncio.sleep(0)
        assert not stop_task.done()
        assert not activate_task.done()

    stop_result, activate_result = await asyncio.gather(
        stop_task, activate_task, return_exceptions=True
    )
    assert stop_result is None
    assert runtime._stopped.is_set()
    assert runtime.scheduler._running is False
    assert runtime.scheduler._task is None
    if isinstance(activate_result, RuntimeError):
        assert str(activate_result) == "Worker Cron runtime is stopped"
    else:
        assert activate_result is None
