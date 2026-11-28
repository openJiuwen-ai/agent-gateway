# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Sharded personal Cron runtime for controlled data Worker processes."""

from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from agent_gateway.application.worker_cron_models import WorkerCronModelResolver
from agent_gateway.auth.credential_access import CredentialAccessError
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.controller import CronController
from agent_gateway.cron.models import CronRunState
from agent_gateway.cron.owned_store import OwnedCronJobStore
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.preflight import require_supported_cron
from agent_gateway.cron.scheduler import CronSchedulerService
from agent_gateway.cron.store import FileCronJobStore

logger = logging.getLogger(__name__)


# Empty, ``default`` and ``default_code`` name the default project.
DEFAULT_CRON_PROJECT_IDS = frozenset({"", "default", "default_code"})
_DEFAULT_STOP_TIMEOUT_SECONDS = 8.0


class WorkerCronRuntimeError(RuntimeError):
    """A Cron gate or store failed in a way that makes the Worker unhealthy."""


class WorkerCronStoreError(WorkerCronRuntimeError):
    """The owned Cron store could not be read safely."""


class WorkerCronStopError(WorkerCronRuntimeError):
    """Worker Cron cleanup finished without confirming an active remote stop."""


class WorkerCronStopTimeout(WorkerCronStopError, TimeoutError):
    """Worker Cron cleanup did not finish within its bounded deadline."""


def requests_real_cron_project(params: Any) -> bool:
    """True when a Cron create body or update patch names a non-default project."""
    raw = params if isinstance(params, dict) else {}
    project_id = str(raw.get("project_id") or "").strip()
    if project_id and project_id not in DEFAULT_CRON_PROJECT_IDS:
        return True
    project_dir = raw.get("project_dir")
    return isinstance(project_dir, str) and bool(project_dir.strip())


class WorkerCronCredentialAccess:
    """Issue no Cron credential instead of falling back to the local login graph."""

    async def request_model_auth(self, auth_session: str | None, model_name: str) -> None:
        del auth_session, model_name
        raise CredentialAccessError("not_enabled")

    async def credential_update_for_ref(self, credential_ref: str) -> None:
        del credential_ref
        raise CredentialAccessError("not_enabled")

    async def credential_refresh_for_ref(self, credential_ref: str, server_id: str) -> None:
        del credential_ref, server_id
        raise CredentialAccessError("not_enabled")

    async def credential_ref_for_login_session(self, auth_session: str | None) -> None:
        del auth_session
        raise CredentialAccessError("not_enabled")

    async def model_auth_for_ref(self, credential_ref: str) -> None:
        del credential_ref
        raise CredentialAccessError("not_enabled")


class WorkerCronScheduler(CronSchedulerService):
    """A single-slot scheduler with execution-time capability and model gates."""

    _ACTIVE_RUN_STATUSES = frozenset({"pending", "running", "awaiting_approval"})

    def __init__(self, *, model_resolver: WorkerCronModelResolver, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.model_resolver = model_resolver
        self._fatal_error: BaseException | None = None
        self.stop_timeout_seconds = _DEFAULT_STOP_TIMEOUT_SECONDS
        self._run_admission_lock = asyncio.Lock()
        self._run_admissions: set[asyncio.Task[Any]] = set()
        self._cron_stopping = False

    def _record_fatal(self, error: BaseException) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
        self._running = False
        self._reload_event.set()

    @staticmethod
    def _consume_done_task(task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except Exception:
            pass

    async def _wait_tasks(
        self,
        tasks: list[asyncio.Task[Any]],
        *,
        deadline: float,
    ) -> tuple[set[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
        """Wait for tasks until an absolute deadline without cancelling them."""
        remaining = deadline - asyncio.get_running_loop().time()
        timeout = max(0.0, remaining)
        if not tasks:
            return set(), set()
        return await asyncio.wait(set(tasks), timeout=timeout)

    async def project_execution_allowed(
        self,
        project_id: str | None,
        user_id: str | None = None,
        group_id: str | None = None,
        bot_id: str | None = None,
    ) -> bool:
        """Admit default-project jobs only, without any project lifecycle work.

        Project coordination is not part of this assembly. A non-default
        project is refused here instead of asking the AgentServer, and no
        lifecycle-owner registry is written: the owning slot follows from the
        job's user, not from that registry.
        """
        del user_id, group_id, bot_id
        return project_id in DEFAULT_CRON_PROJECT_IDS and project_id not in self._hiding_projects

    async def _owned_job(self, job_id: str) -> Any | None:
        try:
            job = await self._store.get_job(job_id)
        except Exception as exc:
            error = WorkerCronStoreError("owned Cron store is unavailable")
            self._record_fatal(error)
            raise error from exc
        if job is not None:
            self._jobs[job.id] = job
        return job

    async def _validate_job_for_execution(self, job: Any) -> None:
        require_supported_cron(job)
        await self.model_resolver.validate_job(job)

    async def trigger_run_now_info(self, job_id: str) -> dict[str, Any]:
        async with self._run_admission_lock:
            if self._cron_stopping:
                raise RuntimeError("Worker Cron is stopping")
            task = asyncio.current_task()
            self._run_admissions.add(task)
            try:
                job = await self._owned_job(job_id)
                if job is None:
                    raise KeyError("job not found")
                if getattr(job, "expired", False):
                    raise ValueError("任务已过期，无法执行")
                if not job.enabled:
                    raise ValueError("任务已暂停，无法执行")
                await self._validate_job_for_execution(job)
                if self._cron_stopping:
                    raise RuntimeError("Worker Cron is stopping")
                return await super().trigger_run_now_info(job_id)
            finally:
                self._run_admissions.discard(task)

    async def _allocate_execution_session(
        self, job: Any, *, mode: str, run_id: str,
    ) -> str:
        # Manual runs already hold admission. Scheduled execution allocates in
        # a separately spawned task, which waits here for the same gate.
        if asyncio.current_task() in self._run_admissions:
            return await super()._allocate_execution_session(job, mode=mode, run_id=run_id)
        async with self._run_admission_lock:
            if self._cron_stopping:
                raise RuntimeError("Worker Cron is stopping")
            return await super()._allocate_execution_session(job, mode=mode, run_id=run_id)

    async def _on_wake(self, job: Any, run_id: str) -> None:
        async with self._run_admission_lock:
            current_job = await self._owned_job(job.id)
            if current_job is None or not current_job.enabled:
                return
            state = self._runs.get(run_id)
            if state is not None and (
                state.status in ("succeeded", "failed", "awaiting_approval")
                or (state.result_text and state.pushed_final)
            ):
                return
            if (
                run_id in self._run_tasks
                and not self._run_tasks[run_id].done()
            ):
                return
            try:
                await self._validate_job_for_execution(current_job)
            except ValueError as exc:
                if state is None:
                    try:
                        push_ts = int(run_id.split(":")[-1])
                    except Exception:
                        push_ts = int(self._now_fn())
                    push_dt = datetime.fromtimestamp(
                        push_ts, tz=ZoneInfo(current_job.timezone)
                    )
                    wake_dt = push_dt - timedelta(
                        seconds=max(0, int(current_job.wake_offset_seconds or 0))
                    )
                    state = CronRunState(
                        run_id=run_id,
                        job_id=current_job.id,
                        wake_at_iso=wake_dt.isoformat(),
                        push_at_iso=push_dt.isoformat(),
                        job_name=current_job.name,
                        targets=current_job.targets,
                        session_id=current_job.session_id,
                        chat_type=current_job.chat_type,
                        timezone=current_job.timezone,
                        exec_user_id=str(current_job.user_id or "").strip() or None,
                    )
                    self._runs[run_id] = state
                logger.warning(
                    "[Cron] wake rejected by job validation job=%s run_id=%s: %s",
                    job.id,
                    run_id,
                    exc,
                )
                if state is not None:
                    state.status = "failed"
                    state.error = str(exc)
                    state.result_text = f"[cron] 任务执行失败: {exc}"
                    state.finished_at = self._now_fn()
                return
            await super()._on_wake(current_job, run_id)

    async def _handle_event(self, event: Any) -> None:
        try:
            # The slot view must hydrate an event for a job this slot owns.
            if (
                event.kind in ("wake", "push", "push_update")
                and self._jobs.get(event.job_id) is None
            ):
                await self._owned_job(event.job_id)
            await super()._handle_event(event)
        except WorkerCronRuntimeError as exc:
            self._record_fatal(exc)
            raise

    async def _check_store_changed(self) -> bool:
        if bool(getattr(self._store, "supports_watch", False)):
            return False
        try:
            revision = int(await self._store.get_revision())
        except Exception as exc:
            error = WorkerCronStoreError("owned Cron store is unavailable")
            self._record_fatal(error)
            raise error from exc
        if revision == self._last_store_revision or (
            revision == 0 and self._last_store_revision == 0
        ):
            return False
        try:
            await self.reload()
        except Exception as exc:
            error = WorkerCronStoreError("owned Cron store is unavailable")
            self._record_fatal(error)
            raise error from exc
        return True

    async def _loop(self) -> None:
        while self._running:
            try:
                if not self._events:
                    self._reload_event.clear()
                    try:
                        await asyncio.wait_for(
                            self._reload_event.wait(),
                            timeout=self._store_poll_interval,
                        )
                    except asyncio.TimeoutError:
                        await self._check_store_changed()
                    continue

                now = self._now_fn()
                at_ts, _, event = self._events[0]
                delay = max(0.0, at_ts - now)

                if delay > 0:
                    self._reload_event.clear()
                    try:
                        await asyncio.wait_for(
                            self._reload_event.wait(),
                            timeout=min(delay, self._store_poll_interval),
                        )
                        continue
                    except asyncio.TimeoutError:
                        if await self._check_store_changed():
                            continue
                        if self._now_fn() < at_ts:
                            continue

                heapq.heappop(self._events)
                await self._handle_event(event)
            except asyncio.CancelledError:
                break
            except WorkerCronRuntimeError as exc:
                self._record_fatal(exc)
                raise
            except Exception as exc:
                logger.warning("[Cron] scheduler loop error: %s", exc, exc_info=True)
                await asyncio.sleep(0.5)

    async def stop(self) -> None:
        """Stop this slot and confirm the AgentServer stopped every active run."""
        self._cron_stopping = True
        self._running = False
        self._reload_event.set()

        loop = asyncio.get_running_loop()
        started_at = loop.time()
        deadline = started_at + self.stop_timeout_seconds
        admission_deadline = started_at + self.stop_timeout_seconds * 0.25
        remote_deadline = started_at + self.stop_timeout_seconds * 0.6
        local_deadline = started_at + self.stop_timeout_seconds * 0.8
        timed_out = False

        lifecycle_tasks: list[asyncio.Task[Any]] = []
        for attribute in ("_watch_task", "_task"):
            task = getattr(self, attribute, None)
            if task is not None:
                task.cancel()
                lifecycle_tasks.append(task)
                setattr(self, attribute, None)

        # An admitted run_now may still be allocating an AS session. Let it
        # register its state before taking the cancellation snapshot.
        try:
            async with asyncio.timeout_at(admission_deadline):
                async with self._run_admission_lock:
                    pass
        except TimeoutError:
            timed_out = True
            admission_tasks = list(self._run_admissions)
            for task in admission_tasks:
                task.cancel()
            lifecycle_tasks.extend(admission_tasks)
            await self._wait_tasks(admission_tasks, deadline=remote_deadline)

        active_states = {
            run_id: state
            for run_id, state in self._runs.items()
            if str(getattr(state, "status", "") or "").strip()
            in self._ACTIVE_RUN_STATUSES
        }
        remote_cancel_items = [
            (
                asyncio.create_task(
                    self._cancel_agent_session(
                        state,
                        reason="worker_stop",
                        strict=True,
                    ),
                ),
                state,
            )
            for state in active_states.values()
            if str(getattr(state, "exec_session_id", "") or "").strip()
        ]
        remote_cancels = [task for task, _state in remote_cancel_items]
        remote_done, remote_pending = await self._wait_tasks(
            remote_cancels, deadline=remote_deadline
        )
        remote_cancel_confirmed = not remote_pending
        confirmed_approval_holds: set[tuple[str, str]] = set()
        for task, state in remote_cancel_items:
            if task not in remote_done:
                continue
            self._consume_done_task(task)
            if task.cancelled():
                remote_cancel_confirmed = False
                continue
            try:
                confirmed = bool(task.result())
            except Exception:
                confirmed = False
            remote_cancel_confirmed = remote_cancel_confirmed and confirmed
            if not confirmed:
                continue
            if state.status == "awaiting_approval":
                confirmed_approval_holds.add((state.job_id, state.exec_session_id or ""))
            state.status = "failed"
            state.error = "worker_stopped"
            state.finished_at = self._now_fn()

        self._events.clear()
        for task in remote_pending:
            task.cancel()

        active_run_tasks = {
            run_id: task
            for run_id, task in self._run_tasks.items()
            if run_id in active_states and not task.done()
        }
        for task in active_run_tasks.values():
            task.cancel()

        for job_id, session_id in confirmed_approval_holds:
            self._permission_hold.pop(job_id, None)
            remaining = local_deadline - loop.time()
            try:
                await asyncio.wait_for(
                    self._redis_clear_permission_hold(
                        job_id, session_id=session_id
                    ),
                    timeout=max(0.0, remaining),
                )
            except TimeoutError:
                timed_out = True
            except Exception:
                logger.warning(
                    "[Cron] permission hold clear failed during worker stop job=%s",
                    job_id,
                    exc_info=True,
                )

        local_tasks = [
            *lifecycle_tasks,
            *remote_pending,
            *active_run_tasks.values(),
        ]
        _done, local_pending = await self._wait_tasks(
            local_tasks, deadline=local_deadline
        )
        for task in local_tasks:
            if task.done():
                self._consume_done_task(task)
            else:
                task.add_done_callback(self._consume_done_task)
        timed_out = bool(local_pending) or timed_out
        self._run_tasks.clear()

        closer = getattr(self._store, "aclose", None)
        if closer is not None:
            remaining = deadline - loop.time()
            try:
                await asyncio.wait_for(closer(), timeout=remaining)
            except TimeoutError:
                timed_out = True
        logger.info("[Cron] Worker scheduler stopped")
        if timed_out:
            raise WorkerCronStopTimeout(
                "Worker Cron cleanup did not finish within its deadline"
            )
        if not remote_cancel_confirmed:
            raise WorkerCronStopError(
                "Worker Cron remote cancellation was not confirmed"
            )


class WorkerCronController(CronController):
    """Recheck capability and model gates before a job is created or enabled."""

    def __init__(
        self,
        *,
        store: Any,
        scheduler: WorkerCronScheduler,
        model_resolver: WorkerCronModelResolver,
    ) -> None:
        super().__init__(store=store, scheduler=scheduler)
        self._model_resolver = model_resolver

    async def _credential_ref_for_login_model(self, auth_session: str) -> str:
        # Never bind a login account here, even when the base resolution and
        # the execution-time resolver disagree about a model's source.
        raise ValueError("Cron login models are not enabled")

    async def _validate_requested_model(self, params: dict[str, Any]) -> None:
        """Refuse login, AgentOS and unknown models before the base stores them."""
        if params.get("model_name") is None and params.get("model_selection") is None:
            return
        await self._model_resolver(
            params.get("model_name"),
            model_selection=params.get("model_selection"),
            user_id=str(params.get("user_id") or "").strip() or None,
        )

    async def create_job(
        self,
        params: dict[str, Any],
        *,
        internal_proactive_sync: bool = False,
    ) -> dict[str, Any]:
        await self._wait_for_startup()
        async with self.mutation_lock:
            params = dict(params or {})
            if requests_real_cron_project(params):
                raise ValueError("Cron jobs for a non-default project are not enabled")
            await self._validate_requested_model(params)
            create_job = getattr(CronController.create_job, "__wrapped__")
            return await create_job(
                self,
                params,
                internal_proactive_sync=internal_proactive_sync,
            )

    async def update_job(
        self,
        job_id: str,
        patch: dict[str, Any],
        *,
        user_id: str = "",
        group_id: str = "",
        bot_id: str = "",
    ) -> dict[str, Any]:
        await self._wait_for_startup()
        patch = dict(patch or {})
        async with self.mutation_lock:
            existing = await self._store.get_job(job_id)
            if existing is None or not self._matches_tenant(
                existing,
                user_id=user_id,
                group_id=group_id,
                bot_id=bot_id,
            ):
                raise KeyError("job not found")
            if requests_real_cron_project(patch):
                raise ValueError("Cron jobs for a non-default project are not enabled")
            will_enable = patch.get("enabled") is True or (
                "enabled" not in patch and existing.enabled
            )
            changes_model = "model_name" in patch or "model_selection" in patch
            if will_enable:
                # An unchanged model is checked on the stored record, so a
                # disabled stock job cannot be enabled by clearing its binding.
                prospective = (
                    self._prospective_model_job(existing, patch)
                    if changes_model
                    else existing
                )
                require_supported_cron(prospective)
                await self._model_resolver.validate_job(prospective)
            update_job = getattr(CronController.update_job, "__wrapped__")
            return await update_job(
                self,
                job_id,
                patch,
                user_id=user_id,
                group_id=group_id,
                bot_id=bot_id,
            )

    @staticmethod
    def _prospective_model_job(job: Any, patch: dict[str, Any]) -> Any:
        """Build the model state that update_job will persist after resolution."""
        fields: dict[str, Any] = {}
        if "model_name" in patch:
            fields["model_name"] = patch["model_name"]
        if "model_selection" in patch:
            fields["model_selection"] = patch["model_selection"]
        return replace(job, **fields, credential_ref="")


class WorkerCronRuntime:
    """Own the lifecycle of one slot-scoped Cron scheduler."""

    def __init__(self, *, controller: CronController, scheduler: WorkerCronScheduler) -> None:
        self.controller = controller
        self.scheduler = scheduler
        self._lifecycle_lock = asyncio.Lock()
        self._activation_started = False
        self._activated = asyncio.Event()
        self._stopped = asyncio.Event()
        self._stopping = False
        self._stop_called = False

    async def activate(self) -> None:
        async with self._lifecycle_lock:
            if self._stopping or self._stop_called or self._stopped.is_set():
                raise RuntimeError("Worker Cron runtime is stopped")
            if self._activation_started or self._activated.is_set():
                raise RuntimeError("Worker Cron runtime can only be activated once")
            self._activation_started = True
            await self.scheduler.start()
            self._activated.set()

    async def wait(self) -> None:
        if self._stopping or self._stopped.is_set():
            return
        activation_wait = asyncio.create_task(self._activated.wait())
        stop_wait = asyncio.create_task(self._stopped.wait())
        try:
            await asyncio.wait(
                (activation_wait, stop_wait), return_when=asyncio.FIRST_COMPLETED
            )
            if self._stopping or self._stopped.is_set():
                return
        finally:
            activation_wait.cancel()
            stop_wait.cancel()
            await asyncio.gather(activation_wait, stop_wait, return_exceptions=True)

        scheduler_task = self.scheduler._task
        if scheduler_task is None:
            if self._stopping or self._stopped.is_set():
                return
            raise WorkerCronRuntimeError("activated Worker Cron scheduler has no task")
        stop_wait = asyncio.create_task(self._stopped.wait())
        try:
            done, _pending = await asyncio.wait(
                (scheduler_task, stop_wait), return_when=asyncio.FIRST_COMPLETED
            )
            if self._stopping or self._stopped.is_set():
                return
            if scheduler_task in done:
                await scheduler_task
                if self.scheduler._fatal_error is not None:
                    raise self.scheduler._fatal_error
                raise WorkerCronRuntimeError("Worker Cron scheduler exited unexpectedly")
        finally:
            stop_wait.cancel()
            await asyncio.gather(stop_wait, return_exceptions=True)

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self._stop_called:
                return
            self._stop_called = True
            self._stopping = True
            self._stopped.set()
            self._reload_event_set()
            await self.scheduler.stop()

    def _reload_event_set(self) -> None:
        self.scheduler._reload_event.set()


async def build_worker_cron(
    *,
    manifest: WorkerManifest,
    slot_id: int,
    agent_client: Any,
    message_handler: Any,
    config: dict | None = None,
    identity_policy: LocalIdentityPolicy | None = None,
) -> WorkerCronRuntime:
    """Prepare one owned Cron shard without starting its background loop.

    Every enabled job this slot owns must pass the static capability gates and
    resolve its model against the target AgentServer, otherwise the Worker is
    not prepared. Disabled records stay visible and are rechecked when enabled.
    """
    from agent_gateway.application.cron_preflight import preflight_controlled_cron
    from agent_gateway.common.paths import get_cron_jobs_path

    if type(manifest) is not WorkerManifest:
        raise ValueError("Worker Cron requires an explicit Worker manifest")
    if type(slot_id) is not int:
        raise ValueError("Worker Cron requires an explicit slot")
    if slot_id not in manifest.slots:
        raise ValueError("Worker Cron slot is absent from the Worker manifest")
    if config is not None and not isinstance(config, dict):
        raise ValueError("Worker Cron configuration must be an object or null")

    ownership = CronOwnership(manifest, identity_policy or LocalIdentityPolicy())
    await preflight_controlled_cron(ownership, config=config, allow_project_jobs=False)
    backend = FileCronJobStore(path=get_cron_jobs_path())
    owned_store = OwnedCronJobStore(backend, ownership, slot=slot_id)
    model_resolver = WorkerCronModelResolver(agent_client)
    scheduler = WorkerCronScheduler(
        store=owned_store,
        agent_client=agent_client,
        message_handler=message_handler,
        model_resolver=model_resolver,
    )
    controller = WorkerCronController(
        store=owned_store,
        scheduler=scheduler,
        model_resolver=model_resolver,
    )
    credential_access = WorkerCronCredentialAccess()
    scheduler.set_credential_access(credential_access)
    controller.bind_agent_client(agent_client)
    controller.set_credential_access(credential_access)

    for job in await owned_store.list_jobs():
        if not job.enabled:
            continue
        if job.project_id not in DEFAULT_CRON_PROJECT_IDS:
            raise ValueError(f"enabled Cron project is not allowed: {job.project_id}")
        require_supported_cron(job)
        try:
            await model_resolver.validate_job(job)
        except Exception as exc:
            raise ValueError(f"cron model validation failed for job {job.id}") from exc
    await scheduler.reload()
    message_handler.set_cron_controller(controller)
    message_handler.set_cron_tool_push_enabled(False)
    return WorkerCronRuntime(controller=controller, scheduler=scheduler)
