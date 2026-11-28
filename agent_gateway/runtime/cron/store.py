from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar

import portalocker

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.paths import get_cron_jobs_path
from agent_gateway.common.work_mode import DEFAULT_WEB_WORK_MODE
from agent_gateway.runtime.cron.cron_job_mutations import (
    _PROACTIVE_TICK_MODE,
    _ProactiveJobProtected,
    apply_cron_job_patch,
    build_new_cron_job,
    ensure_proactive_deletable,
    migrate_work_mode_on_items,
    parse_cron_jobs,
    sort_cron_jobs,
)
from agent_gateway.runtime.cron.models import CronJob, CronTarget, is_valid_target_channel_id

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_FILE_LOCK_TIMEOUT_SEC = 10.0


class FileCronJobStore:
    """Persist cron jobs to ~/.jiuwenswarm/agent/home/cron_jobs.json.

    并发安全:
      - ``asyncio.Lock``：同进程协程互斥；
      - ``portalocker`` 伴生 ``cron_jobs.json.lock``：跨进程（多 Gateway / Agent）互斥。
      整个 read-modify-write 在双层锁内完成，避免 lost update。
    """

    supports_watch = False

    def __init__(
        self,
        path: Path | None = None,
        *,
        file_lock_timeout: float = _FILE_LOCK_TIMEOUT_SEC,
    ) -> None:
        self._path = path or get_cron_jobs_path()
        self._lock = asyncio.Lock()
        self._file_lock_timeout = float(file_lock_timeout)

    @property
    def path(self) -> Path:
        return self._path

    def _call_under_file_lock(self, fn: Callable[[], _T]) -> _T:
        """在伴生 ``cron_jobs.json.lock`` 上拿跨进程锁后执行 fn（不被原子 replace 覆盖）。

        须在进程内 ``asyncio.Lock`` 之内、经 ``to_thread`` 调用，避免阻塞事件循环。
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self._path.with_suffix(self._path.suffix + ".lock")
        with portalocker.Lock(str(lock_path), timeout=self._file_lock_timeout):
            return fn()

    async def _run_locked(self, fn: Callable[[], _T]) -> _T:
        """同进程协程串行 + 跨进程文件锁；文件锁等待放到线程池，避免阻塞事件循环。"""
        async with self._lock:
            return await asyncio.to_thread(self._call_under_file_lock, fn)

    def _file_revision(self) -> int:
        try:
            stat = self._path.stat()
            return (
                (int(stat.st_mtime_ns) << 40)
                ^ (int(stat.st_ctime_ns) << 16)
                ^ int(stat.st_size)
            )
        except OSError:
            return 0

    async def get_revision(self) -> int:
        return self._file_revision()

    @staticmethod
    def _parse_snapshot(data: dict[str, Any]) -> list[CronJob]:
        """Validate the sharding view without old permissive owner/target coercion."""
        if (
            not isinstance(data, dict)
            or type(data.get("version", 1)) is not int
            or data.get("version", 1) != 1
            or not isinstance(data.get("jobs"), list)
        ):
            raise ValueError("invalid cron snapshot schema")
        jobs: list[CronJob] = []
        seen: set[str] = set()
        policy = LocalIdentityPolicy()
        for index, original in enumerate(data["jobs"]):
            try:
                if not isinstance(original, dict):
                    raise ValueError()
                item = dict(original)
                job_id = item.get("id")
                if (
                    not isinstance(job_id, str) or not job_id
                    or job_id != job_id.strip() or job_id in seen
                    or any(ord(character) < 32 for character in job_id)
                ):
                    raise ValueError()
                for key in ("user_id", "group_id", "bot_id"):
                    value = item.get(key, "")
                    if not isinstance(value, str):
                        raise ValueError()
                    if value:
                        IngressIdentity(
                            contract_id=policy.contract_id, mode="local_compat",
                            domain=policy.domain, kind="named", business_user_id=value,
                        )
                for key in ("enabled", "expired", "delete_after_run"):
                    if key in item and type(item[key]) is not bool:
                        raise ValueError()
                ref = item.get("credential_ref", "")
                if not isinstance(ref, str) or ref != ref.strip():
                    raise ValueError()
                if "model_selection" in item and item["model_selection"] is not None:
                    if not isinstance(item["model_selection"], dict):
                        raise ValueError()
                if "work_mode" in item and item["work_mode"] not in ("code", "work"):
                    raise ValueError()
                if "wake_offset_seconds" in item:
                    offset = item["wake_offset_seconds"]
                    if type(offset) is not int or offset < 0:
                        raise ValueError()
                targets = item.get("targets")
                if isinstance(targets, list):
                    if len(targets) != 1 or any(not isinstance(t, dict) for t in targets):
                        raise ValueError()
                    channel_ids = [t.get("channel_id") for t in targets]
                else:
                    channel_ids = [targets]
                if any(not isinstance(t, str) or not is_valid_target_channel_id(t) for t in channel_ids):
                    raise ValueError()
                # Legacy work-mode inference is safe only in this detached copy.
                migrate_work_mode_on_items([item])
                job = CronJob.from_dict(item)
                seen.add(job.id)
                jobs.append(job)
            except Exception:
                # Parser exceptions may embed model/description values. Do not
                # propagate their text into an operator-facing preflight report.
                raise ValueError(f"invalid cron snapshot record at index {index}") from None
        return sort_cron_jobs(jobs)

    def _read_snapshot_data(self) -> dict[str, Any]:
        """Read the exact schema; only an absent file means an empty inventory."""
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {"version": 1, "jobs": []}
        except UnicodeError:
            raise ValueError("invalid cron snapshot encoding") from None
        try:
            return json.loads(raw)
        except ValueError:
            raise ValueError("invalid cron snapshot JSON") from None

    async def read_snapshot(self) -> list[CronJob]:
        """Read one atomic-replace generation, with no data/lock/directory writes.

        File writers replace the whole JSON atomically. Opening it therefore
        observes one complete old or new generation; no sidecar lock is needed.
        This is an inventory, not a transaction spanning subsequent mutations.
        """
        return await asyncio.to_thread(lambda: self._parse_snapshot(self._read_snapshot_data()))

    async def insert_job(self, job: CronJob) -> CronJob:
        """Create exclusively under the file lock; unlike legacy create, no upsert."""
        def body() -> CronJob:
            data = self._read_snapshot_data()
            self._parse_snapshot({'version': 1, 'jobs': [job.to_dict()]})
            existing = self._parse_snapshot(data)
            if any(item.id == job.id for item in existing):
                raise ValueError("cron job already exists")
            data["jobs"].append(job.to_dict())
            self._write_json_unlocked(data)
            return job

        return await self._run_locked(body)

    async def list_jobs(self, *, strict: bool = False) -> list[CronJob]:
        # 惰性迁移:在同一个锁内 read + 推断缺 work_mode 的老 job + writeback,
        # 替代启动迁移 ``migrate_legacy_jobs_at_startup``。
        def _body() -> list[CronJob]:
            data = self._read_json_unlocked(strict=strict)
            jobs_raw = data.get("jobs", [])
            if not isinstance(jobs_raw, list):
                if strict:
                    raise ValueError("cron jobs must be an array")
                return []

            jobs_raw, changed = migrate_work_mode_on_items(jobs_raw)
            if changed:
                data["jobs"] = jobs_raw
                try:
                    self._write_json_unlocked(data)
                except (OSError, ValueError, TypeError) as exc:
                    logger.warning(
                        "Cron 惰性迁移写回 cron_jobs.json 失败: %s", exc
                    )
            if strict:
                return [CronJob.from_dict(item) for item in jobs_raw]
            return parse_cron_jobs(jobs_raw)

        return await self._run_locked(_body)

    async def get_job(self, job_id: str) -> CronJob | None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return None
        for job in await self.list_jobs():
            if job.id == job_id:
                return job
        return None

    @staticmethod
    def build_job(
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
        project_id: str = "",
        model_name: str | None = None,
        model_selection: dict[str, str] | None = None,
        mcp: list[str] | None = None,
        app_id: str = "",
        work_mode: str = DEFAULT_WEB_WORK_MODE,
        user_id: str = "",
        group_id: str = "",
        bot_id: str = "",
        credential_ref: str = "",
    ) -> CronJob:
        """Construct and validate a ``CronJob`` without persisting it.

        Gateway remains the single persistence owner.  Runtime cron tools use
        this builder to produce the same validated wire representation before
        submitting a mutation through the resident host.
        """
        return build_new_cron_job(
            job_id=job_id,
            name=name,
            cron_expr=cron_expr,
            timezone=timezone,
            description=description,
            targets=targets,
            enabled=enabled,
            wake_offset_seconds=wake_offset_seconds,
            session_id=session_id,
            chat_type=chat_type,
            mode=mode,
            delete_after_run=delete_after_run,
            timeout_seconds=timeout_seconds,
            project_id=project_id,
            model_name=model_name,
            model_selection=model_selection,
            mcp=mcp,
            app_id=app_id,
            work_mode=work_mode,
            user_id=user_id,
            group_id=group_id,
            bot_id=bot_id,
            credential_ref=credential_ref,
        )

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
        project_id: str = "",
        model_name: str | None = None,
        model_selection: dict[str, str] | None = None,
        mcp: list[str] | None = None,
        app_id: str = "",
        work_mode: str = DEFAULT_WEB_WORK_MODE,
        user_id: str = "",
        group_id: str = "",
        bot_id: str = "",
        credential_ref: str = "",
    ) -> CronJob:
        job = self.build_job(
            job_id=job_id,
            name=name,
            cron_expr=cron_expr,
            timezone=timezone,
            description=description,
            targets=targets,
            enabled=enabled,
            wake_offset_seconds=wake_offset_seconds,
            session_id=session_id,
            chat_type=chat_type,
            mode=mode,
            delete_after_run=delete_after_run,
            timeout_seconds=timeout_seconds,
            project_id=project_id,
            model_name=model_name,
            model_selection=model_selection,
            mcp=mcp,
            app_id=app_id,
            work_mode=work_mode,
            user_id=user_id,
            group_id=group_id,
            bot_id=bot_id,
            credential_ref=credential_ref,
        )
        await self._upsert_job(job)
        return job

    async def update_job(
        self,
        job_id: str,
        patch: dict[str, Any],
        *,
        guard: Callable[[CronJob], None] | None = None,
    ) -> CronJob:
        job_id = str(job_id or "").strip()
        if not job_id:
            raise ValueError("id is required")

        # 读-改-写必须单次持锁完成（与母仓 #4964 修复同款）：调度器在触发点
        # 标 expired/enabled 与执行完成回写 last_session_id 是并发写入者，
        # 读与写分开加锁时后写者会基于过期快照整条覆盖前者的字段。
        def _body() -> CronJob:
            data = self._read_snapshot_data() if guard is not None else self._read_json_unlocked()
            if guard is not None:
                self._parse_snapshot(data)
            jobs_raw = data.get("jobs") or []
            if not isinstance(jobs_raw, list):
                jobs_raw = []
            # 与 get_job 的读取路径保持一致：兜底缺 work_mode 的老数据。
            if guard is None:
                jobs_raw, _ = migrate_work_mode_on_items(jobs_raw)
            for index, item in enumerate(jobs_raw):
                if not isinstance(item, dict):
                    continue
                if str(item.get("id") or "").strip() != job_id:
                    continue
                try:
                    if guard is not None:
                        item = dict(item)
                        migrate_work_mode_on_items([item])
                    existing = CronJob.from_dict(item)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Ignoring invalid cron job id=%s: %s", job_id, exc)
                    raise KeyError("job not found") from None
                if guard is not None:
                    guard(existing)
                updated = apply_cron_job_patch(existing, dict(patch or {}))
                if guard is not None:
                    guard(updated)
                jobs_raw[index] = updated.to_dict()
                data["version"] = int(data.get("version") or 1)
                data["jobs"] = jobs_raw
                self._write_json_unlocked(data)
                return updated
            raise KeyError("job not found")

        return await self._run_locked(_body)

    async def delete_job(
        self,
        job_id: str,
        *,
        force: bool = False,
        guard: Callable[[CronJob], None] | None = None,
    ) -> bool:
        job_id = str(job_id or "").strip()
        if not job_id:
            return False
        if not force and guard is None:
            ensure_proactive_deletable(await self.get_job(job_id), force=False)

        def _body() -> bool:
            data = self._read_snapshot_data() if guard is not None else self._read_json_unlocked()
            if guard is not None:
                self._parse_snapshot(data)
            jobs_raw = data.get("jobs") or []
            if not isinstance(jobs_raw, list):
                jobs_raw = []
            kept: list[dict[str, Any]] = []
            deleted = False
            for item in jobs_raw:
                if not isinstance(item, dict):
                    continue
                if str(item.get("id") or "").strip() == job_id:
                    if guard is not None:
                        existing = CronJob.from_dict(item)
                        guard(existing)
                        ensure_proactive_deletable(existing, force=force)
                    deleted = True
                    continue
                kept.append(item)
            data["version"] = int(data.get("version") or 1)
            data["jobs"] = kept
            if deleted:
                self._write_json_unlocked(data)
            return deleted

        return await self._run_locked(_body)

    async def disable_project_jobs(self, project_id: str) -> None:
        def body():
            data = self._read_json_unlocked()
            changed = False
            for job in data.get("jobs", []):
                if job.get("project_id") == project_id and job.get("enabled", True):
                    job["enabled"] = False
                    changed = True
            if changed:
                self._write_json_unlocked(data)
        await self._run_locked(body)

    async def _upsert_job(self, job: CronJob) -> None:
        def _body() -> None:
            data = self._read_json_unlocked()
            jobs_raw = data.get("jobs") or []
            if not isinstance(jobs_raw, list):
                jobs_raw = []
            out: list[dict[str, Any]] = []
            found = False
            for item in jobs_raw:
                if not isinstance(item, dict):
                    continue
                if str(item.get("id") or "").strip() == job.id:
                    out.append(job.to_dict())
                    found = True
                else:
                    out.append(item)
            if not found:
                out.append(job.to_dict())
            data["version"] = int(data.get("version") or 1)
            data["jobs"] = out
            self._write_json_unlocked(data)

        await self._run_locked(_body)

    async def _read_json(self) -> dict[str, Any]:
        return await self._run_locked(self._read_json_unlocked)

    def _read_json_unlocked(self, *, strict: bool = False) -> dict[str, Any]:
        path = self._path
        try:
            if not path.exists():
                return {"version": 1, "jobs": []}
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw.strip() else {}
            if not isinstance(data, dict):
                if strict:
                    raise ValueError("cron store must be an object")
                return {"version": 1, "jobs": []}
            if "version" not in data:
                data["version"] = 1
            if "jobs" not in data:
                data["jobs"] = []
            return data
        except Exception:
            if strict:
                raise
            return {"version": 1, "jobs": []}

    def _write_json_unlocked(self, data: dict[str, Any]) -> None:
        path = self._path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)

    async def watch(self, callback: Callable[[], Awaitable[None]]) -> None:
        del callback
        raise NotImplementedError("FileCronJobStore does not support etcd watch")

    async def aclose(self) -> None:
        return None

    @staticmethod
    def _normalize_targets(targets: Any) -> list[CronTarget]:
        out: list[CronTarget] = []
        if isinstance(targets, list):
            for item in targets:
                if isinstance(item, CronTarget):
                    out.append(item)
                elif isinstance(item, dict):
                    out.append(CronTarget.from_dict(item))
        if not out:
            raise ValueError("targets is required")
        return out


# Personal-edition / historical import path.
CronJobStore = FileCronJobStore

__all__ = [
    "FileCronJobStore",
    "CronJobStore",
    "CronJob",
    "_PROACTIVE_TICK_MODE",
    "_ProactiveJobProtected",
]
