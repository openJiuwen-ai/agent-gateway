# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _resolve_agent_client(agent_client: Any) -> Any:
    if isinstance(agent_client, dict):
        return agent_client.get("value")
    return agent_client


def register_tui_cron_handlers(
    channel: Any,
    path: str,
    cron_controller_ref: Any,
    agent_client: Any,
) -> None:
    """Register TUI cron.job.* handlers."""

    def _get_cron():
        """Resolve cron_controller from ref dict or direct instance."""
        if isinstance(cron_controller_ref, dict):
            return cron_controller_ref.get("value")
        return cron_controller_ref

    def _cron_job_field(job, name, default=""):
        """Read a field from ``CronController.get_job`` output (dict or object).

        Real ``get_job`` returns ``CronJob.to_dict()``.  ``getattr`` on a dict
        always yields the default and would reject every authenticated update
        with "job not found".  Keep the object fallback for tests and callers
        that return ``CronJob``-like objects.
        """
        if job is None:
            return default
        if isinstance(job, dict):
            return job.get(name, default)
        return getattr(job, name, default)

    async def _cron_job_list(ws, req_id, params, session_id, user_id=None):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        try:
            # 连接带 user_id 时只列该用户的任务:过滤在 controller 内先于
            # 准入闸门执行,既避免跨用户数据可见,也避免替别人的沙箱付超时。
            uid = str(user_id or getattr(ws, "_gateway_user_id", None) or "").strip()
            jobs = await cc.list_jobs(user_id=uid or None)
            await channel.send_response(ws, req_id, ok=True, payload={"jobs": jobs})
        except Exception as exc:
            logger.warning("[cron.job.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _cron_job_meta(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        try:
            await channel.send_response(ws, req_id, ok=True, payload=cc.job_metadata())
        except Exception as exc:
            logger.warning("[cron.job.meta] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _cron_job_get(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            job = await cc.get_job(job_id)
            if job is None:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except Exception as exc:
            logger.warning("[cron.job.get] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _cron_job_create(ws, req_id, params, session_id, user_id=None):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        try:
            if session_id:
                params["session_id"] = session_id
            # 与 Web _cron_job_create 对齐：写入创建者，执行时透传 AgentOS X-Session-Context。
            uid = str(user_id or getattr(ws, "_gateway_user_id", None) or "").strip()
            if uid:
                params["user_id"] = uid
            # 共享目录单用户才可在 Gateway 读取 session metadata 补齐项目目录。
            # AgentOS 用户 session 不在 Gateway 部署目录，不能在这里作本地反查。
            if "project_dir" not in params and session_id:
                try:
                    from agent_gateway.common.external_providers import provider

                    meta = await provider("session_metadata").get_session_metadata_async(
                        session_id, cache_bust=True, user_id=user_id,
                    )
                    if isinstance(meta, dict):
                        pd = meta.get("project_dir")
                        if isinstance(pd, str) and pd.strip():
                            params["project_dir"] = pd.strip()
                except Exception:  # noqa: BLE001
                    pass
            # A3g：项目绑定统一经目标 AgentServer 解析（legacy 单用户的本地
            # AgentServer 同样服务该方法，注入目录=共享目录）。
            from agent_gateway.routing.e2a_proxy import (
                resolve_agent_cron_project_binding,
            )

            bound, binding = await resolve_agent_cron_project_binding(
                agent_client=_resolve_agent_client(agent_client), params=params,
                user_id=uid or None, channel_id="tui", session_id=session_id,
            )
            if not bound:
                await channel.send_response(
                    ws, req_id, ok=False,
                    error=str(binding.get("error") or "cron project binding failed"),
                    code=str(binding.get("code") or "SERVICE_UNAVAILABLE"),
                )
                return
            resolved_project_id = binding.get("project_id")
            resolved_work_mode = binding.get("work_mode")
            if (
                not isinstance(resolved_project_id, str)
                or not isinstance(resolved_work_mode, str)
                or not resolved_work_mode.strip()
            ):
                await channel.send_response(
                    ws, req_id, ok=False,
                    error="invalid cron project binding",
                    code="BAD_REQUEST",
                )
                return
            params.update({
                "project_id": resolved_project_id,
                "work_mode": resolved_work_mode,
            })
            params.pop("project_dir", None)
            params["_agentos_project_binding_verified"] = True
            job = await cc.create_job(params)
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except Exception as exc:
            logger.warning("[cron.job.create] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")

    async def _cron_job_update(ws, req_id, params, session_id, user_id=None):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        patch = params.get("patch") or {}
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        if not isinstance(patch, dict):
            await channel.send_response(ws, req_id, ok=False, error="patch must be object", code="BAD_REQUEST")
            return
        try:
            patch = dict(patch)
            # 归属校验：与 Web _get_owned_cron_job 语义一致（空 user_id 保持单用户旧行为；
            # 带 user_id 时禁止跨用户读取/更新，包括归属为空的历史 job）。
            uid = str(user_id or getattr(ws, "_gateway_user_id", None) or "").strip()
            existing = None
            if uid:
                existing = await cc.get_job(job_id)
                # 与 Web _get_owned_cron_job 相同的 dict/object 双态读取：
                # 真实 CronController.get_job 返回 to_dict() 的 dict。
                owner_field = _cron_job_field(existing, "user_id", "")
                if existing is None or str(owner_field or "").strip() != uid:
                    await channel.send_response(
                        ws, req_id, ok=False, error="job not found", code="NOT_FOUND"
                    )
                    return
            # 仅当 patch 涉及 project 字段时才解析项目绑定（避免非项目字段的
            # update 因用户侧项目解析失败而被整体拒绝；单用户不 resolve）。
            has_project_fields = "project_id" in patch or "project_dir" in patch
            if has_project_fields:
                from agent_gateway.routing.e2a_proxy import (
                    resolve_agent_cron_project_binding,
                )

                if existing is None:
                    existing = await cc.get_job(job_id)
                binding_params = dict(patch)
                binding_params.setdefault(
                    "work_mode", _cron_job_field(existing, "work_mode", "") or "code"
                )
                bound, binding = await resolve_agent_cron_project_binding(
                    agent_client=_resolve_agent_client(agent_client), params=binding_params,
                    user_id=uid or None, channel_id="tui",
                    session_id=_cron_job_field(existing, "session_id", None),
                )
                if not bound:
                    await channel.send_response(
                        ws, req_id, ok=False,
                        error=str(binding.get("error") or "cron project binding failed"),
                        code=str(binding.get("code") or "SERVICE_UNAVAILABLE"),
                    )
                    return
                resolved_project_id = binding.get("project_id")
                resolved_work_mode = binding.get("work_mode")
                if (
                    not isinstance(resolved_project_id, str)
                    or not isinstance(resolved_work_mode, str)
                    or not resolved_work_mode.strip()
                ):
                    await channel.send_response(
                        ws, req_id, ok=False,
                        error="invalid cron project binding",
                        code="BAD_REQUEST",
                    )
                    return
                patch.update({
                    "project_id": resolved_project_id,
                    "work_mode": resolved_work_mode,
                })
                patch.pop("project_dir", None)
                patch["_agentos_project_binding_verified"] = True
            job = await cc.update_job(job_id, patch)
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError as exc:
            # ZoneInfoNotFoundError is a subclass of KeyError; only treat
            # bare "job not found" KeyError as NOT_FOUND, otherwise surface
            # the real error message.
            if "job not found" in str(exc):
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            else:
                logger.warning("[cron.job.update] %s", exc)
                await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except Exception as exc:
            logger.warning("[cron.job.update] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")

    async def _cron_job_delete(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            deleted = await cc.delete_job(job_id)
            if not deleted:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            await channel.send_response(ws, req_id, ok=True, payload={"deleted": True})
        except Exception as exc:
            from agent_gateway.lifecycle_errors import LifecycleError
            code = exc.code if isinstance(exc, LifecycleError) else (getattr(exc, "code", None) or "INTERNAL_ERROR")
            logger.warning("[cron.job.delete] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code=code)

    async def _cron_job_toggle(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        enabled = params.get("enabled", None)
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        if enabled is None:
            await channel.send_response(ws, req_id, ok=False, error="enabled is required", code="BAD_REQUEST")
            return
        try:
            job = await cc.toggle_job(job_id, bool(enabled))
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except Exception as exc:
            logger.warning("[cron.job.toggle] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _cron_job_preview(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        count = params.get("count", 5)
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            next_runs = await cc.preview_job(job_id, int(count) if count is not None else 5)
            await channel.send_response(ws, req_id, ok=True, payload={"next": next_runs})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except Exception as exc:
            logger.warning("[cron.job.preview] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")

    async def _cron_job_run_now(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            # 先取 job 拿 last_session_id（回退值），再触发 run_now 取 run_id
            # 对齐 chat.send 的 {accepted, session_id} 语义
            job = await cc.get_job(job_id)
            if job is None:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            run_info = await cc.run_now_info(job_id)
            await channel.send_response(
                ws, req_id, ok=True,
                payload={
                    "accepted": True,
                    "run_id": run_info.get("run_id", ""),
                    "session_id": run_info.get("session_id", ""),
                },
            )
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except Exception as exc:
            logger.warning("[cron.job.run_now] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    channel.register_local_handler(path, "cron.job.list", _cron_job_list)
    channel.register_local_handler(path, "cron.job.meta", _cron_job_meta)
    channel.register_local_handler(path, "cron.job.get", _cron_job_get)
    channel.register_local_handler(path, "cron.job.create", _cron_job_create)
    channel.register_local_handler(path, "cron.job.update", _cron_job_update)
    channel.register_local_handler(path, "cron.job.delete", _cron_job_delete)
    channel.register_local_handler(path, "cron.job.toggle", _cron_job_toggle)
    channel.register_local_handler(path, "cron.job.preview", _cron_job_preview)
    channel.register_local_handler(path, "cron.job.run_now", _cron_job_run_now)
