# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Cron RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any

from agent_gateway.common.work_mode import (
    DEFAULT_PROJECT_ID_CODE,
    DEFAULT_TUI_WORK_MODE,
    DEFAULT_WEB_WORK_MODE,
    SUPPORTED_WORK_MODES,
    is_default_project_id,
)


def register_cron_handlers(
    channel: Any, cron_controller: Any, agent_client: Any, _resolve: Any
) -> None:
    """Register cron RPC handlers in their original route order."""

    def _get_cron():
        return _resolve(cron_controller)

    async def _get_owned_cron_job(cc, job_id: str, user_id: str | None):
        """Return a job only when it belongs to the authenticated Web user.

        Empty ``user_id`` preserves the legacy unauthenticated single-user
        behavior.  Authenticated connections must never access a different
        user's job, including historical jobs whose owner is empty.

        ``cc.get_job`` returns a plain dict (``CronJob.to_dict()``), so the
        owner field must be read with ``dict.get``; ``getattr`` on a dict
        always yields the empty default and would reject every authenticated
        access with "job not found".  Keep the object fallback for tests and
        callers that return ``CronJob``-like objects.
        """
        job = await cc.get_job(job_id)
        owner = str(user_id or "").strip()
        if job is None:
            return None
        owner_field = (
            job.get("user_id", "") if isinstance(job, dict) else getattr(job, "user_id", "")
        )
        if owner and str(owner_field or "").strip() != owner:
            return None
        return job

    async def _cron_job_list(ws, req_id, params, session_id, user_id=None):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        # 按 web 连接 user_id 过滤:只返回该用户创建的定时任务(隔离)。
        # 过滤在 controller 内进行,先于按属主路由的准入闸门,避免为其他
        # 用户的沙箱冷启动/超时买单。user_id 为空(连接未带)时维持原行为
        # (返回全部);agent 内部创建的 cron(user_id 空串)在带 user_id 的
        # 连接下被过滤掉(对人类用户不可见)。
        uid_str = str(user_id or "").strip()
        jobs = await cc.list_jobs(user_id=uid_str or None)
        # 可选按 project_id 过滤(支持 default/default_code 虚拟项目)
        if isinstance(params, dict):
            raw_pid = params.get("project_id")
            if isinstance(raw_pid, str) and raw_pid.strip():
                filter_pid = raw_pid.strip()
                if is_default_project_id(filter_pid):
                    # 默认项目:按 filter_pid 精确匹配,用 work_mode 消歧空 project_id。
                    # 避免 default 过滤返回 default_code 的 job（反之亦然）。
                    # 兼容未迁移的老 job(work_mode 为空或非法):按 channel_id 推断
                    # 兜底 work_mode,避免迁移失败场景下 default_code 过滤漏掉老 job。
                    target_wm = DEFAULT_TUI_WORK_MODE if filter_pid == DEFAULT_PROJECT_ID_CODE \
                        else DEFAULT_WEB_WORK_MODE
                    filtered = []
                    for j in jobs:
                        j_pid = j.get("project_id")
                        if j_pid == filter_pid:
                            filtered.append(j)
                            continue
                        if not j_pid:
                            j_wm = j.get("work_mode")
                            if isinstance(j_wm, str) and j_wm.strip() in SUPPORTED_WORK_MODES:
                                if j_wm == target_wm:
                                    filtered.append(j)
                            else:
                                # work_mode 缺失/非法(未迁移的老 job):
                                # 按 target_wm 匹配 default(default→work)或不匹配
                                # default_code(default_code→code,老 job 兜底 work 不匹配)
                                if target_wm == DEFAULT_WEB_WORK_MODE:
                                    filtered.append(j)
                    jobs = filtered
                else:
                    filtered = []
                    for j in jobs:
                        if j.get("project_id") == filter_pid:
                            filtered.append(j)
                    jobs = filtered
        await channel.send_response(ws, req_id, ok=True, payload={"jobs": jobs})

    async def _cron_job_meta(ws, req_id, params, session_id):
        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron not available", code="INTERNAL_ERROR")
            return
        await channel.send_response(ws, req_id, ok=True, payload=cc.job_metadata())

    async def _cron_job_get(ws, req_id, params, session_id, user_id=None):
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
        job = await _get_owned_cron_job(cc, job_id, user_id)
        if job is None:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        await channel.send_response(ws, req_id, ok=True, payload={"job": job})

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
            # 注入创建者 user_id（web 连接携带），执行时透传给 faas 的 X-Session-Context，
            # 否则 CreateSandbox 拉不起 → 60s 超时（见 plan-cron-user-id）。
            if user_id:
                params["user_id"] = str(user_id).strip()
            # 登录会话 id：服务端专有键，无条件覆盖客户端传值。controller 用它给
            # 登录免费模型绑定华为账号凭据句柄（与路由 user_id 是两套独立用户
            # 体系），用后即弃、不落任务数据。
            params["_auth_session"] = getattr(ws, "_jiuwen_auth_session", "") or ""
            # 仅共享目录单用户可由 Gateway 从 session metadata 补 project_dir。
            # AgentOS 下 metadata 在目标 AgentServer 用户目录，Gateway 读部署目录会
            # 误把任务归到默认项目；缺省时让目标 AgentServer / 显式 project_id 决定。
            # 注意：显式传空串 "" 等价于归默认项目，不可覆盖——用 key presence 区分。
            if "project_dir" not in params and session_id:
                # A2g：经注入点读 metadata（AgentServer 侧写入），不再
                # 直读 gateway 本地文件
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
            # AgentServer 同样服务 PROJECT_CRON_RESOLVE_BINDING，注入目录=共享
            # 目录），controller 不再保留 gateway 本地 project_store 反查。
            from agent_gateway.routing.e2a_proxy import (
                resolve_agent_cron_project_binding,
            )

            bound, binding = await resolve_agent_cron_project_binding(
                agent_client=_resolve(agent_client), params=params,
                user_id=user_id, channel_id="web", session_id=session_id,
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
        except Exception as e:  # noqa: BLE001
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")

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
            if await _get_owned_cron_job(cc, job_id, user_id) is None:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            patch = dict(patch)
            # 仅当 patch 涉及 project 字段时才解析项目绑定（避免非项目字段的
            # update 因用户侧项目解析失败而被整体拒绝）。A3g：解析统一经目标
            # AgentServer（PROJECT_CRON_RESOLVE_BINDING），不再区分 AgentOS/
            # legacy 单用户。
            has_project_fields = "project_id" in patch or "project_dir" in patch
            if has_project_fields:
                from agent_gateway.routing.e2a_proxy import (
                    resolve_agent_cron_project_binding,
                )

                existing = await _get_owned_cron_job(cc, job_id, user_id)
                # 与 _get_owned_cron_job 相同的 dict/object 双态读取：
                # 真实 CronController.get_job 返回 to_dict() 的 dict。
                existing_work_mode = str(
                    existing.get("work_mode", "")
                    if isinstance(existing, dict)
                    else getattr(existing, "work_mode", "")
                )
                existing_session_id = (
                    existing.get("session_id")
                    if isinstance(existing, dict)
                    else getattr(existing, "session_id", None)
                )
                binding_params = dict(patch)
                binding_params.setdefault("work_mode", existing_work_mode)
                bound, binding = await resolve_agent_cron_project_binding(
                    agent_client=_resolve(agent_client), params=binding_params,
                    user_id=user_id, channel_id="web", session_id=existing_session_id,
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
            # 登录会话 id（服务端专有键，见 _cron_job_create）：改选登录免费模型时
            # 重绑华为账号凭据句柄；无条件覆盖客户端传值。
            patch["_auth_session"] = getattr(ws, "_jiuwen_auth_session", "") or ""
            job = await cc.update_job(job_id, patch)
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except Exception as e:  # noqa: BLE001
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")

    async def _cron_job_delete(ws, req_id, params, session_id, user_id=None):
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
        # proactive.tick job 由主动推荐开关自动创建/删除，禁止面板删除。
        existing = await _get_owned_cron_job(cc, job_id, user_id)
        if existing is None:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        if existing is not None and str(getattr(existing, "mode", "") or "").strip().lower() == "proactive.tick":
            await channel.send_response(
                ws, req_id, ok=False,
                error="主动推荐定时任务由设置→主动推荐开关控制，不能在面板删除；请到设置关闭开关。",
                code="BAD_REQUEST",
            )
            return
        try:
            deleted = await cc.delete_job(job_id)
        except Exception as exc:
            from agent_gateway.lifecycle_errors import LifecycleError
            code = exc.code if isinstance(exc, LifecycleError) else (getattr(exc, "code", None) or "DELETE_FAILED")
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code=code
            )
            return
        if not deleted:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        await channel.send_response(ws, req_id, ok=True, payload={"deleted": True})

    async def _cron_job_toggle(ws, req_id, params, session_id, user_id=None):
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
        # proactive.tick job 的 enabled 由 config 开关驱动，禁止面板手动切换。
        existing = await _get_owned_cron_job(cc, job_id, user_id)
        if existing is None:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        if existing is not None and str(getattr(existing, "mode", "") or "").strip().lower() == "proactive.tick":
            await channel.send_response(
                ws, req_id, ok=False,
                error="主动推荐定时任务由设置→主动推荐开关控制，不能在面板启停。",
                code="BAD_REQUEST",
            )
            return
        try:
            job = await cc.toggle_job(job_id, bool(enabled))
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")

    async def _cron_job_preview(ws, req_id, params, session_id, user_id=None):
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
            if await _get_owned_cron_job(cc, job_id, user_id) is None:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            next_runs = await cc.preview_job(job_id, int(count) if count is not None else 5)
            await channel.send_response(ws, req_id, ok=True, payload={"next": next_runs})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except Exception as e:  # noqa: BLE001
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")

    async def _cron_job_run_now(ws, req_id, params, session_id, user_id=None):
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
            # 对齐 chat.send 的 {accepted, session_id} 语义；首次执行 last_session_id
            # 为 None → session_id 空串（会话尚未就绪，前端轮询 cron.job.get 获取）
            job = await _get_owned_cron_job(cc, job_id, user_id)
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
        except Exception as e:  # noqa: BLE001
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    channel.register_method("cron.job.list", _cron_job_list)
    channel.register_method("cron.job.meta", _cron_job_meta)
    channel.register_method("cron.job.get", _cron_job_get)
    channel.register_method("cron.job.create", _cron_job_create)
    channel.register_method("cron.job.update", _cron_job_update)
    channel.register_method("cron.job.delete", _cron_job_delete)
    channel.register_method("cron.job.toggle", _cron_job_toggle)
    channel.register_method("cron.job.preview", _cron_job_preview)
    channel.register_method("cron.job.run_now", _cron_job_run_now)
