# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Project RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.routing.e2a_proxy import fetch_agent_unary, proxy_unary_request


logger = logging.getLogger(__name__)


async def _broadcast_project_event(
    channel: Any,
    event: str,
    project_id: str,
    user_id: Any,
) -> None:
    """Notify every connection of ``user_id`` that a project was hidden/restored.

    The project's conversations and cron jobs change visibility with it, so
    other tabs (and the archive page) must refresh; a response to the
    originating socket alone would leave them stale.  Broadcast is scoped
    to the owner's connections to avoid making unrelated users refetch.
    """
    if not project_id:
        return
    payload = {"project_id": project_id}
    for client_ws in list(getattr(channel, "clients", ()) or ()):
        try:
            if str(channel.connection_user_id(client_ws) or "") != str(user_id or ""):
                continue
            await channel.send_event(client_ws, event, payload)
        except Exception:  # noqa: BLE001 - 单条连接异常不得影响其他连接
            logger.debug("project event broadcast failed: %s", event, exc_info=True)


def register_project_handlers(
    channel: Any,
    agent_client: Any,
    _resolve: Any,
    cron_controller: Any = None,
    _schedule_agent_prewarm_sync: Any = None,
) -> None:
    """Register core project RPC handlers in their original route order."""

    async def _project_list(ws, req_id, params, session_id, user_id=None):
        channel.ensure_lifecycle_watch(user_id)
        """获取项目列表(含统计),已排序,包含默认项目。

        filter: ``"all"``(默认) / ``"pinned"`` / ``"unpinned"``
        work_mode: 可选,按工作模式过滤(``"code"`` / ``"work"``),不传则返回全部模式。
            默认项目按 work_mode 拆分:``default``(work)+ ``default_code``(code)。

        统计口径: ``session_count`` / ``last_message_at`` / ``last_user_message_at``
        仅统计该项目的非置顶**普通**会话(``cron_id`` 为空)。置顶会话与 cron 会话
        不计入任何项目统计。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_LIST,
            label="project.list",
        )

    async def _project_get_sessions(ws, req_id, params, session_id, user_id=None):
        """获取项目下的非置顶普通会话列表（目标 AgentServer 执行）。"""
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_GET_SESSIONS,
            label="project.get_sessions",
        )

    async def _project_get_cron_sessions(ws, req_id, params, session_id, user_id=None):
        """获取项目 cron 会话（目标 AgentServer 执行）。"""
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_GET_CRON_SESSIONS,
            label="project.get_cron_sessions",
        )

    async def _project_create(ws, req_id, params, session_id, user_id=None):
        """Forward project creation to the target AgentServer.

        The AgentServer validates paths and owns project/Git state in its
        injected user directory. Gateway only schedules deployment-side warm
        key reconciliation after a successful mutation.
        """
        async def after_create(ok, payload):
            if ok:
                _schedule_agent_prewarm_sync("project.create")
                if payload.get("restored"):
                    await _broadcast_project_event(channel, "project.restored", payload["project_id"], user_id)

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_CREATE,
            label="project.create",
            # 保留 AgentServer 返回的结构化错误明细。
            preserve_error_payload=True,
            on_done=after_create,
        )

    async def _project_rename(ws, req_id, params, session_id, user_id=None):
        """重命名项目,仅修改展示名,不改动工作目录路径。

        默认项目禁止重命名(``FORBIDDEN``)。``name`` 与已有项目(含隐藏)重复时返回 ``CONFLICT``。
        ``name`` 含文件系统非法字符 / 为保留设备名时返回 ``BAD_REQUEST``。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_RENAME,
            label="project.rename",
        )

    async def _project_pin(ws, req_id, params, session_id, user_id=None):
        """置顶/取消置顶项目,操作后对所有置顶项目紧凑重编号为 1..N。幂等。

        默认项目禁止置顶(``FORBIDDEN``)。新置顶项目 ``pin_order`` 默认 0,
        重编号后置于置顶区顶部(与 ``session.pin`` 行为一致)。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_PIN,
            label="project.pin",
        )

    async def _project_remove(ws, req_id, params, session_id, user_id=None):
        """Forward project soft-deletion; stop its cron jobs, clean Git watchers."""
        project_id = str((params or {}).get("project_id") or "").strip()

        if not project_id or project_id in {"default", "default_code"}:
            await channel.send_response(
                ws, req_id, ok=False,
                error="project_id is required" if not project_id else "default project cannot be removed",
                code="BAD_REQUEST" if not project_id else "FORBIDDEN",
            )
            return

        # Removal is accepted immediately; running sessions no longer block it.
        # The AgentServer hides the project and stops those sessions in the
        # background, so this path goes straight to the cron-safe commit.
        async def _after_remove() -> None:
            registry = getattr(channel, "git_watcher_registry", None)
            if registry is not None and project_id:
                registry.cleanup_project(project_id)
            _schedule_agent_prewarm_sync("project.remove")
            # The AgentServer stops these Sessions in the background.  Settle
            # the Gateway's busy cache now: otherwise a later restore would
            # surface them as "still processing" on session.get_metadata.
            stopped = (
                remove_payload.get("stopped_sessions")
                if isinstance(remove_payload, dict)
                else None
            )
            if isinstance(stopped, list):
                clear_busy = getattr(channel, "clear_session_busy", None)
                if callable(clear_busy):
                    for raw_sid in stopped:
                        sid = str(raw_sid or "")
                        if sid:
                            clear_busy(sid)
            await _broadcast_project_event(channel, "project.removed", project_id, user_id)

        class _RemoveCommitError(Exception):
            def __init__(self, payload: dict) -> None:
                self.code = str(payload.get("code") or "BAD_REQUEST")
                super().__init__(str(payload.get("error") or "project.remove failed"))

        remove_payload: dict = {}

        async def commit():
            nonlocal remove_payload
            ok, payload = await fetch_agent_unary(
                agent_client=_resolve(agent_client),
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=ReqMethod.PROJECT_REMOVE,
                channel_id="web",
                label="project.remove",
            )
            if not ok:
                raise _RemoveCommitError(payload)
            remove_payload = payload

        cc = _resolve(cron_controller)
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron service unavailable", code="CRON_STOP_FAILED")
            return
        try:
            cron_stop = await cc.hide_project_jobs(project_id, commit=commit)
        except _RemoveCommitError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code=exc.code)
            return
        except Exception as exc:
            logger.warning("project remove failed: %s", exc, exc_info=True)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="CRON_STOP_FAILED")
            return
        # stopped_cron_jobs 是项目下定时任务总数(含移除前已停用的);0 表示
        # 项目下本就没有定时任务,前端据此只提示"项目已移除",不再附带
        # "其定时任务已停止"。
        if isinstance(cron_stop, dict) and "stopped_cron_jobs" in cron_stop:
            remove_payload = {**remove_payload, "stopped_cron_jobs": cron_stop["stopped_cron_jobs"]}
        await channel.send_response(ws, req_id, ok=True, payload=remove_payload)
        await _after_remove()

    async def _project_restore(ws, req_id, params, session_id, user_id=None):
        """Forward project restoration to the target AgentServer."""
        project_id = str((params or {}).get("project_id") or "").strip()

        async def _after_restore(ok: bool, _payload: object) -> None:
            if not ok:
                return
            _schedule_agent_prewarm_sync("project.restore")
            await _broadcast_project_event(channel, "project.restored", project_id, user_id)

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_RESTORE,
            label="project.restore",
            on_done=_after_restore,
        )

    async def _project_info(ws, req_id, params, session_id, user_id=None):
        """获取单个项目详情(含统计),支持虚拟默认项目。

        ``project_id`` 为 ``"default"`` / ``"default_code"`` 时返回对应虚拟默认项目;
        为真实 project_id 时返回该项目的详情。字段与 ``project.list`` 条目一致。

        统计口径同 ``project.list``:仅统计该项目的非置顶普通会话(``cron_id`` 为空)。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_INFO,
            label="project.info",
        )

    channel.register_method("project.list", _project_list)
    channel.register_method("project.info", _project_info)
    channel.register_method("project.get_sessions", _project_get_sessions)
    channel.register_method("project.get_cron_sessions", _project_get_cron_sessions)
    channel.register_method("project.create", _project_create)
    channel.register_method("project.rename", _project_rename)
    channel.register_method("project.pin", _project_pin)
    channel.register_method("project.remove", _project_remove)
    channel.register_method("project.restore", _project_restore)


def register_project_pinned_sessions_handler(
    channel: Any,
    agent_client: Any,
    _resolve: Any,
) -> None:
    """Register pinned session RPC after lifecycle handlers."""

    async def _project_pinned_sessions(ws, req_id, params, session_id, user_id=None):
        """获取全部置顶会话,按 ``pin_order`` 升序排列。

        置顶会话已从项目分组中剥离,通过本接口独立获取。``project_dir`` 仍指向
        原归属项目。不接受任何参数。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_PINNED_SESSIONS,
            label="project.pinned_sessions",
        )

    channel.register_method("project.pinned_sessions", _project_pinned_sessions)
