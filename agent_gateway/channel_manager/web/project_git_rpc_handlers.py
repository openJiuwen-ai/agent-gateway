# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Project Git RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.routing.e2a_proxy import proxy_unary_request


logger = logging.getLogger(__name__)


def register_project_git_handlers(
    channel: Any,
    agent_client: Any,
    _resolve: Any,
) -> None:
    """Register project Git thin proxies in their original route order."""

    def _mark_git_watcher_dirty(project_id: str) -> None:
        """写操作成功后唤醒 /ws/git watcher(阶段 10 注入后生效)。

        阶段 7 时 ``git_watcher_registry`` 属性可能尚未注入,此处防御性调用;
        阶段 10 在 WebChannel 构造后注入 ``git_watcher_registry`` 即自动启用。
        """
        registry = getattr(channel, "git_watcher_registry", None)
        if registry is None:
            return
        try:
            registry.mark_dirty(project_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "[ProjectGit] mark_dirty failed (project=%s): %s",
                project_id, exc,
            )

    def _make_git_proxy_handler(
        req_method, label, *, doc, mark_dirty=False, mark_dirty_on_failure=False
    ):
        """构造一个 Git RPC 薄代理 handler。
        Git 工作区/项目表/diff 历史访问只发生在目标 AgentServer（E2A 转发）；
        Gateway 仅保留 /ws/git watcher 唤醒（mark_dirty）作为部署侧连接维护。
        统一 ``user_id=None``（AgentOS 下由路由层按连接上下文注入）与
        ``preserve_error_payload=True``（Git 已定义结构化错误明细）。
        """

        async def _handler(ws, req_id, params, session_id, user_id=None):
            callbacks = {}
            if mark_dirty or mark_dirty_on_failure:
                project_id = str((params or {}).get("project_id") or "").strip()

                def _on_done(ok, _payload):
                    # 成功与"已发生局部变更的失败"都唤醒 watcher（commit 失败但 index
                    # 已改变时同样需要刷新）；是否行动由调用方在回调内判定。
                    if ok or mark_dirty_on_failure:
                        return _mark_git_watcher_dirty(project_id)
                    return None

                callbacks["on_done"] = _on_done
            await proxy_unary_request(
                channel=channel,
                agent_client=_resolve(agent_client),
                ws=ws,
                req_id=req_id,
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=req_method,
                label=label,
                preserve_error_payload=True,
                **callbacks,
            )

        _handler.__doc__ = doc
        return _handler

    channel.register_method("project.git.status", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_STATUS,
        "project.git.status",
        doc="Forward Git status to the AgentServer that owns the worktree.",
    ))
    channel.register_method("project.git.probe", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_PROBE,
        "project.git.probe",
        doc="Probe Git in AgentServer and refresh the Gateway watcher afterwards.",
        mark_dirty=True,
    ))
    channel.register_method("project.git.init", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_INIT,
        "project.git.init",
        doc="Initialize Git in the target AgentServer's project worktree.",
        mark_dirty=True,
    ))
    channel.register_method("project.git.switch_branch", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_SWITCH_BRANCH,
        "project.git.switch_branch",
        doc="Switch a branch in the target AgentServer's project worktree.",
        mark_dirty=True,
    ))
    channel.register_method("project.git.create_branch", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_CREATE_BRANCH,
        "project.git.create_branch",
        doc="Create a branch in the target AgentServer's project worktree.",
        mark_dirty=True,
    ))
    channel.register_method("project.git.commit", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_COMMIT,
        "project.git.commit",
        doc=(
            "提交当前工作区改动到当前分支(设计文档 §4.9 ``project.git.commit``)。"
            "``stage_all=true`` 先 ``git add -A`` 暂存全部;``paths`` 显式指定暂存路径"
            "(与 ``stage_all`` 互斥)。``amend=true`` 覆盖最近一次提交。成功/失败后调"
            "``mark_dirty`` 触发 /ws/git 立即重算(HEAD/is_dirty/stats 变化)。"
        ),
        mark_dirty=True,
        mark_dirty_on_failure=True,
    ))
    channel.register_method("project.git.push", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_PUSH,
        "project.git.push",
        doc=(
            "推送本地分支到远程(设计文档 §4.10 ``project.git.push``)。"
            "``remote`` 默认 ``origin``;``branch`` 不传时用当前分支(detached HEAD"
            "须显式传 ``branch``)。``force=true`` 使用 ``--force-with-lease``(更安全)。"
            "``delete=true`` 删除远程分支(与 ``set_upstream``/``force`` 互斥)。push"
            "涉及网络,超时独立配置(``GIT_PUSH_TIMEOUT_SEC``,默认 60s)。成功后调"
            "``mark_dirty`` 刷新 /ws/git(主要更新 upstream 字段)。"
        ),
        mark_dirty=True,
    ))
    channel.register_method("project.git.diff_status", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_DIFF_STATUS,
        "project.git.diff_status",
        doc=(
            "拉取当前分支 diff 和上一轮对话 diff 的快照(设计文档 §4.1.16)。"
            "``include_files=true`` 返回文件列表;``include_hunks=true`` 隐含"
            "``include_files=true`` 并返回 hunk。transient 状态下 ``current``"
            "为 ``null``,仍成功返回 ``repo.transient=true``。"
        ),
    ))
    channel.register_method("project.git.turn_diff_list", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_TURN_DIFF_LIST,
        "project.git.turn_diff_list",
        doc="查询历史 Diff 摘要列表。",
    ))
    channel.register_method("project.git.turn_diff", _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_TURN_DIFF,
        "project.git.turn_diff",
        doc="查询指定轮次 Diff 详情。",
    ))
