# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Session RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import secrets
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.routing.e2a_proxy import proxy_unary_request


def register_session_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register session RPC handlers in their original route order."""

    async def _session_list(ws, req_id, params, session_id, user_id=None):
        """返回会话列表，包含完整的会话管理信息。

        经统一薄代理 E2A 转发目标 AgentServer（SessionAdapter 处理），
        AgentServer 输出与迁移前 Web fallback 投影（to_session_info）完全一致。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_LIST,
            label="session.list",
        )

    async def _session_create(ws, req_id, params, session_id, user_id=None):
        """创建一个新 session（在 agent/sessions 下创建一个新目录）。

        project_id / project_dir / work_mode 绑定规则（A3g）:
          - work_mode 归一化(详见 resolve_session_work_mode_params):未传时按通道
            推断(Web→work,TUI→code);显式传非法值返回 BAD_REQUEST;
          - project_id / project_dir 绑定在目标 AgentServer 解析（经
            resolve_session_work_mode_params 归一后随 session.create E2A 请求
            转发,由 server 侧按注入目录的项目表完成绑定与校验）:
            两者皆空(或 project_id 为 "default"/"default_code" 且 path 为空)→ 默认项目;
            仅传 project_id → 按项目记录自动补齐 project_dir;
            同时传 project_id + project_dir → 校验与项目绑定路径一致,不一致报错;
            仅传 project_dir 而无有效 project_id → 拒绝(BAD_REQUEST)。
          - 真实 project_id 命中后,最终 work_mode 以 Project 记录为准;若请求显式
            传了 work_mode 且与 Project 记录不一致,返回 BAD_REQUEST。
        绑定后 project_id / project_dir / work_mode 不可变(首次锁定)。
        """
        if not isinstance(params, dict):
            await channel.send_response(
                ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST",
            )
            return
        # 统一薄代理 E2A 转发（envelope.user_id 承载认证身份，
        # drop session_id / 缺省注入 create_token 保持与迁移前一致）。
        create_params = dict(params)
        create_params.setdefault("create_token", secrets.token_hex(16))
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=create_params,
            session_id=None,
            user_id=user_id,
            req_method=ReqMethod.SESSION_CREATE,
            label="session.create",
            drop_params=("session_id",),
            default_error_code="SESSION_CREATE_FAILED",
        )

    async def _session_get_metadata(ws, req_id, params, session_id, user_id=None):
        """返回单个会话的元数据（mode / model / project_dir / last_user_message_at 等）。

        经统一薄代理 E2A 转发目标 AgentServer（SessionAdapter
        SESSION_GET_METADATA），由注入目录读取（O(1)，不扫描目录）。运行态不属于
        持久化 metadata：由 AgentServer 返回权威执行快照，Gateway 透传，
        避免 Gateway 重启后用空的本地 busy 缓存覆盖仍在执行的 Worker 状态。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_GET_METADATA,
            label="session.get_metadata",
        )

    async def _session_message_continue_queued(ws, req_id, params, session_id, user_id=None):
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_MESSAGE_CONTINUE_QUEUED,
            label="session.message.continue_queued",
        )

    async def _session_plan_status(ws, req_id, params, session_id, user_id=None):
        """查询会话当前是否处于计划模式（只读，刷新后恢复前端「计划」标签）。

        转发 AgentServer ``SESSION_PLAN_STATUS``：单 agent 读 live
        ``plan_mode``，集群 / 读不到 agent 状态时回退 metadata.mode。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_PLAN_STATUS,
            label="session.plan_status",
        )

    async def _session_rename(ws, req_id, params, session_id, user_id=None):
        """重命名会话标题(查询/设置/清除三种语义)。

        经统一薄代理 E2A 转发目标 AgentServer（SESSION_RENAME，
        AgentServer 注入目录 session_metadata），不再 Gateway 本地直写——AgentOS
        下重命名必须落在用户 AgentServer 的会话目录（方案 §10.3 契约）。
        title 不传→查询、空串/纯空白→清除、非空→设置(截断 200 字符)。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_RENAME,
            label="session.rename",
        )

    async def _session_pin(ws, req_id, params, session_id, user_id=None):
        """置顶/取消置顶会话,操作后对所有置顶会话紧凑重编号为 1..N。幂等。

        经统一薄代理 E2A 转发目标 AgentServer（SessionAdapter
        SESSION_PIN，AgentServer 注入目录 session_metadata）。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.SESSION_PIN,
            label="session.pin",
        )

    channel.register_method("session.list", _session_list)
    channel.register_method("session.create", _session_create)
    channel.register_method("session.get_metadata", _session_get_metadata)
    channel.register_method("session.message.continue_queued", _session_message_continue_queued)
    channel.register_method("session.plan_status", _session_plan_status)
    channel.register_method("session.rename", _session_rename)
    channel.register_method("session.pin", _session_pin)
