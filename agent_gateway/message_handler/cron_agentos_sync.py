# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""AgentOS cron-job snapshot sync extracted from MessageHandler."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from agent_gateway.routing.agent_request_timeout import (
    send_agent_request_with_timeout,
)


if TYPE_CHECKING:
    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)

# 只有会进入 Agent turn 的流式请求才需要刷新 AgentServer 的 cron 工具视图。
# 读取类流(history.get 等)不产生 turn,却会按任务属主触发项目准入 RPC。
_CRON_SNAPSHOT_STREAM_METHODS = frozenset({"chat.send", "command.goal"})


async def sync_agentos_cron_jobs(
    handler: "MessageHandler",
    env: Any,
) -> None:
    """Provide the routed AgentServer an ephemeral view of Gateway cron jobs.

    Gateway remains the sole persistent job-store owner.  This one-way E2A
    request occurs before an Agent turn so cron tools can list/update jobs
    after an AgentServer restart without reading a user-local cron file.
    Failure is non-fatal for ordinary chat; mutation attempts will still
    report their Gateway delivery failure through the existing path.
    """
    # 读取类流不进入 Agent turn,不会调用 cron 工具;对它们刷新快照只会
    # 为任务列表付出按属主路由的准入 RPC。
    if str(getattr(env, "method", "") or "") not in _CRON_SNAPSHOT_STREAM_METHODS:
        return
    from agent_gateway.routing.e2a_proxy import is_agentos_routing_client

    if not is_agentos_routing_client(handler.agent_client):
        return
    controller = handler._cron_controller
    if controller is None:
        return
    user_id = str(getattr(env, "user_id", "") or "").strip()
    if not user_id:
        logger.warning(
            "[Cron] skip AgentOS cron snapshot without user_id: request_id=%s",
            env.request_id,
        )
        return
    try:
        jobs = await controller.list_jobs(user_id=user_id)
        from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
        from openjiuwen_protocol.message import ReqMethod

        snapshot_env = e2a_from_agent_fields(
            request_id=f"cron-jobs-sync-{env.request_id}",
            channel_id=env.channel or "web",
            session_id=env.session_id,
            req_method=ReqMethod.CRON_JOBS_SYNC,
            params={"jobs": jobs},
            is_stream=False,
            timestamp=time.time(),
            user_id=user_id,
        )
        response = await send_agent_request_with_timeout(
            handler.agent_client,
            snapshot_env,
            label="cron.jobs.sync",
        )
        if not response.ok:
            logger.warning(
                "[Cron] AgentOS cron snapshot rejected: request_id=%s payload=%s",
                env.request_id,
                response.payload,
            )
    except Exception as exc:  # noqa: BLE001 - must not block normal chat
        logger.warning(
            "[Cron] AgentOS cron snapshot sync failed: request_id=%s error=%s",
            env.request_id,
            exc,
        )
