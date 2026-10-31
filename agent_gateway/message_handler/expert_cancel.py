# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Targeted expert-cancel ownership checks extracted from MessageHandler."""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.message_handler.runtime_state import expert_stream_route
from agent_gateway.routing.session_metadata_remote import (
    SessionMetadataRemoteUnavailable,
)


logger = logging.getLogger(__name__)


async def is_targeted_expert_cancel(msg: Any) -> bool:
    """Return whether an expert cancel must stay request-scoped."""
    route = expert_stream_route(msg.params)
    if not route or not msg.session_id:
        return False
    try:
        from agent_gateway.common.external_providers import provider

        metadata = await provider("session_metadata").get_session_metadata_async(
            msg.session_id, enable_writeback=False, user_id=msg.user_id,
        )
        owner_id = str((metadata or {}).get("team_name") or "").strip()
    except SessionMetadataRemoteUnavailable:
        # 传输失败≠读为空：owner 未知时宁可照常走定向取消分支（成员
        # 校验由 AgentServer 权威执行，最坏是被拒），绝不放大成会话级
        # 取消——该分支上方的约束是"Never enter the session-wide path"。
        logger.warning(
            "[MessageHandler] expert cancel owner lookup unavailable; "
            "forwarding as targeted cancel: session=%s",
            msg.session_id,
        )
        return True
    except Exception:
        logger.warning(
            "[MessageHandler] expert cancel owner lookup failed: session=%s",
            msg.session_id,
            exc_info=True,
        )
        return False
    # **owner 未知时保守判为「定向」**：这条分支的替代路径是会话级中断，会把 owner
    # 团队正在跑的 turn 一起取消（AgentServer 侧 `_is_targeted_expert_cancel_request`
    # 与本处同向；只有给 chat.send 路由用的 `_is_targeted_expert_request` 兜底方向相反，
    # 那是另一套语义）。
    # 交给 AgentServer 裁决更安全：它会因 owner 未知明确返回 success=false 并给出原因，
    # 而不会取消别人的 turn。查表抛异常时不走这条（元数据服务不可用 ≠ owner 未知，
    # 那时连 target 是否合法都无从判断，维持原有的会话级行为）。
    return not owner_id or route["team_id"] != owner_id
