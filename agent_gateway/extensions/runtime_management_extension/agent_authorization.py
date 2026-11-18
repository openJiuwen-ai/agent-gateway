# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Per-request Agent resource admission, independent of Runtime pool routing."""
import logging
from datetime import datetime, timezone

from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension.invoke_ids import (
    routing_triple_from_envelope,
)
from agent_gateway.extensions.runtime_management_extension.session_route_client import (
    FatalRouteError,
)

logger = logging.getLogger(__name__)
_enterprise_absent_logged = False

# 本单约束发起/继续 Agent 执行的对话入口。会话管理、历史查询与中断
# 沿用各自接口契约，不在这里按前缀扩大授权范围。
CHAT_METHODS = frozenset({
    "chat.send", "chat.resume", "chat.user_answer", "chat.swarmflow_reply",
})


def _enterprise_checkers():
    """Load enterprise admission dependencies; missing modules deny execution.

    Only ModuleNotFoundError is translated here. Other import failures still
    propagate instead of silently disabling admission.
    """
    global _enterprise_absent_logged
    try:
        from agent_gateway.config.enterprise.access import (
            get_enterprise_record_repository,
        )
        from agent_gateway.config.enterprise.expressions import matches
    except ModuleNotFoundError as exc:
        if not _enterprise_absent_logged:
            _enterprise_absent_logged = True
            logger.error(
                "[RuntimeManagement] 企业准入模块缺失，拒绝 Agent 执行: %s",
                exc,
            )
        raise FatalRouteError(
            "Agent admission dependencies are unavailable", code="SERVICE_UNAVAILABLE",
        ) from exc
    return get_enterprise_record_repository, matches


async def authorize_agent(envelope: E2AEnvelope) -> tuple[str, str, str] | None:
    # 版本守卫与 pod_rest_map / pod_http_client 同款：企业资源仓储仅在
    # is_enterprise() 时装配（storage_assembly.setup），个人版 runtime 模式
    # 不做企业准入，聊天请求直接放行——不是 ImportError 可判定的场景。
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return None
    if envelope.method not in CHAT_METHODS:
        return None
    get_enterprise_record_repository, matches = _enterprise_checkers()
    group_id, bot_id, user_id = routing_triple_from_envelope(envelope)
    identity = {"group_id": group_id, "bot_id": bot_id, "user_id": user_id}
    # 聊天准入必须三身份齐全；业务 params 不参与可信身份判定。
    if not all(identity.values()):
        raise FatalRouteError("Agent routing identity is required", code="FORBIDDEN")
    repo = get_enterprise_record_repository("instance_agent_resource")
    if repo is None:
        logger.error(
            "[RuntimeManagement] instance_agent_resource repository unavailable; "
            "Agent admission denied"
        )
        raise FatalRouteError(
            "Agent admission repository is unavailable", code="SERVICE_UNAVAILABLE",
        )
    # Do not cache admission: revocation and expiry apply to the next request.
    row = await repo.get(resource_id=bot_id)
    allowed = False
    # PostgreSQL 读回 bool，MySQL 布尔编解码器读回 1/0。
    enabled = row.get("enabled") if row is not None else None
    if row is not None and enabled in (1, True, "1"):
        try:
            expiry = row.get("expires_at")
            if isinstance(expiry, str):
                expiry = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            if expiry is not None and expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            allowed = (expiry is None or expiry > datetime.now(timezone.utc)) and matches(
                row.get("match_expr"), identity
            )
        except (ValueError, TypeError, AttributeError):
            allowed = False
    if not allowed:
        raise FatalRouteError("Agent access denied", code="FORBIDDEN")
    return group_id, bot_id, user_id
