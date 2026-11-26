# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Per-request Agent resource admission, independent of Runtime pool routing."""
import logging
from datetime import datetime, timezone

from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension.invoke_ids import (
    routing_triple_from_envelope,
)
from agent_gateway.southbound.control.runtime import (
    FatalRouteError,
)

logger = logging.getLogger(__name__)
_enterprise_absent_logged = False

# 发起/继续 Agent 执行的对话入口：这些会产生新的执行，资源过期后必须拒绝。
CHAT_METHODS = frozenset({
    "chat.send", "chat.resume", "chat.user_answer", "chat.swarmflow_reply",
})

# 只读入口：不产生新执行、不修改会话内容。
# 资源过期后仍放行——用户对自己历史对话的读取权不随 bot 授权到期而消失。
# 注意：rename/fork/delete/switch/archive 等会改会话状态的**不在此列**，
# 它们按写操作对待（过期后拒绝）。
READ_ONLY_METHODS = frozenset({
    "history.get",
    "history.list_turns",
    "session.list",
    "session.get_metadata",
    "session.archived.list",
    "team.history.get",
    "team.snapshot",
})

# 需要经过资源准入的方法全集。
_ADMITTED_METHODS = CHAT_METHODS | READ_ONLY_METHODS
# 同一业务族的新方法默认按写操作准入，避免新增写入口绕过过期检查。
_RESOURCE_METHOD_PREFIXES = ("session.", "chat.", "history.", "command.", "team.")


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
    method = envelope.method
    # E2AEnvelope.method 的协议类型为 str | None；无方法的信封不做资源准入。
    if method is None:
        return None
    if method not in _ADMITTED_METHODS and not method.startswith(_RESOURCE_METHOD_PREFIXES):
        return None
    group_id, bot_id, user_id = routing_triple_from_envelope(envelope)
    identity = {"group_id": group_id, "bot_id": bot_id, "user_id": user_id}
    # 聊天入口必须三身份齐全；非聊天入口可能只有团队/用户上下文，
    # 无完整 bot 路由时保持原行为，由 Agent 校验会话/团队访问权。
    # 业务 params 不用于补齐可信身份，也不据其执行 bot 资源准入。
    if not all(identity.values()):
        if method not in CHAT_METHODS:
            return None
        raise FatalRouteError("Agent routing identity is required", code="FORBIDDEN")
    get_enterprise_record_repository, matches = _enterprise_checkers()
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
    # PostgreSQL 读回 bool，MySQL 布尔编解码器读回 1/0。
    enabled = row.get("enabled") if row is not None else None
    if enabled not in (1, True, "1"):
        # 不存在或被主动禁用：只读也不放行。禁用是管理员的显式动作，
        # 与「授权到期」语义不同，不应继续暴露会话内容。
        raise FatalRouteError("Agent access denied", code="FORBIDDEN")

    expired = False
    try:
        expiry = row.get("expires_at")
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if expiry is not None and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        expired = expiry is not None and expiry <= datetime.now(timezone.utc)
    except (ValueError, TypeError, AttributeError):
        # 过期时间读不出来时按「不可用」处理，不放行。
        raise FatalRouteError("Agent access denied", code="FORBIDDEN") from None

    try:
        in_scope = matches(row.get("match_expr"), identity)
    except (ValueError, TypeError, AttributeError):
        in_scope = False
    if not in_scope:
        raise FatalRouteError("Agent access denied", code="FORBIDDEN")

    if expired and method not in READ_ONLY_METHODS:
        raise FatalRouteError("Agent access denied", code="FORBIDDEN")
    return group_id, bot_id, user_id
