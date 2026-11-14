# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Per-request Agent resource admission, independent of Runtime pool routing."""
import logging
from datetime import datetime, timezone

from openjiuwen_protocol.e2a.models import E2AEnvelope

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
    """企业准入模块缺失时返回空并告警；企业版/个人版判定由 is_enterprise() 把关。

    只兜 ModuleNotFoundError（打包裁剪/可选依赖未装），其余导入异常向上抛，
    避免准入校验因内部依赖错误被静默整体关闭。
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
            logger.warning(
                "[RuntimeManagement] 企业准入模块缺失，chat.* 跳过 Agent 资源校验: %s",
                exc,
            )
        return None
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
    checkers = _enterprise_checkers()
    if checkers is None:
        return None
    get_enterprise_record_repository, matches = checkers
    group_id, bot_id, user_id = routing_triple_from_envelope(envelope)
    identity = {"group_id": group_id, "bot_id": bot_id, "user_id": user_id}
    # 刻意的安全边界：聊天准入必须三身份齐全才做 match_expr 判定（路由侧
    # identity_from_envelope 的 "default" 兜底不适用于本函数）。与路由兜底
    # 的口径差异是有意的——缺身份的聊天请求宁可拒绝也不落 wildcard scope。
    if not all(identity.values()):
        raise FatalRouteError("Agent routing identity is required", code="FORBIDDEN")
    repo = get_enterprise_record_repository("instance_agent_resource")
    if repo is None:
        # 企业版但仓储未装配（storage 启动失败/时序早于注入）：与
        # capabilities/workspace/quota.py 的既有约定一致——按未装配放行，
        # 不把每个聊天请求打成 500；装配问题由启动期日志暴露。
        logger.warning(
            "[RuntimeManagement] instance_agent_resource repository unavailable, "
            "chat.* admission skipped"
        )
        return group_id, bot_id, user_id
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
