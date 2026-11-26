"""企业版本地 Skill 上传开关：按 Agent 配置模板向 manager 查询。"""

from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)

POLICY_UNAVAILABLE = "SKILL_UPLOAD_POLICY_UNAVAILABLE"
LOCAL_SKILL_DISABLED = "SKILL_UPLOAD_LOCAL_SKILL_DISABLED"
UNAUTHORIZED = "SKILL_UPLOAD_UNAUTHORIZED"
FORBIDDEN = "SKILL_UPLOAD_FORBIDDEN"

_DEFAULT_TIMEOUT_S = 5.0
_TIMEOUT_ENV = "GATEWAY_MANAGER_PERMISSION_TIMEOUT"


class SkillUploadPolicyError(Exception):
    """策略查询失败；``code`` 与 Gateway HTTP 错误码一致。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _manager_base() -> str:
    from agent_gateway.enterprise.config.manager_config.core.instance.instance_service import (
        resolve_manager_http_base,
    )

    return resolve_manager_http_base()


def _timeout() -> float:
    raw = (os.getenv(_TIMEOUT_ENV) or "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_TIMEOUT_S
    return value if value > 0 else _DEFAULT_TIMEOUT_S


def _authorization_header(authorization: str) -> str:
    value = str(authorization or "").strip()
    if not value.lower().startswith("bearer "):
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "skill upload policy requires a bearer token"
        )
    return value


async def query_allow_local_skill_upload(
    *,
    authorization: str,
    jiuwenclaw_id: str,
    agent_template_id: str,
) -> bool:
    """按用户可访问的 Agent 上下文查询模板开关；匹配不到按拒绝处理。"""
    base = _manager_base()
    if not base:
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "manager endpoint is not configured"
        )
    cluster = str(jiuwenclaw_id or "").strip()
    template = str(agent_template_id or "").strip()
    if not cluster or not template:
        return False

    url = f"{base}/api/v1/user-console/agent-contexts"
    try:
        async with httpx.AsyncClient(
            timeout=_timeout(), trust_env=False
        ) as client:
            response = await client.get(
                url,
                headers={
                    "authorization": _authorization_header(authorization),
                    "accept": "application/json",
                },
            )
    except httpx.HTTPError as exc:
        logger.warning("[SkillUpload] policy query failed: %s", exc)
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "manager is unreachable"
        ) from exc

    if response.status_code == 401:
        # 登录态失效既不是“模板开关未开启”，也不是“服务不可用”：回 401 让
        # 前端引导重新登录，避免 503 掩盖真实原因并污染 5xx 告警。
        logger.warning("[SkillUpload] policy query unauthorized")
        raise SkillUploadPolicyError(
            UNAUTHORIZED, "manager rejected the credentials (401)"
        )
    if response.status_code == 403:
        # 已登录但无权访问该 Agent 上下文：回 403，同样不伪装成开关未开启。
        logger.warning("[SkillUpload] policy query forbidden")
        raise SkillUploadPolicyError(
            FORBIDDEN, "manager denied access to the agent context (403)"
        )
    if response.status_code == 404:
        # manager 没有该路由：多半是版本错配，同样按不可用处理。
        logger.warning("[SkillUpload] policy route not found")
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "manager policy route is unavailable"
        )
    if response.status_code != 200:
        logger.warning(
            "[SkillUpload] policy query status=%s", response.status_code
        )
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, f"manager returned {response.status_code}"
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "manager returned invalid json"
        ) from exc
    data = body.get("data") if isinstance(body, dict) else None
    contexts = data.get("contexts") if isinstance(data, dict) else None
    if not isinstance(contexts, list):
        raise SkillUploadPolicyError(
            POLICY_UNAVAILABLE, "manager response missing contexts"
        )

    for item in contexts:
        if not isinstance(item, dict):
            continue
        if str(item.get("jiuwenclaw_id") or "").strip() != cluster:
            continue
        if str(item.get("agent_template_id") or "").strip() != template:
            continue
        return item.get("allow_local_skill_upload") is True
    return False
