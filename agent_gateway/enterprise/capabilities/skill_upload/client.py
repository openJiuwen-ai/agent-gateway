# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Manager 审批单复核与安装结果回写客户端。

只透传用户 JWT，不缓存结果。任何无法确认的情况都失败关闭。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

CHECK_UNAVAILABLE = "SKILL_UPLOAD_CHECK_UNAVAILABLE"
NOT_APPROVED = "SKILL_UPLOAD_NOT_APPROVED"

_DEFAULT_TIMEOUT_S = 5.0
_TIMEOUT_ENV = "GATEWAY_MANAGER_PERMISSION_TIMEOUT"

# 回写只是后台收尾：瞬时故障本地重试，仍失败就放弃（审批单保持原状态，
# 用户重试 finalize 会再次触发回写）。重试使用同一份 body，不会把单据写成
# 两种状态；若管理面已落库，重复请求最多再写一次相同状态。
_INSTALL_RESULT_ATTEMPTS = 3
_INSTALL_RESULT_BACKOFF_S = 0.2
_RETRYABLE_STATUS = frozenset({429})


def _retryable_status(status: int) -> bool:
    return status in _RETRYABLE_STATUS or status >= 500


class SkillUploadCheckError(Exception):
    """上传复核失败；``code`` 与 Gateway HTTP 错误码一致。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _manager_base() -> str:
    # 与实例启动辅助共用同一份 URL 归一化，避免两处实现漂移后
    # 「鉴权打 A、回写打 B」。懒导入避免 enterprise 配置包与能力包的循环依赖。
    from agent_gateway.enterprise.config.manager_config.core.instance.instance_service import (
        resolve_manager_http_base,
    )

    return resolve_manager_http_base()


def _timeout() -> float:
    import os

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
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "skill upload check requires a bearer token"
        )
    return value


async def query_upload_authorization(
    *,
    authorization: str,
    jiuwenclaw_id: str,
    order_num: str,
) -> dict[str, Any]:
    """读取审批单的授权视图；非本人或不存在一律按"未批准"拒绝。"""
    base = _manager_base()
    if not base:
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "manager endpoint is not configured"
        )
    headers = {
        "authorization": _authorization_header(authorization),
        "accept": "application/json",
    }
    url = (
        f"{base}/api/v1/user-console/skills/upload-requests/"
        f"{quote(order_num, safe='')}"
    )
    try:
        async with httpx.AsyncClient(
            timeout=_timeout(), trust_env=False
        ) as client:
            response = await client.get(
                url,
                params={"view": "authorization", "jiuwenclaw_id": jiuwenclaw_id},
                headers=headers,
            )
    except httpx.HTTPError as exc:
        logger.warning("[SkillUpload] authorization query failed: %s", exc)
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "approval service unreachable"
        ) from exc
    if response.status_code in {401, 403, 404}:
        logger.warning(
            "[SkillUpload] authorization rejected status=%s", response.status_code
        )
        raise SkillUploadCheckError(
            NOT_APPROVED, "approval request is not available to this user"
        )
    if response.status_code != 200:
        logger.warning(
            "[SkillUpload] authorization query status=%s", response.status_code
        )
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE,
            f"approval service returned {response.status_code}",
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "approval service returned invalid json"
        ) from exc
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "approval response missing data"
        )
    return data


async def report_install_result(
    *,
    authorization: str,
    jiuwenclaw_id: str,
    order_num: str,
    upload_id: str,
    digest: str,
    status: str,
    detail: str | None,
) -> None:
    """回写安装结果；瞬时故障在本地有限重试，调用方负责吞掉异常。"""
    base = _manager_base()
    if not base:
        logger.warning("[SkillUpload] install result dropped: manager not configured")
        return
    body = {
        "jiuwenclaw_id": jiuwenclaw_id,
        "upload_id": upload_id,
        "digest": digest,
        "status": status,
        "detail": detail,
    }
    url = (
        f"{base}/api/v1/user-console/skills/upload-requests/"
        f"{quote(order_num, safe='')}/install-result"
    )
    headers = {
        "authorization": _authorization_header(authorization),
        "accept": "application/json",
    }
    for attempt in range(1, _INSTALL_RESULT_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(
                timeout=_timeout(), trust_env=False
            ) as client:
                response = await client.post(url, json=body, headers=headers)
        except httpx.TransportError as exc:
            # 只重试传输层故障（连接/超时/读写）；InvalidURL 之类配置错误
            # 重试也不会成功，直接走下面的失败日志。
            if attempt >= _INSTALL_RESULT_ATTEMPTS:
                logger.warning(
                    "[SkillUpload] install result write-back failed after %d"
                    " attempts order=%s: %s",
                    attempt,
                    order_num,
                    exc,
                )
                return
            logger.warning(
                "[SkillUpload] install result write-back retry %d/%d order=%s: %s",
                attempt,
                _INSTALL_RESULT_ATTEMPTS,
                order_num,
                exc,
            )
            await asyncio.sleep(_INSTALL_RESULT_BACKOFF_S * (2 ** (attempt - 1)))
            continue
        if response.status_code == 200:
            return
        if attempt < _INSTALL_RESULT_ATTEMPTS and _retryable_status(
            response.status_code
        ):
            logger.warning(
                "[SkillUpload] install result write-back retry %d/%d status=%s"
                " order=%s",
                attempt,
                _INSTALL_RESULT_ATTEMPTS,
                response.status_code,
                order_num,
            )
            await asyncio.sleep(_INSTALL_RESULT_BACKOFF_S * (2 ** (attempt - 1)))
            continue
        logger.warning(
            "[SkillUpload] install result write-back status=%s order=%s",
            response.status_code,
            order_num,
        )
        return
