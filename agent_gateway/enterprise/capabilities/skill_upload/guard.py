# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版 Skill 上传窄门禁：只处理 ``skills.import_upload`` 与 ``skills.upload.*``。

非企业版直接放行，其他方法不受影响。HTTP 与 WS 共用同一入口，避免两条
通道出现企业版能力缺口。凭据缺失、Manager 不可达或响应不合法都失败关闭。
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import OrderedDict
from typing import Any

from agent_gateway.common.protocol_methods import (
    SKILL_UPLOAD_FINALIZE,
    SKILL_UPLOAD_STAGE,
)

logger = logging.getLogger(__name__)

RETIRED_CODE = "SKILL_UPLOAD_FEATURE_RETIRED"
IMPORT_UPLOAD_METHOD = "skills.import_upload"
FINALIZE_METHOD = SKILL_UPLOAD_FINALIZE
# 门禁覆盖的方法集合：WS 与 HTTP 两个入口都按它决定是否调用本门禁。
GUARDED_METHODS = frozenset(
    {IMPORT_UPLOAD_METHOD, SKILL_UPLOAD_STAGE, SKILL_UPLOAD_FINALIZE}
)

# Manager 的单据号是 ``apr_<upload_id>``，``upload_id`` 为 uuid4 的 32 位 hex。
# 两者都会被拼进 Manager URL，客户端可伪造，必须按该契约校验后再拼接。
_ORDER_NUM_RE = re.compile(r"apr_[0-9a-f]{32}")
_UPLOAD_ID_RE = re.compile(r"[0-9a-f]{32}")

_MAX_PENDING_RESULTS = 256
_PENDING_RESULTS: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
# 后台回写任务必须持引用，否则可能在完成前被回收。
_WRITEBACK_TASKS: set[asyncio.Task[Any]] = set()

# 这些错误来自 Pod 的确定性判断（包/权限/冲突），可以把单据标为安装失败；
# 503 之类的瞬时故障不落失败状态，用户直接重试即可。
_INSTALL_FAILURE_CODES = frozenset({
    "SKILL_UPLOAD_PACKAGE_INVALID",
    "SKILL_UPLOAD_PACKAGE_TOO_LARGE",
    "SKILL_UPLOAD_STAGE_EXPIRED",
    "SKILL_UPLOAD_STAGE_NOT_FOUND",
    "SKILL_UPLOAD_DIGEST_MISMATCH",
    "SKILL_UPLOAD_NOT_APPROVED",
    "SKILL_UPLOAD_NAME_CONFLICT",
    "SKILL_UPLOAD_BUILTIN_READ_ONLY",
})


def _remember_install_result(request_id: str, record: dict[str, Any]) -> None:
    key = str(request_id or "").strip()
    if not key:
        return
    _PENDING_RESULTS[key] = record
    _PENDING_RESULTS.move_to_end(key)
    while len(_PENDING_RESULTS) > _MAX_PENDING_RESULTS:
        _PENDING_RESULTS.popitem(last=False)


def _pop_install_result(request_id: str) -> dict[str, Any] | None:
    return _PENDING_RESULTS.pop(str(request_id or "").strip(), None)


async def _authorize_finalize(
    *,
    params: dict[str, Any],
    request_id: str,
    authorization: str | None,
    jiuwenclaw_id: str | None,
) -> None:
    from agent_gateway.enterprise.capabilities.skill_upload.client import (
        CHECK_UNAVAILABLE,
        NOT_APPROVED,
        SkillUploadCheckError,
        query_upload_authorization,
    )

    token = str(authorization or "").strip()
    cluster = str(jiuwenclaw_id or "").strip()
    order_num = str(params.get("order_num") or "").strip()
    upload_id = str(params.get("upload_id") or "").strip()
    if not token.lower().startswith("bearer ") or not cluster:
        logger.warning(
            "[SkillUpload] reject finalize: missing credentials cluster=%s",
            bool(cluster),
        )
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE,
            "skill upload cannot be verified: missing credentials or instance id",
        )
    if not _ORDER_NUM_RE.fullmatch(order_num) or not _UPLOAD_ID_RE.fullmatch(
        upload_id
    ):
        raise SkillUploadCheckError(
            "BAD_REQUEST", "order_num or upload_id is malformed"
        )

    data = await query_upload_authorization(
        authorization=token, jiuwenclaw_id=cluster, order_num=order_num
    )
    if str(data.get("upload_id") or "").strip() != upload_id:
        raise SkillUploadCheckError(
            NOT_APPROVED, "approval request does not match this upload"
        )
    if str(data.get("cluster_id") or "").strip() != cluster:
        raise SkillUploadCheckError(
            NOT_APPROVED, "approval request belongs to another instance"
        )
    if str(data.get("status") or "").strip() != "approved":
        raise SkillUploadCheckError(
            NOT_APPROVED, "skill upload has not been approved"
        )
    digest = str(data.get("digest") or "").strip()
    skill_name = str(data.get("skill_name") or "").strip()
    if not digest or not skill_name:
        raise SkillUploadCheckError(
            CHECK_UNAVAILABLE, "approval response missing digest or skill name"
        )

    # 审批单是权威来源：覆盖前端可伪造的同名字段。
    params["expected_digest"] = digest
    params["skill_name"] = skill_name
    params["replace"] = bool(data.get("replace", False))
    _remember_install_result(
        request_id,
        {
            "authorization": token,
            "jiuwenclaw_id": cluster,
            "order_num": order_num,
            "upload_id": upload_id,
            "digest": digest,
        },
    )


async def enterprise_skill_upload_denial(
    *,
    method: str,
    params: dict[str, Any],
    request_id: str,
    authorization: str | None,
    jiuwenclaw_id: str | None,
) -> tuple[str, str] | None:
    """返回 ``(message, code)`` 表示拒绝；``None`` 表示放行。"""
    from agent_gateway.enterprise import is_enterprise

    if not is_enterprise():
        return None
    if method == IMPORT_UPLOAD_METHOD:
        return ("企业版已停止使用该上传方式", RETIRED_CODE)
    if method != FINALIZE_METHOD:
        # skills.upload.stage 仅依赖企业版白名单；其他方法不归本门禁管。
        return None

    from agent_gateway.enterprise.capabilities.skill_upload.client import (
        CHECK_UNAVAILABLE,
        SkillUploadCheckError,
    )

    try:
        await _authorize_finalize(
            params=params,
            request_id=request_id,
            authorization=authorization,
            jiuwenclaw_id=jiuwenclaw_id,
        )
    except SkillUploadCheckError as exc:
        return (exc.message, exc.code)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[SkillUpload] finalize authorization failed: %s", exc)
        return ("skill upload check is unavailable", CHECK_UNAVAILABLE)
    return None


def _discard_writeback(task: asyncio.Task[Any]) -> None:
    """后台回写收尾：释放引用并把异常记进日志，避免无人消费的任务告警。"""
    _WRITEBACK_TASKS.discard(task)
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        logger.warning(
            "[SkillUpload] write-back task failed: %s", error, exc_info=error
        )


def schedule_pending_install_result(
    request_id: str,
    *,
    ok: bool,
    payload: dict[str, Any],
) -> None:
    """把安装结果回写排到后台，让响应先投递。

    ``WebChannel.send()`` 是 WS 与 HTTP 的共用出口，同步 await 写回会让
    Manager 抖动直接变成 finalize 响应的延迟（最坏一个 timeout），因此
    响应出口只做调度，不等待网络往返。
    """
    try:
        task = asyncio.create_task(
            report_pending_install_result(request_id, ok=ok, payload=payload),
            name=f"skill-upload-writeback-{str(request_id or '')[:24]}",
        )
    except RuntimeError:
        logger.warning(
            "[SkillUpload] write-back skipped: no running event loop request=%s",
            request_id,
        )
        return
    _WRITEBACK_TASKS.add(task)
    task.add_done_callback(_discard_writeback)


async def report_pending_install_result(
    request_id: str,
    *,
    ok: bool,
    payload: dict[str, Any],
) -> None:
    """finalize 响应返回后，把安装结果回写 manager；失败只记日志。"""
    record = _pop_install_result(request_id)
    if record is None:
        return
    code = str(payload.get("code") or "").strip()
    if ok:
        status = "installed"
        detail: str | None = None
    elif code in _INSTALL_FAILURE_CODES:
        status = "failed"
        raw = payload.get("error") or payload.get("message") or ""
        detail = str(raw)[:1024] or None
    else:
        return
    from agent_gateway.enterprise.capabilities.skill_upload.client import (
        report_install_result,
    )

    try:
        await report_install_result(
            authorization=record["authorization"],
            jiuwenclaw_id=record["jiuwenclaw_id"],
            order_num=record["order_num"],
            upload_id=record["upload_id"],
            digest=record["digest"],
            status=status,
            detail=detail,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[SkillUpload] install result write-back failed order=%s: %s",
            record["order_num"],
            exc,
        )
