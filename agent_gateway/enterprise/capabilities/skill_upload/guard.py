# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版 Skill 上传窄门禁：只处理 ``skills.import_upload`` 与 ``skills.upload.*``。

非企业版直接放行，其他方法不受影响。HTTP 与 WS 共用同一入口，避免两条
通道出现企业版能力缺口。凭据缺失、Manager 不可达或响应不合法都失败关闭。
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
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
# 记录只服务于「finalize 响应回来后的回写」。回写必须带用户 JWT，因此它
# 能在进程里保留多久由两个约束共同决定：本 TTL（过期后不再回写，并在下次
# 登记时清理）和 256 条 LRU 上限。JWT 不写日志，随记录一起释放。
_PENDING_RESULT_TTL_S = 600.0
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
# SKILL_FILE_TOO_LARGE 与 SKILL_UPLOAD_FEATURE_DISABLED 故意不在此表：
# finalize 前会把前者重映射为 SKILL_UPLOAD_PACKAGE_TOO_LARGE；后者只在非
# 企业版或缺少租户上下文时抛出，属于部署/配置问题，标 failed 会误导用户改包。


def _pending_key(request_id: str) -> str:
    return str(request_id or "").strip()


def _install_result_expired(record: dict[str, Any]) -> bool:
    created = record.get("created_at")
    if not isinstance(created, float):
        return True
    return (time.monotonic() - created) > _PENDING_RESULT_TTL_S


def _prune_expired_results() -> None:
    """Drop expired records (bounded by ``_MAX_PENDING_RESULTS``)."""
    expired = [
        key
        for key, record in _PENDING_RESULTS.items()
        if _install_result_expired(record)
    ]
    for key in expired:
        record = _PENDING_RESULTS.pop(key, None)
        if record is not None:
            logger.debug(
                "[SkillUpload] dropped expired write-back request=%s order=%s",
                key[:24],
                record.get("order_num"),
            )


def _remember_install_result(request_id: str, record: dict[str, Any]) -> bool:
    """登记待回写记录；同一 request_id 绑定到另一个 upload 时返回 False。

    ``request_id`` 由客户端提供（WS 的 ``id`` / HTTP 的 ``X-Request-Id``），
    所以必须防止它被复用来把 A 单的记录套到 B 单的回包上：
    - 已过期：视为空闲，直接替换；
    - 同一 upload：客户端重试，刷新记录；
    - 不同 upload：失败关闭，由调用方返回 BAD_REQUEST。
    """
    key = _pending_key(request_id)
    if not key:
        return True
    _prune_expired_results()
    record["created_at"] = time.monotonic()
    existing = _PENDING_RESULTS.get(key)
    if existing is not None:
        if str(existing.get("upload_id") or "") != str(
            record.get("upload_id") or ""
        ):
            logger.warning(
                "[SkillUpload] request id already bound to another upload:"
                " request=%s order=%s",
                key[:24],
                record.get("order_num"),
            )
            return False
        logger.debug(
            "[SkillUpload] refresh pending write-back request=%s order=%s",
            key[:24],
            record.get("order_num"),
        )
    _PENDING_RESULTS[key] = record
    _PENDING_RESULTS.move_to_end(key)
    while len(_PENDING_RESULTS) > _MAX_PENDING_RESULTS:
        _PENDING_RESULTS.popitem(last=False)
    return True


def _pop_install_result(request_id: str) -> dict[str, Any] | None:
    return _PENDING_RESULTS.pop(_pending_key(request_id), None)


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
    # Manager 正常返回 JSON bool；只接受真正的 bool，字符串 "false" 一律按
    # 不覆盖处理，避免把"不可覆盖"误判成"可覆盖"。
    replace = data.get("replace")
    params["replace"] = replace if isinstance(replace, bool) else False
    # 只保留回写必需字段：请求 id 与 upload_id 绑定，防止回包被套到另一张单。
    if not _remember_install_result(
        request_id,
        {
            "authorization": token,
            "jiuwenclaw_id": cluster,
            "order_num": order_num,
            "upload_id": upload_id,
            "digest": digest,
        },
    ):
        raise SkillUploadCheckError(
            "BAD_REQUEST", "request id is already bound to another upload"
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
    # 只有登记过本次 finalize 的 request_id 才需要回写；流式事件与普通 res
    # 在热路径上直接返回，连后台任务都不建。
    key = _pending_key(request_id)
    if not key or key not in _PENDING_RESULTS:
        return
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
    """finalize 响应返回后，把安装结果回写 manager；失败只记日志。

    网络层重试在 ``client.report_install_result`` 内完成；这里只负责把
    「响应 ok」映射成审批单状态。``ok`` 只代表 RPC 成功，handler 仍可能
    在 payload 里返回 ``success=False``，因此必须同时看 payload。
    成功只认 ``success is True``：request_id 由客户端提供，若另一个方法
    复用了同一个 id，其 ok 响应不能把本次 finalize 标成 installed。
    """
    record = _pop_install_result(request_id)
    if record is None:
        return
    if _install_result_expired(record):
        logger.warning(
            "[SkillUpload] expired write-back dropped order=%s", record.get("order_num")
        )
        return
    code = str(payload.get("code") or "").strip()
    if ok and payload.get("success") is True:
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
