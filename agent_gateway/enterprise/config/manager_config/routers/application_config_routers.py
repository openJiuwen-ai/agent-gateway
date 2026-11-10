# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""应用配置同步 API（对齐 Manager application_config_routers：显式 Body schema）。"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request

from ..core.application_config.agent_capability_config import (
    AgentCapabilityConfigService,
    capability_body,
)
from ..core.application_config.audit_log_config import AuditLogConfigService
from ..core.application_config.cron_policy import CronPolicyService
from ..core.application_config.log_masking_rule import LogMaskingRuleService
from ..core.application_config.logging_config import LoggingConfigService
from ..core.application_config.memory_config import MemoryConfigService
from ..core.application_config.task_memory_config import TaskMemoryConfigService
from ..schemas.application_config_schemas import (
    AuditLogUpsertRequest,
    CronPolicyUpsertRequest,
    LoggingConfigUpsertRequest,
    LogMaskingRuleCreateRequest,
    LogMaskingRuleUpdateRequest,
    A2uiCapabilityUpsertRequest,
    A4pCapabilityUpsertRequest,
    AgentTemplateCapabilityDeleteRequest,
    MemoryConfigUpsertRequest,
    TaskMemoryUpsertRequest,
)
from ..schemas.common_schemas import ResponseModel
from ..schemas.sync_schemas import make_sync_body
from .deps import (
    SyncContext,
    VerifySyncEnvelopeOnly,
    sync_write_data,
    verify_sync,
)
from .runtime_notify import trigger_runtime_config_update

application_config_router = APIRouter()

LoggingSyncBody = make_sync_body("LoggingSyncBody", LoggingConfigUpsertRequest)
TaskMemorySyncBody = make_sync_body("TaskMemorySyncBody", TaskMemoryUpsertRequest)
MemorySyncBody = make_sync_body("MemorySyncBody", MemoryConfigUpsertRequest)
AuditLogSyncBody = make_sync_body("AuditLogSyncBody", AuditLogUpsertRequest)
CronPolicySyncBody = make_sync_body("CronPolicySyncBody", CronPolicyUpsertRequest)
LogMaskingCreateSyncBody = make_sync_body(
    "LogMaskingCreateSyncBody", LogMaskingRuleCreateRequest
)
LogMaskingUpdateSyncBody = make_sync_body(
    "LogMaskingUpdateSyncBody", LogMaskingRuleUpdateRequest
)
A2uiSyncBody = make_sync_body("A2uiSyncBody", A2uiCapabilityUpsertRequest)
A4pSyncBody = make_sync_body("A4pSyncBody", A4pCapabilityUpsertRequest)
CapabilityDeleteSyncBody = make_sync_body(
    "CapabilityDeleteSyncBody", AgentTemplateCapabilityDeleteRequest
)


@application_config_router.put("/logging", response_model=ResponseModel)
async def upsert_logging(
    sync: Annotated[SyncContext, Depends(verify_sync(LoggingSyncBody))],
):
    try:
        result = await LoggingConfigService().upsert(**sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete("/logging", response_model=ResponseModel)
async def delete_logging(
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await LoggingConfigService().delete()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.put("/task-memory", response_model=ResponseModel)
async def upsert_task_memory(
    sync: Annotated[SyncContext, Depends(verify_sync(TaskMemorySyncBody))],
):
    try:
        result = await TaskMemoryConfigService().upsert(sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete("/task-memory", response_model=ResponseModel)
async def delete_task_memory(
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await TaskMemoryConfigService().delete()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.put("/memory", response_model=ResponseModel)
async def upsert_memory(
    sync: Annotated[SyncContext, Depends(verify_sync(MemorySyncBody))],
):
    try:
        result = await MemoryConfigService().upsert(**sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete("/memory", response_model=ResponseModel)
async def delete_memory(
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await MemoryConfigService().delete()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.get("/package-allowlists/catalog", response_model=ResponseModel)
async def list_package_allowlist_catalog(request: Request):
    """Query the Server via E2A, preserving the management response contract."""
    from openjiuwen_protocol.message import ReqMethod
    from agent_gateway.routing.e2a_proxy import fetch_agent_unary

    channel = getattr(request.app.state, "manager_agent_channel", None)
    client = getattr(channel, "agent_client", None)
    if callable(client):
        client = client()
    ok, payload = await fetch_agent_unary(
        agent_client=client,
        req_method=ReqMethod.HUB_EXPERT_PACKAGES_CATALOG,
        params={},
        session_id=None,
        user_id=None,
        channel_id="gateway",
        label="package-allowlists.catalog",
        timeout_seconds=120.0,
    )
    if not ok:
        payload = {"items": [], "error": str(payload.get("error") or "AgentServer is unavailable")}
    payload = _without_hidden_hub_items(payload)
    return ResponseModel(code=200, message="success", data=payload)


def _without_hidden_hub_items(payload: dict[str, Any]) -> dict[str, Any]:
    """管理端目录去掉企业版不提供的 Hub 包。本路由只在企业版注册。"""
    from agent_gateway.common.expert_packages import HIDDEN_HUB_ASSET_IDS

    items = payload.get("items")
    if not isinstance(items, list):
        return payload
    kept = [
        row
        for row in items
        if str((row or {}).get("id") or "").strip() not in HIDDEN_HUB_ASSET_IDS
    ]
    return {**payload, "items": kept}


@application_config_router.put("/cron-policy", response_model=ResponseModel)
async def upsert_cron_policy(
    sync: Annotated[SyncContext, Depends(verify_sync(CronPolicySyncBody))],
):
    try:
        result = await CronPolicyService().upsert(sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete("/cron-policy", response_model=ResponseModel)
async def delete_cron_policy(
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await CronPolicyService().delete()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.put("/audit-log", response_model=ResponseModel)
async def upsert_audit_log(
    sync: Annotated[SyncContext, Depends(verify_sync(AuditLogSyncBody))],
):
    try:
        result = await AuditLogConfigService().upsert(**sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete("/audit-log", response_model=ResponseModel)
async def delete_audit_log(
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await AuditLogConfigService().delete()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.post("/log-masking-rules", response_model=ResponseModel)
async def create_log_masking_rule(
    sync: Annotated[SyncContext, Depends(verify_sync(LogMaskingCreateSyncBody))],
):
    try:
        result = await LogMaskingRuleService().create(sync.business)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.patch(
    "/log-masking-rules/{rule_id}", response_model=ResponseModel
)
async def patch_log_masking_rule(
    rule_id: str,
    sync: Annotated[SyncContext, Depends(verify_sync(LogMaskingUpdateSyncBody))],
):
    try:
        result = await LogMaskingRuleService().update(rule_id, sync.business)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail else 400
        raise HTTPException(status_code=status, detail=detail) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


@application_config_router.delete(
    "/log-masking-rules/{rule_id}", response_model=ResponseModel
)
async def delete_log_masking_rule(
    rule_id: str,
    sync: VerifySyncEnvelopeOnly,
):
    try:
        await LogMaskingRuleService().delete(rule_id)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail else 400
        raise HTTPException(status_code=status, detail=detail) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


async def _upsert_agent_capability(kind: str, sync: SyncContext) -> ResponseModel:
    try:
        template_id = str(sync.business.get("agent_template_id") or "")
        result = await AgentCapabilityConfigService(kind).upsert(
            template_id,
            capability_body(sync.business),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, result))


async def _delete_agent_capability(kind: str, sync: SyncContext) -> ResponseModel:
    try:
        template_id = str(sync.business.get("agent_template_id") or "")
        await AgentCapabilityConfigService(kind).delete(template_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    trigger_runtime_config_update()
    return ResponseModel(code=200, message="success", data=sync_write_data(sync, None))


@application_config_router.put("/a2ui", response_model=ResponseModel)
async def upsert_a2ui(
    sync: Annotated[SyncContext, Depends(verify_sync(A2uiSyncBody))],
):
    return await _upsert_agent_capability("a2ui", sync)


@application_config_router.delete("/a2ui", response_model=ResponseModel)
async def delete_a2ui(
    sync: Annotated[SyncContext, Depends(verify_sync(CapabilityDeleteSyncBody))],
):
    return await _delete_agent_capability("a2ui", sync)


@application_config_router.put("/a4p", response_model=ResponseModel)
async def upsert_a4p(
    sync: Annotated[SyncContext, Depends(verify_sync(A4pSyncBody))],
):
    return await _upsert_agent_capability("a4p", sync)


@application_config_router.delete("/a4p", response_model=ResponseModel)
async def delete_a4p(
    sync: Annotated[SyncContext, Depends(verify_sync(CapabilityDeleteSyncBody))],
):
    return await _delete_agent_capability("a4p", sync)
