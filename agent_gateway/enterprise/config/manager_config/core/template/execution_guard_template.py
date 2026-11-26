# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""执行护栏模板：将 Manager 下发的 execution_guard_templates 写入 Gateway 本地库。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.enterprise.repository import EnterpriseRecordRepository
from agent_gateway.config.enterprise.tables.template_models import (
    EXECUTION_GUARD_TEMPLATE_TABLE_DEF,
)

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import parse_iso_datetime, utc_now
from ...schemas.execution_guard_body import validate_execution_guard_body
from ...schemas.template_schemas import ExecutionGuardTemplateUpdateRequest
from .template_id import normalize_template_id

_TABLE = EXECUTION_GUARD_TEMPLATE_TABLE_DEF.table_name
logger = logging.getLogger(__name__)


async def _get_row(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> dict[str, Any] | None:
    return await repo.get(template_id=normalize_template_id(template_id))


async def update_execution_guard_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
    request: ExecutionGuardTemplateUpdateRequest,
    *,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    tid = normalize_template_id(template_id)
    row = existing if existing is not None else await _get_row(repo, tid)
    if row is None:
        return None

    updates = request.model_dump(exclude_unset=True)
    if "template_name" in updates and updates["template_name"] is not None:
        updates["template_name"] = updates["template_name"].strip()
    if "body" in updates:
        updates["body"] = validate_execution_guard_body(updates["body"])

    if not updates:
        raise ValueError("请求未包含任何可更新的业务字段")

    updates["updated_at"] = utc_now()
    updated = await repo.update({"template_id": tid}, updates)
    if updated is None:
        return None
    return {"template_id": str(updated.get("template_id", tid))}


async def delete_execution_guard_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> bool:
    tid = normalize_template_id(template_id)
    existing = await _get_row(repo, tid)
    if existing is None:
        return False
    return await repo.delete(template_id=tid)


def _build_row_from_template(
    template: dict[str, Any],
    *,
    now: Any,
) -> dict[str, Any]:
    return {
        "template_id": normalize_template_id(template.get("template_id")),
        "template_name": str(template["template_name"]).strip(),
        "description": template.get("description"),
        "enabled": bool(template.get("enabled", True)),
        "body": validate_execution_guard_body(template.get("body")),
        "data": template.get("data"),
        "created_at": now,
        "updated_at": now,
    }


async def _upsert_execution_guard_template_from_sync(
    repo: EnterpriseRecordRepository,
    template: dict[str, Any],
) -> None:
    now = utc_now()
    tid = normalize_template_id(template.get("template_id"))
    existing = await _get_row(repo, tid)
    row_data = _build_row_from_template(template, now=now)
    if existing is None:
        await repo.create(row_data)
        return
    created_at = existing.get("created_at")
    if created_at is not None:
        row_data["created_at"] = parse_iso_datetime(created_at) or now
    updates = {key: value for key, value in row_data.items() if key != "template_id"}
    updates["updated_at"] = utc_now()
    await repo.update({"template_id": tid}, updates)


class ExecutionGuardTemplateService:

    async def create(self, template: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(template, dict):
            raise ValueError("execution_guard_templates.create requires template object")
        if "body" not in template:
            raise ValueError("execution_guard_templates.create requires body object")
        repo = require_enterprise_repository(_TABLE)
        await _upsert_execution_guard_template_from_sync(repo, template)
        result = {"template_id": normalize_template_id(template.get("template_id"))}
        logger.info(
            "[ManagerConfigReceiver] execution_guard_templates create template_id=%s",
            result["template_id"],
        )
        return result

    async def update(self, template_id: str, updates: dict[str, Any]) -> None:
        if template_id is None:
            raise ValueError("execution_guard_templates.update requires template_id")
        if not isinstance(updates, dict) or not updates:
            raise ValueError("execution_guard_templates.update requires non-empty updates")
        req = ExecutionGuardTemplateUpdateRequest.model_validate(updates)
        tid = normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        existing = await _get_row(repo, tid)
        row = await update_execution_guard_template(repo, tid, req, existing=existing)
        if row is None:
            raise ValueError(f"execution guard template template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] execution_guard_templates update template_id=%s",
            tid,
        )

    async def delete(self, template_id: str) -> None:
        if template_id is None:
            raise ValueError("execution_guard_templates.delete requires template_id")
        tid = normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        await delete_execution_guard_template(repo, tid)
        logger.info(
            "[ManagerConfigReceiver] execution_guard_templates delete template_id=%s",
            tid,
        )
