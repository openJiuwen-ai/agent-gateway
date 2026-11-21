# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""React 运行策略模板：将 Claw Manager 下发的 react_templates 写入 Gateway 本地库。

react 配置整段存 data JSON 列（body 作为传输格式，落库写入 data）。
"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.enterprise.repository import EnterpriseRecordRepository
from agent_gateway.config.enterprise.tables.template_models import REACT_TEMPLATE_TABLE_DEF

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import parse_iso_datetime, utc_now
from ...schemas.template_schemas import ReactTemplateUpdateRequest

_TABLE = REACT_TEMPLATE_TABLE_DEF.table_name
logger = logging.getLogger(__name__)


def _normalize_template_id(template_id: Any) -> str:
    normalized = str(template_id or "").strip()
    if not normalized:
        raise ValueError("template_id is required")
    if len(normalized) > 100:
        raise ValueError("template_id must be at most 100 characters")
    return normalized


def _normalize_data(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("data must be a JSON object")
    return value


async def _get_row_for_instance(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> dict[str, Any] | None:
    return await repo.get(template_id=_normalize_template_id(template_id))


def _build_row_from_template(template: dict[str, Any], *, now: Any) -> dict[str, Any]:
    return {
        "template_id": _normalize_template_id(template.get("template_id")),
        "template_name": str(template.get("template_name") or "").strip(),
        "description": template.get("description"),
        "enabled": bool(template.get("enabled", True)),
        "data": _normalize_data(template.get("body")),
        "created_at": now,
        "updated_at": now,
    }


async def _upsert_react_template_from_sync(
    repo: EnterpriseRecordRepository,
    template: dict[str, Any],
) -> None:
    now = utc_now()
    tid = _normalize_template_id(template.get("template_id"))
    existing = await _get_row_for_instance(repo, tid)
    row_data = _build_row_from_template(template, now=now)
    if existing is None:
        await repo.create(row_data)
        return
    created_at = existing.get("created_at")
    if created_at is not None:
        # existing 可能是 ISO 字符串；asyncpg 要求 datetime
        row_data["created_at"] = parse_iso_datetime(created_at) or now
    updates = {k: v for k, v in row_data.items() if k not in ("template_id",)}
    updates["updated_at"] = utc_now()
    await repo.update({"template_id": tid}, updates)


async def _sync_react_templates_records(
    repo: EnterpriseRecordRepository,
    templates: list[dict[str, Any]],
) -> dict[str, Any]:
    incoming_ids: set[str] = set()
    synced = 0
    for item in templates:
        if not isinstance(item, dict):
            raise ValueError("react_templates.sync templates must be objects")
        tid = _normalize_template_id(item.get("template_id"))
        incoming_ids.add(tid)
        await _upsert_react_template_from_sync(repo, item)
        synced += 1
    deleted = 0
    for row in await repo.list():
        tid = str(row.get("template_id") or "")
        if tid and tid not in incoming_ids:
            if await delete_react_template(repo, tid):
                deleted += 1
    return {"synced_count": synced, "deleted_count": deleted}


async def update_react_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
    request: ReactTemplateUpdateRequest,
    *,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    tid = _normalize_template_id(template_id)
    row = existing if existing is not None else await _get_row_for_instance(repo, tid)
    if row is None:
        return None

    updates = request.model_dump(exclude_unset=True)
    if "template_name" in updates and updates["template_name"] is not None:
        updates["template_name"] = updates["template_name"].strip()
    if "body" in updates and updates["body"] is not None:
        updates["data"] = _normalize_data(updates.pop("body"))

    if not updates:
        raise ValueError("请求未包含任何可更新的业务字段")

    updates["updated_at"] = utc_now()
    updated = await repo.update({"template_id": tid}, updates)
    if updated is None:
        return None
    return {"template_id": str(updated.get("template_id", tid))}


async def delete_react_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> bool:
    tid = _normalize_template_id(template_id)
    existing = await _get_row_for_instance(repo, tid)
    if existing is None:
        return False
    return await repo.delete(template_id=tid)


class ReactTemplateService:
    """react_templates 的 Config Receiver：create / update / delete。"""

    async def create(self, template: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(template, dict):
            raise ValueError("react_templates.create requires template object")
        if "body" not in template:
            raise ValueError("react_templates.create requires body object")
        repo = require_enterprise_repository(_TABLE)
        await _upsert_react_template_from_sync(repo, template)
        result = {"template_id": _normalize_template_id(template.get("template_id"))}
        logger.info(
            "[ManagerConfigReceiver] react_templates create template_id=%s",
            result["template_id"],
        )
        return result

    async def update(self, template_id: str, updates: dict[str, Any]) -> None:
        if template_id is None:
            raise ValueError("react_templates.update requires template_id")
        if not isinstance(updates, dict) or not updates:
            raise ValueError("react_templates.update requires non-empty updates")
        req = ReactTemplateUpdateRequest.model_validate(updates)
        tid = _normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        existing = await _get_row_for_instance(repo, tid)
        row = await update_react_template(repo, tid, req, existing=existing)
        if row is None:
            raise ValueError(f"react template template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] react_templates update template_id=%s",
            tid,
        )

    async def delete(self, template_id: str) -> None:
        if template_id is None:
            raise ValueError("react_templates.delete requires template_id")
        tid = _normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        await delete_react_template(repo, tid)
        logger.info(
            "[ManagerConfigReceiver] react_templates delete template_id=%s",
            tid,
        )