# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved

"""高级配置模板：将 Manager 下发的 advanced_config_templates 写入 Gateway 本地库。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.enterprise.repository import EnterpriseRecordRepository
from agent_gateway.config.enterprise.tables.template_models import (
    ADVANCED_CONFIG_TEMPLATE_TABLE_DEF,
)

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import parse_iso_datetime, utc_now
from ...schemas.advanced_config_fields import (
    ADVANCED_CONFIG_BOOL_FIELDS,
    DEFAULT_ADVANCED_CONFIG_SWITCHES,
    normalize_advanced_config_switches,
    validate_advanced_config_data,
)
from ...schemas.template_schemas import AdvancedConfigTemplateUpdateRequest
from .template_id import normalize_template_id

_TABLE = ADVANCED_CONFIG_TEMPLATE_TABLE_DEF.table_name
logger = logging.getLogger(__name__)


async def _get_row(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> dict[str, Any] | None:
    return await repo.get(template_id=normalize_template_id(template_id))


async def update_advanced_config_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
    request: AdvancedConfigTemplateUpdateRequest,
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

    bool_payload = {
        key: updates[key]
        for key in ADVANCED_CONFIG_BOOL_FIELDS
        if key in updates
    }
    if bool_payload:
        updates.update(normalize_advanced_config_switches(bool_payload))

    if "data" in updates:
        updates["data"] = validate_advanced_config_data(updates["data"])

    if not updates:
        raise ValueError("请求未包含任何可更新的业务字段")

    updates["updated_at"] = utc_now()
    updated = await repo.update({"template_id": tid}, updates)
    if updated is None:
        return None
    return {"template_id": str(updated.get("template_id", tid))}


async def delete_advanced_config_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
) -> bool:
    tid = normalize_template_id(template_id)
    existing = await _get_row(repo, tid)
    if existing is None:
        return False
    return await repo.delete(template_id=tid)


def _merge_switches_for_create(template: dict[str, Any]) -> dict[str, bool]:
    payload = dict(DEFAULT_ADVANCED_CONFIG_SWITCHES)
    for key in ADVANCED_CONFIG_BOOL_FIELDS:
        if key in template:
            payload[key] = template[key]
    return normalize_advanced_config_switches(payload, require_all=True)


def _build_row_from_template(
    template: dict[str, Any],
    *,
    now: Any,
) -> dict[str, Any]:
    switches = _merge_switches_for_create(template)
    return {
        "template_id": normalize_template_id(template.get("template_id")),
        "template_name": str(template["template_name"]).strip(),
        "description": template.get("description"),
        "enabled": bool(template.get("enabled", True)),
        **switches,
        "data": validate_advanced_config_data(template.get("data")),
        "created_at": now,
        "updated_at": now,
    }


async def _upsert_advanced_config_template_from_sync(
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


class AdvancedConfigTemplateService:

    async def create(self, template: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(template, dict):
            raise ValueError("advanced_config_templates.create requires template object")
        tid_raw = template.get("template_id")
        name_raw = template.get("template_name")
        if tid_raw is None or not str(tid_raw).strip():
            raise ValueError("advanced_config_templates.create requires template_id")
        if name_raw is None or not str(name_raw).strip():
            raise ValueError("advanced_config_templates.create requires template_name")
        repo = require_enterprise_repository(_TABLE)
        await _upsert_advanced_config_template_from_sync(repo, template)
        result = {"template_id": normalize_template_id(template.get("template_id"))}
        logger.info(
            "[ManagerConfigReceiver] advanced_config_templates create template_id=%s",
            result["template_id"],
        )
        return result

    async def update(self, template_id: str, updates: dict[str, Any]) -> None:
        if template_id is None:
            raise ValueError("advanced_config_templates.update requires template_id")
        if not isinstance(updates, dict) or not updates:
            raise ValueError("advanced_config_templates.update requires non-empty updates")
        req = AdvancedConfigTemplateUpdateRequest.model_validate(updates)
        tid = normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        existing = await _get_row(repo, tid)
        row = await update_advanced_config_template(repo, tid, req, existing=existing)
        if row is None:
            raise ValueError(f"advanced config template template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] advanced_config_templates update template_id=%s",
            tid,
        )

    async def delete(self, template_id: str) -> None:
        if template_id is None:
            raise ValueError("advanced_config_templates.delete requires template_id")
        tid = normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        await delete_advanced_config_template(repo, tid)
        logger.info(
            "[ManagerConfigReceiver] advanced_config_templates delete template_id=%s",
            tid,
        )