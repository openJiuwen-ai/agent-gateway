# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""专家包模板：Manager 下发的 expert_package_template 写入 Gateway 本地库。"""

from __future__ import annotations

import json
import logging
from typing import Any

from agent_gateway.config.enterprise.repository import EnterpriseRecordRepository
from agent_gateway.config.enterprise.tables.template_models import EXPERT_PACKAGE_TEMPLATE_TABLE_DEF
from agent_gateway.common.expert_packages import HIDDEN_HUB_ASSET_IDS

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import parse_iso_datetime, utc_now
from ...schemas.template_schemas import ExpertPackageTemplateUpdateRequest

_TABLE = EXPERT_PACKAGE_TEMPLATE_TABLE_DEF.table_name
_PACKAGE_KINDS = frozenset({"agent_template", "agent_group"})
logger = logging.getLogger(__name__)


def _normalize_template_id(template_id: Any) -> str:
    normalized = str(template_id or "").strip()
    if not normalized:
        raise ValueError("template_id is required")
    if len(normalized) > 100:
        raise ValueError("template_id must be at most 100 characters")
    return normalized


def _normalize_asset_id(value: Any) -> str:
    asset = str(value or "").strip()
    if not asset or asset in {".", ".."} or len(asset) > 128 or "/" in asset or "\\" in asset:
        raise ValueError("invalid hub asset id")
    if asset in HIDDEN_HUB_ASSET_IDS:
        raise ValueError("hidden hub asset cannot be included in an expert package")
    return asset


def _normalize_kind(value: Any) -> str:
    kind = str(value or "").strip()
    if kind not in _PACKAGE_KINDS:
        raise ValueError("package kind must be agent_template or agent_group")
    return kind


def _coerce_packages(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("packages must be a list") from exc
    if not isinstance(value, list):
        raise ValueError("packages must be a list")
    return value


def normalize_packages(value: Any) -> list[dict[str, str]]:
    """按 (kind, asset_id) 去重。隐藏或非法的 Hub 标识不能写入。"""
    seen: set[tuple[str, str]] = set()
    packages: list[dict[str, str]] = []
    for item in _coerce_packages(value):
        if hasattr(item, "model_dump"):
            item = item.model_dump()
        if not isinstance(item, dict):
            raise ValueError("packages item must be an object")
        kind = _normalize_kind(item.get("kind"))
        asset_id = _normalize_asset_id(item.get("asset_id"))
        version = str(item.get("version") or "").strip()
        if len(version) > 64:
            raise ValueError("version must be at most 64 characters")
        key = (kind, asset_id)
        if key in seen:
            continue
        seen.add(key)
        packages.append({"kind": kind, "asset_id": asset_id, "version": version})
    if not packages:
        raise ValueError("packages must contain at least one hub package")
    return packages


def _normalize_row_fields(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("template_name") or "").strip()
    if not name or len(name) > 128:
        raise ValueError("template_name is required")
    description = payload.get("description")
    if description is not None:
        description = str(description).strip() or None
        if description is not None and len(description) > 512:
            raise ValueError("description must be at most 512 characters")
    return {
        "template_name": name,
        "description": description,
        "packages": normalize_packages(payload.get("packages")),
        "enabled": bool(payload.get("enabled", True)),
    }


async def _get_row(repo: EnterpriseRecordRepository, template_id: str) -> dict[str, Any] | None:
    return await repo.get(template_id=_normalize_template_id(template_id))


def _build_row(template: dict[str, Any], *, now: Any) -> dict[str, Any]:
    fields = _normalize_row_fields(template)
    return {
        "template_id": _normalize_template_id(template.get("template_id")),
        **fields,
        "created_at": now,
        "updated_at": now,
    }


async def _upsert(repo: EnterpriseRecordRepository, template: dict[str, Any]) -> None:
    now = utc_now()
    tid = _normalize_template_id(template.get("template_id"))
    existing = await _get_row(repo, tid)
    row_data = _build_row(template, now=now)
    if existing is None:
        await repo.create(row_data)
        return
    created_at = existing.get("created_at")
    if created_at is not None:
        row_data["created_at"] = parse_iso_datetime(created_at) or now
    updates = {key: value for key, value in row_data.items() if key != "template_id"}
    updates["updated_at"] = utc_now()
    await repo.update({"template_id": tid}, updates)


async def update_expert_package_template(
    repo: EnterpriseRecordRepository,
    template_id: str,
    request: ExpertPackageTemplateUpdateRequest,
    *,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    tid = _normalize_template_id(template_id)
    row = existing if existing is not None else await _get_row(repo, tid)
    if row is None:
        return None
    updates = request.model_dump(exclude_unset=True)
    if not updates:
        raise ValueError("请求未包含任何可更新的业务字段")
    merged = dict(row)
    merged.update(updates)
    normalized = _normalize_row_fields(merged)
    updates["template_name"] = normalized["template_name"]
    updates["packages"] = normalized["packages"]
    updates["enabled"] = normalized["enabled"]
    if "description" in request.model_dump(exclude_unset=True) or normalized["description"] != row.get("description"):
        updates["description"] = normalized["description"]
    updates["updated_at"] = utc_now()
    updated = await repo.update({"template_id": tid}, updates)
    if updated is None:
        return None
    return {"template_id": str(updated.get("template_id", tid))}


async def delete_expert_package_template(repo: EnterpriseRecordRepository, template_id: str) -> bool:
    tid = _normalize_template_id(template_id)
    existing = await _get_row(repo, tid)
    if existing is None:
        return False
    return await repo.delete(template_id=tid)


class ExpertPackageTemplateService:
    async def create(self, template: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(template, dict):
            raise ValueError("expert_package_templates.create requires template object")
        repo = require_enterprise_repository(_TABLE)
        await _upsert(repo, template)
        result = {"template_id": _normalize_template_id(template.get("template_id"))}
        logger.info(
            "[ManagerConfigReceiver] expert_package_templates create template_id=%s",
            result["template_id"],
        )
        return result

    async def update(self, template_id: str, updates: dict[str, Any]) -> None:
        if not template_id:
            raise ValueError("expert_package_templates.update requires template_id")
        if not isinstance(updates, dict) or not updates:
            raise ValueError("expert_package_templates.update requires non-empty updates")
        req = ExpertPackageTemplateUpdateRequest.model_validate(updates)
        tid = _normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        existing = await _get_row(repo, tid)
        row = await update_expert_package_template(repo, tid, req, existing=existing)
        if row is None:
            raise ValueError(f"expert package template template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] expert_package_templates update template_id=%s",
            tid,
        )

    async def delete(self, template_id: str) -> None:
        if not template_id:
            raise ValueError("expert_package_templates.delete requires template_id")
        tid = _normalize_template_id(template_id)
        repo = require_enterprise_repository(_TABLE)
        await delete_expert_package_template(repo, tid)
        logger.info(
            "[ManagerConfigReceiver] expert_package_templates delete template_id=%s",
            tid,
        )
