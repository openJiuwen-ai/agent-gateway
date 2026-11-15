# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved

"""Agent 级高级配置：写入 Gateway 本地库 advanced_config。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.enterprise.tables.application_config_models import (
    ADVANCED_CONFIG_TABLE_DEF,
)

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import format_ts, utc_now

_TABLE = ADVANCED_CONFIG_TABLE_DEF.table_name
logger = logging.getLogger(__name__)

_BOOL_FIELDS = (
    "progressive_tool_enabled",
    "progressive_tool_optimize",
    "a4p_enabled",
    "auto_memory_enabled",
    "enable_kv_cache_affinity",
)


def _require_agent_template_id(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("agent_template_id is required")
    if len(value) > 100:
        raise ValueError("agent_template_id must be <= 100 characters")
    return value


def _coerce_bool(raw: Any, *, field: str, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)) and raw in (0, 1):
        return bool(raw)
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field} must be a boolean")


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "agent_template_id": str(row.get("agent_template_id") or "").strip(),
        "progressive_tool_enabled": bool(row.get("progressive_tool_enabled", True)),
        "progressive_tool_optimize": bool(row.get("progressive_tool_optimize", False)),
        "a4p_enabled": bool(row.get("a4p_enabled", False)),
        "auto_memory_enabled": bool(row.get("auto_memory_enabled", False)),
        "enable_kv_cache_affinity": bool(row.get("enable_kv_cache_affinity", False)),
        "data": row.get("data") if isinstance(row.get("data"), dict) else row.get("data"),
        "created_at": format_ts(row.get("created_at")),
        "updated_at": format_ts(row.get("updated_at")),
    }


def _normalize_payload(request: dict[str, Any]) -> dict[str, Any]:
    payload = dict(request or {})
    nested = payload.get("advanced_config")
    if isinstance(nested, dict):
        payload = dict(nested)
    agent_template_id = _require_agent_template_id(payload.get("agent_template_id"))
    defaults = {
        "progressive_tool_enabled": True,
        "progressive_tool_optimize": False,
        "a4p_enabled": False,
        "auto_memory_enabled": False,
        "enable_kv_cache_affinity": False,
    }
    normalized = {"agent_template_id": agent_template_id}
    for field in _BOOL_FIELDS:
        normalized[field] = _coerce_bool(
            payload.get(field),
            field=field,
            default=defaults[field],
        )
    data = payload.get("data")
    if data is None:
        normalized["data"] = None
    elif isinstance(data, dict):
        normalized["data"] = data
    else:
        raise ValueError("data must be an object or null")
    return normalized


async def _upsert_advanced_config_record(request: dict[str, Any]) -> dict[str, Any]:
    repo = require_enterprise_repository(_TABLE)
    normalized = _normalize_payload(request)
    now = utc_now()
    tid = normalized["agent_template_id"]
    existing = await repo.get({"agent_template_id": tid})
    if existing is None:
        created = await repo.create(
            {
                **normalized,
                "created_at": now,
                "updated_at": now,
            }
        )
        return _row_to_dict(created)
    updated = await repo.update(
        {"agent_template_id": tid},
        {
            **{k: v for k, v in normalized.items() if k != "agent_template_id"},
            "updated_at": now,
        },
    )
    if updated is None:
        raise ValueError("failed to update advanced_config")
    return _row_to_dict(updated)


class AdvancedConfigService:

    async def upsert(
        self,
        request: dict[str, Any] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        payload = dict(request or {})
        payload.update(fields)
        result = await _upsert_advanced_config_record(payload)
        logger.info(
            "[ManagerConfigReceiver] advanced_config saved agent_template_id=%s a4p_enabled=%s",
            result.get("agent_template_id"),
            result.get("a4p_enabled"),
        )
        return result

    async def delete(self, agent_template_id: str) -> None:
        tid = _require_agent_template_id(agent_template_id)
        repo = require_enterprise_repository(_TABLE)
        deleted = await repo.delete({"agent_template_id": tid})
        if not deleted:
            raise ValueError(f"advanced_config agent_template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] advanced_config deleted agent_template_id=%s",
            tid,
        )
