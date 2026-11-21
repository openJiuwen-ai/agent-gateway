# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved

"""Agent 级 A2UI 配置：写入 Gateway 本地库 a2ui_config。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.enterprise.tables.application_config_models import (
    A2UI_CONFIG_TABLE_DEF,
)

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import format_ts, parse_iso_datetime, utc_now

_TABLE = A2UI_CONFIG_TABLE_DEF.table_name
logger = logging.getLogger(__name__)

SUPPORTED_A2UI_PROTOCOL_VERSIONS = frozenset({"0.8"})

_BOOL_FIELDS = (
    "enabled",
    "stream_validation_enabled",
    "dev_smoke_tools_enabled",
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


def _require_protocol_version(raw: Any) -> str:
    value = str(raw if raw is not None else "0.8").strip() or "0.8"
    if value not in SUPPORTED_A2UI_PROTOCOL_VERSIONS:
        raise ValueError(
            f"protocol_version must be one of {sorted(SUPPORTED_A2UI_PROTOCOL_VERSIONS)}, "
            f"got {value!r}"
        )
    return value


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "agent_template_id": str(row.get("agent_template_id") or "").strip(),
        "enabled": bool(row.get("enabled", False)),
        "protocol_version": str(row.get("protocol_version") or "0.8").strip() or "0.8",
        "stream_validation_enabled": bool(row.get("stream_validation_enabled", True)),
        "dev_smoke_tools_enabled": bool(row.get("dev_smoke_tools_enabled", False)),
        "data": row.get("data") if isinstance(row.get("data"), dict) else row.get("data"),
        "created_at": format_ts(row.get("created_at")),
        "updated_at": format_ts(row.get("updated_at")),
    }


def _normalize_payload(request: dict[str, Any]) -> dict[str, Any]:
    payload = dict(request or {})
    nested = payload.get("a2ui_config")
    if isinstance(nested, dict):
        payload = dict(nested)
    agent_template_id = _require_agent_template_id(payload.get("agent_template_id"))
    defaults = {
        "enabled": False,
        "stream_validation_enabled": True,
        "dev_smoke_tools_enabled": False,
    }
    normalized: dict[str, Any] = {
        "agent_template_id": agent_template_id,
        "protocol_version": _require_protocol_version(payload.get("protocol_version")),
    }
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


async def _upsert_a2ui_config_record(request: dict[str, Any]) -> dict[str, Any]:
    repo = require_enterprise_repository(_TABLE)
    normalized = _normalize_payload(request)
    now = utc_now()
    tid = normalized["agent_template_id"]
    existing = await repo.get({"agent_template_id": tid})
    created_at = now
    if existing is not None:
        # update 分支沿用已有 created_at，避免 upsert 浅合并冲掉创建时间。
        created_at = parse_iso_datetime(existing.get("created_at")) or now
    saved = await repo.upsert(
        {
            **normalized,
            "created_at": created_at,
            "updated_at": now,
        }
    )
    return _row_to_dict(saved)


class A2UIConfigService:

    async def upsert(
        self,
        request: dict[str, Any] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        payload = dict(request or {})
        payload.update(fields)
        result = await _upsert_a2ui_config_record(payload)
        logger.info(
            "[ManagerConfigReceiver] a2ui_config saved agent_template_id=%s enabled=%s "
            "protocol_version=%s",
            result.get("agent_template_id"),
            result.get("enabled"),
            result.get("protocol_version"),
        )
        return result

    async def delete(self, agent_template_id: str) -> None:
        tid = _require_agent_template_id(agent_template_id)
        repo = require_enterprise_repository(_TABLE)
        deleted = await repo.delete({"agent_template_id": tid})
        if not deleted:
            raise ValueError(f"a2ui_config agent_template_id={tid!r} not found")
        logger.info(
            "[ManagerConfigReceiver] a2ui_config deleted agent_template_id=%s",
            tid,
        )
