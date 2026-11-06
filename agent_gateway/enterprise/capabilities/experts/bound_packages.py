# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""按当前 bot 的 Agent 模板槽展开出站专家包。空名单表示这只 bot 没有可用包。"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

_SLOT = "expert_package"
_PACKAGE_KINDS = frozenset({"agent_template", "agent_group"})


def _template_ref(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("template_ref")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw if isinstance(raw, dict) else {}


def _slot_ids(raw: Any) -> list[str]:
    if isinstance(raw, str):
        text = raw.strip()
        return [text] if text else []
    if not isinstance(raw, list):
        return []
    ids: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if text:
            ids.append(text)
    return ids


def template_ids_from_template_ref(template_ref: Any) -> list[str]:
    """``expert_package`` 槽里的值是 expert_package_template.template_id。"""
    refs = template_ref if isinstance(template_ref, dict) else {}
    ids: list[str] = []
    seen: set[str] = set()
    for template_id in _slot_ids(refs.get(_SLOT)):
        if template_id in seen:
            continue
        seen.add(template_id)
        ids.append(template_id)
    return ids


def _packages_on_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("packages")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def packages_from_template_rows(
    template_ids: list[str],
    rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """启用中的模板行把 packages 展开成 ``{kind, id}``。id 是 Hub asset_id。找不到的标识跳过。"""
    by_id = {
        str(row.get("template_id") or "").strip(): row
        for row in rows
        if isinstance(row, dict)
    }
    packages: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for template_id in template_ids:
        row = by_id.get(template_id)
        if row is None or not row.get("enabled", True):
            continue
        for item in _packages_on_row(row):
            kind = str(item.get("kind") or "").strip()
            asset_id = str(item.get("asset_id") or "").strip()
            if kind not in _PACKAGE_KINDS or not asset_id:
                continue
            key = (kind, asset_id)
            if key in seen:
                continue
            seen.add(key)
            packages.append({"kind": kind, "id": asset_id})
    return packages


async def packages_for_bot(bot_id: str) -> list[dict[str, str]]:
    """企业版按 bot_id 读本机 Gateway 上的 Agent 模板槽，再展开专家包模板行。"""
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return []
    rid = str(bot_id or "").strip()
    if not rid:
        return []
    try:
        from agent_gateway.enterprise.config.effective import db_queries

        resources = await db_queries.list_records(
            "instance_agent_resource",
            filters={"enabled": True, "resource_id": rid},
        )
        if not resources:
            return []
        template_id = str(resources[0].get("ref_template_id") or "").strip()
        if not template_id:
            return []
        templates = await db_queries.list_records(
            "agent_template",
            filters={"enabled": True, "template_id": template_id},
        )
        if not templates:
            return []
        wanted = template_ids_from_template_ref(_template_ref(templates[0]))
        if not wanted:
            return []
        rows = await db_queries.list_records(
            "expert_package_template",
            filters={"enabled": True},
        )
        return packages_from_template_rows(wanted, rows)
    except Exception:
        logger.warning(
            "[expert_packages] bot template slots unreadable, sending empty bot_id=%s",
            rid,
            exc_info=True,
        )
        return []
