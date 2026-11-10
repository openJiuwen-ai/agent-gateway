# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""按 Agent 模板保存的能力开关。一个功能一张表，一行一个模板。"""

from __future__ import annotations

import logging
from typing import Any

from ...infrastructure.repository_access import require_enterprise_repository
from ...infrastructure.utils import utc_now

logger = logging.getLogger(__name__)

CAPABILITY_TABLES: dict[str, str] = {
    "a2ui": "a2ui_config",
    "a4p": "a4p_config",
    "symphony": "symphony_config",
}


def capability_body(business: dict[str, Any]) -> dict[str, Any]:
    """去掉模板编号，剩下的字段原样作为要覆盖的配置。"""
    body = {
        key: value
        for key, value in business.items()
        if key != "agent_template_id" and value is not None
    }
    if not body:
        raise ValueError("capability body must include at least one field")
    return body


class AgentCapabilityConfigService:

    def __init__(self, kind: str) -> None:
        try:
            self._table = CAPABILITY_TABLES[kind]
        except KeyError as exc:
            raise ValueError(f"unknown agent capability {kind!r}") from exc
        self._kind = kind

    async def upsert(
        self,
        agent_template_id: str,
        body: dict[str, Any],
        *,
        source: str = "manager",
    ) -> dict[str, Any]:
        template_id = str(agent_template_id or "").strip()
        if not template_id:
            raise ValueError("agent_template_id is required")
        if not isinstance(body, dict) or not body:
            raise ValueError("capability body must be an object")

        repo = require_enterprise_repository(self._table)
        existing = await repo.get({"agent_template_id": template_id})
        now = utc_now()
        revision = 1
        if existing is not None:
            revision = int(existing.get("revision") or 0) + 1
            saved = await repo.update(
                {"agent_template_id": template_id},
                {
                    "body": dict(body),
                    "source": str(source or "manager"),
                    "revision": revision,
                    "updated_at": now,
                },
            )
            if saved is None:
                raise ValueError(f"failed to update {self._table}")
        else:
            saved = await repo.create(
                {
                    "agent_template_id": template_id,
                    "body": dict(body),
                    "source": str(source or "manager"),
                    "revision": revision,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        logger.info(
            "[ManagerConfigReceiver] %s saved template=%s revision=%s",
            self._table,
            template_id,
            revision,
        )
        return {
            "agent_template_id": template_id,
            "body": dict(body),
            "source": str(source or "manager"),
            "revision": revision,
        }

    async def delete(self, agent_template_id: str) -> None:
        template_id = str(agent_template_id or "").strip()
        if not template_id:
            raise ValueError("agent_template_id is required")
        repo = require_enterprise_repository(self._table)
        await repo.delete({"agent_template_id": template_id})
        logger.info(
            "[ManagerConfigReceiver] %s deleted template=%s",
            self._table,
            template_id,
        )
