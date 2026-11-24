# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Manager 下发的集群默认语言 → Gateway ``user_preference.__default__``。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.config.user_preference import get_user_preference_service
from agent_gateway.config.user_preference.service import normalize_extras

logger = logging.getLogger(__name__)


class UserPreferenceDefaultService:
    async def upsert(self, *, preferred_language: str, **fields: Any) -> dict[str, Any]:
        """写 ``__default__``。

        ``fields`` 含 ``extras`` 时才更新扩展袋（含显式 ``null``）；未传则保留库内值。
        """
        kwargs: dict[str, Any] = {}
        if "extras" in fields:
            kwargs["extras"] = normalize_extras(fields.get("extras"))
        lang = await get_user_preference_service().upsert_default_language(
            preferred_language,
            **kwargs,
        )
        logger.info(
            "[ManagerConfigReceiver] user_preference default upserted language=%s",
            lang,
        )
        payload: dict[str, Any] = {
            "preferred_language": lang,
            "user_id": "__default__",
        }
        if "extras" in kwargs:
            payload["extras"] = kwargs["extras"]
        return payload
