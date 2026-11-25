# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""App selection for Xiaoyi runtime identity updates."""

from typing import Any


def find_xiaoyi_app(apps: list[Any], api_id: str, agent_id: str) -> dict[str, Any] | None:
    """Prefer API ID, then agent ID, then the default, then a sole app."""
    for field, value in (("api_id", api_id), ("agent_id", agent_id)):
        wanted = str(value or "").strip()
        if wanted:
            for app in apps:
                if isinstance(app, dict) and str(app.get(field) or "").strip() == wanted:
                    return app
    for app in apps:
        if isinstance(app, dict) and app.get("is_default", False):
            return app
    if len(apps) == 1 and isinstance(apps[0], dict):
        return apps[0]
    return None
