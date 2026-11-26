# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Normalize match expressions read from persistence."""

from __future__ import annotations

import json
from typing import Any


def coerce_match_expr(raw: Any) -> Any:
    """Return a structured JSON value, or stripped text when parsing fails."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, (list, dict)):
        return raw
    text = str(raw).strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


__all__ = ("coerce_match_expr",)
