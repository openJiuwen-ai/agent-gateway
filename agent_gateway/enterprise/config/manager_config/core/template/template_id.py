"""Shared validation for enterprise template identifiers."""

from __future__ import annotations

from typing import Any

_TEMPLATE_ID_MAX_LENGTH = 100


def normalize_template_id(template_id: Any) -> str:
    normalized = str(template_id or "").strip()
    if not normalized:
        raise ValueError("template_id is required")
    if len(normalized) > _TEMPLATE_ID_MAX_LENGTH:
        raise ValueError("template_id must be at most 100 characters")
    return normalized
