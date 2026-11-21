"""Shared text helpers for inbound and outbound IM messages."""

import re
from typing import Any


def strip_think_tags(text: str) -> str:
    """Remove complete and trailing unclosed think blocks from response text."""
    if not text or not isinstance(text, str):
        return text or ""
    text = re.sub(r"<think>[\s\S]*?</think>", "", text, flags=re.IGNORECASE)
    return re.sub(r"<think>[\s\S]*$", "", text, flags=re.IGNORECASE)
