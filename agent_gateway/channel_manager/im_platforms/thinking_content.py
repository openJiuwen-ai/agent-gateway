# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared filtering of thinking blocks and placeholders in IM responses."""

import re


def strip_think_tags(text: object) -> str:
    """Remove complete and trailing unclosed think blocks from response text."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r"<think>[\s\S]*?</think>", "", text, flags=re.IGNORECASE)
    return re.sub(r"<think>[\s\S]*$", "", text, flags=re.IGNORECASE)


def is_thinking_only_content(text: object) -> bool:
    """Return whether a response is empty, non-text, or only placeholder dots."""
    if not isinstance(text, str):
        return True
    stripped = text.strip()
    return not stripped or re.fullmatch(r"[.\uFF0E\u3002\u2026\s]+", stripped) is not None
