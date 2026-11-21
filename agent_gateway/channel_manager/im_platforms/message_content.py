"""Helpers for selecting user-facing content from gateway messages."""

import json
from typing import Any


def select_message_content(params: Any, payload: Any) -> Any:
    """Select params content before payload content and unwrap output objects."""
    params = params if isinstance(params, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    content = params.get("content") or payload.get("content") or ""
    if isinstance(content, dict):
        if "output" in content:
            return content["output"]
        try:
            return json.dumps(content, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(content)
    return content
