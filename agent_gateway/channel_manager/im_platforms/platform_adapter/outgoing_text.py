"""Shared outgoing text extraction for IM channels."""

from e2a.message import EventType, Message


def extract_outgoing_text(
    msg: Message,
    *,
    strip: bool = True,
    include_error: bool = False,
) -> str:
    """Extract text in the common IM priority order while retaining channel options."""
    payload = getattr(msg, "payload", None) or {}

    if msg.event_type == EventType.HEALTH_CHECK_RELAY and isinstance(payload, dict):
        health_check = payload.get("health_check")
        if health_check:
            return _normalize(str(health_check), strip)

    if isinstance(payload, dict):
        if "content" in payload:
            content = payload.get("content")
            if isinstance(content, dict):
                return _normalize(str(content.get("output", content)), strip)
            return _normalize(str(content or ""), strip)
        if include_error and payload.get("error"):
            return _normalize(str(payload.get("error")), strip)

    params = msg.params
    if params and "content" in params:
        return _normalize(str(params.get("content") or ""), strip)
    if isinstance(msg.payload, str):
        return _normalize(msg.payload, strip)
    return ""


def _normalize(text: str, strip: bool) -> str:
    return text.strip() if strip else text
