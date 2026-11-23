"""Shared outgoing text extraction for IM channels."""

from e2a.message import EventType, Message


def extract_outgoing_text(
    msg: Message,
    *,
    strip: bool = True,
    include_error: bool = False,
) -> str:
    """Extract user-facing text while preserving channel-specific fallbacks."""
    payload = getattr(msg, "payload", None) or {}

    if msg.event_type == EventType.HEALTH_CHECK_RELAY and isinstance(payload, dict):
        health_check = payload.get("health_check")
        if health_check:
            return _normalize(_text_value(health_check), strip)

    if isinstance(payload, dict):
        if "content" in payload:
            return _normalize(_text_value(payload.get("content") or ""), strip)
        if include_error and payload.get("error"):
            return _normalize(_text_value(payload.get("error")), strip)

    params = msg.params
    if params and "content" in params:
        return _normalize(_text_value(params.get("content") or ""), strip)
    if isinstance(msg.payload, str):
        return _normalize(msg.payload, strip)
    return ""


def _text_value(value: object) -> str:
    if isinstance(value, dict):
        for key in ("output", "text"):
            if key in value:
                return _text_value(value[key])
        return ""
    if value is None:
        return ""
    return str(value)


def _normalize(text: str, strip: bool) -> str:
    return text.strip() if strip else text
