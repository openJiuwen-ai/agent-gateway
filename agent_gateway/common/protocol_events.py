# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Register Gateway-owned events missing from the pinned E2A protocol."""

from __future__ import annotations

from e2a.message import EventType

GATEWAY_EVENT_TYPES: tuple[tuple[str, str], ...] = (
    ("CHAT_CLIENT_TOOL_REQUEST", "chat.client_tool_request"),
    ("CLIENT_ATTACHMENTS_CHANGED", "client.attachments_changed"),
)


def _register_event_type(name: str, value: str) -> None:
    existing = EventType.__members__.get(name)
    if existing is not None:
        if existing.value != value:
            raise RuntimeError(
                f"EventType.{name} is {existing.value!r}, expected {value!r}",
            )
        return
    if any(item.value == value for item in EventType):
        raise RuntimeError(f"EventType value {value!r} is already registered")

    member = object.__new__(EventType)
    member._name_ = name  # pylint: disable=protected-access
    member._value_ = value  # pylint: disable=protected-access
    type.__setattr__(EventType, name, member)
    EventType._member_map_[name] = member  # pylint: disable=protected-access
    EventType._value2member_map_[value] = member  # pylint: disable=protected-access
    EventType._member_names_.append(name)  # pylint: disable=protected-access


def ensure_gateway_event_types() -> None:
    """Add only the Gateway-owned events absent from the protocol package."""
    for name, value in GATEWAY_EVENT_TYPES:
        _register_event_type(name, value)
