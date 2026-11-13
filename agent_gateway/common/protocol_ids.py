# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Validation for externally supplied protocol identifiers.

These checks must run before identifiers are stored in routing tables or written
into logs.  The session limit matches the existing persistent-session contract:
80 ASCII characters, with dots and hyphens allowed only in the middle.
"""

from __future__ import annotations

import re
from typing import Any

SESSION_ID_MAX_LEN = 80

_SESSION_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9_](?:[A-Za-z0-9_.-]{0,78}[A-Za-z0-9_])?$"
)


class InvalidProtocolId(ValueError):
    """An externally supplied identifier violates the wire contract."""


def validate_session_id(value: Any) -> str:
    """Return a valid session id or raise a value-safe error.

    The exception deliberately never includes the original value: callers may
    safely return or log the message even when the input is extremely large.
    """
    if not isinstance(value, str):
        raise InvalidProtocolId("session_id must be a string")
    if not value or value != value.strip():
        raise InvalidProtocolId("invalid session_id")
    if len(value) > SESSION_ID_MAX_LEN:
        raise InvalidProtocolId(
            f"session_id exceeds maximum length {SESSION_ID_MAX_LEN}"
        )
    if _SESSION_ID_PATTERN.fullmatch(value) is None:
        raise InvalidProtocolId("invalid session_id")
    return value


def is_valid_session_id(value: Any) -> bool:
    """Return whether *value* satisfies the shared session-id contract."""
    try:
        validate_session_id(value)
    except InvalidProtocolId:
        return False
    return True
