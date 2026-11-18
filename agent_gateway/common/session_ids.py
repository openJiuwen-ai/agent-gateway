# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Generation helpers for gateway-owned protocol identifiers."""

from __future__ import annotations

import secrets
import time


def make_session_id() -> str:
    """Return a session ID compatible with the frontend generator format."""
    ts = format(int(time.time() * 1000), "x")
    suffix = secrets.token_hex(3)
    return f"sess_{ts}_{suffix}"
