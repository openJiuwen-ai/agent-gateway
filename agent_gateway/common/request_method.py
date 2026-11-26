# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared parsing helpers for protocol request methods."""

from __future__ import annotations

from typing import Any

from agent_gateway.common.protocol_methods import ReqMethod


def parse_request_method(method: str) -> ReqMethod | None:
    """Parse a request method string into its protocol enum member."""
    if not method:
        return None
    return next((item for item in ReqMethod if item.value == method), None)


def request_method_name(req_method: Any) -> str:
    """Return the wire name of a request method.

    Method names are opaque strings on the wire: known methods usually arrive as
    ``ReqMethod`` members, while methods added on the AgentServer side stay plain
    strings until the gateway knows about them. Both must normalize identically.
    """
    if req_method is None:
        return ""
    value = getattr(req_method, "value", req_method)
    return str(value or "")
