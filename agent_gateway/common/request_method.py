# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared parsing helpers for protocol request methods."""

from __future__ import annotations

from agent_gateway.common.protocol_methods import ReqMethod


def parse_request_method(method: str) -> ReqMethod | None:
    """Parse a request method string into its protocol enum member."""
    if not method:
        return None
    return next((item for item in ReqMethod if item.value == method), None)
