# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Shared bounded response-body reading for extension source providers."""

from __future__ import annotations

from collections.abc import Callable

import httpx


async def read_response_body_capped(
    response: httpx.Response,
    max_bytes: int,
    error_type: Callable[[str, str], Exception],
) -> bytes:
    """Read a response while preserving the caller's download error contract."""
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError:
            declared = -1
        if declared > max_bytes:
            raise error_type("content_too_large", "下载内容超过来源允许的最大大小")

    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_bytes():
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise error_type("content_too_large", "下载内容超过来源允许的最大大小")
        chunks.append(chunk)
    return b"".join(chunks)
