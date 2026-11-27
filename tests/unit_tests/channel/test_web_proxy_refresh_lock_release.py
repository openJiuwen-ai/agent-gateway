# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Per-refresh-token single-flight locks must not accumulate forever."""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_connect import (
    WebProxyAuthError,
    _WEB_PROXY_REFRESH_CACHE,
    _WEB_PROXY_REFRESH_FAILURES,
    _WEB_PROXY_REFRESH_LOCKS,
    _authenticate_web_proxy,
)
from tests.unit_tests.channel.test_web_proxy_channel import (
    _FakeIamClient,
    _proxy_channel,
)


def _lock_key(refresh_token: str) -> str:
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def _reset_refresh_tables() -> None:
    _WEB_PROXY_REFRESH_CACHE.clear()
    _WEB_PROXY_REFRESH_FAILURES.clear()
    _WEB_PROXY_REFRESH_LOCKS.clear()


@pytest.mark.asyncio
async def test_refresh_lock_released_after_successful_refresh() -> None:
    _reset_refresh_tables()
    iam = _FakeIamClient(user_id="u1", token="fresh-token")
    channel = _proxy_channel(resolver=lambda *_a, **_k: None, iam=iam)

    await _authenticate_web_proxy(
        channel,
        path="/?access_token=expired&user_id=u1",
        headers={"Cookie": "access_token=expired; refresh_token=refresh-good"},
        remote="",
        query_user_id="u1",
        referer="",
    )

    # 每个 token 的锁只在刷新期间有意义，刷新结束后应随最后一个使用者释放。
    assert _lock_key("refresh-good") not in _WEB_PROXY_REFRESH_LOCKS
    _reset_refresh_tables()


@pytest.mark.asyncio
async def test_refresh_lock_released_after_failed_refresh() -> None:
    _reset_refresh_tables()
    iam = _FakeIamClient(user_id="u1", token="fresh-token", refresh_error=True)
    channel = _proxy_channel(resolver=lambda *_a, **_k: None, iam=iam)

    with pytest.raises(WebProxyAuthError):
        await _authenticate_web_proxy(
            channel,
            path="/?access_token=expired&user_id=u1",
            headers={"Cookie": "access_token=expired; refresh_token=refresh-good"},
            remote="",
            query_user_id="u1",
            referer="",
        )

    # 失败路径提前 return，也必须走到释放分支，否则失败过的 token 永远留一把锁。
    assert _lock_key("refresh-good") not in _WEB_PROXY_REFRESH_LOCKS
    _reset_refresh_tables()


@pytest.mark.asyncio
async def test_concurrent_refresh_keeps_single_flight_then_releases() -> None:
    _reset_refresh_tables()
    started = asyncio.Event()
    gate = asyncio.Event()
    iam = _FakeIamClient(
        user_id="u1",
        token="fresh-token",
        refresh_started=started,
        refresh_gate=gate,
    )
    channel = _proxy_channel(resolver=lambda *_a, **_k: None, iam=iam)

    async def authenticate() -> tuple[str, str | None]:
        return await _authenticate_web_proxy(
            channel,
            path="/?access_token=expired&user_id=u1",
            headers={"Cookie": "access_token=expired; refresh_token=refresh-good"},
            remote="",
            query_user_id="u1",
            referer="",
        )

    first = asyncio.create_task(authenticate())
    await asyncio.wait_for(started.wait(), timeout=5)
    second = asyncio.create_task(authenticate())
    await asyncio.sleep(0.05)
    assert not second.done()
    gate.set()
    results = await asyncio.gather(first, second)

    # 锁仍然只让一个请求真正刷新，其余请求命中 30 秒缓存。
    assert iam.refresh_calls == ["refresh-good"]
    assert results == [("u1", "fresh-token"), ("u1", "fresh-token")]
    assert _lock_key("refresh-good") not in _WEB_PROXY_REFRESH_LOCKS
    _reset_refresh_tables()
