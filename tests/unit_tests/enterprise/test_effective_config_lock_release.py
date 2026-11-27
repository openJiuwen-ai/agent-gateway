# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Per-key single-flight locks end with their last user, never earlier."""

import asyncio

import pytest

pytest.importorskip("openjiuwen_runtime")

from agent_gateway.enterprise.config.effective.loader import _TtlSingleFlightCache


@pytest.mark.asyncio
async def test_invalidate_key_releases_the_key_lock():
    cache = _TtlSingleFlightCache(ttl_seconds=600.0)
    await cache.get_or_fetch("row-1", lambda: asyncio.sleep(0, result="value"))

    assert cache._locks == {}

    cache.invalidate_key("row-1")
    assert "row-1" not in cache._entries
    assert "row-1" not in cache._locks


@pytest.mark.asyncio
async def test_purge_expired_releases_locks_without_entries():
    cache = _TtlSingleFlightCache(ttl_seconds=0.0)
    await cache.get_or_fetch("row-1", lambda: asyncio.sleep(0, result="value"))

    cache._purge_expired()

    assert cache._entries == {}
    assert cache._locks == {}


@pytest.mark.asyncio
async def test_invalidate_key_does_not_bypass_in_flight_fetch():
    cache = _TtlSingleFlightCache(ttl_seconds=600.0)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def fetcher():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "value"

    first = asyncio.create_task(cache.get_or_fetch("row-1", fetcher))
    await entered.wait()
    # 条目已发出但调用方还没拿到锁时失效，不能把锁从表里摘掉让后来者绕过单飞。
    cache.invalidate_key("row-1")
    second = asyncio.create_task(cache.get_or_fetch("row-1", fetcher))
    await asyncio.sleep(0)
    assert calls == 1
    assert not second.done()

    release.set()
    assert await first == "value"
    assert await second == "value"
    assert calls == 1
    assert cache._locks == {}
