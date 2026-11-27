# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Per-key single-flight locks end with the cache entry they guard."""

import asyncio

import pytest

pytest.importorskip("openjiuwen_runtime")

from agent_gateway.enterprise.config.effective.loader import _TtlSingleFlightCache


@pytest.mark.asyncio
async def test_invalidate_key_releases_the_key_lock():
    cache = _TtlSingleFlightCache(ttl_seconds=600.0)
    await cache.get_or_fetch("row-1", lambda: asyncio.sleep(0, result="value"))
    assert "row-1" in cache._locks

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
