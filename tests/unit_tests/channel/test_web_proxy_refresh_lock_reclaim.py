"""web_proxy 的每个 refresh token 单飞锁不能永久留在全局表里。

锁表按 refresh token 去重，token 数量随用户增长；空闲锁应在没有协程引用后
自动回收，避免全局字典无界增长。
"""

import asyncio
import gc
import weakref

from agent_gateway.channel_manager.protocol.web_proxy import web_proxy_connect as wp


def test_same_key_shares_one_lock_while_in_use():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _acquire():
        first = await wp._refresh_lock_for("token-a")
        second = await wp._refresh_lock_for("token-a")
        return first, second

    first, second = asyncio.run(_acquire())
    assert first is second
    assert "token-a" in wp._WEB_PROXY_REFRESH_LOCKS


def test_distinct_keys_get_distinct_locks():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _acquire():
        return await wp._refresh_lock_for("token-a"), await wp._refresh_lock_for("token-b")

    first, second = asyncio.run(_acquire())
    assert first is not second


def test_idle_lock_is_reclaimed():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _acquire():
        lock = await wp._refresh_lock_for("token-a")
        return lock, weakref.ref(lock)

    lock, ref = asyncio.run(_acquire())
    assert "token-a" in wp._WEB_PROXY_REFRESH_LOCKS

    del lock
    gc.collect()

    assert ref() is None
    assert "token-a" not in wp._WEB_PROXY_REFRESH_LOCKS
