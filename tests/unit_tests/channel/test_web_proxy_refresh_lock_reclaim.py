"""web_proxy 的每个 refresh token 单飞锁不能永久留在全局表里。

锁表按 refresh token 去重，token 数量随用户增长；锁只应在还有协程使用时
存在，最后一个使用者退出后立即回收，避免全局字典无界增长。
"""

import asyncio

from agent_gateway.channel_manager.protocol.web_proxy import web_proxy_connect as wp


def test_lock_entry_is_dropped_after_use():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _enter_once():
        async with wp._refresh_lock("token-a"):
            lock, users = wp._WEB_PROXY_REFRESH_LOCKS["token-a"]
            assert users == 1
            assert lock is not None
        assert "token-a" not in wp._WEB_PROXY_REFRESH_LOCKS

    asyncio.run(asyncio.wait_for(_enter_once(), timeout=5))


def test_distinct_keys_get_distinct_locks():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _enter_both():
        async with wp._refresh_lock("token-a"):
            async with wp._refresh_lock("token-b"):
                first = wp._WEB_PROXY_REFRESH_LOCKS["token-a"][0]
                second = wp._WEB_PROXY_REFRESH_LOCKS["token-b"][0]
                return first, second

    first, second = asyncio.run(asyncio.wait_for(_enter_both(), timeout=5))
    assert first is not second
    assert wp._WEB_PROXY_REFRESH_LOCKS == {}


def test_waiting_task_keeps_the_lock_registered():
    wp._WEB_PROXY_REFRESH_LOCKS.clear()

    async def _run():
        holder_entered = asyncio.Event()
        holder_release = asyncio.Event()
        holder_lock = None

        async def holder():
            nonlocal holder_lock
            async with wp._refresh_lock("token-a"):
                holder_lock = wp._WEB_PROXY_REFRESH_LOCKS["token-a"][0]
                holder_entered.set()
                await holder_release.wait()

        async def waiter():
            await holder_entered.wait()
            async with wp._refresh_lock("token-a"):
                assert wp._WEB_PROXY_REFRESH_LOCKS["token-a"][0] is holder_lock

        holder_task = asyncio.create_task(holder())
        await holder_entered.wait()
        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0)

        # 持锁者退出前，等待者已经在同一把锁上排队，条目不能被提前回收。
        _, users = wp._WEB_PROXY_REFRESH_LOCKS["token-a"]
        assert users == 2

        holder_release.set()
        await asyncio.gather(holder_task, waiter_task)
        assert "token-a" not in wp._WEB_PROXY_REFRESH_LOCKS

    asyncio.run(asyncio.wait_for(_run(), timeout=5))
