# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""config_poll 调度器的启动/停止接线。

验证企业版存储装配完成后调度器被真正拉起、关闭时被正确停止；
以及从未启动过的调度器 ``stop()`` 是幂等安全的。
"""

from __future__ import annotations

import pytest

import agent_gateway.config_poll.scheduler as scheduler_mod
from agent_gateway.config_poll.scheduler import ConfigPollScheduler


class _FakeCtx:
    """teardown 只用到 ``shutdown()``；这里最小化模拟 StorageContext。"""

    async def shutdown(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _fresh_scheduler_singleton():
    """每个用例从干净单例开始，并兜底清理可能残留的后台任务。"""
    _cancel_any()
    scheduler_mod._scheduler = None
    yield
    _cancel_any()
    scheduler_mod._scheduler = None


def _cancel_any() -> None:
    sched = scheduler_mod._scheduler
    if sched is None:
        return
    sched._running = False
    task = sched._task
    if task is not None and not task.done():
        task.cancel()


@pytest.mark.asyncio
async def test_start_storage_starts_poll_and_teardown_stops_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.storage_assembly import setup

    # 用确定 enabled 的调度器替换单例，避免受 JIUWENSWARM_EDITION 环境影响。
    scheduler_mod._scheduler = ConfigPollScheduler(enabled=True, interval_seconds=0.1)
    sched = scheduler_mod._scheduler

    ctx = _FakeCtx()

    async def _fake_setup_repositories(cfg=None):
        return ctx

    monkeypatch.setattr(setup, "is_enterprise", lambda: True)
    # 固定 overlay 仓库开启，避免受 GATEWAY_STORAGE_REPOSITORIES 环境影响调度器是否启动。
    monkeypatch.setattr(setup, "is_storage_repositories_enabled", lambda cfg: True)
    monkeypatch.setattr(setup, "setup_gateway_storage_repositories", _fake_setup_repositories)
    # 与本测试无关的仓库清理，隔离出去，避免触碰全局 Repository 状态。
    monkeypatch.setattr(setup, "_clear_config_repositories", lambda: None)
    # audit 冷加载由 start_gateway_storage 内部的 try/except 兜底，且与调度器接线无关，
    # 这里不额外 mock，任其命中兜底路径或真实冷加载均不影响断言。

    result = await setup.start_gateway_storage({})
    assert result is ctx
    assert sched._running is True
    assert sched._task is not None

    await setup.teardown_gateway_storage_repositories(ctx)
    assert sched._running is False
    assert sched._task is None


@pytest.mark.asyncio
async def test_start_storage_skips_poll_when_repositories_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.storage_assembly import setup

    # repositories 关闭（运维期望走文件配置）时，即使企业版 + ctx 非空，也不应拉起轮询，
    # 避免共享库中的行覆盖进程日志级别等本地状态。
    scheduler_mod._scheduler = ConfigPollScheduler(enabled=True, interval_seconds=0.1)
    sched = scheduler_mod._scheduler

    ctx = _FakeCtx()

    async def _fake_setup_repositories(cfg=None):
        return ctx

    monkeypatch.setattr(setup, "is_enterprise", lambda: True)
    monkeypatch.setattr(setup, "is_storage_repositories_enabled", lambda cfg: False)
    monkeypatch.setattr(setup, "setup_gateway_storage_repositories", _fake_setup_repositories)
    monkeypatch.setattr(setup, "_clear_config_repositories", lambda: None)

    result = await setup.start_gateway_storage({})
    assert result is ctx
    assert sched._running is False
    assert sched._task is None


@pytest.mark.asyncio
async def test_scheduler_stop_before_start_is_safe() -> None:
    scheduler_mod._scheduler = ConfigPollScheduler(enabled=True, interval_seconds=0.1)
    sched = scheduler_mod._scheduler

    await sched.stop()  # 未 start：task 为 None，直接返回不抛异常

    assert sched._running is False
    assert sched._task is None
