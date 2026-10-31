# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

import pytest

from agent_gateway.agent_prewarm import AgentPrewarmCoordinator


class _FakeClient:
    def __init__(self, *, reject: bool = False):
        self.reject = reject
        self.requests = []
        self.response = None

    async def send_request(self, env):
        self.requests.append(env)
        if self.reject:
            raise RuntimeError("agent server unavailable")
        return self.response


class _FakeChannelManager:
    enabled_channels = {"Web", "acp", "A2A"}


async def test_agent_prewarm_sync_sends_sorted_enabled_channels(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.agent_prewarm.prewarm_enabled_by_env", lambda: True
    )
    client = _FakeClient()
    client.response = SimpleNamespace(ok=True)
    coordinator = AgentPrewarmCoordinator(
        client=client,
        channel_manager=_FakeChannelManager(),
        using_adx_router=False,
    )

    await coordinator.sync_agent_prewarm_channels()

    assert len(client.requests) == 1
    assert client.requests[0].params == {"enabled_channels": ["Web"]}


async def test_agent_prewarm_sync_is_disabled_for_adx_router(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.agent_prewarm.prewarm_enabled_by_env", lambda: True
    )
    client = _FakeClient()
    coordinator = AgentPrewarmCoordinator(
        client=client,
        channel_manager=_FakeChannelManager(),
        using_adx_router=True,
    )

    await coordinator.sync_agent_prewarm_channels()
    coordinator.schedule_agent_prewarm_sync("agent-prewarm-sync")

    assert client.requests == []
    assert coordinator.debounce_task is None


async def test_agent_prewarm_sync_failure_is_non_fatal(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.agent_prewarm.prewarm_enabled_by_env", lambda: True
    )
    client = _FakeClient(reject=True)
    coordinator = AgentPrewarmCoordinator(
        client=client,
        channel_manager=_FakeChannelManager(),
        using_adx_router=False,
    )

    await coordinator.sync_agent_prewarm_channels()

    assert len(client.requests) == 1


async def test_agent_prewarm_stop_debounce_awaits_cancelled_task(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.agent_prewarm.prewarm_enabled_by_env", lambda: True
    )
    coordinator = AgentPrewarmCoordinator(
        client=_FakeClient(),
        channel_manager=_FakeChannelManager(),
        using_adx_router=False,
    )
    coordinator.schedule_agent_prewarm_sync(
        "agent-prewarm-sync", delay_seconds=0.01
    )
    task = coordinator.debounce_task
    assert task is not None

    await coordinator.stop_debounce()

    assert task.cancelled()
