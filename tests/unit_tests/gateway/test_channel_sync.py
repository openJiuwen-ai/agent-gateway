# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.channel_sync import (
    merge_channel_snapshots,
    schedule_channel_snapshot,
    select_channel_snapshot,
)
from agent_gateway.extensions.agentos.agentos_router.agent_manager import BUILTIN_AGENT_TYPE
from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSRouterClient


def test_select_channel_snapshot_keeps_as_channels_and_minimal_other_channels() -> None:
    snapshot = select_channel_snapshot({
        "feishu": {"apps": [{"app_id": "a"}], "send_file_allowed": True},
        "telegram": {"bot_token": "secret-token", "send_file_allowed": False},
        "discord": {"bot_token": "drop-me"},
    })

    assert snapshot == {
        "feishu": {"apps": [{"app_id": "a"}], "send_file_allowed": True},
        "telegram": {"send_file_allowed": False},
    }


@pytest.mark.asyncio
async def test_schedule_channel_snapshot_runs_in_background() -> None:
    called = asyncio.Event()

    class _Client:
        def __init__(self) -> None:
            self.channels = None

        async def sync_channel_config(self, channels):
            self.channels = channels
            called.set()
            return True

    client = _Client()
    schedule_channel_snapshot(client, {"feishu": {"apps": []}})

    await asyncio.wait_for(called.wait(), timeout=1.0)
    assert client.channels == {"feishu": {"apps": []}}


@pytest.mark.asyncio
async def test_schedule_channel_snapshot_coalesces_to_latest() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _SlowClient:
        def __init__(self) -> None:
            self.calls = []
            self.second = asyncio.Event()

        async def sync_channel_config(self, channels):
            self.calls.append(channels)
            if len(self.calls) == 1:
                started.set()
                await release.wait()
            else:
                self.second.set()
            return True

    client = _SlowClient()
    schedule_channel_snapshot(client, {"feishu": {"apps": [{"app_id": "v1"}]}})
    await asyncio.wait_for(started.wait(), timeout=1.0)
    schedule_channel_snapshot(client, {"feishu": {"apps": [{"app_id": "v2"}]}})

    assert len(client.calls) == 1
    release.set()
    await asyncio.wait_for(client.second.wait(), timeout=1.0)
    assert client.calls[-1] == {"feishu": {"apps": [{"app_id": "v2"}]}}


def test_merge_channel_snapshots_uses_later_sections() -> None:
    merged = merge_channel_snapshots(
        {"feishu": {"apps": [{"app_id": "old"}]}},
        {"feishu": {"apps": []}},
        {"telegram": {"bot_token": "drop-me", "send_file_allowed": True}},
    )

    assert merged == {
        "feishu": {"apps": []},
        "telegram": {"send_file_allowed": True},
    }


class _Runtime:
    def __init__(self, sandbox_id: str = "sandbox-1") -> None:
        self.info = SimpleNamespace(
            agent_type=BUILTIN_AGENT_TYPE,
            sandbox_id=sandbox_id,
            user_id="user-1",
        )
        self.envelope = None

    def attach_to_envelope(self, envelope) -> None:
        self.envelope = envelope

    def is_ready(self) -> bool:
        return True


class _Manager:
    def __init__(self, runtimes) -> None:
        self.runtimes = runtimes

    async def list_all_agents(self):
        return self.runtimes


class _Client:
    def __init__(self, *, ok: bool = True, error: Exception | None = None) -> None:
        self.ok = ok
        self.error = error
        self.envelopes = []

    async def send_request(self, envelope, *, timeout):
        _ = timeout
        self.envelopes.append(envelope)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(ok=self.ok)


def _router() -> AgentOSRouterClient:
    router = object.__new__(AgentOSRouterClient)
    router._server_config = {
        "channels": {
            "feishu": {"apps": [{"app_id": "old"}]},
            "telegram": {"bot_token": "drop-me", "send_file_allowed": False},
            "discord": {"bot_token": "drop-me"},
        }
    }
    router._last_channel_snapshot = {"feishu": {"apps": []}}
    router._channel_synced_sandboxes = set()
    router._channel_sync_failures = {}
    router._channel_sync_generation = 0
    return router


@pytest.mark.asyncio
async def test_initial_sync_uses_merged_filtered_snapshot() -> None:
    router = _router()
    client = _Client()

    await router._sync_channel_config_for_runtime(_Runtime(), client)

    assert client.envelopes[0].params == {
        "channels": {
            "feishu": {"apps": []},
            "telegram": {"send_file_allowed": False},
        }
    }
    assert router._channel_synced_sandboxes == {"sandbox-1"}


@pytest.mark.asyncio
async def test_initial_sync_stops_after_repeated_failures() -> None:
    router = _router()
    client = _Client(error=TimeoutError())

    for _ in range(4):
        await router._sync_channel_config_for_runtime(_Runtime(), client)

    assert len(client.envelopes) == 3
    assert router._channel_sync_failures["sandbox-1"] == 3


@pytest.mark.asyncio
async def test_fanout_partial_failure_keeps_last_snapshot() -> None:
    router = _router()
    router._last_channel_snapshot = {"feishu": {"apps": [{"app_id": "old"}]}}
    ok_client = _Client()
    reject_client = _Client(ok=False)
    clients = {"sandbox-ok": ok_client, "sandbox-bad": reject_client}
    router._agent_manager = _Manager([_Runtime("sandbox-ok"), _Runtime("sandbox-bad")])

    async def get_client(runtime):
        return clients[runtime.info.sandbox_id]

    router._get_ws_client = get_client

    accepted = await router.sync_channel_config({"feishu": {"apps": []}})

    assert accepted is False
    assert router._last_channel_snapshot == {
        "feishu": {"apps": [{"app_id": "old"}]}
    }


@pytest.mark.asyncio
async def test_fanout_does_not_mark_sandbox_when_config_changed_mid_flight() -> None:
    router = _router()
    started = asyncio.Event()
    release = asyncio.Event()

    class _SlowClient(_Client):
        async def send_request(self, envelope, *, timeout):
            _ = timeout
            self.envelopes.append(envelope)
            started.set()
            await release.wait()
            return SimpleNamespace(ok=True)

    client = _SlowClient()
    router._agent_manager = _Manager([_Runtime("sandbox-1")])

    async def get_client(runtime):
        return client

    router._get_ws_client = get_client
    task = asyncio.create_task(
        router.sync_channel_config({"feishu": {"apps": []}})
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    router._channel_sync_generation += 1
    release.set()

    assert await asyncio.wait_for(task, timeout=1.0) is False
    assert router._channel_synced_sandboxes == set()
