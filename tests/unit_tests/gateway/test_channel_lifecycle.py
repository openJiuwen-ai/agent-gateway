# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio

from agent_gateway.channel_lifecycle import (
    _is_channel_enabled,
    _should_restart_channel,
    _stop_channel,
)


def test_should_restart_channel_compares_presence_and_value():
    assert _should_restart_channel("feishu", {}, {"feishu": {}})
    assert _should_restart_channel("feishu", {"feishu": {}}, {})
    assert _should_restart_channel(
        "feishu", {"feishu": {"enabled": True}}, {"feishu": {"enabled": False}}
    )
    assert not _should_restart_channel(
        "feishu", {"feishu": {"enabled": True}}, {"feishu": {"enabled": True}}
    )


def test_is_channel_enabled_reports_missing_fields():
    assert _is_channel_enabled(None, []) == (False, "missing or invalid config")
    assert _is_channel_enabled({"enabled": False}, []) == (False, "enabled = false")
    assert _is_channel_enabled({"enabled": True}, []) == (True, "")
    assert _is_channel_enabled({"app_id": "x"}, ["app_id", "app_secret"]) == (
        False,
        "missing app_id,app_secret",
    )


async def test_stop_channel_cancels_task_stops_channel_and_unregisters():
    cancel_started = asyncio.Event()

    async def worker():
        cancel_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    task = asyncio.create_task(worker())
    await cancel_started.wait()

    class _Channel:
        channel_id = "feishu-app"
        stopped = False

        async def stop(self):
            self.stopped = True

    channel = _Channel()

    class _Manager:
        unregistered = []

        def unregister_channel(self, channel_id):
            self.unregistered.append(channel_id)

    manager = _Manager()
    await _stop_channel(
        channel,
        task,
        "feishu",
        channel_manager=manager,
    )

    assert task.done()
    assert channel.stopped
    assert manager.unregistered == ["feishu-app"]
