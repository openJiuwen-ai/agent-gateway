# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.optional_channel_configurator import OptionalChannelConfigurator


class _FakeChannelManager:
    def __init__(self):
        self._restart_pending = set()

    def pop_channel_restart_pending(self):
        pending = self._restart_pending
        self._restart_pending = set()
        return pending

    def force_restart(self, channel_names):
        self._restart_pending.update(channel_names)

    def pop_channels_by_id(self, channel_id):
        return []

    def register_channel(self, channel):
        return None


class _FakeIMPipeline:
    def __init__(self):
        self.unregistered = []

    def unregister_adapter(self, channel_id):
        self.unregistered.append(channel_id)

    def register_adapter(self, channel_id, adapter):
        return None


def _configurator(channel_manager, im_inbound, im_outbound):
    return OptionalChannelConfigurator(
        channel_manager=channel_manager,
        web_channel=object(),
        agent_server_ext=object(),
        im_inbound=im_inbound,
        im_outbound=im_outbound,
        dummy_bus_cls=object,
        get_config=lambda: {},
        schedule_agent_prewarm_sync=lambda *args, **kwargs: None,
        web_proxy_config_cls=object,
        web_port_manager_cls=object,
    )


@pytest.mark.asyncio
async def test_apply_unchanged_channels_are_not_restarted():
    channel_manager = _FakeChannelManager()
    im_inbound = _FakeIMPipeline()
    im_outbound = _FakeIMPipeline()
    configurator = _configurator(channel_manager, im_inbound, im_outbound)

    await configurator.apply({"feishu": {}})
    assert im_inbound.unregistered == ["feishu"]

    im_inbound.unregistered.clear()
    await configurator.apply({"feishu": {}})
    assert im_inbound.unregistered == []


@pytest.mark.asyncio
async def test_restart_pending_forces_restart_without_config_change():
    channel_manager = _FakeChannelManager()
    im_inbound = _FakeIMPipeline()
    im_outbound = _FakeIMPipeline()
    configurator = _configurator(channel_manager, im_inbound, im_outbound)

    await configurator.apply({"feishu": {}})
    im_inbound.unregistered.clear()
    channel_manager.force_restart(["feishu"])
    await configurator.apply({"feishu": {}})
    assert im_inbound.unregistered == ["feishu"]
