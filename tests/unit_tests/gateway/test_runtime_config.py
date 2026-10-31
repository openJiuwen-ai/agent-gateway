# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import agent_gateway.common.config as config_module

from agent_gateway.runtime_config import (
    load_gateway_runtime_config,
    resolve_health_check_config,
)


class FakeMessageHandler:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.saved = []

    def update_evolution_auto_save(self, config):
        if self.error is not None:
            raise self.error
        self.saved.append(config)


async def test_load_gateway_runtime_config_prefers_health_check_and_migrates_legacy(
    monkeypatch,
):
    full_cfg = {
        "health_check": {"every": 30},
        "heartbeat": {"every": 60},
        "channels": {"web": {}},
    }

    def migrate():
        return True

    def get_config():
        return dict(full_cfg)

    monkeypatch.setattr(
        config_module, "migrate_legacy_heartbeat_probe_config", migrate
    )
    monkeypatch.setattr(config_module, "get_config", get_config)
    message_handler = FakeMessageHandler()

    loaded = await load_gateway_runtime_config(message_handler)

    assert loaded == (
        {"health_check": {"every": 30}, "heartbeat": {"every": 60}, "channels": {"web": {}}},
        {"every": 30},
        {"web": {}},
    )
    assert message_handler.saved == [{"health_check": {"every": 30}, "heartbeat": {"every": 60}, "channels": {"web": {}}}]


async def test_load_gateway_runtime_config_resolves_legacy_health_check(
    monkeypatch,
):
    full_cfg = {"heartbeat": {"every": 30, "target": "https://example.com", "active_hours": [9, 18]}}

    def migrate():
        return False

    def get_config():
        return dict(full_cfg)

    monkeypatch.setattr(
        config_module, "migrate_legacy_heartbeat_probe_config", migrate
    )
    monkeypatch.setattr(config_module, "get_config", get_config)
    message_handler = FakeMessageHandler()

    loaded = await load_gateway_runtime_config(message_handler)

    assert loaded == (
        {"heartbeat": {"every": 30, "target": "https://example.com", "active_hours": [9, 18]}},
        {"every": 30, "target": "https://example.com", "active_hours": [9, 18]},
        None,
    )
    assert message_handler.saved == [{"heartbeat": {"every": 30, "target": "https://example.com", "active_hours": [9, 18]}}]


async def test_load_gateway_runtime_config_read_failure_uses_defaults(monkeypatch):
    def migrate():
        return False

    def get_config():
        raise RuntimeError("bad yaml")

    monkeypatch.setattr(
        config_module, "migrate_legacy_heartbeat_probe_config", migrate
    )
    monkeypatch.setattr(config_module, "get_config", get_config)
    message_handler = FakeMessageHandler()

    loaded = await load_gateway_runtime_config(message_handler)

    assert loaded == ({}, None, None)
    assert message_handler.saved == []


async def test_load_gateway_runtime_config_evolution_failure_is_non_fatal(monkeypatch):
    full_cfg = {"channels": {"web": {}}}

    def migrate():
        return False

    def get_config():
        return dict(full_cfg)

    monkeypatch.setattr(
        config_module, "migrate_legacy_heartbeat_probe_config", migrate
    )
    monkeypatch.setattr(config_module, "get_config", get_config)
    message_handler = FakeMessageHandler(error=RuntimeError("bad evolution config"))

    loaded = await load_gateway_runtime_config(message_handler)

    assert loaded == ({"channels": {"web": {}}}, None, {"web": {}})
    assert message_handler.saved == []


def test_resolve_health_check_config_prefers_current_config():
    full_cfg = {
        "health_check": {"every": 30},
        "heartbeat": {"every": 60},
    }

    assert resolve_health_check_config(full_cfg) == {"every": 30}
