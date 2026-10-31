# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.updater_rpc_handlers import (
    register_updater_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


@pytest.mark.asyncio
async def test_updater_routes_are_registered():
    channel = _FakeChannel()
    register_updater_handlers(channel)
    assert {
        "updater.get_status",
        "updater.check",
        "updater.download",
        "updater.upgrade",
        "updater.get_conf",
        "updater.reset_source",
        "updater.set_conf",
    } <= set(channel.methods)


@pytest.mark.asyncio
async def test_updater_check_accepts_manual_parameter(monkeypatch):
    checks = []

    def check(manual):
        checks.append(manual)
        return {"manual": manual}

    service = SimpleNamespace(check=check)
    channel = _FakeChannel()
    register_updater_handlers(channel, service)

    await channel.methods["updater.check"](
        object(), "rpc-1", {"manual": True}, "session-1"
    )

    assert checks == [True]
    assert channel.responses == [
        {"req_id": "rpc-1", "ok": True, "payload": {"manual": True}}
    ]


@pytest.mark.asyncio
async def test_updater_reset_source_persists_defaults(monkeypatch):
    from agent_gateway.common.updater import DEFAULT_SOURCE_CONFIG
    from agent_gateway.channel_manager.web import updater_rpc_handlers

    saved = []

    def save_updates(updates):
        saved.append(updates)

    monkeypatch.setattr(updater_rpc_handlers, "update_updater_in_config", save_updates)
    service = SimpleNamespace(get_runtime_config=lambda: {"timeout_seconds": 20})
    channel = _FakeChannel()
    register_updater_handlers(channel, service)

    await channel.methods["updater.reset_source"](
        object(), "rpc-2", {}, "session-2"
    )

    assert saved == [DEFAULT_SOURCE_CONFIG]
    assert channel.responses == [
        {"req_id": "rpc-2", "ok": True, "payload": {"timeout_seconds": 20}}
    ]


@pytest.mark.asyncio
async def test_updater_set_conf_rejects_invalid_timeout(monkeypatch):
    from agent_gateway.channel_manager.web import updater_rpc_handlers

    saved = []
    monkeypatch.setattr(
        updater_rpc_handlers,
        "update_updater_in_config",
        lambda updates: saved.append(updates),
    )
    channel = _FakeChannel()
    register_updater_handlers(channel)

    await channel.methods["updater.set_conf"](
        object(), "rpc-3", {"timeout_seconds": "bad"}, "session-3"
    )

    assert saved == []
    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": False,
            "error": "timeout_seconds must be integer",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_updater_set_conf_normalizes_values(monkeypatch):
    from agent_gateway.channel_manager.web import updater_rpc_handlers

    saved = []
    monkeypatch.setattr(
        updater_rpc_handlers,
        "update_updater_in_config",
        lambda updates: saved.append(updates),
    )
    service = SimpleNamespace(get_runtime_config=lambda: {"enabled": True})
    channel = _FakeChannel()
    register_updater_handlers(channel, service)

    await channel.methods["updater.set_conf"](
        object(),
        "rpc-4",
        {
            "enabled": True,
            "repo_owner": " owner ",
            "timeout_seconds": "10",
        },
        "session-4",
    )

    assert saved == [
        {"enabled": True, "repo_owner": "owner", "timeout_seconds": 10}
    ]
    assert channel.responses == [
        {"req_id": "rpc-4", "ok": True, "payload": {"enabled": True}}
    ]
