# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.channel_config_rpc_handlers import (
    register_channel_config_handlers,
)


EXPECTED_ROUTES = [
    "health_check.get_conf",
    "health_check.set_conf",
    "heartbeat.get_conf",
    "heartbeat.set_conf",
    "channel.feishu.get_conf",
    "channel.feishu.set_conf",
    "channel.xiaoyi.get_conf",
    "channel.xiaoyi.set_conf",
    "channel.telegram.get_conf",
    "channel.telegram.set_conf",
    "channel.dingtalk.get_conf",
    "channel.dingtalk.set_conf",
    "channel.whatsapp.get_conf",
    "channel.whatsapp.set_conf",
    "channel.discord.get_conf",
    "channel.discord.set_conf",
    "channel.slack.get_conf",
    "channel.slack.set_conf",
    "channel.wecom.get_conf",
    "channel.wecom.set_conf",
    "channel.wechat.get_conf",
    "channel.wechat.set_conf",
    "channel.wechat.get_login_ui",
    "channel.wechat.unbind",
]


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


class _FakeChannelManager:
    def __init__(self, confs=None):
        self._confs = confs or {}
        self.set_conf_calls = []
        self.restart_pending_calls = []

    def get_conf(self, name):
        return self._confs.get(name, {})

    async def set_conf(self, name, conf):
        self.set_conf_calls.append((name, conf))
        self._confs[name] = conf

    def mark_channel_restart_pending(self, name):
        self.restart_pending_calls.append(name)


class _FakeHeartbeatService:
    def __init__(self):
        self.set_conf_calls = []

    def get_health_check_conf(self):
        return {"every": 60, "target": None, "active_hours": None}

    async def set_health_check_conf(self, every=None, target=None, active_hours=None):
        self.set_conf_calls.append(
            {"every": every, "target": target, "active_hours": active_hours}
        )


def _register(channel_manager=None, heartbeat_service=None):
    channel = _FakeChannel()
    register_channel_config_handlers(
        channel,
        channel_manager,
        heartbeat_service,
        object(),
        _resolve=lambda ref: ref,
        _schedule_clear_agent_config_cache=lambda label: None,
        _clear_agent_config_cache=lambda client: None,
    )
    return channel


def test_channel_config_routes_are_registered_in_original_order():
    channel = _register(_FakeChannelManager())

    assert list(channel.methods) == EXPECTED_ROUTES


@pytest.mark.asyncio
async def test_health_check_get_conf_without_service_is_unavailable():
    channel = _register(heartbeat_service=None)

    await channel.methods["health_check.get_conf"](
        object(), "rpc-1", {}, "session-1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": False,
            "error": "health_check service not available",
            "code": "SERVICE_UNAVAILABLE",
        }
    ]


@pytest.mark.asyncio
async def test_heartbeat_set_conf_requires_object_params():
    channel = _register(
        _FakeChannelManager(),
        heartbeat_service=_FakeHeartbeatService(),
    )

    await channel.methods["heartbeat.set_conf"](
        object(), "rpc-2", [], "session-2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "params must be object",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_feishu_get_conf_without_channel_manager_is_unavailable():
    channel = _register(channel_manager=None)

    await channel.methods["channel.feishu.get_conf"](
        object(), "rpc-3", {}, "session-3"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": False,
            "error": "channel manager not available",
            "code": "SERVICE_UNAVAILABLE",
        }
    ]


@pytest.mark.asyncio
async def test_health_check_set_conf_rejects_missing_feishu_target(monkeypatch):
    from agent_gateway.channel_manager.web import channel_config_rpc_handlers

    heartbeat = _FakeHeartbeatService()
    channel = _register(
        _FakeChannelManager(),
        heartbeat_service=heartbeat,
    )
    monkeypatch.setattr(
        channel_config_rpc_handlers,
        "get_config_raw",
        lambda: {
            "channels": {
                "feishu": {
                    "apps": [{"app_id": "app-1"}],
                },
            },
        },
    )

    await channel.methods["health_check.set_conf"](
        object(),
        "rpc-4",
        {"target": "feishu"},
        "session-4",
    )

    assert channel.responses == [
        {
            "req_id": "rpc-4",
            "ok": False,
            "error": "feishuNoTarget",
            "code": "feishuNoTarget",
        }
    ]
    assert heartbeat.set_conf_calls == []


@pytest.mark.asyncio
async def test_feishu_set_conf_merges_by_app_id_and_keeps_old_sensitive_fields(monkeypatch):
    from agent_gateway.channel_manager.web import channel_config_rpc_handlers

    manager = _FakeChannelManager(
        {"feishu": {"apps": [{"app_id": "app-1", "app_secret": "old-secret"}]}}
    )
    persisted = []
    monkeypatch.setattr(
        channel_config_rpc_handlers,
        "replace_channel_subsection_with_cleanup",
        lambda channel, key, apps, allowed: persisted.append((channel, key, apps, allowed)),
    )
    channel = _register(manager)

    await channel.methods["channel.feishu.set_conf"](
        object(),
        "rpc-5",
        {"apps": [{"app_id": "app-1", "name": "new-name"}]},
        "session-5",
    )

    merged_app = manager.set_conf_calls[0][1]["apps"][0]

    assert merged_app["app_id"] == "app-1"
    assert merged_app["app_secret"] == "old-secret"
    assert merged_app["name"] == "new-name"
    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": True,
            "payload": {"config": {"apps": [merged_app]}},
        }
    ]


@pytest.mark.asyncio
async def test_wechat_set_conf_rejects_invalid_numeric_value_before_set_conf():
    manager = _FakeChannelManager()
    channel = _register(manager)

    await channel.methods["channel.wechat.set_conf"](
        object(),
        "rpc-6",
        {"long_poll_timeout_sec": 0.5},
        "session-6",
    )

    assert channel.responses == [
        {
            "req_id": "rpc-6",
            "ok": False,
            "error": "long_poll_timeout_sec 需为整数",
            "code": "BAD_REQUEST",
        }
    ]
    assert manager.set_conf_calls == []


@pytest.mark.asyncio
async def test_wechat_get_login_ui_converts_updated_at_to_int(monkeypatch):
    from agent_gateway.channel_manager.im_platforms.wechat import wechat_connect
    from agent_gateway.channel_manager.web import channel_config_rpc_handlers

    async def snapshot():
        return {"updated_at": 123.7}

    monkeypatch.setattr(wechat_connect, "snapshot_wechat_login_ui_state", snapshot)
    channel = _register(_FakeChannelManager())

    await channel.methods["channel.wechat.get_login_ui"](
        object(), "rpc-7", {}, "session-7"
    )

    assert channel.responses == [
        {"req_id": "rpc-7", "ok": True, "payload": {"updated_at": 123}}
    ]


@pytest.mark.asyncio
async def test_wechat_unbind_marks_restart_pending(monkeypatch):
    from agent_gateway.channel_manager.im_platforms.wechat import wechat_connect
    from agent_gateway.channel_manager.web import channel_config_rpc_handlers

    cleared = []

    def clear_bound_session(conf):
        cleared.append(conf)
        return {"bot_token": None}

    async def reset_login_ui_state():
        return None

    monkeypatch.setattr(wechat_connect, "clear_wechat_bound_session", clear_bound_session)
    monkeypatch.setattr(wechat_connect, "reset_wechat_login_ui_state", reset_login_ui_state)
    manager = _FakeChannelManager({"wechat": {"bot_token": "old-token"}})
    channel = _register(manager)

    await channel.methods["channel.wechat.unbind"](
        object(), "rpc-8", {}, "session-8"
    )

    assert cleared == [{"bot_token": "old-token"}]
    assert manager.restart_pending_calls == ["wechat"]
    assert channel.responses == [
        {
            "req_id": "rpc-8",
            "ok": True,
            "payload": {"config": {"bot_token": None}},
        }
    ]
