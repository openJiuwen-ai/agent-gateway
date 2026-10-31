# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.app_web_handlers import (
    WebHandlersBindParams,
    _register_web_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []
        self.channel_id = "web"

    def register_method(self, name, handler):
        self.methods[name] = handler

    def on_connect(self, handler):
        self.on_connect_handler = handler

    def on_disconnect(self, handler):
        self.on_disconnect_handler = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


@pytest.fixture
def web_channel():
    channel = _FakeChannel()
    _register_web_handlers(WebHandlersBindParams(channel=channel))
    return channel


async def test_external_cli_routes_are_registered(web_channel):
    assert {
        "external_cli.detect",
        "external_cli.codex_install_status",
        "external_cli.install_status",
        "external_cli.install",
        "external_cli.custom_agent.remove",
    } <= set(web_channel.methods)


async def test_external_cli_detect_requires_params_object(
    web_channel, capsys, monkeypatch
):
    sentinel = object()
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._activate_managed_external_cli_paths_if_needed",
        lambda: pytest.fail("detect should not activate paths for invalid params"),
    )
    await web_channel.methods["external_cli.detect"](
        object(), "rpc-1", sentinel, "session-1"
    )
    assert web_channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": False,
            "error": "params must be object",
            "code": "BAD_REQUEST",
        }
    ]


async def test_external_cli_detect_returns_detection_result(
    web_channel, monkeypatch
):
    activated = []
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._activate_managed_external_cli_paths_if_needed",
        lambda: activated.append(True),
    )
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._detect_external_cli_agent",
        lambda agent, path, use_builtin: {
            "cli_agent": agent,
            "cli_path": path,
            "use_builtin": use_builtin,
        },
    )
    await web_channel.methods["external_cli.detect"](
        object(),
        "rpc-2",
        {"cli_agent": " Codex ", "cli_path": "C:/codex", "use_builtin": "true"},
        "session-2",
    )
    assert activated == [True]
    assert web_channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": True,
            "payload": {
                "cli_agent": "codex",
                "cli_path": "C:/codex",
                "use_builtin": True,
            },
        }
    ]


async def test_external_cli_codex_install_status(web_channel, monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._snapshot_codex_dependency_install_status",
        lambda: {"status": "ready"},
    )
    await web_channel.methods["external_cli.codex_install_status"](
        object(), "rpc-3", {}, "session-3"
    )
    assert web_channel.responses == [
        {"req_id": "rpc-3", "ok": True, "payload": {"status": "ready"}}
    ]


async def test_external_cli_install_status_rejects_unknown_agent(
    web_channel, monkeypatch
):
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._known_external_cli_agents",
        lambda: ("codex", "claude"),
    )
    await web_channel.methods["external_cli.install_status"](
        object(), "rpc-4", {"cli_agent": "other"}, "session-4"
    )
    assert web_channel.responses == [
        {
            "req_id": "rpc-4",
            "ok": False,
            "error": "unsupported external cli agent",
            "code": "BAD_REQUEST",
        }
    ]


async def test_external_cli_install_requires_frozen_runtime(
    web_channel, monkeypatch
):
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._known_external_cli_agents",
        lambda: ("codex",),
    )
    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._is_frozen_runtime",
        lambda: False,
    )
    await web_channel.methods["external_cli.install"](
        object(), "rpc-5", {"cli_agent": "codex"}, "session-5"
    )
    assert web_channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": False,
            "error": "managed external CLI runtimes are installed by the desktop app only",
            "code": "UNSUPPORTED",
        }
    ]


async def test_external_cli_remove_maps_value_error(web_channel, monkeypatch):
    def remove_agent(agent):
        raise ValueError("cannot remove")

    monkeypatch.setattr(
        "agent_gateway.channel_manager.web.app_web_handlers._remove_custom_external_cli_agent",
        remove_agent,
    )
    await web_channel.methods["external_cli.custom_agent.remove"](
        object(), "rpc-6", {"cli_agent": "custom"}, "session-6"
    )
    assert web_channel.responses == [
        {
            "req_id": "rpc-6",
            "ok": False,
            "error": "cannot remove",
            "code": "BAD_REQUEST",
        }
    ]
