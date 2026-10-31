# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.workspace_rpc_handlers import (
    register_workspace_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


def _register(agent_client=None, _resolve=lambda ref: ref):
    channel = _FakeChannel()
    register_workspace_handlers(channel, agent_client, _resolve)
    return channel


def test_workspace_routes_are_registered_in_original_order():
    channel = _register(object())

    assert list(channel.methods) == [
        "hooks.list",
        "commands.list",
        "file.upload_chunk",
    ]


@pytest.mark.asyncio
async def test_hooks_list_reads_local_config_for_legacy_client(monkeypatch):
    from agent_gateway.channel_manager.web import workspace_rpc_handlers

    class config:
        disable_all_hooks = False

        def get_event_summary(self):
            return [{"event": "session.start"}]

    monkeypatch.setattr(workspace_rpc_handlers, "get_config", lambda: {"hooks": {}})
    monkeypatch.setattr(
        workspace_rpc_handlers,
        "load_hooks_config",
        lambda raw_config: config(),
    )
    monkeypatch.setattr(
        workspace_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: True,
    )
    channel = _register(object())

    await channel.methods["hooks.list"](
        object(), "rpc-1", {}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": True,
            "payload": {
                "events": [{"event": "session.start"}],
                "disable_all_hooks": False,
                "source": "config.yaml",
            },
        }
    ]


@pytest.mark.asyncio
async def test_hooks_list_forwards_remote_client(monkeypatch):
    from agent_gateway.channel_manager.web import workspace_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(
        workspace_rpc_handlers,
        "proxy_unary_request",
        proxy_unary_request,
    )
    monkeypatch.setattr(
        workspace_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: False,
    )
    channel = _register(object())

    await channel.methods["hooks.list"](
        object(), "rpc-2", {"ignored": True}, "session-2", user_id="u2"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {}
    assert forwarded[0]["req_method"].value == "hooks.list"
    assert forwarded[0]["label"] == "hooks.list"
    assert channel.responses == []


@pytest.mark.asyncio
async def test_commands_list_returns_local_metadata(monkeypatch):
    from agent_gateway.channel_manager.web import workspace_rpc_handlers

    monkeypatch.setattr(
        workspace_rpc_handlers,
        "list_builtin_commands",
        lambda params: {"commands": []},
    )
    channel = _register(object())

    await channel.methods["commands.list"](
        object(), "rpc-3", {}, "session-3"
    )

    assert channel.responses == [
        {"req_id": "rpc-3", "ok": True, "payload": {"commands": []}}
    ]


@pytest.mark.asyncio
async def test_file_upload_chunk_forwards_with_120_second_timeout(monkeypatch):
    from agent_gateway.channel_manager.web import workspace_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(
        workspace_rpc_handlers,
        "proxy_unary_request",
        proxy_unary_request,
    )
    channel = _register(object())

    await channel.methods["file.upload_chunk"](
        object(), "rpc-4", "bad", "session-4", user_id="u4"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {}
    assert forwarded[0]["req_method"].value == "file.upload_chunk"
    assert forwarded[0]["timeout_seconds"] == 120.0
    assert channel.responses == []
