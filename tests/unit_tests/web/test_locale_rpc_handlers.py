# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.locale_rpc_handlers import (
    register_locale_handlers,
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
async def test_locale_routes_are_registered():
    channel = _FakeChannel()
    register_locale_handlers(channel, agent_client=None, _resolve=lambda ref: ref)
    assert {
        "locale.get_conf",
        "locale.set_conf",
    } <= set(channel.methods)


@pytest.mark.asyncio
async def test_locale_get_conf_reads_local_config(monkeypatch):
    from agent_gateway.channel_manager.web import locale_rpc_handlers

    monkeypatch.setattr(
        locale_rpc_handlers,
        "get_config",
        lambda: {"preferred_language": " EN "},
    )
    channel = _FakeChannel()
    register_locale_handlers(channel, agent_client=None, _resolve=lambda ref: ref)

    await channel.methods["locale.get_conf"](
        object(), "rpc-1", {}, "session-1"
    )

    assert channel.responses == [
        {"req_id": "rpc-1", "ok": True, "payload": {"preferred_language": "en"}}
    ]


@pytest.mark.asyncio
async def test_locale_set_conf_rejects_invalid_language():
    channel = _FakeChannel()
    register_locale_handlers(channel, agent_client=None, _resolve=lambda ref: ref)

    await channel.methods["locale.set_conf"](
        object(), "rpc-2", {"preferred_language": "fr"}, "session-2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "preferred_language must be zh or en",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_locale_set_conf_uses_local_config_for_legacy_client(monkeypatch):
    from agent_gateway.channel_manager.web import locale_rpc_handlers

    saved = []
    monkeypatch.setattr(
        locale_rpc_handlers,
        "update_preferred_language_in_config",
        lambda lang: saved.append(lang),
    )
    monkeypatch.setattr(
        locale_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: True,
    )
    channel = _FakeChannel()
    register_locale_handlers(channel, agent_client=object(), _resolve=lambda ref: ref)

    await channel.methods["locale.set_conf"](
        object(), "rpc-3", {"preferred_language": "EN"}, "session-3"
    )

    assert saved == ["en"]
    assert channel.responses == [
        {"req_id": "rpc-3", "ok": True, "payload": {"preferred_language": "en"}}
    ]


@pytest.mark.asyncio
async def test_locale_set_conf_forwards_remote_client(monkeypatch):
    from agent_gateway.channel_manager.web import locale_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(
        locale_rpc_handlers,
        "proxy_unary_request",
        proxy_unary_request,
    )
    monkeypatch.setattr(
        locale_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: False,
    )
    channel = _FakeChannel()
    register_locale_handlers(channel, agent_client=object(), _resolve=lambda ref: ref)

    await channel.methods["locale.set_conf"](
        object(), "rpc-4", {"preferred_language": "en"}, "session-4"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {"preferred_language": "en"}
    assert forwarded[0]["req_method"].value == "locale.set_conf"
    assert channel.responses == []
