# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Cached iLink context tokens must be released at binding/session boundaries."""

from __future__ import annotations

import json

import pytest

from agent_gateway.channel_manager.im_platforms.wechat import wechat_connect
from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
    WechatChannel,
    WechatConfig,
)
from agent_gateway.channel_manager.web import app_web_handlers


def _channel(**overrides) -> WechatChannel:
    return WechatChannel(WechatConfig(**overrides), None)


def test_release_context_tokens_clears_all_or_one_user() -> None:
    ch = _channel()
    ch._context_tokens["user-a"] = "token-a"
    ch._context_tokens["user-b"] = "token-b"

    ch.release_context_tokens("user-a")
    assert ch._context_tokens == {"user-b": "token-b"}

    ch.release_context_tokens()
    assert ch._context_tokens == {}


@pytest.mark.asyncio
async def test_stop_releases_context_tokens() -> None:
    ch = _channel()
    ch._context_tokens["user-a"] = "token-a"

    await ch.stop()

    assert ch._context_tokens == {}


@pytest.mark.asyncio
async def test_qrcode_login_success_releases_stale_context_tokens(monkeypatch) -> None:
    ch = _channel(qrcode_poll_interval_sec=0.0)
    ch._http = object()
    ch._context_tokens["user-a"] = "token-a"
    calls: list[dict] = []

    async def fake_push(**kwargs):
        calls.append(kwargs)

    async def fake_qrcode():
        return {"qrcode": "qr-1", "qrcode_img_content": "payload"}

    async def fake_status(_qrcode):
        return {
            "status": "confirmed",
            "bot_token": "new-token",
            "ilink_bot_id": "bot-1",
            "ilink_user_id": "user-b",
        }

    monkeypatch.setattr(wechat_connect, "push_wechat_login_ui", fake_push)
    monkeypatch.setattr(ch, "_get_qrcode", fake_qrcode)
    monkeypatch.setattr(ch, "_get_qrcode_status", fake_status)
    monkeypatch.setattr(ch, "_save_credentials", lambda: None)

    await ch._login_by_qrcode()

    assert ch.config.bot_token == "new-token"
    assert ch._context_tokens == {}
    assert calls[-1]["phase"] == "success"


@pytest.mark.asyncio
async def test_local_credential_load_releases_stale_context_tokens(
    tmp_path, monkeypatch
) -> None:
    cred_path = tmp_path / "credentials.json"
    cred_path.write_text(
        json.dumps({"botToken": "local-token", "baseUrl": "https://example.test"}),
        encoding="utf-8",
    )
    ch = _channel(credential_file=str(cred_path))
    ch._context_tokens["user-a"] = "token-a"
    calls: list[dict] = []

    async def fake_push(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(wechat_connect, "push_wechat_login_ui", fake_push)

    await ch._load_or_login_credentials()

    assert ch.config.bot_token == "local-token"
    assert ch._context_tokens == {}
    assert calls[-1]["credentials_source"] == "local_file"


@pytest.mark.asyncio
async def test_wechat_unbind_releases_channel_context_tokens(monkeypatch) -> None:
    class FakeChannel:
        channel_id = "web"

        def __init__(self):
            self.responses: list[dict] = []
            self.methods: dict[str, object] = {}

        def register_method(self, name, handler, *, local_only=False):
            self.methods[name] = handler

        def on_connect(self, handler):
            return None

        async def send(self, msg):
            return None

        async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
            self.responses.append({"ok": ok, "payload": payload, "error": error})

    class FakeWechat:
        channel_id = "wechat"

        def __init__(self):
            self.released = 0

        def release_context_tokens(self):
            self.released += 1

    class FakeChannelManager:
        def __init__(self):
            self.wechat = FakeWechat()
            self.conf = {"bot_token": "old-token"}

        def get_conf(self, channel_id):
            return dict(self.conf)

        async def set_conf(self, channel_id, conf):
            self.conf = dict(conf)

        def mark_channel_restart_pending(self, channel_id):
            return None

        def get_channel(self, channel_id):
            return self.wechat if channel_id == "wechat" else None

    fake_channel = FakeChannel()
    cm = FakeChannelManager()
    monkeypatch.setattr(app_web_handlers, "update_channel_in_config", lambda *a, **k: None)
    app_web_handlers._register_web_handlers(
        app_web_handlers.WebHandlersBindParams(
            channel=fake_channel, channel_manager=cm
        )
    )

    handler = fake_channel.methods["channel.wechat.unbind"]
    await handler(None, "req-1", {}, "session-1")

    assert cm.wechat.released == 1
    assert fake_channel.responses[-1]["ok"] is True
    assert fake_channel.responses[-1]["payload"]["config"]["bot_token"] == ""
