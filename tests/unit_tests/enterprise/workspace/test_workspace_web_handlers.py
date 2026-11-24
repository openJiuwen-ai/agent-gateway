# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""workspace Web RPC：企业身份、preview max_bytes 契约、download E2A 上限。"""

from __future__ import annotations

import base64
import sys
import types
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.requests import Request

from agent_gateway.channel_manager.web.http_routes import workspace as http_ws
from agent_gateway.channel_manager.web.web_method_register import workspace as ws_mod
from agent_gateway.channel_manager.web.web_method_register.workspace import (
    WorkspaceIdentityError,
    _PREVIEW_DEFAULT_MAX_BYTES,
    _PREVIEW_HARD_MAX_BYTES,
    _bind_trusted_identity,
    _identity_for_forward,
    parse_preview_max_bytes,
    register_workspace_web_methods,
)
from agent_gateway.common.protocol_methods import ReqMethod
from agent_gateway.routing.agent_http_bridge import E2A_PAYLOAD_MAX_BYTES


@pytest.fixture
def enterprise(monkeypatch):
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: True)


@pytest.fixture
def personal(monkeypatch):
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: False)


def test_enterprise_missing_trusted_identity_unauthorized(enterprise):
    ws = SimpleNamespace(_web_routing={})
    with pytest.raises(WorkspaceIdentityError) as ei:
        _identity_for_forward(ws, {"user_id": "victim", "group_id": "g", "bot_id": "b"})
    assert ei.value.code == "UNAUTHORIZED"


def test_enterprise_params_only_cannot_select_foreign_tenant(enterprise):
    """无受信头时，即便 body 带齐三元组也不能当作权威身份。"""
    ws = SimpleNamespace(_web_routing=None)
    with pytest.raises(WorkspaceIdentityError) as ei:
        _bind_trusted_identity(
            ws,
            {
                "user_id": "other-user",
                "group_id": "__none__",
                "bot_id": "bot-x",
                "relative_path": ".",
            },
        )
    assert ei.value.code == "UNAUTHORIZED"


@pytest.mark.parametrize(
    "params,field",
    [
        ({"user_id": "victim"}, "user_id"),
        ({"group_id": "other-group"}, "group_id"),
        ({"bot_id": "other-bot"}, "bot_id"),
    ],
)
def test_enterprise_conflict_with_client_params(enterprise, params, field):
    ws = SimpleNamespace(
        _web_routing={
            "user_id": "alice",
            "group_id": "__none__",
            "bot_id": "bot-1",
        }
    )
    with pytest.raises(WorkspaceIdentityError) as ei:
        _identity_for_forward(ws, params, user_id="alice")
    assert ei.value.code == "IDENTITY_CONFLICT"
    assert ei.value.field == field


def test_enterprise_trusted_identity_wins_when_params_match(enterprise):
    ws = SimpleNamespace(
        _web_routing={
            "user_id": "alice",
            "group_id": "__none__",
            "bot_id": "bot-1",
        }
    )
    params = {
        "user_id": "alice",
        "group_id": "__none__",
        "bot_id": "bot-1",
        "relative_path": "a.txt",
    }
    uid, gid, bid = _bind_trusted_identity(ws, params, user_id="alice")
    assert (uid, gid, bid) == ("alice", "__none__", "bot-1")
    assert params["user_id"] == "alice"


def test_personal_may_fallback_to_params(personal):
    ws = SimpleNamespace(_web_routing={})
    uid, gid, bid = _identity_for_forward(
        ws, {"user_id": "local", "group_id": "g", "bot_id": "b"}
    )
    assert (uid, gid, bid) == ("local", "g", "b")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    [
        ReqMethod.WORKSPACE_TREE.value,
        ReqMethod.WORKSPACE_USAGE.value,
        ReqMethod.WORKSPACE_ENTRIES_DELETE.value,
        ReqMethod.WORKSPACE_PREVIEW.value,
        ReqMethod.WORKSPACE_DOWNLOAD.value,
    ],
)
async def test_workspace_handlers_reject_cross_tenant_params(
    enterprise, monkeypatch, method
):
    """跨租户：受信为 alice，body 声称 victim → 不转发 Agent。"""
    channel = MagicMock()
    channel.send_response = AsyncMock()
    registered: dict[str, Any] = {}

    def _register(name, handler):
        registered[name] = handler

    channel.register_method.side_effect = _register
    forward = AsyncMock(return_value=(True, {}, None, None))
    monkeypatch.setattr(ws_mod, "_forward_to_agent", forward)
    monkeypatch.setattr(ws_mod, "_use_local_workspace", lambda: False)
    monkeypatch.setattr(ws_mod, "is_workspace_quota_enabled", lambda: True)

    register_workspace_web_methods(channel, agent_client=lambda: None)
    handler = registered[method]
    ws = SimpleNamespace(
        _web_routing={
            "user_id": "alice",
            "group_id": "__none__",
            "bot_id": "bot-1",
        }
    )
    params = {
        "user_id": "victim",
        "group_id": "__none__",
        "bot_id": "bot-1",
        "relative_path": "secret.txt",
        "relative_paths": ["secret.txt"],
    }
    await handler(ws, "req-1", params, "sess-1", user_id="alice")
    forward.assert_not_awaited()
    channel.send_response.assert_awaited()
    kwargs = channel.send_response.await_args.kwargs
    assert kwargs["ok"] is False
    assert kwargs["code"] == "IDENTITY_CONFLICT"


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, _PREVIEW_DEFAULT_MAX_BYTES),
        ("", _PREVIEW_DEFAULT_MAX_BYTES),
        ("  ", _PREVIEW_DEFAULT_MAX_BYTES),
        (1024, 1024),
        ("2048", 2048),
        (" 4096 ", 4096),
        (_PREVIEW_HARD_MAX_BYTES, _PREVIEW_HARD_MAX_BYTES),
        (_PREVIEW_HARD_MAX_BYTES + 1, _PREVIEW_HARD_MAX_BYTES),
        (10 * 1024 * 1024, _PREVIEW_HARD_MAX_BYTES),
        ("2097152", _PREVIEW_HARD_MAX_BYTES),
    ],
)
def test_parse_preview_max_bytes_ok(raw, expected):
    assert parse_preview_max_bytes(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        True,
        False,
        0,
        -1,
        "-10",
        "abc",
        "1.5",
        "1e3",
        1.5,
        [],
        {},
        object(),
    ],
)
def test_parse_preview_max_bytes_rejects_illegal(raw):
    with pytest.raises(ValueError):
        parse_preview_max_bytes(raw)


@pytest.mark.asyncio
async def test_preview_rejects_illegal_max_bytes(monkeypatch):
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: False)
    monkeypatch.setattr(ws_mod, "_use_local_workspace", lambda: True)

    channel = MagicMock()
    channel.send_response = AsyncMock()
    registered: dict[str, object] = {}
    channel.register_method.side_effect = lambda name, handler: registered.update(
        {name: handler}
    )
    register_workspace_web_methods(channel, agent_client=lambda: None)

    handler = registered[ReqMethod.WORKSPACE_PREVIEW.value]

    await handler(
        SimpleNamespace(_web_routing={}),
        "req-1",
        {"relative_path": "a.txt", "max_bytes": "not-a-number"},
        "sess-1",
        user_id="u1",
    )

    channel.send_response.assert_awaited_once()
    kwargs = channel.send_response.await_args.kwargs
    assert kwargs["ok"] is False
    assert kwargs["code"] == "BAD_REQUEST"
    assert "max_bytes" in kwargs["error"]


@pytest.mark.asyncio
async def test_preview_local_applies_default_and_hard_cap(monkeypatch):
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: False)
    monkeypatch.setattr(ws_mod, "_use_local_workspace", lambda: True)

    seen: dict[str, int | None] = {}

    def _fake_local(_params, _rel, max_bytes):
        seen["max_bytes"] = max_bytes
        return {"relative_path": "a.txt", "content": "x", "truncated": False}

    monkeypatch.setattr(ws_mod, "_local_preview_file", _fake_local)

    channel = MagicMock()
    channel.send_response = AsyncMock()
    registered: dict[str, object] = {}
    channel.register_method.side_effect = lambda name, handler: registered.update(
        {name: handler}
    )
    register_workspace_web_methods(channel, agent_client=lambda: None)

    handler = registered[ReqMethod.WORKSPACE_PREVIEW.value]

    await handler(
        SimpleNamespace(_web_routing={}),
        "req-default",
        {"relative_path": "a.txt"},
        "sess-1",
        user_id="u1",
    )
    assert seen["max_bytes"] == _PREVIEW_DEFAULT_MAX_BYTES

    await handler(
        SimpleNamespace(_web_routing={}),
        "req-cap",
        {"relative_path": "a.txt", "max_bytes": _PREVIEW_HARD_MAX_BYTES + 4096},
        "sess-1",
        user_id="u1",
    )
    assert seen["max_bytes"] == _PREVIEW_HARD_MAX_BYTES

    # 恰好 1MiB 不截断到更小
    await handler(
        SimpleNamespace(_web_routing={}),
        "req-exact",
        {"relative_path": "a.txt", "max_bytes": _PREVIEW_HARD_MAX_BYTES},
        "sess-1",
        user_id="u1",
    )
    assert seen["max_bytes"] == _PREVIEW_HARD_MAX_BYTES


@pytest.mark.asyncio
async def test_preview_forward_sends_normalized_max_bytes(monkeypatch):
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: True)
    monkeypatch.setattr(ws_mod, "_use_local_workspace", lambda: False)

    forwarded: dict[str, object] = {}

    async def _fake_forward(fwd):
        forwarded["params"] = dict(fwd.params)
        return True, {"relative_path": "a.txt", "content": "ok"}, None, None

    monkeypatch.setattr(ws_mod, "_forward_to_agent", _fake_forward)

    channel = MagicMock()
    channel.send_response = AsyncMock()
    registered: dict[str, object] = {}
    channel.register_method.side_effect = lambda name, handler: registered.update(
        {name: handler}
    )
    register_workspace_web_methods(channel, agent_client=lambda: None)

    handler = registered[ReqMethod.WORKSPACE_PREVIEW.value]
    ws = SimpleNamespace(
        _web_routing={"user_id": "alice", "group_id": "__none__", "bot_id": "bot-1"}
    )
    await handler(
        ws,
        "req-fwd",
        {
            "relative_path": "a.txt",
            "max_bytes": 5 * 1024 * 1024,
            "user_id": "alice",
            "group_id": "__none__",
            "bot_id": "bot-1",
        },
        "sess-1",
        user_id="alice",
    )

    assert forwarded["params"]["max_bytes"] == _PREVIEW_HARD_MAX_BYTES
    channel.send_response.assert_awaited()
    assert channel.send_response.await_args.kwargs["ok"] is True


@pytest.mark.asyncio
async def test_http_download_redirects_when_download_url_present(monkeypatch):
    """大文件回包只有 download_url 时，HTTP 层应 302 到 /file-api/download。"""

    class _Outbound:
        async def wait_response(self, *_a, **_k):
            return {
                "ok": True,
                "payload": {
                    "filename": "big.bin",
                    "content_type": "application/octet-stream",
                    "size_bytes": E2A_PAYLOAD_MAX_BYTES + 1,
                    "download_url": "/file-api/download?token=abc",
                    "download_token": "abc",
                },
            }

    async def _dispatch(*_a, **_k):
        return _Outbound(), "req-dl", "sess"

    monkeypatch.setattr(http_ws, "dispatch_http_request", _dispatch)
    channel = MagicMock()
    channel.unregister_request_outbound = AsyncMock()

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/v1/workspace/download",
        "headers": [],
        "query_string": b"relative_path=big.bin",
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
        "scheme": "http",
    }
    request = Request(scope)
    response = await http_ws.workspace_download_response(request, channel)
    assert response.status_code == 302
    assert response.headers["location"] == "/file-api/download?token=abc"


@pytest.mark.asyncio
async def test_http_download_still_streams_small_content_b64(monkeypatch):
    payload = b"hello-workspace"
    b64 = base64.b64encode(payload).decode("ascii")

    class _Outbound:
        async def wait_response(self, *_a, **_k):
            return {
                "ok": True,
                "payload": {
                    "filename": "small.txt",
                    "content_type": "text/plain",
                    "size_bytes": len(payload),
                    "content_b64": b64,
                },
            }

    async def _dispatch(*_a, **_k):
        return _Outbound(), "req-sm", "sess"

    monkeypatch.setattr(http_ws, "dispatch_http_request", _dispatch)
    channel = MagicMock()
    channel.unregister_request_outbound = AsyncMock()
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/v1/workspace/download",
        "headers": [],
        "query_string": b"relative_path=small.txt",
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
        "scheme": "http",
    }
    response = await http_ws.workspace_download_response(Request(scope), channel)
    assert response.status_code == 200
    assert response.body == payload


def test_local_download_payload_uses_token_near_e2a_limit(monkeypatch, tmp_path):
    """超过 E2A_PAYLOAD_MAX_BYTES 时本地路径也不得返回超大 content_b64。"""
    monkeypatch.setattr(ws_mod, "is_enterprise", lambda: False)
    monkeypatch.setattr(ws_mod, "_has_local_workspace", lambda: True)

    big = b"x" * (E2A_PAYLOAD_MAX_BYTES + 1)
    svc = SimpleNamespace(
        download_payload=lambda _rel: {
            "filename": "big.bin",
            "content_type": "application/octet-stream",
            "content": big,
        },
        resolve=lambda _rel: (_ for _ in ()).throw(FileNotFoundError()),
    )
    monkeypatch.setattr(ws_mod, "_local_service", lambda _p: svc)

    staged = SimpleNamespace(
        asset_id="a1",
        sealed_path=tmp_path / "a1.bin",
        expires_at=1.0,
        size_bytes=len(big),
        content_digest="d" * 64,
    )
    owner = MagicMock()
    owner.stage.return_value = staged

    # Gateway 单测环境可能未安装 jiuwenswarm；注入最小假模块。
    fake_root = types.ModuleType("jiuwenswarm")
    fake_common = types.ModuleType("jiuwenswarm.common")
    fake_vd = types.ModuleType("jiuwenswarm.common.verified_download_assets")
    fake_tok = types.ModuleType("jiuwenswarm.common.file_download_token")
    fake_vd.get_verified_download_asset_owner = lambda: owner
    fake_tok.build_verified_asset_download_info = lambda *_a, **_k: {
        "download_token": "tok",
        "download_url": "/file-api/download?token=tok",
        "name": "big.bin",
        "size": len(big),
        "mime_type": "application/octet-stream",
    }
    monkeypatch.setitem(sys.modules, "jiuwenswarm", fake_root)
    monkeypatch.setitem(sys.modules, "jiuwenswarm.common", fake_common)
    monkeypatch.setitem(sys.modules, "jiuwenswarm.common.verified_download_assets", fake_vd)
    monkeypatch.setitem(sys.modules, "jiuwenswarm.common.file_download_token", fake_tok)

    out = ws_mod._local_download_payload({}, "big.bin")
    assert "content_b64" not in out
    assert out["download_url"] == "/file-api/download?token=tok"
    assert out["size_bytes"] == len(big)
    # 回归：恰好等于 E2A_PAYLOAD_MAX_BYTES 仍走 inline（帧安全边界）。
    svc_small = SimpleNamespace(
        download_payload=lambda _rel: {
            "filename": "edge.bin",
            "content_type": "application/octet-stream",
            "content": b"y" * E2A_PAYLOAD_MAX_BYTES,
        }
    )
    monkeypatch.setattr(ws_mod, "_local_service", lambda _p: svc_small)
    edge = ws_mod._local_download_payload({}, "edge.bin")
    assert "content_b64" in edge
    assert len(base64.b64decode(edge["content_b64"])) == E2A_PAYLOAD_MAX_BYTES
