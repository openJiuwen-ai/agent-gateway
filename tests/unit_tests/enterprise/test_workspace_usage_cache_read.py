# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""workspace.usage 企业版缓存优先：轮询不 forward AS / force_refresh 才测盘。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.web.web_method_register import workspace as ws_mod
from agent_gateway.enterprise.capabilities.workspace import quota as quota_mod


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({}, False),
        ({"force_refresh": True}, True),
        ({"force_refresh": 1}, True),
        ({"force_refresh": "true"}, True),
        ({"force_refresh": "yes"}, True),
        ({"force_refresh": "0"}, False),
        ({"force_refresh": False}, False),
        (None, False),
    ],
)
def test_force_refresh_requested(params, expected):
    assert ws_mod._force_refresh_requested(params) is expected


@pytest.mark.asyncio
async def test_get_cached_used_bytes_returns_row(monkeypatch):
    import agent_gateway.config.enterprise.access as access

    repo = SimpleNamespace(
        get=AsyncMock(return_value={"used_bytes": 32681984, "user_id": "u"}),
    )
    monkeypatch.setattr(
        access,
        "get_enterprise_record_repository",
        lambda _name: repo,
    )

    used = await quota_mod.get_cached_used_bytes(
        user_id="u", group_id="g", bot_id="b",
    )
    assert used == 32681984
    repo.get.assert_awaited_once_with(
        {"user_id": "u", "group_id": "g", "bot_id": "b"},
    )


@pytest.mark.asyncio
async def test_get_cached_used_bytes_miss(monkeypatch):
    import agent_gateway.config.enterprise.access as access

    repo = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr(
        access,
        "get_enterprise_record_repository",
        lambda _name: repo,
    )
    assert (
        await quota_mod.get_cached_used_bytes(user_id="u", group_id="g", bot_id="b")
    ) is None


@pytest.mark.asyncio
async def test_get_cached_used_bytes_rejects_invalid(monkeypatch):
    import agent_gateway.config.enterprise.access as access

    repo = SimpleNamespace(get=AsyncMock(return_value={"used_bytes": -1}))
    monkeypatch.setattr(
        access,
        "get_enterprise_record_repository",
        lambda _name: repo,
    )
    assert (
        await quota_mod.get_cached_used_bytes(user_id="u", group_id="g", bot_id="b")
    ) is None


@pytest.mark.asyncio
async def test_enterprise_usage_serves_cache_without_forward(monkeypatch):
    """有缓存且无 force_refresh 时，企业版 usage 不得 forward 到 AS。"""
    forwarded: list[object] = []

    async def boom_forward(*_a, **_k):
        forwarded.append(1)
        raise AssertionError("must not forward when cache hits")

    monkeypatch.setattr(ws_mod, "_use_local_workspace", lambda: False)
    monkeypatch.setattr(ws_mod, "is_workspace_quota_enabled", lambda: True)
    monkeypatch.setattr(ws_mod, "_forward_to_agent", boom_forward)
    monkeypatch.setattr(
        ws_mod,
        "get_cached_used_bytes",
        AsyncMock(return_value=32681984),
    )
    monkeypatch.setattr(
        ws_mod,
        "resolve_quota_snapshot",
        AsyncMock(
            return_value=SimpleNamespace(
                used_bytes=32681984,
                limit_bytes=10 * 1024**4,
                percent=0.0003,
                status="ok",
                source_policy_id="local_default",
                unlimited=False,
            )
        ),
    )
    monkeypatch.setattr(
        ws_mod,
        "snapshot_to_dict",
        lambda snap, **kw: {
            "used_bytes": snap.used_bytes,
            "limit_bytes": snap.limit_bytes,
            "percent": snap.percent,
            "status": snap.status,
            "source_policy_id": snap.source_policy_id,
            "unlimited": snap.unlimited,
            "quota_enabled": True,
            **kw,
        },
    )
    monkeypatch.setattr(
        ws_mod,
        "_bind_trusted_identity",
        lambda *_a, **_k: ("u", "g", "b", ""),
    )

    sent: list[dict] = []

    async def fake_send_response(_ws, _rid, **kwargs):
        sent.append(kwargs)

    handler_box: dict[str, object] = {}

    def capture_register(method, handler):
        handler_box[method] = handler

    # register 闭包捕获的是同一个 channel；send_response 必须挂在其上。
    channel = SimpleNamespace(
        register_method=capture_register,
        send_response=fake_send_response,
    )
    ws_mod.register_workspace_web_methods(channel, agent_client=object())
    # ReqMethod.value is the wire method name.
    usage = handler_box.get("workspace.usage") or next(iter(handler_box.values()))
    assert callable(usage)
    await usage(object(), "r1", {"session_id": "s1"}, "s1", user_id="u")

    assert forwarded == []
    assert len(sent) == 1
    assert sent[0]["ok"] is True
    assert sent[0]["payload"]["used_bytes"] == 32681984
