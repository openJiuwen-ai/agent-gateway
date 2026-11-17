# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""技能详情只读 RPC 的 Gateway mapped 路由与企业白名单。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from unittest.mock import AsyncMock

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.channel_manager.web.web_http_dispatch import dispatch_http_request

import agent_gateway

_GATEWAY_ROOT = Path(agent_gateway.__file__).resolve().parent
_MAPPED = _GATEWAY_ROOT / "channel_manager" / "web" / "http_routes" / "mapped.py"

_SKILL_DETAIL_READ_METHODS: dict[str, tuple[str, str]] = {
    "skills.files.list": ("GET", "/skills/{name}/files"),
    "skills.files.get": ("GET", "/skills/{name}/files/content"),
    "skills.versions.list": ("GET", "/skills/{name}/versions"),
}


def _load_mapped():
    spec = importlib.util.spec_from_file_location("skills_detail_mapped_routes", _MAPPED)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("method,expected", sorted(_SKILL_DETAIL_READ_METHODS.items()))
def test_skill_detail_read_methods_have_gateway_mapped_routes(
    method: str, expected: tuple[str, str]
) -> None:
    mapped = _load_mapped()
    verb, path = expected
    matched = [
        route
        for route in mapped.MAPPED_ROUTES
        if route.rpc_method == method and route.http_method == verb and route.path == path
    ]
    assert matched, f"mapped.py missing {verb} {path} → {method}"


@pytest.mark.parametrize("method", sorted(_SKILL_DETAIL_READ_METHODS))
@pytest.mark.asyncio
async def test_skill_detail_read_methods_forwarded_in_enterprise(method, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    upstream = AsyncMock(return_value=True)
    channel.on_message(upstream)
    outbound, _, _ = await dispatch_http_request(
        channel, method=method, params={"name": "example"}, bind_session_param=False,
    )
    try:
        upstream.assert_awaited_once()
        assert upstream.await_args.args[0].req_method == method
        assert upstream.await_args.args[0].params == {"name": "example"}
    finally:
        channel.unregister_request_outbound(outbound)
        await outbound.close()
