# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""会话素材北向路由：挂载、参数映射，以及路由表方法都能被协议枚举解析。"""

from __future__ import annotations

import pytest
from starlette.requests import Request

from agent_gateway.channel_manager.web.http_routes.mapped import (
    WORKSPACE_FILE_ROUTES,
    mapped_routes_for_edition,
)
from agent_gateway.channel_manager.web.web_connect import WebChannel
from agent_gateway.channel_manager.web.web_http_routes import _params_from_mapped_route

_ASSET_ROUTES = (
    ("GET", "/sessions/{session_id}/assets", "session.assets.list"),
    ("POST", "/sessions/{session_id}/assets", "session.assets.register"),
    ("PATCH", "/sessions/{session_id}/assets/{asset_id}", "session.assets.rename"),
)


def _route(http_method: str, path: str):
    found = [
        route
        for route in WORKSPACE_FILE_ROUTES
        if route.http_method == http_method and route.path == path
    ]
    assert len(found) == 1
    return found[0]


def _request(method: str, path: str, path_params: dict[str, str]) -> Request:
    return Request({
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [],
        "path_params": path_params,
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    })


@pytest.mark.parametrize("enterprise", [False, True])
def test_session_asset_routes_mount_for_both_editions(enterprise: bool) -> None:
    mounted = {
        (route.http_method, route.path, route.rpc_method)
        for route in mapped_routes_for_edition(enterprise=enterprise)
    }
    assert set(_ASSET_ROUTES) <= mounted
    assert not any(
        route.rpc_method == "session.assets.remove" or (
            route.http_method == "DELETE" and route.path.endswith("/assets/{asset_id}")
        )
        for route in mapped_routes_for_edition(enterprise=enterprise)
    )


@pytest.mark.asyncio
async def test_register_params_keep_items_and_path_session() -> None:
    route = _route("POST", "/sessions/{session_id}/assets")
    params = await _params_from_mapped_route(
        _request("POST", "/api/v1/sessions/sess-1/assets", {"session_id": "sess-1"}),
        route,
        {"items": [{"path": "/tmp/a.png"}], "session_id": "from-body"},
    )
    assert params["items"] == [{"path": "/tmp/a.png"}]
    assert params["session_id"] == "sess-1"


@pytest.mark.asyncio
async def test_rename_params_keep_name_and_path_ids() -> None:
    route = _route("PATCH", "/sessions/{session_id}/assets/{asset_id}")
    params = await _params_from_mapped_route(
        _request(
            "PATCH",
            "/api/v1/sessions/sess-1/assets/asset-1",
            {"session_id": "sess-1", "asset_id": "asset-1"},
        ),
        route,
        {"name": "新名称", "asset_id": "from-body", "session_id": "from-body"},
    )
    assert params["name"] == "新名称"
    assert params["asset_id"] == "asset-1"
    assert params["session_id"] == "sess-1"


def test_workspace_file_routes_parse_to_req_method() -> None:
    missing = [
        route.rpc_method
        for route in WORKSPACE_FILE_ROUTES
        if WebChannel.parse_req_method(route.rpc_method) is None
    ]
    assert missing == []
