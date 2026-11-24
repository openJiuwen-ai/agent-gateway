# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Web HTTP 路由：mapped 表 + 特殊挂载。"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from agent_gateway.channel_manager.web.http_routes.mapped import (
    MAPPED_ROUTES,
    WebHttpMappedRoute,
    catalog_entries,
    mapped_routes_for_edition,
    validate_mapped_routes,
)

__all__ = [
    "MAPPED_ROUTES",
    "WebHttpMappedRoute",
    "catalog_entries",
    "mapped_routes_for_edition",
    "register_special_http_routes",
    "register_workspace_http_routes",
    "validate_mapped_routes",
]


def register_workspace_http_routes(app: FastAPI, channel: Any) -> None:
    """延迟导入，避免 http_routes ↔ workspace ↔ web_http_routes 循环依赖。"""
    from agent_gateway.channel_manager.web.http_routes.workspace import (
        register_workspace_http_routes as _register,
    )

    _register(app, channel)


def register_special_http_routes(app: FastAPI, channel: Any) -> None:
    """挂载不能走 mapped 表的特殊 HTTP 路由（如 download 文件流）。"""
    register_workspace_http_routes(app, channel)
