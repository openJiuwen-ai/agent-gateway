# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""按环境变量选出 Gateway 往 Agent 发消息用的客户端。"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any


class AgentClientModeError(RuntimeError):
    """启动时环境变量选的南向客户端不对。"""


# ``gateway.agent_client.type`` 中走 router 扩展（非 WebSocket）的类型。
# 新增 router 扩展时只改这里：Runtime 冲突校验与 SSH 渠道准入共用同一口径。
ROUTER_CLIENT_TYPES: frozenset[str] = frozenset({"agentos_router", "adx_router"})


def is_router_client_type(selected: str) -> bool:
    """判断配置选中的南向类型是否属于 router 扩展。"""
    return str(selected or "").strip().lower() in ROUTER_CLIENT_TYPES


def read_agent_client_mode(env: dict[str, str] | None = None) -> str:
    """读 GATEWAY_AGENT_CLIENT_MODE。没写就是空字符串，表示沿用现在的 WebSocket。"""
    source = os.environ if env is None else env
    return str(source.get("GATEWAY_AGENT_CLIENT_MODE") or "").strip().lower()


def is_runtime_agent_client(client: Any) -> bool:
    """显式客户端能力标记；不依赖实现类名，允许子类和装饰适配。"""
    return getattr(client, "agent_client_mode", None) == "runtime"


def validate_agent_client_configuration(config: dict[str, Any], *, mode: str) -> None:
    """在扩展加载前拒绝 Runtime 与配置选择的其他路由器冲突。"""
    gateway = config.get("gateway") or {}
    selected = str((gateway.get("agent_client") or {}).get("type") or "").strip().lower()
    if mode == "runtime" and is_router_client_type(selected):
        raise AgentClientModeError(
            f"GATEWAY_AGENT_CLIENT_MODE=runtime conflicts with gateway.agent_client.type={selected}"
        )


def resolve_agent_server_client(
    extension: Any,
    *,
    mode: str,
    websocket_factory: Callable[[], Any],
) -> Any:
    """按环境变量选出往 Agent 发消息的客户端。

    没写、websocket，以及其他不是 http / runtime 的值：有扩展就用扩展，否则用 WebSocket。
    runtime：注册表里的客户端必须显式声明 agent_client_mode="runtime"，否则启动失败。
    http：没有单独的 HTTP 客户端，启动失败。
    """
    if mode == "http":
        raise AgentClientModeError(
            "GATEWAY_AGENT_CLIENT_MODE=http 不能启动：没有单独的 HTTP 客户端。"
            "发给 Pod 的网页请求由 runtime 客户端在内部完成，请改成 runtime 或 websocket。"
        )
    if mode == "runtime":
        client = extension.get_client() if extension is not None else None
        if not is_runtime_agent_client(client):
            raise AgentClientModeError(
                "GATEWAY_AGENT_CLIENT_MODE=runtime，但没有加载 runtime 扩展包，启动失败。"
                "请确认 runtime_management_extension 已放在扩展搜索目录里。"
            )
        return client
    if extension is not None:
        return extension.get_client()
    return websocket_factory()
