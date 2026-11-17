"""Web 通道请求方法路由：本地白名单 + 默认透传。

Gateway 只负责本地协议（初始化、WebUI 扩展、扩展包管理），业务方法属于
AgentServer。把决策收敛成纯函数后，新增方法无需修改网关即可生效，也便于
用单测覆盖边界行为。
"""

from __future__ import annotations

import os
from typing import AbstractSet

#: 无论如何都不转发的 Web 方法前缀：Gateway 内部与保留命名空间。
DENIED_REQ_PREFIXES: tuple[str, ...] = (
    "gateway.",
    "admin.",
    "extension.internal.",
)

#: 关闭后恢复“仅静态白名单转发”的严格模式，用于灰度与回滚。
FORWARD_UNKNOWN_ENV = "AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS"

ROUTE_FORWARD = "forward"
ROUTE_FORWARD_UNKNOWN = "forward_unknown"
ROUTE_LOCAL = "local"
ROUTE_DENIED = "denied"


def forward_unknown_methods_enabled() -> bool:
    """默认透传未知方法；环境变量显式关闭时走严格模式。"""
    raw = (os.getenv(FORWARD_UNKNOWN_ENV) or "").strip().lower()
    if not raw:
        return True
    return raw not in {"0", "false", "no", "off"}


def route_req_method(
    method: str,
    *,
    forward_methods: AbstractSet[str],
    local_methods: AbstractSet[str] | None,
    forward_unknown: bool | None = None,
) -> str:
    """决定一个 Web 请求方法的处理方式。

    Args:
        method: 请求方法名（如 ``session.list``）。
        forward_methods: 强制转发给 AgentServer 的方法集合；同一方法本地也有
            handler 时以转发为准，保持既有语义。
        local_methods: 本地已注册 handler 的方法集合；为 ``None`` 表示调用方
            未启用默认透传（TUI/ACP 等通道），未知方法一律按本地处理。
        forward_unknown: 覆盖默认透传开关；``None`` 时读取环境变量。

    Returns:
        ``ROUTE_FORWARD`` / ``ROUTE_FORWARD_UNKNOWN`` / ``ROUTE_LOCAL`` / ``ROUTE_DENIED``。
    """
    if method in forward_methods:
        return ROUTE_FORWARD
    if method.startswith(DENIED_REQ_PREFIXES):
        return ROUTE_DENIED
    if local_methods is None:
        return ROUTE_LOCAL
    if method in local_methods:
        return ROUTE_LOCAL
    enabled = forward_unknown_methods_enabled() if forward_unknown is None else forward_unknown
    return ROUTE_FORWARD_UNKNOWN if enabled else ROUTE_LOCAL
