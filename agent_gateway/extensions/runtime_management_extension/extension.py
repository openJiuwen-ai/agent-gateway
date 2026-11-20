# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Runtime Routing Extension（route → HTTP → touch，不管理 AgentServer 生命周期）。"""

from __future__ import annotations

import logging
import os

from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import (
    RuntimeRoutedAgentClient,
)
from agent_gateway.extensions.sdk.agent_server_client import AgentServerClientExtension
from agent_gateway.extensions.types import ExtensionConfig
from agent_gateway.routing.agent_client import AgentServerClient

logger = logging.getLogger(__name__)


class RuntimeManagementExtension(AgentServerClientExtension):
    """Runtime 路由扩展：注册 session route + HTTP 发送客户端（不含 Pod/配置下发）。"""

    def __init__(self, client: AgentServerClient) -> None:
        self._client = client

    async def initialize(self, config: ExtensionConfig) -> None:
        return None

    def get_client(self) -> AgentServerClient:
        return self._client

    async def shutdown(self) -> None:
        try:
            await self._client.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[RuntimeManagement] shutdown error: %s", exc)


def runtime_client_requested(env: dict[str, str] | None = None) -> bool:
    """只有环境变量写成 runtime 时，这个扩展才占用南向客户端。"""
    source = os.environ if env is None else env
    mode = str(source.get("GATEWAY_AGENT_CLIENT_MODE") or "").strip().lower()
    return mode == "runtime"


async def register_extensions(registry) -> list[RuntimeManagementExtension]:
    """注册 Runtime 路由扩展。不是 runtime 模式时不注册，避免盖掉 WebSocket。"""
    if not runtime_client_requested():
        logger.info(
            "[RuntimeManagement] GATEWAY_AGENT_CLIENT_MODE 不是 runtime，不注册 runtime 客户端"
        )
        return []
    client = RuntimeRoutedAgentClient()
    ext = RuntimeManagementExtension(client)
    registry.register_agent_server_client(ext)
    return [ext]
