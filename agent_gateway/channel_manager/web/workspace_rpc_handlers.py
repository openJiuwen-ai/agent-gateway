# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Workspace RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any

from agent_gateway.common.hooks_config import load_hooks_config
from agent_gateway.common.config import get_config
from agent_gateway.routing.e2a_proxy import (
    is_legacy_shared_directory_client,
    proxy_unary_request,
)
from agent_gateway.message_handler.command_parser.slash_command import (
    list_builtin_commands,
)
from openjiuwen_protocol.message import ReqMethod


def register_workspace_handlers(
    channel: Any, agent_client: Any, _resolve: Any
) -> None:
    """Register workspace RPC handlers in their original route order."""

    async def _hooks_list(ws, req_id, params, session_id, user_id=None):
        # 单用户共享目录：本地读取 config.yaml（hooks 属用户态配置，共享目录等价；
        # AgentServer 未启动/断连时保持可用）。AgentOS：经 E2A 在目标注入目录执行。
        resolved_client = _resolve(agent_client)
        if resolved_client is None or is_legacy_shared_directory_client(resolved_client):
            try:
                hooks_config = load_hooks_config(get_config())
                summary = hooks_config.get_event_summary()
                await channel.send_response(
                    ws, req_id, ok=True,
                    payload={
                        "events": summary,
                        "disable_all_hooks": hooks_config.disable_all_hooks,
                        "source": "config.yaml",
                    },
                )
            except Exception as e:  # noqa: BLE001
                await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")
            return

        await proxy_unary_request(
            channel=channel, agent_client=_resolve(agent_client), ws=ws,
            req_id=req_id, params={}, session_id=session_id, user_id=user_id,
            req_method=ReqMethod.HOOKS_LIST, label="hooks.list",
        )

    async def _commands_list(ws, req_id, params, session_id):
        """下发快捷面板命令清单（静态元数据，本地直接返回，不转发 AgentServer）。"""
        try:
            payload = list_builtin_commands(params if isinstance(params, dict) else {})
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except Exception as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _file_upload_chunk(ws, req_id, params, session_id, user_id=None):
        """浏览器分块上传（E2A 转发 AgentServer 的 ``file.upload_chunk``）。

        生产入口（app_web 静态服务）没有 Vite 的 /file-api 上传中间件，画布素材
        改为复用这条已认证的 WS 通道逐块上传（前端 512 KiB/块），由 AgentServer
        落到注入目录；避免前端直连要求 token 的 /file-api/upload 得到
        ``missing_token``（issue #5325）。
        """
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.FILE_UPLOAD_CHUNK,
            label="file.upload_chunk",
            timeout_seconds=120.0,
        )

    channel.register_method("hooks.list", _hooks_list)
    # 注意：commands.list 是本地 handler，刻意不加入 _FORWARD_REQ_METHODS（否则会被
    # 转发到 AgentServer 与本地 handler 冲突）。静态元数据本地返回最快、依赖最少。
    channel.register_method("commands.list", _commands_list)
    channel.register_method("file.upload_chunk", _file_upload_chunk)
