# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Configuration RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any


def register_config_proxy_handlers(
    channel: Any,
    agent_client: Any,
    _resolve: Any,
) -> None:
    """Register configuration RPC handlers in their original order."""

    async def _proxy_config_request(ws, req_id, params, session_id, user_id=None, *, req_method):
        """Execute configuration RPCs in the target AgentServer's directories.

        MoA readiness always uses E2A. Other configuration methods retain their
        shared-directory handlers through ``_register_config_proxy``.
        """
        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="config",
        )

    def _external_cli_install_gate(params: dict[str, Any]) -> dict[str, Any] | None:
        """外部 CLI 依赖安装门控（原 config.set 本地执行的 ensure 回调语义）。

        这是前端"一键安装"的唯一触发通道（``external_cli.install`` 无前端调用
        方），安装器与状态轮询（``external_cli.install_status``）都是 gateway
        本地能力，须随 A4g 保留。命中 codex/claude 相关键且依赖未就绪时启动
        安装并返回状态 payload——调用方据此短路本次 config.set（安装完成前
        不写配置，与原本地语义一致，前端装完后重发）。已就绪/未命中返回
        None，请求正常经 E2A 转发。触发条件为简化版 key 前缀判定：相比本地
        执行少了"当前配置中该 agent 已启用"的前置检查，误触发的代价仅是
        ensure 幂等空转（已装即返回 None）。
        """
        from agent_gateway.common.config_panel.config_set_handlers import (
            team_payload_requests_codex,
        )

        # 键形状与 config_save_all_handler 的展平规则一致：config.set 键在顶层；
        # config.save_all 键嵌套在 params.config（前端外部 CLI 卡片的唯一保存
        # 链路即 save_all 嵌套形状），agents/team 两处皆可出现且**顶层优先**
        # （save_all 的合并顺序：嵌套 config 先建，顶层 agents/team 后写覆盖——
        # 门控若反向合并，碰撞形状下会漏判被顶层覆盖落盘的 team 载荷）。
        raw_config = params.get("config")
        keys_view: dict[str, Any] = (
            dict(raw_config) if isinstance(raw_config, dict) else {}
        )
        for key, value in params.items():
            if key != "config":
                keys_view[key] = value

        codex_touched = any(
            key.startswith("external_cli_agent_codex_") for key in keys_view
        ) or (
            ("agents" in keys_view or "team" in keys_view)
            and team_payload_requests_codex(keys_view)
        )
        claude_touched = any(
            key.startswith("external_cli_agent_claude_") for key in keys_view
        )
        installs: dict[str, Any] = {}
        if codex_touched:
            install = _ensure_codex_dependency_available_or_start_install()
            if install is not None:
                installs["codex"] = install
        if claude_touched:
            install = _ensure_claude_dependency_available_or_start_install()
            if install is not None:
                installs["claude"] = install
        if not installs:
            return None
        return {
            "updated": [],
            "applied_without_restart": False,
            "external_cli_dependency_installs": installs,
        }

    def _register_config_proxy(method_name, req_method, local_handler=None):
        # A4g：配置读写统一经目标 AgentServer 执行（ConfigAdapter 在注入目录
        # 本地执行，自带 in-process reload / zen warmup / config_changed 标记
        # 等后置行为），删除 legacy 单用户的 gateway 本地执行回落。
        # 保留的唯一 gateway 侧门控：外部 CLI 依赖安装——仅共享目录单用户
        # 触发（AgentOS 用户目录在目标沙箱，gateway 主机安装无意义，保持
        # 既有零变化）。local_handler 参数保留为兼容签名，不再使用。
        del local_handler

        async def _handler(ws, req_id, params, session_id, user_id=None):
            from agent_gateway.routing.e2a_proxy import is_legacy_shared_directory_client

            # 登录会话 id 随请求转发（硬规则：客户端传来的键先删，只信连接属性）：
            # models.list 经目标 AgentServer 执行时无法看到 Gateway 连接，登录送的
            # 免费模型合并需要会话 id（AgentOS 既有缺口，A4g 统一后对 legacy 同样关键）。
            if isinstance(params, dict):
                from openjiuwen_protocol.e2a.constants import E2A_AUTH_SESSION_PARAM_KEY

                forwarded = dict(params)
                forwarded.pop(E2A_AUTH_SESSION_PARAM_KEY, None)
                auth_session = str(getattr(ws, "_jiuwen_auth_session", "") or "").strip()
                if auth_session:
                    forwarded[E2A_AUTH_SESSION_PARAM_KEY] = auth_session
                params = forwarded
            if req_method in (
                _ConfigReq.CONFIG_SET,
                _ConfigReq.CONFIG_SAVE_ALL,
            ) and isinstance(params, dict):
                resolved_client = _resolve(agent_client)
                if resolved_client is not None and is_legacy_shared_directory_client(resolved_client):
                    install_payload = _external_cli_install_gate(params)
                    if install_payload is not None:
                        await channel.send_response(ws, req_id, ok=True, payload=install_payload)
                        return
            await _proxy_config_request(
                ws, req_id, params, session_id, user_id, req_method=req_method
            )
        channel.register_method(method_name, _handler)

    from openjiuwen_protocol.message import ReqMethod as _ConfigReq

    async def _config_moa_status(ws, req_id, params, session_id, user_id=None):
        await _proxy_config_request(
            ws, req_id, params, session_id, user_id,
            req_method=_ConfigReq.CONFIG_MOA_STATUS,
        )

    channel.register_method("config.moa_status", _config_moa_status)
    _register_config_proxy("config.get", _ConfigReq.CONFIG_GET)
    _register_config_proxy("config.set", _ConfigReq.CONFIG_SET)
    _register_config_proxy("config.save_all", _ConfigReq.CONFIG_SAVE_ALL)
    _register_config_proxy("config.validate_model", _ConfigReq.CONFIG_VALIDATE_MODEL)
    _register_config_proxy("models.list", _ConfigReq.MODELS_LIST)
    _register_config_proxy("models.get", _ConfigReq.MODELS_GET)
    _register_config_proxy("models.replace_all", _ConfigReq.MODELS_REPLACE_ALL)
    _register_config_proxy("models.validate", _ConfigReq.MODELS_VALIDATE)
    _register_config_proxy("models.upsert", _ConfigReq.MODELS_UPSERT)
    _register_config_proxy("models.delete", _ConfigReq.MODELS_DELETE)
    _register_config_proxy("models.references", _ConfigReq.MODELS_REFERENCES)
    _register_config_proxy("model_groups.list", _ConfigReq.MODEL_GROUPS_LIST)
    _register_config_proxy("model_groups.upsert", _ConfigReq.MODEL_GROUPS_UPSERT)
    _register_config_proxy("model_groups.delete", _ConfigReq.MODEL_GROUPS_DELETE)
    _register_config_proxy("session.selection.set", _ConfigReq.SESSION_SELECTION_SET)
