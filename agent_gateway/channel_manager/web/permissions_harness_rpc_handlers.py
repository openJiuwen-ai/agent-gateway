# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Permissions, A4P, and harness RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import base64
import logging
import uuid
from typing import Any

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.routing.agent_http_bridge import (
    E2A_PAYLOAD_MAX_BYTES,
    upload_file_bytes_via_e2a,
)
from agent_gateway.routing.e2a_proxy import proxy_unary_request


logger = logging.getLogger(__name__)


def register_permissions_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register permissions RPC handlers in their original route order."""

    # 数字分身 — permissions.owner_scopes：A5 起统一经目标 AgentServer 的
    # ConfigAdapter 执行（写入后 server 侧 in-process reload）。
    # 其余 permissions.*（tools / rules / approval_overrides）走 _forward_permissions_to_agent。

    async def _permissions_owner_scopes_get(ws, req_id, params, session_id, user_id=None):
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws, req_id=req_id,
                                  params={}, session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.PERMISSIONS_OWNER_SCOPES_GET,
                                  label="permissions.owner_scopes.get")

    async def _permissions_owner_scopes_set(ws, req_id, params, session_id, user_id=None):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws, req_id=req_id,
                                  params=params, session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.PERMISSIONS_OWNER_SCOPES_SET,
                                  label="permissions.owner_scopes.set")

    channel.register_method("permissions.owner_scopes.get", _permissions_owner_scopes_get)
    channel.register_method("permissions.owner_scopes.set", _permissions_owner_scopes_set)

    async def _forward_permissions_to_agent(ws, req_id, params, session_id, user_id=None, *, req_method):
        """Forward permissions requests; Gateway never falls back to local state."""
        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="permissions",
        )

    def _register_perm(method_name: str, rm: Any) -> None:
        async def _handler(ws, req_id, params, session_id, user_id=None):
            await _forward_permissions_to_agent(ws, req_id, params, session_id, user_id, req_method=rm)

        channel.register_method(method_name, _handler)

    _register_perm("permissions.tools.get", ReqMethod.PERMISSIONS_TOOLS_GET)
    _register_perm("permissions.tools.set", ReqMethod.PERMISSIONS_TOOLS_SET)
    _register_perm("permissions.tools.update", ReqMethod.PERMISSIONS_TOOLS_UPDATE)
    _register_perm("permissions.tools.update_user", ReqMethod.PERMISSIONS_TOOLS_UPDATE_USER)
    _register_perm("permissions.tools.delete", ReqMethod.PERMISSIONS_TOOLS_DELETE)
    _register_perm("permissions.rules.get", ReqMethod.PERMISSIONS_RULES_GET)
    _register_perm("permissions.rules.create", ReqMethod.PERMISSIONS_RULES_CREATE)
    _register_perm("permissions.rules.update", ReqMethod.PERMISSIONS_RULES_UPDATE)
    _register_perm("permissions.rules.delete", ReqMethod.PERMISSIONS_RULES_DELETE)
    _register_perm("permissions.approval_overrides.get", ReqMethod.PERMISSIONS_APPROVAL_OVERRIDES_GET)
    _register_perm("permissions.approval_overrides.delete", ReqMethod.PERMISSIONS_APPROVAL_OVERRIDES_DELETE)


def register_a4p_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register A4P RPC handlers in their original route order."""

    async def _forward_a4p_to_agent(ws, req_id, params, session_id, *, req_method):
        """Forward an A4P User Authorizer RPC over the session's logical Web route."""
        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return
        ac = _resolve(agent_client)
        if ac is None or not getattr(ac, "server_ready", False):
            await channel.send_response(
                ws, req_id, ok=False, error="Agent server is not ready", code="AGENT_NOT_READY"
            )
            return

        route_metadata: dict[str, Any] = {
            "ws_id": str(getattr(ws, "_jiuwen_ws_id", "") or ""),
        }
        route = None
        try:
            routes = await channel.routing_keys_for_ws(ws)
            route = next(
                (item for item in reversed(routes) if item.session_id == session_id),
                None,
            )
            if route is not None:
                route_metadata["app_id"] = route.app_id
                route_metadata["agent_ref"] = {
                    "mode": route.agent_ref.mode,
                    "id": route.agent_ref.id,
                }
        except Exception:
            logger.warning("[a4p] failed to resolve Web authorizer route", exc_info=True)
        if route is None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="No Web authorizer route is registered for this session",
                code="A4P_WEB_SESSION_REQUIRED",
            )
            return

        env = e2a_from_agent_fields(
            request_id=str(req_id) if req_id else "",
            channel_id="web",
            session_id=session_id,
            req_method=req_method,
            params=dict(params) if isinstance(params, dict) else {},
            metadata=route_metadata,
        )
        authorize_completion = req_method in {
            ReqMethod.A4P_AUTHORIZATION_COMPLETE,
            ReqMethod.A4P_AUTHORIZATION_REJECT,
        }
        if authorize_completion:
            # 刷新后经新连接完成/拒绝授权：把该恢复 turn 的输出改绑到当前连接，
            # 其迟到事件（旧 ws 已注销）据此改投，而不是被当孤儿丢弃。
            channel.record_a4p_rebound(session_id, route_metadata["ws_id"])
        try:
            resp = await ac.send_request(env)
        except Exception as exc:
            if authorize_completion:
                channel.clear_a4p_rebound(session_id)
            logger.exception("[a4p] forward to agent failed: %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")
            return
        if not resp.ok:
            if authorize_completion:
                channel.clear_a4p_rebound(session_id)
            payload = resp.payload if isinstance(resp.payload, dict) else {}
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(payload.get("error") or "request failed"),
                code=str(payload.get("code") or "BAD_REQUEST"),
            )
            return
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload=resp.payload if isinstance(resp.payload, dict) else {},
        )

    def _register_a4p(method_name: str, req_method: Any) -> None:
        async def _handler(ws, req_id, params, session_id):
            await _forward_a4p_to_agent(
                ws, req_id, params, session_id, req_method=req_method
            )

        channel.register_method(method_name, _handler)

    _register_a4p("a4p.config.get", ReqMethod.A4P_CONFIG_GET)
    _register_a4p("a4p.config.update", ReqMethod.A4P_CONFIG_UPDATE)
    _register_a4p("a4p.authorization.reprepare", ReqMethod.A4P_AUTHORIZATION_REPREPARE)
    _register_a4p("a4p.authorization.complete", ReqMethod.A4P_AUTHORIZATION_COMPLETE)
    _register_a4p("a4p.authorization.reject", ReqMethod.A4P_AUTHORIZATION_REJECT)
    _register_a4p("a4p.authorization.pending", ReqMethod.A4P_AUTHORIZATION_PENDING)


def register_harness_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register harness and RSI RPC handlers in their original route order."""

    async def _forward_harness_to_agent(ws, req_id, params, session_id, user_id=None, *, req_method):
        """harness.*：经 E2A 转发到 AgentServer（harness 包目录在 AgentServer 注入目录）。

        AgentServer 不可达时：默认本地 WebSocket client（单用户共享目录）由
        ``e2a_proxy`` 的 legacy fallback 在 Gateway 进程内执行同一业务；远程/
        AgentOS client 返回可重试错误，不读部署侧目录。
        """
        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return

        request_params = dict(params if isinstance(params, dict) else {})
        if str(req_method.value).startswith("rsi."):
            request_params["session_id"] = session_id
            if req_method is ReqMethod.RSI_ARTIFACT_DOWNLOAD and user_id:
                request_params["_download_user_id"] = user_id

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=request_params,
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="harness",
        )

    def _register_harness(method_name: str, rm: Any) -> None:
        async def _handler(ws, req_id, params, session_id, user_id=None):
            await _forward_harness_to_agent(ws, req_id, params, session_id, user_id, req_method=rm)

        channel.register_method(method_name, _handler)

    _register_harness("harness.packages", ReqMethod.HARNESS_PACKAGES_GET)
    _register_harness("harness.packages.scan", ReqMethod.HARNESS_PACKAGES_SCAN)
    _register_harness("harness.activate", ReqMethod.HARNESS_PACKAGES_ACTIVATE)
    _register_harness("harness.deactivate", ReqMethod.HARNESS_PACKAGES_DEACTIVATE)
    _register_harness("harness.delete", ReqMethod.HARNESS_PACKAGES_DELETE)

    # RSI 优化平台 18 个 web method（web 契约 v0.3 §4）：经 E2A 转发到 AgentServer。
    # 与 harness.* 同构（仅注册 + proxy_unary_request，不承载业务）。
    rsi_methods = [
        ("rsi.dataset.validate", ReqMethod.RSI_DATASET_VALIDATE),
        ("rsi.task.create", ReqMethod.RSI_TASK_CREATE),
        ("rsi.task.list", ReqMethod.RSI_TASK_LIST),
        ("rsi.task.get", ReqMethod.RSI_TASK_GET),
        ("rsi.task.delete", ReqMethod.RSI_TASK_DELETE),
        ("rsi.training.start", ReqMethod.RSI_TRAINING_START),
        ("rsi.training.pause", ReqMethod.RSI_TRAINING_PAUSE),
        ("rsi.training.resume", ReqMethod.RSI_TRAINING_RESUME),
        ("rsi.training.terminate", ReqMethod.RSI_TRAINING_TERMINATE),
        ("rsi.report.get", ReqMethod.RSI_REPORT_GET),
        ("rsi.usage.get", ReqMethod.RSI_USAGE_GET),
        ("rsi.artifact.download", ReqMethod.RSI_ARTIFACT_DOWNLOAD),
        ("rsi.artifact.files.list", ReqMethod.RSI_ARTIFACT_FILES_LIST),
        ("rsi.artifact.files.get", ReqMethod.RSI_ARTIFACT_FILES_GET),
        ("rsi.tree.get", ReqMethod.RSI_TREE_GET),
        ("rsi.harness.install", ReqMethod.RSI_HARNESS_INSTALL),
        ("rsi.harness.versions.list", ReqMethod.RSI_HARNESS_VERSIONS_LIST),
        ("rsi.harness.rollback", ReqMethod.RSI_HARNESS_ROLLBACK),
    ]
    for _method_name, _req_method in rsi_methods:
        _register_harness(_method_name, _req_method)

    async def _harness_import_handler(ws, req_id, params, session_id, user_id=None):
        """Import harness archives without exceeding the internal WS frame limit."""
        request_params = params if isinstance(params, dict) else {}
        resolved_client = _resolve(agent_client)
        raw_content = request_params.get("file_content")
        # The browser link accepts substantially larger payloads than the
        # Gateway--AgentServer WebSocket.  Move larger archives in bounded E2A
        # chunks first; the final import request then contains only a path in
        # the target AgentServer's injected directory.
        if (
            isinstance(raw_content, str)
            and getattr(resolved_client, "server_ready", False)
        ):
            try:
                archive = base64.b64decode(raw_content, validate=True)
            except Exception:
                archive = None
            if archive is not None and len(archive) > E2A_PAYLOAD_MAX_BYTES:
                ok, upload_payload = await upload_file_bytes_via_e2a(
                    archive,
                    f"auto-harness/temp/uploads/harness_{uuid.uuid4().hex}.zip",
                    agent_client=resolved_client,
                    user_id=user_id,
                    channel_id=channel.channel_id,
                    session_id=session_id,
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False,
                        error=str(upload_payload.get("error") or "archive upload failed"),
                        code=str(upload_payload.get("code") or "SERVICE_UNAVAILABLE"),
                    )
                    return
                request_params = {
                    **request_params,
                    "uploaded_path": str(upload_payload.get("path") or ""),
                }
                request_params.pop("file_content", None)
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws,
                                  req_id=req_id, params=request_params,
                                  session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.HARNESS_PACKAGES_IMPORT,
                                  label="harness.import", preserve_error_payload=True)

    channel.register_method("harness.import", _harness_import_handler)

    async def _harness_export_handler(ws, req_id, params, session_id, user_id=None):
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws,
                                  req_id=req_id, params=params if isinstance(params, dict) else {},
                                  session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.HARNESS_PACKAGES_EXPORT,
                                  label="harness.export", preserve_error_payload=True)

    channel.register_method("harness.export", _harness_export_handler)
