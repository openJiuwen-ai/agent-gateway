# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Inbound request parsing and dispatch for the WebSocket gateway."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from openjiuwen_protocol.message import Message, Mode
from agent_gateway.route_config import LocalHandlerContext as _LocalHandlerContext
from agent_gateway.routing.agent_request_timeout import coerce_client_timeout_ms


logger = logging.getLogger("gateway")


class InboundRequestDispatcher:
    """Handle one raw WebSocket frame for a resolved gateway route."""

    def __init__(self, server: Any) -> None:
        self._server = server

    async def handle_raw_message(
        self,
        ws: Any,
        raw: str,
        request_path: str,
        route: Any,
    ) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(
                json.dumps(
                    {"type": "res", "id": "", "ok": False, "error": "invalid json"},
                    ensure_ascii=False,
                )
            )
            return

        if route.channel_id == "acp":
            handled = await self._server._acp_bridge.handle_jsonrpc_request(ws, data)
            if handled:
                return

        if route.channel_id == "acp" and self._server._acp_bridge.is_jsonrpc_request(data):
            return

        if route.inbound_interceptor is not None:
            try:
                handled = route.inbound_interceptor(ws, data)
                if asyncio.iscoroutine(handled):
                    handled = await handled
                if handled:
                    return
            except Exception:
                logger.warning(
                    "GatewayServer inbound interceptor failed: path=%s",
                    request_path,
                    exc_info=True,
                )

        if not isinstance(data, dict) or data.get("type") != "req":
            await ws.send(
                json.dumps(
                    {
                        "type": "res",
                        "id": "",
                        "ok": False,
                        "error": "invalid request",
                    },
                    ensure_ascii=False,
                )
            )
            return

        req_id = str(data.get("id") or "").strip()
        method = str(data.get("method") or "").strip()
        params = data.get("params") if isinstance(data.get("params"), dict) else {}
        if not req_id or not method:
            await ws.send(
                json.dumps(
                    {
                        "type": "res",
                        "id": req_id,
                        "ok": False,
                        "error": "invalid request",
                    },
                    ensure_ascii=False,
                )
            )
            return

        explicit_session_id = bool(
            str(params.get("session_id") or "").strip()
        )
        session_id = (
            str(params.get("session_id") or "").strip()
        ) or req_id
        session_key = (
            self._server._client_route_key(route.channel_id, session_id)
            if explicit_session_id
            else None
        )
        req_user_id = self._server._connection_user_id(ws)

        if method in route.forward_methods:
            req_method = None
            from openjiuwen_protocol.message import ReqMethod

            for item in ReqMethod:
                if item.value == method:
                    req_method = item
                    break

            if req_method is None:
                await ws.send(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": False,
                            "error": f"unknown method: {method}",
                        },
                        ensure_ascii=False,
                    )
                )
                return

            _agent_ref = params.get("agent_ref")
            request_key = self._server._client_route_key(
                route.channel_id, req_id, _agent_ref
            )
            session_key = (
                self._server._client_route_key(
                    route.channel_id, session_id, _agent_ref
                )
                if explicit_session_id
                else None
            )

            if session_key is not None:
                existing_ws = self._server._connections.session_to_client.get(
                    session_key
                )
                if (
                    existing_ws is not None
                    and existing_ws is not ws
                    and not bool(getattr(existing_ws, "closed", False))
                ):
                    logger.warning(
                        "GatewayServer reject %s: session %s already active in "
                        "another %s connection",
                        method,
                        session_id,
                        route.channel_id,
                    )
                    await self._server.send_response(
                        ws,
                        req_id,
                        ok=False,
                        error=(
                            f"Session {session_id} is already active in "
                            "another window. Close that window first."
                        ),
                        code="SESSION_IN_USE",
                    )
                    return

            if request_key is not None:
                self._server._connections.bind_request(request_key, ws)
            if session_key is not None:
                await self._server._connections.bind_route_session_client(
                    route, session_id, ws
                )

            default_mode = (
                Mode.CODE_NORMAL
                if route.channel_id == "tui"
                else Mode.AGENT
            )
            mode = Mode.from_raw(params.get("mode"), default=default_mode)

            params = dict(params)
            if not isinstance(params.get("mode"), str):
                params["mode"] = mode.value

            params["agent_type"] = self._server._connection_agent_type(ws)

            from agent_gateway.routing.keys import AgentRef as _AgentRef

            _resolved_agent_ref = _agent_ref
            if (
                route.ws_channel is not None
                and not isinstance(_resolved_agent_ref, _AgentRef)
            ):
                _agent_id_raw = (
                    str(params.get("agent_id") or "default").strip() or "default"
                )
                _resolved_agent_ref = _AgentRef(
                    mode=mode.value, id=_agent_id_raw
                )

            _ws_id_for_metadata = ""
            if route.ws_channel is not None:
                from agent_gateway.routing.keys import RoutingKey

                _tui_rk = RoutingKey(
                    user_id="tui",
                    channel_id=route.channel_id,
                    app_id="default",
                    agent_ref=_resolved_agent_ref,
                    session_id=session_id,
                )
                try:
                    await route.ws_channel.register_ws(ws, _tui_rk)
                    _ws_id_for_metadata = (
                        getattr(ws, "_jiuwen_ws_id", "") or ""
                    )
                except Exception:
                    logger.warning(
                        "GatewayServer delegate _register to ws_channel failed: "
                        "channel_id=%s session_id=%s",
                        route.channel_id,
                        session_id,
                        exc_info=True,
                    )

            metadata = {"method": method}
            if _ws_id_for_metadata:
                metadata["ws_id"] = _ws_id_for_metadata
            cwd = params.get("cwd")
            if cwd and isinstance(cwd, str) and cwd.strip():
                metadata["cwd"] = cwd.strip()
            project_dir = params.get("project_dir")
            if project_dir and isinstance(project_dir, str) and project_dir.strip():
                from agent_gateway.common.paths import resolve_git_branch

                metadata["project_dir"] = project_dir.strip()
                metadata["git_branch"] = resolve_git_branch(
                    project_dir.strip()
                )
            client_timeout_ms = coerce_client_timeout_ms(data.get("timeout_ms"))
            if route.channel_id == "tui" and client_timeout_ms is not None:
                metadata["client_timeout_ms"] = client_timeout_ms

            is_stream = bool(data.get("is_stream", False))

            msg = Message(
                id=req_id,
                type="req",
                channel_id=route.channel_id,
                session_id=session_id,
                params=params,
                timestamp=time.time(),
                ok=True,
                req_method=req_method,
                mode=mode,
                metadata=metadata,
                is_stream=is_stream,
                agent_ref=_resolved_agent_ref,
                user_id=req_user_id,
            )

            if self._server._on_message_cb is not None:
                result = self._server._on_message_cb(msg)
                if asyncio.iscoroutine(result):
                    await result

            if route.channel_id == "acp":
                return

            if method in route.forward_no_local_handler_methods:
                return

        local_handler = route.local_handlers.get(method)
        if local_handler is not None:
            if session_key is not None:
                await self._server._connections.bind_route_session_client(
                    route, session_id, ws
                )
            try:
                await self._server._invoke_local_handler(
                    local_handler,
                    _LocalHandlerContext(
                        ws=ws,
                        req_id=req_id,
                        params=params,
                        session_id=session_id,
                        user_id=req_user_id,
                    ),
                )
            except Exception as exc:
                ws_closed = bool(getattr(ws, "closed", False))
                if ws_closed:
                    logger.warning(
                        "GatewayServer local handler aborted on closed ws "
                        "(%s): %s",
                        method,
                        exc,
                    )
                    return
                logger.error(
                    "GatewayServer local handler error (%s): %s",
                    method,
                    exc,
                )
                try:
                    await self._server.send_response(
                        ws,
                        req_id,
                        ok=False,
                        error=f"handler error: {exc}",
                        code="INTERNAL_ERROR",
                    )
                except Exception:
                    logger.debug(
                        "GatewayServer failed to send handler error response: "
                        "method=%s id=%s",
                        method,
                        req_id,
                        exc_info=True,
                    )
            return

        await ws.send(
            json.dumps(
                {
                    "type": "res",
                    "id": req_id,
                    "ok": False,
                    "error": f"unknown method: {method}",
                },
                ensure_ascii=False,
            )
        )
