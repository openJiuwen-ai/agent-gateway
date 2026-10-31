# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Outbound message routing for GatewayServer WebSocket clients.

The Gateway server owns connection lifecycle state, while outbound messages
need a deterministic lookup order: fan-out targets, request-scoped responses,
then session fallback. Keeping this routing separate makes those priorities
testable without standing up a real WebSocket server.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from agent_gateway.message_normalization import build_event_frame as _build_event_frame


logger = logging.getLogger("gateway")


@dataclass
class OutboundMessageDelivery:
    """Deliver a normalized Message to one of the connected WebSocket clients."""

    routes: dict[str, Any]
    request_to_client: dict[tuple, Any]
    session_to_client: dict[tuple, Any]
    client_route_key: Callable[..., tuple | None]
    extract_routing_session_id: Callable[..., str | None]
    lookup_client: Callable[..., Any | None]
    find_channel_clients: Callable[..., list[Any]]
    acp_bridge: Any

    async def send(self, msg, *, routing_target=None) -> None:
        """Route ``msg`` and send its encoded frame to the resolved WebSocket."""
        _agent_ref = getattr(msg, "agent_ref", None) or None
        _send_cid = getattr(msg, "channel_id", None)
        _send_sid = getattr(msg, "session_id", None)
        _send_id = getattr(msg, "id", None)
        ws: Any = None

        # fan-out routes first so a team dispatch can use any active member ws.
        if routing_target is not None:
            routing_keys = getattr(routing_target, "routing_keys", None) or []
            for routing_key in routing_keys:
                key_session_id = getattr(routing_key, "session_id", None)
                if not key_session_id:
                    continue
                candidate = self.lookup_client(
                    self.session_to_client,
                    getattr(routing_key, "channel_id", None) or _send_cid,
                    key_session_id,
                    getattr(routing_key, "agent_ref", None),
                )
                if candidate is not None and not bool(getattr(candidate, "closed", False)):
                    ws = candidate
                    break

        # Request responses return to the originating connection.
        if ws is None:
            ws = self.lookup_client(
                self.request_to_client,
                _send_cid,
                _send_id,
                _agent_ref,
            )

        # Session fallback keeps events alive when request state was overwritten.
        if ws is None or bool(getattr(ws, "closed", False)):
            ws = self.lookup_client(
                self.session_to_client,
                _send_cid,
                _send_sid,
                _agent_ref,
            )

        if ws is None or bool(getattr(ws, "closed", False)):
            if ws is None:
                channel_id = getattr(msg, "channel_id", None)
                # Cron and other pushes may not have a session; broadcast by channel.
                if channel_id and channel_id != "acp":
                    if msg.type == "res":
                        payload_data = dict(msg.payload or {}) if isinstance(msg.payload, dict) else {}
                        frame = {
                            "type": "res",
                            "id": msg.id,
                            "ok": bool(msg.ok),
                            "payload": payload_data,
                        }
                    else:
                        frame = _build_event_frame(msg)

                    session_id = self.extract_routing_session_id(
                        msg, include_top_level=False
                    )
                    if session_id:
                        session_key = self.client_route_key(channel_id, session_id)
                        if session_key:
                            client = self.session_to_client.get(session_key)
                            if client is not None and not bool(getattr(client, "closed", False)):
                                data = json.dumps(frame, ensure_ascii=False)
                                try:
                                    await client.send(data)
                                except Exception:
                                    logger.debug(
                                        "[GatewayServer] session-routed send failed: session_id=%s",
                                        session_id,
                                        exc_info=True,
                                    )
                                return
                    elif not self.extract_routing_session_id(msg, include_top_level=True):
                        clients = self.find_channel_clients(channel_id)
                        if clients:
                            data = json.dumps(frame, ensure_ascii=False)
                            logger.info(
                                "[GatewayServer] broadcast fallback (no session_id): "
                                "channel_id=%s clients=%d id=%s type=%s",
                                channel_id,
                                len(clients),
                                getattr(msg, "id", None),
                                msg.type,
                            )
                            await asyncio.gather(
                                *[c.send(data) for c in clients],
                                return_exceptions=True,
                            )
                            return

                logger.warning(
                    "[GatewayServer] message dropped: no WebSocket client found for"
                    " channel_id=%s session_id=%s id=%s sess_table_keys=%s",
                    getattr(msg, "channel_id", None),
                    getattr(msg, "session_id", None),
                    getattr(msg, "id", None),
                    [str(key) for key in self.session_to_client.keys()],
                )
            else:
                logger.warning(
                    "[GatewayServer] ws already closed, drop: channel_id=%s id=%s session_id=%s ws=%s",
                    _send_cid,
                    _send_id,
                    _send_sid,
                    hex(id(ws)),
                )
            return

        if getattr(msg, "channel_id", None) == "acp":
            handled = await self.acp_bridge.send_message(msg, ws)
            if handled:
                return

        # Route interceptors run exactly once, before generic frame encoding.
        for route in self.routes.values():
            if route.channel_id == msg.channel_id and route.outbound_interceptor is not None:
                try:
                    handled = route.outbound_interceptor(msg, ws)
                    if asyncio.iscoroutine(handled):
                        handled = await handled
                    if handled:
                        return
                except Exception:
                    logger.warning(
                        "GatewayServer outbound interceptor failed: channel_id=%s",
                        msg.channel_id,
                        exc_info=True,
                    )
                break

        if msg.type == "res":
            payload = dict(msg.payload or {}) if isinstance(msg.payload, dict) else {}
            frame: dict[str, Any] = {
                "type": "res",
                "id": msg.id,
                "ok": bool(msg.ok),
                "payload": payload,
            }
            if not msg.ok:
                frame["error"] = str(payload.get("error") or "request failed")
                code_val = payload.get("code")
                if isinstance(code_val, str) and code_val.strip():
                    frame["code"] = code_val.strip()
            await ws.send(json.dumps(frame, ensure_ascii=False))
            return

        event_name = "chat.final"
        if msg.event_type is not None:
            event_name = msg.event_type.value

        if isinstance(msg.payload, dict):
            payload = {**msg.payload}
            payload.setdefault("session_id", msg.session_id)
        else:
            payload = {"session_id": msg.session_id, "content": str(msg.payload or "")}

        frame = {"type": "event", "event": event_name, "payload": payload}
        await ws.send(json.dumps(frame, ensure_ascii=False))
