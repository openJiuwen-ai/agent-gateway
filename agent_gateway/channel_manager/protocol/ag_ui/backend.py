"""Reuse Gateway E2A routing and credential preparation for HTTP runs."""

from __future__ import annotations

import time
from contextlib import aclosing
from typing import Any

from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod

from agent_gateway.routing.e2a_proxy import fetch_agent_unary
from .store import AGUIError


class E2ABackend:
    def __init__(self, channel: Any):
        self.channel = channel

    @property
    def client(self):
        return getattr(self.channel, "ag_ui_agent_client", None)

    async def unary(self, method: ReqMethod, principal: str, session_id: str | None, params: dict):
        ok, payload = await fetch_agent_unary(
            agent_client=self.client, req_method=method, params=params,
            session_id=session_id, user_id=principal, channel_id="ag-ui", label="ag-ui",
        )
        if not ok:
            raise AGUIError("AgentServer request failed", str(payload.get("code") or "HOST_ERROR"), 502)
        return payload

    async def create(self, principal: str, token: str, config: dict) -> str:
        payload = await self.unary(ReqMethod.SESSION_CREATE, principal, None, {
            **config, "create_token": token,
        })
        sid = payload.get("session_id")
        if not isinstance(sid, str) or not sid:
            raise AGUIError("session.create returned no session ID", "HOST_PROTOCOL_ERROR", 502)
        return sid

    async def history(self, principal: str, session_id: str) -> list[dict]:
        records = []
        page = 1
        while True:
            payload = await self.unary(ReqMethod.HISTORY_GET, principal, session_id,
                                       {"session_id": session_id, "page_idx": page})
            messages = payload.get("messages")
            total = payload.get("total_pages")
            if not isinstance(messages, list) or not isinstance(total, int) or total < page:
                raise AGUIError("invalid history response", "HOST_PROTOCOL_ERROR", 502)
            records.extend(messages)
            if page == total:
                return records
            page += 1

    async def stream(self, principal: str, session_id: str, request_id: str, params: dict):
        handler = getattr(self.channel, "ag_ui_message_handler", None)
        if isinstance(handler, dict):
            handler = handler.get("value")
        if handler is None or self.client is None:
            raise AGUIError("Gateway execution service is unavailable", "SERVICE_UNAVAILABLE", 503)
        envelope = e2a_from_agent_fields(
            request_id=request_id, channel_id="ag-ui", session_id=session_id,
            req_method=ReqMethod.CHAT_SEND, params=params, is_stream=True,
            timestamp=time.time(), user_id=principal,
        )
        async with aclosing(handler.stream_application_request(envelope)) as stream:
            async for chunk in stream:
                # The queue is keyed by envelope.request_id. A chunk's own
                # request_id can identify an interaction and must not replace it.
                yield chunk.payload or {}

    async def cancel(self, principal: str, session_id: str, request_id: str) -> bool:
        payload = await self.unary(ReqMethod.CHAT_CANCEL, principal, session_id, {
            "intent": "cancel", "target_request_id": request_id, "wait_for_stop": True,
        })
        return payload.get("success") is True and payload.get("target_request_id") == request_id
