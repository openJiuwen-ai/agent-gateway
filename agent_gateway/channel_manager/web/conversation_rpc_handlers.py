# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Chat and recommendation RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.routing.e2a_proxy import proxy_unary_request

def register_conversation_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register chat/history routes in their original registration order."""

    async def _chat_resume(ws, req_id, params, session_id):
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload={"accepted": True, "session_id": session_id},
        )

    async def _chat_interrupt(ws, req_id, params, session_id):
        intent = params.get("intent") if isinstance(params, dict) else None
        payload = {"accepted": True, "session_id": session_id}
        if isinstance(intent, str) and intent:
            payload["intent"] = intent
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _chat_user_answer(ws, req_id, params, session_id):
        payload = {"accepted": True, "session_id": session_id}
        request_id = params.get("request_id") if isinstance(params, dict) else None
        if isinstance(request_id, str) and request_id:
            payload["request_id"] = request_id
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _chat_swarmflow_reply(ws, req_id, params, session_id):
        # Empty-ack shell — standard 3-layer routing forwards the reply to the
        # agent adapter, which builds HumanAgentMessage and calls team_manager.
        await channel.send_response(
            ws, req_id, ok=True, payload={"accepted": True, "session_id": session_id}
        )

    async def _history_get(ws, req_id, params, session_id):
        payload = {"accepted": True, "session_id": session_id}
        if isinstance(params, dict):
            if "session_id" in params:
                payload["session_id"] = params.get("session_id")
            if "page_idx" in params:
                payload["page_idx"] = params.get("page_idx")
            if "cursor" in params:
                payload["cursor"] = params.get("cursor")
            if "limit" in params:
                payload["limit"] = params.get("limit")
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _forward_recommendation(ws, req_id, params, session_id, user_id=None, *, action: str):
        """Send a recommendation panel call to AgentServer and return its payload."""
        if (
            action == "prompt_suggestion_event"
            and isinstance(params, dict)
            and params.get("event_type") == "input_changed"
        ):
            target_session = params.get("session_id")
            if not isinstance(target_session, str) or not target_session.strip():
                await channel.send_response(
                    ws, req_id, ok=False, error="session_id is required", code="BAD_REQUEST",
                )
                return
        body = dict(params) if isinstance(params, dict) else {}
        if action == "control":
            # The panel verb (enable/disable/delete) shares the name ``action``.
            # Keep it before the RPC action overwrites that field.
            body["control_action"] = body.get("action")
        auth_session = getattr(ws, "_jiuwen_auth_session", "") or ""
        if auth_session:
            body["auth_session"] = auth_session
        body["action"] = action
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=body,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.RECOMMENDATION,
            label="recommendation",
        )

    async def _topic_starters_profile(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="profile")

    async def _topic_starters_restore_direction(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="restore_direction",
        )

    async def _topic_starters_control(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="control")

    async def _topic_starters_status(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="status")

    async def _topic_starters_select_model(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="select_model",
        )

    async def _topic_starters_get(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="get")

    async def _topic_starters_foreground(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="foreground",
        )

    async def _topic_starters_event(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="event")

    async def _prompt_suggestion_event(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="prompt_suggestion_event",
        )

    channel.register_method("chat.resume", _chat_resume)
    channel.register_method("chat.interrupt", _chat_interrupt)
    channel.register_method("chat.user_answer", _chat_user_answer)
    channel.register_method("chat.swarmflow_reply", _chat_swarmflow_reply)
    channel.register_method("history.get", _history_get)
    channel.register_method("topic_starters.profile", _topic_starters_profile)
    channel.register_method("topic_starters.restore_direction", _topic_starters_restore_direction)
    channel.register_method("topic_starters.control", _topic_starters_control)
    channel.register_method("topic_starters.status", _topic_starters_status)
    channel.register_method("topic_starters.select_model", _topic_starters_select_model)
    channel.register_method("topic_starters.get", _topic_starters_get)
    channel.register_method("topic_starters.foreground", _topic_starters_foreground)
    channel.register_method("topic_starters.event", _topic_starters_event)
    channel.register_method("prompt_suggestion.event", _prompt_suggestion_event)
