# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""AgentServer push dispatch extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.e2a.constants import (
    E2A_WIRE_INTERNAL_METADATA_KEYS,
)
from openjiuwen_protocol.message import (
    PROMPT_SUGGESTION_READY_EVENT,
    SESSION_REQUEST_COMPLETED_EVENT,
    TOPIC_STARTERS_CONTROL_CHANGED_EVENT,
    TOPIC_STARTERS_READY_EVENT,
)

from agent_gateway.message_handler.agent_events import merge_agent_metadata
from agent_gateway.message_handler.wire_messages import chunk_to_message
from agent_gateway.routing.agent_request_timeout import (
    send_agent_request_with_timeout,
)


if TYPE_CHECKING:
    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)

_DIAGNOSIS_PUSH_EVENTS = frozenset({"trajectory.diagnosis.progress", "trajectory.diagnosis.status"})
_ACP_CHANNEL_ID = "acp"


async def handle_agent_server_push(
    handler: "MessageHandler",
    wire: dict[str, Any],
) -> None:
    """Serialize server-push handling so a final cannot overtake its deltas."""
    lock = getattr(handler, "_server_push_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        handler._server_push_lock = lock
    async with lock:
        await handle_agent_server_push_ordered(handler, wire)


async def handle_agent_server_push_ordered(
    handler: "MessageHandler",
    wire: dict[str, Any],
) -> None:
    """AgentServer ``send_push`` 下行：与 RPC 共用连接但不得占用 unary/stream 等待队列。"""
    from openjiuwen_protocol.e2a.wire_codec import parse_agent_server_wire_chunk

    try:
        chunk = parse_agent_server_wire_chunk(wire)
    except Exception as e:
        logger.exception("[MessageHandler] server_push 解析失败: %s", e)
        return
    rid = str(chunk.request_id or "")
    if await handler._handle_login_credential_refresh_push(chunk):
        return
    sid_raw = wire.get("session_id")
    if sid_raw is not None and str(sid_raw).strip():
        session_id: str | None = str(sid_raw)
    else:
        session_id = handler._stream_sessions.get(rid)

    from agent_gateway.extensions.video_duplex.backend.tasks.bridge import (
        EVENT,
        handle_checkpoint_push,
    )

    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") == "models.updated":
        await handler._broadcast_models_updated()
        return
    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") == EVENT:
        await handle_checkpoint_push(handler.agent_client, chunk, session_id)
        return
    if await handle_trajectory_update_push(handler, chunk, session_id):
        return
    if await handle_diagnosis_push(handler, chunk, session_id):
        return

    request_metadata = handler._stream_metadata.get(rid)
    wmd = wire.get("metadata")
    if isinstance(wmd, dict):
        resp_md = {
            key: value
            for key, value in wmd.items()
            if key not in E2A_WIRE_INTERNAL_METADATA_KEYS
        }
    else:
        resp_md = None

    bus_metadata = merge_agent_metadata(request_metadata, resp_md)

    if chunk.channel_id == _ACP_CHANNEL_ID:
        session_id = handler._resolve_acp_external_session_id(session_id, bus_metadata)
    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") == SESSION_REQUEST_COMPLETED_EVENT:
        pending_session_id = handler._awaiting_host_completion.pop(rid, None)
        if pending_session_id is not None:
            handler._call_chat_completed(pending_session_id, rid)
        return
    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") in {
        TOPIC_STARTERS_READY_EVENT,
        TOPIC_STARTERS_CONTROL_CHANGED_EVENT,
    }:
        if handler.on_recommendation_event is not None:
            body = {
                key: value
                for key, value in chunk.payload.items()
                if key != "event_type"
            }
            await handler.on_recommendation_event(str(chunk.payload.get("event_type")), body)
        return
    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") == PROMPT_SUGGESTION_READY_EVENT:
        if session_id and handler.on_prompt_suggestion is not None:
            await handler.on_prompt_suggestion(session_id, rid, dict(chunk.payload))
        return
    if isinstance(chunk.payload, dict) and chunk.payload.get("event_type") == "cron.response":
        mapped_owner = str(handler._stream_user_ids.get(rid) or "").strip()
        wire_metadata = wire.get("metadata")
        pushed_owner = (
            str(wire_metadata.get("_jiuwenswarm_cron_owner_user_id") or "").strip()
            if isinstance(wire_metadata, dict)
            else ""
        )
        if mapped_owner and pushed_owner and mapped_owner != pushed_owner:
            logger.warning(
                "[MessageHandler] discard cron push with mismatched owner: request_id=%s",
                rid,
            )
            return
        await handle_cron_push_payload(
            handler,
            payload=dict(chunk.payload),
            request_id=rid,
            channel_id=chunk.channel_id,
            session_id=session_id,
            metadata=bus_metadata,
            user_id=mapped_owner or pushed_owner or None,
        )
        return
    if handler._is_terminal_stream_chunk(chunk):
        task = handler._stream_tasks.get(rid)
        if task is not None and not task.done():
            logger.info(
                "[MessageHandler] server_push 终止 chunk → 取消流式 Task: request_id=%s",
                rid,
            )
            task.cancel()
        else:
            logger.debug(
                "[MessageHandler] server_push 终止 chunk（无活跃 Task）: request_id=%s",
                rid,
            )
        return

    if not await handle_evolution_chunk(handler, chunk, session_id, bus_metadata):
        return

    _push_app_id = (
        handler._stream_app_ids.get(rid, "")
        or (request_metadata.get("app_id") if isinstance(request_metadata, dict) else "")
        or (resp_md.get("app_id") if isinstance(resp_md, dict) else "")
    )
    out = chunk_to_message(
        chunk, session_id=session_id, metadata=bus_metadata,
        app_id=_push_app_id,
    )
    await handler.publish_robot_messages(out)
    logger.info(
        "[MessageHandler] server_push 已写入 robot_messages: request_id=%s channel_id=%s app_id=%s",
        rid,
        chunk.channel_id,
        _push_app_id,
    )


async def handle_trajectory_update_push(
    handler: "MessageHandler",
    chunk: Any,
    session_id: str | None,
) -> bool:
    """Forward one AgentServer trajectory watermark to the browser channel."""
    payload = chunk.payload
    if not isinstance(payload, dict) or payload.get("event_type") != "trace.updated":
        return False
    resolved_session_id = str(payload.get("session_id") or session_id or "").strip()
    trace_id = str(payload.get("trace_id") or "").strip()
    if not resolved_session_id or not trace_id:
        logger.warning("[MessageHandler] invalid trajectory update push ignored")
        return True
    try:
        revision = int(payload.get("revision", 0))
    except (TypeError, ValueError, OverflowError):
        logger.warning("[MessageHandler] invalid trajectory update revision ignored")
        return True
    if revision < 1:
        logger.warning("[MessageHandler] non-positive trajectory update revision ignored")
        return True
    web_channel = handler._resolve_web_channel()
    if web_channel is None:
        logger.debug(
            "[MessageHandler] trajectory update skipped because web channel is unavailable"
        )
        return True
    from agent_gateway.channel_manager.web.trace_update import CommittedTraceUpdate

    try:
        frame_seq = int(payload.get("frame_seq", 0))
    except (TypeError, ValueError, OverflowError):
        logger.warning("[MessageHandler] invalid trajectory frame_seq ignored")
        frame_seq = 0
    web_channel.schedule_trajectory_updates(
        (
            CommittedTraceUpdate(
                session_id=resolved_session_id,
                trace_id=trace_id,
                revision=revision,
                store_epoch=payload.get("store_epoch"),
                lifecycle=str(payload.get("lifecycle") or "final"),
                frame_seq=max(0, frame_seq),
            ),
        )
    )
    return True


async def handle_diagnosis_push(
    handler: "MessageHandler",
    chunk: Any,
    session_id: str | None,
) -> bool:
    """Forward one AgentServer diagnosis event to browser connections."""
    payload = chunk.payload
    if not isinstance(payload, dict):
        return False
    event = payload.get("event_type")
    if event not in _DIAGNOSIS_PUSH_EVENTS:
        return False
    resolved_session_id = str(payload.get("session_id") or session_id or "").strip()
    if not resolved_session_id:
        logger.warning("[MessageHandler] diagnosis push without session_id ignored")
        return True
    web_channel = handler._resolve_web_channel()
    if web_channel is None:
        logger.debug("[MessageHandler] diagnosis push skipped because web channel is unavailable")
        return True
    body = {key: value for key, value in payload.items() if key != "event_type"}
    try:
        await web_channel.send_session_event(resolved_session_id, str(event), body)
    except Exception:  # noqa: BLE001
        logger.warning("[MessageHandler] diagnosis push forward failed", exc_info=True)
    return True


async def handle_cron_push_payload(
    handler: "MessageHandler",
    *,
    payload: dict[str, Any],
    request_id: str,
    channel_id: str,
    session_id: str | None,
    metadata: dict[str, Any] | None,
    user_id: str | None = None,
) -> None:
    cc = handler._cron_controller
    if cc is None:
        return
    action = str(payload.get("action") or "").strip()
    command_id = str(payload.get("command_id") or "").strip()
    params = payload.get("data") or {}
    if not isinstance(params, dict):
        params = {}
    request_metadata = handler._stream_metadata.get(request_id)
    if not isinstance(request_metadata, dict):
        original_message = getattr(
            handler, "_non_stream_chat_messages", {}
        ).get(request_id)
        request_metadata = getattr(original_message, "metadata", None)
    auth_session = (
        str(request_metadata.get("auth_session") or "").strip()
        if isinstance(request_metadata, dict)
        else ""
    )
    owner_user_id = str(user_id or "").strip()

    async def _get_owned_job(job_id: str) -> dict[str, Any] | None:
        job = await cc.get_job(job_id)
        if job is None:
            return None
        job_owner = (
            job.get("user_id", "")
            if isinstance(job, dict)
            else getattr(job, "user_id", "")
        )
        if owner_user_id and str(job_owner or "").strip() != owner_user_id:
            return None
        return job

    try:
        if action == "list":
            data = await cc.list_jobs(user_id=owner_user_id or None)
        elif action == "get":
            job_id = str(params.get("job_id") or "")
            if params.get("authoritative") is True:
                if not isinstance(params.get("job_id"), str) or not job_id.strip():
                    raise ValueError("authoritative cron query requires a nonempty job_id")
                data = await cc.get_job(job_id)
                if data is not None:
                    if (
                        not isinstance(data, dict)
                        or data.get("id") != job_id.strip()
                        or not isinstance(data.get("user_id", ""), str)
                    ):
                        raise ValueError("invalid authoritative cron job")
                    job_owner = data.get("user_id", "")
                    if str(job_owner or "").strip() != owner_user_id:
                        data = {"status": "forbidden", "code": "FORBIDDEN"}
                    else:
                        data = {"status": "found", "job": data}
                else:
                    data = {"status": "missing"}
            else:
                data = await _get_owned_job(job_id)
                if data is None:
                    raise KeyError("job not found")
        elif action == "create":
            params = dict(params)
            params.pop("_auth_session", None)
            if auth_session:
                params["_auth_session"] = auth_session
            if owner_user_id:
                params["user_id"] = owner_user_id
            request_mode = handler._stream_modes.get(request_id)
            if request_mode:
                params["mode"] = request_mode
            params["_agentos_project_binding_verified"] = True
            data = await cc.create_job(params)
        elif action == "update":
            job_id = str(params.get("job_id") or "")
            if await _get_owned_job(job_id) is None:
                raise KeyError("job not found")
            patch = dict(params.get("patch") or {})
            patch.pop("_auth_session", None)
            if auth_session:
                patch["_auth_session"] = auth_session
            patch["_agentos_project_binding_verified"] = True
            data = await cc.update_job(job_id, patch)
        elif action == "delete":
            job_id = str(params.get("job_id") or "")
            if await _get_owned_job(job_id) is None:
                raise KeyError("job not found")
            data = {"deleted": await cc.delete_job(job_id)}
        elif action == "toggle":
            job_id = str(params.get("job_id") or "")
            if await _get_owned_job(job_id) is None:
                raise KeyError("job not found")
            data = await cc.toggle_job(job_id, bool(params.get("enabled")))
        elif action == "preview":
            job_id = str(params.get("job_id") or "")
            if await _get_owned_job(job_id) is None:
                raise KeyError("job not found")
            data = await cc.preview_job(job_id, int(params.get("count", 5)))
        elif action == "run_now":
            job_id = str(params.get("job_id") or "")
            if await _get_owned_job(job_id) is None:
                raise KeyError("job not found")
            data = {"run_id": await cc.run_now(job_id)}
            await push_cron_run_now_ack(
                handler,
                request_id=request_id,
                job_id=str(params.get("job_id") or ""),
                run_id=str(data.get("run_id") or ""),
                user_id=owner_user_id or None,
            )
        else:
            data = {"error": f"unknown cron action: {action}"}
    except Exception as exc:  # noqa: BLE001
        data = {"error": str(exc)}

    if command_id:
        await push_cron_command_ack(
            handler,
            command_id=command_id,
            data=data,
            user_id=owner_user_id or None,
        )

    if not channel_id:
        return

    from openjiuwen_protocol.message import EventType, Message
    out = Message(
        id=request_id,
        type="event",
        channel_id=channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        payload={
            "event_type": "chat.tool_result",
            "tool_name": "cron",
            "result": data,
        },
        event_type=EventType.CHAT_TOOL_RESULT,
        metadata=metadata,
        enable_streaming=False,
    )
    await handler.publish_robot_messages(out)


async def push_cron_command_ack(
    handler: "MessageHandler",
    *,
    command_id: str,
    data: Any,
    user_id: str | None = None,
) -> None:
    """Return a Gateway cron command result to the waiting Agent tool."""
    try:
        from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
        from openjiuwen_protocol.message import ReqMethod

        env = e2a_from_agent_fields(
            request_id=f"cron-command-ack-{command_id}", channel_id="",
            req_method=ReqMethod.CRON_COMMAND_ACK,
            params={"command_id": command_id, "data": data},
            is_stream=False, timestamp=time.time(), user_id=user_id,
        )
        await send_agent_request_with_timeout(
            handler.agent_client, env, label="cron.command.ack"
        )
    except Exception:
        logger.warning("[Cron] command ack delivery failed: command_id=%s", command_id, exc_info=True)


async def push_cron_run_now_ack(
    handler: "MessageHandler",
    *,
    request_id: str,
    job_id: str,
    run_id: str,
    user_id: str | None = None,
) -> None:
    """把 Gateway 生成的 run_id 经 E2A 回传目标 AgentServer（P2）。"""
    if not request_id:
        return
    try:
        from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
        from openjiuwen_protocol.message import ReqMethod

        env = e2a_from_agent_fields(
            request_id=f"cron-run-ack-{request_id}",
            channel_id="",
            req_method=ReqMethod.CRON_RUN_NOW_ACK,
            params={
                "ack_request_id": request_id,
                "job_id": job_id,
                "run_id": run_id,
            },
            is_stream=False,
            timestamp=time.time(),
            user_id=user_id,
        )
        await send_agent_request_with_timeout(
            handler.agent_client,
            env,
            label="cron.run_now.ack",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[Cron] run_now ack push failed: request_id=%s error=%s", request_id, exc
        )
