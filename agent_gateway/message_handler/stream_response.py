# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Streaming response consumption extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from agent_gateway.runtime.session_input import resolve_session_input_mode


if TYPE_CHECKING:
    from openjiuwen_protocol.e2a.models import E2AEnvelope
    from openjiuwen_protocol.message import Message

    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)


async def handle_process_stream(
    handler: "MessageHandler",
    env: "E2AEnvelope",
    session_id: str | None,
    request_metadata: dict[str, Any] | None,
    *,
    emit_processing_status: bool = True,
) -> None:
    """处理流式请求，逐个 chunk 写入 robot_messages."""
    rid = env.request_id or ""
    channel_id = env.channel or ""
    stream_app_id = handler._stream_app_ids.get(rid, "")
    stream_route = handler._expert_stream_route(env.params)
    cancelled = False
    completion_notified = False
    has_processing_status_false = False
    _proc_count = 0
    logger.info(
        "[MessageHandler] process_stream started: request_id=%s channel=%s session_id=%s",
        rid, channel_id, session_id,
        extra={"session_id": session_id} if session_id else {},
    )
    try:
        await handler._sync_agentos_cron_jobs(env)
        await handler._apply_session_login_owner(env)
        async for chunk in handler.agent_client.send_request_stream(env):
            _proc_count += 1
            if _proc_count <= 3:
                _pl = getattr(chunk, "payload", None) or {}
                _et = _pl.get("event_type", "") if isinstance(_pl, dict) else ""
                logger.info(
                    "[MessageHandler] process_stream chunk #%s: request_id=%s event_type=%s",
                    _proc_count, rid, _et,
                )
            if handler._is_terminal_stream_chunk(chunk):
                continue
            payload = chunk.payload or {}
            if isinstance(payload, dict):
                event_type = payload.get("event_type")
                if event_type in ("chat.processing_status", "chat.error"):
                    payload = {"request_id": chunk.request_id, **payload}
                    payload_team = payload.get("team_id") or payload.get("team_name")
                    if stream_route and not payload_team:
                        payload = {**payload, **stream_route}
                    chunk.payload = payload
            owns_status = (
                isinstance(payload, dict)
                and str(payload.get("request_id") or chunk.request_id or "") == rid
                and (
                    str(payload.get("team_id") or payload.get("team_name") or "").strip()
                    == stream_route["team_id"]
                    if stream_route else payload.get("source") != "org_expert_direct"
                )
            )
            is_processing_false = (
                isinstance(payload, dict)
                and payload.get("event_type") == "chat.processing_status"
                and payload.get("is_processing") is False
            )
            ends_owner_stream = owns_status and not stream_route
            if is_processing_false and ends_owner_stream:
                if handler._session_has_streams_blocking_processing_false(
                    session_id,
                    exclude_rid=rid,
                ):
                    # 这是单个请求的结束；同 session 仍有其他对话流时不能向前端
                    # 宣告整个会话空闲；保留自动收尾，让最后一个流退出时发送权威 false。
                    continue
            published = await handler.publish_stream_chunk(
                chunk,
                session_id=session_id,
                request_metadata=request_metadata,
            )
            if not published:
                continue
            if isinstance(payload, dict):
                event_type = payload.get("event_type")
                if event_type == "chat.processing_status":
                    if payload.get("is_processing") is False and owns_status:
                        has_processing_status_false = True
                elif event_type == "chat.processing_status_deferred":
                    # Internal placeholder from the cluster-mode
                    # follow-up short stream: the real round-complete
                    # signal will be broadcast by the background team
                    # stream on team.completed. This marker only
                    # prevents the Gateway from auto-emitting
                    # is_processing=False when this short stream
                    # ends, and is NOT forwarded to the frontend.
                    if owns_status:
                        has_processing_status_false = True
                    continue

            _has_fanout = bool((getattr(chunk, "metadata", None) or {}).get("fan_out_targets"))
            logger.debug(
                "[MessageHandler] Stream chunk 已写入 robot_messages:"
                " request_id=%s event_type=%s has_fanout=%s",
                chunk.request_id,
                payload.get("event_type") if isinstance(payload, dict) else None,
                _has_fanout,
            )
        handler._notify_chat_completed(env, session_id)
        completion_notified = True
        logger.info(
            "[MessageHandler] Stream 正常完成: request_id=%s total_chunks=%s",
            rid, _proc_count,
        )
    except asyncio.CancelledError:
        cancelled = True
        logger.info(
            "[MessageHandler] Stream 被取消: request_id=%s total_chunks=%s",
            rid, _proc_count,
        )
    # Background stream failures must reach the client before per-request cleanup.
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if resolve_session_input_mode(env.params) is not None:
            from openjiuwen_protocol.message import Message, ReqMethod

            request_message = Message(
                id=rid, type="req", channel_id=channel_id, session_id=session_id,
                params=env.params, timestamp=time.time(), ok=True,
                req_method=ReqMethod.CHAT_SEND, metadata=request_metadata,
            )
            await handler.publish_robot_messages(
                handler._build_error_out_message(request_message, exc)
            )
            return
        logger.exception(
            "[MessageHandler] Stream 异常: request_id=%s total_chunks=%s error=%s",
            rid, _proc_count, exc,
        )
        await handler._publish_stream_error(
            env, session_id, request_metadata, exc,
        )
    finally:
        if (
            not cancelled
            and handler._is_interrupt_evolution_approval_answer_payload(env.params)
        ):
            from openjiuwen_protocol.message import Message, ReqMethod

            await handler._complete_evolution_approval_if_current(
                Message(
                    id=rid,
                    type="req",
                    channel_id=channel_id,
                    session_id=session_id,
                    params=dict(env.params or {}),
                    timestamp=time.time(),
                    ok=True,
                    req_method=ReqMethod.CHAT_SEND,
                    is_stream=True,
                    metadata=request_metadata,
                ),
                str((env.params or {}).get("request_id") or ""),
            )
            has_processing_status_false = True
        # 完成边界在 pop 之前通知，与正常结束路径一致：补货守卫要看到 rid 仍在
        # 活跃集里，才分得清这次请求是已经写完成行，还是被取消后宿主仍在收尾。
        if not completion_notified:
            handler._notify_chat_completed(env, session_id, before_host_answer=True)
        # 清理状态：pop tracking 后必须广播 task.global_running 最新快照。
        # 若只裸 pop（不广播），非 chat.send 流（history.get / chat.error 等
        # emit=False 路径）退出时会让 has_active_streams() 计数悬空，前端保存锁
        # 卡在最后一次广播值（通常为 True），仅靠重连自愈。
        await handler._pop_stream_tracking_and_broadcast([rid])
        if session_id is not None and session_id not in handler._stream_sessions.values():
            # Fallback cleanup when stream exits unexpectedly without evolution end signal.
            handler._evolution_approval.clear_session_in_progress(session_id)
        # 标注本 rid 退出后是否还会触发 task.global_running 广播。
        # 若以下分支均不进入 _send_processing_status（其内部会广播），
        # 则前端保存锁可能卡在最后一次广播值，仅靠重连自愈。
        # cancelled 也会补发 is_processing=false（见下方），故不再排除 cancelled。
        # Block false only while chat.send or command.goal remains — not
        # arbitrary emit=False streams (e.g. history.get). Otherwise a short
        # permission-resume chat.send would clear is_processing while Goal
        # is still running; but history.get must not strand the spinner.
        session_blocks_processing_false = (
            not stream_route and handler._session_has_streams_blocking_processing_false(session_id)
        )
        _will_broadcast = (
            emit_processing_status
            and not has_processing_status_false
            and not session_blocks_processing_false
        )
        logger.info(
            "[task.global_running] stream 退出: rid=%s cancelled=%s emit=%s "
            "has_status_false=%s remaining_stream=%d remaining_chat=%d "
            "will_broadcast_processing_status=%s",
            rid, cancelled, emit_processing_status, has_processing_status_false,
            len(handler._stream_modes), len(handler._active_chat_tasks),
            _will_broadcast,
        )
        logger.debug(
            "[MessageHandler] Stream 任务状态已清理: request_id=%s",
            rid,
        )
        # 该 session 流式任务结束后，通知前端处理完成（is_processing=false）。
        # 只有当 AgentServer 没有发送过 processing_status=false 时才补发。
        # 注意：这里不再排除 cancelled 分支。stream 被 esc 取消（cancelled=True）时，
        # AgentServer 的 interrupt_result 可能要等很久（如命中 agent 首次初始化的同步阻塞窗口，
        # 事件循环读不出 cancel 消息），此时前端会一直转圈。主动补发一次 is_processing=false，
        # 让 TUI 立刻停转圈，不等 AgentServer 回 interrupt_result。
        # 守卫：同 session 还有 chat.send 或 command.goal 时不误发 false
        # （避免权限同意短流在 Goal 仍跑时清前端；history.get 不挡）。
        if emit_processing_status and not has_processing_status_false:
            if not session_blocks_processing_false:
                await handler._send_processing_status(
                    rid, session_id, channel_id,
                    is_processing=False, app_id=stream_app_id,
                    team_id=stream_route.get("team_id", ""),
                )
                logger.info(
                    "[MessageHandler] 该 session 流式任务已结束（cancelled=%s），已发送 is_processing=false: session_id=%s",
                    cancelled,
                    session_id,
                )
