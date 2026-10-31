# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Interrupt dispatch extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.message import EventType, ReqMethod


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message

    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)


async def send_interrupt_to_agent(
    handler: Any,
    env: Any,
    *,
    channel_id: str | None = None,
    session_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Fire-and-forget: 发送中断到 AgentServer，不阻塞转发循环.

    ``interrupt_result`` 已由调用方提前推送；此处仍要把响应里的
    ``cancelled_tools`` 转成 ``chat.tool_result``，否则前端 tool 卡片会一直转圈，
    直到刷新历史才看到 ``[Interrupted]``.
    """
    try:
        resp = await handler._send_non_stream_agent_request(env)
        logger.info(
            "[MessageHandler] AgentServer 中断响应: request_id=%s ok=%s",
            resp.request_id, resp.ok,
        )
        payload = resp.payload if isinstance(resp.payload, dict) else {}
        ch = (channel_id or getattr(env, "channel", None) or "").strip()
        sid = (session_id or getattr(env, "session_id", None) or "").strip()
        if ch and sid and payload.get("cancelled_tools"):
            await handler._send_cancelled_tool_results(ch, sid, payload, metadata)
    except Exception as e:
        logger.warning("[MessageHandler] AgentServer 中断请求失败(忽略): %s", e)


async def send_interrupt_result_notification(
    handler: Any,
    request_id: str,
    channel_id: str,
    session_id: str | None,
    intent: str,
    message: str | None = None,
    success: bool = True,
    has_active_task: bool | None = None,
) -> None:
    """发送 interrupt_result 事件到前端（pause / resume 等）."""
    from openjiuwen_protocol.message import Message

    # 根据 has_active_task 调整 pause/resume 的消息
    success_messages_map = {
        "pause": "任务已暂停" if has_active_task else "任务已完成",
        "resume": "任务已恢复" if has_active_task else "任务已完成",
        "cancel": "任务已取消",
        "supplement": "任务已切换",
    }
    failure_messages_map = {
        "pause": "任务暂停失败",
        "resume": "任务恢复失败",
        "cancel": "任务终止失败",
        "supplement": "任务切换失败",
    }
    payload_dict: dict = {
        "event_type": "chat.interrupt_result",
        "intent": intent,
        "success": success,
        "message": message
        or (
            success_messages_map.get(intent, "任务已中断")
            if success
            else failure_messages_map.get(intent, "任务中断失败")
        ),
    }
    if has_active_task is not None:
        payload_dict["has_active_task"] = has_active_task
    notify_msg = Message(
        id=request_id,
        type="event",
        channel_id=channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        payload=payload_dict,
        event_type=EventType.CHAT_INTERRUPT_RESULT,
        metadata=None,
    )
    await handler.publish_robot_messages(notify_msg)
    logger.info(
        "[MessageHandler] 已发送 interrupt_result 通知: intent=%s request_id=%s",
        intent, request_id,
    )


async def send_cancelled_tool_results(
    handler: Any,
    channel_id: str,
    session_id: str,
    payload: dict,
    metadata: dict | None,
) -> None:
    """Send cancelled tool results to frontend from interrupt response payload."""
    cancelled_tools = payload.get("cancelled_tools", [])
    for tool_info in cancelled_tools:
        await handler.publish_robot_messages(
            handler._build_tool_result_message(
                channel_id, session_id, tool_info, metadata
            )
        )


async def handle_interrupt_dispatch(
    handler: "MessageHandler",
    msg: "Message",
) -> None:
    """CHAT_CANCEL 分支：supplement / cancel / pause / resume。"""
    params = msg.params if isinstance(msg.params, dict) else {}
    organization_stop = params.get("organization_owner_stop") is True
    organization_stop = organization_stop or params.get("organization_stop") is True
    if organization_stop or await handler._is_targeted_expert_cancel(msg):
        # Preserve Team routing and return the actual AgentServer result.
        agent_msg = await handler._prepare_agent_dispatch_message(msg)
        await handler._process_non_stream_request(msg, handler.message_to_e2a(agent_msg))
        return
    logger.info(
        "[MessageHandler] 收到中断请求: id=%s channel_id=%s",
        msg.id, msg.channel_id,
    )
    new_input = (msg.params or {}).get("new_input")
    has_new_input = isinstance(new_input, str) and new_input.strip()
    raw_attachments = (msg.params or {}).get("attachments")
    supplement_attachments = (
        raw_attachments if isinstance(raw_attachments, list) else None
    )
    intent = (msg.params or {}).get("intent", "cancel")

    if has_new_input:
        if handler._evolution_approval.should_queue_supplement(msg.session_id):
            queued_input = new_input.strip()
            handler._evolution_approval.queue_supplement(
                msg.session_id,
                queued_input,
                supplement_attachments,
            )
            logger.info(
                "[MessageHandler] evolution phase pending, queue supplement input: session_id=%s",
                msg.session_id,
            )
            await handler._send_interrupt_result_notification(
                msg.id,
                msg.channel_id,
                msg.session_id,
                "supplement",
                message="已加入队列，等待演进完成",
            )
            return

        # 有新输入：取消旧任务 → 保留 todo → 启动新任务（非并发）

        # 1. 取消 gateway 侧当前 session 相关的流式任务（而非所有任务）
        tasks_to_cancel = []
        rids_cancelled = []
        current_sid = msg.session_id
        await handler._cancel_non_stream_chat_tasks(current_sid)
        for rid, task in list(handler._stream_tasks.items()):
            # 只取消与当前 session_id 关联的任务
            if handler._stream_sessions.get(rid) != current_sid:
                continue
            if not task.done():
                logger.info(
                    "[MessageHandler] supplement: 取消流式任务 request_id=%s session_id=%s",
                    rid, current_sid,
                )
                task.cancel()
                tasks_to_cancel.append(task)
                rids_cancelled.append(rid)
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        # 2. 通知前端 supplement（前端据此判断 is_processing 状态）
        await handler._send_interrupt_result_notification(
            msg.id, msg.channel_id, msg.session_id, "supplement",
        )

        # 3. 发送 supplement intent 到 AgentServer（取消任务但保留 todo）
        #    用 await 确保 agent 侧先完成取消再启动新任务
        from openjiuwen_protocol.e2a.gateway_normalize import (
            e2a_from_agent_fields,
        )

        agent_msg = await handler._prepare_agent_dispatch_message(msg)
        source_params = msg.params if isinstance(msg.params, dict) else {}
        runtime_params: dict[str, Any] = {}
        for key in ("cwd", "trusted_dirs", "mode"):
            value = source_params.get(key)
            if value:
                runtime_params[key] = value
        if "cwd" not in runtime_params and isinstance(msg.metadata, dict):
            metadata_cwd = msg.metadata.get("cwd")
            if isinstance(metadata_cwd, str) and metadata_cwd.strip():
                runtime_params["cwd"] = metadata_cwd.strip()
        # 注入 mode 信息，确保 AgentServer 找到正确的 agent
        sup_state = handler._channel_states.get(
            handler._get_channel_state_key(msg.channel_id, msg.session_id)
        ) or handler._channel_states.get(msg.channel_id)
        if "mode" not in runtime_params and sup_state is not None:
            runtime_params["mode"] = (
                sup_state.mode.value
                if hasattr(sup_state.mode, 'value')
                else str(sup_state.mode)
            )
        supplement_params = {
            "intent": "supplement",
            "new_input": new_input.strip(),
            "session_id": agent_msg.session_id,
            **runtime_params,
        }
        supplement_env = e2a_from_agent_fields(
            request_id=f"supplement_{int(time.time() * 1000):x}",
            channel_id=msg.channel_id,
            session_id=agent_msg.session_id,
            req_method=ReqMethod.CHAT_CANCEL,
            params=supplement_params,
            is_stream=False,
            timestamp=time.time(),
            metadata=msg.metadata,
            user_id=getattr(msg, "user_id", None),
        )
        try:
            resp = await handler._send_non_stream_agent_request(supplement_env)
            # 发送被中断工具的 tool_result 给前端
            payload = resp.payload if isinstance(resp.payload, dict) else {}
            await handler._send_cancelled_tool_results(
                msg.channel_id, msg.session_id, payload, msg.metadata
            )
        except Exception:
            pass  # 即使失败也继续启动新任务

        # 4. 入队新任务（单一任务，不并发）
        from openjiuwen_protocol.message import Message

        new_req_id = f"req_{int(time.time() * 1000):x}_{msg.id}"
        sup_meta = dict(msg.metadata) if msg.metadata else None
        original_request = handler._get_session_last_user_query(msg.session_id)
        new_msg = Message(
            id=new_req_id,
            type="req",
            channel_id=msg.channel_id,
            session_id=msg.session_id,
            params={
                "query": handler._build_supplement_continuation_query(
                    new_input,
                    original_request,
                ),
                "supplement_input": new_input.strip(),
                "original_request": original_request,
                "session_id": msg.session_id,
                "is_supplement": True,
                **runtime_params,
                **(
                    {"model_name": (msg.params or {}).get("model_name")}
                    if (msg.params or {}).get("model_name")
                    else {}
                ),
                **(
                    {"attachments": supplement_attachments}
                    if supplement_attachments
                    else {}
                ),
            },
            timestamp=time.time(),
            ok=True,
            req_method=ReqMethod.CHAT_SEND,
            is_stream=True,
            provider=msg.provider,
            chat_id=msg.chat_id,
            user_id=msg.user_id,
            bot_id=msg.bot_id,
            metadata=sup_meta,
        )
        handler._user_messages.put_nowait(new_msg)
        logger.info(
            "[MessageHandler] supplement: 旧任务已取消，新任务已入队: id=%s session_id=%s",
            new_msg.id, msg.session_id,
        )

    elif intent == "cancel":
        # fire_and_forget：避免慢 cancel 阻塞 _forward_loop，
        # 导致后续 session.create 等请求在队列中等待、前端超时。
        await handler._cancel_agent_work_for_session(
            msg,
            msg.session_id,
            agent_notify="fire_and_forget",
        )

    elif intent in ("pause", "resume"):
        # 暂停/恢复：不取消流式任务，转发给 AgentServer 处理 ReAct 循环
        agent_msg = await handler._prepare_agent_dispatch_message(msg)
        # 确保 mode 信息存在，否则从 channel_states 注入
        if isinstance(agent_msg.params, dict) and not agent_msg.params.get("mode"):
            pr_state = handler._channel_states.get(
                handler._get_channel_state_key(msg.channel_id, msg.session_id)
            ) or handler._channel_states.get(msg.channel_id)
            if pr_state is not None:
                agent_msg.params["mode"] = (
                    pr_state.mode.value
                    if hasattr(pr_state.mode, 'value')
                    else str(pr_state.mode)
                )
        env_interrupt = handler.message_to_e2a(agent_msg)
        asyncio.create_task(handler._send_interrupt_to_agent(env_interrupt))
        # 检查当前 session 是否有活跃的流式任务
        current_sid = msg.session_id
        has_active_task = False
        for rid, task in list(handler._stream_tasks.items()):
            if handler._stream_sessions.get(rid) == current_sid and not task.done():
                has_active_task = True
                break
        # 通知前端状态变更（事件）
        await handler._send_interrupt_result_notification(
            msg.id, msg.channel_id, msg.session_id, intent,
            has_active_task=has_active_task,
        )
