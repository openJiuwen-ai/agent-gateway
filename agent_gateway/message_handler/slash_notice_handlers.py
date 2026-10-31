# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Controlled-channel slash notices extracted from MessageHandler."""

from __future__ import annotations

import logging
import secrets
import time
from typing import TYPE_CHECKING, Any

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import Message, ReqMethod

from agent_gateway.message_handler.command_parser.slash_command import (
    format_skills_list_for_notice,
)


if TYPE_CHECKING:
    from agent_gateway.message_handler.message_handler import MessageHandler


logger = logging.getLogger(__name__)


async def send_skills_slash_notice(
    handler: "MessageHandler",
    user_infos: dict[str, Any],
    channel_id: str,
    reply_session_id: str | None,
    msg: "Message",
) -> None:
    """受控通道整行 /skills list：请求 skills.list 并以 CHAT_FINAL 通知透传。

    skills.list 响应载荷形如 ``{"skills": [...]}`` / ``{"error": "..."}``，
    不含 ``content`` 字段。多数 IM 通道（微信/钉钉/企微/WhatsApp 等）的 ``send``
    仅从 ``payload.content`` / ``params.content`` 取文本，缺 ``content`` 即被当作
    空消息丢弃，导致 /skills list 无返回（/skills 本身不经此分支故不受影响）。
    因此这里用 ``format_skills_list_for_notice`` 把载荷渲染成纯文本放入 ``content``，
    同时保留原始字段：飞书仍可经 ``_build_skills_list_card_content`` 识别 ``skills``
    键渲染为卡片，其它 IM 通道则回退到读取 ``content`` 文本。
    """
    req_id = f"skills_slash_{int(time.time() * 1000):x}_{secrets.token_hex(3)}"
    skills_req = Message(
        id=req_id,
        type="req",
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        req_method=ReqMethod.SKILLS_LIST,
        is_stream=False,
        metadata=msg.metadata,
        provider=getattr(msg, "provider", None),
        chat_id=getattr(msg, "chat_id", None),
        user_id=getattr(msg, "user_id", None),
        bot_id=getattr(msg, "bot_id", None),
    )
    try:
        env = handler.message_to_e2a(skills_req)
        resp = await handler._send_non_stream_agent_request(env)
        if resp.ok:
            if isinstance(resp.payload, dict):
                notice_payload: dict[str, Any] = dict(resp.payload)
            else:
                notice_payload = {"data": resp.payload}
        else:
            err = ""
            if isinstance(resp.payload, dict):
                err = str(resp.payload.get("error") or "").strip()
            notice_payload = {
                "error": f"获取技能列表失败{(': ' + err) if err else ''}",
            }
        # 渲染纯文本 content，供只读 content 的 IM 通道（微信等）下发。
        notice_payload["content"] = format_skills_list_for_notice(
            notice_payload if isinstance(notice_payload, dict) else None
        )
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id, notice_payload
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[MessageHandler] /skills list 请求失败: %s", exc)
        await handler.send_channel_notice(
            user_infos,
            channel_id,
            reply_session_id,
            {"content": f"获取技能列表失败：{exc}", "error": f"获取技能列表失败：{exc}"},
        )


async def send_branch_slash_notice(
    handler: "MessageHandler",
    user_infos: dict[str, Any],
    channel_id: str,
    reply_session_id: str | None,
    msg: "Message",
    *,
    branch_name: str = "",
) -> None:
    """受控通道 /branch：分叉当前会话，切换到新 session 并通知。

    Sends session.fork to AgentServer so both filesystem copy (history.json,
    metadata) and in-memory context copy (DeepAgent checkpointer + context
    engine) are performed atomically.
    """
    state = handler.get_or_create_channel_state(msg)
    source_sid = state.session_id
    if not source_sid:
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": "当前无活跃会话，无法分叉"},
        )
        return

    channel_type = handler._resolve_control_channel_type(msg)

    try:
        env = e2a_from_agent_fields(
            request_id=f"branch-{int(time.time() * 1000):x}-{secrets.token_hex(3)}",
            channel_id=channel_type,
            session_id=source_sid,
            req_method=ReqMethod.SESSION_FORK,
            params={
                "source_session_id": source_sid,
                "title": branch_name,
            },
            is_stream=False,
            timestamp=time.time(),
        )
        resp = await handler._send_non_stream_agent_request(env)
        if not resp.ok:
            payload = dict(resp.payload or {}) if isinstance(resp.payload, dict) else {}
            raise ValueError(str(payload.get("error") or "session.fork failed"))

        result = dict(resp.payload or {}) if isinstance(resp.payload, dict) else {}
        fork_sid = str(result.get("session_id") or "").strip()
        if not fork_sid:
            raise ValueError("session.fork returned empty session_id")
        fork_title = result.get("title", branch_name or "Branched conversation")

        old_sid = state.session_id
        state.session_id = fork_sid

        await handler._cancel_agent_work_for_session(msg, old_sid)

        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            f"[收到 /branch 指令] 已分叉会话「{fork_title}」，当前已切换到新会话。",
        )
        logger.info(
            "[MessageHandler] /branch 完成: source=%s fork=%s title=%s",
            source_sid, fork_sid, fork_title,
        )
    except ValueError as e:
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": f"分叉失败：{e}"},
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[MessageHandler] /branch 失败: %s", exc)
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": f"分叉失败：{exc}"},
        )


async def send_rewind_slash_confirm_prompt(
    handler: "MessageHandler",
    user_infos: dict[str, Any],
    channel_id: str,
    reply_session_id: str | None,
    msg: "Message",
    *,
    turn_index: int = 1,
) -> None:
    """受控通道 /rewind N：发送确认提示（两步确认第一步）。

    IM 渠道 /rewind 是不可逆操作，需要确认后才执行。
    """
    state = handler.get_or_create_channel_state(msg)
    target_sid = state.session_id
    if not target_sid:
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": "当前无活跃会话，无法回退"},
        )
        return

    await handler.send_channel_notice(
        user_infos, channel_id, reply_session_id,
        f"[收到 /rewind {turn_index} 指令] 确认要回退到第 {turn_index} 轮吗？\n"
        f"此操作不可逆，将删除第 {turn_index} 轮及之后的所有对话。\n"
        f"请回复 /rewind confirm {turn_index} 确认，或 /rewind cancel 取消。\n"
        f"注意：回退不影响手动编辑的文件或通过 bash 执行的命令。",
    )


async def send_rewind_slash_notice(
    handler: "MessageHandler",
    user_infos: dict[str, Any],
    channel_id: str,
    reply_session_id: str | None,
    msg: "Message",
    *,
    turn_index: int = 1,
) -> None:
    """受控通道 /rewind N：回退当前会话到指定轮次并通知。

    转发到 AgentServer（原子性截断 history + context + checkpointer）；
    E2A 不可达时直接返回错误，不回退 Gateway 本地目录
    （方案 §8：禁止用部署侧目录代替用户目录）。
    """
    state = handler.get_or_create_channel_state(msg)
    target_sid = state.session_id
    if not target_sid:
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": "当前无活跃会话，无法回退"},
        )
        return

    try:
        await handler._cancel_agent_work_for_session(msg, target_sid)

        # --- E2A-first: 转发到 AgentServer 原子性处理 ---
        context_ok = False
        try:
            env = e2a_from_agent_fields(
                request_id=f"rewind-{int(time.time() * 1000):x}-{secrets.token_hex(3)}",
                channel_id=channel_id,
                session_id=target_sid,
                req_method=ReqMethod.SESSION_REWIND,
                params={"session_id": target_sid, "turn_index": turn_index},
                is_stream=False,
                timestamp=time.time(),
            )
            resp = await handler._send_non_stream_agent_request(env)
            if resp.ok:
                pl = resp.payload if isinstance(resp.payload, dict) else {}
                preview = pl.get("content_preview", "")
                remaining = pl.get("remaining_records", 0)
                removed = pl.get("removed_records", 0)
                context_ok = pl.get("rewind_context", False)

                await handler.send_channel_notice(
                    user_infos, channel_id, reply_session_id,
                    f"[收到 /rewind 指令] 已回退到第 {turn_index} 轮"
                    f'（"{preview[:50]}"）'
                    f"，删除 {removed} 条记录，剩余 {remaining} 条。",
                )
                logger.info(
                    "[MessageHandler] /rewind 完成(E2A): session=%s turn=%s context=%s",
                    target_sid, turn_index, context_ok,
                )
                return
            # AgentServer 已返回业务错误，说明传输可用。远程/AgentOS 模式
            # 不能以“不可达”覆盖真实错误；也不回退到 Gateway 本地目录。
            error_payload = resp.payload if isinstance(resp.payload, dict) else {}
            logger.warning("[MessageHandler] /rewind E2A failed: %s", error_payload)
            await handler.send_channel_notice(
                user_infos, channel_id, reply_session_id,
                error_payload or {"error": "回退失败"},
            )
            return
        except Exception as e2a_exc:
            logger.warning("[MessageHandler] /rewind E2A failed: %s", e2a_exc)

        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": "AgentServer 不可达，无法回退"},
        )
    except ValueError as e:
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": f"回退失败：{e}"},
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[MessageHandler] /rewind 失败: %s", exc)
        await handler.send_channel_notice(
            user_infos, channel_id, reply_session_id,
            {"error": f"回退失败：{exc}"},
        )
