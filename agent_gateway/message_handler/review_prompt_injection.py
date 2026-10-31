# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Slash-command prompt injection helpers extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.command_parser.slash_command import (
    ParsedControlAction,
)

from agent_gateway.message_handler.prompts.security_review_prompt import (
    GitPreExecError,
    build_security_review_prompt,
)
from agent_gateway.message_handler.prompts.review_prompt import build_review_prompt


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InboundReviewPromptResult:
    """chat.send 注入结果：skip 表示跳过本次入站处理。"""

    skip: bool = False
    prompt: str | None = None


async def apply_inbound_review_prompt(
    handler: Any,
    msg: "Message",
    parsed: Any,
) -> InboundReviewPromptResult:
    """处理 chat.send 入站路径的 review / security-review 指令。"""
    if parsed.action is ParsedControlAction.REVIEW_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                {"id": msg.id, "meta_data": msg.metadata},
                msg.channel_id,
                msg.session_id,
                "非法指令，/review 参数过长或含有非法控制字符",
            )
        )
        return InboundReviewPromptResult(skip=True)

    if parsed.action is ParsedControlAction.REVIEW_OK:
        pr_arg = parsed.pr_arg or ""
        review_prompt = build_review_prompt(pr_arg)
        msg.params = dict(msg.params)
        msg.params["query"] = review_prompt
        if "content" in msg.params:
            msg.params["content"] = review_prompt
        logger.info(
            "[MessageHandler] /review prompt injected for chat.send "
            "channel=%s pr_arg=%s",
            getattr(msg, "channel_id", ""),
            pr_arg or "<none>",
        )
        return InboundReviewPromptResult(prompt=review_prompt)

    if parsed.action is ParsedControlAction.SECURITY_REVIEW_BAD:
        asyncio.create_task(
            handler.send_channel_notice(
                {"id": msg.id, "meta_data": msg.metadata},
                msg.channel_id,
                msg.session_id,
                "非法指令，/security-review 参数过长或含有非法控制字符",
            )
        )
        return InboundReviewPromptResult(skip=True)

    if parsed.action is ParsedControlAction.SECURITY_REVIEW_OK:
        extra_arg = parsed.security_review_arg or ""
        cwd = (
            msg.metadata.get("cwd")
            if isinstance(msg.metadata, dict)
            else None
        )
        try:
            security_prompt = await asyncio.get_event_loop().run_in_executor(
                None,
                build_security_review_prompt,
                extra_arg,
                cwd,
            )
        except GitPreExecError as exc:
            await handler.send_channel_notice(
                {"id": msg.id, "meta_data": msg.metadata},
                msg.channel_id,
                msg.session_id,
                f"/security-review 无法执行：{exc}",
            )
            return InboundReviewPromptResult(skip=True)
        msg.params = dict(msg.params)
        msg.params["query"] = security_prompt
        if "content" in msg.params:
            msg.params["content"] = security_prompt
        logger.info(
            "[MessageHandler] /security-review prompt injected "
            "for chat.send channel=%s extra_arg=%s",
            getattr(msg, "channel_id", ""),
            extra_arg or "<none>",
        )
        return InboundReviewPromptResult(prompt=security_prompt)

    return InboundReviewPromptResult()


async def inject_review_prompt(
    handler: Any,
    msg: "Message",
    parsed: Any,
    *,
    channel_type: str,
) -> bool:
    """把 /review prompt 注入当前消息，并继续转发给 Agent。"""
    pr_arg = parsed.pr_arg or ""
    review_prompt = build_review_prompt(pr_arg)
    if msg.params is None:
        msg.params = {}
    msg.params["query"] = review_prompt
    logger.info(
        "[MessageHandler] /review prompt injected channel=%s pr_arg=%s",
        channel_type,
        pr_arg or "<none>",
    )
    return False


async def inject_security_review_prompt(
    handler: Any,
    msg: "Message",
    parsed: Any,
    *,
    channel_type: str,
) -> bool:
    """预执行只读 git 后注入 security-review prompt；失败则通知并中止。"""
    extra_arg = parsed.security_review_arg or ""
    cwd = (
        msg.metadata.get("cwd")
        if isinstance(msg.metadata, dict)
        else None
    )
    try:
        security_prompt = await asyncio.get_event_loop().run_in_executor(
            None, build_security_review_prompt, extra_arg, cwd
        )
    except GitPreExecError as exc:
        await handler.send_channel_notice(
            {"id": msg.id, "meta_data": msg.metadata},
            msg.channel_id,
            msg.session_id,
            f"/security-review 无法执行：{exc}",
        )
        return True
    if msg.params is None:
        msg.params = {}
    msg.params["query"] = security_prompt
    logger.info(
        "[MessageHandler] /security-review prompt injected channel=%s extra_arg=%s",
        channel_type,
        extra_arg or "<none>",
    )
    return False
