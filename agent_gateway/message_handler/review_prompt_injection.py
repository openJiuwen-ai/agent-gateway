# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Slash-command prompt injection helpers extracted from MessageHandler."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from agent_gateway.message_handler.prompts.security_review_prompt import (
    GitPreExecError,
    build_security_review_prompt,
)
from agent_gateway.message_handler.prompts.review_prompt import build_review_prompt


if TYPE_CHECKING:
    from openjiuwen_protocol.message import Message


logger = logging.getLogger(__name__)


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
