# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Shared group acknowledgment generation for IM channels."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)

DEFAULT_GROUP_ACK = "好的，我知道了，会跟进处理。"
_FORBIDDEN_PHRASES = (
    "已提醒", "已通知", "私下通知", "私聊", "转告", "提醒了",
    "通知了", "告诉了", "帮你", "帮您", "代为",
)


def normalize_group_ack_text(target_name: str, text: str) -> str:
    """Keep a short first-person reply and reject bot-style narration."""
    normalized = re.split(r"[\r\n]+", (text or "").strip(), maxsplit=1)[0]
    normalized = re.sub(r"\s+", " ", normalized).strip(' "\'\u201c\u201d\u2018\u2019「」')
    if not normalized or (target_name and target_name in normalized):
        return ""
    if any(phrase in normalized for phrase in _FORBIDDEN_PHRASES):
        return ""
    return normalized


def build_group_ack_prompt(platform_name: str, target_name: str, content: str) -> str:
    """Build the platform-specific prompt while keeping its contract shared."""
    return (
        "你是一个{platform}群聊机器人。群里有一条需要{name}关注的消息,"
        "你已经把详细回复私发给了{name}。"
        "现在群里需要补一句很短的话，但这句话必须像{name}本人在群里的直接回复,"
        "而不是机器人旁白或转述。\n\n"
        "要求：\n"
        "- 一句话，不超过30个字\n"
        "- 用第一人称口吻，像当事人本人在说话\n"
        "- 不要提到{name}的名字\n"
        "- 不要写成'我已经提醒了{name}''已通知{name}''我帮{name}处理了'这类机器人/第三人称表达\n"
        "- 更像'好的，我知道了，会准时参加会议''收到，我会跟进这件事'\n"
        "- 不要照搬原文，保留核心动作即可\n\n"
        "你私发给{name}的内容是：\n{content}"
    ).format(platform=platform_name, name=target_name, content=content[:500])


def generate_group_ack_text(
    target_name: str,
    content: str,
    *,
    platform_name: str,
    fallback: str = DEFAULT_GROUP_ACK,
    request_post: Callable[..., Any] | None = None,
) -> str:
    """Generate a short group acknowledgment, falling back without credentials."""
    api_key = os.getenv("API_KEY", "").strip()
    api_base = os.getenv("API_BASE", "").strip()
    model_name = os.getenv("MODEL_NAME", "").strip() or "GLM-4.7"
    if not api_key or not api_base:
        return fallback
    if request_post is None:
        request_post = requests.post
    try:
        response = request_post(
            f"{api_base.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model_name,
                "messages": [
                    {
                        "role": "user",
                        "content": build_group_ack_prompt(platform_name, target_name, content),
                    }
                ],
                "temperature": 0.7,
                "max_tokens": 80,
            },
            timeout=30,
        )
        response.raise_for_status()
        choices = response.json().get("choices") or []
        if choices:
            text = normalize_group_ack_text(
                target_name, (choices[0].get("message") or {}).get("content", "")
            )
            if text:
                return text
    except Exception as exc:  # noqa: BLE001
        logger.warning("生成%s群确认文案失败，使用回退: %s", platform_name, exc)
    return fallback


__all__ = [
    "DEFAULT_GROUP_ACK",
    "build_group_ack_prompt",
    "generate_group_ack_text",
    "normalize_group_ack_text",
]
