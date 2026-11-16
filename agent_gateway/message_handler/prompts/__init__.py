# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from agent_gateway.common.config import get_config


def response_language_line(preferred_language: str | None = None) -> str:
    """使用显式语言生成回复指令；未传入时保留个人版本地配置行为。"""
    raw = get_config().get("preferred_language") if preferred_language is None else preferred_language
    lang = str(raw or "zh").strip().lower()
    if lang != "en":
        lang = "zh"
    return "Respond in Chinese (simplified)." if lang == "zh" else "Respond in English."
