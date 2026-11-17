# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from agent_gateway.common.config import get_config


def response_language_line(language: str | None = None) -> str:
    """生成 Agent 回复语言指令。

    ``language`` 由调用方经 E2A 从 AgentServer 读取；缺失时保留本地
    config.yaml fallback，兼容 no-client/legacy 部署。
    """
    raw = language if language is not None else get_config().get("preferred_language")
    lang = str(raw or "zh").strip().lower()
    if lang != "en":
        lang = "zh"
    return "Respond in Chinese (simplified)." if lang == "zh" else "Respond in English."
