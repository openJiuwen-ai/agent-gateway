# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Channel and task RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.channel_manager.web.task_asr import (
    TaskAsrError,
    transcribe_task_audio,
)


logger = logging.getLogger(__name__)


def register_channel_task_handlers(
    channel: Any,
    channel_manager: Any,
    agent_client: Any,
    _resolve: Any,
) -> None:
    """Register channel and task handlers in their original registration order."""

    async def _channel_get(ws, req_id, params, session_id):
        """返回已注册的 channel 列表."""
        cm = _resolve(channel_manager)
        if cm is not None:
            channels = [{"channel_id": cid} for cid in cm.enabled_channels]
        else:
            channels = []
        await channel.send_response(ws, req_id, ok=True, payload={"channels": channels})

    async def _task_asr_transcribe(ws, req_id, params, session_id):
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(
                ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST"
            )
            return
        try:
            text = await transcribe_task_audio(params)
        except TaskAsrError as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code=exc.code
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("[task.asr.transcribe] unexpected failure")
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )
            return
        await channel.send_response(ws, req_id, ok=True, payload={"text": text})

    channel.register_method("channel.get", _channel_get)
    channel.register_method("task.asr.transcribe", _task_asr_transcribe)
