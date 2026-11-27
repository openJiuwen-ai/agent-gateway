# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Feishu per-request sent-file dedup must be released once the window elapses."""

import os
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from e2a.message import Message
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu import feishu_connect
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)


def _channel() -> FeishuChannel:
    channel = FeishuChannel(
        FeishuConfig(
            enabled=True,
            app_id="cli_test_app",
            app_secret="test_app_secret",
            enable_streaming=False,
        ),
        MagicMock(spec=RobotMessageRouter),
    )
    channel._api_client = MagicMock()
    channel._file_service = MagicMock()
    return channel


def _message(
    event_name: str,
    *,
    request_id: str = "req-1",
    content: str = "the answer",
) -> Message:
    return Message(
        id=request_id,
        type="event",
        channel_id="feishu",
        session_id="feishu_session",
        params={},
        timestamp=0.0,
        ok=True,
        payload={
            "event_type": event_name,
            "content": content,
            "is_complete": True,
        },
        event_type=None,
        metadata={"feishu_chat_id": "oc_chat"},
        enable_streaming=False,
    )


def _patch_send(channel: FeishuChannel, files: list[str]) -> list[str]:
    sent_texts: list[str] = []
    sent_files: list[str] = []

    async def _capture_text(*args: Any, **kwargs: Any) -> None:
        sent_texts.append(str(args[2] if len(args) > 2 else ""))

    async def _capture_file(*args: Any, **kwargs: Any) -> None:
        sent_files.append(str(args[2]))

    channel._send_feishu_message = _capture_text
    channel._send_file_card = _capture_file
    channel._detect_workspace_files = lambda _content: list(files)
    return sent_files


@pytest.mark.asyncio
async def test_final_records_completion_and_keeps_file_dedup(tmp_path) -> None:
    path = tmp_path / "report.txt"
    path.write_text("data", encoding="utf-8")
    channel = _channel()
    sent_files = _patch_send(channel, [str(path)])
    channel._sent_file_paths_by_req["req-1"] = {os.path.abspath(str(path))}

    await channel.send(_message("chat.final"))
    await channel.send(_message("chat.final"))

    assert sent_files == []
    assert isinstance(channel._sent_file_paths_completed_at["req-1"], float)


@pytest.mark.asyncio
async def test_sent_file_paths_are_released_after_window(tmp_path) -> None:
    channel = _channel()
    _patch_send(channel, [])
    channel._sent_file_paths_by_req["req-1"] = {str(tmp_path / "old.txt")}
    channel._sent_file_paths_completed_at["req-1"] = (
        time.monotonic() - feishu_connect._SENT_FILE_PATHS_WINDOW_SECONDS - 1.0
    )
    channel._sent_file_paths_by_req["req-2"] = {str(tmp_path / "keep.txt")}

    await channel.send(_message("chat.final"))

    assert "req-1" not in channel._sent_file_paths_by_req
    assert "req-1" not in channel._sent_file_paths_completed_at
    assert "req-2" in channel._sent_file_paths_by_req


@pytest.mark.asyncio
async def test_error_event_starts_release_window(tmp_path) -> None:
    channel = _channel()
    _patch_send(channel, [])
    channel._sent_file_paths_by_req["req-1"] = {str(tmp_path / "report.txt")}

    await channel.send(_message("chat.error"))

    assert isinstance(channel._sent_file_paths_completed_at["req-1"], float)
