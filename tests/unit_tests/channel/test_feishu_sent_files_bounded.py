# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""飞书 ``_sent_file_paths_by_req`` 必须有界。

该映射记录每个请求已发送过的文件路径，供 ``chat.final`` 兜底发送时去重。
原实现只写不删，一次进程生命周期内每个发过文件的请求都会留下一条
``req_id -> {绝对路径}``，长跑网关里随请求数线性增长。
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from e2a.message import EventType, Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu import feishu_connect
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)


def _make_channel() -> FeishuChannel:
    config = FeishuConfig(
        enabled=True,
        app_id="test_app_id",
        app_secret="test_app_secret",
        enable_file_upload=True,
    )
    channel = FeishuChannel(config, MagicMock(spec=RobotMessageRouter))
    channel._api_client = MagicMock()

    async def _noop_send(*_args: object, **_kwargs: object) -> None:
        return None

    for name in (
        "_send_image_message",
        "_send_audio_message",
        "_send_video_message",
        "_send_file_card",
    ):
        setattr(channel, name, _noop_send)
    return channel


def _file_message(req_id: str, path: Path) -> Message:
    return Message(
        id=req_id,
        type="event",
        channel_id="feishu",
        session_id="sess_001",
        params={},
        timestamp=1000.0,
        ok=True,
        payload={"event_type": "chat.file", "files": [str(path)]},
        event_type=EventType.CHAT_FILE,
        metadata={"feishu_chat_id": "oc_test123"},
    )


@pytest.mark.asyncio
async def test_sent_file_paths_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(feishu_connect, "_SENT_FILE_PATHS_LIMIT", 3)
    channel = _make_channel()
    file_path = tmp_path / "report.txt"
    file_path.write_text("x", encoding="utf-8")

    for index in range(5):
        await channel._send_file_message(_file_message(f"req-{index}", file_path))

    assert list(channel._sent_file_paths_by_req) == ["req-2", "req-3", "req-4"]
    assert channel._sent_file_paths_by_req["req-4"] == {str(file_path)}


@pytest.mark.asyncio
async def test_sent_file_paths_refresh_recency_on_reuse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(feishu_connect, "_SENT_FILE_PATHS_LIMIT", 3)
    channel = _make_channel()
    file_path = tmp_path / "report.txt"
    file_path.write_text("x", encoding="utf-8")

    for req_id in ("req-a", "req-b", "req-a", "req-c", "req-d"):
        await channel._send_file_message(_file_message(req_id, file_path))

    assert list(channel._sent_file_paths_by_req) == ["req-a", "req-c", "req-d"]


@pytest.mark.asyncio
async def test_sent_file_paths_dedupe_within_request(tmp_path: Path) -> None:
    channel = _make_channel()
    file_path = tmp_path / "report.txt"
    file_path.write_text("x", encoding="utf-8")

    await channel._send_file_message(_file_message("req-dup", file_path))
    await channel._send_file_message(_file_message("req-dup", file_path))

    assert channel._sent_file_paths_by_req["req-dup"] == {str(file_path)}
