# pylint: disable=protected-access
"""飞书兜底文件去重表必须有回收点，否则每条发文件的请求各留一份。"""

from __future__ import annotations

import os
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)


def _channel() -> FeishuChannel:
    return FeishuChannel(
        FeishuConfig(enabled=True, app_id="app", app_secret="secret"),
        MagicMock(spec=RobotMessageRouter),
    )


def test_paths_within_ttl_are_still_deduped() -> None:
    """TTL 之内去重语义不变：同一请求同一文件只算一次已发送。"""
    channel = _channel()
    channel._remember_sent_file_path("req_1", "report.md")

    assert channel._already_sent_file_paths("req_1") == {os.path.abspath("report.md")}
    assert len(channel._sent_file_paths_by_req) == 1


def test_requests_past_ttl_are_reclaimed_on_write() -> None:
    """写入路径本身要负责回收：旧请求在下一次登记文件时被清掉。"""
    channel = _channel()
    channel._remember_sent_file_path("req_old", "old.md")
    stale_at = time.time() - channel._SENT_FILE_PATHS_TTL_SECONDS - 1
    channel._sent_file_paths_by_req["req_old"] = (
        stale_at,
        {os.path.abspath("old.md")},
    )
    # 绕开清扫节流，让本次写入必定触发全表清扫。
    channel._sent_file_paths_pruned_at = 0.0

    channel._remember_sent_file_path("req_new", "report.md")

    assert "req_old" not in channel._sent_file_paths_by_req
    assert channel._already_sent_file_paths("req_new") == {os.path.abspath("report.md")}


def test_repeated_writes_refresh_the_deadline() -> None:
    """同一请求后续再发文件要刷新有效期，否则长请求会在 chat.final 兜底时被判过期重发。"""
    channel = _channel()
    first_sent_at = time.time() - 3000
    channel._sent_file_paths_by_req["req_1"] = (
        first_sent_at,
        {os.path.abspath("first.md")},
    )

    channel._remember_sent_file_path("req_1", "second.md")

    seen_at, paths = channel._sent_file_paths_by_req["req_1"]
    assert seen_at > first_sent_at
    assert paths == {os.path.abspath("first.md"), os.path.abspath("second.md")}
    assert time.time() - seen_at < channel._SENT_FILE_PATHS_TTL_SECONDS


def test_expired_entry_is_dropped_when_read() -> None:
    """读取命中过期条目时顺手回收，避免只能等下一次写入才清。"""
    channel = _channel()
    channel._remember_sent_file_path("req_1", "report.md")

    seen_at, paths = channel._sent_file_paths_by_req["req_1"]
    channel._sent_file_paths_by_req["req_1"] = (
        seen_at - channel._SENT_FILE_PATHS_TTL_SECONDS - 1,
        paths,
    )

    assert channel._already_sent_file_paths("req_1") == set()
    assert "req_1" not in channel._sent_file_paths_by_req


@pytest.mark.asyncio
async def test_send_file_message_records_sent_paths(tmp_path) -> None:
    """chat.file 发文件仍然登记路径，兜底去重依赖它。"""
    channel = _channel()
    channel._send_file_card = AsyncMock()
    target = tmp_path / "report.md"
    target.write_text("x", encoding="utf-8")
    msg = Message(
        id="req_9",
        type="event",
        channel_id="feishu",
        session_id="sess_001",
        params={},
        timestamp=1000.0,
        ok=True,
        payload={"files": [str(target)]},
        metadata={"feishu_chat_id": "oc_test123"},
    )

    await channel._send_file_message(msg)

    assert channel._already_sent_file_paths("req_9") == {os.path.abspath(str(target))}


@pytest.mark.asyncio
async def test_stop_releases_the_dedupe_table() -> None:
    """停机清理同族状态时，去重表也要一起放开。"""
    channel = _channel()
    channel._remember_sent_file_path("req_1", "report.md")

    await channel.stop()

    assert channel._sent_file_paths_by_req == {}
