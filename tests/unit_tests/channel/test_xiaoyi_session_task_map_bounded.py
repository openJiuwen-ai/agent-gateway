# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""小艺 ``_session_task_map`` 必须有界，且清理时按键值匹配。

该映射以 ``task_id`` 为键、``session_id`` 为值，用于把出站回复的 task 映射回
平台会话。清理入口 ``_force_cleanup_session`` 拿到的是 ``session_id``，历史上
直接按 session_id 删键，键值永远对不上，于是每条入站消息都会永久新增一条。
"""

from __future__ import annotations

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.xiaoyi import xiaoyi_connect
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XiaoyiChannel,
    XiaoyiChannelConfig,
)


def _channel() -> XiaoyiChannel:
    return XiaoyiChannel(
        XiaoyiChannelConfig(
            channel_id="xiaoyi",
            agent_id="agent-1",
            api_id="api-1",
            push_id="push-1",
        ),
        RobotMessageRouter(),
    )


def test_session_task_map_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xiaoyi_connect, "_SESSION_TASK_MAP_LIMIT", 3)
    channel = _channel()

    for index in range(5):
        channel._remember_session_task(f"task-{index}", "sess-1")

    assert list(channel._session_task_map) == ["task-2", "task-3", "task-4"]


def test_rewriting_task_refreshes_its_position() -> None:
    channel = _channel()
    channel._remember_session_task("task-a", "sess-1")
    channel._remember_session_task("task-b", "sess-1")
    channel._remember_session_task("task-a", "sess-2")

    assert list(channel._session_task_map) == ["task-b", "task-a"]
    assert channel._session_task_map["task-a"] == "sess-2"


def test_empty_task_id_is_not_recorded() -> None:
    channel = _channel()

    channel._remember_session_task("", "sess-1")

    assert not channel._session_task_map


def test_force_cleanup_removes_entries_by_session_value() -> None:
    channel = _channel()
    channel._remember_session_task("task-a1", "sess-a")
    channel._remember_session_task("task-a2", "sess-a")
    channel._remember_session_task("task-b1", "sess-b")

    channel._force_cleanup_session("sess-a")

    assert dict(channel._session_task_map) == {"task-b1": "sess-b"}


def test_reply_resolves_session_for_recorded_task() -> None:
    channel = _channel()
    channel._remember_session_task("task-x", "sess-x")
    message = Message(
        id="task-x",
        type="event",
        channel_id="xiaoyi",
        session_id="",
        params={},
        payload={},
        metadata={},
        timestamp=0,
        ok=True,
    )

    assert channel._extract_platform_receive_info(message) == ("sess-x", "task-x")
