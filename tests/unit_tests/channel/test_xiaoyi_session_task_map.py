# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""小艺 ``_session_task_map`` 按会话生命周期清理，且按映射值匹配。

该映射以 ``task_id`` 为键、``session_id`` 为值，用于把出站回复的 task 映射回
平台会话。清理入口 ``_force_cleanup_session`` 拿到的是 ``session_id``，因此
需要按映射值删除该会话关联的 task。
"""

from __future__ import annotations

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter
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


def test_session_task_map_keeps_all_live_task_mappings() -> None:
    channel = _channel()

    for index in range(4097):
        channel._remember_session_task(f"task-{index}", "sess-1")

    assert len(channel._session_task_map) == 4097
    assert channel._session_task_map["task-0"] == "sess-1"
    assert channel._session_task_map["task-4096"] == "sess-1"


def test_rewriting_task_updates_its_session() -> None:
    channel = _channel()
    channel._remember_session_task("task-a", "sess-1")
    channel._remember_session_task("task-a", "sess-2")

    assert channel._session_task_map == {"task-a": "sess-2"}


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
