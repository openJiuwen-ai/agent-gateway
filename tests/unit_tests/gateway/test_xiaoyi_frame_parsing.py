# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""回归测试：小艺平台 WS 帧解析对非 dict 字段的容错。

平台帧是外部不可信输入，``dict.get(key, default)`` 的默认值只在 key 缺失时
生效，值非 dict 时链式 ``.get()`` 抛 AttributeError；处理异常会在 ``_connect``
的 ``async for`` 外层被捕获、放弃整条 WebSocket 连接——坏帧通常是确定性的，
会形成「崩溃-重连」死循环。修复：所有嵌套读取经 ``_as_dict`` 归一。
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XiaoyiChannel,
    XiaoyiChannelConfig,
    _as_dict,
)

from e2a.message import Message


def _channel() -> XiaoyiChannel:
    channel = XiaoyiChannel(
        XiaoyiChannelConfig(
            channel_id="xiaoyi",
            agent_id="agent-1",
            api_id="api-1",
            push_id="push-1",
            enable_streaming=True,
        ),
        RobotMessageRouter(),
    )
    channel._send_text_response = AsyncMock()
    channel._send_status_update = AsyncMock()
    channel._stop_session_heartbeat = AsyncMock()
    channel._start_session_heartbeat = AsyncMock()
    return channel


def _cancel_timeout_tasks(channel: XiaoyiChannel) -> None:
    for task in list(channel._task_timeout_tasks.values()) + list(channel._session_timeout_tasks.values()):
        task.cancel()


def test_as_dict_normalizes_non_dict_values():
    assert _as_dict({"a": 1}) == {"a": 1}
    assert _as_dict("oops") == {}
    assert _as_dict(None) == {}
    assert _as_dict([1, 2]) == {}
    assert _as_dict(42) == {}


def test_get_a2a_parts_with_malformed_frames():
    channel = _channel()
    # params 为字符串
    assert channel._get_a2a_parts({"method": "x", "params": "oops"}) == []
    # msgDetail 解析出的 JSON 不是对象
    assert channel._get_a2a_parts({"msgType": "data", "msgDetail": "[1, 2]"}) == []
    # parts 元素混入非 dict
    parts = channel._get_a2a_parts({
        "params": {"message": {"parts": ["stray", {"kind": "data", "data": {}}]}}
    })
    assert parts == [{"kind": "data", "data": {}}]


def test_extract_data_event_with_malformed_frames():
    channel = _channel()
    # direct 分支：params 为字符串（修复前 AttributeError）
    assert channel._extract_data_event({"method": "message", "params": "oops"}) is None
    # direct 分支：parts 元素为字符串（修复前 p.get("kind") AttributeError）
    assert channel._extract_data_event({
        "params": {"message": {"parts": ["stray"], "id": "t1"}}
    }) is None
    # wrapped 分支：msgDetail 里 params.message 非 dict
    import json
    wrapped = {
        "msgType": "data",
        "msgDetail": json.dumps({"params": {"message": "oops"}}),
    }
    assert channel._extract_data_event(wrapped) is None


def test_extract_data_event_still_parses_valid_data_only_message():
    """正向对照：合法 data-only 帧的解析行为不变。"""
    channel = _channel()
    event = channel._extract_data_event({
        "sessionId": "sess-1",
        "params": {
            "id": "task-1",
            "message": {"parts": [{"kind": "data", "data": {
                "events": [{"intentName": "GetCurrentLocation", "outputs": {"lat": 1}, "status": "success"}]
            }}]},
        },
    })
    assert event is not None
    assert event.intent_name == "GetCurrentLocation"
    assert event.session_id == "sess-1"
    assert event.task_id == "task-1"


@pytest.mark.asyncio
async def test_message_stream_with_non_dict_params_does_not_crash():
    """修复前：params 为字符串时整条 WS 连接被 _connect 放弃（崩溃-重连死循环）。"""
    channel = _channel()
    channel.bus.route_user_message = AsyncMock()
    await channel._handle_message_stream({
        "method": "message/stream",
        "sessionId": "phys-1",
        "params": "garbage",
    })
    _cancel_timeout_tasks(channel)
    # 坏帧不再抛 AttributeError；空文本消息照常进入路由
    assert channel.bus.route_user_message.await_count == 1
    routed: Message = channel.bus.route_user_message.await_args.args[0]
    assert routed.params["query"] == ""


@pytest.mark.asyncio
async def test_message_stream_skips_non_dict_part_elements():
    channel = _channel()
    channel.bus.route_user_message = AsyncMock()
    await channel._handle_message_stream({
        "method": "message/stream",
        "sessionId": "phys-1",
        "params": {
            "sessionId": "logical-1",
            "id": "task-1",
            "message": {"parts": ["stray", {"kind": "text", "text": "你好"}]},
        },
    })
    _cancel_timeout_tasks(channel)
    routed: Message = channel.bus.route_user_message.await_args.args[0]
    assert routed.params["query"] == "你好"


@pytest.mark.asyncio
async def test_dispatch_gui_agent_events_with_non_dict_event_items():
    channel = _channel()
    seen = []
    channel.register_gui_agent_handler(lambda item: seen.append(item))
    await channel._dispatch_gui_agent_events({
        "params": {"message": {"parts": [
            "stray",
            {"kind": "data", "data": {"events": ["also-stray", {
                "header": {"namespace": "ClawAgent", "name": "InvokeJarvisGUIAgentResponse"},
                "outputs": {},
            }]}},
        ]}},
    })
    assert len(seen) == 1
    assert seen[0]["header"]["name"] == "InvokeJarvisGUIAgentResponse"


@pytest.mark.asyncio
async def test_tasks_cancel_with_non_dict_params():
    channel = _channel()
    channel._ws_connections = {"primary": object()}
    channel._send_agent_response = AsyncMock()
    await channel._handle_tasks_cancel({"sessionId": "sess-1", "params": "oops", "taskId": "task-9"})
    # params 非 dict 不崩：taskId 回退到顶层 taskId，取消响应照常回发
    channel._send_agent_response.assert_awaited_once()
    response = channel._send_agent_response.await_args.args[2]
    assert response["result"]["status"]["state"] == "canceled"


@pytest.mark.asyncio
async def test_message_stream_file_part_with_non_dict_file_field():
    """P1 回归：file 为非对象真值（如字符串）时不得在 .get() 处崩溃。

    修复前 isinstance 检查位于 uri = file_info.get(...) 之后，是防御不可达的
    死代码——{"kind": "file", "file": "abc"} 会在 str.get 抛 AttributeError，
    异常打穿 _connect 的 async for，整条 WS 连接被放弃重连。
    """
    channel = _channel()
    channel.bus.route_user_message = AsyncMock()
    await channel._handle_message_stream({
        "method": "message/stream",
        "sessionId": "phys-1",
        "params": {
            "sessionId": "logical-1",
            "id": "task-1",
            "message": {"parts": [
                {"kind": "file", "file": "abc"},   # file 非对象
                {"kind": "text", "text": "后续文本仍应被处理"},
            ]},
        },
    })
    _cancel_timeout_tasks(channel)
    routed: Message = channel.bus.route_user_message.await_args.args[0]
    assert routed.params["query"] == "后续文本仍应被处理"


@pytest.mark.asyncio
async def test_message_stream_with_non_list_parts_does_not_crash():
    """P2 回归：parts 为不可迭代标量时不得抛 TypeError（`or []` 对真值标量不生效）。"""
    channel = _channel()
    channel.bus.route_user_message = AsyncMock()
    await channel._handle_message_stream({
        "method": "message/stream",
        "sessionId": "phys-1",
        "params": {"sessionId": "logical-1", "id": "task-1", "message": {"parts": 5}},
    })
    _cancel_timeout_tasks(channel)
    assert channel.bus.route_user_message.await_count == 1


def test_get_a2a_parts_with_non_string_msg_detail():
    """P3 回归：msgDetail 非字符串时 json.loads 抛 TypeError 而非 JSONDecodeError。"""
    channel = _channel()
    assert channel._get_a2a_parts({"msgType": "data", "msgDetail": 123}) == []


def test_extract_data_event_with_non_string_msg_detail():
    """P3 回归：wrapped 分支对 msgDetail 非字符串同样容错。"""
    channel = _channel()
    assert channel._extract_data_event({"msgType": "data", "msgDetail": {"already": "dict"}}) is None
