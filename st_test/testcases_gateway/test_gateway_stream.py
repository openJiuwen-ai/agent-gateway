# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway /ws 流式转发契约：AgentServer 分片经 gateway 转成事件帧。"""

from __future__ import annotations


def test_chat_send_stream_relays_terminal_event(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)

    frames = gateway_ws.request(
        "chat.send",
        {"content": "stub-hello", "session_id": "st-session"},
        rid="st-stream",
        is_stream=True,
        timeout=20.0,
    )
    events = [frame.get("event") for frame in frames if frame.get("type") == "event"]

    assert "chat.delta" in events
    assert "chat.final" in events

    envelope = stub_agent_server.requests[before]
    assert envelope["method"] == "chat.send"
    assert envelope["is_stream"] is True
