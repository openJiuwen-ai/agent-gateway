# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway /ws 非流式转发契约：请求经 E2A 下发，响应原样回收。"""

from __future__ import annotations


def test_session_list_is_proxied_to_agent_server(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)

    frames = gateway_ws.request("session.list", {}, rid="st-sessions")
    res = [frame for frame in frames if frame.get("type") == "res"][-1]

    assert res["ok"] is True
    assert res["payload"] == {"sessions": [], "total": 0}

    envelope = stub_agent_server.requests[before]
    assert envelope["method"] == "session.list"
    assert envelope["channel"] == "web"
    assert envelope["is_stream"] is False
    assert envelope["protocol_version"]
