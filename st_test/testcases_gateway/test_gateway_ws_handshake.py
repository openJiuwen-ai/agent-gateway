# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway /ws 握手与本地校验契约。"""

from __future__ import annotations


def test_connection_ack_is_first_frame(gateway_ws) -> None:
    assert gateway_ws.ack["type"] == "event"
    assert gateway_ws.ack["event"] == "connection.ack"
    assert gateway_ws.ack["payload"]["protocol_version"] == "1.0"


def test_unknown_method_is_rejected_locally(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)

    frames = gateway_ws.request("no.such.method", rid="st-unknown")
    res = [frame for frame in frames if frame.get("type") == "res"][-1]

    assert res["ok"] is False
    assert res.get("error")
    # gateway 有 2s 生命周期轮询，会在后台向 AgentServer 发无关请求；
    # 只校验这条未知方法没有被转发，避免用全局请求数做断言造成偶发失败。
    forwarded = [
        entry
        for entry in stub_agent_server.requests[before:]
        if entry.get("request_id") == "st-unknown"
        or entry.get("method") == "no.such.method"
    ]
    assert not forwarded
