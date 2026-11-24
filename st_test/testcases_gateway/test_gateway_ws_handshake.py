# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway /ws 握手与本地校验契约。"""

from __future__ import annotations


def test_connection_ack_is_first_frame(gateway_ws) -> None:
    assert gateway_ws.ack["type"] == "event"
    assert gateway_ws.ack["event"] == "connection.ack"
    assert gateway_ws.ack["payload"]["protocol_version"] == "1.0"


def test_unknown_method_is_forwarded_to_agent_server(gateway_ws, stub_agent_server) -> None:
    """未知方法不再本地拒绝：转发后由 AgentServer 判定是否 METHOD_NOT_FOUND。"""
    before = len(stub_agent_server.requests)
    rid = "st-unknown"

    frames = gateway_ws.request("no.such.method", rid=rid)
    res = [frame for frame in frames if frame.get("type") == "res"][-1]

    # gateway 有 2s 生命周期轮询，会在后台向 AgentServer 发无关请求；
    # 只统计本次请求是否被转发，避免用全局请求数做断言造成偶发失败。
    forwarded = [
        entry
        for entry in stub_agent_server.requests[before:]
        if entry.get("request_id") == rid or entry.get("method") == "no.such.method"
    ]
    assert res["ok"] is True
    assert len(forwarded) == 1
