# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""个人版语言配置与启动模型目录的 E2A 黑盒契约。"""

from __future__ import annotations

import time


def _wait_for_method(stub_agent_server, method: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for envelope in stub_agent_server.requests:
            if envelope.get("method") == method:
                return envelope
        time.sleep(0.05)
    raise AssertionError(f"AgentServer 未收到 {method} 请求")


def test_gateway_startup_queries_remote_models(gateway, stub_agent_server) -> None:
    """file cron store 启动迁移应通过 models.list 获取远端模型目录。"""
    assert gateway.proc.poll() is None
    envelope = _wait_for_method(stub_agent_server, "models.list")
    assert envelope["channel"] == "gateway-cron-migration"


def test_locale_get_conf_is_proxied_to_agent_server(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)
    frames = gateway_ws.request("locale.get_conf", {}, rid="st-locale-get")
    res = [frame for frame in frames if frame.get("type") == "res"][-1]

    assert res["ok"] is True
    assert res["payload"] == {"preferred_language": "en"}
    envelope = next(
        item for item in stub_agent_server.requests[before:]
        if item.get("method") == "locale.get_conf"
    )
    assert envelope["channel"] == "web"
    assert envelope["is_stream"] is False


def test_locale_set_conf_is_proxied_to_agent_server(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)
    frames = gateway_ws.request(
        "locale.set_conf",
        {"preferred_language": "en"},
        rid="st-locale-set",
    )
    res = [frame for frame in frames if frame.get("type") == "res"][-1]

    assert res["ok"] is True
    assert res["payload"] == {"preferred_language": "en"}
    envelope = next(
        item for item in stub_agent_server.requests[before:]
        if item.get("method") == "locale.set_conf"
    )
    assert envelope["params"] == {"preferred_language": "en"}
    assert envelope["channel"] == "web"
    assert envelope["is_stream"] is False
