# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""个人版语言配置 E2A 黑盒契约。"""

from __future__ import annotations


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
