# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""与真实 AgentServer 的连通性与配置契约（经 gateway 对外接口）。"""

from __future__ import annotations

from testcases_support.frames import last_res, unique_rid


def test_connection_status_reports_agent_ready(workswarm_ws) -> None:
    frames = workswarm_ws.request("connection.status", {}, rid=unique_rid("st-status"))
    res = last_res(frames)

    assert res["ok"] is True
    payload = res["payload"]
    assert payload["agent_ready"] is True
    assert payload["protocol_version"] == "1.0"


def test_config_get_relays_agent_server_config(workswarm_ws) -> None:
    frames = workswarm_ws.request("config.get", {}, rid=unique_rid("st-config"))
    res = last_res(frames)

    assert res["ok"] is True
    payload = res["payload"]
    assert isinstance(payload, dict) and payload
    # 真实 AgentServer 的配置面字段，gateway 必须原样透传而不是替换为空壳。
    for key in ("model", "model_provider", "api_base", "endpoint_profile"):
        assert key in payload, f"config.get 缺少字段 {key}: {sorted(payload)[:10]}"
