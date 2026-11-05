# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""南向不可用契约：AgentServer 掉线后 gateway 返回 SERVICE_UNAVAILABLE，不挂死。

不能直接用不可达 URL 启动 gateway：本地 AgentServer 探测会重试约 60s 才失败，
进程根本起不来。这里先让 gateway 正常连上 stub，再把 stub 停掉，验证链路断开
后的降级行为。
"""

from __future__ import annotations

import time

from testcases_gateway.gateway_client import GatewayWsClient
from testcases_gateway.stub_agent_server import StubAgentServer

_RETRY_SECONDS = 15.0


def test_agent_server_disconnect_returns_service_unavailable(gateway_factory) -> None:
    stub = StubAgentServer().start()
    gateway = gateway_factory(stub.url)
    stub.stop()

    client = GatewayWsClient(gateway.ws_url, expect_ack=False)
    try:
        deadline = time.monotonic() + _RETRY_SECONDS
        while True:
            frames = client.request("session.list", {}, rid="st-unavailable", timeout=15.0)
            res = [frame for frame in frames if frame.get("type") == "res"][-1]
            if res.get("ok") is False:
                break
            assert time.monotonic() < deadline, f"南向断开后仍返回成功: {res}"
            time.sleep(0.2)
    finally:
        client.close()

    assert res.get("code") == "SERVICE_UNAVAILABLE"
