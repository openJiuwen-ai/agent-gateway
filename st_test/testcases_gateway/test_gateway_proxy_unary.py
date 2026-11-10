# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway /ws 非流式转发契约：请求经 E2A 下发，响应原样回收。"""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

from testcases_gateway.gateway_client import GatewayWsClient
from testcases_support.process import start_gateway


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


def test_upgraded_host_config_keeps_extension_methods_forwardable(
    tmp_path, monkeypatch, stub_agent_server,
) -> None:
    """Run the upgrade contract when CI has installed the JiuwenSwarm host."""
    host = importlib.util.find_spec("jiuwenswarm")
    if host is None or host.origin is None:
        pytest.skip("Upgrade ST requires the installed JiuwenSwarm host")

    template = Path(host.origin).parent / "resources" / "config.yaml"
    root = tmp_path / ".jiuwenswarm"
    legacy_config = root / "config" / "config.yaml"
    legacy_config.parent.mkdir(parents=True)
    shutil.copyfile(template, legacy_config)
    monkeypatch.delenv("EXTENSION_DIRS", raising=False)
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_WORKSPACE_READY", raising=False)

    gateway = start_gateway(tmp_path, stub_agent_server.url, use_host_config=True)
    try:
        assert not legacy_config.exists()
        assert (root / "infra" / "config" / "config.yaml").is_file()

        client = GatewayWsClient(gateway.ws_url)
        try:
            for method in (
                "session.assets.remove",
                "skills.enterprise.list",
                "skills.source.search",
                "skills.update",
            ):
                before = len(stub_agent_server.requests)
                frames = client.request(
                    method, {"session_id": "upgrade-session", "name": "st-skill"}, rid=f"st-upgrade-{method}",
                )
                assert frames[-1]["ok"] is True, frames[-1]
                assert stub_agent_server.requests[before]["method"] == method
        finally:
            client.close()
    finally:
        gateway.stop()
