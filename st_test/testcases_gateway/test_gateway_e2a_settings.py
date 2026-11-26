# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""个人版语言配置与启动模型目录的 E2A 黑盒契约。"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import time

import pytest
from e2a.agent_models import AgentResponse
from e2a.wire_codec import encode_agent_response_for_wire

from testcases_gateway.stub_agent_server import StubAgentServer
from testcases_support.process import service_env, start_gateway


class UnavailableCatalogStub(StubAgentServer):
    def _unary_wire(self, envelope, request_id, method):
        if method == 'models.list':
            return encode_agent_response_for_wire(AgentResponse(
                request_id=request_id, channel_id=envelope.get('channel') or 'web',
                ok=False, payload={'code':'METHOD_NOT_FOUND','error':'legacy backend has no catalog'},
            ), response_id=request_id)
        return super()._unary_wire(envelope, request_id, method)


def _wait_for_method(stub_agent_server, method: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for envelope in stub_agent_server.requests:
            if envelope.get("method") == method:
                return envelope
        time.sleep(0.05)
    raise AssertionError(f"AgentServer 未收到 {method} 请求")


def _wait_for_cron_ready(gateway) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if "deferred Cron scheduler ready" in gateway.log_path.read_text(encoding="utf-8", errors="replace"):
            return
        time.sleep(.05)
    raise AssertionError("Cron scheduler did not finish startup")


def test_gateway_startup_skips_remote_models_without_legacy_cron(gateway, stub_agent_server) -> None:
    """空任务不为启动迁移额外请求远端模型目录。"""
    assert gateway.proc.poll() is None
    _wait_for_cron_ready(gateway)
    assert not any(item.get("method") == "models.list" and item.get("channel") == "gateway-cron-migration"
                   for item in stub_agent_server.requests)


def test_models_list_is_forwarded_when_requested(gateway_ws, stub_agent_server) -> None:
    before = len(stub_agent_server.requests)
    frames = gateway_ws.request("models.list", {}, rid="st-models-get")
    response = [frame for frame in frames if frame.get("type") == "res"][-1]
    assert response["ok"] is True
    assert response["payload"]["models"][0]["model_name"] == "st-model"
    envelope = next(item for item in stub_agent_server.requests[before:] if item.get("method") == "models.list")
    assert envelope["channel"] == "web" and envelope["is_stream"] is False


@pytest.mark.parametrize('catalog_available', [True, False])
def test_gateway_migrates_existing_legacy_cron(tmp_path, stub_agent_server, catalog_available) -> None:
    home = tmp_path/"legacy-cron"
    home.mkdir()
    seed = '''import json
from agent_gateway.common.paths import get_cron_jobs_path
path = get_cron_jobs_path()
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({"jobs":[{"id":"st-legacy","name":"legacy","enabled":False,
    "cron_expr":"0 0 1 1 *","timezone":"UTC","wake_offset_seconds":0,
    "description":"fixture","targets":"web","mode":"agent.work.normal",
    "work_mode":"work","model_name":"st-model"}]}), encoding="utf-8")
print("SEEDED_PATH:"+str(path))
'''
    seeded = subprocess.run([sys.executable, "-c", seed], env=service_env(home),
                            capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert seeded.returncode == 0, seeded.stderr
    job_path = Path(next(line.removeprefix("SEEDED_PATH:") for line in seeded.stdout.splitlines()
                         if line.startswith("SEEDED_PATH:")))
    backend = stub_agent_server if catalog_available else UnavailableCatalogStub().start()
    before = len(backend.requests)
    instance = None
    try:
        instance = start_gateway(home, backend.url)
        _wait_for_cron_ready(instance)
        catalog_requests = [item for item in backend.requests[before:] if item.get("method") == "models.list"
                            and item.get("channel") == "gateway-cron-migration"]
        assert len(catalog_requests) == (1 if catalog_available else 3)
        stored = json.loads(job_path.read_text(encoding="utf-8"))
        job = stored["jobs"][0]
        if catalog_available:
            selection = job["model_selection"]
            assert selection["type"] == "model" and selection["id"] == "mdl_st_default"
        else:
            assert job.get("model_selection") is None and job["model_name"] == "st-model"
    finally:
        if instance is not None:
            instance.stop()
        if not catalog_available:
            backend.stop()


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
