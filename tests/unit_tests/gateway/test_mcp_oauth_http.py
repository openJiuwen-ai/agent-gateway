# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise the Gateway loopback callback HTTP boundary with a fake provider."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import subprocess
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent_gateway.channel_manager.web import mcp_oauth_http
from agent_gateway.common import external_providers
from agent_gateway.common.mcp_oauth import McpOAuthError


class _FakeMcpRegistry:
    def __init__(self):
        self.calls = []
        self.error = None

    async def complete_oauth_callback_async(self, query):
        self.calls.append(dict(query))
        if self.error is not None:
            raise self.error
        return True


@pytest.fixture(autouse=True)
def capture_callback_logs(caplog, monkeypatch):
    target = mcp_oauth_http.logger
    monkeypatch.setattr(target, "handlers", [caplog.handler])
    caplog.set_level(logging.WARNING, logger=target.name)


@pytest.fixture
def oauth_callback(monkeypatch):
    fake = _FakeMcpRegistry()
    monkeypatch.setitem(external_providers._providers, "mcp_registry", fake)
    app = FastAPI()
    mcp_oauth_http.attach_mcp_oauth_routes(app)
    with TestClient(app) as client:
        yield client, fake


def test_callback_rejects_unknown_state(oauth_callback):
    client, fake = oauth_callback
    fake.error = McpOAuthError("OAuth flow is unknown or expired")

    response = client.get(
        "/mcp/oauth/callback", params={"code": "mock-code", "state": "unknown-state"}
    )

    assert response.status_code == 400
    assert "OAuth flow is unknown or expired" in response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert fake.calls == [{"code": "mock-code", "state": "unknown-state"}]


def test_callback_reports_missing_code(oauth_callback):
    client, fake = oauth_callback
    fake.error = McpOAuthError("OAuth callback is missing the authorization code")

    response = client.get("/mcp/oauth/callback", params={"state": "mock-state"})

    assert response.status_code == 400
    assert "OAuth callback is missing the authorization code" in response.text
    assert fake.calls == [{"state": "mock-state"}]


def test_callback_reports_and_escapes_provider_error(oauth_callback):
    client, fake = oauth_callback
    fake.error = McpOAuthError("access_denied<script>mock</script>")

    response = client.get(
        "/mcp/oauth/callback",
        params={"state": "mock-state", "error": "access_denied<script>mock</script>"},
    )

    assert response.status_code == 400
    assert "access_denied&lt;script&gt;mock&lt;/script&gt;" in response.text
    assert "<script>mock</script>" not in response.text


def test_callback_reports_unavailable_provider_without_exposing_details(monkeypatch, caplog):
    monkeypatch.delitem(external_providers._providers, "mcp_registry", raising=False)
    app = FastAPI()
    mcp_oauth_http.attach_mcp_oauth_routes(app)
    with TestClient(app) as client:
        response = client.get(
            "/mcp/oauth/callback",
            params={"code": "mock-private-code", "state": "mock-private-state"},
        )

    assert response.status_code == 500
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "ExternalProviderUnregistered" in caplog.text
    for secret in ("mock-private-code", "mock-private-state", "mcp_registry"):
        assert secret not in response.text


def test_callback_unexpected_failure_does_not_expose_exception_details(oauth_callback, caplog):
    client, fake = oauth_callback
    fake.error = OSError("mock-private-access-token")

    response = client.get(
        "/mcp/oauth/callback", params={"code": "mock-code", "state": "mock-state"}
    )

    assert response.status_code == 500
    assert "OSError" in caplog.text
    assert "mock-private-access-token" not in response.text
    assert "mock-private-access-token" not in caplog.text


def test_callback_uses_providers_registered_by_real_process_setup(tmp_path):
    code = (
        "import sys\n"
        "from fastapi import FastAPI\n"
        "from fastapi.testclient import TestClient\n"
        "from agent_gateway.common.external_providers import registered_domains\n"
        "from agent_gateway.channel_manager.web.mcp_oauth_http import attach_mcp_oauth_routes\n"
        "assert 'jiuwenswarm.server.runtime.mcp.oauth_flow' not in sys.modules\n"
        "from agent_gateway.routing.mcp_remote import install\n"
        "install()\n"
        "assert 'mcp_registry' in registered_domains()\n"
        "app = FastAPI()\n"
        "attach_mcp_oauth_routes(app)\n"
        "with TestClient(app) as client:\n"
        "    response = client.get('/mcp/oauth/callback', params={'code':'mock-code','state':'unknown-state'})\n"
        "assert response.status_code == 400, response.text\n"
        "assert 'unavailable' in response.text or '授权未完成' in response.text\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[3],
        env={
            **os.environ,
            "JIUWENSWARM_DATA_DIR": str(tmp_path / "data"),
            "PYTHONUTF8": "1",
        },
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
