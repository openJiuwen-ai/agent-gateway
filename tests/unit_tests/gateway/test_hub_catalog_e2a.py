# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Management Hub catalog routing, filtering and degradation in Gateway."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from agent_gateway.enterprise.config.manager_config.routers.application_config_routers import (
    application_config_router,
)

HUB_CATALOG_METHOD = "hub.expert_packages.catalog"


@pytest.mark.asyncio
@pytest.mark.parametrize("hub_failed", [False, True])
async def test_catalog_gateway_rpc_without_session_or_agent(hub_failed):
    payload = (
        {"items": [], "error": "Hub down"}
        if hub_failed
        else {
            "items": [
                {"kind": "agent_template", "id": "asset-a"},
                {"kind": "agent_group", "id": "32c223d69b834c35b40bc3935c6f9f67"},
            ],
            "error": "",
        }
    )
    send_request = AsyncMock(
        return_value=SimpleNamespace(ok=True, payload=payload)
    )
    gateway = FastAPI()
    gateway.state.manager_agent_channel = SimpleNamespace(
        agent_client=SimpleNamespace(send_request=send_request)
    )
    gateway.include_router(application_config_router, prefix="/api/v1")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway), base_url="http://gateway"
    ) as web:
        response = await web.get("/api/v1/package-allowlists/catalog")

    assert response.status_code == 200
    body = response.json()
    assert body["code"] == 200
    assert body["data"]["error"] == payload["error"]
    assert body["data"]["items"] == (
        [] if hub_failed else [{"kind": "agent_template", "id": "asset-a"}]
    )
    send_request.assert_awaited_once()
    envelope = send_request.await_args.args[0]
    assert envelope.method == HUB_CATALOG_METHOD
    assert envelope.params == {}
    assert envelope.session_id is None
    assert envelope.user_id is None
    assert envelope.agent_ref is None
    assert envelope.channel == "gateway"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "offline", "disconnected"])
async def test_catalog_server_unavailable_keeps_management_response(state):
    gateway = FastAPI()
    if state != "missing":
        client = SimpleNamespace(
            server_ready=state != "offline",
            send_request=AsyncMock(side_effect=ConnectionError("Server disconnected")),
        )
        gateway.state.manager_agent_channel = SimpleNamespace(
            agent_client=lambda: client
        )
    gateway.include_router(application_config_router, prefix="/api/v1")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway), base_url="http://gateway"
    ) as web:
        response = await web.get("/api/v1/package-allowlists/catalog")
    assert response.status_code == 200
    assert response.json()["data"]["items"] == []
    assert response.json()["data"]["error"]


def test_hub_catalog_method_needs_no_protocol_enum():
    """The Gateway sends the wire method string and imports no method enum."""
    script = (
        "import sys; "
        "import agent_gateway.common.gateway_methods as gm; "
        f"assert gm.GatewayMethod.HUB_EXPERT_PACKAGES_CATALOG == {HUB_CATALOG_METHOD!r}; "
        "assert not any(name.split('.')[0] in ('jiuwenswarm', 'gateway', 'agent_gateway') "
        "for name in sys.modules if name != 'agent_gateway.common.gateway_methods')"
    )
    subprocess.run([sys.executable, "-I", "-c", script], check=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("authorized", [False, True])
async def test_catalog_keeps_manager_link_authorization(monkeypatch, authorized):
    from unittest.mock import Mock
    from agent_gateway.enterprise.config.manager_config.http import app as manager_app

    def authorize_request(request):
        if not authorized:
            raise manager_app.LinkMTLSError("authenticated TLS peer certificate is missing")

    link = SimpleNamespace(authorize_request=Mock(side_effect=authorize_request))
    monkeypatch.setattr(manager_app, "is_enterprise", lambda: True)
    monkeypatch.setattr(manager_app.LinkMTLSConfig, "from_env", lambda: link)
    send_request = AsyncMock(return_value=SimpleNamespace(ok=True, payload={"items": [], "error": ""}))
    gateway = FastAPI()
    gateway.state.manager_agent_channel = SimpleNamespace(agent_client=SimpleNamespace(send_request=send_request))
    assert manager_app.register_manager_config_routes(gateway)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as web:
        response = await web.get("/api/v1/package-allowlists/catalog")
        assert response.status_code == (200 if authorized else 403)
        if authorized:
            # The compatibility catalogue does not restore the removed cluster allowlist writes.
            assert (await web.put("/api/v1/package-allowlists", json={})).status_code == 404
            assert (await web.delete("/api/v1/package-allowlists")).status_code == 404
    assert link.authorize_request.call_count == 1
    assert send_request.await_count == (1 if authorized else 0)
