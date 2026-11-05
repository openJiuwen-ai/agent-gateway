# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Management Hub catalog routing, filtering and degradation in Gateway."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.enterprise.config.manager_config.routers.application_config_routers import (
    application_config_router,
)


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
    assert envelope.method == ReqMethod.HUB_EXPERT_PACKAGES_CATALOG.value
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


def test_hub_catalog_method_available_without_application_imports():
    """A standalone Gateway can use the pinned protocol without Server shims."""
    script = (
        "import sys; "
        "from openjiuwen_protocol.message import ReqMethod; "
        "assert ReqMethod('hub.expert_packages.catalog') is ReqMethod.HUB_EXPERT_PACKAGES_CATALOG; "
        "assert not any(name.split('.')[0] in ('jiuwenswarm', 'gateway', 'agent_gateway') "
        "for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-I", "-c", script], check=True)
