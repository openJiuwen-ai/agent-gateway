"""ADX create/wait/delete carry the caller business trace end to end."""

import pytest
import httpx

from agent_gateway.extensions.adx.control_client import AdxControlClient
from tests.unit_tests.extensions.test_adx_router import _config

TRACE_HEX = "6a945399111141118111111111111111"
TRACEPARENT = f"00-{TRACE_HEX}-{'b' * 16}-01"


@pytest.mark.asyncio
async def test_create_and_delete_forward_trace(monkeypatch, tmp_path):
    config = _config(monkeypatch)
    created = False
    requests = []

    def handle(request):
        nonlocal created
        requests.append(request)
        if request.method == "GET":
            rows = [{"id": "test-alice", "tenant_id": "appliance", "status": "running"}]
            return httpx.Response(200, json=rows if created else [])
        if request.method == "POST":
            created = True
            return httpx.Response(200, json={"environmentId": "test-alice"})
        return httpx.Response(200, json={"success": True})

    client = AdxControlClient(config, transport=httpx.MockTransport(handle))
    try:
        info = await client.create_sandbox(
            namespace="test", name="alice", workspace=str(tmp_path),
            runtime_spec={"rootfs": {"imageurl": "registry.example/agent:1"}},
            trace_id=TRACE_HEX,
        )
        assert info.metadata["trace_id"] == TRACE_HEX

        await client.delete_sandbox("test-alice", trace_id=TRACE_HEX)
    finally:
        await client.close()

    creates = [request for request in requests if request.method == "POST"]
    assert creates and all(part[1] == TRACE_HEX for part in (
        request.headers["traceparent"].split("-") for request in requests
    ))
    assert all(request.headers["x-trace-id"] == TRACE_HEX for request in requests)
    # ADX replay identity stays independent of the business trace.
    assert creates[0].headers["x-request-id"].startswith("create-")
