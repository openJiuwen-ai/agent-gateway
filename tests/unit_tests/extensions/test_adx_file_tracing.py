"""Trace propagation through native file transfer."""
import json
from uuid import UUID

import httpx
import pytest

from agent_gateway import telemetry
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.file_transport import file_request
from tests.unit_tests.extensions.test_adx_environment import config


@pytest.mark.asyncio
async def test_w3c_context_reaches_control_invoke_and_native_files(monkeypatch, tmp_path):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, content=b'payload') if request.url.path.endswith('/download') else httpx.Response(200, json={})

    client = AdxControlClient(config(monkeypatch, tmp_path), transport=httpx.MockTransport(handle))
    monkeypatch.setattr(telemetry, '_runtime', None)
    monkeypatch.delenv('OTEL_ENABLED', raising=False)
    telemetry.configure_gateway_telemetry({'telemetry': {'enabled': True, 'exporter': 'none'}})
    operation = telemetry.start_request_trace('request', 'file.mkdir', channel='web', connection_id='connection')
    try:
        with telemetry.bind_request_trace(operation):
            await client._request('GET', '/api/instances', trace_id='older-trace')
            await client.invoke('test-alice', 'file.exists', {'path': '/home/agentos'})
            async with file_request(client, 'test-alice', 'GET', '/home/agentos/file') as response:
                assert await response.aread() == b'payload'
        for request in calls:
            assert request.headers['x-trace-id'] == str(UUID(hex=operation.trace_id))
            assert request.headers['traceparent'].split('-')[1] == operation.trace_id
            assert request.headers['authorization'] == 'Bearer platform-secret'
        assert calls[1].headers['x-adx-request-id'].startswith('invoke-')
        assert json.loads(calls[1].content)['requestId'] == calls[1].headers['x-adx-request-id']
    finally:
        operation.finish('success')
        await telemetry.shutdown_gateway_telemetry()
        await client.close()
