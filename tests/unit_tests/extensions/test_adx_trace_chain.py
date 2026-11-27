"""Warmup traceparent reaches ADX create with one key and a real parent."""

import asyncio

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_gateway import telemetry
from tests.unit_tests.extensions.test_adx_router import _config, _router

CALLER_TRACE_HEX = "6a945399111141118111111111111111"
UPSTREAM_SPAN_HEX = "b" * 16


@pytest.mark.asyncio
async def test_warmup_traceparent_reaches_adx_create(monkeypatch):
    monkeypatch.setattr(telemetry, "_runtime", None)
    runtime = telemetry.configure_gateway_telemetry(
        {"telemetry": {"enabled": True, "exporter": "none"}},
    )
    exporter = InMemorySpanExporter()
    runtime.provider.add_span_processor(SimpleSpanProcessor(exporter))
    router, _ = _router(_config(monkeypatch))
    await router.connect("")
    try:
        await router._warmup_user_agent("alice", trace_context={
            "traceparent": f"00-{CALLER_TRACE_HEX}-{UPSTREAM_SPAN_HEX}-01",
        })
        pending = list(getattr(router, "_background_tasks", ()))
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        spans = {span.name: span for span in exporter.get_finished_spans()}
        warmup = spans["gw.connect.warmup"]
        create = spans["gw.agent.create"]
        post = next(r for r in router.test_api.requests if r.method == "POST")
        parts = post.headers["traceparent"].split("-")

        assert warmup.context.trace_id == int(CALLER_TRACE_HEX, 16)
        assert warmup.parent.span_id == int(UPSTREAM_SPAN_HEX, 16)
        assert create.context.trace_id == int(CALLER_TRACE_HEX, 16)
        assert create.parent.span_id == warmup.context.span_id
        assert parts[1] == CALLER_TRACE_HEX
        assert parts[2] == format(create.context.span_id, "016x")
    finally:
        await router.shutdown()
