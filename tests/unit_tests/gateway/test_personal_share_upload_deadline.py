# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Upload deadlines and cancellation must survive a completed receive on Python 3.11."""

import asyncio
import io
from contextlib import suppress

import pytest
from starlette.requests import Request

from agent_gateway.channel_manager.web import personal_share_api as api


async def _finish(task: asyncio.Task) -> None:
    for _ in range(5):
        if task.done():
            break
        task.cancel()
        await asyncio.wait({task}, timeout=0.1)
    assert task.done(), "upload did not stop during test cleanup"
    with suppress(asyncio.CancelledError, TimeoutError):
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation", ["deadline", "external"])
async def test_upload_preserves_cancellation_when_receive_completes(monkeypatch, cancellation):
    monkeypatch.setattr(api, "_UPLOAD_TIMEOUT_SECONDS", 60)
    monkeypatch.setattr(api, "_UPLOAD_IDLE_SECONDS", 60)
    started = asyncio.Event()
    ready = asyncio.Event()
    real_timeout = asyncio.timeout
    scopes = []

    def capture_timeout(delay):
        scope = real_timeout(delay)
        scopes.append(scope)
        return scope

    async def receive():
        started.set()
        await ready.wait()
        ready.clear()
        return {"type": "http.request", "body": b"x", "more_body": True}

    monkeypatch.setattr(api.asyncio, "timeout", capture_timeout)
    stream = io.BytesIO()
    request = Request({"type": "http", "headers": []}, receive=receive)
    task = asyncio.create_task(api._read_upload(request, stream, 1024))
    try:
        async with real_timeout(2):
            await started.wait()
        # Queue receive completion immediately before the outer cancellation.
        ready.set()
        if cancellation == "deadline":
            scopes[0].reschedule(asyncio.get_running_loop().time())
        else:
            asyncio.get_running_loop().call_soon(task.cancel)
        done, _ = await asyncio.wait({task}, timeout=0.5)
        assert task in done, "upload swallowed cancellation after receive completed"
        expected = TimeoutError if cancellation == "deadline" else asyncio.CancelledError
        with pytest.raises(expected):
            await task
    finally:
        await _finish(task)


@pytest.mark.asyncio
async def test_upload_total_deadline_survives_continuous_chunks(monkeypatch):
    monkeypatch.setattr(api, "_UPLOAD_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(api, "_UPLOAD_IDLE_SECONDS", 1)

    async def receive():
        await asyncio.sleep(0.005)
        return {"type": "http.request", "body": b"x", "more_body": True}

    request = Request({"type": "http", "headers": []}, receive=receive)
    task = asyncio.create_task(api._read_upload(request, io.BytesIO(), 1024 * 1024))
    try:
        done, _ = await asyncio.wait({task}, timeout=0.5)
        assert task in done, "total upload deadline did not terminate the stream"
        with pytest.raises(TimeoutError):
            await task
    finally:
        await _finish(task)


@pytest.mark.asyncio
async def test_upload_idle_deadline_cancels_receive(monkeypatch):
    monkeypatch.setattr(api, "_UPLOAD_TIMEOUT_SECONDS", 2)
    monkeypatch.setattr(api, "_UPLOAD_IDLE_SECONDS", 0.01)
    cancelled = asyncio.Event()

    async def receive():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    request = Request({"type": "http", "headers": []}, receive=receive)
    with pytest.raises(TimeoutError):
        await api._read_upload(request, io.BytesIO(), 1)
    assert cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 2, 3])
async def test_upload_keeps_exact_size_validation(monkeypatch, size):
    monkeypatch.setattr(api, "_UPLOAD_TIMEOUT_SECONDS", 2)

    async def receive():
        return {"type": "http.request", "body": b"ab", "more_body": False}

    request = Request({"type": "http", "headers": []}, receive=receive)
    stream = io.BytesIO()
    if size == 2:
        await api._read_upload(request, stream, size)
        assert stream.getvalue() == b"ab"
    else:
        with pytest.raises(ValueError, match="share_export_upload_incomplete"):
            await api._read_upload(request, stream, size)
