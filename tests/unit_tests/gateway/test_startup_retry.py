# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio

import pytest

from agent_gateway.startup_retry import (
    _connect_with_retry,
    _wait_for_web_channel_listening,
)


class _ReadySocket:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _FakeClient:
    def __init__(self):
        self.connect_calls = []

    async def connect(self, uri):
        self.connect_calls.append(uri)


async def test_connect_with_retry_uses_local_exponential_backoff(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.startup_retry.socket.create_connection",
        lambda *_, **__: _ReadySocket(),
    )
    sleeps = []

    async def fast_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    client = _FakeClient()
    client.connect_results = [RuntimeError("first failure"), None]

    async def connect(uri):
        client.connect_calls.append(uri)
        result = client.connect_results[len(client.connect_calls) - 1]
        if isinstance(result, Exception):
            raise result

    client.connect = connect

    await _connect_with_retry(client, "ws://127.0.0.1:19000/agent", max_retries=3)

    assert client.connect_calls == ["ws://127.0.0.1:19000/agent"] * 2
    assert sleeps == [0.05]


async def test_connect_with_retry_raises_after_remote_retry_limit(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.startup_retry.socket.create_connection",
        lambda *_, **__: _ReadySocket(),
    )
    client = _FakeClient()

    async def connect(uri):
        client.connect_calls.append(uri)
        raise OSError("handshake refused")

    client.connect = connect

    with pytest.raises(OSError, match="handshake refused"):
        await _connect_with_retry(client, "ws://agent.example.com/agent", max_retries=1)

    assert client.connect_calls == ["ws://agent.example.com/agent"]


async def test_wait_for_web_channel_listening_returns_when_started():
    class _Channel:
        class config:
            host = "127.0.0.1"
            port = 19000
            path = "/web"

        _uvicorn_server = type("Server", (), {"started": True})()

    channel = _Channel()

    await _wait_for_web_channel_listening(channel, asyncio.Future(), timeout=0.05)


async def test_wait_for_web_channel_listening_times_out():
    class _Channel:
        config = type("Config", (), {})()

    with pytest.raises(TimeoutError):
        await _wait_for_web_channel_listening(
            _Channel(), asyncio.Future(), timeout=0.02,
        )
