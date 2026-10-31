# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.memory_rpc_handlers import (
    _read_proc_meminfo,
    register_memory_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


class _FakeProcess:
    def memory_info(self):
        class info:
            rss = 128 * 1024 * 1024
        return info()


class _FakeVirtualMemory:
    total = 2048 * 1024 * 1024
    available = 1024 * 1024 * 1024


@pytest.mark.asyncio
async def test_memory_routes_are_registered():
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=None, _resolve=lambda ref: ref)
    assert {
        "memory.compute",
        "memory.forbidden.get",
        "memory.forbidden.set",
    } <= set(channel.methods)


@pytest.mark.asyncio
async def test_memory_compute_uses_psutil_payload(monkeypatch):
    from agent_gateway.channel_manager.web import memory_rpc_handlers

    class fake_psutil:
        @staticmethod
        def Process():
            return _FakeProcess()

        @staticmethod
        def virtual_memory():
            return _FakeVirtualMemory()

    monkeypatch.setattr(memory_rpc_handlers, "_HAS_PSUTIL", True)
    monkeypatch.setattr(memory_rpc_handlers, "_psutil", fake_psutil)
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=None, _resolve=lambda ref: ref)

    await channel.methods["memory.compute"](object(), "rpc-0", {}, "session-0")

    assert channel.responses == [
        {"req_id": "rpc-0", "ok": True, "payload": {"rss_mb": 128.0, "total_mb": 2048.0, "available_mb": 1024.0}}
    ]


@pytest.mark.asyncio
async def test_memory_compute_falls_back_to_proc_values(monkeypatch):
    from agent_gateway.channel_manager.web import memory_rpc_handlers

    monkeypatch.setattr(memory_rpc_handlers, "_HAS_PSUTIL", False)
    monkeypatch.setattr(memory_rpc_handlers, "_read_proc_meminfo", lambda: (1.0, 2.0, 3.0))
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=None, _resolve=lambda ref: ref)

    await channel.methods["memory.compute"](object(), "rpc-0", {}, "session-0")

    assert channel.responses == [
        {"req_id": "rpc-0", "ok": True, "payload": {"rss_mb": 1.0, "total_mb": 2.0, "available_mb": 3.0}}
    ]


def test_read_proc_meminfo_parses_proc_values(monkeypatch):
    import builtins
    import io

    meminfo = io.StringIO("MemTotal: 2048 kB\nMemAvailable: 1024 kB\nMemFree: 512 kB\n")
    status = io.StringIO("Name:\tproc\nVmRSS:\t256 kB\n")

    def fake_open(path, *args, **kwargs):
        if path == "/proc/meminfo":
            return meminfo
        if path == "/proc/self/status":
            return status
        raise AssertionError(path)

    monkeypatch.setattr(builtins, "open", fake_open)
    assert _read_proc_meminfo() == (0.25, 2.0, 1.0)


@pytest.mark.asyncio
async def test_memory_forbidden_get_uses_local_config_for_legacy_client(monkeypatch):
    from agent_gateway.channel_manager.web import memory_rpc_handlers

    monkeypatch.setattr(
        memory_rpc_handlers,
        "get_config",
        lambda: {"memory": {"forbidden_memory_definition": {"enabled": True}}},
    )
    monkeypatch.setattr(
        memory_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: True,
    )
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=object(), _resolve=lambda ref: ref)

    await channel.methods["memory.forbidden.get"](
        object(), "rpc-1", {}, "session-1"
    )

    assert channel.responses == [
        {"req_id": "rpc-1", "ok": True, "payload": {"enabled": True}}
    ]


@pytest.mark.asyncio
async def test_memory_forbidden_set_rejects_invalid_params():
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=None, _resolve=lambda ref: ref)

    await channel.methods["memory.forbidden.set"](
        object(), "rpc-2", "bad", "session-2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "params must be object",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_memory_forbidden_set_forwards_remote_client(monkeypatch):
    from agent_gateway.channel_manager.web import memory_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(memory_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    monkeypatch.setattr(
        memory_rpc_handlers,
        "is_legacy_shared_directory_client",
        lambda client: False,
    )
    channel = _FakeChannel()
    register_memory_handlers(channel, agent_client=object(), _resolve=lambda ref: ref)

    await channel.methods["memory.forbidden.set"](
        object(), "rpc-3", {"enabled": False}, "session-3"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {"enabled": False}
    assert forwarded[0]["req_method"].value == "memory.forbidden.set"
    assert channel.responses == []
