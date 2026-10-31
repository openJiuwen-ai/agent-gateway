# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.path_rpc_handlers import register_path_handlers


EXPECTED_ROUTES = [
    "path.get",
    "path.set",
    "path.select_directory",
    "path.select_files",
    "path.select_file",
]


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []
        self.channel_id = "web"

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


def _register(**callbacks):
    channel = _FakeChannel()
    defaults = {
        "_clear_agent_config_cache": lambda client: None,
        "_restart_agent_browser_runtime": lambda client, **kwargs: None,
    }
    defaults.update(callbacks)
    register_path_handlers(channel, object(), lambda ref: ref, **defaults)
    return channel


def test_path_routes_are_registered_in_original_order():
    channel = _register()

    assert list(channel.methods) == EXPECTED_ROUTES


@pytest.mark.asyncio
async def test_path_get_legacy_default_payload(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    def fail_proxy(**kwargs):
        raise AssertionError("legacy client should not use unary proxy")

    monkeypatch.setattr(path_rpc_handlers, "proxy_unary_request", fail_proxy)
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", lambda client: True)
    monkeypatch.setattr(path_rpc_handlers, "browser_decision_state", lambda config: {})
    monkeypatch.setattr(path_rpc_handlers, "get_config", lambda: {"browser": {}})
    channel = _register()

    await channel.methods["path.get"](object(), "rpc-1", {}, "session-1")

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": True,
            "payload": {"chrome_path": "", "headless": True},
        }
    ]


@pytest.mark.asyncio
async def test_path_get_remote_forwards_unary_request(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    def legacy_true(_client):
        return False

    monkeypatch.setattr(path_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", legacy_true)
    channel = _register()

    await channel.methods["path.get"](object(), "rpc-2", {}, "session-2", user_id="u2")

    assert len(forwarded) == 1
    assert forwarded[0]["req_method"].value == "path.get"
    assert forwarded[0]["label"] == "path.get"
    assert forwarded[0]["user_id"] == "u2"
    assert channel.responses == []


@pytest.mark.asyncio
async def test_path_set_rejects_non_string_chrome_path():
    channel = _register()

    await channel.methods["path.set"](
        object(), "rpc-3", {"chrome_path": 123}, "session-3"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": False,
            "error": "chrome_path must be string",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_path_set_remote_fetches_previous_config(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    fetched = []
    forwarded = []
    cleared = []
    restarted = []

    async def fetch_agent_unary(**kwargs):
        fetched.append(kwargs)
        return True, {"chrome_path": "chrome.exe", "headless": False}

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)
        on_done = kwargs.get("on_done")
        if on_done:
            await on_done(True, {})

    async def clear_cache(client):
        cleared.append(client)

    async def restart(client, **kwargs):
        restarted.append((client, kwargs))

    def legacy_true(_client):
        return False

    monkeypatch.setattr(path_rpc_handlers, "fetch_agent_unary", fetch_agent_unary)
    monkeypatch.setattr(path_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", legacy_true)
    channel = _register(
        _clear_agent_config_cache=clear_cache,
        _restart_agent_browser_runtime=restart,
    )

    await channel.methods["path.set"](
        object(), "rpc-4", {"chrome_path": " chrome.exe "}, "session-4", user_id="u4"
    )

    assert len(fetched) == 1
    assert fetched[0]["req_method"].value == "path.get"
    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {"chrome_path": "chrome.exe", "headless": True}
    assert forwarded[0]["label"] == "path.set"
    assert cleared
    assert restarted
    assert restarted[0][1]["previous_chrome_path"] == "chrome.exe"
    assert restarted[0][1]["previous_headless"] is False


@pytest.mark.asyncio
async def test_path_set_legacy_writes_config_and_restarts_runtime(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    writes = []
    decision_writes = []
    cleared = []
    restarted = []

    def write_browser(updates):
        writes.append(updates)

    def write_decision(updates):
        decision_writes.append(updates)

    async def clear_cache(client):
        cleared.append(client)

    async def restart(client, **kwargs):
        restarted.append((client, kwargs))

    def is_legacy(_client):
        return True

    def decision_updates(*args, **kwargs):
        return {"mode": "llm"}, None

    monkeypatch.setattr(path_rpc_handlers, "update_browser_in_config", write_browser)
    monkeypatch.setattr(path_rpc_handlers, "update_browser_decision_in_config", write_decision)
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", is_legacy)
    monkeypatch.setattr(path_rpc_handlers, "browser_decision_updates", decision_updates)
    monkeypatch.setattr(path_rpc_handlers, "browser_decision_state", lambda config: {})
    channel = _register(
        _clear_agent_config_cache=clear_cache,
        _restart_agent_browser_runtime=restart,
    )

    await channel.methods["path.set"](
        object(), "rpc-5", {"chrome_path": " chrome.exe ", "headless": False}, "session-5"
    )

    assert writes == [{"chrome_path": "chrome.exe", "headless": False}]
    assert decision_writes == [{"mode": "llm"}]
    assert cleared
    assert restarted
    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": True,
            "payload": {"chrome_path": "chrome.exe", "headless": False},
        }
    ]


@pytest.mark.asyncio
async def test_path_select_directory_legacy_uses_native_picker(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    calls = []

    def select_directory_native(initial_dir):
        calls.append(initial_dir)
        return "/tmp/project"

    monkeypatch.setattr(
        path_rpc_handlers, "select_directory_native", select_directory_native
    )
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", lambda client: True)
    channel = _register()

    await channel.methods["path.select_directory"](
        object(), "rpc-6", {"initial_dir": " /tmp "}, "session-6"
    )

    assert calls == ["/tmp"]
    assert channel.responses == [
        {
            "req_id": "rpc-6",
            "ok": True,
            "payload": {"path": "/tmp/project", "cancelled": False},
        }
    ]


@pytest.mark.asyncio
async def test_path_select_files_legacy_uses_file_picker(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    calls = []

    def select_and_describe_files(allow_multiple, initial_dir):
        calls.append((allow_multiple, initial_dir))
        return []

    monkeypatch.setattr(
        path_rpc_handlers, "select_and_describe_files", select_and_describe_files
    )
    monkeypatch.setattr(path_rpc_handlers, "is_legacy_shared_directory_client", lambda client: True)
    channel = _register()

    await channel.methods["path.select_files"](
        object(), "rpc-7", {"allow_multiple": True}, "session-7"
    )

    assert calls == [(True, None)]
    assert channel.responses == [
        {
            "req_id": "rpc-7",
            "ok": True,
            "payload": {"files": [], "cancelled": False},
        }
    ]


@pytest.mark.asyncio
async def test_path_select_file_rejects_non_string_title(monkeypatch):
    from agent_gateway.channel_manager.web import path_rpc_handlers

    calls = []

    def select_file_native(initial_dir, title):
        calls.append((initial_dir, title))

    monkeypatch.setattr(path_rpc_handlers, "select_file_native", select_file_native)
    channel = _register()

    await channel.methods["path.select_file"](
        object(), "rpc-8", {"title": 123}, "session-8"
    )

    assert calls == []
    assert channel.responses == [
        {
            "req_id": "rpc-8",
            "ok": False,
            "error": "title must be string",
            "code": "BAD_REQUEST",
        }
    ]
