# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from openjiuwen.extensions.external_provider.openai_auth.openai_account_auth import (
    OpenAIAccountAuthError,
)

from agent_gateway.channel_manager.web.openai_account_rpc_handlers import (
    register_openai_account_handlers,
)


EXPECTED_ROUTES = [
    "openai_account.auth.status",
    "openai_account.auth.start_login",
    "openai_account.auth.pending_login",
    "openai_account.auth.poll_login",
    "openai_account.auth.logout",
    "openai_account.models.list",
]


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


def _register():
    channel = _FakeChannel()
    register_openai_account_handlers(channel)
    return channel


def _status(**overrides):
    values = {
        "authenticated": False,
        "auth_path": "/tmp/auth.json",
        "has_refresh_token": True,
        "expires_at": 123.0,
        "needs_refresh": False,
        "error": None,
    }
    values.update(overrides)
    return type("Status", (), values)()


@pytest.fixture(autouse=True)
def _clear_login_jobs():
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    module._clear_openai_account_login_jobs()
    yield
    module._clear_openai_account_login_jobs()


def test_openai_account_routes_are_registered_in_original_order():
    channel = _register()

    assert list(channel.methods) == EXPECTED_ROUTES


@pytest.mark.asyncio
async def test_auth_status_returns_manager_status(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    manager = type("Manager", (), {"status": lambda self: _status(), "base_url": "https://example.com"})()
    monkeypatch.setattr(module, "OpenAIAccountAuthManager", lambda: manager)
    channel = _register()

    await channel.methods["openai_account.auth.status"](
        object(), "rpc-1", {}, "session-1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": True,
            "payload": {
                "authenticated": False,
                "auth_path": "/tmp/auth.json",
                "has_refresh_token": True,
                "expires_at": 123.0,
                "needs_refresh": False,
                "error": None,
                "base_url": "https://example.com",
            },
        }
    ]


@pytest.mark.asyncio
async def test_start_login_stores_recent_job(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    now = 1000.0
    device_code = type(
        "DeviceCode",
        (),
        {
            "user_code": "abc-def",
            "verification_uri": "https://example.com/device",
            "interval": 3,
            "expires_in": 300,
        },
    )()
    manager = type(
        "Manager",
        (),
        {
            "status": lambda self: _status(),
            "base_url": "https://example.com",
            "start_device_login": lambda self: device_code,
        },
    )()

    def poll_device_login(_device_code):
        raise AssertionError("start_login should not poll immediately")

    manager.poll_device_login = poll_device_login
    monkeypatch.setattr(module, "OpenAIAccountAuthManager", lambda: manager)
    monkeypatch.setattr(module, "time", type("Time", (), {"time": staticmethod(lambda: now)})())
    channel = _register()

    await channel.methods["openai_account.auth.start_login"](
        object(), "rpc-2", {}, "session-2"
    )

    assert len(module._OPENAI_ACCOUNT_LOGIN_JOBS) == 1
    job = next(iter(module._OPENAI_ACCOUNT_LOGIN_JOBS.values()))
    assert job.device_code is device_code
    assert job.created_at == now
    assert job.expires_at == now + 300
    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": True,
            "payload": {
                "status": "pending",
                "login_id": next(iter(module._OPENAI_ACCOUNT_LOGIN_JOBS)),
                "user_code": "abc-def",
                "verification_uri": "https://example.com/device",
                "interval": 3,
                "expires_in": 300,
                "expires_at": 1300.0,
                "auth": {
                    "authenticated": False,
                    "auth_path": "/tmp/auth.json",
                    "has_refresh_token": True,
                    "expires_at": 123.0,
                    "needs_refresh": False,
                    "error": None,
                    "base_url": "https://example.com",
                },
            },
        }
    ]


@pytest.mark.asyncio
async def test_pending_login_reports_none_without_jobs(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    manager = type("Manager", (), {"status": lambda self: _status(), "base_url": "https://example.com"})()
    monkeypatch.setattr(module, "OpenAIAccountAuthManager", lambda: manager)
    channel = _register()

    await channel.methods["openai_account.auth.pending_login"](
        object(), "rpc-3", {}, "session-3"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": True,
            "payload": {
                "status": "none",
                "auth": {
                    "authenticated": False,
                    "auth_path": "/tmp/auth.json",
                    "has_refresh_token": True,
                    "expires_at": 123.0,
                    "needs_refresh": False,
                    "error": None,
                    "base_url": "https://example.com",
                },
            },
        }
    ]


@pytest.mark.asyncio
async def test_poll_login_removes_expired_login(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    now = 3_000_000_000.0
    device_code = object()
    job = module._OpenAIAccountLoginJob(device_code, now, now - 1.0)
    module._store_openai_account_login_job("login-1", job, now)
    monkeypatch.setattr(
        module, "time", type("Time", (), {"time": staticmethod(lambda: now)})()
    )
    channel = _register()

    await channel.methods["openai_account.auth.poll_login"](
        object(), "rpc-4", {"login_id": "login-1"}, "session-4"
    )

    assert module._OPENAI_ACCOUNT_LOGIN_JOBS == {}
    assert channel.responses == [
        {
            "req_id": "rpc-4",
            "ok": True,
            "payload": {"status": "expired", "authenticated": False},
        }
    ]


@pytest.mark.asyncio
async def test_poll_login_maps_relogin_required_error(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    now = 3_000_000_000.0
    device_code = object()
    job = module._OpenAIAccountLoginJob(device_code, now, now + 60)
    module._store_openai_account_login_job("login-1", job, now)
    manager = type(
        "Manager",
        (),
        {
            "poll_device_login": lambda self, _device: (_ for _ in ()).throw(
                OpenAIAccountAuthError(
                    "login expired",
                    code="AUTH_EXPIRED",
                    relogin_required=True,
                )
            ),
        },
    )()
    monkeypatch.setattr(module, "OpenAIAccountAuthManager", lambda: manager)
    channel = _register()

    await channel.methods["openai_account.auth.poll_login"](
        object(), "rpc-5", {"login_id": "login-1"}, "session-5"
    )

    assert module._OPENAI_ACCOUNT_LOGIN_JOBS == {}
    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": False,
            "error": "login expired",
            "code": "AUTH_EXPIRED",
            "payload": {
                "status": "error",
                "error": "login expired",
                "code": "AUTH_EXPIRED",
                "status_code": None,
                "relogin_required": True,
            },
        }
    ]


@pytest.mark.asyncio
async def test_models_list_returns_catalog(monkeypatch):
    from agent_gateway.channel_manager.web import openai_account_rpc_handlers as module

    manager = type(
        "Manager",
        (),
        {
            "status": lambda self: _status(),
            "base_url": "https://example.com",
        },
    )()
    catalog = type(
        "Catalog",
        (),
        {
            "list_model_ids": lambda self, **kwargs: ["gpt-5", "gpt-5-mini"],
        },
    )()
    monkeypatch.setattr(module, "OpenAIAccountAuthManager", lambda: manager)
    monkeypatch.setattr(module, "OpenAIAccountModelCatalog", lambda **kwargs: catalog)
    channel = _register()

    await channel.methods["openai_account.models.list"](
        object(), "rpc-6", {}, "session-6"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-6",
            "ok": True,
            "payload": {
                "models": ["gpt-5", "gpt-5-mini"],
                "base_url": "https://example.com",
                "auth": {
                    "authenticated": False,
                    "auth_path": "/tmp/auth.json",
                    "has_refresh_token": True,
                    "expires_at": 123.0,
                    "needs_refresh": False,
                    "error": None,
                    "base_url": "https://example.com",
                },
            },
        }
    ]
