# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
from __future__ import annotations

import base64
import json
import logging
import time
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent_gateway.auth import account_kit
from agent_gateway.auth import remote_config
from agent_gateway.auth import session_store as store_mod
from agent_gateway.auth.account_kit import AccountKitFlow, OAuthConfig
from agent_gateway.auth.apig import ApigConfig
from agent_gateway.auth.credential_access import CredentialUpdate
from agent_gateway.auth.service import AuthService
from agent_gateway.channel_manager.web import web_http_auth

EXCHANGE_URL = "https://auth.example.com/account-kit/token"
ID_TOKEN_MARKER = "IDTOKEN-LEAK"


@pytest.fixture(autouse=True)
def _reset_remote_config():
    yield
    remote_config.set_config_for_test(None)


def _b64(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def _id_token(openid: str = "openid-1") -> str:
    payload = {"openid": openid, "sub": openid, "exp": int(time.time()) + 3600}
    return f"{_b64({'alg': 'RS256', 'kid': 'k'})}.{_b64(payload)}.{ID_TOKEN_MARKER}"


class FakeTokenEndpoint:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    def __call__(self, method, url, headers=None, data=None, **kwargs):
        self.requests.append({"url": url, "data": dict(data or {})})
        return SimpleNamespace(
            status_code=200,
            json=lambda: {
                "access_token": "AT",
                "id_token": _id_token(),
                "refresh_token": "REFRESH-LEAK",
            },
        )


class RecordingCredentialSink:
    def __init__(self) -> None:
        self.updates: list[CredentialUpdate] = []

    async def publish(self, update: CredentialUpdate) -> bool:
        self.updates.append(update)
        return True


class FailingCredentialSink:
    def __init__(self, failure) -> None:
        self.failure = failure

    async def publish(self, update: CredentialUpdate) -> bool:
        if self.failure == "raise":
            raise RuntimeError("credential sink unavailable")
        return False


def _remote(monkeypatch) -> None:
    monkeypatch.setattr(
        remote_config,
        "config_url",
        lambda: "https://config.example.com",
    )
    remote_config.set_config_for_test(
        remote_config.parse_config(
            {
                "is_effective": True,
                "huaweiaccount_login": {},
                "gateway": {"base_url": ""},
                "models": {"whitelist": []},
            }
        )
    )


def _make_injected_client(
    tmp_path,
    monkeypatch,
    *,
    sink=None,
):
    _remote(monkeypatch)
    monkeypatch.setattr(store_mod, "auth_dir", lambda: tmp_path)
    monkeypatch.setattr(account_kit, "requests_request", FakeTokenEndpoint())
    monkeypatch.setattr(remote_config, "warm_up_in_background", lambda: None)

    service = AuthService(
        flow=AccountKitFlow(
            OAuthConfig(
                client_id="118944053",
                redirect_uri="http://localhost:19000/api/v1/auth/callback",
                scope="openid profile",
                exchange_url=EXCHANGE_URL,
            )
        ),
        store=store_mod.AuthSessionStore(),
    )
    monkeypatch.setattr(service, "refresh_model_catalog", lambda session_id=None: 0)

    provider_calls = 0

    def provider():
        nonlocal provider_calls
        provider_calls += 1
        return service

    def forbidden_global():
        raise AssertionError("HTTP routes must use the injected provider")

    monkeypatch.setattr(web_http_auth, "get_auth_service", forbidden_global)

    app = FastAPI()
    web_http_auth.register_auth_routes(
        app,
        auth_service_provider=provider,
        credential_update_sink=sink,
    )
    client = TestClient(app, headers={web_http_auth.AUTH_REQUEST_HEADER: "1"})
    return SimpleNamespace(
        service=service,
        client=client,
        provider_calls=lambda: provider_calls,
    )


def _authorize(client) -> dict:
    response = client.post("/api/v1/auth/authorize")
    assert response.status_code == 200, response.text
    return response.json()


def _login(client) -> tuple[dict, str]:
    started = _authorize(client)
    callback = client.get(
        "/api/v1/auth/callback",
        params={"state": started["state"], "code": "auth-code-1"},
    )
    assert callback.status_code == 200, callback.text
    claimed = client.post(
        "/api/v1/auth/claim",
        json={"state": started["state"], "claimToken": started["claimToken"]},
    )
    assert claimed.status_code == 200, claimed.text
    return claimed.json(), claimed.headers["X-Auth-Session"]


def test_all_http_routes_use_the_injected_service_and_sink(tmp_path, monkeypatch):
    sink = RecordingCredentialSink()
    from agent_gateway.auth import login_credentials
    from agent_gateway.message_handler.message_handler import MessageHandler

    def forbidden_message_handler(cls):
        raise AssertionError("explicit sink must not query the MessageHandler singleton")

    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"k" * 32)
    monkeypatch.setattr(
        MessageHandler,
        "try_get_instance",
        classmethod(forbidden_message_handler),
    )

    injected = _make_injected_client(tmp_path, monkeypatch, sink=sink)
    client = injected.client
    service = injected.service
    assert injected.provider_calls() == 0, "route registration must not resolve the service"

    started = _authorize(client)
    pending = client.post(
        "/api/v1/auth/claim",
        json={"state": started["state"], "claimToken": started["claimToken"]},
    )
    assert pending.status_code == 202

    callback = client.get(
        "/api/v1/auth/callback",
        params={"state": started["state"], "code": "auth-code-1"},
    )
    assert callback.status_code == 200

    claimed = client.post(
        "/api/v1/auth/claim",
        json={"state": started["state"], "claimToken": started["claimToken"]},
    )
    assert claimed.status_code == 200
    user_id = claimed.json()["userId"]
    session_id = claimed.headers["X-Auth-Session"]

    status = client.get("/api/v1/auth/status", headers={"X-Auth-Session": session_id})
    assert status.status_code == 200
    assert status.json()["islogin"] is True

    cancel_started = _authorize(client)
    cancelled = client.post(
        "/api/v1/auth/cancel",
        json={
            "state": cancel_started["state"],
            "claimToken": cancel_started["claimToken"],
        },
    )
    assert cancelled.status_code == 200

    models_services: list[object] = []

    def fake_get_models(session_id, *, service=None):
        models_services.append(service)
        return [SimpleNamespace(to_dict=lambda: {"name": "login-model"})]

    monkeypatch.setattr(web_http_auth, "get_models", fake_get_models)
    models = client.get("/api/v1/auth/models", headers={"X-Auth-Session": session_id})
    assert models.status_code == 200
    assert models.json()["models"] == [{"name": "login-model"}]
    assert models_services == [service]

    from agent_gateway.auth import apig

    quota_services: list[object] = []

    def fake_resolve_id_token(session_id, *, service=None):
        quota_services.append(service)
        return "IDTOKEN-LEAK"

    monkeypatch.setattr(web_http_auth, "resolve_id_token", fake_resolve_id_token)
    monkeypatch.setattr(
        apig,
        "resolve_apig_config",
        lambda allow_refresh=True: ApigConfig(
            base_url="https://apig.example.com",
            models_path="models",
            quota_path="quota",
            invoke_path="invoke",
        ),
    )
    monkeypatch.setattr(
        apig,
        "fetch_quota",
        lambda id_token, config: SimpleNamespace(
            public_view=lambda: {"balance": 12.0, "exhausted": False}
        ),
    )
    quota = client.get("/api/v1/auth/quota", headers={"X-Auth-Session": session_id})
    assert quota.status_code == 200
    assert quota.json()["quota"]["balance"] == 12.0
    assert quota_services == [service]

    logout = client.post("/api/v1/auth/logout", headers={"X-Auth-Session": session_id})
    assert logout.status_code == 200
    assert logout.json() == {"ok": True, "islogin": False}
    assert service.resolve_session(session_id) is None

    assert injected.provider_calls() >= 8
    assert len(sink.updates) == 1
    expected_ref = login_credentials.credential_ref_for_user(user_id)
    assert sink.updates[0].credential_ref == expected_ref
    assert sink.updates[0].api_key is None
    assert sink.updates[0].revoked is True
    assert sink.updates[0].to_params() == {"credential_ref": expected_ref, "revoked": True}


@pytest.mark.parametrize("failure", [False, "raise"])
def test_logout_remains_successful_when_sink_fails(tmp_path, monkeypatch, caplog, failure):
    sink = FailingCredentialSink(failure)
    injected = _make_injected_client(tmp_path, monkeypatch, sink=sink)
    client = injected.client
    _, session_id = _login(client)

    with caplog.at_level(logging.WARNING, logger=web_http_auth.logger.name):
        response = client.post("/api/v1/auth/logout", headers={"X-Auth-Session": session_id})

    assert response.status_code == 200
    assert response.json() == {"ok": True, "islogin": False}
    assert injected.service.resolve_session(session_id) is None
    assert 'jiuwenswarm_auth=""' in response.headers.get("set-cookie", "")
    assert any("撤销 AgentServer 凭据失败" in record.message for record in caplog.records)
