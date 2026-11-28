# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from websockets.asyncio.client import connect

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.auth import apig, login_credentials, model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.capabilities import management_preflight
from testcases_gateway.stub_agent_server import StubAgentServer


@pytest.mark.skipif(not management_preflight().supported, reason="requires Linux/macOS fd passing")
async def test_control_account_routes_and_worker_credential_revocation(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    monkeypatch.setenv("JIUWENSWARM_CONFIG_URL", "off")
    monkeypatch.setattr("agent_gateway.auth.remote_config.warm_up_in_background", lambda: None)
    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"c" * 32)
    monkeypatch.setattr(model_catalog, "get_models", lambda *a, **k: [model_catalog.LoginModel("remote-model", "Remote")])
    monkeypatch.setattr(apig, "resolve_apig_config", lambda **k: SimpleNamespace(invoke_base_url="https://test.invalid"))
    store = AuthSessionStore(persist=False)
    auth = AuthService(store=store)
    session = store.create(SimpleNamespace(user_id="account-a", user_name="Account A", credential=Credential(
        id_token="synthetic-token", refresh_token="synthetic-refresh", expires_at=time.time() + 3600,
    )))
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    calls = []
    original = ControlCredentialServices._issued_request

    async def observe(self, identity, arguments):
        response = await original(self, identity, arguments)
        calls.append((bool(arguments.get("auth_session")), arguments.get("model_name"), response.get("status")))
        return response

    monkeypatch.setattr(ControlCredentialServices, "_issued_request", observe)
    stub = StubAgentServer().start()
    group = WorkerGroup(stub.url, workers=2, web_port=0, protocol_port=0, auth_service_provider=lambda: auth)
    try:
        await group.start()
        host, port = group.addresses[0]
        headers = {"X-Auth-Session": session.session_id, "X-Jiuwen-Auth": "1"}
        async with httpx.AsyncClient(base_url=f"http://{host}:{port}", headers=headers, trust_env=False) as http:
            status = await http.get("/api/v1/auth/status")
            assert status.json()["islogin"] is True
            assert status.headers["connection"] == "close"
            assert sum(group.pool.adopted_counts) == 0
            async with connect(f"ws://{host}:{port}/ws?user_id=alice", additional_headers=headers) as web:
                async def chat(request_id):
                    await web.send(json.dumps({"type": "req", "id": request_id, "method": "chat.send",
                                               "params": {"session_id": "acct-chat", "content": "hello",
                                                          "model_name": "remote-model"}}))
                    async with asyncio.timeout(7):
                        while True:
                            msg = json.loads(await web.recv())
                            if msg.get("event") == "chat.final" or msg.get("id") == request_id and msg.get("ok") is False:
                                return msg

                assert (await chat("account-live"))["event"] == "chat.final"
                sent = next(req for req in stub.requests if req["request_id"] == "account-live")
                assert (True, "remote-model", "ok") in calls
                assert sent["params"].get("_model_auth", {}).get("api_key") == "synthetic-token"
                assert sent["user_id"] == "alice"
                assert (await http.post("/api/v1/auth/logout")).json()["islogin"] is False
                assert (await http.get("/api/v1/auth/status")).json()["islogin"] is False
                async with asyncio.timeout(3):
                    while not any(r.get("method") == "auth.credentials.update" and r["params"].get("revoked")
                                  for r in stub.requests):
                        await asyncio.sleep(0.01)
                failure = await chat("account-revoked")
                assert failure["ok"] is False and failure["code"] == "CREDENTIAL_SERVICE_FAILED"
                assert not any(r.get("request_id") == "account-revoked" for r in stub.requests)
    finally:
        try:
            await group.stop()
        finally:
            await asyncio.to_thread(stub.stop)
    assert all(process.returncode is not None for process in group.pool.processes)
