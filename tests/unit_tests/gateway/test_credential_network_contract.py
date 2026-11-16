# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real Web-to-AgentServer network contract for the explicit credential path."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import pytest
import uvicorn
from websockets.asyncio.client import connect

from agent_gateway.auth.account_kit import AccountKitFlow, Credential, LoginOutcome, OAuthConfig
from agent_gateway.auth import login_credentials
from agent_gateway.auth import model_catalog
from agent_gateway.auth import remote_config
from agent_gateway.auth import service as service_mod
from agent_gateway.auth import session_owners
from agent_gateway.auth import session_store as store_mod
from agent_gateway.auth.service import AuthService
from testcases_gateway.stub_agent_server import StubAgentServer


LOGIN_USER_ID = "openid-network-contract"
BUSINESS_USER_ID = "business-network-contract"
MODEL_NAME = "network-login-model"
ID_TOKEN = "server-id-token-network-contract"
API_BASE = "https://apig.example.com/invoke"


class _NoopBus:
    async def publish_user_messages(self, msg: Any) -> None:
        return None

    async def route_incoming_message(self, msg: Any) -> None:
        return None


async def _wait_for_uvicorn(server: uvicorn.Server) -> int:
    deadline = time.monotonic() + 10.0
    while not server.started:
        if time.monotonic() >= deadline:
            raise TimeoutError("WebChannel app did not start")
        await asyncio.sleep(0.01)
    sockets = server.servers[0].sockets
    return int(sockets[0].getsockname()[1])


async def _receive_terminal_event(ws) -> dict[str, Any]:
    deadline = time.monotonic() + 15.0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("WebChannel did not relay a terminal chat event")
        frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
        if frame.get("type") == "event" and frame.get("event") in {"chat.final", "chat.error"}:
            return frame


def _request_by_method(stub: StubAgentServer, method: str) -> dict[str, Any]:
    return next(request for request in stub.requests if request.get("method") == method)


@pytest.mark.asyncio
async def test_web_ws_and_http_use_explicit_credentials_over_real_as_network(
    tmp_path,
    monkeypatch,
):
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.application.services import MessageHandlerCredentialUpdateSink
    from agent_gateway.auth import apig
    from agent_gateway.auth.local_credential_access import LocalCredentialAccess
    from agent_gateway.auth.session_login_bindings import LocalSessionLoginBindings
    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    auth_dir = tmp_path / "auth"
    auth_dir.mkdir()
    monkeypatch.setattr(store_mod, "auth_dir", lambda: auth_dir)
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(tmp_path / "gateway-logs"))
    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"network-contract-key" * 4)
    monkeypatch.setattr(remote_config, "warm_up_in_background", lambda: None)

    remote_config.set_config_for_test(
        remote_config.parse_config(
            {
                "is_effective": True,
                "huaweiaccount_login": {},
                "gateway": {
                    "base_url": "https://apig.example.com",
                    "models_path": "/models",
                    "quota_path": "/quota",
                    "invoke_path": "/invoke",
                },
                "models": {"whitelist": [], "blocklist": []},
            }
        )
    )

    discovered_tokens: list[str] = []

    def fake_fetch_models(id_token: str, config: Any) -> list[dict[str, str]]:
        discovered_tokens.append(id_token)
        return [{"id": MODEL_NAME, "name": MODEL_NAME}]

    monkeypatch.setattr(apig, "fetch_models", fake_fetch_models)

    def forbidden_global_service() -> AuthService:
        raise AssertionError("the network contract must use the explicit AuthService provider")

    monkeypatch.setattr(service_mod, "get_auth_service", forbidden_global_service)
    monkeypatch.setattr(MessageHandler, "_instance", None)

    async def no_chat_request_hook(_handler: MessageHandler, _msg: Any) -> None:
        return None

    monkeypatch.setattr(
        MessageHandler,
        "_trigger_before_chat_request_hook",
        no_chat_request_hook,
    )

    model_catalog.clear_cache()
    session_owners.reset_for_test()
    stub = StubAgentServer().start()
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    handler: MessageHandler | None = None
    manager: ChannelManager | None = None
    client = WebSocketAgentServerClient(ping_interval=None, ping_timeout=None)

    try:
        store = store_mod.AuthSessionStore()
        session = store.create(
            LoginOutcome(
                credential=Credential(
                    id_token=ID_TOKEN,
                    refresh_token="unused-refresh-token",
                    expires_at=time.time() + 3600.0,
                ),
                user_id=LOGIN_USER_ID,
                user_name="Network Contract",
            )
        )
        service = AuthService(
            flow=AccountKitFlow(
                OAuthConfig(
                    client_id="network-contract-client",
                    redirect_uri="http://127.0.0.1:1/callback",
                    scope="openid profile",
                    exchange_url="https://auth.example.com/token",
                )
            ),
            store=store,
        )
        assert service.refresh_model_catalog(session.session_id) == 1
        assert discovered_tokens == [ID_TOKEN]

        auth_service_provider = lambda: service  # noqa: E731
        credential_access = LocalCredentialAccess(auth_service_provider)

        handler = MessageHandler(client)
        handler.set_credential_access(credential_access)
        bindings = LocalSessionLoginBindings(auth_service_provider)
        handler.set_session_login_bindings(bindings)
        manager = ChannelManager(handler)
        handler.set_channel_manager(manager)
        sink = MessageHandlerCredentialUpdateSink(handler)

        web_channel = WebChannel(
            WebChannelConfig(enabled=True, host="127.0.0.1", port=0, path="/ws"),
            _NoopBus(),
            agent_client=client,
            auth_service_provider=auth_service_provider,
            credential_update_sink=sink,
        )
        preprocessor = MessagePreprocessor(
            lambda msg: None,
            lambda msg: msg,
            lambda msg: None,
        ).with_credential_access(credential_access)
        manager.register_channel_with_inbound(
            web_channel,
            preprocessor.for_channel(manager, {"chat.send"}, {"chat.send"}, "Web"),
        )

        await client.connect(stub.url)
        await handler.start_forwarding()
        await manager.start_dispatch()

        app = build_web_channel_app(web_channel)
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=0,
                log_level="warning",
                lifespan="off",
            )
        )
        server_task = asyncio.create_task(server.serve(), name="credential-network-web")
        web_port = await _wait_for_uvicorn(server)
        web_base_url = f"http://127.0.0.1:{web_port}"

        forged_auth = {
            "api_base": "https://forged.example.com/invoke",
            "api_key": "forged-client-token",
            "credential_ref": "forged-credential-ref",
        }
        request_id = "credential-network-request"
        async with connect(
            f"ws://127.0.0.1:{web_port}/ws",
            additional_headers={
                "X-Auth-Session": session.session_id,
                "X-User-Id": BUSINESS_USER_ID,
            },
            open_timeout=10,
        ) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "req",
                        "id": request_id,
                        "method": "chat.send",
                        "is_stream": True,
                        "params": {
                            "session_id": "network-contract-session",
                            "content": "server credential please",
                            "model_name": MODEL_NAME,
                            "_model_auth": forged_auth,
                        },
                    }
                )
            )
            terminal = await _receive_terminal_event(ws)

        assert terminal["event"] == "chat.final"
        chat_request = _request_by_method(stub, "chat.send")
        expected_ref = login_credentials.credential_ref_for_user(LOGIN_USER_ID)
        assert chat_request["request_id"] == request_id
        assert chat_request["channel"] == "web"
        assert chat_request["user_id"] == BUSINESS_USER_ID
        assert chat_request["params"]["query"] == "server credential please"
        assert chat_request["params"]["_model_auth"] == {
            "api_base": API_BASE,
            "api_key": ID_TOKEN,
            "credential_ref": expected_ref,
        }
        assert "forged-client-token" not in json.dumps(chat_request)

        binding = await bindings.lookup("network-contract-session")
        assert binding.credential_ref == expected_ref
        assert binding.model_name == MODEL_NAME
        from e2a.gateway_normalize import e2a_from_agent_fields
        from e2a.message import ReqMethod

        im_envelope = e2a_from_agent_fields(
            request_id="credential-network-im-reply",
            req_method=ReqMethod.CHAT_SEND,
            channel_id="feishu",
            session_id="network-contract-session",
            params={"query": "reply without login cookie", "model_name": MODEL_NAME},
            timestamp=time.time(),
            user_id="im-business-user",
        )
        assert "_model_auth" not in im_envelope.params
        assert (await handler._send_non_stream_agent_request(im_envelope)).ok
        im_request = next(request for request in stub.requests
                          if request.get("request_id") == "credential-network-im-reply")
        assert im_request["channel"] == "feishu"
        assert im_request["user_id"] == "im-business-user"
        assert im_request["params"]["_model_auth"] == chat_request["params"]["_model_auth"]

        async with httpx.AsyncClient(base_url=web_base_url) as http_client:
            status_response = await http_client.get(
                "/api/v1/auth/status",
                headers={"X-Auth-Session": session.session_id},
            )
            assert status_response.status_code == 200
            assert status_response.json()["islogin"] is True
            assert status_response.json()["userId"] == LOGIN_USER_ID

            logout_response = await http_client.post(
                "/api/v1/auth/logout",
                headers={
                    "X-Auth-Session": session.session_id,
                    "X-Jiuwen-Auth": "1",
                },
            )

        assert logout_response.status_code == 200
        assert logout_response.json() == {"ok": True, "islogin": False}
        assert service.resolve_session(session.session_id) is None
        # Persisted binding remains, but it must no longer yield a revoked account's token.
        assert await bindings.lookup("network-contract-session") == binding
        assert await bindings.model_auth_for_binding(binding) is None

        update_request = _request_by_method(stub, "auth.credentials.update")
        assert update_request["params"] == {
            "credential_ref": expected_ref,
            "revoked": True,
        }
    finally:
        if server is not None and server_task is not None:
            server.should_exit = True
            try:
                await asyncio.wait_for(server_task, timeout=5.0)
            except asyncio.TimeoutError:
                server_task.cancel()
                await asyncio.gather(server_task, return_exceptions=True)
        if manager is not None:
            await manager.stop_dispatch()
        if handler is not None:
            await handler.stop_forwarding()
        await client.disconnect()
        stub.stop()
        model_catalog.clear_cache()
        session_owners.reset_for_test()
        remote_config.set_config_for_test(None)
