import asyncio
import hashlib
import json
import os
import socket
import time
from types import SimpleNamespace

import pytest

from e2a.agent_models import AgentResponseChunk
from e2a.wire_codec import encode_agent_chunk_for_wire

from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceClient,
    ControlServiceIdentity,
    ControlServiceServer,
)


@pytest.fixture
def isolated_routed_environment(tmp_path, monkeypatch):
    for name in ("home", "data", "logs"):
        (tmp_path / name).mkdir()
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    session_owners.reset_for_test()
    yield tmp_path
    session_owners.reset_for_test()


class RecordingOperationClient:
    def __init__(self, server_id):
        self.server_id = server_id
        self.requests = []
        self.runtime = None

    def set_credential_runtime(self, runtime):
        self.runtime = runtime

    async def send_request(self, envelope, *, on_written=None):
        self.requests.append(dict(envelope.params))
        if on_written is not None:
            on_written()
        return SimpleNamespace(ok=True, payload={"applied": True})


def _identity():
    return ControlServiceIdentity("routed-group", 0, 1, os.getpid(), os.getpid())


def _channel_pair():
    left, right = socket.socketpair()
    return ControlServiceChannel(left), ControlServiceChannel(right)


@pytest.fixture
async def routed_pair(isolated_routed_environment, monkeypatch):
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    store = AuthSessionStore(persist=False)
    sessions = {
        user: store.create(SimpleNamespace(
            user_id=user,
            user_name=user,
            credential=Credential(
                id_token=f"{user}-token",
                refresh_token=f"{user}-refresh",
                expires_at=time.time() + 3600,
            ),
        ))
        for user in ("alice", "bob")
    }
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"r" * 32)
    monkeypatch.setattr(model_catalog, "get_models", lambda *args, **kwargs: [
        model_catalog.LoginModel("login-model", "Login"),
    ])
    monkeypatch.setattr(apig, "resolve_apig_config", lambda **kwargs: SimpleNamespace(
        invoke_base_url="https://test.invalid/invoke",
    ))
    authority = ControlCredentialServices(
        lambda: service, server_ids=("as-alice", "as-bob"),
    )
    identity = _identity()
    worker_channel, control_channel = _channel_pair()
    client = ControlServiceClient(worker_channel, identity)
    server = ControlServiceServer(
        control_channel,
        identity,
        handlers=authority.handlers_for(identity),
        admission=ControlServiceAdmission(),
    )
    operation_clients = {
        "as-alice": RecordingOperationClient("as-alice"),
        "as-bob": RecordingOperationClient("as-bob"),
    }
    await asyncio.gather(client.start(), server.start())
    try:
        yield authority, client, operation_clients, sessions, isolated_routed_environment
    finally:
        await client.stop()
        await server.stop()
        await authority.stop(timeout=2)


def _routed_services(client, operation_clients):
    from agent_gateway.auth.worker_credential_services import RoutedWorkerCredentialServices

    users = {"alice": "as-alice", "bob": "as-bob"}
    if not operation_clients:
        class EmptyRoutedClient:
            def credential_operation_clients(self):
                return {}

            def server_id_for_user(self, user_id):
                raise ValueError(f"no owned credential target for {user_id}")

            def set_credential_runtime(self, runtime):
                self.runtime = runtime

            def set_credential_lifecycle_handler(self, handler):
                self.handler = handler

        empty_client = EmptyRoutedClient()
        services = RoutedWorkerCredentialServices.from_client(
            client, agent_client=empty_client,
        )
        return services, empty_client

    agent_client = SimpleNamespace(
        credential_operation_clients=lambda: operation_clients,
        server_id_for_user=lambda user_id: users[user_id],
        set_credential_runtime=lambda runtime: None,
        set_credential_lifecycle_handler=lambda handler: None,
    )
    return RoutedWorkerCredentialServices.from_client(
        client,
        agent_client=agent_client,
    )


@pytest.mark.parametrize("wire_kind", ["legacy-minimal", "legacy-chunk", "typed"])
async def test_routed_services_issue_bind_refresh_and_report_per_target(
    routed_pair, wire_kind,
):
    authority, client, operation_clients, sessions, path = routed_pair
    services = _routed_services(client, operation_clients)
    assert services.runtime.server_ids == ("as-alice", "as-bob")

    alice_access = services.credentials.for_user("alice")
    bob_access = services.credentials.for_user("bob")
    alice_credential = await alice_access.request_model_auth(
        sessions["alice"].session_id, "login-model",
    )
    bob_credential = await bob_access.request_model_auth(
        sessions["bob"].session_id, "login-model",
    )
    assert alice_credential.lease.server_id == "as-alice"
    assert bob_credential.lease.server_id == "as-bob"
    assert alice_access.server_id == "as-alice"
    assert bob_access.server_id == "as-bob"

    alice_bindings = services.bindings.for_user("alice")
    await alice_bindings.remember(
        "chat-session", alice_credential.credential_ref, "login-model#2",
    )
    binding = await alice_bindings.lookup("chat-session")
    resolved = await alice_bindings.model_auth_for_binding(binding)
    assert resolved.lease.server_id == "as-alice"
    namespaced_key = hashlib.sha256(b"as-alice\0chat-session").hexdigest()
    saved = json.loads((path / "session_owners.json").read_text())
    assert set(saved) == {namespaced_key}
    assert "chat-session" not in saved

    payload = {
        "event_type": login_credentials.CREDENTIAL_REFRESH_EVENT,
        "credential_ref": alice_credential.credential_ref,
    }
    wire = {"payload": payload}
    if wire_kind == "legacy-chunk":
        wire.update({
            "request_id": "refresh-alice", "channel_id": "web", "is_complete": False,
        })
    elif wire_kind == "typed":
        wire = encode_agent_chunk_for_wire(AgentResponseChunk(
            request_id="refresh-alice", channel_id="web", payload=payload, is_complete=False,
        ), response_id="refresh-alice", sequence=0)
    await services.handle_credential_refresh("as-alice", wire)
    assert await services.runtime.wait_for_operations(timeout=2)
    assert len(operation_clients["as-alice"].requests) == 1
    assert operation_clients["as-bob"].requests == []
    assert operation_clients["as-alice"].requests[0]["credential_ref"] == alice_credential.credential_ref

    operations = authority.registry.operations
    assert [
        operation.outcome for operation in operations.values() if operation.target.server_id == "as-alice"
    ] == ["applied"]
    assert all(
        operation.target.server_id == "as-alice" for operation in operations.values()
    )


async def test_routed_services_reject_unknown_source_user_without_operation(
    routed_pair,
):
    _authority, client, operation_clients, sessions, _path = routed_pair
    services = _routed_services(client, operation_clients)
    credential = await services.credentials.for_user("alice").request_model_auth(
        sessions["alice"].session_id, "login-model",
    )
    await services.handle_credential_refresh("unknown-as", {
        "payload": {
            "event_type": login_credentials.CREDENTIAL_REFRESH_EVENT,
            "credential_ref": credential.credential_ref,
        },
    })
    assert all(operation_client.requests == [] for operation_client in operation_clients.values())


async def test_routed_services_handle_invalidation_on_matching_target(
    routed_pair,
):
    authority, client, operation_clients, sessions, _path = routed_pair
    services = _routed_services(client, operation_clients)
    credential = await services.credentials.for_user("alice").request_model_auth(
        sessions["alice"].session_id, "login-model",
    )

    class Endpoint:
        async def notify(self, _event_name, arguments, *, timeout):
            await services.handle_invalidation(arguments)

    authority._endpoints[_identity()] = Endpoint()
    await authority.logout(sessions["alice"].session_id)
    assert await services.runtime.wait_for_operations(timeout=2)
    assert len(operation_clients["as-alice"].requests) == 1
    assert operation_clients["as-bob"].requests == []


async def test_routed_services_with_no_owned_targets_only_rejects_operations(
    routed_pair,
):
    from agent_gateway.auth.credential_access import CredentialAccessError

    _authority, client, _operation_clients, _sessions, _path = routed_pair
    services, empty_client = _routed_services(client, {})
    assert services.runtime.server_ids == ()
    assert empty_client.runtime is services.runtime
    assert empty_client.handler == services.handle_credential_refresh

    with pytest.raises(ValueError, match="no owned credential target"):
        services.credentials.for_user("alice")
    with pytest.raises(CredentialAccessError):
        services.runtime.runtime_for("as-any")
    await services.stop()


async def test_inbound_selects_credential_access_by_normalized_user():
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.auth.credential_access import ModelCredential
    from e2a.message import Message, ReqMethod

    selected_requests = []

    class SelectedAccess:
        async def request_model_auth(self, auth_session, model_name):
            selected_requests.append((auth_session, model_name))
            return ModelCredential(
                api_base="https://test.invalid",
                api_key="synthetic-token",
                credential_ref="a" * 32,
            )

    class RoutedAccess:
        def for_user(self, user_id):
            assert user_id == "alice"
            return SelectedAccess()

    async def refresh(_msg):
        return None

    preprocessor = MessagePreprocessor(
        refresh_credentials=refresh,
        normalize=lambda msg: msg,
        inject_work_mode=lambda _msg: None,
        credential_access=RoutedAccess(),
    )
    message = Message(
        id="request",
        type="req",
        channel_id="web",
        session_id="chat",
        req_method=ReqMethod.CHAT_SEND,
        params={"model_name": "login-model", "query": "hello"},
        metadata={"auth_session": "auth-session"},
        user_id="alice",
        timestamp=time.time(),
        ok=True,
    )
    normalized = await preprocessor._prepare(message)
    assert selected_requests == [("auth-session", "login-model")]
    assert normalized.params["_model_auth"]["credential_ref"] == "a" * 32


async def test_message_handler_selects_bindings_by_envelope_user():
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding
    from agent_gateway.message_handler.message_handler import MessageHandler
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY
    from unittest.mock import AsyncMock

    binding = SessionLoginBinding("a" * 32, "login-model")
    selected = SimpleNamespace(
        remember=AsyncMock(),
        lookup=AsyncMock(return_value=binding),
        model_auth_for_binding=AsyncMock(return_value=ModelCredential(
            "https://test.invalid", "token", "a" * 32,
        )),
    )

    class RoutedBindings:
        def for_user(self, user_id):
            assert user_id == "alice"
            return selected

    handler = object.__new__(MessageHandler)
    handler._effective_session_model = AsyncMock(return_value="login-model")
    handler.set_session_login_bindings(RoutedBindings())

    web = SimpleNamespace(
        channel="web",
        session_id="session-a",
        method="chat.send",
        params={
            "model_name": "login-model#2",
            E2A_MODEL_AUTH_PARAM_KEY: {"credential_ref": "a" * 32},
        },
        user_id="alice",
    )
    await handler._apply_session_login_owner(web)
    selected.remember.assert_awaited_once_with(
        "session-a", "a" * 32, "login-model#2",
    )

    im = SimpleNamespace(
        channel="feishu",
        session_id="session-a",
        method="chat.send",
        params={},
        user_id="alice",
    )
    await handler._apply_session_login_owner(im)
    selected.lookup.assert_awaited_once_with("session-a")
    selected.model_auth_for_binding.assert_awaited_once_with(binding)
    assert im.params[E2A_MODEL_AUTH_PARAM_KEY]["api_key"] == "token"
