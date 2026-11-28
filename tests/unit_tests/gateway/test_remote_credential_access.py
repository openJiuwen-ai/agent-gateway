from __future__ import annotations

import asyncio
import os
import socket
import time
from collections.abc import AsyncGenerator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway.auth.credential_access import (
    CredentialAccessError,
    ModelCredential,
    OwnsResolveDeadline,
)
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.auth.remote_credential_access import (
    _BINDING_AUTH_TIMEOUT_S,
    _REQUEST_TIMEOUT_S,
    RemoteCredentialAccess,
    RemoteSessionLoginBindings,
)
from agent_gateway.auth.session_login_bindings import SessionLoginBinding
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceClient,
    ControlServiceIdentity,
    ControlServiceServer,
)
from agent_gateway.processes.control_channel import ControlServiceChannel


CREDENTIAL_REF = "a" * 32
MODEL_CREDENTIAL = {
    "api_base": "https://model.example",
    "api_key": "secret-token",
    "credential_ref": CREDENTIAL_REF,
}


@pytest.fixture
def isolated_environment(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    data = tmp_path / "data"
    temporary = tmp_path / "tmp"
    logs = tmp_path / "logs"
    for directory in (home, data, temporary, logs):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("TMPDIR", str(temporary))
    monkeypatch.setenv("JIUWENSWARM_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENT_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv(
        "JIUWENSWARM_CONFIG_URL", "http://127.0.0.1:9/isolated-test-config"
    )
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[3]))


def identity() -> ControlServiceIdentity:
    current_pid = os.getpid()
    return ControlServiceIdentity(
        group_id="gateway-group",
        slot_id=0,
        generation=1,
        worker_pid=current_pid,
        control_pid=current_pid,
    )


def channel_pair() -> tuple[ControlServiceChannel, ControlServiceChannel]:
    worker_socket, control_socket = socket.socketpair()
    return ControlServiceChannel(worker_socket), ControlServiceChannel(control_socket)


def unstarted_client_pair() -> tuple[
    ControlServiceClient, ControlServiceChannel, ControlServiceChannel
]:
    worker_channel, control_channel = channel_pair()
    return ControlServiceClient(worker_channel, identity()), worker_channel, control_channel


async def start_services(
    client: ControlServiceClient, server: ControlServiceServer
) -> None:
    await asyncio.gather(server.start(), client.start())


async def stop_services(
    client: ControlServiceClient, server: ControlServiceServer
) -> None:
    await client.stop()
    await server.stop()


async def make_remote_pair(
    handlers: dict[str, Any]
) -> tuple[RemoteCredentialAccess, RemoteSessionLoginBindings, ControlServiceClient, ControlServiceServer]:
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers=handlers,
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    return RemoteCredentialAccess(client), RemoteSessionLoginBindings(client), client, server


def request_handler(response: dict[str, Any]) -> Any:
    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return response

    return handler


@pytest.fixture
async def control_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[tuple[Any, Any, Any], None]:
    from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
    from agent_gateway.auth.account_kit import Credential
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    session_owners.reset_for_test()
    store = AuthSessionStore(persist=False)
    session = store.create(
        SimpleNamespace(
            user_id="account-0",
            user_name="Account 0",
            credential=Credential(
                id_token="remote-integration-token",
                refresh_token="remote-integration-refresh",
                expires_at=time.time() + 3600,
            ),
        )
    )
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"c" * 32)
    monkeypatch.setattr(
        model_catalog,
        "get_models",
        lambda *arguments, **keyword_arguments: [
            model_catalog.LoginModel("login-model", "Login")
        ],
    )
    monkeypatch.setattr(
        apig,
        "resolve_apig_config",
        lambda **keyword_arguments: SimpleNamespace(
            invoke_base_url="https://test.invalid/invoke"
        ),
    )
    services = ControlCredentialServices(lambda: service)
    try:
        yield services, service, session
    finally:
        try:
            await services.stop(timeout=2)
        finally:
            session_owners.reset_for_test()


async def test_remote_adapters_with_control_credential_services_over_socketpair(
    isolated_environment: None, control_authority: tuple[Any, Any, Any]
) -> None:
    from agent_gateway.auth import login_credentials

    services, _service, session = control_authority
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers=services.handlers,
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    access = RemoteCredentialAccess(client)
    bindings = RemoteSessionLoginBindings(client)

    credential = await access.request_model_auth(session.session_id, "login-model#2")
    assert credential is not None
    assert credential.api_base == "https://test.invalid/invoke"
    assert credential.api_key == "remote-integration-token"
    assert await access.request_model_auth(None, "login-model") is None
    with pytest.raises(CredentialAccessError) as missing_session:
        await access.request_model_auth("missing", "login-model")
    assert missing_session.value.reason == "invalid"

    await bindings.remember(
        "chat-session", credential.credential_ref, "login-model#2"
    )
    binding = await bindings.lookup("chat-session")
    assert binding is not None
    assert binding.session_id == "chat-session"
    resolved = await bindings.model_auth_for_binding(binding)
    assert resolved == credential

    other_ref = login_credentials.credential_ref_for_user("account-1")
    with pytest.raises(CredentialAccessError) as stale_ref:
        await bindings.model_auth_for_binding(
            replace(binding, credential_ref=other_ref)
        )
    with pytest.raises(CredentialAccessError) as stale_session:
        await bindings.model_auth_for_binding(
            replace(binding, session_id="missing-session")
        )
    with pytest.raises(CredentialAccessError) as stale_model:
        await bindings.model_auth_for_binding(
            replace(binding, model_name="configured-model")
        )
    assert stale_ref.value.reason == "invalid"
    assert stale_session.value.reason == "invalid"
    assert stale_model.value.reason == "invalid"

    await stop_services(client, server)


async def test_request_model_auth_success_over_real_control_socket(
    isolated_environment: None,
) -> None:
    arguments_seen: list[dict[str, Any]] = []

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        arguments_seen.append(dict(arguments))
        return {
            "status": "ok",
            "credential": MODEL_CREDENTIAL,
        }

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    credential = await access.request_model_auth("", " login-model ")

    assert credential == ModelCredential(
        api_base="https://model.example", api_key="secret-token", credential_ref=CREDENTIAL_REF
    )
    assert arguments_seen == [
        {"auth_session": None, "model_name": "login-model"}
    ]

    await stop_services(client, server)


async def test_response_token_is_preserved_without_stripping(
    isolated_environment: None,
) -> None:
    preserved_token = "  preserved-token  "

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "ok",
            "credential": {
                **MODEL_CREDENTIAL,
                "api_key": preserved_token,
            },
        }

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    credential = await access.request_model_auth("auth-session", "login-model")

    assert credential is not None
    assert credential.api_key == preserved_token

    await stop_services(client, server)


async def test_request_model_auth_not_required_over_real_socket(
    isolated_environment: None,
) -> None:
    access, _, client, server = await make_remote_pair(
        {
            "credentials.request_model_auth": request_handler({"status": "not_required"}),
        }
    )
    assert await access.request_model_auth(None, "custom-model") is None
    await stop_services(client, server)


async def test_request_model_auth_invalid_status_is_invalid_error(
    isolated_environment: None,
) -> None:
    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": request_handler({"status": "invalid"})}
    )
    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "invalid"
    assert str(raised.value) == "Remote credential access failed"

    await stop_services(client, server)


async def test_bad_response_is_protocol_error_without_response_details(
    isolated_environment: None,
) -> None:
    access, _, client, server = await make_remote_pair(
        {
            "credentials.request_model_auth": request_handler(
                {
                    "status": "ok",
                    "credential": {
                        "api_base": "https://model.example",
                        "api_key": "secret-token",
                        "credential_ref": CREDENTIAL_REF,
                        "extra": "secret-extra",
                    },
                }
            )
        }
    )
    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "protocol"
    assert "secret" not in str(raised.value)
    assert raised.value.__cause__ is None

    await stop_services(client, server)


async def test_disabled_operation_is_not_enabled(
    isolated_environment: None,
) -> None:
    access, _, client, server = await make_remote_pair({})
    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "not_enabled"

    await stop_services(client, server)


async def test_timeout_is_unavailable_and_channel_remains_usable(
    isolated_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_gateway.auth import remote_credential_access

    release = asyncio.Event()
    fast_seen = asyncio.Event()

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        if arguments.get("auth_session") == "fast":
            fast_seen.set()
            return {"status": "not_required"}
        await release.wait()
        return {"status": "not_required"}

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    monkeypatch.setattr(remote_credential_access, "_REQUEST_TIMEOUT_S", 0.01)

    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "unavailable"
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True

    release.set()
    continued_request = asyncio.create_task(
        access.request_model_auth("fast", "custom-model")
    )
    await fast_seen.wait()
    assert await continued_request is None

    await stop_services(client, server)


async def test_cancellation_propagates_and_does_not_kill_channel(
    isolated_environment: None,
) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()
    slow_done = asyncio.Event()

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        handler_started.set()
        await release.wait()
        slow_done.set()
        return {"status": "not_required"}

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    request = asyncio.create_task(
        access.request_model_auth("auth-session", "login-model")
    )
    await handler_started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    release.set()
    await slow_done.wait()
    assert await access.request_model_auth("auth-session", "custom-model") is None

    await stop_services(client, server)


async def test_binding_auth_timeout_matches_request_boundary(
    isolated_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_gateway.auth import remote_credential_access

    assert _BINDING_AUTH_TIMEOUT_S == _REQUEST_TIMEOUT_S
    assert _BINDING_AUTH_TIMEOUT_S == 25.0
    release = asyncio.Event()
    fast_seen = asyncio.Event()

    async def lookup_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "ok",
            "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model"},
        }

    async def resolve_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        if arguments.get("session_id") == "fast-session":
            fast_seen.set()
            return {"status": "ok", "credential": MODEL_CREDENTIAL}
        await release.wait()
        return {"status": "invalid"}

    _, bindings, client, server = await make_remote_pair(
        {
            "bindings.lookup": lookup_handler,
            "bindings.resolve_model_auth": resolve_handler,
        }
    )
    binding = await bindings.lookup("session-id")
    assert binding is not None
    monkeypatch.setattr(
        remote_credential_access, "_BINDING_AUTH_TIMEOUT_S", 0.01
    )

    with pytest.raises(CredentialAccessError) as raised:
        await bindings.model_auth_for_binding(binding)
    assert raised.value.reason == "unavailable"
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True

    release.set()
    fast_binding = await bindings.lookup("fast-session")
    assert fast_binding is not None
    continued_resolve = asyncio.create_task(
        bindings.model_auth_for_binding(fast_binding)
    )
    await fast_seen.wait()
    assert await continued_resolve is not None

    await stop_services(client, server)


async def test_outer_timeout_cancellation_is_not_swallowed_during_resolve(
    isolated_environment: None,
) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()

    async def lookup_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "ok",
            "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model"},
        }

    async def resolve_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        handler_started.set()
        await release.wait()
        return {"status": "invalid"}

    _, bindings, client, server = await make_remote_pair(
        {
            "bindings.lookup": lookup_handler,
            "bindings.resolve_model_auth": resolve_handler,
        }
    )
    binding = await bindings.lookup("session-id")
    assert binding is not None

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await bindings.model_auth_for_binding(binding)

    release.set()
    await handler_started.wait()
    await stop_services(client, server)


async def test_remote_calls_have_no_long_lived_cache(
    isolated_environment: None,
) -> None:
    request_calls: list[dict[str, Any]] = []
    lookup_calls: list[dict[str, Any]] = []

    async def request_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        request_calls.append(dict(arguments))
        return {
            "status": "ok",
            "credential": {**MODEL_CREDENTIAL, "api_key": f"token-{len(request_calls)}"},
        }

    async def lookup_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        lookup_calls.append(dict(arguments))
        return {
            "status": "ok",
            "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model"},
        }

    access, bindings, client, server = await make_remote_pair(
        {
            "credentials.request_model_auth": request_handler,
            "bindings.lookup": lookup_handler,
        }
    )
    first = await access.request_model_auth("auth-session", "login-model")
    second = await access.request_model_auth("auth-session", "login-model")
    assert first is not None and second is not None
    assert first.api_key == "token-1"
    assert second.api_key == "token-2"
    assert len(request_calls) == 2

    assert await bindings.lookup("session-id") is not None
    assert await bindings.lookup("session-id") is not None
    assert lookup_calls == [{"session_id": "session-id"}, {"session_id": "session-id"}]

    await stop_services(client, server)


async def test_binding_remember_and_lookup_contract(
    isolated_environment: None,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def remember_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append(("remember", dict(arguments)))
        return {"status": "ok"}

    async def lookup_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append(("lookup", dict(arguments)))
        return {
            "status": "ok",
            "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model"},
        }

    _, bindings, client, server = await make_remote_pair(
        {
            "bindings.remember": remember_handler,
            "bindings.lookup": lookup_handler,
        }
    )
    await bindings.remember("session-id", CREDENTIAL_REF, "login-model")
    binding = await bindings.lookup("session-id")

    assert isinstance(binding, SessionLoginBinding)
    assert binding is not None
    assert binding.session_id == "session-id"
    assert binding.credential_ref == CREDENTIAL_REF
    assert binding.model_name == "login-model"
    assert calls == [
        (
            "remember",
            {
                "session_id": "session-id",
                "credential_ref": CREDENTIAL_REF,
                "model_name": "login-model",
            },
        ),
        ("lookup", {"session_id": "session-id"}),
    ]

    await stop_services(client, server)


async def test_binding_lookup_not_found_returns_none(
    isolated_environment: None,
) -> None:
    _, bindings, client, server = await make_remote_pair(
        {"bindings.lookup": request_handler({"status": "not_found"})}
    )
    assert await bindings.lookup("session-id") is None

    await stop_services(client, server)


async def test_model_auth_for_binding_resolves_session_bound_credential(
    isolated_environment: None,
) -> None:
    calls: list[dict[str, Any]] = []

    async def lookup_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "ok",
            "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model"},
        }

    async def resolve_handler(arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append(dict(arguments))
        return {"status": "ok", "credential": MODEL_CREDENTIAL}

    _, bindings, client, server = await make_remote_pair(
        {
            "bindings.lookup": lookup_handler,
            "bindings.resolve_model_auth": resolve_handler,
        }
    )
    binding = await bindings.lookup("session-id")
    assert binding is not None
    credential = await bindings.model_auth_for_binding(binding)

    assert credential == ModelCredential(
        api_base="https://model.example", api_key="secret-token", credential_ref=CREDENTIAL_REF
    )
    assert calls == [
        {
            "session_id": "session-id",
            "credential_ref": CREDENTIAL_REF,
            "model_name": "login-model",
        }
    ]

    await stop_services(client, server)


async def test_model_auth_for_binding_rejects_publicly_constructed_binding(
    isolated_environment: None,
) -> None:
    _, bindings, client, server = await make_remote_pair({})
    with pytest.raises(CredentialAccessError) as raised:
        await bindings.model_auth_for_binding(
            SessionLoginBinding(CREDENTIAL_REF, "login-model")
        )
    assert raised.value.reason == "invalid"

    await stop_services(client, server)


async def test_local_only_methods_are_disabled_without_rpc(
    isolated_environment: None,
) -> None:
    calls: list[str] = []

    def handler_factory(operation: str) -> Any:
        async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
            calls.append(operation)
            return {"status": "not_required"}

        return handler

    access, bindings, client, server = await make_remote_pair(
        {
            "credentials.request_model_auth": handler_factory("request"),
            "bindings.lookup": handler_factory("lookup"),
        }
    )
    with pytest.raises(CredentialAccessError) as access_update:
        await access.credential_update_for_ref(CREDENTIAL_REF)
    with pytest.raises(CredentialAccessError) as access_login:
        await access.credential_ref_for_login_session("auth-session")
    with pytest.raises(CredentialAccessError) as access_ref:
        await access.model_auth_for_ref(CREDENTIAL_REF)

    assert access_update.value.reason == "not_enabled"
    assert access_login.value.reason == "not_enabled"
    assert access_ref.value.reason == "not_enabled"
    assert calls == []

    await stop_services(client, server)


async def test_request_values_are_validated_without_truncation(
    isolated_environment: None,
) -> None:
    client, worker_channel, control_channel = unstarted_client_pair()
    access = RemoteCredentialAccess(client)
    for auth_session in (123, "a" * 257):
        with pytest.raises(CredentialAccessError) as raised:
            await access.request_model_auth(auth_session, "login-model")
        assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("a" * 256 + " ", "login-model")
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth(None, "m" * 257)
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth(None, "m" * 256 + " ")
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth(None, 123)
    assert raised.value.reason == "invalid"
    worker_channel.close()
    control_channel.close()


async def test_request_permits_empty_model_and_auth_session(
    isolated_environment: None,
) -> None:
    arguments_seen: list[dict[str, Any]] = []

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        arguments_seen.append(dict(arguments))
        return {"status": "not_required"}

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    assert await access.request_model_auth(None, "") is None
    assert await access.request_model_auth("", "") is None
    assert arguments_seen == [
        {"auth_session": None, "model_name": ""},
        {"auth_session": None, "model_name": ""},
    ]

    await stop_services(client, server)


async def test_binding_values_are_validated_without_truncation(
    isolated_environment: None,
) -> None:
    client, worker_channel, control_channel = unstarted_client_pair()
    bindings = RemoteSessionLoginBindings(client)
    with pytest.raises(CredentialAccessError) as raised:
        await bindings.remember("s" * 257, CREDENTIAL_REF, "login-model")
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await bindings.remember("session-id", "not-a-ref", "login-model")
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await bindings.lookup("s" * 257)
    assert raised.value.reason == "invalid"
    worker_channel.close()
    control_channel.close()


async def test_binding_input_raw_length_is_validated_before_normalization(
    isolated_environment: None,
) -> None:
    client, worker_channel, control_channel = unstarted_client_pair()
    bindings = RemoteSessionLoginBindings(client)

    with pytest.raises(CredentialAccessError) as raised:
        await bindings.remember(
            "s" * 256 + " ", CREDENTIAL_REF, "login-model"
        )
    assert raised.value.reason == "invalid"

    with pytest.raises(CredentialAccessError) as raised:
        await bindings.lookup("s" * 256 + " ")
    assert raised.value.reason == "invalid"

    worker_channel.close()
    control_channel.close()


def test_remote_bindings_own_the_resolve_deadline() -> None:
    assert issubclass(RemoteSessionLoginBindings, OwnsResolveDeadline)
    assert RemoteSessionLoginBindings.resolve_deadline_s == _BINDING_AUTH_TIMEOUT_S
    assert RemoteSessionLoginBindings.resolve_deadline_s == 25.0


@pytest.mark.parametrize(
    "response",
    [
        {"status": "unexpected"},
        {"status": "ok", "credential": None},
        {"status": "ok"},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "credential_ref": "not-a-ref"}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_base": 1}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_key": 1}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_key": "t" * 4097}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_base": "b" * 2049}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_key": " "}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "api_base": " "}},
        {"status": "ok", "credential": {**MODEL_CREDENTIAL, "credential_ref": CREDENTIAL_REF + "\n"}},
    ],
)
async def test_response_schema_is_strict(
    isolated_environment: None,
    response: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.processes import control_protocol

    monkeypatch.setattr(control_protocol, "_MAX_JSON_STRING_LENGTH", 8192)
    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": request_handler(response)}
    )
    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "protocol"

    await stop_services(client, server)


@pytest.mark.parametrize(
    "response",
    [
        {"status": "unexpected"},
        {"status": "ok"},
        {"status": "ok", "binding": None},
        {"status": "ok", "binding": {}},
        {"status": "ok", "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "login-model", "extra": 1}},
        {"status": "ok", "binding": {"credential_ref": "bad-ref", "model_name": "login-model"}},
        {"status": "ok", "binding": {"credential_ref": CREDENTIAL_REF, "model_name": "m" * 257}},
        {"status": "ok", "binding": {"credential_ref": CREDENTIAL_REF, "model_name": " "}},
        {"status": "ok", "binding": {"credential_ref": CREDENTIAL_REF + "\n", "model_name": "login-model"}},
    ],
)
async def test_binding_response_schema_is_strict(
    isolated_environment: None, response: dict[str, Any]
) -> None:
    _, bindings, client, server = await make_remote_pair(
        {"bindings.lookup": request_handler(response)}
    )
    with pytest.raises(CredentialAccessError) as raised:
        await bindings.lookup("session-id")
    assert raised.value.reason == "protocol"

    await stop_services(client, server)


async def test_remote_failure_is_sanitized_unavailable(
    isolated_environment: None,
) -> None:
    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("secret-handler-failure")

    access, _, client, server = await make_remote_pair(
        {"credentials.request_model_auth": handler}
    )
    with pytest.raises(CredentialAccessError) as raised:
        await access.request_model_auth("auth-session", "login-model")
    assert raised.value.reason == "unavailable"
    assert str(raised.value) == "Remote credential access failed"
    assert "secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True

    await stop_services(client, server)
