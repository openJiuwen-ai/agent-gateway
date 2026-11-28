import os
import time
from types import SimpleNamespace

import pytest

from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_service import ControlServiceIdentity


@pytest.fixture
async def credential_environment(monkeypatch, tmp_path):
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    session_owners.reset_for_test()
    store = AuthSessionStore(persist=False)
    sessions = [store.create(SimpleNamespace(
        user_id="multi-account",
        user_name="Multi Account",
        credential=Credential(
            id_token="multi-target-token",
            refresh_token="multi-target-refresh",
            expires_at=time.time() + 3600,
        ),
    ))]
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, "_ref_key_cache", b"d" * 32)
    monkeypatch.setattr(model_catalog, "get_models", lambda *args, **kwargs: [
        model_catalog.LoginModel("login-model", "Login"),
    ])
    monkeypatch.setattr(apig, "resolve_apig_config", lambda **kwargs: SimpleNamespace(
        invoke_base_url="https://test.invalid/invoke",
    ))
    monkeypatch.setattr("agent_gateway.auth.service.get_auth_service", lambda: pytest.fail(
        "global authority used"
    ))
    yield service, sessions[0], tmp_path
    session_owners.reset_for_test()


def _identity() -> ControlServiceIdentity:
    return ControlServiceIdentity("multi-group", 0, 1, os.getpid(), os.getpid())


def _services(service, **kwargs):
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    return ControlCredentialServices(lambda: service, **kwargs)


def test_constructor_keeps_direct_and_multi_target_modes_exclusive(credential_environment):
    service, _session, _path = credential_environment

    with pytest.raises(ValueError, match="one credential target policy"):
        _services(service, server_id="as", server_ids=("as",))
    with pytest.raises(TypeError, match="server_ids must be a tuple"):
        _services(service, server_ids=["as"])
    with pytest.raises(ValueError, match="1-32 unique"):
        _services(service, server_ids=())
    with pytest.raises(ValueError, match="1-32 unique"):
        _services(service, server_ids=("as", "as"))
    with pytest.raises(ValueError, match="1-32 unique"):
        _services(service, server_ids=tuple(f"as-{index}" for index in range(33)))

    assert _services(service, server_id="as").handlers
    assert _services(service, server_ids=("as-1", "as-2")).handlers


async def test_direct_target_contract_is_unchanged(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, server_id="direct-as")
    handlers = services.handlers_for(_identity())
    request = handlers["credentials.request_model_auth"]

    result = await request({
        "auth_session": session.session_id,
        "model_name": "login-model",
    })
    assert result["lease"]["server_id"] == "direct-as"
    assert services.registry.registered(
        _identity(), "direct-as", result["credential"]["credential_ref"],
    )

    with pytest.raises(ValueError, match="invalid credential service value"):
        await request({
            "auth_session": session.session_id,
            "model_name": "login-model",
            "server_id": "direct-as",
        })


async def test_multi_target_request_requires_explicit_allowed_target(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, server_ids=("as-1", "as-2"))
    handlers = services.handlers_for(_identity())
    request = handlers["credentials.request_model_auth"]

    result_1 = await request({
        "auth_session": session.session_id,
        "model_name": "login-model",
        "server_id": "as-1",
    })
    result_2 = await request({
        "auth_session": session.session_id,
        "model_name": "login-model",
        "server_id": "as-2",
    })
    assert result_1["lease"]["server_id"] == "as-1"
    assert result_2["lease"]["server_id"] == "as-2"
    credential_ref = result_1["credential"]["credential_ref"]
    assert services.registry.registered(_identity(), "as-1", credential_ref)
    assert services.registry.registered(_identity(), "as-2", credential_ref)

    assert await request({
        "auth_session": session.session_id,
        "model_name": "login-model",
        "server_id": "as-3",
    }) == {"status": "invalid"}
    with pytest.raises(ValueError, match="invalid credential service value"):
        await request({
            "auth_session": session.session_id,
            "model_name": "login-model",
        })


async def test_refresh_and_result_are_bound_to_the_requested_target(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, server_ids=("as-1", "as-2"))
    identity = _identity()
    handlers = services.handlers_for(identity)
    request = handlers["credentials.request_model_auth"]
    refresh = handlers["credentials.refresh_for_ref"]
    result_handler = handlers["credentials.update_result"]

    issued = await request({
        "auth_session": session.session_id,
        "model_name": "login-model",
        "server_id": "as-1",
    })
    credential_ref = issued["credential"]["credential_ref"]

    assert await refresh({
        "server_id": "as-2",
        "credential_ref": credential_ref,
    }) == {"status": "invalid"}

    refreshed = await refresh({
        "server_id": "as-1",
        "credential_ref": credential_ref,
    })
    assert refreshed["server_id"] == "as-1"
    operation_id = refreshed["operation_id"]

    result = {
        "operation_id": operation_id,
        "server_id": "as-1",
        "credential_ref": credential_ref,
        "outcome": "applied",
        "written": True,
        "response_received": True,
        "as_ok": True,
        "as_applied": True,
    }
    assert await result_handler(result) == {"status": "ok"}

    with pytest.raises(ValueError, match="unknown credential operation"):
        await result_handler({**result, "server_id": "as-2"})


async def test_multi_target_binding_resolution_is_target_scoped(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, server_ids=("as-1", "as-2"))
    identity = _identity()
    handlers = services.handlers_for(identity)
    remember = handlers["bindings.remember"]
    resolve = handlers["bindings.resolve_model_auth"]
    credential_ref = login_credentials.credential_ref_for_user(session.user_id)

    assert await remember({
        "session_id": "chat",
        "credential_ref": credential_ref,
        "model_name": "login-model",
        "server_id": "as-1",
    }) == {"status": "ok"}
    resolved = await resolve({
        "session_id": "chat",
        "credential_ref": credential_ref,
        "model_name": "login-model",
        "server_id": "as-1",
    })
    assert resolved["status"] == "ok"
    assert resolved["lease"]["server_id"] == "as-1"
    assert services.registry.registered(identity, "as-1", credential_ref)
    assert not services.registry.registered(identity, "as-2", credential_ref)


async def test_logout_revokes_each_registered_target_independently(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, server_ids=("as-1", "as-2"))
    identity = _identity()
    handlers = services.handlers_for(identity)
    request = handlers["credentials.request_model_auth"]

    for server_id in ("as-1", "as-2"):
        await request({
            "auth_session": session.session_id,
            "model_name": "login-model",
            "server_id": server_id,
        })

    class Endpoint:
        def __init__(self):
            self.events = []

        async def notify(self, event_name, arguments, *, timeout):
            self.events.append((event_name, arguments))

    endpoint = Endpoint()
    services._endpoints[identity] = endpoint
    await services.logout(session.session_id)

    assert [event_name for event_name, _arguments in endpoint.events] == [
        "credentials.invalidate",
        "credentials.invalidate",
    ]
    assert {arguments["server_id"] for _event_name, arguments in endpoint.events} == {
        "as-1",
        "as-2",
    }
    assert len({
        arguments["operation_id"] for _event_name, arguments in endpoint.events
    }) == 2


async def test_manifest_prevents_one_worker_requesting_another_workers_target(credential_environment):
    service, session, _path = credential_environment
    services = _services(service, slot_server_ids={0: ('as-1',), 1: ('as-2',), 2: ()})
    try:
        handlers = services.handlers_for(_identity())
        arguments = {"auth_session": session.session_id, "model_name": "login-model", "server_id": "as-2"}
        assert await handlers['credentials.request_model_auth'](arguments) == {'status': 'invalid'}
        assert services.registry.target_count == 0
    finally:
        await services.stop()
