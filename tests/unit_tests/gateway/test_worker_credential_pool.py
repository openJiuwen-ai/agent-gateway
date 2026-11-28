import asyncio
from types import SimpleNamespace

import pytest

from agent_gateway.processes.control_service import ControlServiceIdentity


@pytest.fixture
def isolated_pool_environment(tmp_path, monkeypatch):
    home = tmp_path / "home"
    data = tmp_path / "data"
    logs = tmp_path / "logs"
    for directory in (home, data, logs):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(logs))
    return {"home": home, "data": data, "logs": logs}


def _identity() -> ControlServiceIdentity:
    return ControlServiceIdentity(
        group_id="group",
        slot_id=1,
        generation=7,
        worker_pid=100,
        control_pid=200,
    )


class RecordingOperationClient:
    def __init__(self, server_id: str) -> None:
        self.server_id = server_id
        self.requests: list[dict[str, object]] = []

    async def send_request(self, envelope, *, on_written=None):
        self.requests.append(dict(envelope.params))
        if on_written is not None:
            on_written()
        return SimpleNamespace(ok=True, payload={"applied": True})


def _pool(identity, clients, report_result=None, **kwargs):
    from agent_gateway.auth.worker_credential_pool import WorkerCredentialPool

    return WorkerCredentialPool(
        identity=identity,
        operation_clients=clients,
        report_result=report_result,
        **kwargs,
    )


def _envelope_with_lease(lease, *, request_id="request"):
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY
    from e2a.models import E2AEnvelope
    from agent_gateway.auth.credential_access import CredentialParams

    return E2AEnvelope(
        request_id=request_id,
        method="chat.send",
        channel="web",
        params={
            E2A_MODEL_AUTH_PARAM_KEY: CredentialParams(
                api_base="https://test.invalid",
                api_key="synthetic-token",
                credential_ref=lease.credential_ref,
                credential_lease=lease,
            )
        },
    )


async def test_pool_rejects_invalid_and_duplicate_target_registration(
    isolated_pool_environment,
):
    identity = _identity()
    clients = {"as-1": RecordingOperationClient("as-1")}
    pool = _pool(identity, clients)

    assert pool.identity == identity
    assert pool.server_ids == ("as-1",)
    assert pool.runtime_for("as-1").server_id == "as-1"
    assert pool.runtime_for("as-1").identity == identity

    with pytest.raises(ValueError, match="duplicate target"):
        _pool(identity, {"as-1": clients["as-1"], " as-1 ": RecordingOperationClient("as-1")})
    with pytest.raises(ValueError, match="invalid target"):
        _pool(identity, {"": RecordingOperationClient("")})
    with pytest.raises(TypeError, match="operation client"):
        _pool(identity, {"as-1": object()})
    with pytest.raises(TypeError, match="report_result"):
        _pool(identity, clients, report_result=object())


async def test_pool_routes_private_leases_by_exact_server_id(
    isolated_pool_environment,
):
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY
    from e2a.models import E2AEnvelope
    from agent_gateway.auth.credential_access import CredentialAccessError, CredentialParams
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialLease

    pool = _pool(
        _identity(),
        {
            "as-1": RecordingOperationClient("as-1"),
            "as-2": RecordingOperationClient("as-2"),
        },
    )
    lease_1 = pool.issue("a" * 32, 1, "as-1")
    lease_2 = pool.issue("b" * 32, 1, "as-2")
    assert lease_1.server_id == "as-1"
    assert lease_2.server_id == "as-2"
    assert pool.active_lease_count == 2

    envelope_1 = _envelope_with_lease(lease_1, request_id="request-1")
    envelope_2 = _envelope_with_lease(lease_2, request_id="request-2")
    async with pool.before_send(envelope_1):
        pool.check(envelope_1)
        pool.mark_written(envelope_1)
    pool.release(envelope_1)
    async with pool.before_send(envelope_2):
        pool.check(envelope_2)
        pool.mark_written(envelope_2)
    pool.release(envelope_2)
    assert pool.active_lease_count == 0

    forged = WorkerCredentialLease(
        credential_ref="c" * 32,
        ref_revision=1,
        server_id="unknown-as",
        group_id="group",
        slot_id=1,
        generation=7,
    )
    with pytest.raises(CredentialAccessError):
        async with pool.before_send(_envelope_with_lease(forged)):
            pass

    no_lease = E2AEnvelope(
        request_id="request-plain",
        method="chat.send",
        channel="web",
        params={
            E2A_MODEL_AUTH_PARAM_KEY: CredentialParams(
                api_base="https://test.invalid",
                api_key="synthetic-token",
                credential_ref="a" * 32,
            )
        },
        channel_context={"server_id": "as-1"},
    )
    async with pool.before_send(no_lease):
        pool.check(no_lease)
    assert pool.active_lease_count == 0


async def test_pool_refreshes_only_the_declared_agent_server(
    isolated_pool_environment,
):
    reported = []
    client_1 = RecordingOperationClient("as-1")
    client_2 = RecordingOperationClient("as-2")
    pool = _pool(
        _identity(),
        {"as-1": client_1, "as-2": client_2},
        report_result=reported.append,
    )
    lease_1 = pool.issue("a" * 32, 1, "as-1")
    lease_2 = pool.issue("b" * 32, 1, "as-2")

    assert pool.refresh({
        "operation_id": "operation-as-1",
        "server_id": "as-1",
        "credential_ref": "a" * 32,
        "ref_revision": 2,
        "update": {"credential_ref": "a" * 32, "api_key": "token-1"},
    })
    assert pool.refresh({
        "operation_id": "operation-as-2",
        "server_id": "as-2",
        "credential_ref": "b" * 32,
        "ref_revision": 2,
        "update": {"credential_ref": "b" * 32, "api_key": "token-2"},
    })
    assert await pool.wait_for_operations(timeout=1)

    assert client_1.requests == [{"credential_ref": "a" * 32, "api_key": "token-1"}]
    assert client_2.requests == [{"credential_ref": "b" * 32, "api_key": "token-2"}]
    assert [result.server_id for result in reported] == ["as-1", "as-2"]
    assert [result.outcome for result in reported] == ["applied", "applied"]
    assert lease_1.revoked is False
    assert lease_2.revoked is False

    assert pool.refresh({
        "operation_id": "operation-unknown",
        "server_id": "unknown-as",
        "credential_ref": "a" * 32,
        "ref_revision": 3,
        "update": {"credential_ref": "a" * 32, "api_key": "token-3"},
    }) is False
    assert len(client_1.requests) == 1
    assert len(client_2.requests) == 1


async def test_pool_invalidate_revokes_only_the_matching_agent_server(
    isolated_pool_environment,
):
    reported = []
    client_1 = RecordingOperationClient("as-1")
    client_2 = RecordingOperationClient("as-2")
    pool = _pool(
        _identity(),
        {"as-1": client_1, "as-2": client_2},
        report_result=reported.append,
    )
    lease_1 = pool.issue("a" * 32, 1, "as-1")
    lease_2 = pool.issue("b" * 32, 1, "as-2")

    assert pool.invalidate({
        "operation_id": "revoke-as-1",
        "server_id": "as-1",
        "credential_ref": "a" * 32,
        "cutoff_revision": 1,
    })
    assert lease_1.revoked is True
    assert lease_2.revoked is False
    assert await pool.wait_for_operations(timeout=1)

    assert client_1.requests == [{"credential_ref": "a" * 32, "revoked": True}]
    assert client_2.requests == []
    assert len(reported) == 1
    assert reported[0].server_id == "as-1"
    assert reported[0].outcome == "applied"

    assert pool.invalidate({
        "operation_id": "revoke-unknown",
        "server_id": "unknown-as",
        "credential_ref": "b" * 32,
        "cutoff_revision": 1,
    }) is False
    assert client_2.requests == []


async def test_pool_stop_closes_every_runtime_and_rejects_new_leases(
    isolated_pool_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError

    entered = asyncio.Event()

    class BlockingClient:
        async def send_request(self, envelope, *, on_written=None):
            entered.set()
            await asyncio.sleep(3600)

    pool = _pool(
        _identity(),
        {"as-1": BlockingClient(), "as-2": RecordingOperationClient("as-2")},
    )
    assert pool.refresh({
        "operation_id": "operation-blocked",
        "server_id": "as-1",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "token"},
    })
    await asyncio.wait_for(entered.wait(), timeout=1)
    await pool.stop(timeout=0.2)

    with pytest.raises(CredentialAccessError):
        pool.issue("a" * 32, 2, "as-1")
    with pytest.raises(CredentialAccessError):
        pool.issue("b" * 32, 2, "as-2")
