import asyncio
from types import SimpleNamespace

import pytest

from agent_gateway.processes.control_service import ControlServiceIdentity


@pytest.fixture
def isolated_runtime_environment(tmp_path, monkeypatch):
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


def _envelope_with_lease(lease):
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY
    from e2a.models import E2AEnvelope
    from agent_gateway.auth.credential_access import CredentialParams

    return E2AEnvelope(
        request_id="request",
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


async def test_model_credential_lease_is_private_and_survives_param_copy():
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialLease

    lease = WorkerCredentialLease(
        credential_ref="a" * 32,
        ref_revision=1,
        server_id="agent-server",
        group_id="group",
        slot_id=1,
        generation=7,
    )
    credential = ModelCredential(
        "https://test.invalid",
        "synthetic-token",
        "a" * 32,
        lease=lease,
    )
    assert "synthetic-token" not in repr(credential)
    assert "lease" not in repr(credential)
    assert credential == ModelCredential("https://test.invalid", "synthetic-token", "a" * 32)

    params = credential.to_params()
    assert params == {
        "api_base": "https://test.invalid",
        "api_key": "synthetic-token",
        "credential_ref": "a" * 32,
    }
    assert params.credential_lease is lease
    copied = params.copy()
    assert copied == params
    assert copied.credential_lease is lease


async def test_runtime_issues_bounded_leases_and_rejects_remote_forged_credentials(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=object(),
    )
    leases = [
        runtime.issue("ref-" + str(index).zfill(28), 1, "agent-server")
        for index in range(32)
    ]
    assert len({lease.credential_ref for lease in leases}) == 32
    with pytest.raises(CredentialAccessError, match="Remote credential access failed"):
        runtime.issue("ref-" + "0" * 27 + "x", 1, "agent-server")

    for lease in leases:
        runtime.release(_envelope_with_lease(lease))

    assert runtime.issue("ref-" + "1" * 28, 1, "agent-server") is not None
    with pytest.raises(CredentialAccessError, match="Remote credential access failed"):
        runtime.issue("ref-" + "2" * 28, 1, "other-agent-server")


async def test_runtime_before_send_rejects_invalid_and_forged_credentials(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=object(),
    )
    lease = runtime.issue("a" * 32, 3, "agent-server")
    envelope = _envelope_with_lease(lease)

    async with runtime.before_send(envelope):
        runtime.check(envelope)
        runtime.mark_written(envelope)
    runtime.release(envelope)

    with pytest.raises(CredentialAccessError):
        async with runtime.before_send(envelope):
            pass

    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY
    from e2a.models import E2AEnvelope

    forged = E2AEnvelope(
        request_id="forged",
        method="chat.send",
        params={
            E2A_MODEL_AUTH_PARAM_KEY: {
                "api_base": "https://test.invalid",
                "api_key": "client-token",
                "credential_ref": "a" * 32,
            }
        },
    )
    with pytest.raises(CredentialAccessError):
        async with runtime.before_send(forged):
            pass


async def test_runtime_invalidate_gates_old_sends_and_reports_bounded_revoke(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            reported.append(("sent", envelope.params))
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    old_lease = runtime.issue("a" * 32, 2, "agent-server")
    assert runtime.invalidate(
        {
            "operation_id": "operation-1",
            "server_id": "agent-server",
            "credential_ref": "a" * 32,
            "cutoff_revision": 2,
        }
    )
    assert old_lease.revoked is True
    with pytest.raises(Exception):
        async with runtime.before_send(_envelope_with_lease(old_lease)):
            pass

    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0][0] == "sent"
    assert reported[0][1] == {"credential_ref": "a" * 32, "revoked": True}
    result = reported[1]
    assert result.operation_id == "operation-1"
    assert result.server_id == "agent-server"
    assert result.credential_ref == "a" * 32
    assert result.outcome == "applied"
    assert result.written is True
    assert result.response_received is True
    assert result.as_ok is True
    assert result.as_applied is True


async def test_runtime_supersedes_revoke_only_after_new_revision_actual_send(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            reported.append(("sent", dict(envelope.params)))
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    old_lease = runtime.issue("a" * 32, 2, "agent-server")
    assert runtime.invalidate(
        {
            "operation_id": "operation-1",
            "server_id": "agent-server",
            "credential_ref": "a" * 32,
            "cutoff_revision": 2,
        }
    )
    runtime.release(_envelope_with_lease(old_lease))

    new_lease = runtime.issue("a" * 32, 3, "agent-server")
    new_envelope = _envelope_with_lease(new_lease)
    async with runtime.before_send(new_envelope):
        runtime.check(new_envelope)
        runtime.mark_written(new_envelope)
    runtime.release(new_envelope)

    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "superseded"
    assert reported[0].written is False


async def test_runtime_unknown_revoke_blocks_same_ref_until_explicit_recovery(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            await asyncio.sleep(0.2)
            raise ConnectionError("synthetic AS failure")

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        operation_timeout_s=0.05,
    )
    runtime.issue("a" * 32, 2, "agent-server")
    runtime.invalidate(
        {
            "operation_id": "operation-1",
            "server_id": "agent-server",
            "credential_ref": "a" * 32,
            "cutoff_revision": 2,
        }
    )
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "timeout_unknown"
    with pytest.raises(CredentialAccessError):
        runtime.issue("a" * 32, 3, "agent-server")


async def test_runtime_check_and_written_are_noop_without_model_auth(
    isolated_runtime_environment,
):
    from e2a.models import E2AEnvelope

    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    envelope = E2AEnvelope(request_id="auth-credentials-operation", method="auth.credentials.update")
    runtime.check(envelope)
    runtime.mark_written(envelope)
    runtime.release(envelope)

    assert runtime.refresh({
        "operation_id": "operation-1",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "synthetic-token"},
    })
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "applied"


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (SimpleNamespace(ok=False, payload={"applied": True}), "failed"),
        (SimpleNamespace(ok=True, payload={"applied": "yes"}), "failed"),
        (SimpleNamespace(ok=True, payload={"applied": False}), "failed"),
        (SimpleNamespace(ok=True, payload={}), "confirmed_unknown"),
        (SimpleNamespace(ok=False, payload={"applied": False}), "rejected"),
    ],
)
async def test_runtime_strictly_validates_agent_server_applied_contract(
    isolated_runtime_environment, response, expected
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            return response

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    assert runtime.refresh({
        "operation_id": "operation-1",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "synthetic-token"},
    })
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == expected


async def test_runtime_target_states_fail_closed_at_bound(isolated_runtime_environment):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=object(),
        max_target_refs=1,
    )
    first = runtime.issue("a" * 32, 1, "agent-server")
    runtime.release(_envelope_with_lease(first))
    with pytest.raises(CredentialAccessError):
        runtime.issue("b" * 32, 1, "agent-server")


async def test_unwritten_revoke_failure_does_not_block_new_generation(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            raise ConnectionError("synthetic AS preparation failure")

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    runtime.issue("a" * 32, 1, "agent-server")
    runtime.invalidate({
        "operation_id": "operation-1",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "cutoff_revision": 1,
    })
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "failed"
    assert reported[0].written is False
    runtime.issue("a" * 32, 2, "agent-server")


async def test_stale_refresh_after_logout_is_not_sent_to_agent_server(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []
    allow_send = asyncio.Event()

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            await allow_send.wait()
            if on_written is not None:
                on_written()
            sent.append(dict(envelope.params))
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    runtime.issue("a" * 32, 1, "agent-server")
    assert runtime.refresh({
        "operation_id": "operation-refresh",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "old-synthetic-token"},
    })
    assert runtime.invalidate({
        "operation_id": "operation-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "cutoff_revision": 1,
    })
    allow_send.set()
    await runtime.wait_for_operations(timeout=0.5)

    assert sent == [{"credential_ref": "a" * 32, "revoked": True}]
    assert [result.outcome for result in reported] == ["superseded", "applied"]


async def test_stale_refresh_after_new_generation_actual_send_is_not_sent(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            sent.append(dict(envelope.params))
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    runtime.issue("a" * 32, 1, "agent-server")
    assert runtime.refresh({
        "operation_id": "operation-refresh",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "old-synthetic-token"},
    })
    new_lease = runtime.issue("a" * 32, 2, "agent-server")
    new_envelope = _envelope_with_lease(new_lease)
    async with runtime.before_send(new_envelope):
        runtime.check(new_envelope)
        runtime.mark_written(new_envelope)
    runtime.release(new_envelope)
    await runtime.wait_for_operations(timeout=0.5)

    assert sent == []
    assert [result.outcome for result in reported] == ["superseded"]


async def test_revoked_refresh_uses_revoke_fence_and_unknown_semantics(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            sent.append(dict(envelope.params))
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    lease = runtime.issue("a" * 32, 1, "agent-server")
    assert runtime.refresh({
        "operation_id": "operation-revoked-refresh",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    })
    assert lease.revoked is True
    with pytest.raises(CredentialAccessError):
        async with runtime.before_send(_envelope_with_lease(lease)):
            pass
    await runtime.wait_for_operations(timeout=0.5)

    assert sent == [{"credential_ref": "a" * 32, "revoked": True}]
    assert reported[0].outcome == "applied"


async def test_unknown_revoke_rejects_duplicate_and_new_revoke_without_resend(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            params = dict(envelope.params)
            sent.append(params)
            if on_written is not None:
                on_written()
            return SimpleNamespace(ok=False, payload={"applied": True})

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        operation_timeout_s=0.5,
    )
    revoke_event = {
        "operation_id": "operation-unknown-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    }
    assert runtime.refresh(revoke_event)
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "failed"

    assert runtime.refresh(revoke_event) is False
    assert runtime.refresh({
        "operation_id": "operation-new-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 2,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    })
    await runtime.wait_for_operations(timeout=0.5)

    assert sent == [{"credential_ref": "a" * 32, "revoked": True}]
    assert [result.operation_id for result in reported] == [
        "operation-unknown-revoke",
        "operation-new-revoke",
    ]
    assert [result.outcome for result in reported] == ["failed", "rejected"]
    assert reported[1].written is False
    assert reported[1].response_received is False
    with pytest.raises(CredentialAccessError):
        runtime.issue("a" * 32, 3, "agent-server")


async def test_duplicate_operation_id_pending_and_completed_are_noop(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []
    release = asyncio.Event()

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            sent.append(dict(envelope.params))
            if on_written is not None:
                on_written()
            await release.wait()
            return SimpleNamespace(ok=True, payload={"applied": True})

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        operation_timeout_s=0.5,
    )
    event = {
        "operation_id": "operation-duplicate",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "api_key": "synthetic-token"},
    }
    assert runtime.refresh(event)
    await asyncio.sleep(0)
    assert runtime.refresh(event) is False
    assert runtime.pending_operation_count == 1

    release.set()
    await runtime.wait_for_operations(timeout=0.5)
    assert runtime.refresh(event) is False
    assert runtime.pending_operation_count == 0
    assert len(sent) == 1
    assert [result.operation_id for result in reported] == ["operation-duplicate"]


async def test_unknown_revoke_blocks_queued_non_revoked_refresh_at_serial_boundary(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            params = dict(envelope.params)
            sent.append(params)
            if params.get("revoked") is True:
                if on_written is not None:
                    on_written()
                return SimpleNamespace(ok=False, payload={"applied": True})
            return SimpleNamespace(ok=True, payload={"applied": True})

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        operation_timeout_s=0.05,
    )
    assert runtime.refresh({
        "operation_id": "operation-unknown-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    })
    assert runtime.refresh({
        "operation_id": "operation-queued-refresh",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 2,
        "update": {"credential_ref": "a" * 32, "api_key": "synthetic-token"},
    })
    await runtime.wait_for_operations(timeout=0.5)

    assert sent == [{"credential_ref": "a" * 32, "revoked": True}]
    assert [result.outcome for result in reported] == [
        "failed",
        "rejected",
    ]
    assert reported[1].written is False
    assert reported[1].response_received is False
    with pytest.raises(CredentialAccessError):
        runtime.issue("a" * 32, 3, "agent-server")
    assert runtime.issue("b" * 32, 1, "agent-server") is not None


async def test_revoke_revision_watermark_survives_result_eviction(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            sent.append(dict(envelope.params))
            if on_written is not None:
                on_written()
            return SimpleNamespace(ok=True, payload={"applied": True})

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        max_operations=2,
    )
    revoke_event = {
        "operation_id": "operation-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    }
    assert runtime.refresh(revoke_event)
    await runtime.wait_for_operations(timeout=0.5)

    for suffix, operation_id in (("b", "operation-refresh-b"), ("c", "operation-refresh-c")):
        assert runtime.refresh({
            "operation_id": operation_id,
            "server_id": "agent-server",
            "credential_ref": suffix * 32,
            "ref_revision": 1,
            "update": {"credential_ref": suffix * 32, "api_key": "synthetic-token"},
        })
        await runtime.wait_for_operations(timeout=0.5)

    assert runtime.refresh(revoke_event) is False
    assert runtime.pending_operation_count == 0
    assert len(sent) == 3
    assert [result.operation_id for result in reported] == [
        "operation-revoke",
        "operation-refresh-b",
        "operation-refresh-c",
    ]


async def test_unknown_revoke_watermark_prevents_duplicate_after_result_eviction(
    isolated_runtime_environment,
):
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []
    sent = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            params = dict(envelope.params)
            sent.append(params)
            if on_written is not None:
                on_written()
            if params.get("revoked") is True:
                return SimpleNamespace(ok=False, payload={"applied": True})
            return SimpleNamespace(ok=True, payload={"applied": True})

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
        max_operations=2,
    )
    revoke_event = {
        "operation_id": "operation-unknown-revoke",
        "server_id": "agent-server",
        "credential_ref": "a" * 32,
        "ref_revision": 1,
        "update": {"credential_ref": "a" * 32, "revoked": True},
    }
    assert runtime.refresh(revoke_event)
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "failed"

    for suffix, operation_id in (("b", "operation-refresh-b"), ("c", "operation-refresh-c")):
        assert runtime.refresh({
            "operation_id": operation_id,
            "server_id": "agent-server",
            "credential_ref": suffix * 32,
            "ref_revision": 1,
            "update": {"credential_ref": suffix * 32, "api_key": "synthetic-token"},
        })
        await runtime.wait_for_operations(timeout=0.5)

    assert runtime.refresh(revoke_event) is False
    assert runtime.pending_operation_count == 0
    assert len(sent) == 3
    assert [result.operation_id for result in reported] == [
        "operation-unknown-revoke",
        "operation-refresh-b",
        "operation-refresh-c",
    ]
    with pytest.raises(CredentialAccessError):
        runtime.issue("a" * 32, 2, "agent-server")


async def test_active_write_barrier_waits_only_for_final_send_fragment(
    isolated_runtime_environment,
):
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime

    reported = []

    class Client:
        async def send_request(self, envelope, *, on_written=None):
            if on_written is not None:
                on_written()
            return SimpleResponse()

    class SimpleResponse:
        ok = True
        payload = {"applied": True}

    runtime = WorkerCredentialRuntime(
        identity=_identity(),
        server_id="agent-server",
        client=Client(),
        report_result=reported.append,
    )
    lease = runtime.issue("a" * 32, 1, "agent-server")
    envelope = _envelope_with_lease(lease)
    async with runtime.before_send(envelope):
        runtime.check(envelope)
        assert runtime.invalidate({
            "operation_id": "operation-1",
            "server_id": "agent-server",
            "credential_ref": "a" * 32,
            "cutoff_revision": 1,
        })
        drained = asyncio.create_task(runtime.wait_for_writes(
            "a" * 32, "agent-server", 1, timeout=0.5
        ))
        await asyncio.sleep(0)
        assert not drained.done()

        runtime.mark_written(envelope)
        await asyncio.sleep(0)
        assert not drained.done()

    runtime.release(envelope)
    assert await asyncio.wait_for(drained, timeout=0.5) is True
    await runtime.wait_for_operations(timeout=0.5)
    assert reported[0].outcome == "applied"
