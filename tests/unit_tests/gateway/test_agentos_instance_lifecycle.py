# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Physical sandbox lifecycle over the managed control plane, shared with the router."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from agent_gateway import telemetry
from agent_gateway.extensions.agentos.agentos_router.backend import (
    DEFAULT_RUNTIME_PROBE_SETTINGS,
    RuntimeProbeSettings,
    SandboxInfo,
    AgentApiError,
    AgentCreateUncertainError,
    AgentDeleteRejectedError,
    AgentFailedError,
    AgentGenerationMismatchError,
    AgentNotFoundError,
    AgentNotVisibleError,
    AgentRequestNotSentError,
    AgentTimeoutError,
)
from agent_gateway.southbound.control.managed import ManagedSandboxControl
from agent_gateway.southbound.orchestration.agent_manager import (
    AgentCreateFailed,
    AgentManager,
    AgentPreCreateError,
    AgentRebuildPending,
)
from agent_gateway.southbound.orchestration.instance_lifecycle import (
    MAX_UNCERTAIN_CREATE_ATTEMPTS,
    AgentOSInstanceLifecycle,
    AgentOSSandboxAttempt,
    AgentOSSandboxLifecycle,
)
from agent_gateway.southbound.orchestration.managed_router import ManagedRouter


class FakeControl:
    def __init__(self) -> None:
        self.delete_calls: list[str] = []
        self.get_calls: list[str] = []
        self.wait_calls: list[tuple[str, str]] = []
        self.get_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.wait_error: Exception | None = None

    async def query(self, instance_id: str) -> dict[str, Any]:
        self.get_calls.append(instance_id)
        if self.get_error is not None:
            raise self.get_error
        return {"status": "running"}

    async def delete(self, instance_id: str) -> None:
        self.delete_calls.append(instance_id)
        if self.delete_error is not None:
            raise self.delete_error

    async def verify_generation(
        self, sandbox: SandboxInfo, *, generation: str, deadline: float,
    ) -> dict[str, Any]:
        self.wait_calls.append((sandbox.sandbox_id, generation))
        if self.wait_error is not None:
            raise self.wait_error
        return {"status": "running"}


def _service(
    control: FakeControl, operations: Any = None,
) -> tuple[AgentOSInstanceLifecycle, list[tuple[str, str, str]]]:
    cleanup_steps: list[tuple[str, str, str]] = []

    def cleanup_step(step: str, outcome: str, reason: str) -> None:
        cleanup_steps.append((step, outcome, reason))

    service = AgentOSInstanceLifecycle(
        control,
        RuntimeProbeSettings(
            wait_running_timeout_seconds=0.2,
            wait_running_interval_seconds=0.001,
        ),
        cleanup_step=cleanup_step,
        operations=operations,
    )
    return service, cleanup_steps


def _attempt(create: Any, **fields: Any) -> AgentOSSandboxAttempt:
    return AgentOSSandboxAttempt(
        generation="generation-1", deterministic_id="candidate", create=create, **fields,
    )


async def test_uncertain_create_keeps_same_attempt_for_replay() -> None:
    control = FakeControl()
    service, _ = _service(control)
    creates = 0

    async def create() -> SandboxInfo:
        nonlocal creates
        creates += 1
        if creates == 1:
            raise AgentTimeoutError("response lost")
        return SandboxInfo("candidate")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()

    with pytest.raises(AgentRebuildPending, match="create result is unknown"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is attempt

    sandbox = await service.provision_sandbox(lifecycle, attempt)
    assert sandbox.sandbox_id == "candidate"
    assert creates == 2
    assert control.wait_calls == [("candidate", "generation-1")]
    assert lifecycle.attempt is None
    assert attempt.uncertain_attempts == 1


async def test_uncertain_replay_is_bounded_then_retires_without_a_second_post() -> None:
    control = FakeControl()
    service, cleanup_steps = _service(control)
    creates = 0

    async def create() -> SandboxInfo:
        nonlocal creates
        creates += 1
        raise AgentCreateUncertainError("outcome unknown")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()

    for _ in range(MAX_UNCERTAIN_CREATE_ATTEMPTS - 1):
        with pytest.raises(AgentRebuildPending, match="create result is unknown"):
            await service.provision_sandbox(lifecycle, attempt)
        assert lifecycle.attempt is attempt
    with pytest.raises(AgentCreateFailed, match="stayed uncertain"):
        await service.provision_sandbox(lifecycle, attempt)

    assert creates == MAX_UNCERTAIN_CREATE_ATTEMPTS
    assert control.wait_calls == []
    assert attempt.uncertain_attempts == MAX_UNCERTAIN_CREATE_ATTEMPTS
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == "candidate"
    assert lifecycle.delete_retry_at is not None
    assert control.delete_calls == ["candidate"]
    assert cleanup_steps == [("frontend", "incomplete", "result_unconfirmed")]


async def test_first_create_timeout_retains_attempt_instead_of_minting_a_generation() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise AgentTimeoutError("response lost")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentRebuildPending, match="create result is unknown"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is attempt
    assert attempt.uncertain_attempts == 1
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_unsent_first_create_is_retryable_without_retaining_the_name() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise AgentRequestNotSentError("connection setup failed")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentRebuildPending, match="create request was not sent"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == ""


async def test_rate_limited_first_create_is_a_retryable_pre_create_failure() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise AgentApiError("rate limited", status_code=429)

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentPreCreateError, match="rate"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is None
    assert control.delete_calls == []


async def test_rejected_create_retires_candidate_and_requests_delete() -> None:
    control = FakeControl()
    service, cleanup_steps = _service(control)
    creates = 0

    async def create() -> SandboxInfo:
        nonlocal creates
        creates += 1
        if creates == 1:
            raise AgentTimeoutError("response lost")
        raise ValueError("invalid workspace")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentRebuildPending, match="create result is unknown"):
        await service.provision_sandbox(lifecycle, attempt)
    with pytest.raises(AgentRebuildPending, match="replacement creation was rejected"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == "candidate"
    assert control.delete_calls == ["candidate"]
    assert cleanup_steps == [("frontend", "incomplete", "result_unconfirmed")]


async def test_replayed_create_never_deletes_a_different_generation() -> None:
    control = FakeControl()
    service, _ = _service(control)
    operations = SimpleNamespace(request_sandbox_delete=AsyncMock(return_value=False))

    async def create() -> SandboxInfo:
        raise AgentGenerationMismatchError("another Agent generation")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle(
        attempt=attempt, retired_instance="previous-instance",
    )
    with pytest.raises(AgentRebuildPending, match="generation did not match"):
        await service.post_sandbox_attempt(
            lifecycle, attempt, asyncio.get_running_loop().time() + 1,
        )
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == "previous-instance"
    operations.request_sandbox_delete.assert_not_awaited()
    assert control.delete_calls == []


async def test_rate_limited_replay_keeps_the_possible_live_instance() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise AgentApiError("rate limited", status_code=429)

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt)
    with pytest.raises(AgentRebuildPending, match="rate limited"):
        await service.post_sandbox_attempt(
            lifecycle, attempt, asyncio.get_running_loop().time() + 1,
        )
    assert lifecycle.attempt is attempt
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_retirement_completes_only_on_explicit_not_found() -> None:
    control = FakeControl()
    service, _ = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    control.get_error = AgentApiError("temporary failure")

    assert not await service.observe_retired_sandbox(lifecycle)
    control.get_error = AgentNotFoundError("gone")
    assert await service.observe_retired_sandbox(lifecycle)
    assert control.get_calls == ["old", "old"]
    assert lifecycle.retired_instance == "old"


async def test_retirement_wait_times_out_without_deleting() -> None:
    control = FakeControl()
    service, _ = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    control.get_error = AgentApiError("temporary failure")

    with pytest.raises(AgentRebuildPending, match="waiting for the old sandbox"):
        await service.wait_for_retired_sandbox(
            lifecycle, asyncio.get_running_loop().time() + 0.05,
        )
    assert len(control.get_calls) == 1
    assert control.delete_calls == []


async def test_retirement_wait_returns_once_absence_is_observed() -> None:
    control = FakeControl()
    service, _ = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    control.get_error = AgentApiError("temporary failure")

    async def disappear_later() -> None:
        await asyncio.sleep(0.01)
        control.get_error = AgentNotFoundError("gone")

    waiter = asyncio.create_task(service.wait_for_retired_sandbox(
        lifecycle, asyncio.get_running_loop().time() + 1,
    ))
    await asyncio.gather(disappear_later(), waiter)
    assert waiter.exception() is None


async def test_generation_mismatch_stays_pending_without_delete() -> None:
    control = FakeControl()
    service, _ = _service(control)
    control.wait_error = AgentGenerationMismatchError("wrong generation")
    attempt = _attempt(AsyncMock(), sandbox=SandboxInfo("candidate"))
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt)

    with pytest.raises(AgentRebuildPending, match="generation did not match"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is attempt
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_failed_replacement_is_retired_and_delete_is_requested() -> None:
    control = FakeControl()
    service, _ = _service(control)
    control.wait_error = AgentFailedError("terminal")
    attempt = _attempt(AsyncMock(), sandbox=SandboxInfo("candidate"))
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt)

    with pytest.raises(AgentRebuildPending, match="failed to start"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == "candidate"
    assert control.delete_calls == ["candidate"]


async def test_not_visible_candidate_can_resume_same_attempt() -> None:
    control = FakeControl()
    service, _ = _service(control)
    control.wait_error = AgentNotVisibleError("not visible")
    attempt = _attempt(AsyncMock(), sandbox=SandboxInfo("candidate"))
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt)

    with pytest.raises(AgentRebuildPending, match="not visible yet"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is attempt
    assert attempt.sandbox is None


async def test_rejected_delete_reports_failure_and_schedules_retry() -> None:
    control = FakeControl()
    service, cleanup_steps = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    control.delete_error = AgentDeleteRejectedError("delete rejected")

    assert await service.request_sandbox_delete(lifecycle)
    assert control.delete_calls == ["old"]
    assert lifecycle.delete_retry_at is not None
    assert cleanup_steps == [("frontend", "failed", "AgentDeleteRejectedError")]


async def test_accepted_delete_stays_unconfirmed_and_schedules_a_slow_retry() -> None:
    control = FakeControl()
    service, cleanup_steps = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")

    assert not await service.request_sandbox_delete(lifecycle)
    assert control.delete_calls == ["old"]
    assert lifecycle.delete_retry_at is not None
    assert lifecycle.delete_retry_at - asyncio.get_running_loop().time() > 0.9
    assert cleanup_steps == [("frontend", "incomplete", "result_unconfirmed")]


async def test_observer_retries_delete_only_after_the_scheduled_positive_observation() -> None:
    control = FakeControl()
    operations = SimpleNamespace(
        request_sandbox_delete=AsyncMock(return_value=False),
    )
    service, _ = _service(control, operations)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old", delete_retry_at=0.0)

    assert not await service.observe_retired_sandbox(lifecycle)
    operations.request_sandbox_delete.assert_awaited_once_with(lifecycle)
    operations.request_sandbox_delete.return_value = True
    with pytest.raises(AgentRebuildPending, match="old sandbox deletion was rejected"):
        await service.observe_retired_sandbox(lifecycle)
    assert not await service.observe_retired_sandbox(lifecycle, allow_retry=False)


async def test_rejected_retry_surfaces_as_pending_not_as_a_positive_observation() -> None:
    control = FakeControl()
    operations = SimpleNamespace(request_sandbox_delete=AsyncMock(return_value=True))
    service, _ = _service(control, operations)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old", delete_retry_at=0.0)

    with pytest.raises(AgentRebuildPending, match="old sandbox deletion was rejected"):
        await service.observe_retired_sandbox(lifecycle)


@pytest.mark.parametrize("error", [
    AgentApiError("unavailable", status_code=503),
    AgentApiError("request timeout", status_code=408),
])
async def test_first_create_5xx_or_408_is_uncertain_and_retains_the_attempt(error) -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise error

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentRebuildPending, match="create result is unknown"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is attempt
    assert attempt.uncertain_attempts == 1
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_other_rejected_first_create_propagates_without_retaining_the_name() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise AgentApiError("invalid runtime spec", status_code=400)

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(AgentApiError, match="invalid runtime spec"):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_cancelled_first_create_retains_the_attempt_for_the_next_caller() -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise asyncio.CancelledError()

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle()
    with pytest.raises(asyncio.CancelledError):
        await service.provision_sandbox(lifecycle, attempt)
    assert lifecycle.attempt is attempt
    assert lifecycle.retired_instance == ""


@pytest.mark.parametrize("error", [
    AgentApiError("unavailable", status_code=503),
    AgentApiError("request timeout", status_code=408),
])
async def test_replayed_5xx_or_408_keeps_the_same_attempt(error) -> None:
    control = FakeControl()
    service, _ = _service(control)

    async def create() -> SandboxInfo:
        raise error

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt, retired_instance="previous-instance")
    with pytest.raises(AgentRebuildPending, match="create result is unknown"):
        await service.post_sandbox_attempt(
            lifecycle, attempt, asyncio.get_running_loop().time() + 1,
        )
    assert lifecycle.attempt is attempt
    assert attempt.uncertain_attempts == 1
    assert lifecycle.retired_instance == "previous-instance"
    assert control.delete_calls == []


@pytest.mark.parametrize("error", [
    AgentApiError("still verifying"),
    AgentTimeoutError("timed out"),
])
async def test_unverified_candidate_stays_pending_and_resumable(error) -> None:
    control = FakeControl()
    service, _ = _service(control)
    control.wait_error = error
    attempt = _attempt(AsyncMock(), sandbox=SandboxInfo("candidate"))
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt)

    with pytest.raises(AgentRebuildPending, match="not verified yet"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is attempt
    assert attempt.sandbox is not None
    assert lifecycle.retired_instance == ""
    assert control.delete_calls == []


async def test_replacement_reusing_the_retired_id_is_never_published() -> None:
    control = FakeControl()
    service, _ = _service(control)
    attempt = _attempt(AsyncMock(), sandbox=SandboxInfo("retired-1"))
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt, retired_instance="retired-1")

    with pytest.raises(AgentRebuildPending, match="reused the retired sandbox ID"):
        await service.provision_sandbox(lifecycle, attempt)

    assert lifecycle.attempt is attempt
    assert lifecycle.retired_instance == "retired-1"
    assert control.wait_calls == []


async def test_verified_replacement_waits_for_the_retired_sandbox_before_posting() -> None:
    control = FakeControl()
    service, _ = _service(control)
    control.get_error = AgentNotFoundError("gone")
    creates = 0

    async def create() -> SandboxInfo:
        nonlocal creates
        creates += 1
        return SandboxInfo("candidate")

    attempt = _attempt(create)
    lifecycle = AgentOSSandboxLifecycle(attempt=attempt, retired_instance="old")

    sandbox = await service.provision_sandbox(lifecycle, attempt)

    assert sandbox.sandbox_id == "candidate"
    assert creates == 1
    assert control.get_calls == ["old"]
    assert control.wait_calls == [("candidate", "generation-1")]
    assert lifecycle.attempt is None
    assert lifecycle.retired_instance == ""
    assert lifecycle.delete_retry_at is None


async def test_unsent_delete_is_a_definite_rejection_with_a_fast_retry() -> None:
    control = FakeControl()
    service, cleanup_steps = _service(control)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    control.delete_error = AgentRequestNotSentError("not sent")

    assert await service.request_sandbox_delete(lifecycle)
    assert control.delete_calls == ["old"]
    assert lifecycle.delete_retry_at is not None
    assert lifecycle.delete_retry_at - asyncio.get_running_loop().time() < 0.9
    assert cleanup_steps == [("frontend", "failed", "AgentRequestNotSentError")]


def _router(tmp_path) -> ManagedRouter:
    (tmp_path / "user").mkdir()
    backend = SimpleNamespace(
        agent_namespace="default",
        frontend_endpoint="http://frontend.test",
        server_ready=False,
        connect=AsyncMock(),
        disconnect=AsyncMock(),
        get_agent_info=AsyncMock(return_value={"status": "running"}),
        wait_until_running=AsyncMock(return_value={"status": "running"}),
        delete_sandbox=AsyncMock(),
    )
    registry = SimpleNamespace(unregister_agent=AsyncMock())
    return ManagedRouter(
        ManagedSandboxControl(backend), registry, AgentManager(),
        workspace_root=str(tmp_path), connect_warmup_enabled=False,
    )


async def test_router_lifecycle_wrappers_delegate_to_the_extracted_service(tmp_path) -> None:
    router = _router(tmp_path)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    service = router._sandbox_lifecycle_service()
    calls = []
    for name in (
        "wait_for_retired_sandbox", "observe_retired_sandbox", "replay_or_fail_uncertain_create",
        "post_sandbox_attempt", "provision_sandbox", "delete_retired_sandbox", "request_sandbox_delete",
    ):
        async def record(*args, _name=name, **_kwargs):
            calls.append(_name)
            return False

        setattr(service, name, record)

    await router._wait_for_retired_sandbox(lifecycle, 1.0)
    await router._observe_retired_sandbox(lifecycle)
    await router._replay_or_fail_uncertain_create(lifecycle, _attempt(AsyncMock()), RuntimeError())
    await router._post_sandbox_attempt(lifecycle, _attempt(AsyncMock()), 1.0)
    await router._provision_sandbox(lifecycle, _attempt(AsyncMock()))
    await router._delete_retired_sandbox(lifecycle)
    assert await router._request_sandbox_delete(lifecycle) is False
    assert calls == [
        "wait_for_retired_sandbox", "observe_retired_sandbox", "replay_or_fail_uncertain_create",
        "post_sandbox_attempt", "provision_sandbox", "delete_retired_sandbox", "request_sandbox_delete",
    ]


async def test_router_wrapper_still_calls_patchable_router_delete(tmp_path, monkeypatch) -> None:
    router = _router(tmp_path)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    patched: list[AgentOSSandboxLifecycle] = []

    async def delete_retired_sandbox(patched_lifecycle: AgentOSSandboxLifecycle) -> None:
        patched.append(patched_lifecycle)
        patched_lifecycle.delete_retry_at = 123.0

    monkeypatch.setattr(router, "_delete_retired_sandbox", delete_retired_sandbox)
    assert not await router._request_sandbox_delete(lifecycle)
    assert patched == [lifecycle]


async def test_router_service_follows_reassigned_probe_settings(tmp_path) -> None:
    router = _router(tmp_path)
    service = router._sandbox_lifecycle_service()
    assert service.probe_settings is DEFAULT_RUNTIME_PROBE_SETTINGS

    changed = replace(DEFAULT_RUNTIME_PROBE_SETTINGS, wait_running_timeout_seconds=7)
    router._probe_settings = changed
    await router._observe_retired_sandbox(AgentOSSandboxLifecycle(retired_instance="old"))
    assert router._sandbox_lifecycle_service().probe_settings is changed


async def test_router_keeps_a_recorded_delete_result_instead_of_downgrading_it(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(telemetry, "_runtime", None)
    telemetry.configure_gateway_telemetry({"telemetry": {"enabled": True, "exporter": "none"}})
    router = _router(tmp_path)
    lifecycle = AgentOSSandboxLifecycle(retired_instance="old")
    operation = telemetry.start_gateway_trace("gw.agent.delete", root=True)
    assert operation is not None

    with telemetry.bind_request_trace(operation), telemetry.bind_cleanup_trace(operation):
        telemetry.record_cleanup_result("frontend.delete", "success")
        assert not await router._request_sandbox_delete(lifecycle)
        assert telemetry.get_cleanup_result("frontend.delete") == "success"


def test_managed_router_retains_its_lifecycle_record_aliases() -> None:
    from agent_gateway.southbound.orchestration import managed_router

    assert managed_router._SandboxAttempt is AgentOSSandboxAttempt
    assert managed_router._SandboxLifecycle is AgentOSSandboxLifecycle
    assert managed_router._MAX_UNCERTAIN_CREATE_ATTEMPTS == MAX_UNCERTAIN_CREATE_ATTEMPTS
