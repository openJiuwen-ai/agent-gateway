# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Physical sandbox lifecycle over the managed control plane.

The business router and a worker's dynamic-instance leaf share these
operations so uncertain-create replay, retirement fencing and delete retries
cannot diverge. Nothing here routes requests, touches the registry or publishes
a runtime: callers hold the per-name ``AgentOSSandboxLifecycle`` lock and own
what becomes visible to requests.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from agent_gateway.extensions.agentos.agentos_router.backend import (
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
    AgentPreCreateError,
    AgentRebuildPending,
)


# Keep the logger name this code used before moving out of the AgentOS router client.
logger = logging.getLogger("agent_gateway.extensions.agentos.agentos_router.router_client")

CleanupStep = Callable[[str, str, str], None]

# 同一个 attempt 的创建结果不确定时，首次 + 最多 3 次同名重放；超限后
# 退役当前实例并转 FAILED，避免确定性 5xx 让用户永远停在 pending。
MAX_UNCERTAIN_CREATE_ATTEMPTS = 4


@dataclass
class AgentOSSandboxAttempt:
    generation: str
    deterministic_id: str
    create: Callable[[], Awaitable[SandboxInfo]]
    sandbox: SandboxInfo | None = None
    uncertain_attempts: int = 0


@dataclass
class AgentOSSandboxLifecycle:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0
    retired_instance: str = ""
    delete_retry_at: float | None = None
    retirement_task: asyncio.Task[None] | None = None
    attempt: AgentOSSandboxAttempt | None = None
    provisioned_instance: str = ""
    shared_sandbox: SandboxInfo | None = None
    shared_sandbox_owner: str = ""


class AgentOSInstanceLifecycleOperations(Protocol):
    """Lifecycle steps a host may wrap; the service uses itself when none are given."""

    async def wait_for_retired_sandbox(
        self, lifecycle: AgentOSSandboxLifecycle, deadline: float,
    ) -> None: ...

    async def observe_retired_sandbox(
        self, lifecycle: AgentOSSandboxLifecycle, *, allow_retry: bool = True,
    ) -> bool: ...

    async def replay_or_fail_uncertain_create(
        self, lifecycle: AgentOSSandboxLifecycle, attempt: AgentOSSandboxAttempt, exc: Exception,
    ) -> None: ...

    async def post_sandbox_attempt(
        self, lifecycle: AgentOSSandboxLifecycle, attempt: AgentOSSandboxAttempt, deadline: float,
    ) -> None: ...

    async def delete_retired_sandbox(self, lifecycle: AgentOSSandboxLifecycle) -> None: ...

    async def request_sandbox_delete(self, lifecycle: AgentOSSandboxLifecycle) -> bool: ...


class AgentOSInstanceLifecycle:
    """Create and retire one physical AgentOS sandbox, without publishing a runtime."""

    def __init__(
        self,
        control: ManagedSandboxControl,
        probe_settings: RuntimeProbeSettings,
        *,
        cleanup_step: CleanupStep,
        operations: AgentOSInstanceLifecycleOperations | None = None,
    ) -> None:
        self._control = control
        self._probe_settings = probe_settings
        self._cleanup_step = cleanup_step
        self._operations: AgentOSInstanceLifecycleOperations = operations if operations is not None else self

    @property
    def probe_settings(self) -> RuntimeProbeSettings:
        return self._probe_settings

    @probe_settings.setter
    def probe_settings(self, value: RuntimeProbeSettings) -> None:
        self._probe_settings = value

    async def wait_for_retired_sandbox(
        self, lifecycle: AgentOSSandboxLifecycle, deadline: float,
    ) -> None:
        loop = asyncio.get_running_loop()
        retries = 0
        try:
            async with asyncio.timeout_at(deadline):
                while True:
                    retry_at = lifecycle.delete_retry_at
                    if await self._operations.observe_retired_sandbox(lifecycle, allow_retry=retries < 3):
                        return
                    if lifecycle.delete_retry_at != retry_at:
                        retries += 1
                    await asyncio.sleep(min(
                        max(0.1, self._probe_settings.wait_running_interval_seconds),
                        max(0.0, deadline - loop.time()),
                    ))
        except TimeoutError as exc:
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: waiting for the old sandbox") from exc

    async def observe_retired_sandbox(
        self, lifecycle: AgentOSSandboxLifecycle, *, allow_retry: bool = True,
    ) -> bool:
        """Return true only for an explicit absence; retry only after a positive observation."""
        try:
            await self._control.query(lifecycle.retired_instance)
        except AgentNotFoundError:
            lifecycle.delete_retry_at = None
            return True
        except AgentApiError:
            return False
        retry_at = lifecycle.delete_retry_at
        if allow_retry and retry_at is not None and asyncio.get_running_loop().time() >= retry_at:
            if await self._operations.request_sandbox_delete(lifecycle):
                raise AgentRebuildPending("AGENT_REBUILD_PENDING: old sandbox deletion was rejected")
        return False

    async def replay_or_fail_uncertain_create(
        self,
        lifecycle: AgentOSSandboxLifecycle,
        attempt: AgentOSSandboxAttempt,
        exc: Exception,
    ) -> None:
        """Replay an uncertain create with the same name, up to a hard limit.

        The same name cannot create a second sandbox, but a deterministic 5xx
        would otherwise replay forever and never let the runtime reach FAILED.
        """
        attempt.uncertain_attempts += 1
        if attempt.uncertain_attempts < MAX_UNCERTAIN_CREATE_ATTEMPTS:
            lifecycle.attempt = attempt
            raise AgentRebuildPending(
                "AGENT_REBUILD_PENDING: the create result is unknown"
            ) from exc

        lifecycle.retired_instance = attempt.deterministic_id
        lifecycle.delete_retry_at = None
        lifecycle.attempt = None
        await self._operations.request_sandbox_delete(lifecycle)
        raise AgentCreateFailed(
            "sandbox create stayed uncertain after "
            f"{attempt.uncertain_attempts} attempts: {exc}"
        ) from exc

    async def post_sandbox_attempt(
        self, lifecycle: AgentOSSandboxLifecycle, attempt: AgentOSSandboxAttempt, deadline: float,
    ) -> None:
        try:
            async with asyncio.timeout_at(deadline):
                attempt.sandbox = await attempt.create()
        except (TimeoutError, AgentTimeoutError, AgentCreateUncertainError) as exc:
            await self._operations.replay_or_fail_uncertain_create(lifecycle, attempt, exc)
        except AgentGenerationMismatchError as exc:
            # A replay may find a name owned by another generation.
            lifecycle.attempt = None
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: the sandbox generation did not match") from exc
        except (ValueError, AgentApiError) as exc:
            status = getattr(exc, "status_code", None)
            if isinstance(exc, AgentApiError) and status == 429:
                # 429 未创建新沙箱，但上一次结果不确定的 POST 可能已经建好实例。
                # 保留 attempt 让下次请求重放，不要退役并删除这个可能存活的实例。
                raise AgentRebuildPending(
                    "AGENT_REBUILD_PENDING: replacement creation was rate limited"
                ) from exc
            if isinstance(exc, AgentApiError) and (
                status == 408 or (isinstance(status, int) and status >= 500)
            ):
                # 408/5xx 的结果不确定：保留 attempt，下一次请求重放同一名称。
                await self._operations.replay_or_fail_uncertain_create(lifecycle, attempt, exc)
            # Retire this name so the next request starts a new generation instead of replaying a refused create.
            lifecycle.retired_instance = attempt.deterministic_id
            lifecycle.attempt = None
            await self._operations.request_sandbox_delete(lifecycle)
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: replacement creation was rejected") from exc

    async def provision_sandbox(
        self,
        lifecycle: AgentOSSandboxLifecycle,
        attempt: AgentOSSandboxAttempt,
    ) -> SandboxInfo:
        if not lifecycle.retired_instance and lifecycle.attempt is None:
            try:
                return await attempt.create()
            except AgentRequestNotSentError as exc:
                raise AgentRebuildPending("AGENT_REBUILD_PENDING: create request was not sent") from exc
            except (AgentTimeoutError, AgentCreateUncertainError) as exc:
                # Keep this attempt so later callers replay the same name
                # instead of minting a generation that could orphan this POST.
                await self._operations.replay_or_fail_uncertain_create(lifecycle, attempt, exc)
            except asyncio.CancelledError:
                lifecycle.attempt = attempt
                raise
            except AgentApiError as exc:
                # 429 is an explicit rejection: no sandbox was created by this
                # attempt. Treat it as a retryable pre-create failure instead of
                # caching a permanent FAILED runtime. Other 4xx may be
                # deterministic (bad runtime_spec etc.) and keep the old path.
                status = getattr(exc, "status_code", None)
                if status == 429:
                    raise AgentPreCreateError(
                        "AGENT_PRE_CREATE_RETRYABLE: sandbox create was rate "
                        f"limited: {exc}"
                    ) from exc
                if status == 408 or (isinstance(status, int) and status >= 500):
                    # 408/5xx 的结果不确定：保留 attempt，下一次请求重放同一名称。
                    await self._operations.replay_or_fail_uncertain_create(lifecycle, attempt, exc)
                raise

        deadline = asyncio.get_running_loop().time() + self._probe_settings.wait_running_timeout_seconds
        lifecycle.attempt = attempt
        if attempt.sandbox is None:
            if lifecycle.retired_instance:
                await self._operations.wait_for_retired_sandbox(lifecycle, deadline)
            # Environment derives the instance ID from the name and returns the existing instance
            # on a repeated create (AGENT_REUSE_EXISTING_INSTANCE), so a replay cannot start a second one.
            await self._operations.post_sandbox_attempt(lifecycle, attempt, deadline)
        sandbox = attempt.sandbox
        if sandbox.sandbox_id == lifecycle.retired_instance:
            # A delayed DELETE still targets this ID even if its marker
            # changed. Never publish it as a replacement.
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: replacement reused the retired sandbox ID")
        try:
            await self._control.verify_generation(
                sandbox, generation=attempt.generation, deadline=deadline,
            )
        except AgentGenerationMismatchError as exc:
            # Each generation has its own name. A mismatch cannot establish
            # ownership of the instance or justify deleting it.
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: the sandbox generation did not match") from exc
        except AgentFailedError as exc:
            lifecycle.retired_instance = sandbox.sandbox_id
            lifecycle.attempt = None
            await self._operations.request_sandbox_delete(lifecycle)
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: replacement sandbox failed to start") from exc
        except AgentNotVisibleError as exc:
            if sandbox.sandbox_id == attempt.deterministic_id:
                # A later caller can retry this same physical name after
                # the acknowledged instance remains invisible.
                attempt.sandbox = None
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: replacement sandbox is not visible yet") from exc
        except AgentApiError as exc:
            # Keep the candidate so the next caller can resume polling it.
            raise AgentRebuildPending("AGENT_REBUILD_PENDING: replacement sandbox is not verified yet") from exc
        lifecycle.retired_instance = ""
        lifecycle.delete_retry_at = None
        lifecycle.attempt = None
        return sandbox

    async def delete_retired_sandbox(self, lifecycle: AgentOSSandboxLifecycle) -> None:
        # The frontend may ACK before its background kill runs, or lose its
        # response after accepting the request. In both cases give that kill
        # time to complete before a positive observation can trigger a retry.
        retry_delay = max(1.0, self._probe_settings.wait_running_interval_seconds)
        try:
            async with asyncio.timeout(self._probe_settings.wait_running_timeout_seconds):
                await self._control.delete(lifecycle.retired_instance)
        except (AgentRequestNotSentError, AgentDeleteRejectedError):
            retry_delay = max(0.01, self._probe_settings.wait_running_interval_seconds)
            raise
        finally:
            lifecycle.delete_retry_at = asyncio.get_running_loop().time() + retry_delay

    async def request_sandbox_delete(self, lifecycle: AgentOSSandboxLifecycle) -> bool:
        """Request deletion and return whether the frontend definitely rejected it."""
        try:
            await self._operations.delete_retired_sandbox(lifecycle)
            self._cleanup_step("frontend", "incomplete", "result_unconfirmed")
        except Exception as exc:
            self._cleanup_step("frontend", "failed", type(exc).__name__)
            logger.exception("Environment sandbox delete failed: instance_id=%s", lifecycle.retired_instance)
            return isinstance(exc, (AgentRequestNotSentError, AgentDeleteRejectedError))
        return False
