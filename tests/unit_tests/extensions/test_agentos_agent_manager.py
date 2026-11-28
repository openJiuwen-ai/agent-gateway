from __future__ import annotations

import asyncio
import logging
import time

import pytest

from agent_gateway.extensions.agentos.agentos_router.agent_manager import (
    AgentCreateFailed,
    AgentCreatingTimeout,
    AgentDeleted,
    AgentManager,
    AgentRebuildPending,
    AgentRuntime,
    normalize_agent_key_fields,
)
from agent_gateway.extensions.agentos.agentos_router.models import AgentInfo, AgentStatus


@pytest.mark.asyncio
async def test_get_or_create_agent_is_single_flight() -> None:
    agent_manager = AgentManager()
    creator_started = asyncio.Event()
    allow_creator = asyncio.Event()
    create_calls = 0

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        creator_started.set()
        await allow_creator.wait()
        agent.sandbox_id = "sandbox-1"
        return agent

    first = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await creator_started.wait()
    second = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )

    creating = await agent_manager.list_user_agents("u1")
    assert len(creating) == 1
    assert creating[0].info.status is AgentStatus.CREATING

    allow_creator.set()
    first_agent, second_agent = await asyncio.gather(first, second)
    assert create_calls == 1
    assert first_agent.info.agent_id == second_agent.info.agent_id
    assert first_agent.info.status is AgentStatus.READY
    assert first_agent.info.sandbox_id == "sandbox-1"


@pytest.mark.asyncio
async def test_waiting_for_creation_times_out() -> None:
    agent_manager = AgentManager(creating_timeout_seconds=0.1)
    creator_started = asyncio.Event()
    never = asyncio.Event()

    async def creator(agent: AgentInfo) -> AgentInfo:
        creator_started.set()
        await never.wait()
        return agent

    owner = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await creator_started.wait()

    with pytest.raises(AgentCreatingTimeout, match="AGENT_CREATING_TIMEOUT"):
        await agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)

    owner.cancel()
    await asyncio.gather(owner, return_exceptions=True)
    failed = await agent_manager.list_user_agents("u1")
    assert failed[0].info.status is AgentStatus.FAILED


@pytest.mark.asyncio
async def test_failed_create_does_not_retry() -> None:
    agent_manager = AgentManager()
    create_calls = 0

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)

    with pytest.raises(AgentCreateFailed, match="AGENT_CREATE_FAILED"):
        await agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)

    assert create_calls == 1
    failed = await agent_manager.list_user_agents("u1")
    assert failed[0].info.status is AgentStatus.FAILED


@pytest.mark.asyncio
async def test_cancelled_create_does_not_retry() -> None:
    agent_manager = AgentManager()
    create_calls = 0
    creator_started = asyncio.Event()
    never = asyncio.Event()

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        creator_started.set()
        await never.wait()
        return agent

    owner = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await creator_started.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner

    with pytest.raises(AgentCreateFailed, match="AGENT_CREATE_FAILED"):
        await agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)

    assert create_calls == 1


@pytest.mark.asyncio
async def test_failed_runtime_list_and_pop_respect_age_and_holds() -> None:
    manager = AgentManager()
    failed = await manager.get_or_create_agent("u1", "jiuwenswarm")
    stored = manager._runtimes[failed.key]
    stored.mark_failed(RuntimeError("boom"))
    stored.info.updated_at = time.time() - 301

    listed = await manager.list_failed_agents(300)
    assert [item.info.agent_id for item in listed] == [failed.info.agent_id]
    assert await manager.pop_failed_if_expired(failed.key, 300) is not None
    assert await manager.get_agent("u1", "jiuwenswarm") is None

    held = await manager.get_or_create_agent("u2", "jiuwenswarm", acquire=True)
    held_stored = manager._runtimes[held.key]
    held_stored.mark_failed(RuntimeError("boom"))
    held_stored.info.updated_at = time.time() - 301
    assert await manager.list_failed_agents(300) == []
    assert await manager.pop_failed_if_expired(held.key, 300) is None

    await manager.release(held.key)
    assert await manager.pop_failed_if_expired(held.key, 300) is not None


@pytest.mark.asyncio
async def test_state_transition_events_and_invalid_transition_is_logged(caplog) -> None:
    runtime = AgentRuntime.for_key(
        ("u1", "jiuwenswarm"), user_id="u1", agent_type="jiuwenswarm",
    )
    ready = runtime.info.copy()

    with caplog.at_level(logging.INFO):
        runtime.mark_ready(ready, reason="create_ok")
    assert "agent.state.changed" in caplog.text
    assert "from_status=creating" in caplog.text
    assert "to_status=ready" in caplog.text

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        runtime.mark_failed(RuntimeError("boom"), reason="test_failure")
    assert "agent.state.invalid_transition" in caplog.text
    assert "from_status=ready" in caplog.text
    assert "to_status=failed" in caplog.text
    # Best-effort: the illegal transition is reported but still applied.
    assert runtime.info.status is AgentStatus.FAILED


@pytest.mark.asyncio
async def test_agent_manager_get_delete_and_list() -> None:
    agent_manager = AgentManager()
    created = await agent_manager.get_or_create_agent("u1", "jiuwenswarm")

    fetched = await agent_manager.get_agent("u1", "jiuwenswarm")
    assert fetched is not None
    assert fetched.info == created.info
    assert [
        item.info.agent_id for item in await agent_manager.list_user_agents("u1")
    ] == [created.info.agent_id]

    await agent_manager.delete_agent("u1", "jiuwenswarm")
    assert await agent_manager.get_agent("u1", "jiuwenswarm") is None
    assert await agent_manager.list_user_agents("u1") == []


@pytest.mark.asyncio
async def test_delete_during_creation_wakes_waiter() -> None:
    agent_manager = AgentManager(creating_timeout_seconds=5.0)
    creator_started = asyncio.Event()
    allow_creator = asyncio.Event()
    create_calls = 0

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        creator_started.set()
        await allow_creator.wait()
        agent.sandbox_id = "sandbox-1"
        return agent

    owner = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await creator_started.wait()
    waiter = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await asyncio.sleep(0.05)

    await agent_manager.delete_agent("u1", "jiuwenswarm")

    with pytest.raises(AgentDeleted, match="AGENT_DELETED"):
        await waiter

    allow_creator.set()
    with pytest.raises(AgentDeleted, match="AGENT_DELETED"):
        await owner

    assert await agent_manager.get_agent("u1", "jiuwenswarm") is None
    assert create_calls == 1


@pytest.mark.asyncio
async def test_get_or_create_after_delete_during_creation_can_retry() -> None:
    agent_manager = AgentManager()
    creator_started = asyncio.Event()
    allow_creator = asyncio.Event()
    create_calls = 0

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        creator_started.set()
        await allow_creator.wait()
        agent.sandbox_id = f"sandbox-{create_calls}"
        return agent

    owner = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await creator_started.wait()
    waiter = asyncio.create_task(
        agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    await asyncio.sleep(0.05)

    await agent_manager.delete_agent("u1", "jiuwenswarm")

    allow_creator.set()
    results = await asyncio.gather(waiter, owner, return_exceptions=True)
    assert all(isinstance(result, AgentDeleted) for result in results)

    recreated = await agent_manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    assert recreated.info.status is AgentStatus.READY
    assert recreated.info.sandbox_id == "sandbox-2"
    assert create_calls == 2


def test_normalize_agent_type_keeps_registry_case() -> None:
    assert AgentRuntime.normalize_agent_type("Claude-Code") == "Claude-Code"
    assert AgentRuntime.normalize_agent_type("  claude-code  ") == "claude-code"
    assert AgentRuntime.normalize_agent_type("JiuwenSwarm") == "jiuwenswarm"
    assert AgentRuntime.normalize_agent_type("JIUWENSWARM") == "jiuwenswarm"
    assert AgentRuntime.normalize_agent_type("") == "jiuwenswarm"
    assert AgentRuntime.normalize_agent_type(None) == "jiuwenswarm"


@pytest.mark.asyncio
async def test_get_or_create_reuses_builtin_ignoring_case() -> None:
    agent_manager = AgentManager()
    first = await agent_manager.get_or_create_agent("u1", "JiuwenSwarm")
    second = await agent_manager.get_or_create_agent("u1", "jiuwenswarm")
    mixed = await agent_manager.get_or_create_agent("u1", "Claude-Code")
    assert first.key == second.key == ("u1", "jiuwenswarm")
    assert first.info.agent_type == "jiuwenswarm"
    assert mixed.key == ("u1", "Claude-Code")
    assert mixed.info.agent_type == "Claude-Code"


def test_normalize_agent_key_fields_defaults_and_aliases() -> None:
    assert normalize_agent_key_fields(None) == ("user_id", "agent_type")
    assert normalize_agent_key_fields("user_id+agent_type") == (
        "user_id",
        "agent_type",
    )
    assert normalize_agent_key_fields(
        ["user_id", "agent_type", "session_id"]
    ) == ("user_id", "agent_type", "session_id")
    with pytest.raises(ValueError, match="unsupported agent_key_field"):
        normalize_agent_key_fields(["user_id", "channel"])
    with pytest.raises(ValueError, match="must include user_id and agent_type"):
        normalize_agent_key_fields(["user_id"])


@pytest.mark.asyncio
async def test_acquire_and_release_track_task_count() -> None:
    agent_manager = AgentManager()

    first = await agent_manager.get_or_create_agent(
        "u1", "jiuwenswarm", acquire=True
    )
    assert first.task_count == 1

    second = await agent_manager.get_or_create_agent(
        "u1", "jiuwenswarm", acquire=True
    )
    assert second.task_count == 2

    await agent_manager.release(first.key)
    await agent_manager.release(first.key)
    fetched = await agent_manager.get_agent("u1", "jiuwenswarm")
    assert fetched is not None
    assert fetched.task_count == 0

    # Extra release stays floored at zero; release on a missing key is a no-op.
    await agent_manager.release(first.key)
    fetched = await agent_manager.get_agent("u1", "jiuwenswarm")
    assert fetched.task_count == 0
    await agent_manager.delete_agent("u1", "jiuwenswarm")
    await agent_manager.release(first.key)


@pytest.mark.asyncio
async def test_get_agent_acquire_holds_ready_runtime() -> None:
    agent_manager = AgentManager()
    created = await agent_manager.get_or_create_agent("u1", "claude")
    assert created.task_count == 0

    held = await agent_manager.get_agent("u1", "claude", acquire=True)
    assert held is not None
    assert held.task_count == 1
    assert await agent_manager.pop_if_idle(held.key, 0.0) is None

    plain = await agent_manager.get_agent("u1", "claude")
    assert plain is not None
    assert plain.task_count == 1

    await agent_manager.release(held.key)
    released = await agent_manager.get_agent("u1", "claude")
    assert released is not None
    assert released.task_count == 0


@pytest.mark.asyncio
async def test_pop_if_idle_respects_holds_and_staleness() -> None:
    agent_manager = AgentManager()

    async def creator(agent: AgentInfo) -> AgentInfo:
        agent.sandbox_id = "sandbox-1"
        return agent

    held = await agent_manager.get_or_create_agent(
        "u1", "jiuwenswarm", creator=creator, acquire=True
    )
    key = held.key

    # Held (task_count > 0): never reclaimed even when "stale".
    await asyncio.sleep(0.05)
    assert await agent_manager.pop_if_idle(key, 0.01) is None

    # Released but not idle long enough.
    await agent_manager.release(key)
    assert await agent_manager.pop_if_idle(key, 60.0) is None

    # Released and stale: reclaimed atomically with sandbox info preserved.
    await asyncio.sleep(0.05)
    reclaimed = await agent_manager.pop_if_idle(key, 0.01)
    assert reclaimed is not None
    assert reclaimed.info.status is AgentStatus.DELETED
    assert reclaimed.info.sandbox_id == "sandbox-1"
    assert await agent_manager.get_agent("u1", "jiuwenswarm") is None

    # Missing key after pop.
    assert await agent_manager.pop_if_idle(key, 0.01) is None


@pytest.mark.asyncio
async def test_pop_if_idle_respects_expected_generation() -> None:
    agent_manager = AgentManager()
    first = await agent_manager.get_or_create_agent("u1", "jiuwenswarm")
    assert await agent_manager.pop_agent(
        "u1", "jiuwenswarm", expected_agent_id=first.info.agent_id,
    ) is not None
    second = await agent_manager.get_or_create_agent("u1", "jiuwenswarm")
    await asyncio.sleep(0.05)

    assert await agent_manager.pop_if_idle(
        first.key, 0.01, expected_agent_id=first.info.agent_id,
    ) is None
    current = await agent_manager.get_agent("u1", "jiuwenswarm")
    assert current is not None
    assert current.info.agent_id == second.info.agent_id


@pytest.mark.asyncio
async def test_pop_if_idle_disabled_with_nonpositive_timeout() -> None:
    agent_manager = AgentManager()
    runtime = await agent_manager.get_or_create_agent("u1", "jiuwenswarm")
    await asyncio.sleep(0.05)

    assert await agent_manager.pop_if_idle(runtime.key, 0) is None
    assert await agent_manager.pop_if_idle(runtime.key, -1) is None
    assert await agent_manager.get_agent("u1", "jiuwenswarm") is not None
    assert await agent_manager.list_keys() == [runtime.key]


@pytest.mark.asyncio
async def test_session_scoped_key_creates_independent_agents() -> None:
    agent_manager = AgentManager(
        key_fields=["user_id", "agent_type", "session_id"]
    )
    create_calls = 0

    async def creator(agent: AgentInfo) -> AgentInfo:
        nonlocal create_calls
        create_calls += 1
        agent.sandbox_id = f"sandbox-{create_calls}"
        return agent

    first = await agent_manager.get_or_create_agent(
        "u1",
        "jiuwenswarm",
        key_values={"session_id": "sess-1"},
        creator=creator,
    )
    second = await agent_manager.get_or_create_agent(
        "u1",
        "jiuwenswarm",
        key_values={"session_id": "sess-2"},
        creator=creator,
    )
    reused = await agent_manager.get_or_create_agent(
        "u1",
        "jiuwenswarm",
        key_values={"session_id": "sess-1"},
        creator=creator,
    )

    assert create_calls == 2
    assert first.info.agent_id != second.info.agent_id
    assert reused.info.agent_id == first.info.agent_id
    assert first.key == ("u1", "jiuwenswarm", "sess-1")
    assert second.key == ("u1", "jiuwenswarm", "sess-2")

    await agent_manager.delete_agent(
        "u1", "jiuwenswarm", key_values={"session_id": "sess-1"}
    )
    assert await agent_manager.get_agent(
        "u1", "jiuwenswarm", key_values={"session_id": "sess-1"}
    ) is None
    assert await agent_manager.get_agent(
        "u1", "jiuwenswarm", key_values={"session_id": "sess-2"}
    ) is not None


async def _create_shared_alias(manager, session, sandbox="same"):
    async def creator(info):
        info.sandbox_id = sandbox
        return info

    return await manager.get_or_create_agent(
        "user", "test", key_values={"session_id": session}, creator=creator,
    )


def _shared_alias_manager():
    return AgentManager(key_fields=["user_id", "agent_type", "session_id"])


async def test_forced_pop_removes_aliases_but_not_other_sandboxes():
    manager = _shared_alias_manager()
    first = await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    unrelated = await _create_shared_alias(manager, "three", "different")

    assert await manager.is_current(first)
    assert await manager.pop_agent("user", "test", key_values={"session_id": "one"},
                                   expected_agent_id="wrong", include_shared_sandbox=True) is None
    assert await manager.is_current(sibling)
    popped = await manager.pop_agent("user", "test", key_values={"session_id": "one"},
                                     include_shared_sandbox=True)
    assert popped.info.agent_id == first.info.agent_id
    assert popped.is_deleted()
    assert not await manager.is_current(first)
    assert not await manager.is_current(sibling)
    assert await manager.is_current(unrelated)


@pytest.mark.parametrize("blocked", ["held", "recent", "failed"])
async def test_idle_pop_refuses_until_every_alias_is_idle(blocked):
    manager = _shared_alias_manager()
    first = await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    manager._runtimes[first.key].last_active_at = time.time() - 100
    other = manager._runtimes[sibling.key]
    other.last_active_at = time.time() - 100
    if blocked == "held":
        other.task_count = 1
    elif blocked == "recent":
        other.last_active_at = time.time()
    else:
        other.mark_failed(RuntimeError("failure"))
    assert await manager.pop_if_idle(first.key, 30, include_shared_sandbox=True) is None
    assert await manager.get_agent("user", "test", key_values={"session_id": "one"}) is not None
    assert await manager.get_agent("user", "test", key_values={"session_id": "two"}) is not None


async def test_idle_pop_removes_idle_aliases_and_default_pop_remains_scoped():
    manager = _shared_alias_manager()
    first = await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    for runtime in manager._runtimes.values():
        runtime.last_active_at = time.time() - 100
    assert (await manager.pop_if_idle(first.key, 30, include_shared_sandbox=True)).is_deleted()
    assert not await manager.is_current(sibling)

    first = await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    await manager.pop_agent("user", "test", key_values={"session_id": "one"})
    assert not await manager.is_current(first)
    assert await manager.is_current(sibling)


async def test_wait_for_creation_times_out_instead_of_blocking_forever():
    manager = AgentManager(creating_timeout_seconds=0.05)
    entered = asyncio.Event()
    never = asyncio.Event()

    async def creator(info):
        entered.set()
        await never.wait()
        return info

    creation = asyncio.create_task(
        manager.get_or_create_agent("u1", "jiuwenswarm", creator=creator)
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runtime = await manager.get_agent("u1", "jiuwenswarm")
        assert runtime is not None
        assert await manager.wait_for_creation(
            "u1", "jiuwenswarm", expected_agent_id=runtime.info.agent_id,
        ) is False
    finally:
        never.set()
        creation.cancel()
        await asyncio.gather(creation, return_exceptions=True)


async def test_idle_pop_refuses_while_shared_sandbox_is_creating():
    manager = _shared_alias_manager()
    ready = await _create_shared_alias(manager, "one")
    manager._runtimes[ready.key].last_active_at = time.time() - 100
    entered = asyncio.Event()
    release = asyncio.Event()

    async def creator(info):
        entered.set()
        await release.wait()
        info.sandbox_id = "same"
        return info

    creating = asyncio.create_task(
        manager.get_or_create_agent(
            "user", "test", key_values={"session_id": "two"}, creator=creator,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await manager.pop_if_idle(
            ready.key, 30, include_shared_sandbox=True,
        ) is None
    finally:
        release.set()
        await asyncio.gather(creating, return_exceptions=True)


async def test_recover_creating_agent_uses_cas_and_wakes_waiters():
    manager = AgentManager(creating_timeout_seconds=5.0)
    creator_started = asyncio.Event()
    release_creator = asyncio.Event()

    async def creator(info):
        creator_started.set()
        await release_creator.wait()
        info.sandbox_id = "physical-instance"
        info.metadata["physical_generation"] = "physical-generation"
        return info

    owner = asyncio.create_task(
        manager.get_or_create_agent("alice", "jiuwenswarm", creator=creator, acquire=True)
    )
    await creator_started.wait()
    creating = await manager.get_agent("alice", "jiuwenswarm")
    assert creating is not None
    logical_id = creating.info.agent_id
    waiter = asyncio.create_task(
        manager.get_or_create_agent("alice", "jiuwenswarm", creator=creator)
    )
    await asyncio.sleep(0)

    wrong_recovery = AgentInfo(
        user_id="alice", agent_type="jiuwenswarm", agent_id=logical_id,
        sandbox_id="wrong-instance",
    )
    assert await manager.recover_creating_agent(
        "alice", "jiuwenswarm", expected_agent_id="wrong-id", created=wrong_recovery,
    ) is None

    recovered_info = AgentInfo(
        user_id="alice", agent_type="jiuwenswarm", agent_id=logical_id,
        sandbox_id="physical-instance", metadata={"physical_generation": "physical-generation"},
    )
    recovered = await manager.recover_creating_agent(
        "alice", "jiuwenswarm", expected_agent_id=logical_id, created=recovered_info,
    )
    assert recovered is not None
    assert recovered.is_ready()
    assert recovered.info.agent_id == logical_id
    assert recovered.info.sandbox_id == "physical-instance"
    assert recovered.info.metadata["physical_generation"] == "physical-generation"

    waited = await asyncio.wait_for(waiter, 1)
    assert waited.info.agent_id == logical_id
    assert waited.info.sandbox_id == "physical-instance"

    release_creator.set()
    finished = await owner
    assert finished.info.agent_id == logical_id
    assert finished.info.sandbox_id == "physical-instance"
    assert finished.info.metadata["physical_generation"] == "physical-generation"

    assert await manager.recover_creating_agent(
        "alice", "jiuwenswarm", expected_agent_id=logical_id, created=recovered_info,
    ) is None


async def test_precreate_failure_after_recovery_keeps_the_ready_runtime():
    manager = AgentManager(creating_timeout_seconds=5.0)
    creator_started = asyncio.Event()
    fail_creator = asyncio.Event()

    async def creator(info):
        creator_started.set()
        await fail_creator.wait()
        # A superseded generation gives up instead of publishing a second instance.
        raise AgentRebuildPending("AGENT_REBUILD_PENDING: instance logical identity was recovered")

    owner = asyncio.create_task(manager.get_or_create_agent("alice", "jiuwenswarm", creator=creator))
    await creator_started.wait()
    creating = await manager.get_agent("alice", "jiuwenswarm")
    assert creating is not None
    assert await manager.recover_creating_agent(
        "alice", "jiuwenswarm", expected_agent_id=creating.info.agent_id,
        created=AgentInfo(
            user_id="alice", agent_type="jiuwenswarm", agent_id=creating.info.agent_id,
            sandbox_id="physical-instance",
        ),
    ) is not None

    fail_creator.set()
    with pytest.raises(AgentRebuildPending, match="logical identity was recovered"):
        await owner

    # Discarding the failed creator must not drop the recovered, already READY runtime.
    current = await manager.get_agent("alice", "jiuwenswarm")
    assert current is not None and current.is_ready()
    assert current.info.sandbox_id == "physical-instance"


async def test_physical_write_fence_blocks_idle_detach_without_holding_manager_lock():
    manager = AgentManager()
    runtime = await manager.get_or_create_agent('alice', 'jiuwenswarm')
    entered = asyncio.Event()
    done = asyncio.Event()

    async def write():
        entered.set()
        await done.wait()

    sending = asyncio.create_task(manager.send_if_current(runtime, write))
    try:
        await entered.wait()
        # Inspection for unrelated keys must not be serialized behind socket I/O.
        async with asyncio.timeout(1):
            assert await manager.get_agent('bob', 'jiuwenswarm') is None
        await asyncio.sleep(.01)
        assert await manager.pop_if_idle(runtime.key, .001) is None
    finally:
        done.set()
        await sending
    assert await manager.pop_if_idle(runtime.key, .001) is not None


async def test_physical_write_fence_blocks_explicit_detach_of_a_shared_sandbox():
    manager = _shared_alias_manager()
    first = await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    unrelated = await _create_shared_alias(manager, "three", "different")
    entered = asyncio.Event()
    done = asyncio.Event()

    async def write():
        entered.set()
        await done.wait()

    sending = asyncio.create_task(manager.send_if_current(sibling, write))
    try:
        await entered.wait()
        # "one" shares the sandbox with the fenced "two": a shared detach would strand the write.
        with pytest.raises(RuntimeError, match="fenced write"):
            await manager.pop_agent(
                "user", "test", key_values={"session_id": "one"}, include_shared_sandbox=True,
            )
        assert await manager.is_current(first)
        assert await manager.is_current(sibling)
        # A detach scoped to "one" does not touch the fenced alias and is unaffected.
        assert await manager.pop_agent("user", "test", key_values={"session_id": "one"}) is not None
        assert await manager.is_current(sibling)
    finally:
        done.set()
        await sending
    assert await manager.is_current(unrelated)


async def test_idle_detach_refuses_while_a_shared_alias_write_is_fenced():
    manager = _shared_alias_manager()
    await _create_shared_alias(manager, "one")
    sibling = await _create_shared_alias(manager, "two")
    entered = asyncio.Event()
    done = asyncio.Event()

    async def write():
        entered.set()
        await done.wait()

    sending = asyncio.create_task(manager.send_if_current(sibling, write))
    try:
        await entered.wait()
        await asyncio.sleep(0.01)
        assert await manager.pop_if_idle(sibling.key, 0.001, include_shared_sandbox=True) is None
        assert await manager.is_current(sibling)
    finally:
        done.set()
        await sending
    assert await manager.pop_if_idle(sibling.key, 0.001, include_shared_sandbox=True) is not None


async def test_stale_snapshot_cannot_write_and_a_failed_write_releases_the_fence():
    manager = AgentManager()
    stale = await manager.get_or_create_agent('alice', 'jiuwenswarm')
    assert await manager.pop_agent('alice', 'jiuwenswarm') is not None

    async def stale_write():
        raise AssertionError("a stale generation must never write")

    with pytest.raises(RuntimeError, match="stale instance generation"):
        await manager.send_if_current(stale, stale_write)

    replaced = await manager.get_or_create_agent('alice', 'jiuwenswarm')

    async def failing_write():
        raise ConnectionError("socket closed")

    with pytest.raises(ConnectionError):
        await manager.send_if_current(replaced, failing_write)
    # A failed write must not pin its generation forever.
    assert await manager.pop_agent('alice', 'jiuwenswarm') is not None
