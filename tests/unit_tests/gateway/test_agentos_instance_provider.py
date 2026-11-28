# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Owner-scoped acquisition uses the real AgentManager, not a second registry."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.extensions.agentos.agentos_router.models import AgentStatus
from agent_gateway.southbound.orchestration.agent_manager import (
    AgentCreateFailed, AgentManager, AgentPreCreateError,
)
from agent_gateway.southbound.orchestration.instance_provider import AgentOSInstanceProvider


class Control:
    """Stands in for the managed control plane's data-endpoint surface."""

    def __init__(self, frontend_endpoint: str = 'http://127.0.0.1:18888', ws_url: str | None = None) -> None:
        self.frontend_endpoint = frontend_endpoint
        self.override = ws_url
        self.url_calls: list[tuple[str, int]] = []

    def ws_url(self, instance_id: str, port: int) -> str:
        self.url_calls.append((instance_id, port))
        if self.override is not None:
            return self.override
        scheme = 'wss' if self.frontend_endpoint.startswith('https://') else 'ws'
        netloc = self.frontend_endpoint.split('://', 1)[1]
        return f'{scheme}://{netloc}/{instance_id}/{port}'


class Creator:
    def __init__(self) -> None:
        self.calls = []
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()
        self.gate.set()
        self.error = None

    async def create(self, info):
        self.calls.append(info.copy())
        self.entered.set()
        await self.gate.wait()
        if self.error is not None:
            raise self.error
        info.status = AgentStatus.READY
        info.sandbox_id = f'instance-{info.agent_id}'
        info.metadata.update(physical_generation=info.agent_id, agent_port=18092)
        return info


def provider(creator: Creator, manager: AgentManager | None = None, control: Control | None = None):
    return AgentOSInstanceProvider(
        manager or AgentManager(), creator=creator.create, control=control or Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37), slot_id=0,
        admitted_users=('alice', 'bob'),
    )


async def test_parallel_acquisition_creates_one_instance_and_releases_exact_holds() -> None:
    creator = Creator()
    owner = provider(creator)
    route = owner.validate_owner('alice')
    leases = await asyncio.gather(*(owner.acquire(route) for _ in range(12)))
    assert len(creator.calls) == 1
    assert len({lease.target.physical_id for lease in leases}) == 1
    assert all([await owner.is_current(lease) for lease in leases])
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 12
    await asyncio.gather(*(owner.release(lease, outcome='finished') for lease in leases))
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
    assert owner.active_lease_count == 0
    await owner.close()


async def test_existing_only_does_not_create_and_owner_validation_is_pure() -> None:
    creator = Creator()
    control = Control()
    owner = provider(creator, control=control)
    for user in (None, '', 'unknown', ' alice'):
        with pytest.raises(ValueError):
            owner.validate_owner(user)
    route = owner.validate_owner('alice')
    with pytest.raises(ValueError):
        owner.validate_owner('alice', agent_type='opencode')
    assert await owner.acquire(route, existing_only=True) is None
    assert not creator.calls
    with pytest.raises(ValueError):
        await owner.acquire(replace(route))
    assert not creator.calls
    assert control.url_calls == []
    await owner.close()


async def test_unknown_execution_retains_hold_until_explicit_reconciliation() -> None:
    owner = provider(Creator())
    lease = await owner.acquire(owner.validate_owner('alice'))
    await owner.release(lease, outcome='unknown')
    assert owner.uncertain_lease_count == 1
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 1
    await owner.release(lease, outcome='finished')
    await owner.release(lease, outcome='finished')
    assert owner.active_lease_count == 0
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
    await owner.close()


async def test_old_release_and_forged_lease_do_not_touch_replacement() -> None:
    owner = provider(Creator())
    route = owner.validate_owner('alice')
    first = await owner.acquire(route)
    await owner.manager.pop_agent('alice', 'jiuwenswarm', expected_agent_id=first.agent_id)
    second = await owner.acquire(route)
    assert first.target.generation != second.target.generation
    assert not await owner.is_current(first)
    with pytest.raises(ValueError):
        await owner.release(replace(second), outcome='finished')
    await owner.release(first, outcome='finished')
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 1
    await owner.release(second, outcome='finished')
    await owner.close()


async def test_precreate_failure_can_retry_but_create_failure_cannot_duplicate() -> None:
    creator = Creator()
    owner = provider(creator)
    creator.error = AgentPreCreateError('before POST')
    with pytest.raises(AgentPreCreateError):
        await owner.acquire(owner.validate_owner('alice'))
    creator.error = None
    lease = await owner.acquire(owner.validate_owner('alice'))
    await owner.release(lease, outcome='not_sent')
    creator.error = RuntimeError('uncertain transport')
    with pytest.raises(RuntimeError):
        await owner.acquire(owner.validate_owner('bob'))
    with pytest.raises(AgentCreateFailed):
        await owner.acquire(owner.validate_owner('bob'))
    assert len(creator.calls) == 3
    await owner.close()


async def test_cancelled_acquisition_and_close_reap_owned_tasks_without_delete() -> None:
    creator = Creator()
    creator.gate.clear()
    owner = provider(creator)
    task = asyncio.create_task(owner.acquire(owner.validate_owner('alice')))
    await creator.entered.wait()
    await owner.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert owner.pending_acquisition_count == 0
    assert not owner.active_lease_count
    with pytest.raises(RuntimeError):
        await owner.acquire(owner.validate_owner('alice'))


async def test_unproven_physical_generation_is_rejected_and_hold_is_released() -> None:
    creator = Creator()

    async def invalid(info):
        result = await creator.create(info)
        result.metadata.pop('physical_generation')
        return result

    owner = AgentOSInstanceProvider(
        AgentManager(), creator=invalid, control=Control('https://manager.example'),
        namespace='ns', identity_policy=LocalIdentityPolicy(),
        manifest=WorkerManifest((0,)), slot_id=0, admitted_users=('alice',),
    )
    with pytest.raises(ValueError, match='physical'):
        await owner.acquire(owner.validate_owner('alice'))
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
    await owner.close()


async def test_physical_generation_is_independent_of_logical_agent_id() -> None:
    creator = Creator()

    async def create(info):
        result = await creator.create(info)
        result.metadata['physical_generation'] = 'physical-generation-1'
        return result

    owner = AgentOSInstanceProvider(
        AgentManager(), creator=create, control=Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37), slot_id=0,
        admitted_users=('alice', 'bob'),
    )
    lease = await owner.acquire(owner.validate_owner('alice'))

    assert lease.agent_id != 'physical-generation-1'
    assert lease.target.generation == 'physical-generation-1'
    assert lease.target.physical_id == f'test:instance-{lease.agent_id}'
    await owner.release(lease, outcome='finished')
    await owner.close()


async def test_generation_cache_cannot_cross_users_and_capacity_does_not_write_state() -> None:
    class Creator:
        def __init__(self) -> None:
            self.calls = 0

        async def create(self, info):
            self.calls += 1
            info.status = AgentStatus.READY
            info.sandbox_id = f'instance-{self.calls}'
            info.metadata.update(physical_generation='same-generation', agent_port=18092)
            return info

    creator = Creator()
    owner = AgentOSInstanceProvider(
        AgentManager(), creator=creator.create, control=Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37), slot_id=0,
        admitted_users=('alice', 'bob'),
    )
    alice = await owner.acquire(owner.validate_owner('alice'))
    assert alice.target.generation == 'same-generation'
    with pytest.raises(ValueError, match='already owned'):
        await owner.acquire(owner.validate_owner('bob'))
    assert owner._physical_owners == {'test:instance-1': (alice.route.target_id, 'same-generation')}
    await owner.release(alice, outcome='finished')
    await owner.close()


async def test_generation_capacity_rejects_before_physical_owner_write() -> None:
    class Creator:
        async def create(self, info):
            info.status = AgentStatus.READY
            info.sandbox_id = 'instance-capacity'
            info.metadata.update(physical_generation='generation-capacity', agent_port=18092)
            return info

    owner = AgentOSInstanceProvider(
        AgentManager(), creator=Creator().create, control=Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37), slot_id=0,
        admitted_users=('alice',),
    )
    for number in range(4096):
        owner._targets[f'generation-{number}'] = object()
    with pytest.raises(RuntimeError, match='generation capacity'):
        await owner.acquire(owner.validate_owner('alice'))
    assert 'test:instance-capacity' not in owner._physical_owners
    await owner.close()


@pytest.mark.parametrize('changes', [
    {'slot_id': True}, {'slot_id': 9}, {'admitted_users': ()},
    {'admitted_users': ('alice', 'alice')}, {'admitted_users': (' alice',)},
    {'control': None}, {'control': object()},
    {'control': Control('http://user:pass@127.0.0.1')},
    {'control': Control('http://127.0.0.1?token=secret')},
    {'namespace': ''}, {'manager': AgentManager(key_fields=('user_id', 'agent_type', 'session_id'))},
])
def test_constructor_rejects_unbounded_or_unimplemented_scope(changes) -> None:
    values = dict(
        manager=AgentManager(), creator=Creator().create, control=Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest((0,)),
        slot_id=0, admitted_users=('alice',),
    )
    values.update(changes)
    with pytest.raises(ValueError):
        AgentOSInstanceProvider(**values)


async def test_cancellation_after_manager_acquires_hold_releases_before_return(monkeypatch) -> None:
    owner = provider(Creator())
    entered = asyncio.Event()

    async def delayed_current(runtime):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(owner.manager, 'is_current', delayed_current)
    acquiring = asyncio.create_task(owner.acquire(owner.validate_owner('alice')))
    await entered.wait()
    acquiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquiring
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
    assert owner.active_lease_count == 0
    await owner.close()


async def test_close_waits_for_cancellation_release_cleanup(monkeypatch) -> None:
    async def unproven_creator(info):
        info.status = AgentStatus.READY
        info.sandbox_id = 'instance-unproven'
        info.metadata['agent_port'] = 18092
        return info

    owner = AgentOSInstanceProvider(
        AgentManager(), creator=unproven_creator, control=Control(),
        namespace='test', identity_policy=LocalIdentityPolicy(domain='instances'),
        manifest=WorkerManifest((0,), config_version=37), slot_id=0,
        admitted_users=('alice',),
    )
    release_entered = asyncio.Event()
    allow_release = asyncio.Event()
    original_release = owner.manager.release

    async def delayed_release(key, *, expected_agent_id=None):
        release_entered.set()
        await allow_release.wait()
        await original_release(key, expected_agent_id=expected_agent_id)

    monkeypatch.setattr(owner.manager, 'release', delayed_release)
    acquiring = asyncio.create_task(owner.acquire(owner.validate_owner('alice')))
    await release_entered.wait()
    closing = asyncio.create_task(owner.close())
    await asyncio.sleep(0)
    allow_release.set()
    await closing
    with pytest.raises(asyncio.CancelledError):
        await acquiring

    runtime = await owner.manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.task_count == 0


async def test_is_current_rechecks_closed_after_await(monkeypatch) -> None:
    owner = provider(Creator())
    lease = await owner.acquire(owner.validate_owner('alice'))
    current_entered = asyncio.Event()
    allow_current = asyncio.Event()
    original_current = owner.manager.is_current

    async def delayed_current(runtime):
        current_entered.set()
        await allow_current.wait()
        return await original_current(runtime)

    monkeypatch.setattr(owner.manager, 'is_current', delayed_current)
    checking = asyncio.create_task(owner.is_current(lease))
    await current_entered.wait()
    closing = asyncio.create_task(owner.close())
    await asyncio.sleep(0)
    allow_current.set()
    assert await checking is False
    await closing


async def test_close_preserves_unknown_remote_execution_hold() -> None:
    owner = provider(Creator())
    lease = await owner.acquire(owner.validate_owner('alice'))
    await owner.release(lease, outcome='unknown')
    await owner.close()
    assert owner.uncertain_lease_count == 1
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 1


async def test_target_endpoint_is_derived_by_the_control_for_the_verified_instance() -> None:
    creator = Creator()
    control = Control('https://adx.example')
    owner = provider(creator, control=control)
    lease = await owner.acquire(owner.validate_owner('alice'))

    assert lease.target.ws_url == f'wss://adx.example/instance-{lease.agent_id}/18092'
    assert control.url_calls == [(f'instance-{lease.agent_id}', 18092)]
    assert 'wss://' not in repr(lease.target)
    # One endpoint per physical generation; later leases reuse it without asking again.
    again = await owner.acquire(owner.validate_owner('alice'))
    assert again.target is lease.target
    assert len(control.url_calls) == 1
    await owner.release(lease, outcome='finished')
    await owner.release(again, outcome='finished')
    await owner.close()


@pytest.mark.parametrize('endpoint', [
    'ws://elsewhere.example/instance-1/18092',
    'wss://127.0.0.1:18888/instance-1/18092',
    'http://127.0.0.1:18888/instance-1/18092',
])
async def test_endpoint_outside_the_admitted_frontend_is_rejected_and_hold_released(endpoint) -> None:
    owner = provider(Creator(), control=Control(ws_url=endpoint))
    with pytest.raises(ValueError, match='admitted frontend'):
        await owner.acquire(owner.validate_owner('alice'))
    assert (await owner.manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
    assert owner.active_lease_count == 0
    await owner.close()
