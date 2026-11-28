# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Static provider ownership and purity, independent of an asyncio runtime."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import socket

import pytest

from agent_gateway.application.worker_route_provider import StaticRouteProvider
from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


@pytest.fixture
def directory() -> StaticRouteDirectory:
    policy = LocalIdentityPolicy(domain='provider-test')
    manifest = WorkerManifest((0, 1), config_version=53)
    targets = []
    for index, user in enumerate(('alice', 'bob', '用户甲')):
        identity = IngressIdentity(policy.contract_id, 'local_compat', policy.domain, 'named', user)
        targets.append(SouthboundTarget(
            target_id=f'target-{index}', physical_id=f'physical-{index}', generation=7 + index,
            user_id=user, agent_type='jiuwenswarm',
            owner_slot=FixedUserHash(manifest, policy).select(identity),
            ws_url=f'ws://127.0.0.1:{18000 + index}/agent',
        ))
    return StaticRouteDirectory(11, policy, manifest, tuple(targets))


def test_owned_targets_and_resolved_objects_preserve_directory_identity(directory: StaticRouteDirectory) -> None:
    before = directory.to_json()
    for slot in directory.manifest.slots:
        provider = StaticRouteProvider(directory, slot_id=slot)
        assert provider.identity_policy is directory.identity_policy
        assert type(provider.owned_targets) is tuple
        expected = tuple(target for target in directory.targets if target.owner_slot == slot)
        assert len(provider.owned_targets) == len(expected)
        for actual, target in zip(provider.owned_targets, expected):
            assert actual is target
            assert provider.resolve_bound_user(target.user_id) is target
            assert provider.resolve_bound_user(target.user_id, agent_type='jiuwenswarm') is target
    assert directory.to_json() == before


def test_other_owner_is_rejected_with_original_fixed_error(directory: StaticRouteDirectory) -> None:
    for target in directory.targets:
        provider = StaticRouteProvider(directory, slot_id=1 - target.owner_slot)
        with pytest.raises(ValueError, match='^request has no owned static southbound target$'):
            provider.resolve_bound_user(target.user_id)


def test_empty_slot_has_no_default_target_and_keeps_full_manifest(directory: StaticRouteDirectory) -> None:
    target = directory.targets[0]
    single = replace(directory, targets=(target,))
    provider = StaticRouteProvider(single, slot_id=1 - target.owner_slot)
    assert provider.owned_targets == ()
    assert provider.identity_policy is single.identity_policy
    with pytest.raises(ValueError, match='^request has no owned static southbound target$'):
        provider.resolve_bound_user(target.user_id)
    assert single.manifest is directory.manifest
    assert single.manifest.config_version == 53


@pytest.mark.parametrize('user', [None, '', 'unknown', 'local-default', ' alice', 'alice ', 7, True, [], {}])
def test_invalid_or_unknown_users_have_no_fallback(directory: StaticRouteDirectory, user: object) -> None:
    provider = StaticRouteProvider(directory, slot_id=directory.targets[0].owner_slot)
    with pytest.raises(ValueError, match='^request has no owned static southbound target$'):
        provider.resolve_bound_user(user)


@pytest.mark.parametrize('agent_type', ['', 'other', True, 1, [], {}])
def test_unsupported_agent_types_keep_original_fixed_error(directory: StaticRouteDirectory, agent_type: object) -> None:
    target = directory.targets[0]
    provider = StaticRouteProvider(directory, slot_id=target.owner_slot)
    with pytest.raises(ValueError, match='^request has no owned static southbound target$'):
        provider.resolve_bound_user(target.user_id, agent_type=agent_type)


@pytest.mark.parametrize('slot', [None, True, '0', -1, 2, 0.0])
def test_slot_is_validated_before_runtime_side_effects(directory: StaticRouteDirectory, slot: object) -> None:
    with pytest.raises(ValueError, match='^slot is not in the static route manifest$'):
        StaticRouteProvider(directory, slot_id=slot)


@pytest.mark.parametrize('value', [None, {}, object()])
def test_directory_requires_original_exact_type(value: object) -> None:
    with pytest.raises(ValueError, match='^routed client requires an explicit static directory$'):
        StaticRouteProvider(value, slot_id=0)


def test_directory_subclasses_do_not_open_a_provider_injection_path(directory: StaticRouteDirectory) -> None:
    class DerivedDirectory(StaticRouteDirectory):
        pass

    derived = DerivedDirectory(
        directory.revision, directory.identity_policy, directory.manifest, directory.targets,
    )
    with pytest.raises(ValueError, match='^routed client requires an explicit static directory$'):
        StaticRouteProvider(derived, slot_id=0)


def test_policy_and_targets_are_read_only(directory: StaticRouteDirectory) -> None:
    provider = StaticRouteProvider(directory, slot_id=0)
    with pytest.raises(AttributeError):
        provider.identity_policy = LocalIdentityPolicy()
    with pytest.raises(AttributeError):
        provider.owned_targets = ()


def test_construction_and_resolution_need_no_socket_or_running_loop(
    directory: StaticRouteDirectory, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_io(*args, **kwargs) -> None:
        pytest.fail('static provider attempted network or background task creation')

    monkeypatch.setattr(socket, 'socket', unexpected_io)
    monkeypatch.setattr(socket, 'create_connection', unexpected_io)
    monkeypatch.setattr(asyncio, 'create_task', unexpected_io)
    with pytest.raises(RuntimeError, match='no running event loop'):
        asyncio.get_running_loop()
    target = directory.targets[0]
    provider = StaticRouteProvider(directory, slot_id=target.owner_slot)
    assert provider.resolve_bound_user(target.user_id) is target
