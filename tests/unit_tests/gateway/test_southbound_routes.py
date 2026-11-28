# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Static local route contracts, independent of networking and runtime assembly."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import json

import pytest

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


def identity(user: str = 'alice', *, domain: str = 'local') -> IngressIdentity:
    return IngressIdentity('local-v1', 'local_compat', domain, 'named', user)


def target(user: str = 'alice', *, index: int = 0) -> SouthboundTarget:
    policy = LocalIdentityPolicy()
    slot = FixedUserHash(WorkerManifest((0, 1)), policy).select(identity(user))
    return SouthboundTarget(
        target_id=f'target-{index}', physical_id=f'physical-{index}', generation=1,
        user_id=user, agent_type='jiuwenswarm', owner_slot=slot,
        ws_url=f'ws://127.0.0.1:{18000 + index}/agent',
    )


def directory(*targets: SouthboundTarget) -> StaticRouteDirectory:
    return StaticRouteDirectory(
        revision=1, identity_policy=LocalIdentityPolicy(),
        manifest=WorkerManifest((0, 1)), targets=targets or (target(),),
    )


def test_roundtrip_preserves_policy_manifest_and_exact_owner() -> None:
    route = directory(target(), target('用户&乙', index=1))
    restored = StaticRouteDirectory.from_json(route.to_json())

    assert restored == route
    for item in route.targets:
        assert restored.resolve(identity(item.user_id), slot_id=item.owner_slot) == item
        assert item in restored.targets_for_slot(item.owner_slot)
    assert restored.manifest == WorkerManifest((0, 1))


def test_directory_and_targets_are_frozen() -> None:
    route = directory()
    with pytest.raises(FrozenInstanceError):
        route.revision = 2
    with pytest.raises(FrozenInstanceError):
        route.targets[0].owner_slot = 1


@pytest.mark.parametrize('user', ['', ' alice', 'alice ', 'a\n', '\ud800', '字' * 86])
def test_target_requires_existing_canonical_named_identity(user: str) -> None:
    with pytest.raises(ValueError):
        replace(target(), user_id=user)


@pytest.mark.parametrize(
    'changes',
    [
        {'target_id': ''}, {'target_id': 'x' * 257}, {'target_id': 'bad\n'},
        {'physical_id': ''}, {'physical_id': 5}, {'generation': True},
        {'generation': 0}, {'generation': 2**63}, {'owner_slot': True},
        {'owner_slot': -1}, {'agent_type': 'other'}, {'agent_type': None},
    ],
)
def test_target_rejects_invalid_fields(changes: dict) -> None:
    with pytest.raises(ValueError):
        replace(target(), **changes)


@pytest.mark.parametrize(
    'url',
    [
        'wss://127.0.0.1:18000', 'ws://localhost:18000', 'ws://example.com:18000',
        'ws://127.0.0.1', 'ws://127.0.0.1:0', 'ws://127.0.0.1:65536',
        'ws://127.0.0.1:bad', 'ws://[::1', 'ws://user:secret@127.0.0.1:18000',
        'ws://127.0.0.1:18000?token=secret', 'ws://127.0.0.1:18000#secret',
        'ws://127.0.0.1:18000/path with spaces', 'ws://127.0.0.1:18000/\n',
        'ws://127.0.0.1:18000/' + 'x' * 2048, None,
    ],
)
def test_target_requires_bounded_nonsecret_explicit_loopback_ws(url: str) -> None:
    with pytest.raises(ValueError) as caught:
        replace(target(), ws_url=url)
    assert 'user:secret@' not in str(caught.value)
    assert 'token=secret' not in str(caught.value)


def test_ipv6_loopback_and_existing_url_path_are_supported() -> None:
    item = replace(target(), ws_url='ws://[::1]:18000/agent')
    assert directory(item).targets == (item,)


@pytest.mark.parametrize('field', ['target_id', 'physical_id', 'user_id', 'ws_url'])
def test_directory_rejects_duplicate_ownership(field: str) -> None:
    first = target()
    second = replace(target('bob', index=1), **{field: getattr(first, field)})
    if field == 'user_id':
        second = replace(second, owner_slot=first.owner_slot)
    with pytest.raises(ValueError):
        directory(first, second)


def test_directory_rejects_obvious_url_alias() -> None:
    first = replace(target(), ws_url='ws://127.0.0.1:18000')
    second = replace(target('bob', index=1), ws_url='ws://127.0.0.1:18000/')
    with pytest.raises(ValueError):
        directory(first, second)


def test_directory_recomputes_owner_from_complete_manifest() -> None:
    item = target()
    with pytest.raises(ValueError):
        directory(replace(item, owner_slot=1 - item.owner_slot))
    with pytest.raises(ValueError):
        directory(replace(item, owner_slot=7))


@pytest.mark.parametrize(
    'changes',
    [
        {'schema_version': True}, {'schema_version': 2}, {'revision': True},
        {'revision': 0}, {'revision': 2**63}, {'identity_policy': None},
        {'manifest': (0, 1)}, {'targets': []}, {'targets': ()},
        {'targets': (object(),)},
    ],
)
def test_directory_rejects_malformed_snapshot(changes: dict) -> None:
    with pytest.raises(ValueError):
        replace(directory(), **changes)


def test_snapshot_revision_does_not_change_hrw_owner() -> None:
    route = directory()
    changed = replace(route, revision=2, manifest=replace(route.manifest, config_version=2))
    assert changed.resolve(identity(), slot_id=target().owner_slot) == target()


def test_resolve_rejects_other_slot_unknown_user_or_wrong_identity_policy() -> None:
    route = directory()
    for user, slot, agent_type in [
        (identity(), 1 - target().owner_slot, 'jiuwenswarm'),
        (identity('unknown'), target().owner_slot, 'jiuwenswarm'),
        (identity(domain='other'), target().owner_slot, 'jiuwenswarm'),
        (identity(), target().owner_slot, 'other'),
        (IngressIdentity('local-v1', 'local_compat', 'local', 'anonymous', None), 0, 'jiuwenswarm'),
    ]:
        with pytest.raises(ValueError):
            route.resolve(user, slot_id=slot, agent_type=agent_type)
    for bad_slot in (True, -1, 7):
        with pytest.raises(ValueError):
            route.targets_for_slot(bad_slot)


def test_json_rejects_unknown_fields_at_every_level() -> None:
    for section in (None, 'identity_policy', 'manifest', 'target'):
        value = json.loads(directory().to_json())
        container = value if section is None else value['targets'][0] if section == 'target' else value[section]
        container['token'] = 'never-log-this-secret'
        with pytest.raises(ValueError) as caught:
            StaticRouteDirectory.from_json(json.dumps(value))
        assert 'never-log-this-secret' not in str(caught.value)


def test_json_rejects_duplicate_keys_including_nested_objects() -> None:
    encoded = directory().to_json()
    for key, value in [('revision', '2'), ('domain', '"other"'), ('user_id', '"bob"'), ('config_version', '2')]:
        duplicate = encoded.replace(f'"{key}":', f'"{key}":{value},"{key}":', 1)
        with pytest.raises(ValueError):
            StaticRouteDirectory.from_json(duplicate)


@pytest.mark.parametrize('value', ['', 'null', '[]', 'true', '{', 'NaN', b'{}', '[' * 1100])
def test_json_rejects_nonobject_or_malformed_input(value: str) -> None:
    with pytest.raises(ValueError):
        StaticRouteDirectory.from_json(value)


def test_json_does_not_coerce_nested_types() -> None:
    for section, key, bad in [
        (None, 'revision', '1'), ('identity_policy', 'domain', 3),
        ('manifest', 'slots', [True, 1]), ('manifest', 'config_version', True),
        ('target', 'generation', '1'), ('target', 'owner_slot', True),
    ]:
        value = json.loads(directory().to_json())
        container = value if section is None else value['targets'][0] if section == 'target' else value[section]
        container[key] = bad
        with pytest.raises(ValueError):
            StaticRouteDirectory.from_json(json.dumps(value))


def test_json_rejects_missing_fields() -> None:
    for section, key in [(None, 'schema_version'), ('identity_policy', 'domain'), ('manifest', 'slots'), ('target', 'physical_id')]:
        value = json.loads(directory().to_json())
        container = value if section is None else value['targets'][0] if section == 'target' else value[section]
        del container[key]
        with pytest.raises(ValueError):
            StaticRouteDirectory.from_json(json.dumps(value))


def test_target_count_and_utf8_serialization_are_bounded() -> None:
    route = directory(*(target(f'user-{index}', index=index) for index in range(32)))
    assert len(StaticRouteDirectory.from_json(route.to_json()).targets) == 32
    with pytest.raises(ValueError):
        directory(*(target(f'user-{index}', index=index) for index in range(33)))
    # Every user is valid under P7; the entire JSON exceeds 16 KiB in UTF-8.
    items = tuple(
        replace(target('字' * 70 + str(index), index=index), physical_id='p' * 200 + str(index))
        for index in range(32)
    )
    with pytest.raises(ValueError):
        directory(*items)
    for raw in (route.to_json() + ' ' * 16384, '\ud800', '{"x":"' + '字' * 6000 + '"}'):
        with pytest.raises(ValueError):
            StaticRouteDirectory.from_json(raw)


def test_raw_json_limit_counts_utf8_bytes_and_accepts_exact_boundary() -> None:
    route = directory(target('用户&乙'))
    encoded = route.to_json()
    assert len(encoded.encode('utf-8')) > len(encoded)
    exact = encoded + ' ' * (16384 - len(encoded.encode('utf-8')))
    assert len(exact.encode('utf-8')) == 16384
    assert StaticRouteDirectory.from_json(exact) == route
    with pytest.raises(ValueError):
        StaticRouteDirectory.from_json(exact + ' ')


def test_unused_worker_slot_does_not_drop_out_of_the_manifest() -> None:
    route = directory()
    empty_slot = 1 - target().owner_slot
    assert route.targets_for_slot(empty_slot) == ()
    assert StaticRouteDirectory.from_json(route.to_json()).manifest.slots == (0, 1)


@pytest.mark.parametrize(
    'manifest',
    [WorkerManifest((0, 1), config_version=2**63), WorkerManifest((0, 1, 2**63))],
)
def test_directory_bounds_manifest_integers_without_changing_shared_hrw(manifest: WorkerManifest) -> None:
    # The legacy manifest permits these; the new serialized fixture is stricter.
    with pytest.raises(ValueError):
        replace(directory(), manifest=manifest)
    value = json.loads(directory().to_json())
    value['manifest'] = {
        'slots': list(manifest.slots), 'config_version': manifest.config_version,
        'algorithm': manifest.algorithm,
    }
    with pytest.raises(ValueError):
        StaticRouteDirectory.from_json(json.dumps(value))
