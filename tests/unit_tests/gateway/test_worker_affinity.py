# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Stable ownership of normalized users, independent of process and health state."""

from dataclasses import FrozenInstanceError
import json
import os
import subprocess
import sys
from urllib.parse import quote

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity


def user_identity(user, *, domain='local', entry='web'):
    query = b'' if user is None else ('user_id=' + quote(user)).encode('ascii')
    return resolve_local_identity(query, (), entry=entry, policy=LocalIdentityPolicy(domain=domain))


@pytest.mark.parametrize(('user', 'expected'), [
    ('alice', 3), ('bob', 2), (None, 0), ('local-default', 1), ('张三', 1),
])
def test_fixed_vectors_select_stable_slots(user, expected):
    # Independently calculated with openssl dgst -sha256, not this selector.
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    assert selector.select(user_identity(user)) == expected


def test_slot_order_and_config_version_do_not_move_a_user():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    selectors = [
        FixedUserHash(WorkerManifest(slots, config_version=version), LocalIdentityPolicy())
        for slots, version in [((0, 1, 2, 3), 1), ((3, 1, 0, 2), 2), ((2, 3, 1, 0), 100)]
    ]
    assert [selector.select(user_identity('alice')) for selector in selectors] == [3, 3, 3]
    assert selectors[1].manifest.slots == (0, 1, 2, 3)
    with pytest.raises(FrozenInstanceError):
        selectors[0].manifest.slots = (0,)
    with pytest.raises(FrozenInstanceError):
        selectors[0].identity_policy = LocalIdentityPolicy(domain='other')


@pytest.mark.parametrize('slots', [(), [], (0, 0), (-1,), (True,), ('0',), (0, 1.0)])
def test_invalid_slot_lists_cannot_silently_change_ownership(slots):
    from agent_gateway.common.worker_affinity import WorkerManifest

    with pytest.raises(ValueError):
        WorkerManifest(slots)


@pytest.mark.parametrize('version', [0, -1, True, '1', 1.0])
def test_invalid_configuration_versions_are_rejected(version):
    from agent_gateway.common.worker_affinity import WorkerManifest

    with pytest.raises(ValueError):
        WorkerManifest((0,), config_version=version)


def test_unknown_algorithm_and_identity_domain_fail_before_selection():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    with pytest.raises(ValueError):
        WorkerManifest((0,), algorithm='python-hash')
    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    with pytest.raises(ValueError, match='identity policy'):
        selector.select(user_identity('alice', domain='other'))
    with pytest.raises(ValueError):
        selector.select(b'alice')
    with pytest.raises(ValueError):
        FixedUserHash((0,), LocalIdentityPolicy())
    with pytest.raises(ValueError):
        FixedUserHash(WorkerManifest((0,)), None)


def test_same_business_user_across_protocols_and_credentials_keeps_one_slot():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    identities = [user_identity('alice', entry=entry) for entry in ('web', 'http', 'tui', 'acp')]
    for token in ('old-token', 'new-token'):
        identities.append(resolve_local_identity(
            f'user_id=alice&token={token}&auth_session={token}&session_id={token}'.encode(),
            (), entry='http', policy=LocalIdentityPolicy(),
        ))
    assert [selector.select(identity) for identity in identities] == [3] * 6


def test_sparse_slots_select_actual_ids_instead_of_array_offsets():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    selector = FixedUserHash(WorkerManifest((3, 9)), LocalIdentityPolicy())
    selected = {selector.select(user_identity(f'user-{index}')) for index in range(100)}
    assert selected == {3, 9}


def test_single_slot_accepts_all_users_without_creating_new_business_identity():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    selector = FixedUserHash(WorkerManifest((0,)), LocalIdentityPolicy())
    anonymous = user_identity(None)
    assert [selector.select(user_identity(user)) for user in ('alice', 'bob', '张三', None)] == [0] * 4
    assert anonymous.business_user_id is None


def test_adding_slot_only_moves_users_to_that_slot_removing_it_preserves_others():
    from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

    before = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    added = FixedUserHash(WorkerManifest((0, 1, 2, 3, 4), config_version=2), LocalIdentityPolicy())
    removed = FixedUserHash(WorkerManifest((0, 1, 3), config_version=3), LocalIdentityPolicy())
    counts = [0, 0, 0, 0]
    moved = 0
    for index in range(4000):
        identity = user_identity(f'user-{index}')
        old_slot = before.select(identity)
        counts[old_slot] += 1
        new_slot = added.select(identity)
        if new_slot != old_slot:
            moved += 1
            assert new_slot == 4
        if old_slot != 2:
            assert removed.select(identity) == old_slot
    assert 600 < moved < 1000
    assert all(800 < count < 1200 for count in counts)


def test_fresh_interpreters_with_different_hash_seeds_agree():
    script = '''
import json
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
s = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
print(json.dumps([s.select(resolve_local_identity(q, (), entry='web', policy=LocalIdentityPolicy()))
                 for q in (b'user_id=alice', b'user_id=bob', b'')]))
'''
    for seed in ('1', '999'):
        result = subprocess.run(
            [sys.executable, '-c', script], env={**os.environ, 'PYTHONHASHSEED': seed},
            text=True, capture_output=True, check=True, timeout=10,
        )
        assert json.loads(result.stdout) == [3, 2, 0]
