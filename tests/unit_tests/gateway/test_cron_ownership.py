"""Cron ownership must agree with independently fixed ingress vectors."""

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest


def ownership():
    from agent_gateway.cron.ownership import CronOwnership
    return CronOwnership(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())


@pytest.mark.parametrize(('user', 'slot'), [('alice', 3), ('bob', 2), ('local-default', 1), ('张三', 1)])
def test_cron_owner_uses_the_same_fixed_vectors_as_ingress(user, slot):
    assert ownership().owner_slot(user) == slot


@pytest.mark.parametrize('user', ['', 'default', None, 42, ' alice ', 'alice\n', 'x' * 257])
def test_legacy_or_noncanonical_owner_cannot_silently_select_a_worker(user):
    with pytest.raises(ValueError):
        ownership().owner_slot(user)


def test_slot_must_belong_to_the_complete_manifest():
    from agent_gateway.cron.owned_store import OwnedCronJobStore
    from agent_gateway.cron.store import FileCronJobStore
    with pytest.raises(ValueError):
        OwnedCronJobStore(FileCronJobStore(), ownership(), slot=7)
    with pytest.raises(ValueError):
        OwnedCronJobStore(FileCronJobStore(), ownership(), slot=True)
