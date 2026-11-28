"""Control issuance watermarks are bounded and scoped to real endpoints."""

import pytest

from agent_gateway.auth.credential_issuance import CredentialIssuanceRegistry
from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceIdentity


def identity(slot=0, generation=1):
    return ControlServiceIdentity('group', slot, generation, 100 + slot, 99)


def test_refresh_keeps_revision_relogin_advances_and_logout_keeps_historical_targets():
    registry = CredentialIssuanceRegistry()
    first = registry.issue(identity(), 'as-a', 'a' * 32, 'epoch-one')
    assert first == registry.issue(identity(), 'as-a', 'a' * 32, 'epoch-one')
    second = registry.issue(identity(), 'as-b', 'a' * 32, 'epoch-two')
    assert second > first
    cutoff = registry.begin_logout('a' * 32, 'epoch-two')
    assert cutoff == second
    targets = registry.targets('a' * 32, cutoff)
    assert {target.server_id for target in targets} == {'as-a', 'as-b'}
    with pytest.raises(ValueError):
        registry.issue(identity(), 'as-b', 'a' * 32, 'epoch-two')
    registry.finish_logout('a' * 32, 'epoch-two')
    assert registry.issue(identity(), 'as-b', 'a' * 32, 'epoch-three') > cutoff


def test_budgets_fail_closed_and_incarnation_cleanup_is_exact():
    registry = CredentialIssuanceRegistry(max_refs=1, max_targets=2)
    registry.issue(identity(), 'as', 'a' * 32, 'one')
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(), 'as', 'b' * 32, 'two')
    registry.issue(identity(1), 'as', 'a' * 32, 'one')
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(2), 'as', 'a' * 32, 'one')
    registry.release(identity(0, 2))
    assert registry.target_count == 2
    registry.release(identity())
    assert registry.target_count == 1
    assert registry.ref_count == 1


def test_applied_old_operation_does_not_clear_later_issuance_and_results_are_bound():
    registry = CredentialIssuanceRegistry()
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    operation = registry.operation(target, revision, revoked=True)
    registry.issue(identity(), 'as', 'a' * 32, 'two')
    result = {'operation_id': operation, 'server_id': 'as', 'credential_ref': 'a' * 32,
              'outcome': 'applied', 'written': True, 'response_received': True,
              'as_ok': True, 'as_applied': True}
    with pytest.raises(ValueError):
        registry.result(identity(1), result)
    registry.result(identity(), result)
    registry.result(identity(), result)
    assert registry.target_count == 1
    with pytest.raises(ValueError):
        registry.result(identity(), {**result, 'operation_id': 'unknown'})


def test_revocation_budget_is_reserved_before_issuance_and_separate_from_refresh():
    registry = CredentialIssuanceRegistry(max_operations=1, max_revocations=1)
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    refresh_id = registry.operation(target, revision, revoked=False)
    with pytest.raises(ControlServiceBusy):
        registry.operation(target, revision, revoked=False)
    revoke_id = registry.operation(target, revision, revoked=True)
    assert registry.operation(target, revision, revoked=True) == revoke_id
    assert len(registry.operations) == 2
    assert registry.operations[refresh_id].outcome is None
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(), 'as', 'a' * 32, 'two')
    assert registry.latest_revision('a' * 32) == revision
    registry.result(identity(), {'operation_id': revoke_id, 'server_id': 'as',
        'credential_ref': 'a' * 32, 'outcome': 'applied', 'written': True,
        'response_received': True, 'as_ok': True, 'as_applied': True})
    assert registry.issue(identity(), 'as', 'a' * 32, 'two') > revision
    assert revoke_id not in registry.operations
    assert refresh_id in registry.operations


def test_reserved_revocation_capacity_reuses_unspent_slots_and_cleans_exact_identity():
    registry = CredentialIssuanceRegistry(max_revocations=1)
    registry.issue(identity(), 'as', 'a' * 32, 'one')
    revision = registry.issue(identity(), 'as', 'a' * 32, 'two')
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(1), 'as', 'a' * 32, 'two')
    registry.release(identity(0, 2))
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(1), 'as', 'a' * 32, 'two')
    registry.release(identity())
    assert registry.issue(identity(1), 'as', 'a' * 32, 'two') == revision


def test_completed_refresh_cannot_evict_pending_revocation():
    registry = CredentialIssuanceRegistry(max_operations=1, max_revocations=1)
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    revoke_id = registry.operation(target, revision, revoked=True)
    refresh_id = registry.operation(target, revision, revoked=False)
    registry.result(identity(), {'operation_id': refresh_id, 'server_id': 'as',
        'credential_ref': 'a' * 32, 'outcome': 'failed', 'written': False,
        'response_received': False, 'as_ok': None, 'as_applied': None})
    registry.operation(target, revision, revoked=False)
    assert refresh_id not in registry.operations
    assert registry.operations[revoke_id].outcome is None


def test_historical_target_reuses_its_actual_issuance_cutoff():
    registry = CredentialIssuanceRegistry(max_revocations=2)
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    operation_id = registry.operation(target, revision, revoked=True)
    newer = registry.issue(identity(1), 'as', 'a' * 32, 'two')
    assert registry.operation(target, newer, revoked=True) == operation_id
    assert registry.operations[operation_id].cutoff_revision == revision


def test_terminal_revocation_keeps_a_live_targets_fence_reservation():
    registry = CredentialIssuanceRegistry(max_revocations=1)
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    operation_id = registry.operation(target, revision, revoked=True)
    registry.result(identity(), {'operation_id': operation_id, 'server_id': 'as',
        'credential_ref': 'a' * 32, 'outcome': 'failed', 'written': False,
        'response_received': False, 'as_ok': False, 'as_applied': False})
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(1), 'as', 'b' * 32, 'other')
    assert registry.operation(target, revision, revoked=True) == operation_id
    assert registry.target_count == 1
    assert registry.issue(identity(), 'as', 'a' * 32, 'two') > revision


@pytest.mark.parametrize('outcome', ['timeout_unknown', 'confirmed_unknown', 'failed'])
def test_written_unknown_revocation_is_not_an_evictable_summary(outcome):
    registry = CredentialIssuanceRegistry(max_revocations=1)
    revision = registry.issue(identity(), 'as', 'a' * 32, 'one')
    target = registry.targets('a' * 32, revision)[0]
    operation_id = registry.operation(target, revision, revoked=True)
    registry.result(identity(), {'operation_id': operation_id, 'server_id': 'as',
        'credential_ref': 'a' * 32, 'outcome': outcome, 'written': True,
        'response_received': False, 'as_ok': None, 'as_applied': None})
    with pytest.raises(ControlServiceBusy):
        registry.issue(identity(), 'as', 'a' * 32, 'two')
    assert registry.operations[operation_id].outcome == outcome
    assert registry.latest_revision('a' * 32) == revision
