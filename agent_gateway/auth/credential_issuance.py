# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bounded, same-loop credential issuance facts without storing tokens."""
from __future__ import annotations

from dataclasses import dataclass
import secrets
from typing import Any

from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceIdentity


@dataclass
class _RefState:
    epoch: str
    revision: int = 1
    phase: str = 'active'


@dataclass(frozen=True)
class CredentialTarget:
    identity: ControlServiceIdentity
    server_id: str
    credential_ref: str


@dataclass
class CredentialOperation:
    target: CredentialTarget
    cutoff_revision: int
    revoked: bool
    local_ack: str = 'pending'
    outcome: str | None = None
    written: bool = False
    response_received: bool = False
    as_ok: bool | None = None
    as_applied: bool | None = None

    @property
    def settled(self) -> bool:
        """An uncertain written revoke remains a safety fact, not audit history."""
        return (self.outcome in {'applied', 'rejected', 'superseded'}
                or (self.outcome in {'failed', 'timeout_unknown'} and not self.written))


class CredentialIssuanceRegistry:
    """Retain watermarks until group shutdown; never evict safety facts for space."""

    def __init__(self, *, max_refs: int = 1024, max_targets: int = 4096,
                 max_operations: int = 4096, max_revocations: int = 4096) -> None:
        for limit in (max_refs, max_targets, max_operations, max_revocations):
            if type(limit) is not int or limit < 1:
                raise ValueError('credential budgets must be positive integers')
        self._max_refs = max_refs
        self._max_targets = max_targets
        self._max_operations = max_operations
        self._max_revocations = max_revocations
        self._refs: dict[str, _RefState] = {}
        self._targets: dict[CredentialTarget, int] = {}
        self.operations: dict[str, CredentialOperation] = {}
        self._revocations: dict[tuple[CredentialTarget, int], str | None] = {}

    @property
    def ref_count(self) -> int:
        return len(self._refs)

    @property
    def target_count(self) -> int:
        return len(self._targets)

    def issue(self, identity: ControlServiceIdentity, server_id: str,
              credential_ref: str, epoch: str) -> int:
        target = CredentialTarget(identity, server_id, credential_ref)
        if target not in self._targets and len(self._targets) >= self._max_targets:
            raise ControlServiceBusy()
        state = self._refs.get(credential_ref)
        revision = 1 if state is None else state.revision + (state.epoch != epoch)
        if state is None:
            if len(self._refs) >= self._max_refs:
                raise ControlServiceBusy()
        elif state.epoch == epoch and state.phase != 'active':
            raise ValueError('credential login is closing')
        # Reserve revocation capacity before returning any new credential. A
        # full refresh backlog must never consume a live lease's fence budget.
        self._reserve_revocation(target, revision)
        if state is None:
            state = _RefState(epoch)
            self._refs[credential_ref] = state
        elif state.epoch != epoch:
            state.epoch = epoch
            state.revision += 1
            state.phase = 'active'
        self._targets[target] = state.revision
        return state.revision

    def active(self, credential_ref: str, epoch: str) -> bool:
        state = self._refs.get(credential_ref)
        return state is None or state.epoch != epoch or state.phase == 'active'

    def begin_logout(self, credential_ref: str, epoch: str) -> int:
        state = self._refs.get(credential_ref)
        if state is None:
            if len(self._refs) >= self._max_refs:
                raise ControlServiceBusy()
            state = _RefState(epoch)
            self._refs[credential_ref] = state
        elif state.epoch != epoch:
            state.epoch = epoch
            state.revision += 1
        state.phase = 'closing'
        return state.revision

    def finish_logout(self, credential_ref: str, epoch: str) -> None:
        state = self._refs[credential_ref]
        if state.epoch == epoch and state.phase == 'closing':
            state.phase = 'revoked'

    def targets(self, credential_ref: str, cutoff: int) -> list[CredentialTarget]:
        return [target for target, revision in self._targets.items()
                if target.credential_ref == credential_ref and revision <= cutoff]

    def registered(self, identity: ControlServiceIdentity, server_id: str,
                   credential_ref: str) -> bool:
        return CredentialTarget(identity, server_id, credential_ref) in self._targets

    def revision(self, credential_ref: str, epoch: str) -> int:
        state = self._refs.get(credential_ref)
        if state is None or state.epoch != epoch or state.phase != 'active':
            raise ValueError('credential login is not active')
        return state.revision

    def latest_revision(self, credential_ref: str) -> int:
        state = self._refs.get(credential_ref)
        if state is None:
            raise ValueError('credential has not been issued')
        return state.revision

    def operation(self, target: CredentialTarget, cutoff: int, *, revoked: bool) -> str:
        if revoked:
            # A historical consumer only needs to fence versions actually
            # issued to it, not a later login used by a different Worker.
            cutoff = min(cutoff, self._targets.get(target, cutoff))
            key = (target, cutoff)
            operation_id = self._revocations.get(key)
            if operation_id is not None:
                return operation_id
            self._reserve_revocation(target, cutoff)
        elif sum(not operation.revoked for operation in self.operations.values()) >= self._max_operations:
            completed = next((key for key, operation in self.operations.items()
                              if not operation.revoked and operation.outcome is not None), None)
            if completed is None:
                raise ControlServiceBusy()
            del self.operations[completed]
        operation_id = secrets.token_hex(16)
        self.operations[operation_id] = CredentialOperation(target, cutoff, revoked)
        if revoked:
            self._revocations[(target, cutoff)] = operation_id
        return operation_id

    def _reserve_revocation(self, target: CredentialTarget, revision: int) -> None:
        key = (target, revision)
        if key in self._revocations:
            return
        unused = next((old for old, operation_id in self._revocations.items()
                       if old[0] == target and operation_id is None), None)
        if unused is not None:
            del self._revocations[unused]
        elif len(self._revocations) >= self._max_revocations:
            completed = next((old for old, operation_id in self._revocations.items()
                              if operation_id is not None
                              and self.operations[operation_id].settled
                              and (old[0] == target or old[0] not in self._targets)), None)
            if completed is None:
                raise ControlServiceBusy()
            operation_id = self._revocations.pop(completed)
            del self.operations[operation_id]
        self._revocations[key] = None

    def result(self, identity: ControlServiceIdentity, result: dict[str, Any]) -> None:
        fields = {'operation_id', 'server_id', 'credential_ref', 'outcome',
                  'written', 'response_received', 'as_ok', 'as_applied'}
        if set(result) != fields:
            raise ValueError('invalid credential result')
        operation = self.operations.get(result['operation_id'])
        if operation is None or operation.target != CredentialTarget(
            identity, result['server_id'], result['credential_ref'],
        ):
            raise ValueError('unknown credential operation')
        outcome = result['outcome']
        if outcome not in {'applied', 'rejected', 'failed', 'timeout_unknown',
                           'confirmed_unknown', 'superseded'}:
            raise ValueError('invalid credential outcome')
        for name in ('written', 'response_received'):
            if type(result[name]) is not bool:
                raise ValueError('invalid credential result flag')
        for name in ('as_ok', 'as_applied'):
            if result[name] is not None and type(result[name]) is not bool:
                raise ValueError('invalid credential result flag')
        if outcome == 'applied' and not all(result[name] is True for name in
            ('written', 'response_received', 'as_ok', 'as_applied')):
            raise ValueError('invalid applied credential outcome')
        if operation.outcome is not None:
            previous = (operation.outcome, operation.written, operation.response_received,
                        operation.as_ok, operation.as_applied)
            current = tuple(result[name] for name in ('outcome', 'written', 'response_received',
                                                     'as_ok', 'as_applied'))
            if previous != current:
                raise ValueError('conflicting credential result')
            return
        operation.outcome = outcome
        operation.written = result['written']
        operation.response_received = result['response_received']
        operation.as_ok = result['as_ok']
        operation.as_applied = result['as_applied']
        if operation.revoked and outcome == 'applied':
            revision = self._targets.get(operation.target)
            if revision is not None and revision <= operation.cutoff_revision:
                del self._targets[operation.target]

    def release(self, identity: ControlServiceIdentity) -> None:
        for key in tuple(self._revocations):
            if key[0].identity == identity:
                del self._revocations[key]
        for target in tuple(self._targets):
            if target.identity == identity:
                del self._targets[target]
        for key, operation in tuple(self.operations.items()):
            if operation.target.identity == identity:
                del self.operations[key]
