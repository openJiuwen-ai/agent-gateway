# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fixed user ownership over an immutable Worker slot manifest."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy


_ALGORITHM = 'hrw-sha256-v1'
_SCORE_PREFIX = b'gateway-worker-hrw-v1\0'


@dataclass(frozen=True)
class WorkerManifest:
    """Configured slots, never a filtered snapshot of healthy processes.

    Configuration versions identify the manifest, but do not salt user hashes.
    Slot membership may only change across an explicitly rebuilt runtime group.
    """

    slots: tuple[int, ...]
    config_version: int = 1
    algorithm: str = _ALGORITHM

    def __post_init__(self) -> None:
        if (
            type(self.slots) is not tuple or not self.slots
            or any(type(slot) is not int or slot < 0 for slot in self.slots)
            or len(set(self.slots)) != len(self.slots)
        ):
            raise ValueError('worker slots must be a non-empty tuple of distinct non-negative integers')
        if type(self.config_version) is not int or self.config_version < 1:
            raise ValueError('worker manifest configuration version must be positive')
        if self.algorithm != _ALGORITHM:
            raise ValueError('unsupported worker affinity algorithm')
        object.__setattr__(self, 'slots', tuple(sorted(self.slots)))


@dataclass(frozen=True)
class FixedUserHash:
    """Select one stable slot; availability and admission are checked afterwards."""

    manifest: WorkerManifest
    identity_policy: LocalIdentityPolicy

    def __post_init__(self) -> None:
        if type(self.manifest) is not WorkerManifest:
            raise ValueError('an explicit WorkerManifest is required')
        if type(self.identity_policy) is not LocalIdentityPolicy:
            raise ValueError('an explicit LocalIdentityPolicy is required')

    def select(self, identity: IngressIdentity) -> int:
        """Use SHA-256 rendezvous scores, with the smaller slot breaking ties."""
        if type(identity) is not IngressIdentity:
            raise ValueError('a normalized IngressIdentity is required')
        if (
            identity.contract_id != self.identity_policy.contract_id
            or identity.domain != self.identity_policy.domain
        ):
            raise ValueError('identity policy does not match worker affinity')
        prefix = _SCORE_PREFIX + identity.user_key_bytes() + b'\0'
        selected = self.manifest.slots[0]
        highest = b''
        for slot in self.manifest.slots:
            score = hashlib.sha256(prefix + str(slot).encode('ascii')).digest()
            if score > highest:
                selected = slot
                highest = score
        return selected
