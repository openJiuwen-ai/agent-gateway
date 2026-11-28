# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure Cron ownership, using the same identity and manifest as ingress."""

from dataclasses import dataclass

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


@dataclass(frozen=True)
class CronOwnership:
    """Select configured slots, without using health, job IDs or credentials."""

    manifest: WorkerManifest
    identity_policy: LocalIdentityPolicy

    def __post_init__(self) -> None:
        FixedUserHash(self.manifest, self.identity_policy)

    def owner_slot(self, user_id: str) -> int:
        """Reject legacy owners rather than invent or normalize their identity."""
        if not isinstance(user_id, str) or not user_id or user_id == 'default':
            raise ValueError('cron requires a canonical named owner')
        identity = IngressIdentity(
            contract_id=self.identity_policy.contract_id, mode='local_compat',
            domain=self.identity_policy.domain, kind='named', business_user_id=user_id,
        )
        return FixedUserHash(self.manifest, self.identity_policy).select(identity)
