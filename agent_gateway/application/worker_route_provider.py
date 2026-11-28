# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure, slot-scoped access to the existing fixed southbound directory."""

from __future__ import annotations

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory


class StaticRouteProvider:
    """Resolve fixed owned targets without discovery, networking or lifecycle work."""

    def __init__(self, directory: StaticRouteDirectory, *, slot_id: int) -> None:
        if type(directory) is not StaticRouteDirectory:
            # Keep the original routed-client construction boundary.
            raise ValueError('routed client requires an explicit static directory')
        self._owned_targets = directory.targets_for_slot(slot_id)
        self._directory = directory
        self._slot_id = slot_id

    @property
    def identity_policy(self) -> LocalIdentityPolicy:
        """Return the original immutable ingress policy."""
        return self._directory.identity_policy

    @property
    def owned_targets(self) -> tuple[SouthboundTarget, ...]:
        """Return this slot's original target objects in directory order."""
        return self._owned_targets

    def resolve_bound_user(
        self, user_id: str | None, *, agent_type: str | None = None,
    ) -> SouthboundTarget:
        """Resolve a named local identity with the original ownership error."""
        try:
            if agent_type not in (None, 'jiuwenswarm'):
                raise ValueError
            policy = self._directory.identity_policy
            identity = IngressIdentity(
                policy.contract_id, 'local_compat', policy.domain, 'named', user_id,
            )
            return self._directory.resolve(identity, slot_id=self._slot_id)
        except (ValueError, TypeError, AttributeError, KeyError):
            raise ValueError('request has no owned static southbound target') from None
