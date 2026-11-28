# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Small credential values and ports used by data processing consumers."""
from __future__ import annotations

from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialLease


_CREDENTIAL_ACCESS_ERROR_REASONS = frozenset(
    {"unavailable", "invalid", "protocol", "not_enabled"}
)


class CredentialAccessError(Exception):
    """A remote credential or binding access failure without secret details."""

    def __init__(self, reason: str = "unavailable") -> None:
        if reason not in _CREDENTIAL_ACCESS_ERROR_REASONS:
            raise ValueError("Unsupported credential failure reason")
        self.reason = reason
        super().__init__("Remote credential access failed")


class OwnsResolveDeadline(ABC):
    """Bindings that enforce their own typed resolve deadline."""

    @property
    @abstractmethod
    def resolve_deadline_s(self) -> float:
        """Return the port-owned resolve deadline in seconds."""


LOGIN_REFRESH_WAIT_S = 20.0


@dataclass(frozen=True)
class ModelCredential:
    """Request-scoped model credentials; secret values stay out of repr."""

    api_base: str
    api_key: str = field(repr=False)
    credential_ref: str
    lease: "WorkerCredentialLease | None" = field(default=None, repr=False, compare=False)

    def to_params(self) -> "CredentialParams":
        return CredentialParams(
            api_base=self.api_base,
            api_key=self.api_key,
            credential_ref=self.credential_ref,
            credential_lease=self.lease,
        )


class CredentialParams(dict[str, str]):
    """Wire-shaped credential params with a non-serialized request lease."""

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        credential_ref: str,
        credential_lease: "WorkerCredentialLease | None" = None,
    ) -> None:
        super().__init__({
            'api_base': api_base,
            'api_key': api_key,
            'credential_ref': credential_ref,
        })
        self.credential_lease = credential_lease

    def copy(self) -> "CredentialParams":
        return CredentialParams(
            api_base=self['api_base'],
            api_key=self['api_key'],
            credential_ref=self['credential_ref'],
            credential_lease=self.credential_lease,
        )


@dataclass(frozen=True)
class CredentialUpdate:
    """Exactly one credential replacement or revocation."""

    credential_ref: str
    api_key: str | None = field(default=None, repr=False)
    revoked: bool = False

    def __post_init__(self) -> None:
        if (self.revoked and self.api_key is not None) or (not self.revoked and not self.api_key):
            raise ValueError('A credential update requires either a token or revocation')

    def to_params(self) -> dict[str, str | bool]:
        if self.revoked:
            return {'credential_ref': self.credential_ref, 'revoked': True}
        assert self.api_key is not None
        return {'credential_ref': self.credential_ref, 'api_key': self.api_key}


@dataclass(frozen=True)
class CredentialRefresh:
    """A Control-authorized credential update for one explicit target."""

    operation_id: str
    server_id: str
    ref_revision: int
    update: CredentialUpdate


class CredentialAccess(Protocol):
    """Only return the credential information needed by this consumer."""

    async def request_model_auth(self, auth_session: str | None, model_name: str) -> ModelCredential | None: ...

    async def credential_update_for_ref(self, credential_ref: str) -> CredentialUpdate | None: ...

    async def credential_refresh_for_ref(
        self, credential_ref: str, server_id: str
    ) -> CredentialRefresh | None: ...

    async def credential_ref_for_login_session(self, auth_session: str | None) -> str | None: ...

    async def model_auth_for_ref(self, credential_ref: str) -> ModelCredential | None: ...


class CredentialUpdateSink(Protocol):
    """Send an update through the existing AgentServer transport."""

    async def publish(self, update: CredentialUpdate) -> bool: ...
