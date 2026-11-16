# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Small credential values and ports used by data processing consumers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class ModelCredential:
    """Request-scoped model credentials; secret values stay out of repr."""

    api_base: str
    api_key: str = field(repr=False)
    credential_ref: str

    def to_params(self) -> dict[str, str]:
        return {'api_base': self.api_base, 'api_key': self.api_key, 'credential_ref': self.credential_ref}


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


class CredentialAccess(Protocol):
    """Only return the credential information needed by this consumer."""

    async def request_model_auth(self, auth_session: str | None, model_name: str) -> ModelCredential | None: ...

    async def credential_update_for_ref(self, credential_ref: str) -> CredentialUpdate | None: ...


class CredentialUpdateSink(Protocol):
    """Send an update through the existing AgentServer transport."""

    async def publish(self, update: CredentialUpdate) -> bool: ...
