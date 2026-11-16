# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Local access to existing session-to-model-account bindings, not Worker owners."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TYPE_CHECKING

from agent_gateway.auth.credential_access import ModelCredential

if TYPE_CHECKING:
    from agent_gateway.auth.service import AuthService


@dataclass(frozen=True)
class SessionLoginBinding:
    """The account handle and model recorded by the original binding store."""

    credential_ref: str
    model_name: str


class SessionLoginBindings(Protocol):
    """Access bindings without exposing sessions, stores or refresh tokens."""

    async def remember(self, session_id: str, credential_ref: str, model_name: str) -> None: ...

    async def lookup(self, session_id: str) -> SessionLoginBinding | None: ...

    async def model_auth_for_binding(self, binding: SessionLoginBinding) -> ModelCredential | None: ...


class LocalSessionLoginBindings:
    """Reuse the original persistence, TTL and model availability rules."""

    def __init__(self, auth_service_provider: Callable[[], AuthService]) -> None:
        self._auth_service_provider = auth_service_provider

    async def remember(self, session_id: str, credential_ref: str, model_name: str) -> None:
        from agent_gateway.auth import session_owners

        session_owners.remember(session_id, credential_ref, model_name)

    async def lookup(self, session_id: str) -> SessionLoginBinding | None:
        from agent_gateway.auth import session_owners

        owner = session_owners.lookup(session_id)
        if owner is None:
            return None
        return SessionLoginBinding(owner.credential_ref, owner.model_name)

    async def model_auth_for_binding(self, binding: SessionLoginBinding) -> ModelCredential | None:
        from agent_gateway.auth import session_owners

        def resolve() -> dict | None:
            return session_owners.login_auth_for_owner(
                session_owners.SessionOwner(binding.credential_ref, binding.model_name),
                service=self._auth_service_provider(),
            )

        auth = await asyncio.to_thread(resolve)
        if auth is None:
            return None
        return ModelCredential(
            api_base=auth['api_base'], api_key=auth['api_key'], credential_ref=auth['credential_ref'],
        )
