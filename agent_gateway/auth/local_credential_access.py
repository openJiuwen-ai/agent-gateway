# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Single-process credential access, using the existing login implementation."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from typing import TYPE_CHECKING

from agent_gateway.auth.credential_access import CredentialUpdate, ModelCredential, LOGIN_REFRESH_WAIT_S

if TYPE_CHECKING:
    from agent_gateway.auth.service import AuthService

logger = logging.getLogger(__name__)
_LOGIN_REFRESH_WAIT_S = LOGIN_REFRESH_WAIT_S


class LocalCredentialAccess:
    """Lazily use the original service; construction has no login side effects."""

    def __init__(self, auth_service_provider: Callable[[], AuthService]) -> None:
        self._auth_service_provider = auth_service_provider

    async def request_model_auth(self, auth_session: str | None, model_name: str) -> ModelCredential | None:
        from agent_gateway.auth.passthrough import _build_model_auth, _is_login_model, expired_login_session

        model_name = model_name.strip()
        if not auth_session or not model_name:
            return None
        service = self._auth_service_provider()
        if not _is_login_model(model_name, auth_session, service=service):
            return None
        session = expired_login_session({'model_name': model_name}, auth_session, service=service)
        if session is not None:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(service.try_refresh, session), timeout=_LOGIN_REFRESH_WAIT_S,
                )
            except asyncio.TimeoutError:
                logger.warning('[ModelAuth] Login credential refresh timed out; checking current credentials')
            except Exception:  # A failed refresh must not prevent the original request.
                logger.debug('[ModelAuth] Login credential refresh failed', exc_info=True)
        auth = _build_model_auth(model_name, auth_session, service=service)
        if auth is None:
            return None
        return ModelCredential(
            api_base=auth['api_base'], api_key=auth['api_key'], credential_ref=auth['credential_ref'],
        )

    async def credential_update_for_ref(self, credential_ref: str) -> CredentialUpdate | None:
        from agent_gateway.auth.login_credentials import is_credential_ref
        from agent_gateway.auth.passthrough import refreshed_credential_for_ref

        if not is_credential_ref(credential_ref):
            return None
        update = await asyncio.to_thread(
            refreshed_credential_for_ref, credential_ref, service=self._auth_service_provider(),
        )
        if update is None:
            return None
        return CredentialUpdate(
            credential_ref=update['credential_ref'],
            api_key=update.get('api_key'), revoked=bool(update.get('revoked', False)),
        )


    async def credential_ref_for_login_session(self, auth_session: str | None) -> str | None:
        from agent_gateway.auth.login_credentials import credential_ref_for_user
        from agent_gateway.auth.service import ModelAuthRequired, live_session

        if not auth_session:
            return None
        try:
            session = live_session(
                auth_session, allow_refresh=False, service=self._auth_service_provider(),
            )
        except ModelAuthRequired:
            return None
        return credential_ref_for_user(session.user_id)

    async def model_auth_for_ref(self, credential_ref: str) -> ModelCredential | None:
        from agent_gateway.auth.login_credentials import is_credential_ref
        from agent_gateway.auth.passthrough import _model_auth_for_ref

        if not is_credential_ref(credential_ref):
            return None
        auth = await asyncio.to_thread(
            _model_auth_for_ref, credential_ref, service=self._auth_service_provider(),
        )
        if auth is None:
            return None
        return ModelCredential(
            api_base=auth['api_base'], api_key=auth['api_key'], credential_ref=auth['credential_ref'],
        )
