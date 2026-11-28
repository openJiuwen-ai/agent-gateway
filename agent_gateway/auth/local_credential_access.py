# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Single-process credential access, using the existing login implementation."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, replace
import logging
from typing import Any, TYPE_CHECKING

from agent_gateway.auth.credential_access import CredentialUpdate, ModelCredential, LOGIN_REFRESH_WAIT_S

if TYPE_CHECKING:
    from agent_gateway.auth.service import AuthService
    from agent_gateway.auth.control_blocking_work import ControlBlockingWork
    from agent_gateway.auth.session_store import AuthSessionStore

logger = logging.getLogger(__name__)
_LOGIN_REFRESH_WAIT_S = LOGIN_REFRESH_WAIT_S


def _consume_refresh_result(future: asyncio.Future) -> None:
    """Retrieve late failures without logging credential-bearing exceptions."""
    if not future.cancelled():
        future.exception()


def _finish_initial(future: asyncio.Future, value: Any) -> None:
    if not future.done():
        future.set_result(value)


def _fail_initial(future: asyncio.Future, error: BaseException) -> None:
    if future.cancelled():
        return
    if future.done():
        _consume_refresh_result(future)
        return
    future.set_exception(error)


@dataclass(frozen=True)
class LocalCredentialSnapshot:
    """Immutable identity facts for a synchronous Control commit."""

    store: AuthSessionStore
    session_id: str
    user_id: str
    created_at: float
    credential_ref: str


@dataclass(frozen=True)
class LocalCredentialResult:
    """A request-scoped credential result and its immutable identity snapshot."""

    credential: ModelCredential | None
    snapshot: LocalCredentialSnapshot | None
    model_required: bool
    expired: bool = False
    model_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class _RequestWork:
    future: Future
    initial: asyncio.Future


class LocalCredentialAccess:
    """Lazily use the original service; construction has no login side effects."""

    def __init__(
        self, auth_service_provider: Callable[[], AuthService], *,
        blocking_work: ControlBlockingWork | None = None,
    ) -> None:
        self._auth_service_provider = auth_service_provider
        self._blocking_work = blocking_work
        self._refreshing: dict[str, _RequestWork] = {}

    async def request_model_auth(self, auth_session: str | None, model_name: str) -> ModelCredential | None:
        result = await self.request_model_auth_with_snapshot(auth_session, model_name)
        return None if result is None else result.credential

    async def request_model_auth_with_snapshot(
        self, auth_session: str | None, model_name: str,
    ) -> LocalCredentialResult | None:
        from agent_gateway.auth.passthrough import _build_model_auth, _is_login_model, expired_login_session

        model_name = model_name.strip()
        if self._blocking_work is not None:
            return await self._bounded_request_model_auth(auth_session, model_name)
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
            except Exception:
                logger.debug('[ModelAuth] Login credential refresh failed', exc_info=True)
        auth = _build_model_auth(model_name, auth_session, service=service)
        if auth is None:
            return LocalCredentialResult(None, None, True)
        current = service.resolve_session(auth_session)
        if current is None:
            return LocalCredentialResult(None, None, True)
        snapshot = self._snapshot(service, current)
        return LocalCredentialResult(
            ModelCredential(
                api_base=auth['api_base'],
                api_key=auth['api_key'],
                credential_ref=auth['credential_ref'],
            ),
            snapshot,
            True,
            current.credential.is_expired(),
        )

    async def _bounded_request_model_auth(
        self, auth_session: str | None, model_name: str,
    ) -> LocalCredentialResult | None:
        """Keep account refresh bounded while each request judges its own model."""
        from agent_gateway.auth.apig import resolve_apig_config
        from agent_gateway.auth.login_credentials import bare_model_name
        from agent_gateway.auth.model_catalog import get_models
        from agent_gateway.auth.service import REFRESH_AHEAD_S

        if not auth_session or not model_name:
            return LocalCredentialResult(None, None, False)
        service = self._auth_service_provider()
        work = self._refreshing.get(auth_session)
        if work is None or work.future.done():
            loop = asyncio.get_running_loop()
            initial = loop.create_future()
            initial.add_done_callback(_consume_refresh_result)

            def load_and_refresh():
                try:
                    models = get_models(auth_session, allow_refresh=False, service=service)
                    model_names = tuple(model.model_name for model in models)
                    current = service.resolve_session(auth_session)
                    config = resolve_apig_config(allow_refresh=False)
                    snapshot = None if current is None else self._snapshot(service, current)
                    if current is None or config is None:
                        result = LocalCredentialResult(
                            None, snapshot, False, False, model_names,
                        )
                    else:
                        result = LocalCredentialResult(
                            ModelCredential(
                                api_base=config.invoke_base_url,
                                api_key=current.credential.id_token,
                                credential_ref=snapshot.credential_ref,
                            ),
                            snapshot,
                            False,
                            current.credential.is_expired(),
                            model_names,
                        )
                    loop.call_soon_threadsafe(_finish_initial, initial, result)
                    if current is None or not current.credential.expires_within(REFRESH_AHEAD_S):
                        return result
                    try:
                        service.try_refresh(current)
                    except Exception:
                        logger.debug('[ModelAuth] Control credential refresh failed', exc_info=True)
                    current = service.resolve_session(auth_session)
                    config = resolve_apig_config(allow_refresh=False)
                    if current is None or config is None:
                        return LocalCredentialResult(
                            None, result.snapshot, False, True, model_names,
                        )
                    snapshot = self._snapshot(service, current)
                    return LocalCredentialResult(
                        ModelCredential(
                            api_base=config.invoke_base_url,
                            api_key=current.credential.id_token,
                            credential_ref=snapshot.credential_ref,
                        ),
                        snapshot,
                        False,
                        current.credential.is_expired(),
                        model_names,
                    )
                except BaseException as error:
                    loop.call_soon_threadsafe(_fail_initial, initial, error)
                    raise

            future = self._blocking_work.submit(load_and_refresh)
            future.add_done_callback(_consume_refresh_result)
            work = _RequestWork(future, initial)
            self._refreshing[auth_session] = work

            def clear(completed: Future) -> None:
                def remove() -> None:
                    if self._refreshing.get(auth_session) is work:
                        self._refreshing.pop(auth_session, None)
                try:
                    loop.call_soon_threadsafe(remove)
                except RuntimeError:
                    pass

            future.add_done_callback(clear)

        initial_result: LocalCredentialResult = await asyncio.shield(work.initial)
        model_required = (
            model_name in initial_result.model_names
            or bare_model_name(model_name) in initial_result.model_names
        )
        if not model_required:
            return LocalCredentialResult(
                None, initial_result.snapshot, False, initial_result.expired,
                initial_result.model_names,
            )
        if initial_result.credential is None or not initial_result.expired:
            return replace(initial_result, model_required=True)

        waiter = asyncio.wrap_future(work.future)
        waiter.add_done_callback(_consume_refresh_result)
        try:
            async with asyncio.timeout(_LOGIN_REFRESH_WAIT_S):
                final_result = await asyncio.shield(waiter)
        except asyncio.TimeoutError:
            logger.warning('[ModelAuth] Control credential refresh timed out')
            return LocalCredentialResult(
                None, initial_result.snapshot, True, True, initial_result.model_names,
            )
        except Exception:
            return LocalCredentialResult(
                None, initial_result.snapshot, True, True, initial_result.model_names,
            )
        return replace(final_result, model_required=True)

    def _snapshot(self, service: AuthService, session: Any) -> LocalCredentialSnapshot:
        from agent_gateway.auth.login_credentials import credential_ref_for_user

        return LocalCredentialSnapshot(
            store=service._store,
            session_id=session.session_id,
            user_id=session.user_id,
            created_at=session.created_at,
            credential_ref=credential_ref_for_user(session.user_id),
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
