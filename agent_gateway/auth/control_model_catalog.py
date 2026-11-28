# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Opt-in public discovery using Control's existing blocking-work budget."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
import threading
from typing import Any, TYPE_CHECKING

from agent_gateway.auth.control_blocking_work import ControlBlockingWork
from agent_gateway.processes.control_service import ControlServiceBusy

if TYPE_CHECKING:
    from agent_gateway.auth.service import AuthService
    from agent_gateway.auth.session_store import AuthSessionStore

_HANDLER_TIMEOUT_S = 15.0
_SNAPSHOT_HANDLER_TIMEOUT_S = 8.0


@dataclass(frozen=True)
class _CatalogCommit:
    result: dict[str, Any]
    store: AuthSessionStore
    session_id: str
    user_id: str
    created_at: float


def _consume_result(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


class ControlPublicModelCatalog:
    """Validate one explicit session and publish only bounded public metadata.

    The existing discovery cache is shared. This port validates who is asking;
    it does not turn that cache into an account-specific entitlement list.
    """

    def __init__(
        self, provider: Callable[[], AuthService], *, blocking_work: ControlBlockingWork,
    ) -> None:
        self._provider = provider
        self._blocking_work = blocking_work
        self._lock = threading.Lock()
        self._pending = False

    def _finished(self, future: Future[Any]) -> None:
        # Release on actual thread completion, never on caller cancellation.
        with self._lock:
            self._pending = False

    def _unauthenticated_snapshot(self) -> dict[str, Any]:
        return {'version': 1, 'status': 'unauthenticated', 'api_base': '', 'models': []}

    def _unavailable_snapshot(self) -> dict[str, Any]:
        return {'version': 1, 'status': 'unavailable', 'api_base': '', 'models': []}

    async def list_login_models(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from agent_gateway.auth.public_model_catalog import validate_auth_session

        if not isinstance(arguments, dict) or set(arguments) != {'auth_session'}:
            raise ValueError('invalid catalog service value')
        try:
            session_id = validate_auth_session(arguments['auth_session'])
        except ValueError:
            raise ValueError('invalid catalog service value') from None
        if session_id is None:
            return {'status': 'unauthenticated', 'models': []}
        with self._lock:
            if self._pending:
                raise ControlServiceBusy()
            self._pending = True
        try:
            future = self._blocking_work.submit(self._load, session_id)
        except BaseException:
            with self._lock:
                self._pending = False
            raise
        future.add_done_callback(self._finished)
        waiter = asyncio.wrap_future(future)
        waiter.add_done_callback(_consume_result)
        try:
            async with asyncio.timeout(_HANDLER_TIMEOUT_S):
                result = await asyncio.shield(waiter)
            if isinstance(result, _CatalogCommit):
                current = result.store.matches_live_memory(
                    result.session_id, result.user_id, result.created_at,
                )
                if current is None:
                    return {'status': 'unavailable', 'models': []}
                if not current:
                    return {'status': 'unauthenticated', 'models': []}
                return result.result
            return result
        except TimeoutError:
            return {'status': 'unavailable', 'models': []}

    async def get_login_model_snapshot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from agent_gateway.auth.public_model_catalog import validate_auth_session

        if not isinstance(arguments, dict) or set(arguments) != {'auth_session'}:
            raise ValueError('invalid catalog service value')
        try:
            session_id = validate_auth_session(arguments['auth_session'])
        except ValueError:
            raise ValueError('invalid catalog service value') from None
        if session_id is None:
            return self._unauthenticated_snapshot()
        with self._lock:
            if self._pending:
                raise ControlServiceBusy()
            self._pending = True
        try:
            future = self._blocking_work.submit(self._load_snapshot, session_id)
        except BaseException:
            with self._lock:
                self._pending = False
            raise
        future.add_done_callback(self._finished)
        waiter = asyncio.wrap_future(future)
        waiter.add_done_callback(_consume_result)
        try:
            async with asyncio.timeout(_SNAPSHOT_HANDLER_TIMEOUT_S):
                result = await asyncio.shield(waiter)
            if isinstance(result, _CatalogCommit):
                current = result.store.matches_live_memory(
                    result.session_id, result.user_id, result.created_at,
                )
                if current is None:
                    return self._unavailable_snapshot()
                if not current:
                    return self._unauthenticated_snapshot()
                return result.result
            return result
        except TimeoutError:
            return self._unavailable_snapshot()

    def _load(self, session_id: str) -> _CatalogCommit | dict[str, Any]:
        from agent_gateway.auth.model_catalog import get_models
        from agent_gateway.auth.public_model_catalog import CatalogError, validate_catalog_response
        from agent_gateway.auth.service import ModelAuthRequired, live_session

        try:
            service = self._provider()
            session = live_session(session_id, service=service)
            epoch = (session.session_id, session.user_id, session.created_at)
            if not session.credential.id_token:
                return {'status': 'unauthenticated', 'models': []}
            models = get_models(session_id, service=service)
            current = live_session(session_id, service=service)
            if ((current.session_id, current.user_id, current.created_at) != epoch
                or not current.credential.id_token):
                return {'status': 'unauthenticated', 'models': []}
            result = validate_catalog_response({
                'status': 'ok', 'models': [model.to_dict() for model in models],
            }).to_dict()
            return _CatalogCommit(result, service._store, *epoch)
        except ModelAuthRequired:
            return {'status': 'unauthenticated', 'models': []}
        except CatalogError as error:
            return {'status': 'too_large' if error.reason == 'too_large' else 'unavailable', 'models': []}
        except Exception:
            # Provider/HTTP exceptions may include credentials or response bodies.
            return {'status': 'unavailable', 'models': []}

    def _load_snapshot(self, session_id: str) -> _CatalogCommit | dict[str, Any]:
        from agent_gateway.auth.apig import resolve_apig_config
        from agent_gateway.auth.model_catalog import get_models
        from agent_gateway.auth.public_model_catalog import (
            CatalogError, validate_login_model_snapshot,
        )
        from agent_gateway.auth.service import ModelAuthRequired, live_session

        try:
            service = self._provider()
            session = live_session(session_id, service=service)
            epoch = (session.session_id, session.user_id, session.created_at)
            if not session.credential.id_token:
                return self._unauthenticated_snapshot()
            config = resolve_apig_config(allow_refresh=False)
            if config is None:
                current = live_session(session_id, service=service)
                if ((current.session_id, current.user_id, current.created_at) != epoch
                        or not current.credential.id_token):
                    return self._unauthenticated_snapshot()
                result = {'version': 1, 'status': 'ok', 'api_base': '', 'models': []}
                return _CatalogCommit(result, service._store, *epoch)
            models = get_models(session_id, service=service)
            current = live_session(session_id, service=service)
            if ((current.session_id, current.user_id, current.created_at) != epoch
                    or not current.credential.id_token):
                return self._unauthenticated_snapshot()
            result = validate_login_model_snapshot({
                'version': 1,
                'status': 'ok',
                'api_base': config.invoke_base_url,
                'models': [model.to_dict() for model in models],
            }).to_dict()
            return _CatalogCommit(result, service._store, *epoch)
        except ModelAuthRequired:
            return self._unauthenticated_snapshot()
        except CatalogError as error:
            return {
                'version': 1,
                'status': 'too_large' if error.reason == 'too_large' else 'unavailable',
                'api_base': '',
                'models': [],
            }
        except Exception:
            # Provider/HTTP exceptions may include credentials or response bodies.
            return self._unavailable_snapshot()
