# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Explicit Control-owned credential handlers; not a production account switch."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
import hashlib
import hmac
import secrets
from types import MappingProxyType
from typing import Any, TYPE_CHECKING

from agent_gateway.auth.credential_access import ModelCredential
from agent_gateway.auth.control_blocking_work import ControlBlockingWork
from agent_gateway.auth.credential_issuance import CredentialIssuanceRegistry, CredentialTarget
from agent_gateway.auth.local_credential_access import (
    LocalCredentialAccess,
    LocalCredentialResult,
    LocalCredentialSnapshot,
)
from agent_gateway.auth.session_login_bindings import LocalSessionLoginBindings
from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceIdentity, Handler

if TYPE_CHECKING:
    from agent_gateway.auth.service import AuthService
    from agent_gateway.auth.session_store import AuthSessionStore


def _fields(value: object, names: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != names:
        raise ValueError('invalid credential service value')
    return value


def _text(value: object, limit: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > limit or (not empty and not value.strip()):
        raise ValueError('invalid credential service value')
    return value


def _ref(value: object) -> str:
    from agent_gateway.auth.login_credentials import is_credential_ref

    result = _text(value, 32)
    if not is_credential_ref(result):
        raise ValueError('invalid credential service value')
    return result


def _credential(value: ModelCredential | None) -> dict[str, Any]:
    if value is None:
        return {'status': 'invalid'}
    return {'status': 'ok', 'credential': {
        'api_base': _text(value.api_base, 2048),
        'api_key': _text(value.api_key, 4096),
        'credential_ref': _ref(value.credential_ref),
    }}


@dataclass(frozen=True)
class _SessionSnapshot:
    """Immutable identity facts captured by a bounded store read."""

    store: AuthSessionStore
    session_id: str
    user_id: str
    created_at: float
    credential_ref: str
    epoch: str


@dataclass(frozen=True)
class _CredentialRead:
    """Immutable credential facts captured by a bounded store read."""

    id_token: str
    expired: bool


@dataclass(frozen=True)
class _SessionRead:
    """A mutable session object plus the immutable commit identity."""

    snapshot: _SessionSnapshot
    session: Any
    credential: _CredentialRead


@dataclass(frozen=True)
class _RefreshWork:
    """A shared explicit refresh submitted to the real bounded pool."""

    future: Future


class ControlCredentialServices:
    """Reuse one authority; lifecycle handlers require explicit endpoint binding.

    The private socket authenticates a Worker incarnation, not a business user.
    Binding operations are internal calls after server-side credential injection;
    no northbound method accepts a credential reference as an account selector.
    """

    def __init__(
        self, auth_service_provider: Callable[[], AuthService], *, max_blocking_work: int = 4,
        server_id: str | None = None,
        enable_public_catalog: bool = False,
    ) -> None:
        if not callable(auth_service_provider):
            raise ValueError('auth service provider must be callable')
        self._provider = auth_service_provider
        self._prepared_service: AuthService | None = None
        self._blocking_work = ControlBlockingWork(limit=max_blocking_work)
        self._credentials = LocalCredentialAccess(self._service, blocking_work=self._blocking_work)
        self._bindings = LocalSessionLoginBindings(self._service, blocking_work=self._blocking_work)
        self._server_id = None if server_id is None else _text(server_id, 256)
        self.registry = CredentialIssuanceRegistry()
        self._epoch_key = secrets.token_bytes(32)
        self._endpoints: dict[ControlServiceIdentity, Any] = {}
        self._group_id: str | None = None
        self._callers: dict[int, ControlServiceIdentity] = {}
        self._refreshing: dict[tuple[str, str], _RefreshWork] = {}
        handlers: dict[str, Handler] = {
            'credentials.request_model_auth': self.request_model_auth,
            'bindings.remember': self.remember,
            'bindings.lookup': self.lookup,
            'bindings.resolve_model_auth': self.resolve_model_auth,
        }
        self._public_catalog = None
        if enable_public_catalog:
            from agent_gateway.auth.control_model_catalog import ControlPublicModelCatalog

            self._public_catalog = ControlPublicModelCatalog(
                self._service, blocking_work=self._blocking_work,
            )
            handlers['catalog.list_login_models'] = self._public_catalog.list_login_models
            handlers['catalog.get_login_model_snapshot'] = self._public_catalog.get_login_model_snapshot
        self.handlers: Mapping[str, Handler] = MappingProxyType(handlers)

    @property
    def pending_work_count(self) -> int:
        """Count actual submitted blocking work, including timed-out waiters."""
        return self._blocking_work.pending_count

    async def stop(self, *, timeout: float = 1.0) -> None:
        """Close explicit Control work ownership without claiming hard cancellation."""
        await self._blocking_work.stop(timeout=timeout)

    @property
    def prepared_service(self) -> AuthService:
        """Return the prepared authority without resolving or creating it."""
        if self._prepared_service is None:
            raise RuntimeError('credential authority has not been prepared')
        return self._prepared_service

    def _service(self) -> AuthService:
        if self._prepared_service is None:
            return self._provider()
        return self._prepared_service

    async def prepare(self) -> None:
        """Prime the original authority store without login or network work."""
        def load_original_authority() -> AuthService:
            service = self._service()
            service._store.list_sessions()
            return service

        service = await self._blocking_work.run(load_original_authority)
        if self._prepared_service is None:
            self._prepared_service = service
        elif self._prepared_service is not service:
            raise ValueError('auth service provider returned a different authority')

    def handlers_for(self, identity: ControlServiceIdentity) -> Mapping[str, Handler]:
        """Bind authority to an authenticated socket, not Worker-supplied identity."""
        if self._server_id is None:
            raise ValueError('credential lifecycle requires an explicit AgentServer target')
        if self._group_id is None:
            self._group_id = identity.group_id
        elif self._group_id != identity.group_id:
            raise ValueError('credential authority belongs to one process group')
        previous = self._callers.get(identity.slot_id)
        if previous is not None and previous != identity:
            raise ValueError('credential slot already has a live incarnation')
        if previous is None and len(self._callers) >= 4096:
            from agent_gateway.processes.control_service import ControlServiceBusy
            raise ControlServiceBusy()
        self._callers[identity.slot_id] = identity

        async def request(arguments: dict[str, Any]) -> dict[str, Any]:
            return await self._issued_request(identity, arguments)

        async def resolve(arguments: dict[str, Any]) -> dict[str, Any]:
            return await self._issued_binding(identity, arguments)

        async def refresh(arguments: dict[str, Any]) -> dict[str, Any]:
            return await self._refresh_for_ref(identity, arguments)

        async def result(arguments: dict[str, Any]) -> dict[str, Any]:
            self.registry.result(identity, arguments)
            return {'status': 'ok'}

        return MappingProxyType({**self.handlers,
            'credentials.request_model_auth': request,
            'bindings.resolve_model_auth': resolve,
            'credentials.refresh_for_ref': refresh,
            'credentials.update_result': result,
        })

    def endpoint_connected(self, endpoint: Any) -> None:
        """Remember a real server endpoint for directed invalidation only."""
        if (endpoint.identity.group_id != self._group_id
            or self._callers.get(endpoint.identity.slot_id) != endpoint.identity):
            raise ValueError('credential endpoint has no live bound incarnation')
        self._endpoints[endpoint.identity] = endpoint

    def endpoint_disconnected(self, identity: ControlServiceIdentity) -> None:
        self._endpoints.pop(identity, None)
        if self._callers.get(identity.slot_id) == identity:
            self._callers.pop(identity.slot_id)
        self.registry.release(identity)

    def _epoch(self, session: Any) -> str:
        return self._epoch_for_id(session.session_id)

    def _epoch_for_id(self, session_id: str) -> str:
        return hmac.new(self._epoch_key, session_id.encode(), hashlib.sha256).hexdigest()

    def _snapshot(self, service: AuthService, session: Any) -> _SessionSnapshot:
        from agent_gateway.auth.login_credentials import credential_ref_for_user

        return _SessionSnapshot(
            store=service._store,
            session_id=session.session_id,
            user_id=session.user_id,
            created_at=session.created_at,
            credential_ref=credential_ref_for_user(session.user_id),
            epoch=self._epoch(session),
        )

    def _snapshot_from_local(self, snapshot: LocalCredentialSnapshot) -> _SessionSnapshot:
        return _SessionSnapshot(
            store=snapshot.store,
            session_id=snapshot.session_id,
            user_id=snapshot.user_id,
            created_at=snapshot.created_at,
            credential_ref=snapshot.credential_ref,
            epoch=self._epoch_for_id(snapshot.session_id),
        )

    def _read_session(self, session_id: str | None) -> _SessionRead | None:
        service = self._service()
        session = service.resolve_session(session_id)
        if session is None:
            return None
        credential = _CredentialRead(
            session.credential.id_token,
            session.credential.is_expired(),
        )
        return _SessionRead(self._snapshot(service, session), session, credential)

    async def _session_for_ref(self, credential_ref: str) -> _SessionRead | None:
        from agent_gateway.auth.login_credentials import credential_ref_for_user

        def find_session() -> _SessionRead | None:
            service = self._service()
            session = next((session for session in service._store.list_sessions()
                            if credential_ref_for_user(session.user_id) == credential_ref), None)
            if session is None:
                return None
            credential = _CredentialRead(
                session.credential.id_token,
                session.credential.is_expired(),
            )
            return _SessionRead(self._snapshot(service, session), session, credential)

        return await self._blocking_work.run(find_session)

    def _commit_issuance(self, identity: ControlServiceIdentity, before: _SessionSnapshot | None,
                         response: dict[str, Any]) -> dict[str, Any]:
        """Postcheck, version and register synchronously before returning a secret."""
        if self._callers.get(identity.slot_id) != identity:
            return {'status': 'invalid'}
        if before is None or response.get('status') != 'ok':
            return {'status': 'invalid'} if response.get('status') == 'ok' else response
        snapshot = before
        credential = response['credential']
        live = snapshot.store.matches_live_memory(
            snapshot.session_id,
            snapshot.user_id,
            snapshot.created_at,
            expected_id_token=credential['api_key'],
        )
        if (live is not True or credential['credential_ref'] != snapshot.credential_ref
                or not self.registry.active(credential['credential_ref'], snapshot.epoch)):
            return {'status': 'invalid'}
        assert self._server_id is not None
        revision = self.registry.issue(identity, self._server_id,
                                       credential['credential_ref'], snapshot.epoch)
        return {'status': 'ok', 'credential': {**credential,
            'api_key': _text(credential['api_key'], 4096)}, 'lease': {
            'group_id': identity.group_id, 'slot_id': identity.slot_id,
            'generation': identity.generation, 'server_id': self._server_id,
            'credential_ref': credential['credential_ref'], 'ref_revision': revision}}

    async def _issued_request(self, identity: ControlServiceIdentity,
                              arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'auth_session', 'model_name'})
        raw = arguments['auth_session']
        session_id = None if raw is None else _text(raw, 256, empty=True).strip() or None
        model_name = _text(arguments['model_name'], 256, empty=True).strip()
        result: LocalCredentialResult | None = None
        if session_id and model_name:
            result = await self._credentials.request_model_auth_with_snapshot(session_id, model_name)
            if result is not None and result.snapshot is None:
                return {'status': 'invalid'}
        if result is None or not result.model_required:
            return {'status': 'not_required'}
        if result.credential is None or result.snapshot is None:
            return {'status': 'invalid'}
        response = _credential(result.credential)
        before = self._snapshot_from_local(result.snapshot)
        return self._commit_issuance(identity, before, response)

    async def _issued_binding(self, identity: ControlServiceIdentity,
                              arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'session_id', 'credential_ref', 'model_name'})
        credential_ref = _ref(arguments['credential_ref'])
        before = await self._session_for_ref(credential_ref)
        response = await self.resolve_model_auth(arguments)
        return self._commit_issuance(identity, None if before is None else before.snapshot, response)

    async def _refresh_for_ref(self, identity: ControlServiceIdentity,
                               arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'server_id', 'credential_ref'})
        server_id = _text(arguments['server_id'], 256)
        credential_ref = _ref(arguments['credential_ref'])
        if (self._callers.get(identity.slot_id) != identity or server_id != self._server_id
            or not self.registry.registered(identity, server_id, credential_ref)):
            return {'status': 'invalid'}
        preawait_revision = self.registry.latest_revision(credential_ref)
        before = await self._session_for_ref(credential_ref)
        if before is None:
            if (self._callers.get(identity.slot_id) != identity
                    or not self.registry.registered(identity, server_id, credential_ref)):
                return {'status': 'invalid'}
            revision = preawait_revision
            update: dict[str, Any] = {'credential_ref': credential_ref, 'revoked': True}
        else:
            epoch = before.snapshot.epoch
            if not self.registry.active(credential_ref, epoch):
                return {'status': 'invalid'}
            from agent_gateway.auth.login_credentials import REFRESH_AHEAD_S

            refresh_key = (credential_ref, epoch)
            work = self._refreshing.get(refresh_key)
            if work is None or work.future.done():
                loop = asyncio.get_running_loop()
                future = self._blocking_work.submit(partial(
                    self._service().try_refresh, before.session, ahead_s=REFRESH_AHEAD_S,
                ))
                future.add_done_callback(
                    lambda completed: None if completed.cancelled() else completed.exception(),
                )
                work = _RefreshWork(future)
                self._refreshing[refresh_key] = work

                def clear(completed: Future) -> None:
                    def remove() -> None:
                        if self._refreshing.get(refresh_key) is work:
                            self._refreshing.pop(refresh_key, None)

                    try:
                        loop.call_soon_threadsafe(remove)
                    except RuntimeError:
                        pass

                future.add_done_callback(clear)

            waiter = asyncio.wrap_future(work.future)
            waiter.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            try:
                async with asyncio.timeout(20):
                    await asyncio.shield(waiter)
            except TimeoutError:
                return {'status': 'invalid'}
            current = await self._blocking_work.run(self._read_session, before.snapshot.session_id)
            identity_live = (None if current is None else current.snapshot.store.matches_identity_memory(
                current.snapshot.session_id,
                current.snapshot.user_id,
                current.snapshot.created_at,
            ))
            if (self._callers.get(identity.slot_id) != identity or current is None
                or identity_live is not True or current.snapshot.epoch != epoch
                    or not self.registry.registered(identity, server_id, credential_ref)
                    or not self.registry.active(credential_ref, epoch)):
                return {'status': 'invalid'}
            token_live = current.snapshot.store.matches_identity_memory(
                current.snapshot.session_id,
                current.snapshot.user_id,
                current.snapshot.created_at,
                expected_id_token=current.credential.id_token,
            )
            if token_live is None:
                raise ControlServiceBusy()
            if token_live is not True:
                return {'status': 'invalid'}
            # A refresh is not a new consumer. A re-login is observed only by
            # successful issuance; never relabel this in-flight result as new.
            try:
                revision = self.registry.revision(credential_ref, epoch)
            except ValueError:
                return {'status': 'invalid'}
            update = ({'credential_ref': credential_ref, 'revoked': True}
                      if current.credential.expired else
                      {'credential_ref': credential_ref,
                       'api_key': _text(current.credential.id_token, 4096)})
        operation_id = self.registry.operation(
            CredentialTarget(identity, server_id, credential_ref), revision,
            revoked=update.get('revoked') is True,
        )
        revision = self.registry.operations[operation_id].cutoff_revision
        return {'status': 'ok', 'operation_id': operation_id, 'server_id': server_id,
                'ref_revision': revision, 'update': update}

    async def logout(self, auth_session: str | None) -> None:
        """Delete locally first; wait only for bounded local invalidation ACKs."""
        service = self._service()
        before = await self._blocking_work.run(self._read_session, auth_session)
        if before is None:
            return
        snapshot = before.snapshot
        identity_live = snapshot.store.matches_identity_memory(
            snapshot.session_id,
            snapshot.user_id,
            snapshot.created_at,
        )
        if identity_live is None:
            raise ControlServiceBusy()
        if identity_live is not True:
            return
        credential_ref = snapshot.credential_ref
        epoch = snapshot.epoch
        cutoff = self.registry.begin_logout(credential_ref, epoch)
        # Materialize reserved operations before yielding: a concurrent login
        # must not reuse an old consumer's unspent reservation.
        operations = [(target, self.registry.operation(target, cutoff, revoked=True))
                      for target in self.registry.targets(credential_ref, cutoff)]
        await self._blocking_work.run(service.logout, snapshot.session_id)
        self.registry.finish_logout(credential_ref, epoch)
        if not operations:
            return
        deadline = asyncio.get_running_loop().time() + 2.0
        # Four fixed workers share one deadline; no task per consumer and no
        # batch-by-batch renewal of the HTTP wait budget.
        iterator = iter(operations)

        async def notify_targets() -> None:
            for target, operation_id in iterator:
                if asyncio.get_running_loop().time() >= deadline:
                    return
                operation = self.registry.operations.get(operation_id)
                if operation is None or operation.outcome is not None:
                    continue
                endpoint = self._endpoints.get(target.identity)
                if endpoint is None:
                    operation.local_ack = 'unavailable'
                    continue
                try:
                    await endpoint.notify('credentials.invalidate', {
                        'operation_id': operation_id, 'server_id': target.server_id,
                        'credential_ref': credential_ref, 'cutoff_revision': operation.cutoff_revision,
                    }, timeout=max(0.001, deadline - asyncio.get_running_loop().time()))
                    operation.local_ack = 'ok'
                except asyncio.CancelledError:
                    operation.local_ack = 'unknown'
                    raise
                except ControlServiceBusy:
                    operation.local_ack = 'busy'
                except Exception:
                    operation.local_ack = 'unknown'

        tasks = [asyncio.create_task(notify_targets(), name='credential-logout-notify')
                 for _ in range(min(4, len(operations)))]
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.gather(*tasks)
        except TimeoutError:
            pass
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def request_model_auth(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'auth_session', 'model_name'})
        raw_session = arguments['auth_session']
        auth_session = None if raw_session is None else _text(raw_session, 256, empty=True).strip() or None
        model_name = _text(arguments['model_name'], 256, empty=True).strip()
        if not auth_session or not model_name:
            return {'status': 'not_required'}
        result = await self._credentials.request_model_auth_with_snapshot(auth_session, model_name)
        if result is None or not result.model_required:
            return {'status': 'not_required'}
        if result.credential is None:
            return {'status': 'invalid'}
        return _credential(result.credential)

    async def remember(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'session_id', 'credential_ref', 'model_name'})
        session_id = _text(arguments['session_id'], 256)
        credential_ref = _ref(arguments['credential_ref'])
        model_name = _text(arguments['model_name'], 256)
        await self._bindings.remember(session_id, credential_ref, model_name)
        return {'status': 'ok'}

    async def lookup(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'session_id'})
        binding = await self._bindings.lookup(_text(arguments['session_id'], 256))
        if binding is None:
            return {'status': 'not_found'}
        return {'status': 'ok', 'binding': {
            'credential_ref': _ref(binding.credential_ref), 'model_name': _text(binding.model_name, 256),
        }}

    async def resolve_model_auth(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _fields(arguments, {'session_id', 'credential_ref', 'model_name'})
        session_id = _text(arguments['session_id'], 256)
        credential_ref = _ref(arguments['credential_ref'])
        model_name = _text(arguments['model_name'], 256)
        binding = await self._bindings.lookup(session_id)
        # Check the current persisted binding, including TTL, rather than
        # accepting an arbitrary reference or a stale lookup result.
        if binding is None or (binding.credential_ref, binding.model_name) != (credential_ref, model_name):
            return {'status': 'invalid'}
        return _credential(await self._bindings.model_auth_for_binding(binding))
