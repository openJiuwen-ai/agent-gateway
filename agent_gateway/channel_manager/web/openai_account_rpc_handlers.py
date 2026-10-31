# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""OpenAI account RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from openjiuwen.extensions.external_provider.openai_auth.openai_account_auth import (
    OpenAIAccountAuthError,
    OpenAIAccountAuthManager,
    OpenAIAccountDeviceCode,
)
from openjiuwen.extensions.external_provider.openai_auth.openai_account_models import (
    OpenAIAccountModelCatalog,
    OpenAIAccountModelListError,
)


logger = logging.getLogger(__name__)

_OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS = 5 * 60
_OPENAI_ACCOUNT_LOGIN_JOBS: dict[str, "_OpenAIAccountLoginJob"] = {}
_OPENAI_ACCOUNT_LOGIN_JOBS_LOCK = threading.RLock()
_OPENAI_ACCOUNT_AUTH_OPERATION_LOCK = threading.RLock()
_OPENAI_ACCOUNT_LOCAL_ERRORS = (OSError, TypeError, ValueError)


@dataclass
class _OpenAIAccountLoginJob:
    device_code: OpenAIAccountDeviceCode
    created_at: float
    expires_at: float


def _cleanup_openai_account_login_jobs(now: float | None = None) -> None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        expired = [
            login_id
            for login_id, job in _OPENAI_ACCOUNT_LOGIN_JOBS.items()
            if job.expires_at <= current
        ]
        for login_id in expired:
            _OPENAI_ACCOUNT_LOGIN_JOBS.pop(login_id, None)


def _latest_openai_account_login_job(
        now: float | None = None,
) -> tuple[str, "_OpenAIAccountLoginJob"] | None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        if not _OPENAI_ACCOUNT_LOGIN_JOBS:
            return None
        return max(_OPENAI_ACCOUNT_LOGIN_JOBS.items(), key=lambda item: item[1].created_at)


def _get_openai_account_login_job(
        login_id: str,
        now: float | None = None,
) -> "_OpenAIAccountLoginJob" | None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        return _OPENAI_ACCOUNT_LOGIN_JOBS.get(login_id)


def _store_openai_account_login_job(
        login_id: str,
        job: "_OpenAIAccountLoginJob",
        now: float | None = None,
) -> None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        _OPENAI_ACCOUNT_LOGIN_JOBS[login_id] = job


def _remove_openai_account_login_job(login_id: str) -> None:
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _OPENAI_ACCOUNT_LOGIN_JOBS.pop(login_id, None)


def _clear_openai_account_login_jobs() -> None:
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _OPENAI_ACCOUNT_LOGIN_JOBS.clear()


def _openai_account_auth_status_payload(
        manager: OpenAIAccountAuthManager | None = None,
) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        auth_manager = manager or OpenAIAccountAuthManager()
        status = auth_manager.status()
        return {
            "authenticated": status.authenticated,
            "auth_path": str(status.auth_path),
            "has_refresh_token": status.has_refresh_token,
            "expires_at": status.expires_at,
            "needs_refresh": status.needs_refresh,
            "error": status.error,
            "base_url": auth_manager.base_url,
        }


def _openai_account_login_payload(
        login_id: str,
        job: "_OpenAIAccountLoginJob",
        manager: OpenAIAccountAuthManager | None = None,
        now: float | None = None,
) -> dict[str, Any]:
    current = time.time() if now is None else now
    device_code = job.device_code
    return {
        "status": "pending",
        "login_id": login_id,
        "user_code": device_code.user_code,
        "verification_uri": device_code.verification_uri,
        "interval": device_code.interval,
        "expires_in": max(0, int(job.expires_at - current)),
        "expires_at": job.expires_at,
        "auth": _openai_account_auth_status_payload(manager),
    }


def _openai_account_pending_login_payload(
        manager: OpenAIAccountAuthManager | None = None,
) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        current = time.time()
        latest_job = _latest_openai_account_login_job(current)
        if latest_job is None:
            return {
                "status": "none",
                "auth": _openai_account_auth_status_payload(manager),
            }
        login_id, job = latest_job
        return _openai_account_login_payload(login_id, job, manager, current)


def _openai_account_start_login_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        now = time.time()
        latest_job = _latest_openai_account_login_job(now)
        if latest_job is not None:
            login_id, job = latest_job
            return _openai_account_login_payload(login_id, job, manager, now)

        device_code = manager.start_device_login()
        now = time.time()
        raw_expires_in = device_code.expires_in or _OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS
        expires_in = min(int(raw_expires_in), _OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS)
        expires_at = now + expires_in
        login_id = uuid.uuid4().hex
        job = _OpenAIAccountLoginJob(
            device_code=device_code,
            created_at=now,
            expires_at=expires_at,
        )
        _store_openai_account_login_job(login_id, job, now)
        return _openai_account_login_payload(login_id, job, manager, now)


def _openai_account_poll_login_payload(login_id: str) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        now = time.time()
        job = _get_openai_account_login_job(login_id, now)
        if job is None:
            return {"status": "expired", "authenticated": False}

        manager = OpenAIAccountAuthManager()
        try:
            tokens = manager.poll_device_login(job.device_code)
        except OpenAIAccountAuthError as exc:
            if exc.relogin_required:
                _remove_openai_account_login_job(login_id)
            raise
        if tokens is None:
            return {
                "status": "pending",
                "authenticated": False,
                "expires_at": job.expires_at,
            }

        _remove_openai_account_login_job(login_id)
        return {
            "status": "authenticated",
            "authenticated": True,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_logout_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        _clear_openai_account_login_jobs()
        logged_out = manager.logout()
        return {
            "logged_out": logged_out,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_models_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        catalog = OpenAIAccountModelCatalog(base_url=manager.base_url)
        models = catalog.list_model_ids(auth_manager=manager)
        return {
            "models": models,
            "base_url": manager.base_url,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_auth_error_payload(exc: OpenAIAccountAuthError) -> dict[str, Any]:
    return {
        "status": "error",
        "error": str(exc),
        "code": exc.code,
        "status_code": exc.status_code,
        "relogin_required": exc.relogin_required,
    }


def register_openai_account_handlers(channel: Any) -> None:
    """Register OpenAI account RPC handlers in their original route order."""

    async def _openai_account_auth_status(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_auth_status_payload)
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.status] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.status] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_start_login(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_start_login_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.start_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.start_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_pending_login(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_pending_login_payload)
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.pending_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.pending_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_poll_login(ws, req_id, params, session_id):
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        login_id = str(params.get("login_id") or "").strip()
        if not login_id:
            await channel.send_response(ws, req_id, ok=False, error="login_id is required", code="BAD_REQUEST")
            return
        try:
            payload = await asyncio.to_thread(_openai_account_poll_login_payload, login_id)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.poll_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.poll_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_logout(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_logout_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.logout] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.logout] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_models_list(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_models_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except OpenAIAccountModelListError as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="MODEL_LIST_ERROR")
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    channel.register_method("openai_account.auth.status", _openai_account_auth_status)
    channel.register_method("openai_account.auth.start_login", _openai_account_auth_start_login)
    channel.register_method("openai_account.auth.pending_login", _openai_account_auth_pending_login)
    channel.register_method("openai_account.auth.poll_login", _openai_account_auth_poll_login)
    channel.register_method("openai_account.auth.logout", _openai_account_auth_logout)
    channel.register_method("openai_account.models.list", _openai_account_models_list)
