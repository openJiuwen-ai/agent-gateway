from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from agent_gateway.common.retry import jittered_delay
from agent_gateway.extensions.agentos.agentos_router.logutil import log_agentos
from agent_gateway.extensions.auth.credential_authenticator import (
    AuthContext,
    AuthResult,
    CredentialAuthenticator,
)

logger = logging.getLogger(__name__)

#: The Web/TUI handshake has a 10 s hard limit (websockets library default).
#: Leave room for the rest of the handshake; auth must finish within 8 s.
_AUTH_MAX_BUDGET_SECONDS = 8.0
_AUTH_MAX_ATTEMPTS = 3
_AUTH_BACKOFF_INITIAL_SECONDS = 0.2
_AUTH_BACKOFF_FACTOR = 2.0


class AgentOSAuthenticator(CredentialAuthenticator):
    """通过 agent-os IAM ``POST /api/v1/auth/verify`` 校验 token。
    门禁只依据响应中的 ``valid``（身份是否合法），不消费 ``authorized``
    （资源级授权留给后续能力）。
    成功时 ``AuthResult`` 字段约定（供后续会话路由 / 注册中心 / 实例创建贯通）：
    - ``user_id``: IAM 返回的用户 ID（权威身份）
    - ``extensions.username``: 用户名（可选）
    - ``extensions.role``: 角色（可选）
    - ``extensions.auth_method``: 固定为 ``"token"``
    """

    def __init__(self, auth_service_url: str,  # agent-os 后端地址，例如 "http://localhost:8000"
                 timeout: float = 10.0):  # 认证总预算秒数（含重试），上限 8 秒
        self._auth_service_url = auth_service_url.rstrip("/")
        # The configured value is a *total* budget, not a single-request
        # timeout. The Web/TUI handshake has a 10 s hard limit, so auth must
        # finish well before that even when retries are used.
        self._timeout = max(0.1, min(float(timeout), _AUTH_MAX_BUDGET_SECONDS))
        self._auth_client = httpx.AsyncClient(timeout=self._timeout)

    @staticmethod
    def _failure(error: str, code: str) -> AuthResult:
        return AuthResult(
            success=False,
            user_id="",
            error=error,
            extensions={"error_code": code},
        )

    @staticmethod
    def _remaining(deadline: float) -> float:
        return deadline - asyncio.get_running_loop().time()

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        text = raw.strip()
        if not text:
            return None
        try:
            return max(0.0, float(text))
        except ValueError:
            pass
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())

    def _log_retry(
        self,
        *,
        attempt: int,
        error: str,
        status: object = "",
        retry_in: float | None = None,
        final: bool = False,
    ) -> None:
        fields: dict[str, object] = {"error": error, "attempt": attempt}
        if status != "":
            fields["status"] = status
        if retry_in is not None:
            fields["retry_in"] = f"{retry_in:.2f}s"
        if final:
            fields["final"] = "yes"
        log_agentos(logger, logging.WARNING, "auth.iam_error", **fields)

    async def _authenticate_token(self, token: str,
                                  extra_headers: dict | None = None
                                  ) -> AuthResult:

        # 1. 如果 extra_headers 中有 Authorization header，优先从中提取 token, 如果token为空直接返回失败
        if extra_headers:
            lower_headers = {k.lower(): v for k, v in extra_headers.items()}
            auth_header = lower_headers.get("authorization", "")
            # header拿不到，降级使用传入的token参数
            if not auth_header:
                auth_header = token
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]  # 覆盖 token 参数

        token = (token or "").strip()
        if not token:
            return AuthResult(
                success=False,
                user_id="",
                error="缺少 token",
                extensions={"error_code": "MISSING_TOKEN"},
            )

        # 2. 构建 HTTP 验证：合并自定义 header，只验证身份是否合法，不验证资源权限
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        # 不要把 WS 握手头（Host/Upgrade/authorization 等）原样并入 IAM 请求，
        # 否则可能产生重复 Authorization 或错误 Host，导致 verify 失败。
        deadline = asyncio.get_running_loop().time() + self._timeout
        backoff = _AUTH_BACKOFF_INITIAL_SECONDS
        json_retry_used = False
        attempt = 0
        while attempt < _AUTH_MAX_ATTEMPTS:
            remaining = self._remaining(deadline)
            if remaining <= 0:
                self._log_retry(attempt=attempt, error="budget_exhausted", final=True)
                return self._failure("登录服务响应超时", "IAM_UNAVAILABLE")
            attempt += 1
            try:
                # httpx applies a float timeout to connect/read/write/pool
                # separately, so it is not an end-to-end limit. Bound the whole
                # attempt with the remaining auth budget instead.
                async with asyncio.timeout(max(0.001, remaining)):
                    resp = await self._auth_client.post(
                        f"{self._auth_service_url}/api/v1/auth/verify",
                        json={"token": token, "resource_id": "apps", "action_id": "read"},
                        headers=headers,
                        timeout=max(0.001, remaining),
                    )
            except (TimeoutError, httpx.TimeoutException) as exc:
                # The per-request timeout consumes the remaining budget; a
                # second request cannot finish within the handshake limit.
                self._log_retry(
                    attempt=attempt,
                    error=type(exc).__name__,
                    final=True,
                )
                return self._failure(str(exc) or "登录服务响应超时", "IAM_UNAVAILABLE")
            except httpx.RequestError as exc:
                if attempt >= _AUTH_MAX_ATTEMPTS:
                    self._log_retry(
                        attempt=attempt,
                        error=type(exc).__name__,
                        final=True,
                    )
                    return self._failure(str(exc), "IAM_UNAVAILABLE")
                delay = jittered_delay(backoff)
                if delay <= 0 or delay >= self._remaining(deadline):
                    self._log_retry(
                        attempt=attempt,
                        error=type(exc).__name__,
                        final=True,
                    )
                    return self._failure(str(exc), "IAM_UNAVAILABLE")
                self._log_retry(
                    attempt=attempt,
                    error=type(exc).__name__,
                    retry_in=delay,
                )
                await asyncio.sleep(delay)
                backoff *= _AUTH_BACKOFF_FACTOR
                continue

            status = int(getattr(resp, "status_code", 0) or 0)
            if status == 429:
                wait = self._retry_after_seconds(resp)
                remaining = self._remaining(deadline)
                if (
                    attempt >= _AUTH_MAX_ATTEMPTS
                    or wait is None
                    or wait >= remaining
                ):
                    self._log_retry(
                        attempt=attempt,
                        error="RATE_LIMITED",
                        status=status,
                        final=True,
                    )
                    return self._failure("登录服务限流，请稍后重试", "IAM_UNAVAILABLE")
                self._log_retry(
                    attempt=attempt,
                    error="RATE_LIMITED",
                    status=status,
                    retry_in=wait,
                )
                await asyncio.sleep(wait)
                continue

            if status >= 500:
                if attempt >= _AUTH_MAX_ATTEMPTS:
                    self._log_retry(
                        attempt=attempt,
                        error="IAM_UNAVAILABLE",
                        status=status,
                        final=True,
                    )
                    return self._failure(
                        f"登录服务暂时不可用 (HTTP {status})", "IAM_UNAVAILABLE",
                    )
                delay = jittered_delay(backoff)
                if delay <= 0 or delay >= self._remaining(deadline):
                    self._log_retry(
                        attempt=attempt,
                        error="IAM_UNAVAILABLE",
                        status=status,
                        final=True,
                    )
                    return self._failure(
                        f"登录服务暂时不可用 (HTTP {status})", "IAM_UNAVAILABLE",
                    )
                self._log_retry(
                    attempt=attempt,
                    error="IAM_UNAVAILABLE",
                    status=status,
                    retry_in=delay,
                )
                await asyncio.sleep(delay)
                backoff *= _AUTH_BACKOFF_FACTOR
                continue

            if status in (401, 403):
                return self._failure("Token 无效或已过期", "INVALID_TOKEN")
            if not 200 <= status < 300:
                self._log_retry(
                    attempt=attempt,
                    error="INVALID_RESPONSE",
                    status=status,
                    final=True,
                )
                return self._failure(
                    f"登录服务返回异常状态 (HTTP {status})", "INVALID_RESPONSE",
                )

            try:
                body = resp.json()
            except ValueError:
                if (
                    not json_retry_used
                    and attempt < _AUTH_MAX_ATTEMPTS
                ):
                    delay = jittered_delay(backoff)
                    if delay > 0 and delay < self._remaining(deadline):
                        json_retry_used = True
                        self._log_retry(
                            attempt=attempt,
                            error="INVALID_RESPONSE",
                            status=status,
                            retry_in=delay,
                        )
                        await asyncio.sleep(delay)
                        backoff *= _AUTH_BACKOFF_FACTOR
                        continue
                self._log_retry(
                    attempt=attempt,
                    error="INVALID_RESPONSE",
                    status=status,
                    final=True,
                )
                return self._failure("登录服务返回了无效响应", "INVALID_RESPONSE")

            data = body.get("data", {}) if isinstance(body, dict) else {}
            if not isinstance(data, dict):
                data = {}

            # 3. 仅校验身份合法性（valid）；authorized 不参与门禁决策
            if data.get("valid"):
                return AuthResult(
                    success=True,
                    user_id=str(data.get("user_id") or ""),
                    extensions={
                        "username": data.get("username"),
                        "role": data.get("role"),
                        "auth_method": "token",
                    },
                )
            if "valid" in data:
                return self._failure(
                    str(data.get("error") or "Token 无效或已过期"),
                    "INVALID_TOKEN",
                )
            self._log_retry(
                attempt=attempt,
                error="INVALID_RESPONSE",
                status=status,
                final=True,
            )
            return self._failure("登录服务返回了无效响应", "INVALID_RESPONSE")

        return self._failure("登录服务暂时不可用", "IAM_UNAVAILABLE")

    async def authenticate(self, context: AuthContext) -> AuthResult:
        """支持Token、API-KEY、SSH证书等多种认证方式"""
        credentials = context.credentials or {}
        extra_headers = getattr(context, 'headers', None) or {}  # 从 context 取自定义 header

        # 1. Token认证（Web/TUI Channel）
        if "token" in credentials:
            return await self._authenticate_token(
                str(credentials.get("token") or ""),
                extra_headers,
            )

        return AuthResult(
            success=False,
            user_id="",
            error="No valid credentials",
            extensions={"error_code": "UNSUPPORTED_CREDENTIAL"},
        )