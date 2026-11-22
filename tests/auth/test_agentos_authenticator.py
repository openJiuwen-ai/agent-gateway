"""测试 agentos_authenticator.py"""
import asyncio
import time
from contextlib import asynccontextmanager

import httpx
import pytest

# 然后再导入测试目标
from agent_gateway.extensions.agentos.agentos_router import (
    agentos_authenticator as auth_module,
)
from agent_gateway.extensions.agentos.agentos_router.agentos_authenticator import (
    AgentOSAuthenticator,
)
from agent_gateway.extensions.auth.credential_authenticator import (
    AuthContext,
    AuthResult,
    CredentialAuthenticator,
)


class TestAgentOSAuthenticatorInit:

    def test_init_with_required_params(self):
        """验证必填参数初始化"""
        auth = AgentOSAuthenticator(auth_service_url="http://localhost:8000")
        assert auth._auth_service_url == "http://localhost:8000"
        assert auth._timeout == 8.0

    def test_init_strips_trailing_slash(self):
        """验证初始化时去除尾部斜杠"""
        auth = AgentOSAuthenticator(auth_service_url="http://localhost:8000/")
        assert auth._auth_service_url == "http://localhost:8000"

    def test_init_with_custom_timeout(self):
        """验证自定义 timeout"""
        auth = AgentOSAuthenticator(auth_service_url="http://localhost:8000", timeout=30.0)
        assert auth._timeout == 8.0

    def test_init_creates_async_client(self):
        """验证初始化时创建了 AsyncClient"""
        auth = AgentOSAuthenticator(auth_service_url="http://localhost:8000")
        assert isinstance(auth._auth_client, httpx.AsyncClient)

    def test_is_credential_authenticator(self):
        """验证 AgentOSAuthenticator 是 CredentialAuthenticator 的子类"""
        auth = AgentOSAuthenticator(auth_service_url="http://localhost:8000")
        assert isinstance(auth, CredentialAuthenticator)


class TestAuthenticateToken:

    @pytest.fixture
    def auth(self):
        return AgentOSAuthenticator(auth_service_url="http://test-auth:8000")

    # ── 空 token 场景 ──

    @pytest.mark.asyncio
    async def test_empty_token(self, auth):
        """验证空 token 返回 MISSING_TOKEN"""
        result = await auth._authenticate_token("")
        assert result.success is False
        assert result.error == "缺少 token"
        assert result.extensions.get("error_code") == "MISSING_TOKEN"

    @pytest.mark.asyncio
    async def test_none_token(self, auth):
        """验证 None token 返回 MISSING_TOKEN"""
        result = await auth._authenticate_token(None)  # type: ignore
        assert result.success is False
        assert result.error == "缺少 token"

    @pytest.mark.asyncio
    async def test_whitespace_token(self, auth):
        """验证空白 token 返回 MISSING_TOKEN"""
        result = await auth._authenticate_token("   ")
        assert result.success is False
        assert result.error == "缺少 token"

class TestAuthenticate:

    @pytest.fixture
    def auth(self):
        return AgentOSAuthenticator(auth_service_url="http://test-auth:8000")

    @pytest.mark.asyncio
    async def test_no_credentials(self, auth):
        """验证无凭证时返回 UNSUPPORTED_CREDENTIAL"""
        context = AuthContext(channel_type="web", credentials={})
        result = await auth.authenticate(context)
        assert result.success is False
        assert result.error == "No valid credentials"
        assert result.extensions.get("error_code") == "UNSUPPORTED_CREDENTIAL"

    @pytest.mark.asyncio
    async def test_none_credentials(self, auth):
        """验证 credentials 为 None 时返回 UNSUPPORTED_CREDENTIAL"""
        context = AuthContext(channel_type="web", credentials=None)  # type: ignore
        result = await auth.authenticate(context)
        assert result.success is False
        assert result.error == "No valid credentials"

@asynccontextmanager
async def _auth_with_transport(handler):
    auth = AgentOSAuthenticator(auth_service_url="http://test-auth:8000")
    await auth._auth_client.aclose()
    auth._auth_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        yield auth
    finally:
        await auth._auth_client.aclose()


def _valid_response():
    return httpx.Response(
        200,
        json={"data": {"valid": True, "user_id": "u1", "username": "alice"}},
    )


class TestAuthRetryBehavior:

    @pytest.fixture(autouse=True)
    def _fast_backoff(self, monkeypatch):
        # Keep the retry structure but avoid real 0.2/0.4 s waits in tests.
        monkeypatch.setattr(auth_module, "jittered_delay", lambda base: 0.001)

    @pytest.mark.asyncio
    async def test_network_error_retries_then_succeeds(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                raise httpx.ConnectError("connection reset", request=request)
            return _valid_response()

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is True
        assert result.user_id == "u1"
        assert calls["n"] == 2

    @pytest.mark.asyncio
    async def test_5xx_returns_iam_unavailable_after_retries(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(503, json={"error": "maintenance"})

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "IAM_UNAVAILABLE"
        assert calls["n"] == 3

    @pytest.mark.asyncio
    async def test_valid_false_is_never_retried(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(
                200, json={"data": {"valid": False, "error": "expired"}},
            )

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "INVALID_TOKEN"
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_invalid_json_retries_once(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(200, text="not-json")

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "INVALID_RESPONSE"
        assert calls["n"] == 2

    @pytest.mark.asyncio
    async def test_429_honors_retry_after(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return _valid_response()

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is True
        assert calls["n"] == 2

    @pytest.mark.asyncio
    async def test_429_beyond_budget_fails_without_retry(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(429, headers={"Retry-After": "999"})

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "IAM_UNAVAILABLE"
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_timeout_is_not_retried(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            raise httpx.ReadTimeout("slow", request=request)

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "IAM_UNAVAILABLE"
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_total_budget_is_a_hard_limit(self, monkeypatch):
        auth = AgentOSAuthenticator(
            auth_service_url="http://test-auth:8000", timeout=0.2,
        )
        calls = {"n": 0}

        async def slow_post(*args, **kwargs):
            calls["n"] += 1
            await asyncio.sleep(1.0)
            return _valid_response()

        monkeypatch.setattr(auth._auth_client, "post", slow_post)
        started = time.monotonic()
        try:
            result = await auth._authenticate_token("token")
        finally:
            await auth._auth_client.aclose()
        elapsed = time.monotonic() - started

        assert result.success is False
        assert result.extensions.get("error_code") == "IAM_UNAVAILABLE"
        assert calls["n"] == 1
        assert elapsed < 0.9

    @pytest.mark.asyncio
    async def test_401_is_invalid_token_without_retry(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(401, json={"error": "bad token"})

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "INVALID_TOKEN"
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_200_without_valid_field_is_invalid_response(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(200, json={"data": {"ok": True}})

        async with _auth_with_transport(handler) as auth:
            result = await auth._authenticate_token("token")

        assert result.success is False
        assert result.extensions.get("error_code") == "INVALID_RESPONSE"
        assert calls["n"] == 1
