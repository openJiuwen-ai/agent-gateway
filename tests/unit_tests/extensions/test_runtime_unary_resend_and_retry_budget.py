# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""unary 重发与路由重试预算的回归测试。

通过内存替身验证业务请求不会被不安全地重发，路由重试也不会超出预算。
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from agent_gateway.extensions.runtime_management_extension import runtime_routed_client as runtime_client_module
from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import (
    RuntimeRoutedAgentClient,
)
from agent_gateway.extensions.runtime_management_extension.session_route_client import (
    RetryableRouteError,
)


class _FakeRoute:
    """固定返回递增的 Pod 地址，记录被调用次数。"""

    def __init__(self, *, retry_after: float | None = None, always_fail: bool = False) -> None:
        self.calls: list[str] = []
        self._retry_after = retry_after
        self._always_fail = always_fail

    async def route(self, **kwargs):
        self.calls.append(str(kwargs.get("request_id")))
        if self._always_fail:
            raise RetryableRouteError(
                "overloaded", code="TRANSPORT", retry_after=self._retry_after
            )
        index = len(self.calls)
        return type(
            "_Result",
            (),
            {
                "pod_sse_url": f"http://pod{index}:8766/api/v1/sse",
                "request_id": kwargs.get("request_id"),
            },
        )()

    async def touch(self, **kwargs):
        return True


class _FakeHTTP:
    """第一次调用抛指定异常，之后成功；记录每次的目标地址。"""

    def __init__(self, first_error: BaseException | None) -> None:
        self.sent: list[str | None] = []
        self._first_error = first_error

    async def send_request(self, envelope, *, base_url=None, timeout=None):
        self.sent.append(base_url)
        if len(self.sent) == 1 and self._first_error is not None:
            raise self._first_error
        return type(
            "_Response",
            (),
            {"ok": True, "payload": {}, "request_id": envelope.request_id},
        )()


class _Envelope:
    """够 identity_from_envelope 用的最小信封。"""

    def __init__(self, method: str) -> None:
        self.method = method
        self.params: dict = {}
        self.session_id = "S1"
        self.channel_context = {"routing": {"group_id": "g", "bot_id": "b"}, "user_id": "u"}
        self.user_id = "u"
        self.request_id = "RID"
        self.chat_id = None
        self.agent_ref = None
        self.is_stream = False
        self.agent_id = None


@pytest.fixture()
def patched_identity(monkeypatch):
    """跳过真实准入与租户 ID 加工，聚焦在本文件要验的行为。"""

    async def _authorize(_envelope):
        return ("g", "b", "u")

    monkeypatch.setattr(runtime_client_module, "authorize_agent", _authorize)
    monkeypatch.setattr(
        runtime_client_module, "apply_invoke_ids_to_envelope", lambda envelope: envelope
    )
    return None


def _make_client(route: _FakeRoute, http: _FakeHTTP) -> RuntimeRoutedAgentClient:
    client = RuntimeRoutedAgentClient(route_client=route, http_client=http)
    client._connected = True
    return client


# --------------------------------------------------------------------------- #
# 07：unary 跨 Pod 重发
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "error",
    [httpx.ReadError("response lost"), httpx.ReadTimeout("slow")],
    ids=["read-error", "read-timeout"],
)
def test_non_idempotent_method_is_not_resent_when_response_may_be_lost(
    patched_identity, error
):
    """ReadError/ReadTimeout 说明请求可能已被执行，session.create 不能再发一次。"""
    route, http = _FakeRoute(), _FakeHTTP(error)
    client = _make_client(route, http)

    with pytest.raises(type(error)):
        asyncio.run(client.send_request(_Envelope("session.create")))

    assert http.sent == ["http://pod1:8766"], "只应发往第一个 Pod，不应重发"


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("refused"), httpx.ConnectTimeout("timeout")],
    ids=["connect-error", "connect-timeout"],
)
def test_connect_failures_are_always_resent(patched_identity, error):
    """建连阶段失败可证明 Agent 没收到字节，任何方法都可以换 Pod 重发。"""
    route, http = _FakeRoute(), _FakeHTTP(error)
    client = _make_client(route, http)

    asyncio.run(client.send_request(_Envelope("session.create")))

    assert http.sent == ["http://pod1:8766", "http://pod2:8766"]


@pytest.mark.parametrize("retry_after", [86400, 5])
def test_route_does_not_retry_when_wait_exceeds_limit_or_budget(retry_after):
    route = _FakeRoute(retry_after=retry_after, always_fail=True)
    client = _make_client(route, _FakeHTTP(None))

    with pytest.raises(RetryableRouteError) as caught:
        asyncio.run(client._route_with_retry(
            session_id="S1", group_id="g", bot_id="b", request_id="RID",
            user_id="u", budget_seconds=1.0,
        ))

    assert caught.value.retry_after == retry_after
    assert route.calls == ["RID"]


def test_route_budget_bounds_an_in_flight_request(monkeypatch):
    route = _FakeRoute()
    client = _make_client(route, _FakeHTTP(None))
    cancelled = False

    async def stall(**_kwargs):
        nonlocal cancelled
        try:
            await asyncio.Future()
        finally:
            cancelled = True

    monkeypatch.setattr(route, "route", stall)
    with pytest.raises(RetryableRouteError, match="route deadline exceeded"):
        asyncio.run(client._route_with_retry(
            session_id="S1", group_id="g", bot_id="b", request_id="RID",
            user_id="u", budget_seconds=0.01,
        ))
    assert cancelled


# --------------------------------------------------------------------------- #
# PR258 review：重发策略分档、发送前失败、预算错误透传
# --------------------------------------------------------------------------- #

def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://pod1/api/v1/chat/completions")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("status", request=request, response=response)


@pytest.mark.parametrize("status", [429, 503, 504])
def test_server_side_rejection_is_resendable_for_any_method(status):
    """服务端明确没执行（限流/网关层不可用）时，非幂等方法也可重发。"""
    assert runtime_client_module._may_resend(_status_error(status), "session.create") is True


@pytest.mark.parametrize("status", [500, 502])
def test_ambiguous_server_error_is_not_resendable_for_create(status):
    """5xx 不一定没执行，非幂等方法不重发。"""
    assert runtime_client_module._may_resend(_status_error(status), "session.create") is False


def test_read_method_is_resendable_on_read_error():
    assert runtime_client_module._may_resend(
        httpx.ReadError("lost"), "history.get"
    ) is True


def test_non_idempotent_method_is_not_resendable_on_read_error():
    assert runtime_client_module._may_resend(
        httpx.ReadError("lost"), "session.create"
    ) is False


def test_push_setup_failure_is_not_treated_as_maybe_executed(patched_identity):
    """建推送通道失败时业务请求还没发出，应走重试而不是被「可能已执行」拦下。"""
    route = _FakeRoute()
    http = _FakeHTTP(None)
    client = _make_client(route, http)

    async def fail_push(_base_url):
        raise httpx.ConnectError("push channel down")

    client._ensure_pod_push = fail_push

    with pytest.raises(httpx.ConnectError):
        asyncio.run(client.send_request(_Envelope("session.create")))
    # 没有业务请求被发出，也没有被误判成「已执行」的日志路径
    assert http.sent == []


def test_budget_exhaustion_re_raises_last_retryable_error(monkeypatch):
    """预算到点时应保留服务端给的 retry_after，而不是换成通用错误。"""

    async def _no_sleep(_delay):
        return None

    monkeypatch.setattr(runtime_client_module.asyncio, "sleep", _no_sleep)
    route = _FakeRoute(retry_after=1.0, always_fail=True)
    client = _make_client(route, _FakeHTTP(None))

    # 单次 route 调用挂住超过预算，让 timeout_at 先于重试逻辑触发
    inner = runtime_client_module.RetryableRouteError(
        "overloaded", code="TRANSPORT", retry_after=1.0
    )

    # 先让 route 抛一次可重试错误（记录 last_error），再让下一次挂住
    calls = {"n": 0}

    async def fail_then_stall(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise inner
        await asyncio.sleep(10)

    route.route = fail_then_stall

    with pytest.raises(RetryableRouteError) as caught:
        asyncio.run(
            client._route_with_retry(
                session_id="S1", group_id="g", bot_id="b", request_id="RID",
                user_id="u", budget_seconds=0.05,
            )
        )
    # 抛出的是服务端原始错误（带 retry_after），而不是通用的 deadline 错误
    assert caught.value is inner
    assert caught.value.retry_after == 1.0
    assert "deadline" not in str(caught.value)
