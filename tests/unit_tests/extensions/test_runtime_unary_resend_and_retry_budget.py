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
