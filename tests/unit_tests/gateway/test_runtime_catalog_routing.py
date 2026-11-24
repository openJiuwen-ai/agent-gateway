# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Catalog reads must not occupy a runtime scope slot.

``session.list`` / catalog ``team.list`` / ``team.quota.get`` and the read-only
``project.*`` queries have no business session. Routing them used to pin the
synthesized ``group:bot:user`` key and consume one ``scope_concurrency`` slot
until the TTL expired, which under a small scope (e.g. 4) crowded out real
chats and finally made the catalog call itself fail with ``SCOPE_FULL``. These
tests pin the replacement behaviour: reuse a known Pod HTTP endpoint, or
discover one via a single route that is released immediately.

Real HTTP envelopes never carry an empty ``session_id``:
``bind_http_session`` always fills the top level with either the client
session or a synthesized affinity id (``shared_user_<user>`` for catalog RPCs,
``webhttp_<hex>`` otherwise). Session-sensitive cases (catalog vs scoped
``team.list``) therefore build envelopes through ``bind_http_session`` to pin
the exact production shape.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from unittest.mock import AsyncMock

from e2a.models import E2AEnvelope
from e2a.agent_models import AgentResponse
from agent_gateway.channel_manager.web.outbound import bind_http_session
from agent_gateway.enterprise.team.route_binding_store import (
    ROUTE_STATUS_ACTIVE,
    InMemoryTeamRouteBindingStore,
)
from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import (
    RuntimeRoutedAgentClient,
)
from agent_gateway.extensions.runtime_management_extension.session_route_client import (
    FatalRouteError,
    RouteResult,
)
from agent_gateway.enterprise.team.route import TeamRouteResolver


def _envelope(
    *,
    method: str,
    session_id: str = "",
    params: dict[str, Any] | None = None,
    request_id: str = "req-1",
) -> E2AEnvelope:
    request_params = dict(params or {})
    if session_id:
        request_params.setdefault("session_id", session_id)
    return E2AEnvelope(
        method=method,
        session_id=session_id or None,
        request_id=request_id,
        channel="web",
        user_id="u1",
        channel_context={"routing": {"group_id": "g1", "bot_id": "b1", "user_id": "u1"}},
        params=request_params,
    )


class _FakeRoute:
    def __init__(self, base_url: str = "http://route-pod.invalid:8086") -> None:
        self.base_url = base_url
        self.route_calls: list[dict[str, Any]] = []
        self.touch_calls: list[tuple[str, str]] = []
        self.rebind_calls: list[dict[str, Any]] = []
        self.route_error: Exception | None = None

    async def route(self, **kwargs: Any) -> RouteResult:
        self.route_calls.append(kwargs)
        if self.route_error is not None:
            raise self.route_error
        return RouteResult(
            pod_sse_url=f"{self.base_url}/sse",
            pod_id="pod-1",
            request_id=str(kwargs["request_id"]),
        )

    async def touch(self, *, session_id: str, request_id: str) -> bool:
        self.touch_calls.append((session_id, request_id))
        return True

    async def rebind(self, **kwargs: Any) -> str:
        self.rebind_calls.append(kwargs)
        return "evicted"

    async def aclose(self) -> None:
        return None


class _FakeHttp:
    def __init__(self) -> None:
        self.send_bases: list[str] = []
        self.send_params: list[dict[str, Any]] = []
        self.fail_bases: set[str] = set()
        self.responses: dict[str, AgentResponse] = {}

    async def send_request(self, envelope: E2AEnvelope, *, base_url: str, timeout: float | None = None) -> AgentResponse:
        self.send_bases.append(base_url)
        self.send_params.append(dict(envelope.params or {}))
        if base_url in self.fail_bases:
            raise httpx.ConnectError("pod gone")
        canned = self.responses.get(base_url)
        if canned is not None:
            return canned
        return AgentResponse(
            request_id=str(envelope.request_id or ""),
            channel_id="web",
            ok=True,
            payload={"sessions": []},
        )

    async def disconnect(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _authorized(monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.extensions.runtime_management_extension.runtime_routed_client.authorize_agent",
        AsyncMock(return_value=("g1", "b1", "u1")),
    )


def _client(*, monkeypatch, route: _FakeRoute | None = None, http: _FakeHttp | None = None):
    route = route or _FakeRoute()
    http = http or _FakeHttp()
    client = RuntimeRoutedAgentClient(
        route_client=route,
        http_client=http,
        team_route_resolver=TeamRouteResolver(store=InMemoryTeamRouteBindingStore()),
        touch_interval_seconds=999,
    )
    client._connected = True
    client._ensure_pod_push = AsyncMock()
    return client, route, http


@pytest.mark.asyncio
async def test_catalog_reuses_pod_seen_by_a_routed_request(monkeypatch):
    client, route, http = _client(monkeypatch=monkeypatch)

    await client.send_request(_envelope(method="session.get_metadata", session_id="sess-1"))
    assert len(route.route_calls) == 1

    await client.send_request(_envelope(method="session.list", request_id="req-2"))

    assert len(route.route_calls) == 1
    assert http.send_bases[-1] == "http://route-pod.invalid:8086"
    assert route.touch_calls == [("sess-1", "req-1")]


@pytest.mark.asyncio
async def test_catalog_still_works_when_route_would_be_scope_full(monkeypatch):
    client, route, http = _client(monkeypatch=monkeypatch)
    client._remember_pod_base("http://cached-pod.invalid:8086")
    route.route_error = FatalRouteError("scope full", code="SCOPE_FULL")

    for method, params in (
        ("session.list", {}),
        ("session.archived.list", {}),
        ("team.quota.get", {}),
        ("project.list", {"filter": "all"}),
        ("project.info", {"project_id": "default"}),
        ("project.get_sessions", {"project_id": "default", "limit": 20}),
        ("project.get_cron_sessions", {"project_id": "default", "cron_id": "cron-1"}),
        ("project.pinned_sessions", {}),
    ):
        response = await client.send_request(_envelope(method=method, params=params))
        assert response.ok
        assert http.send_params[-1] == params
        assert http.send_bases[-1] == "http://cached-pod.invalid:8086"

    assert route.route_calls == []
    assert route.rebind_calls == []


@pytest.mark.asyncio
async def test_catalog_uses_live_push_pod_when_cache_is_empty(monkeypatch):
    """网关重启后 ``_catalog_bases`` 为空，仍可用推送连接中的活 Pod 服务目录读。"""
    client, route, http = _client(monkeypatch=monkeypatch)
    client._pod_clients["http://push-pod.invalid:8086"] = object()  # type: ignore[assignment]
    route.route_error = FatalRouteError("scope full", code="SCOPE_FULL")

    response = await client.send_request(
        _envelope(method="project.get_sessions", params={"project_id": "default"}),
    )

    assert response.ok
    assert route.route_calls == []
    assert http.send_bases == ["http://push-pod.invalid:8086"]


@pytest.mark.asyncio
async def test_catalog_uses_team_binding_when_cache_is_empty(monkeypatch):
    """网关重启后冷缓存：用团队绑定里的活 Pod 入口服务目录读，不抢 scope slot。"""
    client, route, http = _client(monkeypatch=monkeypatch)
    store = client._team_routes.store
    store.upsert(
        team_session_id="team-sess-live",
        endpoint="http://bound-pod.invalid:8766",
        status=ROUTE_STATUS_ACTIVE,
    )
    store.upsert(
        team_session_id="team-sess-stale",
        endpoint="http://alternate-pod.invalid:8766",
        status="unhealthy",
    )
    route.route_error = FatalRouteError("scope full", code="SCOPE_FULL")

    response = await client.send_request(
        _envelope(method="project.get_sessions", params={"project_id": "default"}),
    )

    assert response.ok
    assert route.route_calls == []
    assert http.send_bases == ["http://bound-pod.invalid:8766"]
    assert client._catalog_bases == ["http://bound-pod.invalid:8766"]


@pytest.mark.asyncio
async def test_catalog_skips_dead_team_binding_and_falls_back_to_route(monkeypatch):
    """团队绑定指向已回收的 Pod 时，忘记该入口并回退原有 route 路径。"""
    client, route, http = _client(monkeypatch=monkeypatch)
    store = client._team_routes.store
    store.upsert(
        team_session_id="team-sess-dead",
        endpoint="http://bound-pod.invalid:8766",
        status=ROUTE_STATUS_ACTIVE,
    )
    http.fail_bases.add("http://bound-pod.invalid:8766")

    response = await client.send_request(_envelope(method="session.list"))

    assert response.ok
    assert http.send_bases == ["http://bound-pod.invalid:8766", "http://route-pod.invalid:8086"]
    assert len(route.route_calls) == 1


@pytest.mark.asyncio
async def test_session_scoped_team_list_still_uses_route(monkeypatch):
    """带客户端会话的 team.list 必须照旧 route，不得走目录快路径。

    会话态 team.list 读取该会话所在 Pod 的运行态团队池：即使已有目录缓存
    入口也不能直连，须 route 到会话所属 Pod 并持续 touch 保活。
    信封由 ``bind_http_session``（GET /teams, bind_session_param=False，
    客户端带 session_id 查询参数）生成：顶层与 ``params`` 均为真实会话号。
    """
    client, route, http = _client(monkeypatch=monkeypatch)
    client._remember_pod_base("http://cached-pod.invalid:8086")

    sid, params = bind_http_session(
        "team.list", {"session_id": "sess-team"}, bind_param=False,
    )
    assert sid == "sess-team"
    assert params["session_id"] == "sess-team"

    await client.send_request(_envelope(method="team.list", session_id=sid, params=params))

    assert len(route.route_calls) == 1
    assert route.route_calls[0]["session_id"] == "sess-team"
    assert route.touch_calls == [("sess-team", "req-1")]
    assert http.send_bases == [route.base_url]


@pytest.mark.asyncio
async def test_http_catalog_synthetic_session_is_released(monkeypatch):
    """HTTP 目录请求带 ``webhttp_*``/``shared_user_*`` 合成号，也必须释放 slot。

    这是 HTTP 目录读的真实信封形状（``bind_http_session`` 恒填顶层会话号）：
    冷缓存时合成号仍用于一次性发现，但必须立即驱逐释放 slot。
    """
    for synthetic in ("webhttp_abc123def456", "shared_user_u1"):
        client, route, http = _client(monkeypatch=monkeypatch)

        response = await client.send_request(
            _envelope(
                method="session.list",
                session_id=synthetic,
                params={"session_id": synthetic},
            ),
        )

        assert response.ok
        assert http.send_bases == ["http://route-pod.invalid:8086"]
        assert len(route.route_calls) == 1
        assert route.route_calls[0]["session_id"] == synthetic
        assert [call["to_session_id"] for call in route.rebind_calls] == [None]
        assert route.touch_calls == []


@pytest.mark.asyncio
async def test_business_session_catalog_keeps_its_binding(monkeypatch):
    """显式业务会话（WS 客户端）走目录方法时不得驱逐其真实绑定。"""
    client, route, _ = _client(monkeypatch=monkeypatch)

    await client.send_request(
        _envelope(method="team.quota.get", session_id="sess-real", params={"session_id": "sess-real"}),
    )

    assert route.route_calls[0]["session_id"] == "sess-real"
    assert route.rebind_calls == []


@pytest.mark.asyncio
async def test_http_catalog_team_list_uses_shared_user_affinity(monkeypatch):
    """GET /teams 目录态：``shared_user_*`` 合成亲和号必须按目录处理。

    真实链路（``GET /teams`` → ``bind_session_param=False`` →
    ``bind_http_session``）无客户端会话时，信封顶层是 ``shared_user_<user>``
    合成亲和号、``params`` 无 ``session_id``。只按非空判断会把目录态误判
    成会话态、照旧 route 占 scope 槽：scope 已满时团队列表轮询自己就被
    ``SCOPE_FULL`` 挡住（本用例即该场景，修复前 route 会撞上 SCOPE_FULL）。
    """
    client, route, http = _client(monkeypatch=monkeypatch)
    client._remember_pod_base("http://cached-pod.invalid:8086")
    route.route_error = FatalRouteError("scope full", code="SCOPE_FULL")

    sid, params = bind_http_session(
        "team.list", {"include_deleted": False}, bind_param=False,
    )
    # 锁定真实形状：目录态 team.list 顶层是合成亲和号，params 无 session_id。
    assert sid.startswith("shared_user_")
    assert "session_id" not in params

    envelope = E2AEnvelope(
        method="team.list",
        session_id=sid,
        request_id="req-1",
        channel="web",
        user_id="u1",
        channel_context={"routing": {"group_id": "g1", "bot_id": "b1", "user_id": "u1"}},
        params=params,
    )

    response = await client.send_request(envelope)

    assert response.ok
    assert route.route_calls == []
    assert http.send_bases == ["http://cached-pod.invalid:8086"]


@pytest.mark.asyncio
async def test_catalog_forwards_timeout(monkeypatch):
    client, _, http = _client(monkeypatch=monkeypatch)
    client._remember_pod_base("http://cached-pod.invalid:8086")
    send = AsyncMock(return_value=AgentResponse(
        request_id="req-1", channel_id="web", ok=True, payload={},
    ))
    http.send_request = send
    envelope = _envelope(method="session.list")

    await client.send_request(envelope, timeout=1.5)

    send.assert_awaited_once_with(envelope, base_url="http://cached-pod.invalid:8086", timeout=1.5)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_catalog_permission_error_keeps_pod(monkeypatch, status):
    client, route, http = _client(monkeypatch=monkeypatch)
    base = "http://cached-pod.invalid:8086"
    client._remember_pod_base(base)
    response = httpx.Response(status, request=httpx.Request("GET", base))
    http.send_request = AsyncMock(side_effect=httpx.HTTPStatusError(
        "permission denied", request=response.request, response=response,
    ))

    with pytest.raises(httpx.HTTPStatusError):
        await client.send_request(_envelope(method="session.list"))

    assert client._catalog_bases == [base]
    assert http.send_request.await_count == 1
    assert route.route_calls == []


@pytest.mark.asyncio
async def test_catalog_total_deadline_bounds_hung_pod(monkeypatch):
    import asyncio
    from agent_gateway.extensions.runtime_management_extension import runtime_routed_client as module
    monkeypatch.setattr(module, "_CATALOG_BUDGET_SECONDS", 0.02)
    client, route, http = _client(monkeypatch=monkeypatch)
    client._remember_pod_base("http://hung.invalid:8086")

    async def hang(*args, **kwargs):
        await asyncio.Event().wait()

    http.send_request = hang
    with pytest.raises(module.RetryableRouteError, match="catalog deadline exceeded"):
        await asyncio.wait_for(client.send_request(_envelope(method="session.list")), 0.5)
    assert route.route_calls == []


@pytest.mark.asyncio
async def test_binding_candidates_page_to_later_healthy_pod(monkeypatch):
    monkeypatch.setattr("agent_gateway.extensions.runtime_management_extension.runtime_routed_client._CATALOG_BINDING_CACHE_SECONDS", 0)
    client, _, _ = _client(monkeypatch=monkeypatch)
    store = client._team_routes.store
    for index in range(5):
        store.upsert(team_session_id=f"real-{index}", endpoint=f"http://pod-{index}.invalid:8086", status=ROUTE_STATUS_ACTIVE)
    first = await client._catalog_bases_from_team_bindings()
    second = await client._catalog_bases_from_team_bindings()
    assert len(first) == 3
    assert len(second) == 2
    assert len(set(first + second)) == 5
    assert await client._catalog_bases_from_team_bindings() == first


@pytest.mark.asyncio
async def test_catalog_limits_attempts_and_backs_off_failed_pods(monkeypatch):
    client, route, http = _client(monkeypatch=monkeypatch)
    bases = [f"http://failed-{index}.invalid:8086" for index in range(5)]
    for base in bases:
        client._remember_pod_base(base)
        http.fail_bases.add(base)
    assert (await client.send_request(_envelope(method="session.list"))).ok
    assert http.send_bases == bases[:3] + [route.base_url]
    assert set(client._catalog_failed_until) == set(bases[:3])
    assert client._catalog_bases == bases[3:] + [route.base_url]
    assert len(route.route_calls) == 1
    # Push endpoints cannot bypass the failure backoff.
    for base in bases[:3]:
        client._pod_clients[base] = object()
    http.send_bases.clear()
    assert (await client.send_request(_envelope(method="session.list"))).ok
    assert not set(http.send_bases).intersection(bases[:3])


@pytest.mark.asyncio
async def test_binding_lookup_shared_after_waiter_cancel_and_empty_result_cached(monkeypatch):
    import asyncio
    import threading
    from unittest.mock import Mock
    client, _, _ = _client(monkeypatch=monkeypatch)
    started = threading.Event()
    release = threading.Event()

    def query(*args):
        started.set()
        assert release.wait(2)
        return []

    query_mock = Mock(side_effect=query)
    monkeypatch.setattr(client._team_routes.store, "catalog_endpoints", query_mock)
    first = asyncio.create_task(client._catalog_bases_from_team_bindings())
    try:
        await asyncio.wait_for(asyncio.to_thread(started.wait), 1)
        second = asyncio.create_task(client._catalog_bases_from_team_bindings())
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        assert await second == []
        assert await client._catalog_bases_from_team_bindings() == []
        assert query_mock.call_count == 1
        client._catalog_binding_expires = 0
        assert await client._catalog_bases_from_team_bindings() == []
        assert query_mock.call_count == 2
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.asyncio
async def test_binding_lookup_error_is_short_cached(monkeypatch):
    from unittest.mock import Mock
    client, _, _ = _client(monkeypatch=monkeypatch)
    query = Mock(side_effect=RuntimeError("database unavailable"))
    monkeypatch.setattr(client._team_routes.store, "catalog_endpoints", query)
    assert await client._catalog_bases_from_team_bindings() == []
    assert await client._catalog_bases_from_team_bindings() == []
    assert query.call_count == 1
