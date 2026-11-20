# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""南向 REST 表与协议包的对拍契约测试。

Gateway 的 ``pod_rest_map.REST_ROUTES`` 是手工维护的静态表，真源在协议包
``e2a.runtime_http_routes``。两边一旦漂移，表现是线上 404 或打到错误动作，
CI 里发现不了。本测试把漂移变成红灯。

方向选择：只做「协议包有 → Gateway 必须能解析出同一网址」，因为 Gateway 允许
对少数方法做特判（``chat.send``/``chat.resume``/``history.get`` 流式变体），
也允许登记协议包尚未发布的方法（见 ``protocol_methods`` 的 shim）。反向
（Gateway 多出协议包没有的）单独用白名单守住，抓的是「单方面加映射」。
"""

from __future__ import annotations

import pytest

from agent_gateway.extensions.runtime_management_extension.pod_rest_map import (
    REST_ROUTES,
    HttpMethodNotHandled,
    _lookup_route,
)

#: Gateway 上的特判路由：不在 REST_ROUTES 里，但 _lookup_route 会给出答案。
_SPECIAL_CASED = frozenset({"chat.send", "chat.resume", "history.get"})

#: Gateway 有、协议包还没有的方法。新增项必须写明为什么、以及跟谁对齐。
_GATEWAY_ONLY: dict[str, str] = {
    "mcp.sources.providers": (
        "企业连接器广场：Gateway 单方面加了北向路由与转发名单，"
        "Agent 侧尚无对应 HTTP 端点（见 review 第 65 条 mcp.sources.providers）"
    ),
}


def _protocol_routes() -> dict[str, tuple[str, str]]:
    from e2a.runtime_http_routes import RUNTIME_HTTP_ROUTES

    return {row.method: (row.verb, row.path) for row in RUNTIME_HTTP_ROUTES}


def _protocol_stream_routes() -> dict[str, tuple[str, str]]:
    from e2a.runtime_http_routes import RUNTIME_HTTP_STREAM_OVERRIDES

    return {row.method: (row.verb, row.path) for row in RUNTIME_HTTP_STREAM_OVERRIDES}


@pytest.fixture()
def enterprise_edition(monkeypatch):
    """按企业版（可解析路由的超集）语义跑对拍。

    团队控制面方法只在企业版开放（个人版既不登记协议枚举也不给南向路由），
    所以契约测试用企业版口径，避免把有意为之的版本隔离当成漂移。
    """
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")


def test_every_protocol_route_is_resolvable_by_gateway(enterprise_edition):
    """协议包声明的网址，Gateway 必须能组装出同一 verb + path。"""
    declared = _protocol_routes()
    missing: list[str] = []
    mismatched: list[tuple[str, object, object]] = []

    for method, expected in sorted(declared.items()):
        try:
            actual = _lookup_route(method, is_stream=False)
        except HttpMethodNotHandled:
            missing.append(method)
            continue
        if actual != expected:
            mismatched.append((method, actual, expected))

    assert not missing, (
        "以下协议包已声明的方法在 Gateway 里没有南向路由，runtime 模式会回 "
        f"HTTP_METHOD_NOT_HANDLED：{missing}\n"
        "修法：在 pod_rest_map._ROUTE_ROWS 补对应行（path 照抄协议表）。"
    )
    assert not mismatched, (
        "以下方法的南向网址与协议包不一致（会 404 或打到错误动作）："
        f"{mismatched}"
    )


def test_protocol_stream_overrides_are_honoured(enterprise_edition):
    """协议包的流式覆盖网址（如 history.get）必须与 Gateway 特判一致。"""
    for method, expected in _protocol_stream_routes().items():
        assert _lookup_route(method, is_stream=True) == expected, method


def test_gateway_does_not_invent_protocol_routes(enterprise_edition):
    """Gateway 不应单方面声明协议包没有的方法。

    失败时二选一：让协议包补声明（两仓配套提交），或用 ``_GATEWAY_ONLY``
    显式登记并写清原因。
    """
    declared = set(_protocol_routes())
    invented = {
        method
        for method in REST_ROUTES
        if method not in declared and method not in _SPECIAL_CASED
        and method not in _GATEWAY_ONLY
    }
    assert not invented, (
        f"以下方法只在 Gateway 侧声明、协议包没有：{sorted(invented)}\n"
        "runtime 模式发过去会 404。请在 _GATEWAY_ONLY 登记原因，或撤回映射。"
    )


def test_gateway_only_allowlist_has_no_stale_entries(enterprise_edition):
    """白名单要能自我清理：协议包补上声明后必须从白名单删掉。"""
    declared = _protocol_routes()
    stale = sorted(method for method in _GATEWAY_ONLY if method in declared)
    assert not stale, (
        f"协议包已声明这些方法，应从 _GATEWAY_ONLY 移除：{stale}"
    )


@pytest.mark.parametrize("method", sorted(_GATEWAY_ONLY))
def test_gateway_only_entries_are_documented(method):
    """白名单条目必须带非空理由，避免变成垃圾桶。"""
    reason = _GATEWAY_ONLY[method].strip()
    assert len(reason) >= 20, f"{method} 的白名单理由太短，写清原因与跟进方"
