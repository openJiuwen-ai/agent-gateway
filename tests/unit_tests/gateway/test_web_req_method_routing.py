# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Web 请求方法路由：本地白名单 + 默认透传。"""

import pytest

from agent_gateway.channel_manager.web.req_method_routing import (
    ROUTE_DENIED,
    ROUTE_FORWARD,
    ROUTE_FORWARD_UNKNOWN,
    ROUTE_LOCAL,
    route_req_method,
)


def test_forward_list_wins_over_local_registration():
    assert (
        route_req_method("session.create", forward_methods={"session.create"}, local_methods={"session.create"})
        == ROUTE_FORWARD
    )


def test_registered_local_method_stays_local():
    assert (
        route_req_method("session.list", forward_methods={"session.create"}, local_methods={"session.list"})
        == ROUTE_LOCAL
    )


def test_unknown_method_is_forwarded_by_default():
    assert route_req_method("feature.new", forward_methods=set(), local_methods=set()) == ROUTE_FORWARD_UNKNOWN


def test_unknown_method_not_forwarded_when_switch_is_off():
    assert (
        route_req_method("feature.new", forward_methods=set(), local_methods=set(), forward_unknown=False)
        == ROUTE_LOCAL
    )


@pytest.mark.parametrize("method", ["gateway.internal.call", "admin.reset", "extension.internal.tick"])
def test_denied_prefixes_are_never_forwarded(method):
    assert route_req_method(method, forward_methods=set(), local_methods=set()) == ROUTE_DENIED


def test_channels_without_local_registry_keep_strict_behavior():
    assert route_req_method("feature.new", forward_methods=set(), local_methods=None) == ROUTE_LOCAL


def test_env_switch_disables_passthrough(monkeypatch):
    monkeypatch.setenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", "0")
    assert route_req_method("feature.new", forward_methods=set(), local_methods=set()) == ROUTE_LOCAL


def test_ensure_req_method_registers_missing_member():
    from agent_gateway.common.protocol_compat import ensure_req_method

    member = ensure_req_method("feature.brand_new")
    assert member is not None
    assert member.value == "feature.brand_new"
    assert ensure_req_method("feature.brand_new") is member


def test_ensure_req_method_rejects_invalid_value():
    from agent_gateway.common.protocol_compat import ensure_req_method

    assert ensure_req_method("") is None
    assert ensure_req_method("!!!") is None
