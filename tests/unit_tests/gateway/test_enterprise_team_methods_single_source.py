# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业团队控制面方法只允许有一份定义。

TUI 转发名单、Web 转发名单与 Web HTTP 路由表各自需要这份名单，但它们必须指向
``agent_gateway.common.enterprise_methods`` 里的同一个 frozenset；任何一处重新抄
一份字符串都会让个人版门禁出现漏口子。
"""

from __future__ import annotations

from agent_gateway.channel_manager.tui import tui_connect
from agent_gateway.channel_manager.web import app_web_handlers
from agent_gateway.channel_manager.web.http_routes import mapped
from agent_gateway.common.enterprise_methods import ENTERPRISE_TEAM_CONTROL_METHODS

_EXPECTED = frozenset({
    "team.create",
    "team.get",
    "team.stop",
    "team.member.join",
    "team.member.leave",
    "team.members.list",
    "team.quota.get",
})


def test_team_control_methods_have_a_single_definition():
    assert ENTERPRISE_TEAM_CONTROL_METHODS == _EXPECTED
    assert tui_connect.ENTERPRISE_TEAM_CONTROL_METHODS is ENTERPRISE_TEAM_CONTROL_METHODS
    assert (
        app_web_handlers.ENTERPRISE_TEAM_CONTROL_METHODS
        is ENTERPRISE_TEAM_CONTROL_METHODS
    )
    assert mapped.ENTERPRISE_TEAM_CONTROL_METHODS is ENTERPRISE_TEAM_CONTROL_METHODS


def test_personal_edition_hides_team_control_routes():
    personal_routes = mapped.mapped_routes_for_edition(enterprise=False)
    exposed = {route.rpc_method for route in personal_routes}
    assert exposed.isdisjoint(ENTERPRISE_TEAM_CONTROL_METHODS)
    assert "team.delete" in exposed
