# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业团队控制面方法清单（仅企业版暴露）。

个人版进程既不转发这些方法，也不注册对应的 Web HTTP 路由。TUI 转发名单、Web 转发
名单和 HTTP 路由表共用这一份定义，避免三处各抄一遍字符串后悄悄漂移。

``team.list`` / ``team.delete`` 等旧方法不在此列，个人版照常保留。

这里只放方法名，不含任何 edition 判定：调用方自行决定企业版是否放行。
"""

from __future__ import annotations

__all__ = ["ENTERPRISE_TEAM_CONTROL_METHODS"]


ENTERPRISE_TEAM_CONTROL_METHODS: frozenset[str] = frozenset({
    "team.create",
    "team.get",
    "team.stop",
    "team.member.join",
    "team.member.leave",
    "team.members.list",
    "team.quota.get",
})
