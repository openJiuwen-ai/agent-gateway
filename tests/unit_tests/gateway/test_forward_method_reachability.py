# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""转发方法可达性门禁。

`_FORWARD_REQ_METHODS` 里的方法必须先能被 `parse_request_method` 解析，否则转发
门禁拿到空串直接短路，请求退化成 ``METHOD_NOT_FOUND`` —— 用户侧看到的是「未知方法」，
而不是「方法未实现」，排障成本很高。这类回归曾真实发生（team.* 七方法与
mcp.sources.providers），本测试把它变成 CI 门禁。

白名单只允许「明确知道为何暂不可达、且有人在跟」的条目；新增项必须写理由。
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

# 子进程要导入完整 app_gateway 应用栈（web 通道 + 企业平面），冷启动或高负载
# CI 上可能接近一分钟。放宽到 180s，避免与代码质量无关的不稳定失败。
_SUBPROCESS_TIMEOUT_SECONDS = 180.0

# 协议包尚未发布、gateway 侧也没有补齐的方法。键是方法名，值是「为何还在名单里」。
_KNOWN_UNREACHABLE: dict[str, str] = {
    # 企业连接器广场：gateway 单方面加了北向路由与转发名单，Agent 侧无端点。
    # 待 connector 作者确认真实端点后再决定「补 Agent 端点」还是「撤回映射」。
    "mcp.sources.providers": "connector 广场 SPI 未在 Agent 侧落地（commit b9a5dcae2）",
    "session.assets.remove": "协议包未发布该成员，当前无调用方",
    "skills.enterprise.install": "企业技能仓方法待协议包发布",
    "skills.enterprise.list": "企业技能仓方法待协议包发布",
    "skills.enterprise.source.providers": "企业技能仓方法待协议包发布",
    "skills.enterprise.source.search": "企业技能仓方法待协议包发布",
    "skills.enterprise.uninstall": "企业技能仓方法待协议包发布",
    "skills.source.install": "技能来源方法待协议包发布",
    "skills.source.providers": "技能来源方法待协议包发布",
    "skills.source.search": "技能来源方法待协议包发布",
    "skills.update": "技能更新方法待协议包发布",
    "skills.updates.check": "技能更新方法待协议包发布",
}


def _run_in_subprocess(code: str, tmp_path, *, edition: str):
    """在独立解释器里跑断言，返回 (CompletedProcess, 实际耗时秒数)。"""
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
        env=dict(
            os.environ,
            JIUWENSWARM_HOME=str(tmp_path),
            JIUWENSWARM_DATA_DIR=str(tmp_path / "data"),
            JIUWENSWARM_EDITION=edition,
        ),
    )
    return result, time.monotonic() - started


def test_enterprise_team_forwarding_contract(tmp_path):
    """Exercise real startup and protocol registration without mutating pytest's enum."""
    code = """
from agent_gateway.app_gateway import ReqMethod
from agent_gateway.channel_manager.web.app_web_handlers import (
    _ENTERPRISE_TEAM_CONTROL_METHODS, _FORWARD_REQ_METHODS,
)
from agent_gateway.common.request_method import parse_request_method
from agent_gateway.extensions.runtime_management_extension.pod_rest_map import _lookup_route
from e2a.runtime_http_routes import resolve_runtime_http_route

for method in _ENTERPRISE_TEAM_CONTROL_METHODS:
    assert method in _FORWARD_REQ_METHODS, method
    assert parse_request_method(method) is ReqMethod(method), method
    declared = resolve_runtime_http_route(method)
    assert declared is not None, method
    assert _lookup_route(method, is_stream=False) == (declared.verb, declared.path), method

unreachable = {m for m in _FORWARD_REQ_METHODS if parse_request_method(m) is None}
expected = set(%r)
assert unreachable == expected, unreachable ^ expected
""" % sorted(_KNOWN_UNREACHABLE)
    result, elapsed = _run_in_subprocess(code, tmp_path, edition="enterprise")
    assert result.returncode == 0, (
        f"子进程失败（耗时 {elapsed:.1f}s / 上限 {_SUBPROCESS_TIMEOUT_SECONDS}s）:\n"
        f"{result.stdout}{result.stderr}"
    )


def test_personal_team_control_is_not_enabled(tmp_path):
    code = """
import agent_gateway.app_gateway
from agent_gateway.channel_manager.web.app_web_handlers import (
    _ENTERPRISE_TEAM_CONTROL_METHODS, _FORWARD_REQ_METHODS,
)
from agent_gateway.common.request_method import parse_request_method
from agent_gateway.extensions.runtime_management_extension.pod_rest_map import (
    HttpMethodNotHandled, _lookup_route,
)
assert _ENTERPRISE_TEAM_CONTROL_METHODS.isdisjoint(_FORWARD_REQ_METHODS)
for method in _ENTERPRISE_TEAM_CONTROL_METHODS:
    assert parse_request_method(method) is None, method
    try:
        _lookup_route(method, is_stream=False)
    except HttpMethodNotHandled:
        continue
    raise AssertionError(method)
"""
    result, elapsed = _run_in_subprocess(code, tmp_path, edition="personal")
    assert result.returncode == 0, (
        f"子进程失败（耗时 {elapsed:.1f}s / 上限 {_SUBPROCESS_TIMEOUT_SECONDS}s）:\n"
        f"{result.stdout}{result.stderr}"
    )
