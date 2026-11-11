# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""透传契约 ST：白名单转发方法按文档参数约束调用后，原样到达 AgentServer。

用例覆盖三类断言（与文档《Gateway 接口文档》§1.3/§10.6 的标注一一对应）：

1. **纯透传**：``_FORWARD_NO_LOCAL_HANDLER_METHODS``（及 issue.*/schedule.* 转发项）
   的 ``params`` 在 stub 侧逐键相等——网关除文档声明的归一化外不做任何改写。
2. **带网关处理**：chat 族/history.get 断言文档化改写
   （content→query 复制、chat.resume→chat.interrupt+intent、强制流式、
   cancel 的 cancel_params 重构），其余键不变。
3. **帧级拒绝 / 不可达**：违反网关侧约束（mode 非字符串、session_id 非法、
   group_id 不匹配、swarmflow run_id 不一致）在网关即被拒，不触达 AgentServer；
   白名单中无协议枚举成员的方法（个人版）帧级回 METHOD_NOT_FOUND。

方法清单直接导入 ``agent_gateway`` 的转发注册表与协议枚举动态计算，
保证用例覆盖与代码同步，不手工维护列表。
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import pytest

# 与网关子进程一致：个人版白名单（去掉企业版开关，避免本进程导入注册表时
# 误减 team 控制面方法）。
os.environ.pop("JIUWENSWARM_EDITION", None)

from testcases_gateway.gateway_client import GatewayWsClient  # noqa: E402

SESSION = "st_pass_sess_0001"

# 文档声明了网关处理逻辑的方法（转发 + 本地 handler + 参数改写/重构）。
SPECIAL_METHODS = (
    "chat.send",
    "chat.resume",
    "chat.interrupt",
    "chat.user_answer",
    "chat.swarmflow_reply",
    "history.get",
)

# 文档声明"强制流式"的方法（其余方法透传客户端顶层 is_stream）。
STREAM_FORCED = frozenset({"chat.send", "history.get"})

STREAM_REQUIRED = ("personal_context.context.stream_graph", "personal_context.context.stream_tree")


def _forward_registry() -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """导入转发注册表与协议枚举，返回 (forward, no_local, unreachable)。"""
    from agent_gateway.channel_manager.web import app_web_handlers as awh
    from e2a.message import ReqMethod

    forward: frozenset[str] = awh._FORWARD_REQ_METHODS
    no_local: frozenset[str] = awh._FORWARD_NO_LOCAL_HANDLER_METHODS
    enum_values = {member.value for member in ReqMethod}
    unreachable = frozenset(method for method in forward if method not in enum_values)
    return forward, no_local, unreachable


def _params_for(method: str) -> dict[str, Any]:
    """按文档参数约束构造最小合法 params（仅满足网关侧校验，业务值任意）。"""
    params: dict[str, Any] = {"session_id": SESSION}
    if method == "chat.send":
        params["content"] = "st-passthrough"
    elif method == "chat.user_answer":
        params.update(request_id="st_req_0001", answers=[{"selected_options": ["ok"]}])
    elif method == "chat.swarmflow_reply":
        params.update(correlation_id="st-corr-0001", answer="st-answer")
    elif method in ("swarmflow.pause", "swarmflow.resume", "swarmflow.stop"):
        params["run_id"] = "st-run-0001"
    elif method == "command.goal":
        params["action"] = "get"
    elif method == "group.create":
        params.update(group_id=SESSION, name="st-group", members=[])
    elif method.startswith("group."):
        params["group_id"] = SESSION
    return params


def _is_stream_for(method: str) -> bool:
    return method in STREAM_REQUIRED


def _drain(ws_client: GatewayWsClient, *, seconds: float = 1.5) -> None:
    """消费响应帧，避免网关侧 per-connection 出站队列被积压帧填满。"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            ws_client._ws.recv(timeout=0.05)  # noqa: SLF001 - ST 客户端内部接口
        except TimeoutError:  # websockets 同步客户端超时语义
            continue
        except Exception:  # noqa: BLE001 - 连接关闭等，退出消费
            return


def _send_and_await_stub(
    gateway_ws: GatewayWsClient,
    stub_agent_server: Any,
    method: str,
    params: dict[str, Any],
    *,
    rid: str,
    is_stream: bool,
    expect_method: str,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """发送一帧并轮询 stub 记录，返回匹配的 E2A 信封（不依赖 res 帧形态）。"""
    frame: dict[str, Any] = {"type": "req", "id": rid, "method": method, "params": params}
    if is_stream:
        frame["is_stream"] = True
    start = len(stub_agent_server.requests)
    gateway_ws._ws.send(json.dumps(frame))  # noqa: SLF001 - ST 客户端内部接口
    deadline = time.monotonic() + timeout
    fallback: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        for envelope in stub_agent_server.requests[start:]:
            if envelope.get("request_id") == rid:
                _drain(gateway_ws)
                return envelope
            if envelope.get("method") == expect_method and fallback is None:
                fallback = envelope
        time.sleep(0.05)
    _drain(gateway_ws)
    assert fallback is not None, f"请求未到达 AgentServer: {method} (rid={rid})"
    return fallback


# ---------------------------------------------------------------------------
# 1. 纯透传：params 逐键相等
# ---------------------------------------------------------------------------


def _pure_cases() -> list[str]:
    forward, no_local, unreachable = _forward_registry()
    pure = sorted(
        method
        for method in forward
        if method not in unreachable and method not in SPECIAL_METHODS
    )
    assert pure, "转发注册表为空"
    return pure


@pytest.mark.parametrize("method", _pure_cases())
def test_forward_params_passthrough(gateway_ws, stub_agent_server, method: str) -> None:
    """纯透传方法：stub 收到的 params 与发送的完全相等，channel=web。"""
    rid = f"st-{method}-pure"
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        method,
        _params_for(method),
        rid=rid,
        is_stream=_is_stream_for(method),
        expect_method=method,
    )
    assert envelope["method"] == method
    assert envelope["channel"] == "web"
    assert envelope["session_id"] == SESSION
    assert envelope["is_stream"] is _is_stream_for(method)
    assert envelope["params"] == _params_for(method), (
        f"{method} 的 params 被网关改写：发送 {_params_for(method)} 实到 {envelope['params']}"
    )


# ---------------------------------------------------------------------------
# 2. 带网关处理：断言文档声明的改写，其余键不变
# ---------------------------------------------------------------------------


def _special_expected(method: str) -> tuple[str, bool, dict[str, Any]]:
    """返回 (期望到达方法, 期望 is_stream, 期望 params)。"""
    params = _params_for(method)
    if method == "chat.send":
        expected = dict(params)
        expected["query"] = params["content"]
        return "chat.send", True, expected
    if method == "chat.resume":
        expected = dict(params)
        expected.setdefault("intent", "resume")
        return "chat.interrupt", False, expected
    if method == "chat.interrupt":
        # cancel 分支重构信封：{intent:"cancel", session_id} + 定位参数透传（缺省不带）。
        return "chat.interrupt", False, {"intent": "cancel", "session_id": SESSION}
    if method == "history.get":
        return "history.get", True, dict(params)
    return method, False, dict(params)


@pytest.mark.parametrize("method", SPECIAL_METHODS)
def test_forward_documented_mutations(gateway_ws, stub_agent_server, method: str) -> None:
    """chat 族/history.get：只发生文档声明的网关改写，无未声明变更。"""
    expect_method, expect_stream, expect_params = _special_expected(method)
    rid = f"st-{method}-special"
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        method,
        _params_for(method),
        rid=rid,
        is_stream=False,
        expect_method=expect_method,
    )
    assert envelope["method"] == expect_method
    assert envelope["channel"] == "web"
    assert envelope["is_stream"] is expect_stream
    assert envelope["params"] == expect_params, (
        f"{method} 期望 {expect_params} 实到 {envelope['params']}"
    )


def test_chat_interrupt_locators_passthrough(gateway_ws, stub_agent_server) -> None:
    """chat.interrupt 携带的定位参数（mode/team/trusted_dirs/work_mode/project_dir）原样进入重构信封。"""
    params = {
        "session_id": SESSION,
        "mode": "agent",
        "team": False,
        "trusted_dirs": ["/tmp/st"],
        "work_mode": "work",
        "project_dir": "/tmp/st-project",
    }
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        "chat.interrupt",
        params,
        rid="st-interrupt-locators",
        is_stream=False,
        expect_method="chat.interrupt",
    )
    assert envelope["params"] == {
        "intent": "cancel",
        "session_id": SESSION,
        "mode": "agent",
        "trusted_dirs": ["/tmp/st"],
        "work_mode": "work",
        "project_dir": "/tmp/st-project",
    }


def test_chat_resume_keeps_client_intent(gateway_ws, stub_agent_server) -> None:
    """chat.resume 显式携带 intent 时不覆盖（setdefault 语义）。"""
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        "chat.resume",
        {"session_id": SESSION, "intent": "resume"},
        rid="st-resume-intent",
        is_stream=False,
        expect_method="chat.interrupt",
    )
    assert envelope["method"] == "chat.interrupt"
    assert envelope["params"] == {"session_id": SESSION, "intent": "resume"}


# ---------------------------------------------------------------------------
# 3. E2A 代理路径：params 透传 + 文档声明注入（rsi session_id）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "params", "expect_extra"),
    [
        ("session.list", {"session_id": SESSION}, {}),
        ("config.get", {"session_id": SESSION}, {}),
        (
            "rsi.task.get",
            {"session_id": SESSION, "task_id": "rsi-st0001"},
            {},
        ),
        (
            "rsi.artifact.download",
            {"session_id": SESSION, "task_id": "rsi-st0001"},
            {},
        ),
    ],
)
def test_proxy_params_passthrough(
    gateway_ws, stub_agent_server, method: str, params: dict, expect_extra: dict
) -> None:
    """E2A 代理方法：params 原样到达（rsi 由网关注入 session_id，此处已显式携带）。"""
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        method,
        params,
        rid=f"st-{method}-proxy",
        is_stream=False,
        expect_method=method,
    )
    assert envelope["method"] == method
    assert envelope["params"] == dict(params, **expect_extra)


def test_rsi_session_id_injected(gateway_ws, stub_agent_server) -> None:
    """rsi.* 未携带 session_id 时由网关注入连接会话 id（文档 §10.6 声明）。"""
    envelope = _send_and_await_stub(
        gateway_ws,
        stub_agent_server,
        "rsi.task.get",
        {"task_id": "rsi-st0002"},
        rid="st-rsi-inject",
        is_stream=False,
        expect_method="rsi.task.get",
    )
    injected = envelope["params"].pop("session_id")
    assert injected == envelope["session_id"]
    assert envelope["params"] == {"task_id": "rsi-st0002"}


# ---------------------------------------------------------------------------
# 4. 帧级拒绝 / 不可达方法
# ---------------------------------------------------------------------------


def _request_and_expect_error(
    gateway_ws: GatewayWsClient, stub_agent_server: Any, method: str, params: dict[str, Any], code: str
) -> None:
    before = len(stub_agent_server.requests)
    frames = gateway_ws.request(method, params, rid=f"st-err-{method}-{code}")
    res = frames[-1]
    assert res["type"] == "res" and res["ok"] is False
    assert res.get("code") == code, res
    assert len(stub_agent_server.requests) == before, "被拒请求不应触达 AgentServer"


def test_frame_rejects_non_string_mode(gateway_ws, stub_agent_server) -> None:
    _request_and_expect_error(
        gateway_ws, stub_agent_server, "team.list", {"session_id": SESSION, "mode": 123}, "BAD_REQUEST"
    )


def test_frame_rejects_invalid_session_id(gateway_ws, stub_agent_server) -> None:
    _request_and_expect_error(
        gateway_ws, stub_agent_server, "team.list", {"session_id": "bad id!"}, "BAD_REQUEST"
    )


def test_frame_rejects_group_id_mismatch(gateway_ws, stub_agent_server) -> None:
    _request_and_expect_error(
        gateway_ws,
        stub_agent_server,
        "chat.send",
        {"session_id": SESSION, "group_id": "st_other_group", "content": "x"},
        "BAD_REQUEST",
    )


def test_frame_rejects_swarmflow_run_id_mismatch(gateway_ws, stub_agent_server) -> None:
    _request_and_expect_error(
        gateway_ws,
        stub_agent_server,
        "swarmflow.pause",
        {"session_id": SESSION, "run_id": "st-run-a", "workflow_run_id": "st-run-b"},
        "BAD_REQUEST",
    )


def test_frame_rejects_unknown_method(gateway_ws, stub_agent_server) -> None:
    _request_and_expect_error(
        gateway_ws, stub_agent_server, "st.no.such.method", {"session_id": SESSION}, "METHOD_NOT_FOUND"
    )


def test_personal_edition_rejects_team_control(gateway_ws, stub_agent_server) -> None:
    """个人版从白名单剔除 team 控制面 7 方法：帧级 METHOD_NOT_FOUND。"""
    _request_and_expect_error(
        gateway_ws, stub_agent_server, "team.create", {"session_id": SESSION}, "METHOD_NOT_FOUND"
    )


def test_unreachable_forward_methods() -> None:
    """白名单中无协议枚举成员的方法当前不可达（帧级 METHOD_NOT_FOUND）——与文档标注一致。"""
    _, _, unreachable = _forward_registry()
    expected = {
        "session.assets.remove",
        "skills.enterprise.install",
        "skills.enterprise.list",
        "skills.enterprise.source.providers",
        "skills.enterprise.source.search",
        "skills.enterprise.uninstall",
        "skills.source.install",
        "skills.source.providers",
        "skills.source.search",
        "skills.update",
        "skills.updates.check",
    }
    assert unreachable == expected, (
        f"不可达方法集合变化（协议包/网关 shim 已补枚举？请同步更新文档标注）: {unreachable ^ expected}"
    )


@pytest.mark.parametrize(
    "method",
    sorted(
        {
            "session.assets.remove",
            "skills.update",
            "skills.source.search",
            "skills.enterprise.list",
        }
    ),
)
def test_unreachable_method_returns_not_found(gateway_ws, stub_agent_server, method: str) -> None:
    """抽查不可达方法：白名单在册但协议枚举缺失，帧级即被拒。"""
    _request_and_expect_error(
        gateway_ws, stub_agent_server, method, {"session_id": SESSION, "name": "st-skill"}, "METHOD_NOT_FOUND"
    )
