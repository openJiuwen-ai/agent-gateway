# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""真实模型对话契约（默认不跑）。

需要可用的模型配置，属外部依赖，默认跳过；设置 ``ST_WORKSWARM_CHAT=1`` 开启。
"""

from __future__ import annotations

import pytest

from testcases_integration.conftest import chat_enabled
from testcases_support.frames import event_names, last_res, unique_rid

pytestmark = pytest.mark.slow

_CHAT_TIMEOUT = 180.0


def test_chat_send_single_round(workswarm_ws) -> None:
    if not chat_enabled():
        pytest.skip("真实模型对话默认关闭：设置 ST_WORKSWARM_CHAT=1 开启")

    create_res = last_res(
        workswarm_ws.request("session.create", {}, rid=unique_rid("st-chat-create"))
    )
    assert create_res["ok"] is True
    session_id = create_res["payload"]["session_id"]

    try:
        frames = workswarm_ws.request(
            "chat.send",
            {
                "session_id": session_id,
                "content": "用一句话回答：1+1 等于几？",
                "mode": "agent",
                "enable_swarmflow": False,
            },
            rid=unique_rid("st-chat"),
            is_stream=True,
            timeout=_CHAT_TIMEOUT,
        )
        accepted = last_res(frames)
        assert accepted["ok"] is True
        assert accepted["payload"]["accepted"] is True
        assert accepted["payload"]["session_id"] == session_id

        names = event_names(frames)
        assert names, f"受理后未收到任何事件帧: {frames}"
        for frame in frames:
            payload = frame.get("payload") or {}
            if frame.get("event") == "chat.error" and payload.get("code") == "model_not_configured":
                pytest.skip("环境未配置可用模型，跳过真实对话断言")
        assert "chat.error" not in names, f"对话返回错误事件: {frames[-1]}"
        assert set(names) & {"chat.delta", "chat.final"}, f"未收到正文事件: {names}"
    finally:
        try:
            workswarm_ws.request(
                "session.delete",
                {"session_id": session_id},
                rid=unique_rid("st-chat-cleanup"),
            )
        except Exception:  # noqa: BLE001 - 收尾失败不影响结论
            pass
