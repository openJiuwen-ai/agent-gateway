# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""会话生命周期契约：建会话 → 列表可见 → 删除后消失。

黑盒口径：只经过 gateway 的 ``/ws``，断言真实 AgentServer 落库后的对外字段。
"""

from __future__ import annotations

from testcases_support.frames import last_res, unique_rid


def _session_ids(client) -> list[str]:
    res = last_res(client.request("session.list", {}, rid=unique_rid("st-list")))
    assert res["ok"] is True
    return [item["session_id"] for item in res["payload"]["sessions"]]


def _delete_session(client, session_id: str) -> None:
    try:
        client.request("session.delete", {"session_id": session_id}, rid=unique_rid("st-del"))
    except Exception:  # noqa: BLE001 - 清理失败不改变用例结论
        pass


def test_session_create_list_delete_roundtrip(workswarm_ws) -> None:
    create_res = last_res(
        workswarm_ws.request("session.create", {}, rid=unique_rid("st-create"))
    )
    assert create_res["ok"] is True
    payload = create_res["payload"]
    session_id = payload.get("session_id")
    assert session_id, f"session.create 未返回 session_id: {payload}"
    assert payload.get("sessionId") == session_id
    assert payload.get("projectId") == "default"

    try:
        assert session_id in _session_ids(workswarm_ws)

        list_res = last_res(
            workswarm_ws.request("session.list", {}, rid=unique_rid("st-list-detail"))
        )
        entry = next(
            item for item in list_res["payload"]["sessions"] if item["session_id"] == session_id
        )
        # 真实 AgentServer 的会话默认形态；gateway 不应改写。
        assert entry["mode"] == "agent.work.normal"
        assert entry["project_id"] == "default"
        assert entry["message_count"] == 0

        delete_res = last_res(
            workswarm_ws.request(
                "session.delete", {"session_id": session_id}, rid=unique_rid("st-delete")
            )
        )
        assert delete_res["ok"] is True
        assert delete_res["payload"]["session_id"] == session_id

        assert session_id not in _session_ids(workswarm_ws)
    finally:
        _delete_session(workswarm_ws, session_id)
