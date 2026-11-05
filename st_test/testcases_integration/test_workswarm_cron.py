# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""定时任务 CRUD 契约：经 gateway 透传到真实 AgentServer 落库。

参考 jiuwen-test 的 cron.job.* 用例口径，但只做黑盒核心字段核对。
"""

from __future__ import annotations

from testcases_support.frames import last_res, unique_rid


def test_cron_job_create_get_delete(workswarm_ws) -> None:
    params = {
        "name": "ST 每日提醒",
        "cron_expr": "0 9 * * *",
        "description": "agent-gateway ST 创建的临时任务",
        "targets": "web",
        "mode": "agent",
    }
    created = last_res(
        workswarm_ws.request("cron.job.create", params, rid=unique_rid("st-cron-create"))
    )
    assert created["ok"] is True, f"创建任务失败: {created}"
    job = created["payload"]["job"]
    job_id = job["id"]
    assert job["name"] == params["name"]
    assert job["description"] == params["description"]
    assert job["targets"] == params["targets"]
    assert job["enabled"] is True
    assert job["timezone"]
    # AgentServer 把标准 cron 规范化成 7 段（秒 分 时 日 月 周 年）。
    assert job["cron_expr"].split() == ["0", "0", "9", "*", "*", "*", "*"]

    try:
        fetched = last_res(
            workswarm_ws.request("cron.job.get", {"id": job_id}, rid=unique_rid("st-cron-get"))
        )
        assert fetched["ok"] is True
        assert fetched["payload"]["job"] == job
    finally:
        deleted = last_res(
            workswarm_ws.request(
                "cron.job.delete", {"id": job_id}, rid=unique_rid("st-cron-delete")
            )
        )
        assert deleted["ok"] is True
        assert deleted["payload"]["deleted"] is True

    missing = last_res(
        workswarm_ws.request("cron.job.get", {"id": job_id}, rid=unique_rid("st-cron-missing"))
    )
    assert missing["ok"] is False
