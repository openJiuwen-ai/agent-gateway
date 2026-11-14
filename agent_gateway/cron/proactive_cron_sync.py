# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Legacy proactive.tick compatibility constants.

主动推荐功能已随上游移除；Gateway 不再自动注册或清理 ``proactive.tick`` 任务。
固定 id 仍供 cron controller/store 的遗留任务保护逻辑使用。
"""

from __future__ import annotations

PROACTIVE_JOB_ID = "proactive-tick-auto"
