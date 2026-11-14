# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""专家包允许名单（拆仓适配）。

母仓（jiuwenclaw/gateway）里本模块从企业库 ``package_allowlist`` 表读当前行
（无行/非法内容返回空列表）。agent-gateway 拆分后该表与写入服务未随仓迁移
（应用配置模型仅保留 channel/log_masking/logging/task_memory/memory/
cron_policy/audit_log；``/package-allowlists/catalog`` 只读目录路由仍在），
此处保留读入口并固定返回空列表——与"表存在但无行"时母仓的行为等价，
runtime_management_extension.pod_http_client 出站名单绑定即退化为空。

企业版完整恢复该特性时：补 ``PACKAGE_ALLOWLIST_TABLE_DEF``（含 layouts 注册）
与 ``PackageAllowlistService``，再把本函数改回真读库。
"""

from __future__ import annotations

from typing import Any


async def read_package_allowlist_packages() -> list[dict[str, Any]]:
    """拆仓适配期固定空列表（等价于企业库表无行）。"""
    return []
