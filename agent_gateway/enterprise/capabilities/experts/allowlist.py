# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""过渡模块。jiuwenswarm 切到 outbound_packages 后删除。

上游 develop 仍按本路径惰性导入。旧名字留在这里，实现转到新模块。
"""

from __future__ import annotations

from agent_gateway.enterprise.capabilities.experts.outbound_packages import (
    apply_packages_to_envelope as apply_allowlist_to_envelope,
    attach_expert_packages_header,
    bind_outbound_packages as bind_outbound_allowlist,
    outbound_packages as outbound_allowlist_packages,
)

__all__ = [
    "apply_allowlist_to_envelope",
    "attach_expert_packages_header",
    "bind_outbound_allowlist",
    "outbound_allowlist_packages",
]
