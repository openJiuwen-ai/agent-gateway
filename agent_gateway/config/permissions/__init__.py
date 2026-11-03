# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""permissions_config：Repository + access（yaml ``/permissions`` 段）。"""

from agent_gateway.config.permissions.repository import (
    PERMISSIONS_CONFIG_STORE_NAME,
    PermissionsConfigRepository,
)
from agent_gateway.config.section import (
    SectionDocument,
    YamlSectionCodec,
)

__all__ = [
    "PERMISSIONS_CONFIG_STORE_NAME",
    "PermissionsConfigRepository",
    "SectionDocument",
    "YamlSectionCodec",
]
