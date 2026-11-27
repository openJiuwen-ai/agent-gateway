# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Compatibility import for managed southbound orchestration."""

import sys
from agent_gateway.southbound.orchestration import managed_router

sys.modules[__name__] = managed_router
