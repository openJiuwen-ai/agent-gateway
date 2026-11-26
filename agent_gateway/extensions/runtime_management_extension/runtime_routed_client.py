# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility import for delegated southbound orchestration."""

import sys
from agent_gateway.southbound.orchestration import delegated_router

sys.modules[__name__] = delegated_router
