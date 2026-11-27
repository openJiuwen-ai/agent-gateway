# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Compatibility import for the southbound AgentManager state machine."""

import sys
from agent_gateway.southbound.orchestration import agent_manager

sys.modules[__name__] = agent_manager
