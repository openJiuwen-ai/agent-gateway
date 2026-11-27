# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility import for the Runtime control-plane client."""

import sys
from agent_gateway.southbound.control import runtime

sys.modules[__name__] = runtime
