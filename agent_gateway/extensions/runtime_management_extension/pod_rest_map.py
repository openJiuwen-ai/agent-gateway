# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility import for the shared AgentServer REST mapping."""

from agent_gateway.southbound.data import rest_map as _implementation
from agent_gateway.southbound.data.rest_map import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_implementation, name)
