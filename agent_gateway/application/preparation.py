# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Prepare optional SDK callbacks without blocking the Web control loop."""

from __future__ import annotations

import importlib
import logging


def preload_callback_modules() -> None:
    """Import modules in a worker thread; construct SDK resources on the loop."""
    from agent_gateway.common.openjiuwen_logging import ensure_openjiuwen_core_logging

    ensure_openjiuwen_core_logging()
    importlib.import_module("openjiuwen.core.runner")


def callback_framework():
    from openjiuwen.core.common.logging import LogManager
    from openjiuwen.core.runner import Runner

    for logger in LogManager.get_all_loggers().values():
        logger.set_level(logging.INFO)
    return Runner.callback_framework


def preload_web_modules() -> None:
    """Only import declarations; channel creation and plugin binding stay on-loop."""
    importlib.import_module("agent_gateway.channel_manager.web.app_web_handlers")
    importlib.import_module("agent_gateway.channel_manager.web.web_connect")
