# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure management-stage capability checks; no resource acquisition or probing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import socket
import signal
import sys


@dataclass(frozen=True)
class ManagementPreflight:
    supported: bool
    reasons: tuple[str, ...]
    stage: str = 'management_preflight'
    runtime_verified: bool = False
    business_ready: bool = False
    recommendation: str = 'Continue using single mode; only management-probe tests runtime support.'


def management_preflight() -> ManagementPreflight:
    """Inspect prerequisites without opening files, sockets or subprocesses."""
    reasons = []
    if sys.platform not in {'linux', 'darwin'}:
        reasons.append('unsupported_platform')
    if getattr(sys, 'frozen', False):
        reasons.append('frozen_interpreter')
    if not isinstance(sys.executable, str) or not sys.executable:
        reasons.append('python_entry_unavailable')
    if not callable(getattr(socket, 'socketpair', None)):
        reasons.append('socketpair_unavailable')
    if getattr(socket, 'AF_UNIX', None) is None:
        reasons.append('unix_sockets_unavailable')
    if not callable(getattr(asyncio, 'create_subprocess_exec', None)):
        reasons.append('subprocess_exec_unavailable')
    if not callable(getattr(os, 'set_inheritable', None)):
        reasons.append('fd_inheritance_unavailable')
    if not callable(getattr(signal, 'pthread_sigmask', None)):
        reasons.append('signal_mask_unavailable')
    return ManagementPreflight(supported=not reasons, reasons=tuple(reasons))
