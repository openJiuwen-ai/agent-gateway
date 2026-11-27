# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Static capability checks for the future worker process pool.

This preflight does not change the Gateway's default single-process behavior.
The v1 worker launcher supports non-frozen Linux/macOS Python interpreters.
Windows and frozen application bundles are excluded for now.

``frozen_interpreter`` is reported because ``sys.executable`` points to the
packaged application binary rather than a normal CPython interpreter. The
current worker launch contract assumes a normal Python executable.

These checks are deliberately static: they do not open files, sockets, or
subprocesses, and they do not prove that the platform event loop or business
runtime is ready.
"""

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
