# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Shared subprocess capture for native file and directory dialogs."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping


def run_dialog_capture(
    command: list[str],
    *,
    timeout: float = 600.0,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a dialog helper and decode its path output as UTF-8.

    The helpers (osascript, zenity, kdialog, yad, tkinter children) emit UTF-8
    regardless of the interpreter's text mode. Decode explicitly so a
    non-UTF-8 locale cannot raise inside the reader thread and silently turn a
    selection into empty stdout.
    """
    return subprocess.run(  # noqa: S603
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )
