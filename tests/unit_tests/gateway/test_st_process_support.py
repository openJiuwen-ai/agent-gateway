# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The shared ST launcher must also reclaim Windows test processes."""

from types import SimpleNamespace
from unittest.mock import Mock
import signal
import subprocess

from st_test.testcases_support import process


def test_windows_st_cleanup_uses_native_process_handles(monkeypatch):
    monkeypatch.setattr(process, 'os', SimpleNamespace(name='nt'))
    monkeypatch.setattr(process, '_descendants', lambda pid: [])
    proc = Mock(pid=123)
    proc.poll.return_value = None
    proc.wait.side_effect = [subprocess.TimeoutExpired('fixture',10),None]
    process.stop_process(proc)
    proc.send_signal.assert_called_once_with(signal.SIGTERM)
    proc.kill.assert_called_once_with()
