# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Unit tests for management worker capability checks."""

import asyncio
import os
import signal
import socket
import sys

import pytest

from agent_gateway.processes.capabilities import management_preflight


def test_preflight_supports_normal_linux_python(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(sys, "executable", "/usr/bin/python3")

    report = management_preflight()

    assert report.supported is True
    assert report.reasons == ()


@pytest.mark.parametrize("platform", ["win32", "cygwin"])
def test_preflight_rejects_unsupported_platform(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(sys, "executable", "/usr/bin/python3")

    report = management_preflight()

    assert report.supported is False
    assert "unsupported_platform" in report.reasons


def test_preflight_rejects_frozen_interpreter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", "/Applications/Gateway.app/Contents/MacOS/Gateway")

    report = management_preflight()

    assert report.supported is False
    assert "frozen_interpreter" in report.reasons


@pytest.mark.parametrize(
    ("module", "attribute", "reason"),
    [
        (socket, "socketpair", "socketpair_unavailable"),
        (socket, "AF_UNIX", "unix_sockets_unavailable"),
        (asyncio, "create_subprocess_exec", "subprocess_exec_unavailable"),
        (os, "set_inheritable", "fd_inheritance_unavailable"),
        (signal, "pthread_sigmask", "signal_mask_unavailable"),
    ],
)
def test_preflight_reports_each_missing_capability(
    monkeypatch: pytest.MonkeyPatch,
    module: object,
    attribute: str,
    reason: str,
) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(sys, "executable", "/usr/bin/python3")
    monkeypatch.setattr(module, attribute, None)

    report = management_preflight()

    assert report.supported is False
    assert reason in report.reasons


def test_preflight_rejects_missing_python_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(sys, "executable", "")

    report = management_preflight()

    assert report.supported is False
    assert "python_entry_unavailable" in report.reasons
