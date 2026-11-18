# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""登录失效提示的 60 秒限流闸门（同一 session 每个窗口最多一条）."""

from __future__ import annotations

import time

import pytest

pytest.importorskip("e2a")

from agent_gateway.message_handler import message_handler as mh


@pytest.fixture(autouse=True)
def _reset_gate_state():
    mh._login_expired_log_last.clear()
    yield
    mh._login_expired_log_last.clear()


def test_same_session_allowed_once_per_window() -> None:
    assert mh._login_expired_log_allowed("s1") is True
    assert mh._login_expired_log_allowed("s1") is False


def test_sessions_are_independent() -> None:
    assert mh._login_expired_log_allowed("s1") is True
    assert mh._login_expired_log_allowed("s2") is True


def test_window_expiry_allows_again() -> None:
    assert mh._login_expired_log_allowed("s1") is True
    mh._login_expired_log_last["s1"] = (
        time.monotonic() - mh._LOGIN_EXPIRED_LOG_WINDOW_S - 1.0
    )
    assert mh._login_expired_log_allowed("s1") is True


def test_stale_entries_pruned_when_table_grows() -> None:
    expired_at = time.monotonic() - mh._LOGIN_EXPIRED_LOG_WINDOW_S - 1.0
    for i in range(300):
        mh._login_expired_log_last[f"old-{i}"] = expired_at

    assert mh._login_expired_log_allowed("fresh") is True
    assert "fresh" in mh._login_expired_log_last
    assert len(mh._login_expired_log_last) == 1
