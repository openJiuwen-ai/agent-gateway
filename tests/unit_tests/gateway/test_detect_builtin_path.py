# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""detect_external_cli_agent 携带内置（托管）CLI 路径的行为（#6647，主仓同步）。

配置本地可执行文件后，探测结果与 PATH 扫描都不再指向内置路径，
路径下拉框需要探测响应里的 builtin_path 作为常驻选项，
否则内置路径会从下拉框消失并被历史清理剪掉。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_gateway.common.config_panel import config_set_handlers as handlers


@pytest.fixture
def builtin_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """内置托管 CLI 的假可执行文件；版本命令与参考版本打桩为一致输出。

    托管路径按 agent 区分（与真实实现一致）：claude 有托管路径，
    其余（自定义 agent）返回空串。
    """
    managed = tmp_path / "site-packages" / "claude_agent_sdk" / "_bundled" / "claude.exe"
    managed.parent.mkdir(parents=True)
    managed.write_text("stub", encoding="utf-8")
    monkeypatch.setattr(handlers, "external_cli_managed_cli_path", lambda agent: str(managed) if agent == "claude" else "")
    # 版本命令打桩：输出需含 "Claude Code" 身份标识（547ffa1b56 引入的校验）
    monkeypatch.setattr(
        handlers,
        "run_external_cli_version_command",
        lambda _agent, _path: ("Claude Code version 1.2.3", ""),
    )
    # 参考版本与桩版本一致，避免环境里真实 SDK 版本把状态推成 warning。
    monkeypatch.setattr(handlers, "external_cli_reference_version", lambda _agent: "1.2.3")
    return managed


def test_detect_result_carries_builtin_path_for_missing_local_path(
    tmp_path: Path, builtin_cli: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """标准探测分支：配置的本地路径不存在时探测失败，内置路径仍随结果返回。"""
    missing = tmp_path / "local" / "claude.exe"
    result = handlers.detect_external_cli_agent("claude", str(missing))
    assert result["status"] == "missing"
    assert result["path"] == ""
    assert result["builtin_path"] == str(builtin_cli)


def test_detect_result_carries_builtin_path_for_local_path_hit(
    tmp_path: Path, builtin_cli: Path
) -> None:
    """标准探测分支：本地路径可用时探测成功，builtin_path 与探测路径并存。"""
    local = tmp_path / "local" / "claude.exe"
    local.parent.mkdir(parents=True)
    local.write_text("stub", encoding="utf-8")
    result = handlers.detect_external_cli_agent("claude", str(local))
    assert result["status"] == "ok"
    assert result["path"] == str(local)
    assert result["builtin_path"] == str(builtin_cli)


def test_detect_result_builtin_path_empty_when_managed_cli_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """标准探测分支：内置 CLI 未安装（托管文件缺失）且 PATH 无可执行文件时，
    builtin_path 为空串、状态为 missing。

    显式打桩 shutil.which 隔离 PATH：本机/CI 是否装有 claude 不影响断言
    （环境相关 flaky，atomgit-bot AI 审查 P2）。"""
    monkeypatch.setattr(handlers, "external_cli_managed_cli_path", lambda _agent: "")
    monkeypatch.setattr(handlers.shutil, "which", lambda *_a, **_k: None)
    result = handlers.detect_external_cli_agent("claude", "")
    assert result["status"] == "missing"
    assert result["builtin_path"] == ""


def test_detect_result_builtin_path_empty_for_unsupported_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """不支持类型分支：unavailable 时也携带 builtin_path 键（空串，可序列化）。"""
    result = handlers.detect_external_cli_agent("definitely-not-an-agent")
    assert result["status"] == "unavailable"
    assert json.dumps(result)  # 结果可序列化
    assert result["builtin_path"] == ""


def test_detect_result_carries_builtin_path_for_custom_agent(
    tmp_path: Path, builtin_cli: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """自定义 Agent 分支：同样携带 builtin_path 键；自定义 agent 无托管路径，
    其值恒为 external_cli_managed_cli_path 的返回（空串）。"""
    monkeypatch.setattr(handlers, "external_cli_agent_kinds", lambda: ("claude", "codex", "custom-x"))
    custom_exe = tmp_path / "local" / "custom-agent.exe"
    custom_exe.parent.mkdir(parents=True)
    custom_exe.write_text("stub", encoding="utf-8")

    result = handlers.detect_external_cli_agent("custom-x", str(custom_exe))

    assert result["cli_agent"] == "custom-x"
    assert result["status"] == "ok"
    assert result["path"] == str(custom_exe)
    assert result["builtin_path"] == ""
    assert result["builtin_path"] == handlers.external_cli_managed_cli_path("custom-x")
