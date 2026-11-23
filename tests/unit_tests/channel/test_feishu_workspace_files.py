"""Tests for Feishu workspace file fallback detection."""

import pytest

from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)


def _make_channel() -> FeishuChannel:
    config = FeishuConfig(enabled=True, app_id="app", app_secret="secret")
    channel = FeishuChannel.__new__(FeishuChannel)
    channel.config = config
    return channel


def test_detects_absolute_path_inside_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(tmp_path),
    )
    target = tmp_path / "notes.txt"
    target.write_text("hello", encoding="utf-8")
    channel = _make_channel()

    assert channel._detect_workspace_files(f"see {target}") == [str(target)]


def test_detects_quoted_file_name_inside_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(tmp_path),
    )
    target = tmp_path / "report.docx"
    target.write_text("hello", encoding="utf-8")
    channel = _make_channel()

    assert channel._detect_workspace_files("see 'report.docx'") == [str(target)]


def test_ignores_workspace_file_without_a_path_reference(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(tmp_path),
    )
    (tmp_path / "notes.txt").write_text("hello", encoding="utf-8")
    channel = _make_channel()

    assert channel._detect_workspace_files("notes.txt") == []


@pytest.mark.parametrize(
    "candidate",
    ["../outside.txt", "nested/report.pdf", r"nested\report.pdf"],
)
def test_rejects_quoted_paths_outside_workspace_root(
    tmp_path, monkeypatch, candidate
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(workspace),
    )
    (tmp_path / "outside.txt").write_text("secret", encoding="utf-8")
    (workspace / "report.pdf").write_text("report", encoding="utf-8")

    assert _make_channel()._detect_workspace_files(f'"{candidate}"') == []


def test_rejects_absolute_path_that_escapes_workspace(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(workspace),
    )

    escaped = workspace / ".." / outside.name
    assert _make_channel()._detect_workspace_files(str(escaped)) == []


def test_rejects_symlink_that_resolves_outside_workspace(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    link = workspace / "linked.txt"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks are unavailable: {exc}")
    monkeypatch.setattr(
        "agent_gateway.common.paths.get_agent_workspace_dir",
        lambda: str(workspace),
    )

    assert _make_channel()._detect_workspace_files(str(link)) == []
    assert _make_channel()._detect_workspace_files("'linked.txt'") == []
