"""Tests for Feishu workspace file fallback detection."""

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
