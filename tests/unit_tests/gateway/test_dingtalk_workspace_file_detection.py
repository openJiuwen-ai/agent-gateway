# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
    DingTalkChannel,
    _workspace_path_pattern,
)


def test_workspace_path_pattern_is_cached_per_directory(tmp_path):
    workspace = str(tmp_path / "workspace")
    other_workspace = str(tmp_path / "other")

    assert _workspace_path_pattern(workspace) is _workspace_path_pattern(workspace)
    assert _workspace_path_pattern(workspace) is not _workspace_path_pattern(other_workspace)


def test_workspace_file_detection_keeps_absolute_and_quoted_filename_matches(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    report = workspace / "report.pdf"
    report.write_text("report", encoding="utf-8")

    channel = object.__new__(DingTalkChannel)

    assert channel._detect_workspace_files(
        f'Located {report} and "report.pdf".', str(workspace)
    ) == [str(report)]
