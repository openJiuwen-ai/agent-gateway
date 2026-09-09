# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from pathlib import Path

from jiuwenswarm.agents.harness.common.tools.video_tools import (
    _build_dashscope_video_call,
    _switch_wan_task,
)


def test_switch_wan_task_keeps_flash_suffix() -> None:
    assert _switch_wan_task("wan2.6-t2v", "i2v") == "wan2.6-i2v"
    assert _switch_wan_task("wan2.6-i2v-flash", "i2v") == "wan2.6-i2v-flash"
    assert _switch_wan_task("wan2.6-t2v", "r2v") == "wan2.6-r2v"


def test_build_call_uses_first_keyframe_for_i2v(tmp_path: Path) -> None:
    frame = tmp_path / "shot1.png"
    extra = tmp_path / "shot2.png"
    frame.write_bytes(b"png")
    extra.write_bytes(b"png")
    params = _build_dashscope_video_call(
        "wan2.6-t2v",
        first_frame=str(frame),
        reference_images=[str(frame), str(extra)],
    )
    assert params["model"] == "wan2.6-i2v"
    assert params["img_url"] == frame.resolve().as_uri()
    assert params["shot_type"] == "multi"
    assert params["resolution"] == "720P"
    assert "size" not in params


def test_build_call_uses_single_shot_for_one_keyframe(tmp_path: Path) -> None:
    frame = tmp_path / "shot1.png"
    frame.write_bytes(b"png")
    params = _build_dashscope_video_call(
        "wan2.6-t2v",
        first_frame=str(frame),
    )
    assert params["model"] == "wan2.6-i2v"
    assert params["img_url"] == frame.resolve().as_uri()
    assert params["shot_type"] == "single"


def test_build_call_stays_text_to_video_without_images() -> None:
    params = _build_dashscope_video_call("wan2.6-t2v")
    assert params["model"] == "wan2.6-t2v"
    assert params["size"] == "1280*720"
    assert "img_url" not in params
