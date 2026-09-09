# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import base64
from pathlib import Path

from jiuwenswarm.agents.harness.common.tools.image_tools import image_generation_message_content


def _png_data_uri(payload: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(payload).decode("ascii")


def test_image_generation_message_content_appends_shot_text_after_both_images(
    tmp_path: Path,
) -> None:
    character = tmp_path / "character.png"
    scene = tmp_path / "scene.png"
    character.write_bytes(b"png-character")
    scene.write_bytes(b"png-scene")
    prompt = "分镜内容：镜号 2；人物变化 主体入画。"
    content = image_generation_message_content(
        prompt,
        [str(character), str(scene)],
    )
    assert isinstance(content, list)
    assert content[0]["image"] == _png_data_uri(b"png-character")
    assert content[1]["image"] == _png_data_uri(b"png-scene")
    assert content[2]["text"] == prompt


def test_image_generation_message_content_encodes_file_uri(tmp_path: Path) -> None:
    image = tmp_path / "ref.png"
    image.write_bytes(b"png-ref")
    content = image_generation_message_content("关键帧", [image.resolve().as_uri()])
    assert isinstance(content, list)
    assert content[0]["image"] == _png_data_uri(b"png-ref")
    assert content[1]["text"] == "关键帧"


def test_image_generation_message_content_keeps_https_url() -> None:
    content = image_generation_message_content(
        "关键帧",
        ["https://example.com/character.png"],
    )
    assert isinstance(content, list)
    assert content[0]["image"] == "https://example.com/character.png"
    assert content[1]["text"] == "关键帧"


def test_image_generation_message_content_is_text_only_without_refs() -> None:
    assert image_generation_message_content("关键帧", None) == "关键帧"
