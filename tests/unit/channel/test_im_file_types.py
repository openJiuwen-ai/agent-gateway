# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Tests for shared IM attachment file type detection."""

import pytest

from agent_gateway.channel_manager.im_platforms import file_types
from agent_gateway.channel_manager.im_platforms.dingtalk import (
    dingtalk_file_service,
)
from agent_gateway.channel_manager.im_platforms.wecom import (
    wecom_file_service,
)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00", ".png"),
        (b"\xff\xd8\xff\xe0" + b"\x00" * 8, ".jpg"),
        (b"GIF89a\x00\x00\x00\x00\x00\x00", ".gif"),
        (b"RIFF\x00\x00\x00\x00WEBP", ".webp"),
        (b"RIFF\x00\x00\x00\x00WAVE", ".wav"),
        (b"ID3\x04" + b"\x00" * 8, ".mp3"),
        (b"\xff\xfb" + b"\x00" * 10, ".mp3"),
        (b"\xff\xfa" + b"\x00" * 10, ".mp3"),
        (b"fLaC\x00\x00\x00\x00\x00\x00\x00\x00", ".flac"),
        (b"OggS\x00\x00\x00\x00\x00\x00\x00\x00", ".ogg"),
        (b"\x00\x00\x00\x20ftypisom\x00\x00", ".mp4"),
        (b"\x00\x00\x00\x20moofisom\x00\x00", ".mp4"),
        (b"\x00\x00\x00\x20moovisom\x00\x00", ".mp4"),
        (b"\x1a\x45\xdf\xa3" + b"\x00" * 8, ".mkv"),
        (b"FLV\x01\x05\x00\x00\x00\x00\x00\x00\x00", ".flv"),
    ],
)
def test_detect_file_extension_signatures(content: bytes, expected: str) -> None:
    assert file_types.detect_file_extension(content) == expected


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"", ""),
        (b"\x89PNG", ""),
        (b"\x89PNG\r\n\x1a\n\x00\x00\x00", ""),
        (b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00", ".png"),
        (b"unknown-file", ""),
        (b"RIFF\x00\x00\x00\x00NOPE", ".webp"),
    ],
)
def test_detect_file_extension_boundaries(content: bytes, expected: str) -> None:
    assert file_types.detect_file_extension(content) == expected


@pytest.mark.parametrize(
    ("extension", "expected"),
    [
        (".png", "image/png"),
        (".JPG", "image/jpeg"),
        (".WebP", "image/webp"),
        (".wav", "audio/wav"),
        (".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        (".json", "application/json"),
        (".unknown", "application/octet-stream"),
        ("", "application/octet-stream"),
    ],
)
def test_get_mime_type(extension: str, expected: str) -> None:
    assert file_types.get_mime_type(extension) == expected


def test_platform_modules_share_file_type_helpers() -> None:
    assert dingtalk_file_service.detect_file_extension is file_types.detect_file_extension
    assert dingtalk_file_service.get_mime_type is file_types.get_mime_type
    assert wecom_file_service.detect_file_extension is file_types.detect_file_extension
    assert wecom_file_service.get_mime_type is file_types.get_mime_type
