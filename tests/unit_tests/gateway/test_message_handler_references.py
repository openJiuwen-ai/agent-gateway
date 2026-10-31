# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from agent_gateway.message_handler.message_handler import MessageHandler


def test_resolve_at_file_references_inlines_file_with_line_suffix(
    tmp_path, monkeypatch
):
    target = tmp_path / "sample.txt"
    target.write_text("hello gateway", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    resolved = MessageHandler.resolve_at_file_references(
        "@sample.txt#L1-3", cwd=str(tmp_path)
    )

    assert 'path="sample.txt"' in resolved
    assert "hello gateway" in resolved


def test_resolve_at_file_references_truncates_large_file(tmp_path):
    path = tmp_path / "large.txt"
    path.write_text("x" * 8, encoding="utf-8")

    resolved = MessageHandler.resolve_at_file_references(
        f'@"{path}"', cwd=str(tmp_path), max_file_size=4
    )

    assert "xxxx" in resolved
    assert "... (truncated, original_size=8 bytes)" in resolved


def test_reference_path_resolution_rules():
    assert MessageHandler._is_absolute_reference_path("C:\\tmp\\file.txt")
    assert MessageHandler._is_absolute_reference_path("/tmp/file.txt")
    assert not MessageHandler._is_absolute_reference_path("tmp/file.txt")
    assert MessageHandler._resolve_reference_path(
        "~/doc.txt", cwd="C:/tmp"
    ).endswith("doc.txt")


def test_normalize_structured_attachments_deduplicates_paths(tmp_path):
    path = str(tmp_path / "sample.txt")
    normalized = MessageHandler._normalize_structured_attachments(
        [
            {"path": path, "type": "text", "filename": "sample.txt"},
            {"path": path, "type": "ignored", "filename": "ignored.txt"},
            {"path": ""},
            "not-a-dict",
        ],
        cwd=str(tmp_path),
    )

    assert normalized == [
        {
            "path": path,
            "type": "text",
            "filename": "sample.txt",
        }
    ]


def test_strip_attached_mentions_keeps_agent_mentions_and_other_paths(
    tmp_path,
):
    attached = [{"path": str(tmp_path / "a.txt")}]
    cleaned = MessageHandler.strip_attached_mentions(
        "@agent-codex run @a.txt", attached, cwd=str(tmp_path)
    )

    assert cleaned == "@agent-codex run a.txt"


def test_extract_agent_mentions_deduplicates():
    mentions = MessageHandler.extract_agent_mentions(
        '@agent-codex @agent-codex @"reviewer (agent)"'
    )

    assert mentions == ["reviewer", "codex"]


def test_resolve_structured_attachments_inlines_files(tmp_path):
    path = tmp_path / "note.txt"
    path.write_text("note text", encoding="utf-8")

    resolved = MessageHandler._resolve_structured_attachments(
        "Use the attached file.",
        [{"path": str(path), "type": "file", "filename": "note.txt"}],
        cwd=str(tmp_path),
    )

    assert "note text" in resolved
