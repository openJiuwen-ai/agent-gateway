import inspect
import os
from pathlib import Path

import pytest

from agent_gateway.message_handler.message_handler import MessageHandler


def _minimal_pdf_bytes() -> bytes:
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 100] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    stream = b"BT /F1 12 Tf 20 50 Td (Issue 6122) Tj ET"
    parts = [b"%PDF-1.4\n"]
    offsets = [0]
    for number, body in enumerate(objects, 1):
        offsets.append(sum(map(len, parts)))
        parts.append(f"{number} 0 obj\n".encode("ascii") + body + b"\nendobj\n")
    offsets.append(sum(map(len, parts)))
    parts.append(f"5 0 obj\n<< /Length {len(stream)} >>\nstream\n".encode("ascii"))
    parts.append(stream + b"\nendstream\nendobj\n")
    xref_offset = sum(map(len, parts))
    parts.append(b"xref\n0 6\n0000000000 65535 f \n")
    for offset in offsets[1:]:
        parts.append(f"{offset:010d} 00000 n \n".encode("ascii"))
    parts.append(b"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n")
    parts.append(f"{xref_offset}\n%%EOF\n".encode("ascii"))
    return b"".join(parts)


PDF_BYTES = _minimal_pdf_bytes()


def test_minimal_pdf_fixture_is_valid_for_pdfplumber(tmp_path: Path) -> None:
    pdfplumber = pytest.importorskip("pdfplumber")
    path = tmp_path / "fixture.pdf"
    path.write_bytes(PDF_BYTES)

    with pdfplumber.open(path) as document:
        assert document.pages[0].extract_text() == "Issue 6122"


def test_pdf_disguised_as_txt_is_not_inlined(tmp_path: Path) -> None:
    path = tmp_path / "payload.txt"
    path.write_bytes(PDF_BYTES)

    result = MessageHandler.resolve_at_file_references(
        f"review @{path.name}",
        cwd=str(tmp_path),
    )

    assert "<file-content" not in result
    assert f'path="{path.resolve()}"' in result
    assert 'filename="payload.txt"' in result
    assert 'type="PDF"' in result
    assert 'reason="non_text"' in result
    assert "%PDF-1.4" not in result
    assert "Issue 6122" not in result


@pytest.mark.parametrize(
    ("filename", "payload"),
    [
        ("readme.txt", b"Normal README text that mentions %PDF- only in prose.\n"),
        ("source.py", b"magic = 'b%PDF-'\n"),
    ],
)
def test_pdf_marker_inside_text_is_not_treated_as_pdf_magic(
    tmp_path: Path,
    filename: str,
    payload: bytes,
) -> None:
    path = tmp_path / filename
    path.write_bytes(payload)

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" in result
    assert "<file-reference" not in result
    assert payload.decode("utf-8") in result


@pytest.mark.parametrize(
    "extension",
    [".pdf", ".docx", ".png", ".zip"],
)
def test_dangerous_extensions_are_not_inlined_even_with_text_payload(
    tmp_path: Path,
    extension: str,
) -> None:
    path = tmp_path / f"payload{extension}"
    path.write_text("plain text", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" not in result
    assert "<file-reference" in result
    assert 'reason="non_text"' in result


@pytest.mark.parametrize(
    ("filename", "payload"),
    [
        ("document.pdf", PDF_BYTES),
        ("document.txt", PDF_BYTES),
        ("legacy.doc", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"),
        ("legacy.txt", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"),
        ("image.png", b"\x89PNG\r\n\x1a\n"),
        ("image.txt", b"\x89PNG\r\n\x1a\n"),
        ("archive.zip", b"PK\x03\x04"),
        ("archive.txt", b"PK\x03\x04"),
    ],
)
def test_magic_bytes_are_not_inlined_even_with_text_extension(
    tmp_path: Path,
    filename: str,
    payload: bytes,
) -> None:
    path = tmp_path / filename
    path.write_bytes(payload)

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" not in result
    assert "<file-reference" in result
    assert 'reason="non_text"' in result


def test_normal_utf8_text_is_inlined(tmp_path: Path) -> None:
    path = tmp_path / "notes.txt"
    path.write_text("中文🎉", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" in result
    assert "中文🎉" in result
    assert "<file-reference" not in result


def test_utf8_byte_boundary_is_respected_when_truncating(tmp_path: Path) -> None:
    path = tmp_path / "boundary.txt"
    path.write_text("中🎉", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
        max_file_size=4,
    )

    assert "<file-content" in result
    assert "中" in result
    assert "🎉" not in result
    assert "\ufffd" not in result


def test_eof_incomplete_utf8_uses_safe_file_reference(tmp_path: Path) -> None:
    path = tmp_path / "incomplete.txt"
    path.write_bytes(b"\xe4\xb8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" not in result
    assert "<file-reference" in result
    assert 'type="binary"' in result
    assert 'reason="invalid_utf8"' in result
    assert "\ufffd" not in result


def test_internal_invalid_utf8_before_truncation_is_not_dropped(tmp_path: Path) -> None:
    path = tmp_path / "truncated-invalid.txt"
    path.write_bytes(b"abc\xffdef")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
        max_file_size=4,
    )

    assert "<file-content" not in result
    assert "<file-reference" in result
    assert 'type="binary"' in result
    assert 'reason="invalid_utf8"' in result
    assert "\ufffd" not in result


def test_multibyte_truncation_to_empty_uses_file_reference(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("🎉", encoding="utf-8")
    second.write_text("AB", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{first.name}" @"{second.name}"',
        cwd=str(tmp_path),
        max_file_size=2,
        max_total_inline_bytes=5,
    )

    assert result.count("<file-content") == 1
    assert 'reason="truncated_empty"' in result
    assert "🎉" not in result
    assert "AB" in result


def test_per_file_default_is_128_kib(tmp_path: Path) -> None:
    path = tmp_path / "large.txt"
    size = 128 * 1024 + 1
    path.write_text("A" * size, encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" in result
    assert "truncated" in result
    assert f"original_size={size} bytes" in result
    assert "A" * (128 * 1024) in result
    assert "A" * (128 * 1024 + 1) not in result


def test_total_budget_is_byte_based_and_sequential(tmp_path: Path) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("中", encoding="utf-8")
    second.write_text("🎉", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{first.name}" @"{second.name}"',
        cwd=str(tmp_path),
        max_file_size=None,
        max_total_inline_bytes=5,
    )

    assert "中" in result
    assert "🎉" not in result
    assert 'reason="inline_budget_exhausted"' in result


def test_max_total_inline_bytes_is_keyword_only_with_256_kib_default() -> None:
    parameter = inspect.signature(
        MessageHandler.resolve_at_file_references
    ).parameters["max_total_inline_bytes"]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default == 256 * 1024


def test_unlimited_single_file_does_not_bypass_total_budget(tmp_path: Path) -> None:
    path = tmp_path / "large.txt"
    path.write_text("C" * (300 * 1024), encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
        max_file_size=None,
    )

    assert result.count("C") == 256 * 1024
    assert 'reason="inline_budget_exhausted"' in result


def test_zero_total_budget_preserves_pdf_and_defers_text(tmp_path: Path) -> None:
    pdf_path = tmp_path / "document.pdf"
    text_path = tmp_path / "notes.txt"
    pdf_path.write_bytes(PDF_BYTES)
    text_path.write_text("normal text", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{pdf_path.name}" @"{text_path.name}"',
        cwd=str(tmp_path),
        max_file_size=None,
        max_total_inline_bytes=0,
    )

    assert "<file-content" not in result
    assert 'type="PDF"' in result
    assert 'reason="non_text"' in result
    assert 'reason="inline_budget_exhausted"' in result


def test_duplicate_references_share_one_inlined_body(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.txt"
    path.write_text("duplicate body", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path}" again @"{path.name}"',
        cwd=str(tmp_path),
    )

    assert result.count("<file-content") == 1
    assert result.count("<file-reference") == 1
    assert 'reason="already_inlined"' in result
    assert result.count("duplicate body") == 1


def test_parent_path_normalization_deduplicates_references(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    path = tmp_path / "same.txt"
    path.write_text("normalized duplicate", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        f'@"{path}" and @"../same.txt"',
        cwd=str(nested),
    )

    assert result.count("<file-content") == 1
    assert result.count("<file-reference") == 1
    assert 'reason="already_inlined"' in result
    assert result.count("normalized duplicate") == 1


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX symlinks are required",
)
def test_symlink_path_normalization_deduplicates_references(tmp_path: Path) -> None:
    path = tmp_path / "target.txt"
    link = tmp_path / "link.txt"
    path.write_text("symlink duplicate", encoding="utf-8")
    os.symlink(path, link)

    result = MessageHandler.resolve_at_file_references(
        f'@"{path}" and @"{link}"',
        cwd=str(tmp_path),
    )

    assert result.count("<file-content") == 1
    assert result.count("<file-reference") == 1
    assert 'reason="already_inlined"' in result
    assert result.count("symlink duplicate") == 1


def test_file_content_path_is_escaped_and_preserved(tmp_path: Path) -> None:
    path = tmp_path / "safe & name.txt"
    path.write_text("escaped content", encoding="utf-8")

    result = MessageHandler.resolve_at_file_references(
        '@"safe & name.txt"',
        cwd=str(tmp_path),
    )

    assert "<file-content" in result
    assert 'path="safe &amp; name.txt"' in result
    assert "escaped content" in result


def test_file_reference_path_is_resolved_and_escaped(tmp_path: Path) -> None:
    path = tmp_path / "binary & data.pdf"
    path.write_bytes(PDF_BYTES)
    expected_path = str(path.resolve()).replace("&", "&amp;")

    result = MessageHandler.resolve_at_file_references(
        '@"binary & data.pdf"',
        cwd=str(tmp_path),
    )

    assert "<file-reference" in result
    assert f'path="{expected_path}"' in result
    assert 'type="PDF"' in result
    assert 'reason="non_text"' in result


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"abc\xffdef", "invalid_utf8"),
        (b"abc\x00def", "non_text"),
        (b"abc\x01\x02\x03def", "non_text"),
    ],
)
def test_unsafe_text_payloads_use_safe_file_references(
    tmp_path: Path,
    payload: bytes,
    reason: str,
) -> None:
    path = tmp_path / "payload.txt"
    path.write_bytes(payload)

    result = MessageHandler.resolve_at_file_references(
        f'@"{path.name}"',
        cwd=str(tmp_path),
    )

    assert "<file-content" not in result
    assert "<file-reference" in result
    assert 'type="binary"' in result
    assert f'reason="{reason}"' in result
    assert "\ufffd" not in result


@pytest.mark.skipif(
    os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="POSIX non-root permissions are required",
)
def test_unreadable_file_preserves_original_reference(tmp_path: Path) -> None:
    path = tmp_path / "unreadable.txt"
    path.write_text("secret", encoding="utf-8")
    content = f'check @"{path.name}"'
    path.chmod(0)
    try:
        result = MessageHandler.resolve_at_file_references(
            content,
            cwd=str(tmp_path),
        )
    finally:
        path.chmod(0o644)

    assert result == content


def test_structured_pdf_attachments_keep_all_paths_without_raw_bodies(
    tmp_path: Path,
) -> None:
    paths = []
    attachments = []
    for index in range(8):
        path = tmp_path / f"document-{index}.pdf"
        path.write_bytes(PDF_BYTES)
        paths.append(path)
        attachments.append({"path": str(path), "type": "file"})

    result = MessageHandler._resolve_structured_attachments(
        "review attachments",
        attachments,
    )

    assert result.count("<file-reference") == 8
    for path in paths:
        assert f'path="{path.resolve()}"' in result
    assert "%PDF-1.4" not in result
    assert "Issue 6122" not in result


def test_structured_text_attachment_is_inlined(tmp_path: Path) -> None:
    path = tmp_path / "notes.txt"
    path.write_text("structured text", encoding="utf-8")
    attachments = [{"path": str(path), "type": "file"}]

    result = MessageHandler._resolve_structured_attachments(
        "review",
        attachments,
    )

    assert "<file-content" in result
    assert "structured text" in result
    assert "<file-reference" not in result
