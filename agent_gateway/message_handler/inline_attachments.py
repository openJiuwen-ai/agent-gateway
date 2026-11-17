"""Bound file expansion without decoding document or media bytes as text."""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from html import escape
from pathlib import Path

DEFAULT_TOTAL_INLINE_BYTES = 256 * 1024
_SNIFF_BYTES = 1024
_NON_TEXT_SUFFIXES = {
    **dict.fromkeys((".pdf",), "PDF"),
    **dict.fromkeys(
        (".doc", ".docx", ".docm", ".xls", ".xlsx", ".xlsm", ".ppt", ".pptx", ".pptm",
         ".odt", ".ods", ".odp", ".rtf", ".pages", ".numbers", ".key"),
        "Office",
    ),
    **dict.fromkeys(
        (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".ico",
         ".svg", ".heic", ".avif", ".psd"),
        "image",
    ),
    **dict.fromkeys((".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar", ".zst"), "archive"),
    **dict.fromkeys((".mp3", ".wav", ".ogg", ".flac", ".mp4", ".mov", ".avi", ".webm", ".m4a"), "media"),
    **dict.fromkeys((".exe", ".dll", ".so", ".dylib", ".bin", ".wasm", ".pyc", ".sqlite", ".db"), "binary"),
}
_MAGIC_TYPES = (
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "Office"),
    (b"PK\x03\x04", "archive"),
    (b"PK\x05\x06", "archive"),
    (b"PK\x07\x08", "archive"),
    (b"\x1f\x8b", "archive"),
    (b"BZh", "archive"),
    (b"\xfd7zXZ\x00", "archive"),
    (b"7z\xbc\xaf\x27\x1c", "archive"),
    (b"Rar!\x1a\x07", "archive"),
    (b"\x28\xb5\x2f\xfd", "archive"),
    (b"\x89PNG\r\n\x1a\n", "image"),
    (b"\xff\xd8\xff", "image"),
    (b"GIF87a", "image"),
    (b"GIF89a", "image"),
    (b"II\x2a\x00", "image"),
    (b"MM\x00\x2a", "image"),
    (b"8BPS", "image"),
    (b"ID3", "media"),
    (b"fLaC", "media"),
    (b"OggS", "media"),
    (b"\x7fELF", "binary"),
    (b"\x00asm", "binary"),
    (b"SQLite format 3\x00", "binary"),
)


@dataclass(frozen=True)
class InlineFile:
    """Describe either a bounded text body or a deferred file reference."""

    file_type: str
    original_size: int
    text: str | None = None
    truncated: bool = False
    reason: str = ""


def _non_text_type(path: Path, prefix: bytes) -> str | None:
    if prefix.startswith(b"%PDF-"):
        return "PDF"
    suffix_type = _NON_TEXT_SUFFIXES.get(path.suffix.lower())
    if suffix_type:
        return suffix_type
    for signature, file_type in _MAGIC_TYPES:
        if prefix.startswith(signature):
            return file_type
    if prefix.startswith(b"RIFF"):
        return "image" if prefix[8:12] == b"WEBP" else "media"
    if prefix[257:262] == b"ustar":
        return "archive"
    return None


def read_inline_file(path: Path, max_bytes: int) -> InlineFile:
    """Read at most the body budget plus a fixed file-type probe.

    UTF-8 decoding is strict except for an incomplete character at a real
    truncation boundary, which the incremental decoder leaves unread.
    """
    with path.open("rb") as handle:
        original_size = path.stat().st_size
        prefix = handle.read(_SNIFF_BYTES)
        file_type = _non_text_type(path, prefix)
        if file_type:
            return InlineFile(file_type, original_size, reason="non_text")
        if max_bytes <= 0:
            return InlineFile("file", original_size, reason="inline_budget_exhausted")
        data = prefix + handle.read(max(0, max_bytes + 1 - len(prefix)))
    truncated = len(data) > max_bytes
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    try:
        text = decoder.decode(data[:max_bytes], final=not truncated)
    except UnicodeDecodeError:
        return InlineFile("binary", original_size, reason="invalid_utf8")
    if any((ord(char) < 32 and char not in "\t\n\r") or ord(char) == 127 for char in text):
        return InlineFile("binary", original_size, reason="non_text")
    return InlineFile("text", original_size, text=text, truncated=truncated)


def file_reference(path: Path, file_type: str, reason: str) -> str:
    """Render an escaped reference that a document or file tool can consume."""
    if reason == "already_inlined":
        notice = "This file was already included or referenced earlier in this message."
    elif reason == "inline_budget_exhausted":
        notice = "Content not inlined: the message inline byte budget is exhausted. Use a file reading tool as needed."
    elif reason == "truncated_empty":
        notice = "Content not inlined: the byte limit removed all text at the truncation boundary. Use a file reading tool to read the file."
    elif file_type == "PDF":
        notice = "Content not inlined. Use a document reading tool to read PDF text by page as needed."
    else:
        notice = "Content not inlined. Use an appropriate document, media or file reading tool as needed."
    return (
        f'\n<file-reference path="{escape(str(path), quote=True)}" '
        f'filename="{escape(path.name, quote=True)}" type="{file_type}" reason="{reason}">\n'
        f"{notice}\n</file-reference>\n"
    )
