# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


_DEFAULT_INLINE_FILE_SIZE_LIMIT = 128 * 1024


def resolve_at_file_references(
    content: str,
    cwd: str | None = None,
    max_file_size: int | None = _DEFAULT_INLINE_FILE_SIZE_LIMIT,
) -> str:
    """Parse ``@path`` references in *content* and inline the file text.

    Supported forms:
    - ``@relative/path`` / ``@/absolute/path`` — resolved against *cwd*
    - ``@"path with spaces"`` — quoted paths
    - ``@path#L10-20`` — line-range suffix (ignored for now, whole file read)

    Returns content with ``@path`` replaced by a ``<file-content>`` block
    containing the actual text.  If a file cannot be read the original
    ``@path`` is kept unchanged.
    """
    if not content:
        return content

    working_dir = cwd or os.getcwd()

    # Match @path or @"quoted path", optionally followed by #L... line range
    pattern = re.compile(
        r'(?P<prefix>(?:^|(?<=\s)))@(?:"(?P<quoted>[^"]+)"|(?P<plain>[^\s#]+))(?:#[^#\s]*)?'
    )

    def _replacer(m: re.Match[str]) -> str:
        raw = m.group("quoted") or m.group("plain") or ""
        if not raw:
            return m.group(0)

        # Skip @agent-xxx mentions (not file references)
        if raw.startswith("agent-") or raw.startswith("agent:"):
            return m.group(0)

        # Resolve path
        if raw.startswith("~/"):
            home = os.path.expanduser("~")
            resolved = os.path.join(home, raw[2:])
        elif _is_absolute_reference_path(raw):
            resolved = raw
        else:
            resolved = os.path.join(working_dir, raw)

        try:
            path = Path(resolved)
            if not path.is_file():
                return m.group(0)
            size = path.stat().st_size
            truncated = False
            if max_file_size is None:
                text = path.read_text(encoding="utf-8", errors="replace")
            else:
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    text = handle.read(max_file_size + 1)
                if size > max_file_size or len(text) > max_file_size:
                    truncated = True
                if len(text) > max_file_size:
                    text = text[:max_file_size]
                if truncated:
                    suffix = f"\n... (truncated, original_size={size} bytes)"
                    text = f"{text}{suffix}"
            return (
                f'\n<file-content path="{raw}">\n{text}\n</file-content>\n'
            )
        except (OSError, UnicodeDecodeError):
            return m.group(0)

    return pattern.sub(_replacer, content)


def extract_agent_mentions(content: str) -> list[str]:
    """Parse ``@agent-xxx`` and ``@"xxx (agent)"`` mentions from content.

    Returns list of agent type names (without "agent-" prefix), deduplicated.
    """
    if not content:
        return []

    results: list[str] = []

    # Match quoted format: @"<type> (agent)"
    for m in re.finditer(r'(^|\s)@"([\w:.@-]+)\s+\(agent\)"', content):
        results.append(m.group(2))

    # Match unquoted format: @agent-<type>
    for m in re.finditer(r'(^|\s)@(agent-[\w:.@-]+)', content):
        full = m.group(2)
        results.append(full[len("agent-"):])

    # Deduplicate preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for name in results:
        if name not in seen:
            seen.add(name)
            unique.append(name)
    return unique


def _is_absolute_reference_path(raw: str) -> bool:
    return raw.startswith("/") or (len(raw) >= 3 and raw[1] == ":" and raw[2] == "\\")


def _resolve_reference_path(raw: str, cwd: str | None = None) -> str:
    working_dir = cwd or os.getcwd()
    if raw.startswith("~/"):
        return os.path.join(os.path.expanduser("~"), raw[2:])
    if _is_absolute_reference_path(raw):
        return raw
    return os.path.join(working_dir, raw)


def _normalize_structured_attachments(
    attachments: Any,
    cwd: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(attachments, list):
        return []

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in attachments:
        if not isinstance(item, dict):
            continue
        raw_path = str(item.get("path") or "").strip()
        if not raw_path:
            continue
        resolved_path = _resolve_reference_path(raw_path, cwd)
        if resolved_path in seen:
            continue
        seen.add(resolved_path)
        normalized.append(
            {
                "path": resolved_path,
                "type": str(item.get("type") or "file").strip() or "file",
                "filename": str(item.get("filename") or Path(resolved_path).name).strip(),
            }
        )
    return normalized


def strip_attached_mentions(
    content: str,
    attachments: list[dict[str, Any]],
    cwd: str | None = None,
) -> str:
    if not content or not attachments:
        return content

    attached_paths = {
        _resolve_reference_path(str(item.get("path") or ""), cwd)
        for item in attachments
        if str(item.get("path") or "").strip()
    }
    if not attached_paths:
        return content

    pattern = re.compile(
        r'(?P<prefix>(?:^|(?<=\s)))@(?:"(?P<quoted>[^"]+)"|(?P<plain>[^\s#]+))(?:#[^#\s]*)?'
    )

    def _replacer(match: re.Match[str]) -> str:
        raw = match.group("quoted") or match.group("plain") or ""
        if not raw:
            return match.group(0)
        # Skip @agent-xxx mentions (not file references)
        if raw.startswith("agent-") or raw.startswith("agent:"):
            return match.group(0)
        resolved = _resolve_reference_path(raw, cwd)
        if resolved not in attached_paths:
            return match.group(0)
        return f"{match.group('prefix')}{raw}"

    return pattern.sub(_replacer, content)


def _resolve_structured_attachments(
    content: str,
    attachments: Any,
    cwd: str | None = None,
) -> str:
    normalized = _normalize_structured_attachments(attachments, cwd)
    if not normalized:
        return content

    prefix = " ".join(f'@"{item["path"]}"' for item in normalized)
    cleaned_content = strip_attached_mentions(content, normalized, cwd)
    merged_content = f"{prefix} {cleaned_content}".strip()
    return resolve_at_file_references(merged_content, cwd=cwd)
