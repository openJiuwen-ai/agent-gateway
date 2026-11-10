"""Resolve a verified, session-scoped attachment on the shared host filesystem."""

import hashlib
import os
import re
import stat
from pathlib import Path

from agent_gateway.common.paths import get_domain_data_dir, get_user_workspace_dir


def resolve_output_file(key: object, file_ref: object, size: object) -> Path | None:
    """Accept only the recorded bytes below a real employee/session output directory."""
    parts = key.split("/") if isinstance(key, str) else []
    if (
        len(parts) != 6 or parts[2] != "sessions" or parts[4] != "output"
        or any(not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", parts[i]) for i in (0, 1, 3))
        or parts[5] in ("", ".", "..") or any(c in parts[5] for c in ("\\", ":", "\0"))
        or type(size) is not int or size < 0
        or not isinstance(file_ref, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", file_ref)
    ):
        return None
    for root in (get_domain_data_dir(), get_user_workspace_dir()):
        verified = _resolve_in_root(root, parts, file_ref, size)
        if verified is not None:
            return verified
    return None


def _resolve_in_root(path: Path, parts: list[str], file_ref: str, size: int) -> Path | None:
    try:
        for part in ("jiuwenmate", "workspaces", *parts[:-1]):
            path = path / part
            meta = path.lstat()
            if not stat.S_ISDIR(meta.st_mode) or _reparse_point(meta):
                return None
        path = path / parts[-1]
        meta = path.lstat()
        if not stat.S_ISREG(meta.st_mode) or _reparse_point(meta) or meta.st_size != size:
            return None
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(path, flags), "rb") as stream:
            if not os.path.samestat(meta, os.fstat(stream.fileno())):
                return None
            digest = hashlib.sha256()
            remaining = size
            while remaining:
                chunk = stream.read(min(remaining, 1 << 20))
                if not chunk:
                    return None
                digest.update(chunk)
                remaining -= len(chunk)
            if stream.read(1) or "sha256:" + digest.hexdigest() != file_ref:
                return None
    except (OSError, ValueError):
        return None
    return path


def _reparse_point(metadata: os.stat_result) -> bool:
    return bool(getattr(metadata, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)
