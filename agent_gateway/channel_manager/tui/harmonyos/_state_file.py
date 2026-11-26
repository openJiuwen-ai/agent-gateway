# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Persist HarmonyOS metadata without exposing a partially written file."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def write_state_json(target: Path, payload: dict[str, Any], *, mode: int | None = None) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{target.stem}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if mode is not None:
            os.chmod(name, mode)
        os.replace(name, target)
    finally:
        Path(name).unlink(missing_ok=True)
