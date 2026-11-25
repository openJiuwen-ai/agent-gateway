# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""JSON checkpoints used when resuming WeLink message delivery."""

from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


def read_checkpoint(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_checkpoint(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            staged = Path(stream.name)
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        staged.replace(path)
    except BaseException:
        if staged is not None:
            staged.unlink(missing_ok=True)
        raise
