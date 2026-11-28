"""Import the shared cron files once, before opening the Gateway file store."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import portalocker

from agent_gateway.common.paths import get_legacy_cron_jobs_path


def migrate_legacy_cron_files(destination: Path) -> None:
    """Initialize the authoritative file and lifecycle registry atomically per file."""
    if destination.exists() and destination.with_name("cron_lifecycle_owners.json").exists():
        return

    legacy = get_legacy_cron_jobs_path()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with portalocker.Lock(str(destination) + ".migration.lock", timeout=10):
        for source, target, key in (
            (legacy, destination, "jobs"),
            (
                legacy.with_name("cron_lifecycle_owners.json"),
                destination.with_name("cron_lifecycle_owners.json"),
                "owners",
            ),
        ):
            if target.exists():
                continue
            try:
                content = source.read_bytes()
            except FileNotFoundError:
                content = json.dumps({key: []}).encode()
            data = json.loads(content)
            if not isinstance(data, dict) or not isinstance(data.get(key), list):
                raise ValueError(f"Invalid legacy cron data in {source}")
            fd, name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
            temporary = Path(name)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
