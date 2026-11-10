# Keep the cron settings and upgrade rules aligned with the JiuwenSwarm host.
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Resolve and migrate Gateway cron storage configuration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portalocker.exceptions import LockException
from ruamel.yaml.error import YAMLError

from agent_gateway.common.paths import get_cron_jobs_path

logger = logging.getLogger(__name__)
_DEFAULT_PREFIX = "/jiuwenswarm/cron/jobs/"


@dataclass(frozen=True)
class CronStoreSettings:
    backend: str = "file"
    endpoints: tuple[str, ...] = ()
    prefix: str = _DEFAULT_PREFIX
    file_path: Path | None = None


def _as_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_endpoints(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    text = str(raw or "").strip()
    if not text:
        return ()
    return tuple(part.strip() for part in text.split(",") if part.strip())


def load_cron_store_settings(config: dict[str, Any] | None = None) -> CronStoreSettings:
    cfg = config if isinstance(config, dict) else {}
    cron = _as_mapping(_as_mapping(cfg.get("gateway")).get("cron"))
    backend = str(cron.get("store_backend") or "file").strip().lower() or "file"
    prefix = str(cron.get("etcd_prefix") or _DEFAULT_PREFIX).strip() or _DEFAULT_PREFIX
    return CronStoreSettings(
        backend=backend,
        endpoints=_parse_endpoints(cron.get("etcd_endpoints")),
        prefix=prefix,
        file_path=get_cron_jobs_path(),
    )


def restore_migrated_cron_etcd_settings(config_path: Path) -> None:
    """Recover etcd settings from the first partition-migration conflict backup.

    Keep the backup after recovery and mark it consumed so a later switch to
    local file storage does not reapply the old settings.
    """
    backup = config_path.with_name(f"{config_path.name}.migrated")
    restored = backup.with_name(f"{backup.name}.restored")
    if not backup.is_file() or restored.exists():
        return

    from agent_gateway.common.config import config_write_lock, dump_yaml_round_trip, load_yaml_round_trip

    try:
        with config_write_lock(config_path=config_path):
            if not backup.is_file() or restored.exists():
                return
            legacy = load_yaml_round_trip(backup)
            legacy_store = load_cron_store_settings(legacy)
            if legacy_store.backend != "etcd" or not legacy_store.endpoints:
                return

            current = load_yaml_round_trip(config_path)
            if not isinstance(current, dict):
                return
            current_store = load_cron_store_settings(current)
            if (current_store.backend, current_store.endpoints, current_store.prefix) != (
                "file", (), _DEFAULT_PREFIX,
            ):
                return
            gateway = current.setdefault("gateway", {})
            if not isinstance(gateway, dict):
                return
            cron = gateway.setdefault("cron", {})
            if not isinstance(cron, dict):
                return

            legacy_cron = legacy["gateway"]["cron"]
            cron.update(
                store_backend="etcd",
                etcd_endpoints=legacy_cron["etcd_endpoints"],
                etcd_prefix=legacy_cron.get("etcd_prefix") or _DEFAULT_PREFIX,
            )
            dump_yaml_round_trip(config_path, current)
            backup.rename(restored)
    except (OSError, LockException, YAMLError):
        logger.warning("Could not restore migrated cron etcd settings", exc_info=True)
