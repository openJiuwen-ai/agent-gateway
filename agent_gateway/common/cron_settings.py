# Keep the cron settings and upgrade rules aligned with the JiuwenSwarm host.
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Resolve and migrate Gateway cron storage configuration."""

from __future__ import annotations

import logging
import os
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
    """Evaluate the first legacy config backup once, retaining its contents."""
    backup = config_path.with_name(f"{config_path.name}.migrated")
    restored = backup.with_name(f"{backup.name}.restored")
    skipped = backup.with_name(f"{backup.name}.skipped")
    if not backup.is_file() or restored.exists() or skipped.exists():
        return

    from agent_gateway.common.config import (
        config_write_lock,
        dump_yaml_round_trip,
        load_yaml_round_trip,
        resolve_env_vars,
    )

    try:
        with config_write_lock(config_path=config_path):
            if not backup.is_file() or restored.exists() or skipped.exists():
                return

            legacy = load_yaml_round_trip(backup)
            current = load_yaml_round_trip(config_path)
            legacy_cron = _migration_cron_section(legacy)
            current_cron = _migration_cron_section(current)
            if legacy_cron is None or current_cron is None:
                logger.warning("Skipping cron etcd recovery because a config document is not a mapping")
                return

            legacy_store = load_cron_store_settings({"gateway": {"cron": resolve_env_vars(legacy_cron)}})
            current_store = load_cron_store_settings({"gateway": {"cron": resolve_env_vars(current_cron)}})

            explicit_backend = (
                current_cron.get("store_backend") == "${CRON_STORE_BACKEND:-file}"
                and bool(os.environ.get("CRON_STORE_BACKEND"))
            )
            if legacy_store.backend != "etcd" or explicit_backend or (
                current_store.backend, current_store.endpoints, current_store.prefix
            ) != ("file", (), _DEFAULT_PREFIX):
                backup.rename(skipped)
                return
            if not legacy_store.endpoints:
                # An endpoint may be supplied by a deployment environment on a later boot.
                return

            current.setdefault("gateway", {}).setdefault("cron", {}).update(
                store_backend=legacy_cron["store_backend"],
                etcd_endpoints=legacy_cron["etcd_endpoints"],
                etcd_prefix=legacy_cron.get("etcd_prefix") or _DEFAULT_PREFIX,
            )
            dump_yaml_round_trip(config_path, current)
            backup.rename(restored)
    except (OSError, LockException, UnicodeError, YAMLError):
        logger.warning("Could not restore migrated cron etcd settings from %s", backup, exc_info=True)


def _migration_cron_section(config: Any) -> dict[str, Any] | None:
    if not isinstance(config, dict):
        return None
    gateway = config.get("gateway", {})
    if not isinstance(gateway, dict):
        return None
    cron = gateway.get("cron", {})
    return cron if isinstance(cron, dict) else None
