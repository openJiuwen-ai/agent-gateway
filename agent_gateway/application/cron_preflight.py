# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Read-only assembly gate for the explicitly enabled sharded personal Cron."""

from __future__ import annotations

from typing import Any

from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.preflight import CronPreflightReport, inspect_cron_store
from agent_gateway.cron.store import FileCronJobStore


async def inspect_controlled_cron(
    ownership: CronOwnership, *, config: dict[str, Any] | None = None,
    store: FileCronJobStore | None = None,
    allow_login_credentials: bool = False,
    allow_project_jobs: bool = True,
) -> CronPreflightReport:
    from agent_gateway.common.config import get_config
    from agent_gateway.common.cron_settings import load_cron_store_settings
    from agent_gateway.enterprise import is_enterprise

    if is_enterprise():
        raise ValueError('sharded Cron requires the personal file backend')
    config = get_config() if config is None else config
    if not isinstance(config, dict):
        raise ValueError('invalid Cron preflight configuration')
    gateway = config.get('gateway', {})
    if not isinstance(gateway, dict) or not isinstance(gateway.get('cron', {}), dict):
        raise ValueError('invalid Cron preflight configuration')
    settings = load_cron_store_settings(config)
    if settings.backend != 'file':
        raise ValueError('sharded Cron requires the personal file backend')
    proactive = config.get('proactive_recommendation', {})
    if not isinstance(proactive, dict):
        raise ValueError('invalid Cron preflight configuration')
    enabled = proactive.get('enabled', False)
    if type(enabled) is not bool:
        raise ValueError('invalid Cron preflight configuration')
    backend = store if store is not None else FileCronJobStore(path=settings.file_path)
    return await inspect_cron_store(
        backend, ownership, proactive_enabled=enabled,
        allow_login_credentials=allow_login_credentials, allow_project_jobs=allow_project_jobs,
    )


async def preflight_controlled_cron(
    ownership: CronOwnership, *, config: dict[str, Any] | None = None,
    store: FileCronJobStore | None = None, allow_login_credentials: bool = False,
    allow_project_jobs: bool = True,
) -> CronPreflightReport:
    """Refuse to activate Cron while an enabled record uses an unsupported capability."""
    report = await inspect_controlled_cron(
        ownership, config=config, store=store, allow_login_credentials=allow_login_credentials,
        allow_project_jobs=allow_project_jobs,
    )
    if not report.ready:
        raise ValueError('Cron preflight rejected unsupported enabled capabilities')
    return report


def check_cron_cli(worker_count: int) -> int:
    """Inspect a static snapshot without preparing a runtime or touching AS."""
    import asyncio
    from dataclasses import asdict
    import json

    import yaml

    from agent_gateway.common.ingress_identity import LocalIdentityPolicy
    from agent_gateway.common.worker_affinity import WorkerManifest

    ownership = CronOwnership(WorkerManifest(tuple(range(worker_count))), LocalIdentityPolicy())
    result: dict[str, Any] = {'scope': 'static_file_only', 'agentserver_checked': False}
    try:
        report = asyncio.run(inspect_controlled_cron(ownership, allow_project_jobs=False))
    except (ValueError, OSError, AttributeError, TypeError, yaml.YAMLError):
        # YAML/parser and OS errors may carry paths or values; do not echo them.
        result.update(ready=False, error='invalid_or_unreadable_cron_configuration_or_snapshot')
    else:
        result.update(asdict(report), ready=report.ready)
    print(json.dumps(result, ensure_ascii=True))
    return 0 if result['ready'] else 1
