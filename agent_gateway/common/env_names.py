# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway 环境变量的新名（``AGENT_GATEWAY_*``）与旧名（``JIUWENSWARM_*``）。

读取侧兼容：先读新名，未设置或为空白时按顺序回退旧名。写入方（JiuwenSwarm
启动器、部署脚本、Gateway 注入给 AgentServer 的变量）仍写旧名，已有部署行为
不变。

新旧名同时设置且取值不同时，路径类变量直接报错，避免 Gateway 与 AgentServer
解析出不同目录；其他变量以新名为准，并告警一次（不输出取值）。空白取值一律
视为未设置，会继续回退后面的名字，也不参与冲突比较。
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import overload

logger = logging.getLogger(__name__)


class EnvVarConflictError(RuntimeError):
    """新旧路径变量同时设置且指向不同路径。"""


@dataclass(frozen=True)
class EnvVar:
    """一个 Gateway 环境变量。

    Attributes:
        name: 新名，``AGENT_GATEWAY_*``。
        legacy: 旧名，按查找顺序排列。
        is_path: 是否为路径；路径类变量的新旧取值冲突时报错。
    """

    name: str
    legacy: tuple[str, ...]
    is_path: bool = False

    @property
    def names(self) -> tuple[str, ...]:
        """新名在前、旧名在后的全部名字。"""
        return (self.name, *self.legacy)


HOME = EnvVar("AGENT_GATEWAY_HOME", ("JIUWENSWARM_HOME",), is_path=True)
DATA_DIR = EnvVar("AGENT_GATEWAY_DATA_DIR", ("JIUWENSWARM_DATA_DIR",), is_path=True)
CONFIG_DIR = EnvVar("AGENT_GATEWAY_CONFIG_DIR", ("JIUWENSWARM_GATEWAY_CONFIG_DIR",), is_path=True)
DOWNLOAD_ASSET_ROOT = EnvVar(
    "AGENT_GATEWAY_DOWNLOAD_ASSET_ROOT",
    ("JIUWENSWARM_DOWNLOAD_ASSET_ROOT",),
    is_path=True,
)
EDITION = EnvVar("AGENT_GATEWAY_EDITION", ("JIUWENSWARM_EDITION",))
SSL_VERIFY = EnvVar("AGENT_GATEWAY_SSL_VERIFY", ("JIUWENSWARM_SSL_VERIFY",))
ENABLE_ORIGIN_CHECK = EnvVar("AGENT_GATEWAY_ENABLE_ORIGIN_CHECK", ("JIUWENSWARM_ENABLE_ORIGIN_CHECK",))
WS_ALLOWED_ORIGIN_HOSTS = EnvVar(
    "AGENT_GATEWAY_WS_ALLOWED_ORIGIN_HOSTS",
    ("JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS",),
)
LOG_MASKING_PROBE = EnvVar(
    "AGENT_GATEWAY_LOG_MASKING_PROBE",
    ("JIUWENCLAW_LOG_MASKING_PROBE", "JIUWENSWARM_LOG_MASKING_PROBE"),
)
FILE_DOWNLOAD_SECRET = EnvVar(
    "AGENT_GATEWAY_FILE_DOWNLOAD_SECRET",
    ("JIUWENSWARM_FILE_DOWNLOAD_SECRET",),
)
REQUEST_EXT_FORWARD_HEADERS = EnvVar(
    "AGENT_GATEWAY_REQUEST_EXT_FORWARD_HEADERS",
    ("JIUWENSWARM_REQUEST_EXT_FORWARD_HEADERS", "JIUWENCLAW_REQUEST_EXT_FORWARD_HEADERS"),
)
SUPERVISOR_PID = EnvVar("AGENT_GATEWAY_SUPERVISOR_PID", ("JIUWENSWARM_SUPERVISOR_PID",))

ALL: tuple[EnvVar, ...] = (
    HOME,
    DATA_DIR,
    CONFIG_DIR,
    DOWNLOAD_ASSET_ROOT,
    EDITION,
    SSL_VERIFY,
    ENABLE_ORIGIN_CHECK,
    WS_ALLOWED_ORIGIN_HOSTS,
    LOG_MASKING_PROBE,
    FILE_DOWNLOAD_SECRET,
    REQUEST_EXT_FORWARD_HEADERS,
    SUPERVISOR_PID,
)

_warned_conflicts: set[str] = set()


def lookup(
    var: EnvVar,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str] | None:
    """返回第一个已设置且非空的名字及其取值，都未设置时返回 ``None``。

    Args:
        var: 要读取的变量。
        environ: 读取的环境，默认 ``os.environ``。

    Returns:
        ``(名字, 取值)``；新旧名都未设置时为 ``None``。

    Raises:
        EnvVarConflictError: 路径类变量的新旧名同时设置且指向不同路径。
    """
    env = os.environ if environ is None else environ
    for name in var.names:
        value = env.get(name)
        if value is None or not value.strip():
            continue
        if name == var.name:
            _check_legacy_conflict(var, value, env)
        return name, value
    return None


@overload
def get(
    var: EnvVar,
    default: str,
    *,
    environ: Mapping[str, str] | None = ...,
) -> str: ...


@overload
def get(
    var: EnvVar,
    default: None = ...,
    *,
    environ: Mapping[str, str] | None = ...,
) -> str | None: ...


def get(
    var: EnvVar,
    default: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """按新名、旧名的顺序读取第一个非空取值；都未设置时返回 *default*。

    Args:
        var: 要读取的变量。
        default: 新旧名都未设置时的返回值。
        environ: 读取的环境，默认 ``os.environ``。

    Returns:
        第一个非空取值，或 *default*。

    Raises:
        EnvVarConflictError: 路径类变量的新旧名同时设置且指向不同路径。
    """
    found = lookup(var, environ=environ)
    return default if found is None else found[1]


def _check_legacy_conflict(
    var: EnvVar,
    value: str,
    env: Mapping[str, str],
) -> None:
    for legacy_name in var.legacy:
        legacy_value = env.get(legacy_name)
        if legacy_value is None or not legacy_value.strip():
            continue
        if _same_value(var, value, legacy_value):
            continue
        if var.is_path:
            raise EnvVarConflictError(
                f"{var.name}={value!r} and {legacy_name}={legacy_value!r} point to different "
                "paths; unset one of them or set both to the same path"
            )
        if var.name not in _warned_conflicts:
            _warned_conflicts.add(var.name)
            logger.warning(
                "%s and %s are both set with different values; using %s",
                var.name,
                legacy_name,
                var.name,
            )
        return


def _same_value(var: EnvVar, left: str, right: str) -> bool:
    if var.is_path:
        return _normalize_path(left) == _normalize_path(right)
    return left.strip() == right.strip()


def _normalize_path(value: str) -> str:
    return os.path.normcase(os.path.normpath(os.path.expanduser(value.strip())))
