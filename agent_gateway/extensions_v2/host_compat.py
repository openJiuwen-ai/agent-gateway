# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Host version compatibility declared by Gateway extension packages.

A Gateway package may declare the JiuwenSwarm releases it supports in
``manifest.json``::

    "compatibility": {"swarm": "0.2.9.beta3"}

The value is either an exact release tag (``0.2.9.beta3``) or a whitespace
separated comparison list (``>=0.2.10``). The running host version comes from
the installed ``workswarm`` distribution; ``JIUWENSWARM_HOST_VERSION`` overrides
it for tests and deployments without distribution metadata.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.extension import ExtensionFailure

HOST_VERSION_ENV = "JIUWENSWARM_HOST_VERSION"

_VERSION_RE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)(?:[.-]?(a|b|rc|alpha|beta)\.?(\d+))?$",
    re.IGNORECASE,
)
_CLAUSE_RE = re.compile(r"^(>=|<=|!=|==|>|<)?\s*(\S+)$")
_STAGE_RANK = {"a": 0, "alpha": 0, "b": 1, "beta": 1, "rc": 2, "": 3}


def host_version() -> str | None:
    """Return the running JiuwenSwarm version, or None when it cannot be read."""

    override = os.environ.get(HOST_VERSION_ENV, "").strip()
    if override:
        return override
    for distribution in ("workswarm", "agent-gateway"):
        try:
            from importlib.metadata import version

            return version(distribution).strip()
        except Exception:  # noqa: BLE001 - a source checkout may not be installed
            continue
    return None


def numeric_host_version() -> str | None:
    """Return ``MAJOR.MINOR.PATCH`` for Core's ``requires.host`` checks."""

    raw = host_version()
    parsed = _parse_version(raw) if raw else None
    if parsed is None:
        return None
    return f"{parsed[0]}.{parsed[1]}.{parsed[2]}"


def swarm_spec(document: Mapping[str, Any]) -> str | None:
    """Read ``compatibility.swarm`` from a raw manifest."""

    compatibility = document.get("compatibility")
    if not isinstance(compatibility, Mapping):
        return None
    value = compatibility.get("swarm")
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def swarm_spec_matches(host: str, specifier: str) -> bool:
    """Return whether ``host`` satisfies every clause in ``specifier``.

    Raises:
        ValueError: The host version or a clause cannot be parsed.
    """

    parsed_host = _parse_version(host)
    if parsed_host is None:
        raise ValueError(f"cannot parse host version {host!r}")
    text = specifier.strip()
    if not text:
        return True
    for clause in text.split():
        match = _CLAUSE_RE.fullmatch(clause)
        if match is None:
            raise ValueError(f"cannot parse version specifier clause {clause!r}")
        operator = match.group(1) or "=="
        parsed_other = _parse_version(match.group(2))
        if parsed_other is None:
            raise ValueError(f"cannot parse version specifier clause {clause!r}")
        if not _compare(parsed_host, operator, parsed_other):
            return False
    return True


def is_swarm_compatible(specifier: str | None) -> bool:
    """Return True when the package's declaration accepts the running host."""

    if specifier is None:
        return True
    try:
        ensure_swarm_compatible(specifier)
    except ExtensionFailure:
        return False
    return True


def ensure_swarm_compatible(specifier: str | None) -> None:
    """Raise a Core extension failure when the running host is not supported."""

    if specifier is None:
        return
    host = host_version()
    if host is None:
        raise ExtensionFailure(
            StatusCode.EXTENSION_HOST_VERSION_MISSING,
            msg=f"requires swarm {specifier}",
        )
    try:
        matched = swarm_spec_matches(host, specifier)
    except ValueError as exc:
        raise ExtensionFailure(
            StatusCode.EXTENSION_MANIFEST_INVALID,
            msg=f"compatibility.swarm: {exc}",
        ) from exc
    if not matched:
        raise ExtensionFailure(
            StatusCode.EXTENSION_HOST_VERSION_TOO_LOW,
            msg=f"requires swarm {specifier}, current host is {host}",
        )


def _parse_version(text: str) -> tuple[int, int, int, int, int] | None:
    match = _VERSION_RE.fullmatch(text.strip())
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups()[:3])
    stage = (match.group(4) or "").lower()
    stage_number = int(match.group(5) or 0)
    return major, minor, patch, _STAGE_RANK[stage], stage_number


def _compare(left: tuple[int, ...], operator: str, right: tuple[int, ...]) -> bool:
    if operator == "==":
        return left == right
    if operator == "!=":
        return left != right
    if operator == ">":
        return left > right
    if operator == ">=":
        return left >= right
    if operator == "<":
        return left < right
    if operator == "<=":
        return left <= right
    raise ValueError(f"unsupported version comparison {operator}")
