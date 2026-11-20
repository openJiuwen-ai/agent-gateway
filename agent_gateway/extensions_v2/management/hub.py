# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Resolve and download Gateway-domain extension packages from SkillHub.

The Gateway owns ``package.domain: gateway`` packages, so their Hub installs
must complete here: query the Hub asset, download the published ZIP, verify its
SHA-256 checksum, and let ``GatewayPackageManager.enable_version`` activate it.
Forwarding these assets to the AgentServer cannot work because the AgentServer
refuses to activate a Gateway-domain package (``EXTENSION_DOMAIN_MISMATCH``).

The module is deliberately transport-only: no caching, no registry writes.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx

DEFAULT_HUB_BASE_URL = "https://swarmskills.openjiuwen.com"
SYSTEM_PLUGIN_KIND = "system_plugin"
DOMAIN_GATEWAY = "gateway"

_MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024
_DEFAULT_TIMEOUT = 30.0
_DEFAULT_ALLOWED_HOSTS = (
    "openjiuwen-market.obs.*.myhuaweicloud.com",
    "*.obs.*.myhuaweicloud.com",
    "127.0.0.1",
    "localhost",
)
_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}


class HubUnavailable(RuntimeError):
    """Raised when the Hub cannot be reached or answers with a malformed payload."""


class HubDownloadError(RuntimeError):
    """Raised when a Hub artifact fails URL, size, checksum, or ZIP validation."""


@dataclass(frozen=True)
class HubPackage:
    """One published Gateway package version resolved through the Hub."""

    asset_id: str
    package_id: str
    version: str
    download_url: str
    checksum_sha256: str


def _allow_insecure_http() -> bool:
    return os.getenv("TEAM_SKILLS_HUB_ALLOW_INSECURE_HTTP", "").strip().lower() in _TRUE_VALUES


def hub_base_url() -> str:
    """Return the configured Hub endpoint (SkillHub compatible API)."""

    configured = os.getenv("TEAM_SKILLS_HUB_BASE_URL", "").strip() or DEFAULT_HUB_BASE_URL
    return configured.rstrip("/")


def _timeout() -> float:
    raw = os.getenv("TEAM_SKILLS_HUB_TIMEOUT", "").strip()
    try:
        return max(1.0, float(raw)) if raw else _DEFAULT_TIMEOUT
    except ValueError:
        return _DEFAULT_TIMEOUT


def _headers() -> dict[str, str] | None:
    system_token = os.getenv("TEAM_SKILLS_HUB_SYSTEM_TOKEN", "").strip()
    if system_token:
        return {"X-System-Token": system_token}
    user_token = os.getenv("TEAM_SKILLS_HUB_USER_TOKEN", "").strip()
    if user_token:
        return {"Authorization": f"Bearer {user_token}"}
    return None


async def _get_data(path: str, params: dict[str, str] | None = None) -> dict:
    """GET one Hub API path and unwrap the ``{code, data}`` envelope."""

    url = f"{hub_base_url()}{path}"
    try:
        async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=False) as client:
            response = await client.get(url, params=params, headers=_headers())
    except Exception as exc:  # noqa: BLE001 - transport failures all degrade the same way
        raise HubUnavailable(f"Hub is unreachable: {type(exc).__name__}") from exc
    if response.status_code == 404:
        raise HubUnavailable("Hub asset not found")
    if not response.is_success:
        raise HubUnavailable(f"Hub answered HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise HubUnavailable("Hub response is not JSON") from exc
    if not isinstance(payload, dict):
        raise HubUnavailable("Hub response is not an object")
    try:
        code = int(payload.get("code", 200))
    except (TypeError, ValueError) as exc:
        raise HubUnavailable("Hub response code is malformed") from exc
    if code != 200:
        raise HubUnavailable("Hub response reports a failure")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise HubUnavailable("Hub response data is malformed")
    return data


async def query_gateway_package(asset_id: str, version: str = "") -> HubPackage | None:
    """Resolve a Hub asset to a Gateway package, or ``None`` when the Hub does not own it.

    ``None`` means the asset is unknown to the Hub or belongs to the AgentServer;
    the caller keeps the previous forwarding behaviour in that case.
    """

    wanted = str(asset_id or "").strip()
    if not wanted:
        return None
    page = await _get_data(
        "/api/v1/plugins",
        {"asset_id": wanted, "plugin_type": SYSTEM_PLUGIN_KIND, "page": "1", "page_size": "100"},
    )
    items = page.get("items")
    if not isinstance(items, list):
        raise HubUnavailable("Hub plugin page is malformed")
    item = next(
        (entry for entry in items if isinstance(entry, dict) and entry.get("asset_id") == wanted),
        None,
    )
    if item is None:
        return None
    target_version = str(version or "").strip() or str(item.get("public_latest_version") or "").strip()
    package_id = str(item.get("name") or "").strip()
    if not target_version or not package_id:
        return None
    detail = await _get_data(
        f"/api/v1/plugins/{quote(wanted, safe='')}/versions/{quote(target_version, safe='')}"
    )
    profile = detail.get("package_profile")
    domain = ""
    if isinstance(profile, dict):
        domain = str(profile.get("domain") or "").strip().lower()
    if domain != DOMAIN_GATEWAY:
        # An omitted domain keeps Core's default, which is agentserver.
        return None
    artifact = await _get_data(
        f"/api/v1/artifacts/{quote(wanted, safe='')}",
        {"version": target_version, "is_cli_download": "false"},
    )
    download_url = str(artifact.get("download_url") or "").strip()
    checksum = str(artifact.get("checksum_sha256") or "").strip().lower()
    resolved_version = str(artifact.get("version") or "").strip() or target_version
    if not download_url or not checksum:
        raise HubUnavailable("Hub artifact is missing download_url or checksum_sha256")
    return HubPackage(
        asset_id=wanted,
        package_id=str(artifact.get("name") or package_id).strip() or package_id,
        version=resolved_version,
        download_url=download_url,
        checksum_sha256=checksum,
    )


async def download_package(package: HubPackage, destination: Path) -> Path:
    """Download one package ZIP to ``destination`` after host, size, checksum and ZIP checks."""

    _assert_download_url_allowed(package.download_url)
    try:
        async with httpx.AsyncClient(timeout=max(60.0, _timeout()), follow_redirects=False) as client:
            async with client.stream("GET", package.download_url) as response:
                response.raise_for_status()
                declared = response.headers.get("content-length")
                if declared and declared.isdigit() and int(declared) > _MAX_DOWNLOAD_BYTES:
                    raise HubDownloadError("Hub ZIP exceeds the download limit")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > _MAX_DOWNLOAD_BYTES:
                        raise HubDownloadError("Hub ZIP exceeds the download limit")
    except HubDownloadError:
        raise
    except Exception as exc:  # noqa: BLE001 - surface every transport failure the same way
        raise HubDownloadError(f"Hub download failed: {type(exc).__name__}") from exc
    if not body:
        raise HubDownloadError("Hub download is empty")
    if not re.fullmatch(r"[0-9a-f]{64}", package.checksum_sha256):
        raise HubDownloadError("Hub checksum is not a SHA-256 hex digest")
    if hashlib.sha256(bytes(body)).hexdigest() != package.checksum_sha256:
        raise HubDownloadError("Hub download checksum mismatch")
    if not bytes(body).startswith(b"PK"):
        raise HubDownloadError("Hub download is not a ZIP archive")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(bytes(body))
    return destination


def _allowed_hosts() -> tuple[str, ...]:
    raw = os.getenv("TEAM_SKILLS_HUB_ALLOWED_DOWNLOAD_HOSTS", "").strip()
    if not raw:
        return _DEFAULT_ALLOWED_HOSTS
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


def _host_matches(host: str, rule: str) -> bool:
    if rule.startswith("."):
        return host.endswith(rule)
    host_parts = host.split(".")
    rule_parts = rule.split(".")
    if len(host_parts) != len(rule_parts):
        return False
    return all(expected == "*" or actual == expected for actual, expected in zip(host_parts, rule_parts))


def _assert_download_url_allowed(url: str) -> None:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise HubDownloadError("Hub download_url must use HTTP or HTTPS")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise HubDownloadError("Hub download_url has no host")
    if (
        parsed.scheme == "http"
        and host not in {"localhost", "127.0.0.1", "::1"}
        and not _allow_insecure_http()
    ):
        raise HubDownloadError("Hub downloads must use HTTPS outside loopback")
    if not any(_host_matches(host, rule) for rule in _allowed_hosts()):
        raise HubDownloadError(f"Hub download host is not allowlisted: {host}")
