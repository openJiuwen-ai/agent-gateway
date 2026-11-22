# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Installed splash and appearance providers, read the way the Gateway reads them (S_09 section 4).

The Web UI server asks this before the Gateway is reachable, so the first paint knows whether a
splash may play and whether the startup gate should wait for appearance parts. It reuses the
Gateway configuration, the loader's manifest rules and the user's splash preferences, and never
runs package code. The Gateway's own answers stay final: the browser confirms with them once connected.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from types import SimpleNamespace

from agent_gateway.extensions_v2.config import GatewayExtensionConfig, load_gateway_config
from agent_gateway.extensions_v2.loader import DOMAIN_GATEWAY, discover_package_roots, read_package_manifest

logger = logging.getLogger(__name__)


def _installed(config: GatewayExtensionConfig | None, what: str,
               declares: Callable[[GatewayExtensionConfig], Callable[[dict], bool]]) -> list[str]:
    """Ids of installed Gateway packages whose manifest passes the predicate built for this config."""
    try:
        config = config if config is not None else load_gateway_config()
    except Exception:  # noqa: BLE001 - a bad config means no providers, not a failed Web UI start
        logger.exception("[GatewayExtensions] invalid configuration; no %s providers", what)
        return []
    if not config.enabled:
        return []
    predicate = declares(config)
    providers: list[str] = []
    for root in discover_package_roots(config.install_dir):
        try:
            manifest = read_package_manifest(root)
            if manifest["domain"] != DOMAIN_GATEWAY:
                continue
            declared = predicate(manifest)
        except ValueError as exc:
            logger.debug("[GatewayExtensions] %s is not a %s candidate: %s", root, what, exc)
            continue
        if declared:
            providers.append(manifest["package_id"])
    return providers


def installed_splash_providers(config: GatewayExtensionConfig | None = None) -> list[str]:
    """Return the ids of installed Gateway packages with an enabled ``application.splash``."""

    from agent_gateway.extensions_v2.webui.bridge import BrowserBridge
    from agent_gateway.extensions_v2.webui.host_settings import SplashPreferences

    def declares(config: GatewayExtensionConfig) -> Callable[[dict], bool]:
        preferences = SplashPreferences(config.data_dir)
        preferences.load()
        return lambda manifest: (
            BrowserBridge.splash_declaration(SimpleNamespace(ui=manifest["ui"])) is not None
            and preferences.enabled(manifest["package_id"])
        )

    return _installed(config, "splash", declares)


def installed_appearance_providers(config: GatewayExtensionConfig | None = None) -> list[str]:
    """Return the ids of installed Gateway packages that declare ``application.appearance``."""

    from agent_gateway.extensions_v2.webui.bridge import BrowserBridge

    return _installed(config, "appearance",
                      lambda config: lambda manifest: BrowserBridge.appearance_declaration(
                          SimpleNamespace(ui=manifest["ui"])))
