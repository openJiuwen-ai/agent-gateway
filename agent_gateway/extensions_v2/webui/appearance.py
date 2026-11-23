# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Controlled appearance: part validation and per-connection layers (S_08).

The appearance is made of parts (colors, identity, welcome, mascot). Each package keeps its own
latest value of each part on each browser connection; per part the package on the highest layer
wins, so packages that provide different parts are shown together.

The browser copy of the validation lives in ``features/webui/appearance``; both
sides run the shared cases in ``gatewayAppearanceCases.json``.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Callable, Mapping
from typing import Any

from agent_gateway.extensions_v2.webui.component_rules import valid_media_url

PART_COLORS = "colors"
PART_IDENTITY = "identity"
PART_WELCOME = "welcome"
PART_MASCOT = "mascot"
#: Every part, in snapshot order.
PARTS = (PART_COLORS, PART_IDENTITY, PART_WELCOME, PART_MASCOT)
#: Work modes the welcome text can address; host defaults cover the rest.
WORK_MODES = frozenset({"work", "code", "design"})
LANGUAGES = frozenset({"zh", "en"})
MAX_NAME_CHARS = 32
MAX_WELCOME_CHARS = 64
IMAGE_SUFFIXES = frozenset({".png", ".webp", ".svg", ".gif"})
#: The brand name in the welcome line: one colour, or two for a left-to-right gradient.
MAX_HIGHLIGHT_COLORS = 2
MIN_HOVER_MS = 500
MAX_HOVER_MS = 10_000
#: Largest image file of each image field, checked when the part is submitted.
IMAGE_LIMITS = {
    (PART_IDENTITY, "logo"): 1 << 20,
    (PART_IDENTITY, "favicon"): 1 << 20,
    (PART_MASCOT, "src"): 4 << 20,
    (PART_MASCOT, "hoverSrc"): 4 << 20,
}

APPEARANCE_FIELDS = frozenset({"accent", "surface", "surfaceOpacity", "tokens"})
SURFACE_MODES = frozenset({"opaque", "translucent"})
#: Decorative variables a package may set directly; text and surfaces stay host-owned.
TOKEN_NAMES = frozenset({
    "--color-scrollbar-thumb",
    "--color-sidebar-nav-active",
    "--color-sidebar-nav-active-hover",
    "--color-conversation-active-surface",
    "--color-chat-accent-subtle",
    "--color-action-primary-subtle",
    "--color-action-primary-glow",
    "--color-border-accent",
})
MIN_SURFACE_OPACITY = 0.3

_ACCENT = re.compile(r"#[0-9A-Fa-f]{6}")
_HEX_COLOR = re.compile(r"#(?:[0-9A-Fa-f]{3}|[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})")
_CHANNEL = r" *([0-9]{1,3}) *"
_ALPHA = r" *(0(?:\.[0-9]+)?|1(?:\.0+)?|\.[0-9]+) *"
_RGB = re.compile(rf"rgb\({_CHANNEL},{_CHANNEL},{_CHANNEL}\)")
_RGBA = re.compile(rf"rgba\({_CHANNEL},{_CHANNEL},{_CHANNEL},{_ALPHA}\)")


def valid_color(value: Any) -> bool:
    """Accept hex colors and rgb()/rgba() with integer channels up to 255."""
    if not isinstance(value, str):
        return False
    if _HEX_COLOR.fullmatch(value):
        return True
    match = _RGB.fullmatch(value) or _RGBA.fullmatch(value)
    return match is not None and all(int(channel) <= 255 for channel in match.groups()[:3])


def validate_appearance(value: Any) -> dict[str, Any]:
    """Return the parts an appearance object sets or withdraws, or raise ``ValueError``.

    The result maps each named part to its normalized value, or to ``None`` when the call withdraws
    the caller's own value of that part. An object made only of color fields is the colors part.
    """
    if not isinstance(value, Mapping) or not value:
        raise ValueError("appearance must be a non-empty object")
    keys = set(value)
    unknown = keys - APPEARANCE_FIELDS - set(PARTS)
    if unknown:
        raise ValueError(f"unknown appearance fields: {sorted(unknown)}")
    if keys <= APPEARANCE_FIELDS:
        return {PART_COLORS: validate_colors(value)}
    if keys & APPEARANCE_FIELDS:
        raise ValueError("color fields and appearance parts cannot be mixed")
    return {part: None if value[part] is None else _PART_VALIDATORS[part](value[part])
            for part in PARTS if part in value}


def validate_colors(value: Any) -> dict[str, Any]:
    """Return a plain copy of a valid colors part or raise ``ValueError``."""
    if not isinstance(value, Mapping) or not value:
        raise ValueError("colors must be a non-empty object")
    unknown = set(value) - APPEARANCE_FIELDS
    if unknown:
        raise ValueError(f"unknown color fields: {sorted(unknown)}")
    result: dict[str, Any] = {}
    if "accent" in value:
        if not isinstance(value["accent"], str) or not _ACCENT.fullmatch(value["accent"]):
            raise ValueError("accent must be #RRGGBB")
        result["accent"] = value["accent"]
    if "surface" in value:
        if value["surface"] not in SURFACE_MODES:
            raise ValueError("surface must be 'opaque' or 'translucent'")
        result["surface"] = value["surface"]
    if "surfaceOpacity" in value:
        opacity = value["surfaceOpacity"]
        if (type(opacity) not in (int, float) or not math.isfinite(opacity)
                or not MIN_SURFACE_OPACITY <= opacity <= 1):
            raise ValueError("surfaceOpacity must be between 0.3 and 1")
        if value.get("surface") != "translucent":
            raise ValueError("surfaceOpacity requires surface 'translucent'")
        result["surfaceOpacity"] = opacity
    if "tokens" in value:
        tokens = value["tokens"]
        if not isinstance(tokens, Mapping) or not tokens:
            raise ValueError("tokens must be a non-empty object")
        for name, color in tokens.items():
            if name not in TOKEN_NAMES:
                raise ValueError(f"token not allowed: {name}")
            if not valid_color(color):
                raise ValueError(f"invalid color for {name}")
        result["tokens"] = dict(tokens)
    return result


def _text(value: Any, limit: int, what: str) -> str:
    """Plain single-line text of 1 to ``limit`` characters (code points), trimmed."""
    if not isinstance(value, str):
        raise ValueError(f"{what} must be a string")
    text = value.strip()
    if not 0 < len(text) <= limit or any(unicodedata.category(char) in ("Cc", "Zl", "Zp") for char in text):
        raise ValueError(f"{what} must be 1 to {limit} characters on one line")
    return text


def _localized(value: Any, limit: int, what: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or not value or set(value) - LANGUAGES:
        raise ValueError(f"{what} must be an object with zh, en or both")
    return {language: _text(value[language], limit, f"{what}.{language}")
            for language in ("zh", "en") if language in value}


def _image(value: Any, what: str) -> str:
    """A package or data asset path naming a supported image; ownership is checked by the bridge."""
    if not isinstance(value, str) or not valid_media_url(value):
        raise ValueError(f"{what} must be a package asset path")
    path = re.split(r"[?#]", value, maxsplit=1)[0].lower()
    if not path.endswith(tuple(IMAGE_SUFFIXES)):
        raise ValueError(f"{what} must be a .png, .webp, .svg or .gif image")
    return value


def validate_identity(value: Any) -> dict[str, Any]:
    """Brand name, logo and tab icon; the tab icon defaults to the logo."""
    if not isinstance(value, Mapping) or not value or set(value) - {"name", "logo", "favicon"}:
        raise ValueError("identity accepts name, logo and favicon")
    result: dict[str, Any] = {}
    if "name" in value:
        result["name"] = _text(value["name"], MAX_NAME_CHARS, "identity.name")
    if "logo" in value:
        result["logo"] = _image(value["logo"], "identity.logo")
    if "favicon" in value:
        result["favicon"] = _image(value["favicon"], "identity.favicon")
    elif "logo" in result:
        result["favicon"] = result["logo"]
    return result


def validate_welcome(value: Any) -> dict[str, Any]:
    """The second half of the welcome line, per work mode and as a default, and the brand name's colour."""
    if not isinstance(value, Mapping) or not value or set(value) - {"default", "modes", "highlight"}:
        raise ValueError("welcome accepts default, modes and highlight")
    result: dict[str, Any] = {}
    if "default" in value:
        result["default"] = _localized(value["default"], MAX_WELCOME_CHARS, "welcome.default")
    if "modes" in value:
        modes = value["modes"]
        if not isinstance(modes, Mapping) or not modes or set(modes) - WORK_MODES:
            raise ValueError("welcome.modes accepts work, code and design")
        result["modes"] = {mode: _localized(modes[mode], MAX_WELCOME_CHARS, f"welcome.modes.{mode}")
                           for mode in sorted(modes)}
    if "highlight" in value:
        colors = value["highlight"]
        if (not isinstance(colors, list) or not 0 < len(colors) <= MAX_HIGHLIGHT_COLORS
                or not all(isinstance(color, str) and _ACCENT.fullmatch(color) for color in colors)):
            raise ValueError("welcome.highlight must be one or two #RRGGBB colours")
        result["highlight"] = list(colors)
    return result


def validate_mascot(value: Any) -> dict[str, Any]:
    """A still image and, optionally, a hover image shown for ``hoverMs``."""
    if not isinstance(value, Mapping) or "src" not in value or set(value) - {"src", "hoverSrc", "hoverMs"}:
        raise ValueError("mascot needs src and accepts hoverSrc and hoverMs")
    if ("hoverSrc" in value) != ("hoverMs" in value):
        raise ValueError("mascot.hoverSrc and mascot.hoverMs go together")
    result: dict[str, Any] = {"src": _image(value["src"], "mascot.src")}
    if "hoverSrc" in value:
        duration = value["hoverMs"]
        if type(duration) is not int or not MIN_HOVER_MS <= duration <= MAX_HOVER_MS:
            raise ValueError(f"mascot.hoverMs must be an integer from {MIN_HOVER_MS} to {MAX_HOVER_MS}")
        result["hoverSrc"] = _image(value["hoverSrc"], "mascot.hoverSrc")
        result["hoverMs"] = duration
    return result


_PART_VALIDATORS: dict[str, Callable[[Any], dict[str, Any]]] = {
    PART_COLORS: validate_colors,
    PART_IDENTITY: validate_identity,
    PART_WELCOME: validate_welcome,
    PART_MASCOT: validate_mascot,
}


def part_images(part: str, value: Mapping[str, Any]) -> list[tuple[str, str, int]]:
    """``(field, path, byte limit)`` of each image a normalized part names."""
    return [(field, value[field], limit) for (owner, field), limit in IMAGE_LIMITS.items()
            if owner == part and field in value]


class AppearanceLayers:
    """Each browser connection's appearance: every package's latest parts and the frames sent.

    ``rank`` orders packages into layers, a higher rank being a higher layer; per part the
    highest package that holds the part is shown (S_08 section 5).
    """

    def __init__(self, rank: Callable[[str], int]) -> None:
        self._rank = rank
        # connection -> package -> part -> normalized value
        self._parts: dict[str, dict[str, dict[str, Any]]] = {}
        # connection -> (revision, snapshot) of the newest frame that reached the browser
        self._delivered: dict[str, tuple[int, dict[str, Any]]] = {}
        self._revisions: dict[str, int] = {}
        # Packages whose appearance frame is being sent, per connection: a release must
        # send a newer frame there too, because the in-flight one may still show its parts.
        self._sending: dict[str, set[str]] = {}

    def parts_of(self, connection_id: str, package_id: str) -> dict[str, Any]:
        """The parts this package currently holds on the connection."""
        return dict(self._parts.get(connection_id, {}).get(package_id, {}))

    def compose(self, connection_id: str, pending: tuple[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
        """The snapshot of a connection; ``pending`` replaces one package's parts for a call in progress."""
        packages = dict(self._parts.get(connection_id, {}))
        if pending is not None:
            package_id, parts = pending
            if parts:
                packages[package_id] = dict(parts)
            else:
                packages.pop(package_id, None)
        snapshot: dict[str, Any] = {}
        for part in PARTS:
            holders = [package_id for package_id, parts in packages.items() if part in parts]
            if holders:
                top = max(holders, key=lambda package_id: (self._rank(package_id), package_id))
                snapshot[part] = {"package_id": top, "value": packages[top][part]}
        return snapshot

    def delivered(self, connection_id: str) -> dict[str, Any]:
        """The snapshot of the newest frame that reached the browser; empty means host defaults."""
        return self._delivered.get(connection_id, (0, {}))[1]

    def provider(self, connection_id: str, part: str = PART_COLORS) -> str | None:
        """The package whose value of ``part`` the browser shows."""
        entry = self.delivered(connection_id).get(part)
        return entry["package_id"] if entry else None

    def revision(self, connection_id: str) -> int:
        return self._revisions.get(connection_id, 0)

    def next_revision(self, connection_id: str) -> int:
        revision = self._revisions.get(connection_id, 0) + 1
        self._revisions[connection_id] = revision
        return revision

    def commit(self, connection_id: str, package_id: str, parts: Mapping[str, Any]) -> None:
        """Keep a package's parts on the connection; empty parts forget the package there."""
        packages = self._parts.setdefault(connection_id, {})
        if parts:
            packages[package_id] = dict(parts)
        else:
            packages.pop(package_id, None)
        if not packages:
            del self._parts[connection_id]

    def record_delivery(self, connection_id: str, revision: int, snapshot: Mapping[str, Any]) -> None:
        """Note a frame that reached the browser unless a newer one already did."""
        if revision >= self._delivered.get(connection_id, (0, {}))[0]:
            self._delivered[connection_id] = (revision, dict(snapshot))

    def begin_send(self, connection_id: str, package_id: str) -> None:
        """Record that a frame of this package is on its way to the connection."""
        self._sending.setdefault(connection_id, set()).add(package_id)

    def finish_send(self, connection_id: str, package_id: str) -> bool:
        """End an in-flight send; False when the package was released meanwhile."""
        packages = self._sending.get(connection_id)
        if not packages or package_id not in packages:
            return False
        packages.discard(package_id)
        if not packages:
            del self._sending[connection_id]
        return True

    def release(self, package_id: str) -> list[tuple[str, int]]:
        """Forget a released package's parts and reserve one revision per connection that may show them.

        Connections that only have a frame of the package in flight get one as well. The snapshot
        itself is composed when the frame is sent, so it reflects every call that finished before.
        """
        affected: list[str] = []
        for connection_id, packages in list(self._parts.items()):
            if packages.pop(package_id, None) is not None:
                affected.append(connection_id)
                if not packages:
                    del self._parts[connection_id]
        for connection_id, packages in list(self._sending.items()):
            if package_id in packages:
                packages.discard(package_id)
                if not packages:
                    del self._sending[connection_id]
                if connection_id not in affected:
                    affected.append(connection_id)
        return [(connection_id, self.next_revision(connection_id)) for connection_id in affected]

    def drop_connection(self, connection_id: str) -> None:
        self._parts.pop(connection_id, None)
        self._delivered.pop(connection_id, None)
        self._revisions.pop(connection_id, None)
        self._sending.pop(connection_id, None)

    def clear(self) -> None:
        self._parts.clear()
        self._delivered.clear()
        self._revisions.clear()
        self._sending.clear()
