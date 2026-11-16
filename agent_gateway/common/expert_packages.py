# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""专家包的线上约定。

只规范化 kind、id、体积和报文。不读产品形态。
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

METADATA_KEY = "expert_packages"
HEADER_NAME = "X-Jiuwenswarm-Expert-Packages"
MAX_EXPERT_PACKAGES_JSON_BYTES = 16 * 1024

# 企业版目录不展示的 Hub asset id。公共层只保留这一份，不判断产品形态。
HIDDEN_HUB_ASSET_IDS = frozenset(
    {
        "794db1bfe8884d6897d62d3584894a59",  # 鸿蒙 App 开发专家
        "f633b145c3ab4e42b89e36fa369b2c00",  # 健康生活顾问
        "32c223d69b834c35b40bc3935c6f9f67",  # 旅游规划专家团
        "359f38ea0e77416b8d96d23ae2c12be5",  # 身材管理专家团
    }
)

_CANONICAL_KINDS = {
    "agent_template": "agent_template",
    "agent_templates": "agent_template",
    "agent_group": "agent_group",
    "agent_groups": "agent_group",
}


def canonical_kind(kind: str) -> str | None:
    return _CANONICAL_KINDS.get(str(kind or "").strip())


def normalize_packages(raw: Any) -> list[dict[str, Any]]:
    """收成 ``[{kind, id}]``。重复的 kind+id 去掉，非法结构拒绝。其余字段忽略。"""
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise ValueError("packages must be a list")
    seen: set[tuple[str, str]] = set()
    packages: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("expert package entry must be an object")
        kind = canonical_kind(str(item.get("kind") or ""))
        if kind is None:
            raise ValueError("kind must be agent_template or agent_group")
        package_id = str(item.get("id") or "").strip()
        if (
            not package_id
            or package_id in {".", ".."}
            or "/" in package_id
            or "\\" in package_id
            or len(package_id) > 128
        ):
            raise ValueError("package id is invalid")
        key = (kind, package_id)
        if key in seen:
            continue
        seen.add(key)
        packages.append({"kind": kind, "id": package_id})
    return packages


def _packages_json(packages: list[dict[str, Any]]) -> bytes:
    return json.dumps(
        packages,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def ensure_packages_fit(packages: list[dict[str, Any]]) -> None:
    """专家包 JSON 超过 16KiB 时拒绝。"""
    if len(_packages_json(packages)) > MAX_EXPERT_PACKAGES_JSON_BYTES:
        raise ValueError("expert packages exceed 16384 bytes")


def encode_expert_packages_header(packages: list[dict[str, Any]]) -> str:
    """把专家包编成内部 HTTP 头。结构非法或过大时改发空列表。"""
    try:
        payload = normalize_packages(packages)
        raw = _packages_json(payload)
        if len(raw) > MAX_EXPERT_PACKAGES_JSON_BYTES:
            raise ValueError("expert packages exceed 16384 bytes")
    except ValueError:
        logger.warning("[expert_packages] header encode failed, sending empty")
        raw = b"[]"
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_expert_packages_header(value: str | None) -> list[dict[str, Any]]:
    """还原内部 HTTP 头。缺失或结构损坏时为空列表。"""
    if value is None or not str(value).strip():
        return []
    encoded = str(value).strip()
    try:
        encoded_bytes = encoded.encode("ascii")
        padding = b"=" * (-len(encoded_bytes) % 4)
        raw = base64.b64decode(encoded_bytes + padding, altchars=b"-_", validate=True)
        if len(raw) > MAX_EXPERT_PACKAGES_JSON_BYTES:
            raise ValueError("expert packages exceed 16384 bytes")
        decoded = json.loads(raw.decode("utf-8"))
        return normalize_packages(decoded)
    except (ValueError, UnicodeError, json.JSONDecodeError):
        logger.warning("[expert_packages] header rejected, treating as empty")
        return []
