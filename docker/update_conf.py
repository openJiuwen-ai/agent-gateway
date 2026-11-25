#!/usr/bin/env python3
"""企业版 gateway config.yaml 生成器
用法：python3 update_conf.py [config_dir]（缺省 = ~/.jiuwenswarm/infra/config；config.yaml 文件名固定）
模板：agent_gateway/resources/config.yaml
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from ruamel.yaml import YAML

logger = logging.getLogger("update_conf")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SRC_FILE = REPO_ROOT / "agent_gateway" / "resources" / "config.yaml"
DEFAULT_CONFIG_DIR = Path.home() / ".jiuwenswarm" / "infra" / "config"


def config_dir() -> Path:
    """配置目录：CLI 参数（argv[1]）> 默认目录（数据根 infra/config）。"""
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).expanduser()
    return DEFAULT_CONFIG_DIR


def setpath(d, dotted_path: str, value) -> None:
    """等价 yq eval '.a.b.c = value'：中间键不存在则创建。"""
    keys = dotted_path.split(".")
    cur = d
    for key in keys[:-1]:
        node = cur.get(key)
        if not isinstance(node, dict):
            node = {}
            cur[key] = node
        cur = node
    cur[keys[-1]] = value


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="[update_conf] %(levelname)s %(message)s")

    config = config_dir()
    dest = config / "config.yaml"
    if not SRC_FILE.is_file():
        logger.error("source not found: %s", SRC_FILE)
        return 1

    parser = YAML()
    parser.preserve_quotes = True
    data = parser.load(SRC_FILE)

    setpath(data, "gateway.session_map_scope", "${GATEWAY_SESSION_MAP_SCOPE:-per_chat_bot}")

    config.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as f:
        parser.dump(data, f)

    logger.info("generated %s (from %s)", dest, SRC_FILE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
