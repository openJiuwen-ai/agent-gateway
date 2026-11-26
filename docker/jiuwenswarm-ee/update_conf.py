#!/usr/bin/env python3
"""企业版 gateway config-gateway.yaml 生成器
用法：python3 update_conf.py [config_dir]（缺省 = ~/.jiuwenswarm/config；config-gateway.yaml 文件名固定；JIUWENSWARM_GATEWAY_CONFIG_DIR 仅支持绝对路径）
模板：agent_gateway/resources/config.yaml（模板资源名未随落盘名改名）
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from ruamel.yaml import YAML

logger = logging.getLogger("update_conf")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent  # 脚本位于 docker/jiuwenswarm-ee/，需上溯两层才是仓库根
SRC_FILE = REPO_ROOT / "agent_gateway" / "resources" / "config.yaml"
DEFAULT_CONFIG_DIR = Path.home() / ".jiuwenswarm" / "config"


def config_dir() -> Path:
    """配置目录：CLI 参数（argv[1]）> ``JIUWENSWARM_GATEWAY_CONFIG_DIR`` > 默认目录。

    环境变量仅支持绝对路径：相对值直接报错退出，不静默按进程 cwd 解析——
    与运行时读取侧 ``agent_gateway/common/paths.get_config_file`` 的取值口径
    保持一致，避免生成物与 gateway 实际加载路径分裂。
    """
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).expanduser()
    env = os.getenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", "").strip()
    if env:
        override = Path(env).expanduser()
        if not override.is_absolute():
            logger.error(
                "JIUWENSWARM_GATEWAY_CONFIG_DIR 必须为绝对路径: %s", override
            )
            raise SystemExit(2)
        return override
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
    dest = config / "config-gateway.yaml"
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
