#!/usr/bin/env bash
# agent-gateway 容器启动入口
# 流程：env 驱动生成配置（update_conf.py）→ 启动 gateway
# 配置目录等环境变量由 k8s envFrom 注入
set -euo pipefail

echo "[start.sh] JIUWENSWARM_EDITION='${JIUWENSWARM_EDITION:-}'"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[start.sh] generating config via update_conf.py"
python3 "${SCRIPT_DIR}/update_conf.py"

exec agent-gateway "$@"
