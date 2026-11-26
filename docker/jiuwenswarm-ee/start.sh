#!/usr/bin/env bash
# agent-gateway 容器启动入口
# 流程：env 驱动生成配置（update_conf.py）→ 启动 gateway
# 配置目录等环境变量由 k8s envFrom 注入
set -euo pipefail

echo "[start.sh] JIUWENSWARM_EDITION='${JIUWENSWARM_EDITION:-}'"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[start.sh] generating config via update_conf.py"
# 环境注意：必须钉 /usr/local/bin/python3 绝对路径，不能裸用 python3——
#   · 依赖在系统 python 环境：构建期 uv pip install --system 把包装进
#     /usr/lib/python3.11/site-packages（UBI9 系统解释器 /usr/bin/python3.11）
#   · 镜像内有两套 python：UBI9 python-311 自带 /opt/app-root venv（不含系统
#     site-packages），基础镜像用软链 /usr/local/bin/python3 → /usr/bin/python3.11
#     并前置 PATH 兜住构建期解析
#   · 运行时 PATH 不可靠：pod 环境变量（envFrom）覆盖镜像 PATH 后，实测 python3
#     命中 /opt/app-root venv → 缺 ruamel，容器 ModuleNotFoundError 反复重启
#   · /usr/local/bin/python3 是指向系统解释器的软链，恒为装依赖的环境，与
#     运行时 PATH 无关，故显式钉死
if ! /usr/local/bin/python3 "${SCRIPT_DIR}/update_conf.py"; then
    echo "[start.sh] update_conf.py FAILED" >&2
    exit 1
fi

exec agent-gateway "$@"
