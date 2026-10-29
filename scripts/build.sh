#!/usr/bin/env bash
# jiuwenswarm-gateway 打包脚本
# 1. 同步依赖（uv sync，构建本身使用隔离环境）
# 2. 清理历史产物
# 3. 构建 wheel / sdist 到 dist/
#
# 用法:
#   bash scripts/build.sh              # 清理并构建 wheel + sdist
#   bash scripts/build.sh --wheel      # 只构建 wheel
#   bash scripts/build.sh --sdist      # 只构建 sdist
#   bash scripts/build.sh --no-clean   # 保留 build/ 历史产物

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "[build] 项目根目录: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

CLEAN=true
BUILD_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --no-clean) CLEAN=false ;;
        --wheel|--sdist) BUILD_ARGS+=("$arg") ;;
        -h|--help)
            sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[build] 未知参数: $arg" >&2
            exit 2
            ;;
    esac
done

# 1. 同步依赖（缺失 uv 时跳过，构建阶段仍可用 python -m build）
if command -v uv >/dev/null 2>&1; then
    echo "[build] 同步依赖..."
    uv sync
else
    echo "[build] 未找到 uv，跳过依赖同步" >&2
fi

# 2. 清理历史产物
if [[ "$CLEAN" == "true" ]]; then
    echo "[build] 清理 build/ dist/ *.egg-info ..."
    rm -rf build dist ./*.egg-info
fi

# 3. 构建
if command -v uv >/dev/null 2>&1; then
    echo "[build] 构建发行包: uv build"
    if [[ ${#BUILD_ARGS[@]} -gt 0 ]]; then
        uv build "${BUILD_ARGS[@]}"
    else
        uv build
    fi
else
    echo "[build] 构建发行包: python -m build"
    if [[ ${#BUILD_ARGS[@]} -gt 0 ]]; then
        python -m build "${BUILD_ARGS[@]}" --outdir dist
    else
        python -m build --outdir dist
    fi
fi

# 4. 校验产物
if ! compgen -G "dist/*" >/dev/null; then
    echo "[build] 错误: dist/ 下没有产物" >&2
    exit 1
fi

echo "[build] 完成! 产物位于: $PROJECT_ROOT/dist"
ls -la dist/
