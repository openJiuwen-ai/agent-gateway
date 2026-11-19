ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root

ARG AGENT_CORE_REPOS=https://gitcode.com/openJiuwen/agent-core.git
ARG AGENT_CORE_COMMIT=f3f8f576164cc405c046e43079c8ad00ee17f0c4
ARG AGENT_PROTOCOL_REPOS=https://gitcode.com/openJiuwen/agent-protocol.git
# 2bc6e46：协议仓把 e2a/ 迁到 openjiuwen-e2a/，并带上 MULTIMODAL_* 枚举；
# 旧 d3d1681 + subdirectory=e2a 缺这些字段，gateway 启动会 AttributeError。
ARG AGENT_PROTOCOL_COMMIT=2bc6e46dedd0336e550a44be39303a2b7650959e
ARG AGENT_RUNTIME_REPOS=https://gitcode.com/openJiuwen/agent-runtime.git
ARG AGENT_RUNTIME_BRANCH=develop
ARG PIP_EXTRA_ARGS="-i https://mirrors.aliyun.com/pypi/simple/"

# 业务用户 app(1000)，组权限兼容 OpenShift 任意 UID
RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash app \
    && mkdir -p /app/agent-gateway /app/config /opt/tiktoken-cache \
    && chown 1000:0 /app/agent-gateway /app/config /opt/tiktoken-cache /home/app \
    && chmod -R g+rwX /app/agent-gateway /app/config /opt/tiktoken-cache /home/app

WORKDIR /app/agent-gateway
COPY --chown=1000:0 . /app/agent-gateway/
RUN chgrp -R 0 /app/agent-gateway && chmod -R g+rwX /app/agent-gateway

# 上游依赖由流水线构建参数安装，从 pyproject 移除对应声明避免重复解析：
#   dependencies 可能是旧 git URL（"openjiuwen @ git+..."）或版本区间
#   （"openjiuwen>=0.1.20.post3"）；只删 git 行会漏掉区间约束，与流水线
#   钉的 commit（包版本常仍为 0.1.20）打架导致 uv ResolutionImpossible。
#   因此 openjiuwen 不论有无 git 都删；[tool.uv.sources] git 行同样清掉。
#   e2a / openjiuwen-e2a：协议仓改名前后两种声明都要删，避免与下方 ARG 双装。
#   末尾断言兜底——任何残留 git source 让构建显式失败而非静默漏删。
RUN sed -i -E '/^[[:space:]]*"openjiuwen([>=<!~=]| @ |")/d' pyproject.toml \
    && sed -i -E '/^[[:space:]]*"(e2a|openjiuwen-e2a) @ git+/d' pyproject.toml \
    && sed -i '/"openjiuwen-runtime-foundation",/d' pyproject.toml \
    && sed -i -E '/^[[:space:]]*"?(openjiuwen|openjiuwen-runtime-foundation|e2a|openjiuwen-e2a)"?[[:space:]]*=[[:space:]]*\{[[:space:]]*git[[:space:]]*=/d' pyproject.toml \
    && if grep -Eq 'git[[:space:]]*=' pyproject.toml; then \
         echo "[docker] pyproject 仍残留 git source：rev 须由流水线参数或 pyproject 单边钉定（避免同包双 URL 冲突）" >&2; \
         exit 1; \
       fi

# .[a2a]：装入 optional extra a2a-sdk，否则企业 manager_config 因缺 a2a
# 跳过挂载，管理面探活 /api/health 会 404。
# 协议包 PyPI 名已是 openjiuwen-e2a，import 名仍是 e2a（packages.find include e2a*）。
RUN uv pip install --system \
    "openjiuwen @ git+${AGENT_CORE_REPOS}@${AGENT_CORE_COMMIT}" \
    "openjiuwen-e2a @ git+${AGENT_PROTOCOL_REPOS}@${AGENT_PROTOCOL_COMMIT}#subdirectory=openjiuwen-e2a" \
    "openjiuwen-runtime-foundation @ git+${AGENT_RUNTIME_REPOS}@${AGENT_RUNTIME_BRANCH}#subdirectory=foundation" \
    -e ".[a2a]" \
    ${PIP_EXTRA_ARGS}

# tiktoken BPE 预热（openjiuwen 传递依赖，缺失则跳过）
ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')" 2>/dev/null \
    || echo "[docker] tiktoken not installed, skip BPE warmup"

ENV HOME=/home/app \
    PATH=/usr/local/bin:$PATH:/home/app/.local/bin \
    PYTHONUNBUFFERED=1 \
    WEB_HOST=0.0.0.0 \
    GATEWAY_HOST=0.0.0.0

# 端口：19000=WebChannel(uvicorn)，19001=gateway client face
EXPOSE 19000 19001

CMD ["/app/agent-gateway/docker/start.sh"]
