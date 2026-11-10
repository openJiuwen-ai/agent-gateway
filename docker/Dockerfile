ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root

ARG AGENT_CORE_REPOS=https://gitcode.com/openJiuwen/agent-core.git
ARG AGENT_CORE_COMMIT=f3f8f576164cc405c046e43079c8ad00ee17f0c4
ARG AGENT_PROTOCOL_REPOS=https://gitcode.com/openJiuwen/agent-protocol.git
ARG AGENT_PROTOCOL_COMMIT=4c5f3e9eb41bd33d34d012b06a3e364e8497ac11
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

# 上游 git 依赖由流水线构建参数安装，从 pyproject 移除对应声明避免重复解析
RUN sed -i '/"openjiuwen @ git+/d' pyproject.toml \
    && sed -i '/"openjiuwen-protocol @ git+/d' pyproject.toml \
    && sed -i '/"openjiuwen-runtime-foundation",/d' pyproject.toml

RUN uv pip install --system \
    "openjiuwen @ git+${AGENT_CORE_REPOS}@${AGENT_CORE_COMMIT}" \
    "openjiuwen-protocol @ git+${AGENT_PROTOCOL_REPOS}@${AGENT_PROTOCOL_COMMIT}#subdirectory=openjiuwen-protocol" \
    "openjiuwen-runtime-foundation @ git+${AGENT_RUNTIME_REPOS}@${AGENT_RUNTIME_BRANCH}#subdirectory=foundation" \
    -e . \
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
