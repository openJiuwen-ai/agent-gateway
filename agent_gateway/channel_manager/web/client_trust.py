# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Trust policy for client-supplied identity headers shared by HTTP and WS."""

from __future__ import annotations

import os

from agent_gateway.enterprise import is_enterprise


def trust_client_identity_headers(client_host: object) -> bool:
    """Whether this entry may trust client-supplied identity values.

    ``GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS`` is the single switch for both the
    HTTP and WebSocket entries: an explicit ``0/false/no`` disables client
    identity everywhere, while ``1/true/yes`` enables it. Without the override,
    enterprise deployments trust authenticated user-plane traffic; personal
    deployments only trust loopback requests.

    The switch covers the client identity this policy owns: the HTTP identity
    headers (``X-User-Id``/``X-Group-Id``/``X-Bot-Id``/``X-Gateway-Id`` plus the
    agent display identity ``X-Agent-Name``/``X-Agent-Identity-Id``/
    ``X-Runtime-Template-Id``/``X-Agent-Template-Id``) and the WebSocket agent
    display identity (``agent_name``/``agent_identity_id``/
    ``runtime_template_id``/``agent_template_id``) from the handshake query.

    The WebSocket routing identity (``group_id``/``bot_id``/``gateway_id`` from
    the handshake query) is not gated by this switch; it keeps its existing
    per-entry handling.

    The WebSocket entry passes ``(host, port)``; HTTP passes a host string.
    """
    explicit = os.getenv("GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS", "").strip().lower()
    if explicit in {"1", "true", "yes"}:
        return True
    if explicit in {"0", "false", "no"}:
        return False
    # 企业用户面通过 NodePort/Web Pod 访问时，客户端地址不是回环地址。
    # 企业版的身份边界由上游认证和 ``is_enterprise()`` 控制，必须把选中的
    # user/group/bot 透传给 Runtime 路由；个人版继续只信任本机请求。
    if is_enterprise():
        return True
    if isinstance(client_host, (tuple, list)):
        client_host = client_host[0] if client_host else None
    host = str(client_host or "").strip().lower()
    return host in {"127.0.0.1", "::1", "localhost"}
