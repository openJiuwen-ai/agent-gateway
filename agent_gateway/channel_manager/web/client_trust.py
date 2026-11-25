# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Trust policy for client-supplied identity headers shared by HTTP and WS."""

from __future__ import annotations

import os

from agent_gateway.enterprise import is_enterprise


def trust_client_identity_headers(client_host: object) -> bool:
    """Whether this entry may trust client-supplied identity values.

    ``GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS`` covers the client identity this
    policy owns: the HTTP identity headers (``X-User-Id``/``X-Group-Id``/
    ``X-Bot-Id``/``X-Gateway-Id``/``X-Agent-Template-Id``) and the WebSocket
    ``agent_template_id`` from the handshake query. An explicit ``0/false/no``
    disables them, while ``1/true/yes`` enables them. Without the override,
    enterprise deployments trust authenticated user-plane traffic; personal
    deployments only trust loopback requests.

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
