# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""登录链路的出网请求。"""

from __future__ import annotations

from typing import Any

import requests

from agent_gateway.common import env_names


def ssl_verify_enabled() -> bool:
    """``AGENT_GATEWAY_SSL_VERIFY``（旧名 ``JIUWENSWARM_SSL_VERIFY``），默认开。"""
    return (env_names.get(env_names.SSL_VERIFY) or "").strip().lower() not in (
        "0", "false", "no", "off",
    )


def requests_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """发一次请求；代理本身连不上时退回直连重试一次。"""
    kwargs.setdefault("verify", ssl_verify_enabled())
    try:
        return requests.request(method.upper(), url, **kwargs)
    except requests.exceptions.ProxyError:
        with requests.Session() as session:
            session.trust_env = False
            return session.request(method.upper(), url, **kwargs)
