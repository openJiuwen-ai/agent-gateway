# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ADX Environment control and ingress endpoint metadata."""

from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from agent_gateway.southbound.control.managed import ManagedSandboxControl


class AdxControl(ManagedSandboxControl):
    def _url(self, instance_id: str, port: int, protocol: str) -> str:
        origin = urlsplit(self.backend.frontend_endpoint)
        target = 'urn:adx:instance:' + quote(instance_id, safe='')
        return urlunsplit(('wss' if protocol == 'ws' else 'https', origin.netloc,
                          f'/agent/{protocol}', urlencode({'target': target, 'port': port}), ''))

    def ws_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'ws')

    def http_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'http')

    def web_ws_url(self, instance_id: str, port: int) -> str:
        return self.ws_url(instance_id, port)
