# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""AgentServer 最小替身：为 gateway 独立契约 ST 提供 E2A 南向应答。

只实现契约用例需要的少量方法，不依赖 jiuwenswarm：

- 连接建立后立刻回 ``connection.ack``（gateway 连接首帧等待 5s）；
- 非流式请求按 ``method`` 返回固定 payload；
- ``chat.send`` 流式请求返回 delta + final 两帧。

请求/响应线格式复用 e2a 协议包，和真实 AgentServer 保持同一套编解码。
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.wire_codec import (
    encode_agent_chunk_for_wire,
    encode_agent_response_for_wire,
)
from websockets.sync.server import serve

logger = logging.getLogger(__name__)

CONNECTION_ACK = {
    "type": "event",
    "event": "connection.ack",
    "payload": {"status": "ready", "readiness": "AGENT_READY"},
}

_UNARY_PAYLOADS: dict[str, dict[str, Any]] = {
    "session.list": {"sessions": [], "total": 0},
    "connection.status": {"server_ready": True, "agent_ready": True},
    "config.get": {"config": {}},
    "cron.job.list": {"jobs": [], "total": 0},
}


class StubAgentServer:
    """在后台线程里跑的 AgentServer 替身。"""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self._connections: set[Any] = set()
        # gateway 启动探测会先 TCP 连一下就断开，ws 服务端默认会打一条
        # "opening handshake failed" 错误日志，这里静音避免污染用例输出。
        ws_logger = logging.getLogger("stub-agent-server.ws")
        ws_logger.propagate = False
        ws_logger.setLevel(logging.CRITICAL)
        self._server = serve(self._handle, "127.0.0.1", 0, logger=ws_logger)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="stub-agent-server",
            daemon=True,
        )

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    @property
    def port(self) -> int:
        return int(self._server.socket.getsockname()[1])

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def start(self) -> "StubAgentServer":
        self._thread.start()
        logger.info("[stub-agent-server] listening on %s", self.url)
        return self

    def stop(self) -> None:
        # shutdown() 只停止 accept，已建立的连接要显式关闭，
        # 否则模拟“南向掉线”的用例会一直拿到正常应答。
        for connection in list(self._connections):
            try:
                connection.close()
            except Exception:  # noqa: BLE001 - 连接可能已被对端关闭
                pass
        self._server.shutdown()
        self._thread.join(timeout=5)

    # ------------------------------------------------------------------ #
    # 协议处理
    # ------------------------------------------------------------------ #
    def _handle(self, connection) -> None:
        self._connections.add(connection)
        connection.send(json.dumps(CONNECTION_ACK))
        try:
            for raw in connection:
                envelope = json.loads(raw)
                self.requests.append(envelope)
                request_id = str(envelope.get("request_id") or "")
                method = str(envelope.get("method") or "")
                if envelope.get("is_stream"):
                    self._send_stream(connection, envelope, request_id, method)
                else:
                    connection.send(json.dumps(self._unary_wire(envelope, request_id, method)))
        except Exception:  # noqa: BLE001 - 连接被对端关闭属正常退出
            logger.debug("[stub-agent-server] connection closed", exc_info=True)
        finally:
            self._connections.discard(connection)

    def _unary_wire(self, envelope: dict[str, Any], request_id: str, method: str) -> dict[str, Any]:
        payload = _UNARY_PAYLOADS.get(method, {"echo_method": method})
        response = AgentResponse(
            request_id=request_id,
            channel_id=str(envelope.get("channel") or "web"),
            payload=payload,
            ok=True,
        )
        return encode_agent_response_for_wire(response, response_id=request_id)

    def _send_stream(self, connection, envelope: dict[str, Any], request_id: str, method: str) -> None:
        channel_id = str(envelope.get("channel") or "web")
        chunks = (
            {"event_type": "chat.delta", "content": "stub-hello"},
            {"event_type": "chat.final", "content": "stub-hello"},
        )
        for sequence, payload in enumerate(chunks):
            chunk = AgentResponseChunk(
                request_id=request_id,
                channel_id=channel_id,
                payload=payload,
                is_complete=sequence == len(chunks) - 1,
            )
            connection.send(
                json.dumps(
                    encode_agent_chunk_for_wire(
                        chunk,
                        response_id=request_id,
                        sequence=sequence,
                    )
                )
            )
