# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""AgentServerClient - Gateway 与 AgentServer 的 WebSocket 客户端."""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from contextlib import aclosing
from dataclasses import asdict, replace
from time import monotonic
from typing import Any, AsyncIterator
from urllib.parse import urlsplit

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.models import E2AEnvelope
from e2a.wire_codec import (
    parse_agent_server_wire_chunk,
    parse_agent_server_wire_unary,
)
from websockets.exceptions import ConnectionClosed, PayloadTooBig

from agent_gateway.common.moa_config import redact_moa_credentials
from agent_gateway.common.ws_diagnostics import (
    describe_ws_exception,
    describe_ws_peer,
    format_ws_diagnostics,
)
from agent_gateway.common.ws_limits import AGENT_WS_MAX_MESSAGE_BYTES
from agent_gateway.telemetry import (
    bind_agent_request_trace,
    bind_request_trace,
    current_request_trace,
    get_message_trace,
    inject_request_headers,
    inject_request_trace,
    trace_operation,
)

logger = logging.getLogger(__name__)
_STREAM_TRAILING_MESSAGE_GRACE_SECONDS = 0.7
AGENT_REQUEST_TIMEOUT_SECONDS: float = 600.0
_UNARY_REQUEST_TIMEOUT_SECONDS = AGENT_REQUEST_TIMEOUT_SECONDS


def _is_inactive_trace(trace: object | None) -> bool:
    """Whether a trace is absent or already finished."""
    return trace is None or trace.finished


def _operation_matches(operation: object | None, rid: str, envelope: E2AEnvelope) -> bool:
    """Whether an operation belongs to this request envelope."""
    if operation is None or operation.finished:
        return False
    if operation.request_id != rid:
        return False
    if operation.channel != str(envelope.channel or ""):
        return False
    return operation.method == str(envelope.method or "")


class AgentServerUnaryTimeout(RuntimeError):
    """非流式请求等待 AgentServer 响应超时。

    继承 ``RuntimeError`` 以保持向后兼容：调用方此前依赖 ``RuntimeError`` 与
    "AgentServer 非流式请求超时" 文案。作为独立类型后，cron 等显式传入
    ``timeout`` 的调用方能精确捕获并执行自身的超时收尾（如 cancel 后端
    会话），而不是被写死的内层 600s 截断成裸 ``RuntimeError``、跳过收尾。
    """

    def __init__(self, request_id: str, timeout: float) -> None:
        super().__init__(
            f"AgentServer 非流式请求超时 (request_id={request_id}, timeout={timeout}s)"
        )
        self.request_id = request_id
        self.timeout = timeout


class DuplicateRequestIdError(RuntimeError):
    """同一连接上 request_id 撞号，第二个请求被拒绝注册响应队列。

    该拒绝发生在**发送之前**：请求尚未写入 WebSocket，AgentServer 侧没有任何
    副作用，因此调用方换一个新 request_id 重试是安全的。保留 RuntimeError 继承
    关系，避免破坏既有 ``except RuntimeError`` 的调用方。
    """

    def __init__(self, request_id: str) -> None:
        super().__init__(
            f"WebSocketAgentServerClient: duplicate in-flight request_id={request_id!r}; "
            "refusing to register queue (would mis-route responses, e.g. stream chunks to unary waiters)."
        )
        self.request_id = request_id


class _ReceiverFailure:
    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


def _wire_request_id_key(request_id: Any) -> str:
    """与 AgentServer 回包 ``request_id`` 对齐：统一为 str，避免 JSON 数字/字符串导致队列键不一致。"""
    if request_id is None:
        return ""
    return str(request_id)


def _to_json(data: Any) -> str:
    """将任意对象序列化为日志友好的 JSON 字符串."""
    data = redact_moa_credentials(data)
    try:
        return json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return repr(data)


def _build_ws_origin(uri: str) -> str | None:
    """将 ws/wss URI 转为标准浏览器 Origin。"""
    try:
        parsed = urlsplit(uri)
    except ValueError:
        return None

    if not parsed.netloc:
        return None

    scheme = "https" if parsed.scheme == "wss" else "http"
    return f"{scheme}://{parsed.netloc}"


# AgentServerClient 抽象契约以 gateway.common.client.agent_client 为唯一实现。
# 此处 re-export 保持既有 import 路径兼容。
from agent_gateway.common.client.agent_client import AgentServerClient  # noqa: F401


def _e2a_to_wire(envelope: E2AEnvelope) -> dict[str, Any]:
    """E2AEnvelope → WebSocket JSON（与 AgentServer from_dict 对齐）。"""
    return envelope.to_dict()


async def _stamp_expert_packages(envelope: E2AEnvelope) -> None:
    """企业版按这只 bot 的 Agent 模板槽覆盖 channel_context。个人版不读库。"""
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return
    from agent_gateway.enterprise.capabilities.experts.outbound_packages import apply_packages_to_envelope
    from agent_gateway.enterprise.capabilities.experts.bound_packages import packages_for_bot
    from agent_gateway.common.request_identity import web_routing_identity

    bot_id = web_routing_identity(envelope.channel_context).get("bot_id", "")
    apply_packages_to_envelope(envelope, await packages_for_bot(bot_id))


class WebSocketAgentServerClient(AgentServerClient):
    """
    基于 websockets 的 AgentServer WebSocket 客户端实现。

    协议约定：
    - 发送：JSON 对象为 E2AEnvelope.to_dict()（含 protocol_version、method、channel、params、is_stream 等）。
    - 接收（非流式）：一条 **E2AResponse** 线 JSON（或过渡期 legacy AgentResponse 形），解析为 AgentResponse。
    - 接收（流式）：多条 E2AResponse 线 JSON（或 legacy chunk），解析为 AgentResponseChunk。
    """

    def __init__(
        self,
        *,
        ping_interval: float | None = 30.0,
        ping_timeout: float | None = 300.0,
        auto_reconnect: bool = False,
    ) -> None:
        self._uri: str | None = None
        self._extra_headers: dict[str, str] = {}
        self._ws: Any = None
        self._lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_enabled = False
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._server_ready: bool = False
        self._agent_ready: bool = False
        self._readiness_state: str | None = None
        # 消息分发机制：根据 request_id 路由到对应队列
        self._message_queues: dict[str, asyncio.Queue] = {}
        self._queue_lock = asyncio.Lock()  # 保护队列操作的锁
        self._cancelled_request_ids: set[str] = set()  # 已取消但等待清理的 request_id
        self._receiver_task: asyncio.Task | None = None
        self._running = False
        # AgentServer send_push：旁路投递，勿进入与 request_id 绑定的 RPC 等待队列
        self._on_server_push: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self._server_push_tails: dict[str, asyncio.Task] = {}
        # receiver 致命错误（连接断开 / ping 超时 / 发送失败）后的断连通知回调
        self._on_disconnect: Callable[[BaseException], Awaitable[None]] | None = None
        # 断连后是否由客户端自身后台恢复连接。app_gateway 的默认 client 开启；
        # agentos_router 等复用本类的场景必须保持 False——其断连语义由外层
        # 清理逻辑（删沙箱/摘除缓存）接管，客户端擅自重连会破坏该约定。
        self._auto_reconnect = auto_reconnect
        self._auto_reconnect_task: asyncio.Task | None = None

    def set_server_push_handler(
        self, handler: Callable[[dict[str, Any]], Awaitable[None]] | None
    ) -> None:
        """注册 Agent 主动推送处理回调（metadata 含 ``E2A_WIRE_SERVER_PUSH_KEY`` 的帧）。"""
        self._on_server_push = handler

    def set_disconnect_handler(
        self, handler: Callable[[BaseException], Awaitable[None]] | None
    ) -> None:
        """注册南向断连通知回调（参数为触发 ``_stop_receiver_after_fatal_error`` 的异常）。

        receiver 捕获 ConnectionClosed / PayloadTooBig，或发送路径捕获
        ConnectionClosed / OSError 后触发；正常 ``disconnect()``（取消
        receiver 任务）不触发。回调以 fire-and-forget 任务派发，异常被
        吞掉记日志，不影响 receiver / 发送路径。
        """
        self._on_disconnect = handler

    def _diagnostic_state(self, ws: Any | None = None) -> dict[str, Any]:
        target_ws = self._ws if ws is None else ws
        return {
            "uri": self._uri,
            "running": self._running,
            "server_ready": self._server_ready,
            "agent_ready": self._agent_ready,
            "readiness": self._readiness_state,
            "pending_requests": len(self._message_queues),
            "cancelled_requests": len(self._cancelled_request_ids),
            "ping_interval": self._ping_interval,
            "ping_timeout": self._ping_timeout,
            **describe_ws_peer(target_ws),
        }

    def set_or_update_server_config(
        self,
        *,
        config: dict[str, Any],
        env: dict[str, str] | None = None,
    ) -> None:
        """默认 WebSocket client 不处理服务端配置缓存，留给扩展 client 自行实现."""
        return None

    @property
    def server_ready(self) -> bool:
        """Front transport is connected. Does not mean Agent Runtime can execute."""
        return self._server_ready

    @property
    def can_reconnect(self) -> bool:
        """Whether a request can restore the configured transport."""
        return bool(self._uri) and (
            self._reconnect_lock.locked()
            or (self._reconnect_enabled and self._ws is None)
        )

    @property
    def agent_ready(self) -> bool:
        """Agent Runtime can execute. False while warming or failed."""
        return self._agent_ready

    def _apply_connection_ack(self, data: dict[str, Any]) -> None:
        payload = data.get("payload")
        if not isinstance(payload, dict):
            payload = data.get("params") if isinstance(data.get("params"), dict) else {}
        status = str(payload.get("status") or "").strip().lower()
        readiness = payload.get("readiness")
        self._readiness_state = str(readiness) if isinstance(readiness, str) else None
        # Any ack means Front transport is up. FAILED/DRAINING describe Runtime,
        # not the socket; e2a_proxy must still reach Front control RPCs.
        self._server_ready = True
        self._agent_ready = self._readiness_state in {"AGENT_READY", "DEGRADED"}
        logger.info(
            "[WebSocketAgentServerClient] 收到 connection.ack status=%s readiness=%s "
            "server_ready=%s agent_ready=%s",
            status or "ready",
            self._readiness_state,
            self._server_ready,
            self._agent_ready,
        )

    async def connect(
        self,
        uri: str,
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        with (trace_operation("gw.agent.ws.connect")
              if current_request_trace() is not None else nullcontext(None)) as operation:
            # Initial connection and its startup retries own connection setup until
            # one attempt finishes. Proxy requests must not start another attempt.
            self._reconnect_enabled = False
            if self._ws is not None:
                await self.disconnect()
            logger.debug("[WebSocketAgentServerClient] 正在连接: %s", uri)
            self._uri = uri
            self._server_ready = False
            self._agent_ready = False
            self._readiness_state = None
            origin = _build_ws_origin(uri)
            connect_kwargs: dict[str, Any] = {
                "origin": origin,
                "ping_interval": self._ping_interval,
                "ping_timeout": self._ping_timeout,
                "close_timeout": 5.0,
                "max_size": AGENT_WS_MAX_MESSAGE_BYTES,
            }
            cleaned_headers = {
                str(key): str(value)
                for key, value in dict(extra_headers or {}).items()
                if str(value).strip()
            }
            # 内部通道身份（连接级，P1 修正）：握手头部绑定共享密钥，供
            # AgentServer 侧的 gateway 专有 action（如 command.mcp 的
            # oauth_callback）做连接级放行——载荷字段（channel_id）可被任
            # 意客户端伪造，不能作权限判据。
            internal_token = os.getenv("JIUWENSWARM_GATEWAY_INTERNAL_TOKEN", "").strip()
            if internal_token:
                cleaned_headers["X-Jiuwen-Gateway-Internal"] = internal_token
            # Retain authentication and business headers without an expired SDK trace.
            sdk_trace_id = inject_request_headers({}).get("X-Trace-Id")
            if sdk_trace_id:
                sdk_trace_hex = sdk_trace_id.replace("-", "").lower()
                self._extra_headers = {}
                for key, value in cleaned_headers.items():
                    lower_key = key.lower()
                    if lower_key in {"traceparent", "tracestate"}:
                        continue
                    if lower_key == "x-trace-id" and value.replace("-", "").lower() == sdk_trace_hex:
                        continue
                    self._extra_headers[key] = value
            else:
                self._extra_headers = cleaned_headers
            cleaned_headers = inject_request_headers(cleaned_headers)
            try:
                from websockets.legacy.client import connect as legacy_connect
                connect_fn = legacy_connect
                if cleaned_headers:
                    connect_kwargs["extra_headers"] = list(cleaned_headers.items())
            except ImportError:
                import websockets
                connect_fn = websockets.connect
                if cleaned_headers:
                    connect_kwargs["additional_headers"] = cleaned_headers
            self._ws = await connect_fn(uri, **connect_kwargs)
            logger.info("[WebSocketAgentServerClient] 已连接: %s", uri)

            # 读取 AgentServer 的 connection.ack 事件
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                logger.debug("[WebSocketAgentServerClient] connect 首帧(raw): %s", raw)
                data = json.loads(raw)
                logger.debug("[WebSocketAgentServerClient] connect 首帧(parsed): %s", _to_json(data))
                if data.get("type") == "event" and data.get("event") == "connection.ack":
                    self._apply_connection_ack(data)
                else:
                    if operation is not None:
                        operation.record_outcome("incomplete", reason="connection_ack_missing")
                    logger.warning(
                        "[WebSocketAgentServerClient] 首帧非 connection.ack: %s",
                        data.get("type"),
                    )
            except asyncio.TimeoutError:
                if operation is not None:
                    operation.record_outcome("timeout", reason="connection_ack_timeout")
                logger.warning("[WebSocketAgentServerClient] 等待 connection.ack 超时")
            except Exception as e:
                if operation is not None:
                    operation.record_outcome("failed", reason="connection_ack_failed")
                logger.warning("[WebSocketAgentServerClient] 读取 connection.ack 失败: %s", e)

            # 启动消息接收和分发任务
            self._running = True
            with (bind_request_trace(None) if current_request_trace() is not None else nullcontext()):
                self._receiver_task = asyncio.create_task(self._message_receiver_loop())
            self._reconnect_enabled = True
            logger.info("[WebSocketAgentServerClient] 消息接收任务已启动")

    async def _message_receiver_loop(self) -> None:
        """后台任务：从 WebSocket 接收消息并根据 request_id 分发到对应队列."""
        try:
            while self._running and self._ws is not None:
                try:
                    raw = await self._ws.recv()
                    data = json.loads(raw)
                    # 迟到的 connection.ack（connect 等待首帧 5s 超时后才到达）：
                    # 补置就绪状态。connect 只 recv 一次首帧，超时后该 ack 只能由
                    # 接收循环收到；若不补置，server_ready 永久为 False，会令
                    # e2a_proxy 等依赖 server_ready 的入口（如 Web session.list）
                    # 永久返回 SERVICE_UNAVAILABLE，即使连接实际已建立。
                    if data.get("type") == "event" and data.get("event") == "connection.ack":
                        self._apply_connection_ack(data)
                        continue
                    meta = data.get("metadata")
                    if isinstance(meta, dict) and meta.get(E2A_WIRE_SERVER_PUSH_KEY):
                        if self._on_server_push is not None:
                            # Push callbacks may await I/O. Independent tasks can
                            # overtake each other, putting a delta after its final.
                            self._schedule_server_push(data)
                        else:
                            logger.warning(
                                "[WebSocketAgentServerClient] 收到 server_push 但未注册 handler，已丢弃: "
                                "request_id=%s",
                                data.get("request_id"),
                            )
                        continue
                    request_id = _wire_request_id_key(data.get("request_id"))

                    # 使用锁保护队列访问，避免竞态条件
                    async with self._queue_lock:
                        # 已取消请求的迟到帧：静默丢弃。
                        # 不放宽为"rid 已有新 queue 就放行"——弃单（超时/取消）后旧请求
                        # 的迟到响应可能晚于新请求注册到达，放行会串进新请求的 queue，
                        # 导致新请求拿到错误响应（响应串线，比超时更危险）。
                        # 该 2s 丢弃窗口只针对弃单残余；unary 正常完成后不再立此标记
                        # （见 _drain_and_remove_queue），复用同 rid 的新请求不受影响。
                        if request_id in self._cancelled_request_ids:
                            if request_id in self._message_queues:
                                # 墓碑窗口内复用了同 rid：新等待者的响应正在被丢弃，
                                # 它必将超时——提升为 warning，让这类吞没可观测。
                                logger.warning(
                                    "[WebSocketAgentServerClient] 已取消 request_id 被新请求复用，"
                                    "新请求的响应被丢弃（等待方将超时）: request_id=%s",
                                    request_id,
                                )
                            else:
                                logger.debug(
                                    "[WebSocketAgentServerClient] 收到已取消请求的残余消息，已丢弃: request_id=%s",
                                    request_id
                                )
                            continue

                        if request_id and request_id in self._message_queues:
                            await self._message_queues[request_id].put(data)
                        else:
                            # 没有对应的队列（非预期情况）——可能是 E2A 编解码导致
                            # request_id 不匹配，此时 send_request 会等满自身超时
                            # （默认 600s，或调用方透传的 timeout）。
                            # 提升为 warning 让问题可见，便于排查"到点没推送"类故障。
                            logger.warning(
                                "[WebSocketAgentServerClient] 收到无目标队列的消息（等待方将超时）: request_id=%s",
                                request_id
                            )
                except asyncio.CancelledError:
                    break
                except PayloadTooBig as e:
                    logger.error(
                        "[WebSocketAgentServerClient] AgentServer 消息超过 WebSocket 限制: %s",
                        format_ws_diagnostics(
                            self._diagnostic_state(),
                            describe_ws_exception(e),
                        ),
                    )
                    await self._stop_receiver_after_fatal_error(e)
                    break
                except ConnectionClosed as e:
                    logger.info(
                        "[WebSocketAgentServerClient] AgentServer WebSocket 已关闭: %s",
                        format_ws_diagnostics(
                            self._diagnostic_state(),
                            describe_ws_exception(e),
                        ),
                    )
                    await self._stop_receiver_after_fatal_error(e)
                    break
                except Exception as e:
                    logger.exception(
                        "[WebSocketAgentServerClient] 消息接收循环异常: %s",
                        format_ws_diagnostics(
                            self._diagnostic_state(),
                            describe_ws_exception(e),
                        ),
                    )
                    await asyncio.sleep(0.1)  # 避免快速循环
        finally:
            logger.info("[WebSocketAgentServerClient] 消息接收任务已停止")

    def _schedule_server_push(self, data: dict[str, Any]) -> None:
        """Keep one turn's push frames ordered without blocking other turns."""
        request_id = _wire_request_id_key(data.get("request_id"))
        body = data.get("body")
        turn_id = body.get("turn_request_id") if isinstance(body, dict) else None
        if isinstance(body, dict) and not (isinstance(turn_id, str) and turn_id.strip()):
            payload = body.get("delta")
            if not isinstance(payload, dict):
                payload = body.get("result")
            if not isinstance(payload, dict):
                payload = body.get("details")
            turn_id = payload.get("turn_request_id") if isinstance(payload, dict) else None
        # Interaction frames may use their own wire request_id inside a turn.
        order_key = (
            f"turn:{turn_id.strip()}" if isinstance(turn_id, str) and turn_id.strip()
            else f"request:{request_id}"
        )
        previous = self._server_push_tails.get(order_key)
        handler = self._on_server_push

        async def deliver() -> None:
            try:
                if previous is not None:
                    await previous
                if handler is not None:
                    await handler(data)
            except Exception:
                logger.exception(
                    "[WebSocketAgentServerClient] server_push 处理失败: request_id=%s",
                    request_id,
                )
            finally:
                if self._server_push_tails.get(order_key) is asyncio.current_task():
                    self._server_push_tails.pop(order_key, None)

        self._server_push_tails[order_key] = asyncio.create_task(deliver())

    async def _stop_receiver_after_fatal_error(self, exc: BaseException) -> None:
        detail = format_ws_diagnostics(
            self._diagnostic_state(),
            describe_ws_exception(exc),
        )
        self._running = False
        self._server_ready = False
        self._agent_ready = False
        self._readiness_state = None
        self._ws = None
        failure = _ReceiverFailure(exc)
        async with self._queue_lock:
            for queue in self._message_queues.values():
                queue.put_nowait(failure)
        logger.info("[WebSocketAgentServerClient] 接收任务已停止并通知等待队列: %s", detail)
        self._fire_disconnect_handler(exc)
        self._maybe_start_auto_reconnect()

    def _maybe_start_auto_reconnect(self) -> None:
        """连接意外断开后按需启动后台自动重连（构造参数 opt-in）.

        断连后若无人恢复连接，server-push（cron 结果、交互请求、chat.file
        推送等）会持续丢失，直到某个业务请求碰巧触发懒重连——个人版没有
        外层守护者，客户端自身需要负责尽快恢复连接。显式 ``disconnect()``
        置 ``_reconnect_enabled=False``，不会走到这里。
        """
        if not self._auto_reconnect:
            return
        if not self._reconnect_enabled or not self._uri:
            # 显式断开之后，或从未成功建立过连接：不自动恢复
            return
        task = self._auto_reconnect_task
        if task is not None and not task.done():
            return
        try:
            self._auto_reconnect_task = asyncio.create_task(self._auto_reconnect_loop())
            logger.info(
                "[WebSocketAgentServerClient] 已启动后台自动重连: %s",
                self._uri,
            )
        except RuntimeError:
            # 无运行中的事件循环（同步关停路径）：无处派发，静默放弃
            logger.warning(
                "[WebSocketAgentServerClient] 无运行中的事件循环，后台自动重连未启动"
            )

    async def _auto_reconnect_loop(self) -> None:
        """后台恢复南向连接：指数退避重试，成功或显式断开后退出.

        与懒重连协同：连接尝试持有 ``_reconnect_lock``；业务请求的
        ``_ensure_connected_for_request`` 若先行恢复连接，本循环在下一轮
        检查时直接退出，反之亦然。
        """
        backoff = 0.2
        max_backoff = 5.0
        attempt = 0
        while True:
            if not self._reconnect_enabled or self._uri is None:
                # 显式 disconnect()：停止重连
                return
            if self._ws is not None:
                # 已由其它路径（懒重连/上层重建）恢复
                return
            uri = self._uri
            headers = dict(self._extra_headers)
            reconnect_enabled = self._reconnect_enabled
            attempt += 1
            try:
                async with self._reconnect_lock:
                    if self._ws is not None:
                        return
                    await self.connect(uri, extra_headers=headers)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # connect() 中途失败可能留下半更新状态（如 extra_headers 被
                # 覆写）：复位现场并保留重试能力。不能调 disconnect()——它
                # 会先取消本任务自身（等待自身结束即死锁），故复用不含任务
                # 管理的 _teardown_connection（与懒重连路径同语义）。
                await self._teardown_connection()
                self._uri = uri
                self._extra_headers = headers
                self._reconnect_enabled = reconnect_enabled
                # 节流日志：前 3 次逐次告警，之后每 10 次一条，避免长时间
                # 不可达时刷屏
                if attempt <= 3 or attempt % 10 == 0:
                    logger.warning(
                        "[WebSocketAgentServerClient] 后台自动重连失败（第 %d 次）: %s",
                        attempt,
                        describe_ws_exception(exc),
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            else:
                if self._ws is None or not self._running:
                    # connect 期间并发 fatal（如请求发送失败触发
                    # _stop_receiver_after_fatal_error）已把现场清掉，而
                    # _maybe_start_auto_reconnect 因本任务未结束跳过了重调度：
                    # 接续重试，别把后台重连丢在这一轮。
                    logger.info(
                        "[WebSocketAgentServerClient] 重连返回后连接已断开，继续后台重连"
                    )
                    continue
                logger.info(
                    "[WebSocketAgentServerClient] 后台自动重连成功: %s（第 %d 次尝试）",
                    uri,
                    attempt,
                )
                return

    async def _cancel_auto_reconnect_task(self) -> None:
        """取消并回收后台自动重连任务；已结束则仅清理引用."""
        task = self._auto_reconnect_task
        if task is None:
            return
        if task.done():
            self._auto_reconnect_task = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning("[WebSocketAgentServerClient] 后台自动重连任务异常结束", exc_info=True)
        finally:
            if self._auto_reconnect_task is task:
                self._auto_reconnect_task = None

    def _fire_disconnect_handler(self, exc: BaseException) -> None:
        """派发断连通知回调：fire-and-forget，不阻塞 receiver / 发送路径。"""
        handler = self._on_disconnect
        if handler is None:
            return

        async def _run() -> None:
            try:
                await handler(exc)
            except Exception:
                logger.warning(
                    "[WebSocketAgentServerClient] 断连回调执行失败",
                    exc_info=True,
                )

        try:
            asyncio.create_task(_run())
        except RuntimeError:
            # 无运行中的事件循环（同步关停路径）：无处派发，静默放弃
            logger.warning(
                "[WebSocketAgentServerClient] 断连回调无处派发（无运行中的事件循环）: %s",
                type(exc).__name__,
            )

    async def _teardown_connection(self) -> None:
        """停止接收任务、关闭 socket 并复位连接态.

        不触碰等待队列与后台重连任务，也不清除 ``_uri``/``_extra_headers``
        （保留给调用方决定——显式断开要清空，重连失败路径要保留）。
        供 ``disconnect()``（显式断开）与自动重连失败路径复用——后者不能
        走 ``disconnect()``，否则会取消自身所在任务造成死锁。
        """
        self._running = False
        if self._receiver_task and not self._receiver_task.done():
            self._receiver_task.cancel()
            try:
                await self._receiver_task
            except asyncio.CancelledError:
                pass
        self._receiver_task = None

        # 关闭 WebSocket
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception as e:
            logger.warning(
                "关闭 AgentServer WebSocket 时异常: %s",
                format_ws_diagnostics(
                    self._diagnostic_state(),
                    describe_ws_exception(e),
                ),
            )
        finally:
            self._ws = None
            self._reconnect_enabled = False
            self._server_ready = False
            self._agent_ready = False
            self._readiness_state = None

    async def disconnect(self) -> None:
        # 停止后台自动重连：显式断开后不再自动恢复
        await self._cancel_auto_reconnect_task()
        await self._teardown_connection()
        self._uri = None
        self._extra_headers = {}

        # 等待者立即失败并清理队列。若只清注册不通知，等待者会拿着已注销的
        # 队列一直空等到自身超时（unary 最长 600s）——断开就该快速失败。
        failure = _ReceiverFailure(RuntimeError("AgentServer WebSocket disconnected"))
        async with self._queue_lock:
            for queue in self._message_queues.values():
                queue.put_nowait(failure)
        self._message_queues.clear()
        logger.info("[WebSocketAgentServerClient] 已断开")

    def _ensure_connected(self) -> None:
        if self._ws is None:
            raise RuntimeError("未连接 AgentServer，请先调用 connect(uri)")

    async def _ensure_connected_for_request(self) -> None:
        # A reconnect publishes the socket before its acknowledgement arrives.
        # Other requests must wait until that attempt has finished.
        if self._ws is not None and not self._reconnect_lock.locked():
            return
        async with self._reconnect_lock:
            if self._ws is not None:
                return
            uri = self._uri
            if not uri:
                raise RuntimeError("未连接 AgentServer，请先调用 connect(uri)")
            headers = dict(self._extra_headers)
            reconnect_enabled = self._reconnect_enabled
            logger.info(
                "[WebSocketAgentServerClient] WebSocket 已断开，准备按需重连: %s",
                format_ws_diagnostics(self._diagnostic_state(), uri=uri),
            )
            try:
                await self.connect(uri, extra_headers=headers)
            except (Exception, asyncio.CancelledError):
                # A request timeout can interrupt the acknowledgement handshake.
                # Close any partial socket and retain configuration for the next
                # request; never replay a business request already sent.
                try:
                    await self.disconnect()
                finally:
                    # Cleanup can itself be cancelled; future requests must not
                    # lose the last successful connection's retry capability.
                    self._uri = uri
                    self._extra_headers = headers
                    self._reconnect_enabled = reconnect_enabled
                raise

    async def _send_wire_payload(self, payload_text: str, *, payload: dict[str, Any]) -> None:
        """发送已序列化的 JSON 文本；``payload`` 仅供失败诊断取字段."""
        ws = self._ws
        if ws is None:
            raise RuntimeError("未连接 AgentServer，请先调用 connect(uri)")
        try:
            await ws.send(payload_text)
        except (ConnectionClosed, OSError) as exc:
            logger.warning(
                "[WebSocketAgentServerClient] AgentServer WebSocket 发送失败，连接将重置: %s",
                format_ws_diagnostics(
                    self._diagnostic_state(ws),
                    describe_ws_exception(exc),
                    request_id=_wire_request_id_key(payload.get("request_id")),
                    channel=payload.get("channel"),
                    method=payload.get("method"),
                ),
            )
            await self._stop_receiver_after_fatal_error(exc)
            raise RuntimeError("AgentServer WebSocket connection closed") from exc

    async def send_request(
        self, envelope: E2AEnvelope, *, timeout: float | None = None,
    ) -> AgentResponse:
        with bind_agent_request_trace(envelope):
            return await self._send_request_scoped(envelope, timeout=timeout)

    async def _send_request_scoped(
        self,
        envelope: E2AEnvelope,
        *,
        timeout: float | None = None,
    ) -> AgentResponse:
        await self._ensure_connected_for_request()
        await _stamp_expert_packages(envelope)
        # 非流式 API 必须与 AgentServer 的 unary 路径一致；忽略信封上误带的 is_stream=True。
        envelope.is_stream = False
        rid = _wire_request_id_key(envelope.request_id)
        # 调用方可显式覆盖等待上限（如 cron 任务的 timeout_seconds）；未指定时
        # 回退到客户端默认 600s。这样任务自身的超时真正生效，而非被内层默认值
        # 提前截断成裸 RuntimeError、跳过调用方的超时收尾（cancel 后端会话等）。
        effective_timeout = (
            float(timeout) if timeout is not None else _UNARY_REQUEST_TIMEOUT_SECONDS
        )
        sid = str(envelope.session_id or "")
        logger.debug(
            "[E2A][out][nostream] request_id=%s channel=%s method=%s session_id=%s is_stream=%s",
            rid,
            envelope.channel,
            envelope.method,
            sid,
            envelope.is_stream,
            extra={"session_id": sid} if sid else {},
        )
        # 全载荷序列化的 debug 日志必须惰性求值：logging 参数在 debug 关闭时
        # 也会先求值，等于每个请求多付两次全量 to_dict + redact + dumps。
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[WebSocketAgentServerClient] 发送请求(非流式) E2A: %s",
                _to_json(envelope.to_dict()),
            )

        if rid in self._message_queues:
            raise DuplicateRequestIdError(rid)

        # 创建该请求的消息队列
        queue = asyncio.Queue()
        self._message_queues[rid] = queue

        operation = current_request_trace()
        if not _operation_matches(operation, rid, envelope):
            operation = None
        wire_envelope = envelope
        if operation is not None:
            wire_envelope = replace(envelope, channel_context=dict(envelope.channel_context or {}))
            inject_request_trace(wire_envelope)

        response_delivered = False
        try:
            # 发送请求
            lock_started = monotonic()
            send_started = None
            send_outcome = "failed"
            try:
                # JSON 序列化在发送锁外完成：锁内只保留 ws.send 这一个 await
                # 点，大载荷的 CPU 序列化开销不再放大锁持有时间（不能用
                # to_thread 兜底事件循环阻塞——json 的 C 编码器不释放 GIL）。
                payload = _e2a_to_wire(wire_envelope)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[WebSocketAgentServerClient] 发送请求(非流式) payload: %s",
                        _to_json(payload),
                    )
                async with self._lock:
                    lock_wait_ms = (monotonic() - lock_started) * 1000
                    send_started = monotonic()
                    await self._send_wire_payload(payload_text, payload=payload)
                    send_outcome = "written"
            finally:
                if operation is not None:
                    operation.event("send.returned" if send_outcome == "written" else "send.failed", {
                        "outcome": send_outcome,
                        "lock_wait_ms": round((monotonic() - lock_started) * 1000, 3)
                        if send_started is None else round(lock_wait_ms, 3),
                        "send_ms": 0.0 if send_started is None
                        else round((monotonic() - send_started) * 1000, 3),
                    })

            response_started = monotonic()
            try:
                data = await asyncio.wait_for(queue.get(), timeout=effective_timeout)
                if isinstance(data, _ReceiverFailure):
                    raise RuntimeError("AgentServer WebSocket connection closed") from data.exc
            except asyncio.TimeoutError as e:
                logger.warning(
                    "[WebSocketAgentServerClient] 非流式请求超时: request_id=%s timeout=%ss",
                    rid,
                    effective_timeout,
                )
                raise AgentServerUnaryTimeout(rid, effective_timeout) from e
            if operation is not None:
                operation.event("response.first", {
                    "response_wait_ms": round((monotonic() - response_started) * 1000, 3),
                })
            resp = parse_agent_server_wire_unary(data)
            if operation is not None:
                operation.event("response.received", {
                    "outcome": "success" if resp.ok else "failed",
                    "response_wait_ms": round((monotonic() - response_started) * 1000, 3),
                })
            if operation is not None and not resp.ok:
                operation.record_outcome("failed", reason="agent_response")
            response_delivered = True
            return resp
        except asyncio.CancelledError:
            if operation is not None:
                operation.event("response.failed", {"reason": "cancelled"})
                operation.record_outcome("cancelled", reason="cancelled")
            raise
        except Exception as exc:
            if operation is not None:
                operation.event("response.failed", {"error_type": type(exc).__name__})
                operation.record_outcome("timeout" if isinstance(exc, AgentServerUnaryTimeout) else "failed",
                                         reason=type(exc).__name__)
            raise
        finally:
            # 清理队列。unary 正常送达后不再立"已取消"标记：协议上每个
            # request_id 恰好一帧响应且已被消费，立标记只会让 2s 窗口内复用
            # 同 rid 的新请求丢响应（#4940）。超时/取消/解析失败仍按弃单保留
            # 标记，防止旧请求的迟到帧串进复用同 rid 的新等待者。
            await self._drain_and_remove_queue(rid, mark_cancelled=not response_delivered)

    async def send_request_stream(self, envelope: E2AEnvelope) -> AsyncIterator[AgentResponseChunk]:
        # Preserve the original flow when no tracing operation is associated.
        current = current_request_trace()
        attached = get_message_trace(envelope)
        if _is_inactive_trace(current) and _is_inactive_trace(attached):
            async with aclosing(self._send_request_stream_scoped(envelope)) as stream:
                async for chunk in stream:
                    yield chunk
            return
        # Run the scope in its own task so yielding does not leak its context to the caller.
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        consumed = asyncio.Event()

        async def produce() -> None:
            try:
                with bind_agent_request_trace(envelope):
                    async with aclosing(self._send_request_stream_scoped(envelope)) as stream:
                        async for chunk in stream:
                            await queue.put(("chunk", chunk))
                            await consumed.wait()
                            consumed.clear()
            except asyncio.CancelledError as exc:
                if asyncio.current_task().cancelling():
                    raise
                await queue.put(("error", exc))
            except Exception as exc:
                await queue.put(("error", exc))
            else:
                await queue.put(("done", None))

        task = asyncio.create_task(produce())
        try:
            while True:
                kind, value = await queue.get()
                if kind == "done":
                    break
                if kind == "error":
                    raise value
                yield value
                consumed.set()
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _send_request_stream_scoped(
        self, envelope: E2AEnvelope
    ) -> AsyncIterator[AgentResponseChunk]:
        await self._ensure_connected_for_request()
        await _stamp_expert_packages(envelope)
        envelope.is_stream = True
        rid = _wire_request_id_key(envelope.request_id)
        sid = str(envelope.session_id or "")
        logger.debug(
            "[E2A][out][stream] request_id=%s channel=%s method=%s session_id=%s is_stream=%s",
            rid,
            envelope.channel,
            envelope.method,
            sid,
            envelope.is_stream,
            extra={"session_id": sid} if sid else {},
        )
        # 同非流式路径：全载荷 debug 日志惰性求值，避免每请求两次无谓序列化。
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[WebSocketAgentServerClient] 发送请求(流式) E2A: %s",
                _to_json(envelope.to_dict()),
            )

        if rid in self._message_queues:
            raise DuplicateRequestIdError(rid)

        # 创建该请求的消息队列
        queue = asyncio.Queue()
        self._message_queues[rid] = queue

        operation = current_request_trace()
        if not _operation_matches(operation, rid, envelope):
            operation = None
        wire_envelope = envelope
        if operation is not None:
            wire_envelope = replace(envelope, channel_context=dict(envelope.channel_context or {}))
            inject_request_trace(wire_envelope)

        try:
            lock_started = monotonic()
            send_started = None
            send_outcome = "failed"
            try:
                # 同非流式路径：序列化在发送锁外完成，锁内只留 ws.send。
                payload = _e2a_to_wire(wire_envelope)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[WebSocketAgentServerClient] 发送请求(流式) payload: %s",
                        _to_json(payload),
                    )
                async with self._lock:
                    lock_wait_ms = (monotonic() - lock_started) * 1000
                    send_started = monotonic()
                    await self._send_wire_payload(payload_text, payload=payload)
                    send_outcome = "written"
            finally:
                if operation is not None:
                    operation.event("send.returned" if send_outcome == "written" else "send.failed", {
                        "outcome": send_outcome,
                        "lock_wait_ms": round((monotonic() - lock_started) * 1000, 3)
                        if send_started is None else round(lock_wait_ms, 3),
                        "send_ms": 0.0 if send_started is None else round((monotonic() - send_started) * 1000, 3),
                    })
            response_started = monotonic()
            # 从队列中接收流式响应
            chunk_count = 0
            saw_complete = False
            while True:
                if saw_complete:
                    try:
                        data = await asyncio.wait_for(
                            queue.get(),
                            timeout=_STREAM_TRAILING_MESSAGE_GRACE_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        break
                else:
                    data = await queue.get()
                if isinstance(data, _ReceiverFailure):
                    raise RuntimeError("AgentServer WebSocket connection closed") from data.exc
                if chunk_count == 0 and operation is not None:
                    operation.event("response.first", {
                        "response_wait_ms": round((monotonic() - response_started) * 1000, 3),
                    })
                chunk = parse_agent_server_wire_chunk(data)
                chunk_count += 1
                if chunk_count == 1 and operation is not None:
                    operation.event("response.received", {
                        "outcome": "success",
                        "response_wait_ms": round((monotonic() - response_started) * 1000, 3),
                    })
                if chunk_count <= 3:
                    _pl = getattr(chunk, "payload", None) or {}
                    _et = _pl.get("event_type", "") if isinstance(_pl, dict) else ""
                    logger.debug(
                        "[WebSocketAgentServerClient] stream chunk received:"
                        " request_id=%s seq=%s event_type=%s",
                        rid, chunk_count, _et,
                    )
                yield chunk
                if chunk.is_complete:
                    saw_complete = True
            # The one-per-stream completion/cancellation INFO lives in
            # MessageHandler.process_stream (with total_chunks); do not print a
            # duplicate here.
        except asyncio.CancelledError:
            if operation is not None:
                operation.event("response.failed", {"reason": "cancelled"})
                operation.record_outcome("cancelled", reason="cancelled")
            raise
        except Exception as exc:
            if operation is not None:
                operation.event("response.failed", {"error_type": type(exc).__name__})
                operation.record_outcome("timeout" if isinstance(exc, AgentServerUnaryTimeout) else "failed",
                                         reason=type(exc).__name__)
            raise
        finally:
            # 清理队列；流式路径保留已取消标记：complete 之后仍可能有重复/迟到
            # chunk（见 _STREAM_TRAILING_MESSAGE_GRACE_SECONDS），复用同 rid 时
            # 需由接收循环丢弃，避免串进新等待者。
            await self._drain_and_remove_queue(rid)

    async def _drain_and_remove_queue(self, rid: str, *, mark_cancelled: bool = True) -> None:
        """清空队列中的残余消息并移除队列，弃单路径同时标记 request_id 为已取消.

        Args:
            mark_cancelled: 是否将 rid 加入 ``_cancelled_request_ids``。unary
                请求正常送达时传 False——响应已被消费且协议上不会再有该 rid
                的后续帧，立标记会让 2s 窗口内复用同 rid 的新请求丢响应
                （#4940）。超时/取消/流式结束等仍可能收到迟到帧的路径保持
                True，由接收循环丢弃残余，避免串进复用同 rid 的新等待者。

        标记为已取消后，后续到达的残余消息会被 _message_receiver_loop 丢弃。
        使用锁保护，确保操作的原子性。
        """
        async with self._queue_lock:
            queue = self._message_queues.get(rid)
            if queue is None:
                return
            # 1. 弃单路径：先标记为已取消，阻止迟到消息进入后续复用者的队列
            if mark_cancelled:
                self._cancelled_request_ids.add(rid)
            # 2. 删除队列注册
            del self._message_queues[rid]
            # 3. 清空队列中的残余消息（非阻塞）
            drained_count = 0
            while True:
                try:
                    queue.get_nowait()
                    drained_count += 1
                except asyncio.QueueEmpty:
                    break
            logger.debug(
                "[WebSocketAgentServerClient] 队列已清空并移除: request_id=%s 清理消息数=%d",
                rid,
                drained_count,
            )
            # 4. 异步延迟清理已取消标记（给 AgentServer 一点时间发送残余消息）
            if rid in self._cancelled_request_ids:
                asyncio.create_task(self._delayed_cleanup_cancelled_request_id(rid))

    async def _delayed_cleanup_cancelled_request_id(self, rid: str) -> None:
        """延迟清理已取消的 request_id 标记.

        等待一段时间后清理，确保 AgentServer 的残余消息能够被静默丢弃而不打印日志。
        """
        # 等待足够时间让 AgentServer 的残余消息被接收和丢弃
        await asyncio.sleep(2.0)  # 2秒应该足够
        async with self._queue_lock:
            self._cancelled_request_ids.discard(rid)
            logger.debug(
                "[WebSocketAgentServerClient] 已取消标记已清理: request_id=%s",
                rid,
            )


# ---------------------------------------------------------------------------
# Mock AgentServer（协议兼容，供示例或测试使用）
# ---------------------------------------------------------------------------


async def mock_agent_server_handler(ws: Any) -> None:
    """
    协议兼容的 Mock AgentServer：按 is_stream 回 E2AResponse 线 JSON（与生产 AgentServer 一致）。
    """
    import websockets
    from e2a.wire_codec import (
        encode_agent_chunk_for_wire,
        encode_agent_response_for_wire,
    )

    try:
        while True:
            raw = await ws.recv()
            data = json.loads(raw)
            req_id = data.get("request_id", "")
            ch_id = data.get("channel") or data.get("channel_id", "")
            params = data.get("params", {})
            is_stream = data.get("is_stream", False)
            params_str = json.dumps(params, ensure_ascii=False) if isinstance(params, dict) else str(params)

            if is_stream:
                for i, part in enumerate(["流式-1 ", "流式-2 ", "流式-3(完)"]):
                    chunk = AgentResponseChunk(
                        request_id=req_id,
                        channel_id=ch_id,
                        payload={"content": part},
                        is_complete=i == 2,
                    )
                    wire = encode_agent_chunk_for_wire(
                        chunk, response_id=req_id, sequence=i
                    )
                    await ws.send(json.dumps(wire, ensure_ascii=False))
            else:
                meta = data.get("metadata") or data.get("channel_context")
                if meta is not None and not isinstance(meta, dict):
                    meta = None
                resp = AgentResponse(
                    request_id=req_id,
                    channel_id=ch_id,
                    ok=True,
                    payload={"content": f"Echo: {params_str}"},
                    metadata=dict(meta) if isinstance(meta, dict) else None,
                )
                wire = encode_agent_response_for_wire(resp, response_id=req_id)
                await ws.send(json.dumps(wire, ensure_ascii=False))
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.exception("[MockAgentServer] 处理异常: %s", e)


async def run_mock_agent_server(
    host: str = "127.0.0.1",
    port: int = 8000,
) -> Any:
    """
    启动 Mock AgentServer（使用 mock_agent_server_handler），监听 host:port。
    返回 Server，调用方需在结束时 server.close(); await server.wait_closed()。
    websockets 14+ 使用 legacy.server.serve，与 legacy 客户端一致，避免 InvalidMessage。
    """
    try:
        from websockets.legacy.server import serve as legacy_serve
        server = await legacy_serve(mock_agent_server_handler, host, port)
    except ImportError:
        import websockets
        server = await websockets.serve(mock_agent_server_handler, host, port)
    logger.info("[MockAgentServer] 已启动: ws://%s:%s", host, port)
    return server


# ---------------------------------------------------------------------------
# 自验证：内存 Mock 服务端 + main
# ---------------------------------------------------------------------------


async def _run_verification() -> None:
    """用内存 Mock 服务端验证 WebSocketAgentServerClient 的 connect/send_request/send_request_stream."""
    from e2a.gateway_normalize import e2a_from_agent_fields

    port = 18765
    uri = f"ws://127.0.0.1:{port}"
    server = await run_mock_agent_server("127.0.0.1", port)
    logger.info("[main] Mock AgentServer 已启动: %s", uri)

    client = WebSocketAgentServerClient()
    try:
        await client.connect(uri)

        # 1. 非流式请求
        req1 = e2a_from_agent_fields(
            request_id="req-1",
            channel_id="ch-1",
            session_id="sess-1",
            params={"message": "你好"},
        )
        resp1 = await client.send_request(req1)
        assert resp1.request_id == "req-1"
        assert resp1.ok is True
        assert "Echo:" in str(resp1.payload)
        logger.info("[main] 非流式验证通过: payload=%s", resp1.payload)

        # 2. 流式请求
        req2 = e2a_from_agent_fields(
            request_id="req-2",
            channel_id="ch-1",
            session_id="sess-1",
            params={"message": "流式测试"},
        )
        chunks = []
        async for ch in client.send_request_stream(req2):
            chunks.append(ch)
        assert len(chunks) == 3
        assert chunks[-1].is_complete
        full_content = "".join(c.payload.get("content", "") for c in chunks if c.payload)
        logger.info("[main] 流式验证通过: 共 %s 个 chunk, 拼接内容=%r", len(chunks), full_content)
    finally:
        await client.disconnect()
        server.close()
        await server.wait_closed()
    logger.info("[main] 验证完成，功能正常")


def main() -> None:
    """入口：配置日志并运行自验证."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(_run_verification())


if __name__ == "__main__":
    main()
