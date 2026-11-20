# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

import logging
import asyncio
import inspect
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from e2a.message import Message
from agent_gateway.common.event_names import event_name_text
from agent_gateway.routing.session_sharing import RoutingTarget


logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    pass

# 连接钩子签名: (ws) -> None | Awaitable[None]
ConnectHook = Callable[..., Any]
_UNAUTHORIZED_BODY = b"Unauthorized\n"

# 仅 Web/TUI 消费的运行时遥测事件。IM 渠道没有这些事件的呈现语义，而各渠道的
# 通用内容提取会把任何带文本的 payload 当作正文发送，导致 chat.llm_call_end 在
# chat.delta/chat.final 已展示答案之后再发一条重复消息（心跳与工具轮次都会触发）。
# 所有 IM 渠道在 send() 入口统一丢弃，不要依赖上游是否过滤。
WEB_ONLY_TELEMETRY_EVENTS: frozenset[str] = frozenset(
    {
        "chat.llm_call_start",
        "chat.llm_call_end",
        "chat.output_phase",
        "chat.usage_metadata",
        "chat.usage_summary",
        "context.usage",
    }
)


def resolve_event_name(msg: Message) -> str:
    """取消息事件名：兼容 EventType 枚举、裸字符串与 payload.event_type。"""
    event_name = event_name_text(getattr(msg, "event_type", None))
    if not event_name:
        payload = getattr(msg, "payload", None)
        if isinstance(payload, dict):
            event_name = event_name_text(payload.get("event_type"))
    return event_name


def is_web_only_telemetry_event(msg: Message) -> bool:
    """判断消息是否为只给 Web/TUI 的运行时遥测；IM 渠道 send() 入口应直接丢弃。"""
    return resolve_event_name(msg) in WEB_ONLY_TELEMETRY_EVENTS


class ChannelType(str, Enum):
    """Channel 类型枚举."""
    ACP = "acp"
    WEB = "web"
    FEISHU = "feishu"
    XIAOYI = "xiaoyi"
    DINGTALK = "dingtalk"
    TELEGRAM = "telegram"
    DISCORD = "discord"
    SLACK = "slack"
    WHATSAPP = "whatsapp"
    WECOM = "wecom"
    WECHAT = "wechat"
    SSH = "ssh"
    CLI = "tui"


@dataclass
class ChannelMetadata:
    """Channel 元数据."""

    channel_id: str
    source: str
    user_id: str | None = None
    extra: dict[str, Any] | None = None


class RobotMessageRouter:
    """管理整个系统的入站（从通道到机器人）和出站（从机器人到通道）消息队列，并提供出站消息的订阅/分发机制。"""
    def __init__(self):
        self._user_messages: asyncio.Queue[Message] = asyncio.Queue()
        self._robot_messages: asyncio.Queue[Message] = asyncio.Queue()
        self._channel_subscriptions: dict[str, list[Callable[[Message], Awaitable[None]]]] = {}
        self._is_active = False

    async def route_user_message(self, msg: Message) -> None:
        """将接收到的消息放入user_messages队列，等待机器人处理。"""
        await self._user_messages.put(msg)

    async def wait_for_user_message(self) -> Message:
        """阻塞地从user_messages队列中取出一条消息进行处理。"""
        return await self._user_messages.get()

    async def queue_robot_message(self, msg: Message) -> None:
        """将生成的回复消息放入robot_messages队列。"""
        await self._robot_messages.put(msg)

    async def wait_for_robot_message(self) -> Message:
        """阻塞地从robot_messages队列取消息，主要用于调试或直接消费（但框架通常使用订阅分发机制）。"""
        return await self._robot_messages.get()

    def register_channel_subscription(
        self,
        channel: str,
        callback: Callable[[Message], Awaitable[None]]
    ) -> None:
        """允许通道（或其他组件）注册一个异步回调函数，专门接收目标为特定通道ID的出站消息。"""
        if channel not in self._channel_subscriptions:
            self._channel_subscriptions[channel] = []
        self._channel_subscriptions[channel].append(callback)

    async def dispatch_robot_messages(self) -> None:
        """
        持续监听robot_messages队列，将每条消息分发给对应通道的订阅回调。
        """
        self._is_active = True
        while self._is_active:
            try:
                msg = await asyncio.wait_for(self._robot_messages.get(), timeout=1.0)
                subscribers = self._channel_subscriptions.get(msg.channel_id, [])
                for callback in subscribers:
                    try:
                        await callback(msg)
                    except Exception as e:
                        logger.error(f"Error dispatching to {msg.channel_id}: {e}")
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        """Stop the dispatcher loop."""
        self._is_active = False

    @property
    def pending_incoming_count(self) -> int:
        """待处理的入站消息数量"""
        return self._user_messages.qsize()

    @property
    def pending_outgoing_count(self) -> int:
        """待发送的出站消息数量"""
        return self._robot_messages.qsize()


class BaseChannel(ABC):
    """
    Channel实现的抽象基类。

    每个Channel都应该实现这个接口
    以集成到纳米机器人消息总线中。
    """

    name: str = "base"

    def __init__(self, config: Any, router: RobotMessageRouter):
        """
        初始化Channel
        """
        self.config = config
        self.bus = router
        self._running = False
        self.start_task: Any = None

    @abstractmethod
    async def start(self) -> None:
        """
        启动Channel并开始监听消息

        一个长期运行的异步任务，需要：
        1. 连接到聊天平台
        2. 监听传入消息
        3. 通过_handle_message()将消息转发到总线
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """停止Channel并清理资源"""
        pass

    async def send(
        self,
        msg: Message,
        *,
        routing_target: RoutingTarget | None = None,
    ) -> None:
        """通过 Channel 发送消息。

        V2 签名（2 参数）：
          msg            — 消息内容
          routing_target — RoutingTarget（自包含：intent + routing_keys + at_user_ids + delivery）
        """
        raise NotImplementedError(f"{self.channel_id} does not implement send()")

    def is_allowed(self, sender_id: str) -> bool:
        """
        检查发送者是否被允许使用此机器人
        """
        allow_list = getattr(self.config, "allow_from", [])

        # If no allow list, allow everyone
        if not allow_list:
            return True

        sender_str = str(sender_id)
        if sender_str in allow_list:
            return True
        if "|" in sender_str:
            for part in sender_str.split("|"):
                if part and part in allow_list:
                    return True
        return False

    async def _handle_message(
            self,
            chat_id: str,
            content: str,
            metadata: dict[str, Any] | None = None
    ) -> None:

        msg = Message(
            id=chat_id,
            type="req",
            channel_id=self.name,
            session_id=str(chat_id),
            params={'content': content},
            timestamp=time.time(),
            ok=True,
            metadata=metadata
        )

        await self.bus.route_user_message(msg)

    @property
    def is_running(self) -> bool:
        """Check if the channel_id is running."""
        return self._running


class BaseWebChannel(BaseChannel):

    def __init__(self, config: Any, router: RobotMessageRouter):
        """
        初始化Channel
        """
        super().__init__(config, router)
        self._connect_hooks: list[ConnectHook] = []
        self._disconnect_hooks: list[ConnectHook] = []
        self._handshake_auth: Callable[..., Any] | None = None

    def on_connect(self, callback: ConnectHook) -> None:
        self._connect_hooks.append(callback)

    def on_disconnect(self, callback: ConnectHook) -> None:
        self._disconnect_hooks.append(callback)

    def set_handshake_auth(self, callback: Callable[..., Any]) -> None:
        self._handshake_auth = callback

    async def authenticate_handshake(
        self,
        *,
        path: str,
        headers: Any,
        remote: str = "",
        channel: str = "",
    ) -> Any:
        """Return the handshake result so callers can retain its user identity."""
        hook = self._handshake_auth
        if hook is None:
            return None
        result = hook(path=path, headers=headers, remote=remote, channel=channel)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def handshake_auth_denied(
        self,
        *,
        path: str,
        headers: Any,
        remote: str = "",
        channel: str = "",
    ) -> bool:
        """Return True when handshake auth is set and rejected the upgrade."""
        result = await self.authenticate_handshake(
            path=path, headers=headers, remote=remote, channel=channel
        )
        return result is not None and not bool(getattr(result, "success", True))

    @staticmethod
    def unauthorized_handshake_response(process_request_args: tuple[Any, ...]) -> Any:
        """Build a 401 response for legacy/new websockets process_request APIs."""
        status = HTTPStatus.UNAUTHORIZED
        headers = [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Content-Length", str(len(_UNAUTHORIZED_BODY))),
        ]

        if process_request_args and not isinstance(process_request_args[0], str):
            from websockets.datastructures import Headers
            from websockets.http11 import Response

            return Response(status.value, status.phrase, Headers(headers), _UNAUTHORIZED_BODY)

        return status, headers, _UNAUTHORIZED_BODY
