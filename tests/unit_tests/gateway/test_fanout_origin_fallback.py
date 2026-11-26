"""fan_out 无人接收时回退发起连接（避免 team 广播帧被静默丢弃）。

回归背景：web 首次 ``chat.send`` 不带 ``mode`` 时，Gateway 不会为该连接注册
GodView；AgentServer 仍按会话的 team 模式产出带 fan_out_targets 的帧，
``dispatch_to_session`` 找不到订阅者，整条流被丢弃，客户端等不到终态。
"""

from __future__ import annotations

import pytest

from e2a.message import Message
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.routing.keys import AgentRef, ChannelKey, RoutingKey, make_delivery_target
from agent_gateway.routing.session_sharing import (
    LogicalTarget,
    SessionDispatcher,
    SessionSharingRegistry,
    SubRole,
)


class _RecordingChannel:
    def __init__(self) -> None:
        self.sends: list[object] = []

    async def send(self, msg, routing_target=None) -> None:
        self.sends.append(routing_target)


class _FailingChannel:
    async def send(self, msg, routing_target=None) -> None:
        raise RuntimeError("send failed")


class _Channels:
    def __init__(self, mapping: dict[ChannelKey, _RecordingChannel]) -> None:
        self._mapping = mapping
        self.enabled_channels: list[str] = []

    def get_channel(self, channel_id):
        if isinstance(channel_id, ChannelKey):
            return self._mapping.get(channel_id)
        for key, channel in self._mapping.items():
            if key.channel_id == channel_id:
                return channel
        return None


def _message(*, delivery_scope: str = "") -> Message:
    metadata: dict = {"fan_out_targets": [{"intent": "godview"}], "ws_id": "ws-1"}
    if delivery_scope:
        metadata["delivery_scope"] = delivery_scope
    return Message(
        id="chunk-1",
        type="event",
        channel_id="web",
        app_id="default",
        session_id="sess-1",
        params={},
        timestamp=0,
        ok=True,
        payload={"event_type": "chat.delta", "content": "hi", "session_id": "sess-1"},
        metadata=metadata,
    )


async def _registry(*, with_godview: bool) -> SessionSharingRegistry:
    registry = SessionSharingRegistry()
    if not with_godview:
        return registry
    await registry.register(
        "sess-1",
        SubRole.GODVIEW,
        RoutingKey(
            user_id="web-user",
            channel_id="web",
            app_id="default",
            agent_ref=AgentRef("team", "default"),
            session_id="sess-1",
        ),
        make_delivery_target("web", ws_id="ws-1"),
    )
    return registry


@pytest.mark.asyncio
class TestDispatchReturnsDeliveryCount:
    async def test_godview_subscriber_is_counted(self) -> None:
        channel = _RecordingChannel()
        channels = _Channels({ChannelKey("web", "default"): channel})

        delivered = await SessionDispatcher.dispatch(
            _message(),
            "sess-1",
            [LogicalTarget(intent="godview")],
            channels,
            await _registry(with_godview=True),
        )

        assert delivered == 1
        assert len(channel.sends) == 1

    async def test_missing_godview_reports_zero(self) -> None:
        channel = _RecordingChannel()
        channels = _Channels({ChannelKey("web", "default"): channel})

        delivered = await SessionDispatcher.dispatch(
            _message(),
            "sess-1",
            [LogicalTarget(intent="godview")],
            channels,
            await _registry(with_godview=False),
        )

        assert delivered == 0
        assert channel.sends == []

    async def test_subscriber_with_missing_channel_reports_zero(self) -> None:
        # 订阅还在 registry 里，但物理 channel 已注销/重建：不能算送达，
        # 否则上层不会回退，客户端仍等不到终态帧。
        channels = _Channels({})

        delivered = await SessionDispatcher.dispatch(
            _message(),
            "sess-1",
            [LogicalTarget(intent="godview")],
            channels,
            await _registry(with_godview=True),
        )

        assert delivered == 0

    async def test_subscriber_with_failing_send_reports_zero(self) -> None:
        channels = _Channels({ChannelKey("web", "default"): _FailingChannel()})

        delivered = await SessionDispatcher.dispatch(
            _message(),
            "sess-1",
            [LogicalTarget(intent="godview")],
            channels,
            await _registry(with_godview=True),
        )

        assert delivered == 0

    async def test_session_live_web_delivery_is_counted(self) -> None:
        # session_live 下 web 订阅被剔除出 sent_containers，但直播帧已由
        # _deliver_session_live_once 投给 web；不计入会让上层误判无人接收，
        # 触发 origin 回退并把同一帧二次投递给 web。
        web = _RecordingChannel()
        channels = _Channels({ChannelKey("web", "default"): web})

        delivered = await SessionDispatcher.dispatch(
            _message(delivery_scope="session_live"),
            "sess-1",
            [LogicalTarget(intent="godview")],
            channels,
            await _registry(with_godview=True),
        )

        assert delivered == 1
        assert len(web.sends) == 1


class TestOriginFallbackDecision:
    def test_godview_without_subscriber_falls_back(self) -> None:
        assert ChannelManager._fanout_needs_origin_fallback(
            [LogicalTarget(intent="godview")], 0,
        )

    def test_godview_with_delivery_does_not_fall_back(self) -> None:
        assert not ChannelManager._fanout_needs_origin_fallback(
            [LogicalTarget(intent="godview")], 1,
        )

    def test_mention_without_match_does_not_fall_back(self) -> None:
        # 未认领成员的定向消息是正常语义，不能广播给发起者。
        assert not ChannelManager._fanout_needs_origin_fallback(
            [LogicalTarget(intent="mention", member_names=("nobody",))], 0,
        )

    def test_empty_fan_out_does_not_fall_back(self) -> None:
        assert not ChannelManager._fanout_needs_origin_fallback([], 0)


class TestFallbackLogThrottle:
    def test_same_session_warns_once_within_interval(self) -> None:
        manager = object.__new__(ChannelManager)
        manager._fanout_fallback_warned_at = {}

        assert manager._should_warn_fanout_fallback("sess-1")
        # 流式 chunk 逐帧回退，同一 session 窗口内不再刷 warning。
        assert not manager._should_warn_fanout_fallback("sess-1")
        assert manager._should_warn_fanout_fallback("sess-2")
