"""微信通道的 context_token 缓存必须有上限。

每个用户首次带 token 的消息都会写入 ``_context_tokens``，全仓没有删除路径，
用户量增长时字典无界膨胀。
"""

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
    _CONTEXT_TOKEN_LIMIT,
    WechatChannel,
    WechatConfig,
)


class _BoundedWechatChannel(WechatChannel):
    @property
    def context_tokens(self):
        return self._context_tokens

    def remember_context_token(self, user_id: str, token: str) -> None:
        self._remember_context_token(user_id, token)


def _build() -> _BoundedWechatChannel:
    return _BoundedWechatChannel(
        WechatConfig(enabled=True, bot_token="token"),
        RobotMessageRouter(),
    )


def test_context_tokens_stay_bounded():
    channel = _build()
    total = _CONTEXT_TOKEN_LIMIT * 3
    for i in range(total):
        channel.remember_context_token(f"user-{i}", f"tok-{i}")

    assert len(channel.context_tokens) == _CONTEXT_TOKEN_LIMIT
    assert "user-0" not in channel.context_tokens
    assert channel.context_tokens[f"user-{total - 1}"] == f"tok-{total - 1}"


def test_rewriting_token_refreshes_position():
    channel = _build()
    first = "user-keep"
    channel.remember_context_token(first, "tok-keep")
    for i in range(_CONTEXT_TOKEN_LIMIT - 1):
        channel.remember_context_token(f"user-{i}", f"tok-{i}")

    channel.remember_context_token(first, "tok-keep-2")
    channel.remember_context_token("user-extra", "tok-extra")

    assert len(channel.context_tokens) == _CONTEXT_TOKEN_LIMIT
    assert channel.context_tokens[first] == "tok-keep-2"
    assert "user-0" not in channel.context_tokens


def test_empty_user_or_token_is_ignored():
    channel = _build()
    channel.remember_context_token("", "tok")
    channel.remember_context_token("user", "  ")

    assert channel.context_tokens == {}
