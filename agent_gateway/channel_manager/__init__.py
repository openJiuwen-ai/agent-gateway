# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Channel 模块 - 客户端连接抽象."""

from typing import TYPE_CHECKING, Any

from agent_gateway.channel_manager.base import BaseChannel, ChannelMetadata
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.web_connect import WebChannel

if TYPE_CHECKING:
    from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
        XiaoyiChannel, XiaoyiChannelConfig,
    )
    from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import (
        TelegramChannel, TelegramChannelConfig,
    )
    from agent_gateway.channel_manager.im_platforms.discord.discord_connect import (
        DiscordChannel, DiscordChannelConfig,
    )
    from agent_gateway.channel_manager.im_platforms.slack.slack_connect import (
        SlackChannel, SlackChannelConfig,
    )
    from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
        DingTalkChannel, DingTalkConfig,
    )
    from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import (
        WhatsAppChannel, WhatsAppChannelConfig,
    )
    from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import WecomChannel, WecomConfig
    from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import WechatChannel, WechatConfig
    from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpChannel, AcpChannelConfig

__all__ = [
    "BaseChannel",
    "ChannelManager",
    "ChannelMetadata",
    "WebChannel",
    "XiaoyiChannel",
    "XiaoyiChannelConfig",
    "TelegramChannel",
    "TelegramChannelConfig",
    "DiscordChannel",
    "DiscordChannelConfig",
    "SlackChannel",
    "SlackChannelConfig",
    "DingTalkChannel",
    "DingTalkConfig",
    "WhatsAppChannel",
    "WhatsAppChannelConfig",
    "WecomChannel",
    "WecomConfig",
    "WechatChannel",
    "WechatConfig",
    "AcpChannel",
    "AcpChannelConfig",
]


def __getattr__(name: str) -> Any:
    """Resolve optional channel exports on first access.

    Optional channel modules import their platform SDKs (telegram, discord,
    slack_bolt, dingtalk_stream, wecom_aibot_sdk, ...). Importing them while
    initializing this package would load every SDK at process start even when
    none of these channels is enabled, so each export is resolved lazily.
    """
    if name in ("XiaoyiChannel", "XiaoyiChannelConfig"):
        from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
            XiaoyiChannel, XiaoyiChannelConfig,
        )

        exports = {"XiaoyiChannel": XiaoyiChannel, "XiaoyiChannelConfig": XiaoyiChannelConfig}
    elif name in ("TelegramChannel", "TelegramChannelConfig"):
        from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import (
            TelegramChannel, TelegramChannelConfig,
        )

        exports = {"TelegramChannel": TelegramChannel, "TelegramChannelConfig": TelegramChannelConfig}
    elif name in ("DiscordChannel", "DiscordChannelConfig"):
        from agent_gateway.channel_manager.im_platforms.discord.discord_connect import (
            DiscordChannel, DiscordChannelConfig,
        )

        exports = {"DiscordChannel": DiscordChannel, "DiscordChannelConfig": DiscordChannelConfig}
    elif name in ("SlackChannel", "SlackChannelConfig"):
        from agent_gateway.channel_manager.im_platforms.slack.slack_connect import SlackChannel, SlackChannelConfig

        exports = {"SlackChannel": SlackChannel, "SlackChannelConfig": SlackChannelConfig}
    elif name in ("DingTalkChannel", "DingTalkConfig"):
        from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import DingTalkChannel, DingTalkConfig

        exports = {"DingTalkChannel": DingTalkChannel, "DingTalkConfig": DingTalkConfig}
    elif name in ("WhatsAppChannel", "WhatsAppChannelConfig"):
        from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import (
            WhatsAppChannel, WhatsAppChannelConfig,
        )

        exports = {"WhatsAppChannel": WhatsAppChannel, "WhatsAppChannelConfig": WhatsAppChannelConfig}
    elif name in ("WecomChannel", "WecomConfig"):
        from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import WecomChannel, WecomConfig

        exports = {"WecomChannel": WecomChannel, "WecomConfig": WecomConfig}
    elif name in ("WechatChannel", "WechatConfig"):
        from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import WechatChannel, WechatConfig

        exports = {"WechatChannel": WechatChannel, "WechatConfig": WechatConfig}
    elif name in ("AcpChannel", "AcpChannelConfig"):
        from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpChannel, AcpChannelConfig

        exports = {"AcpChannel": AcpChannel, "AcpChannelConfig": AcpChannelConfig}
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals().update(exports)
    return exports[name]


def __dir__() -> list[str]:
    """Keep exports that are not resolved yet visible to dir()."""
    return sorted(set(globals()) | set(__all__))
