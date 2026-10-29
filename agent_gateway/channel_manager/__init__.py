# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Channel 模块 - 客户端连接抽象."""

from agent_gateway.channel_manager.base import BaseChannel, ChannelMetadata
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.web_connect import WebChannel
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XiaoyiChannel, XiaoyiChannelConfig,
)
from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import TelegramChannel, \
    TelegramChannelConfig
from agent_gateway.channel_manager.im_platforms.discord.discord_connect import DiscordChannel, DiscordChannelConfig
from agent_gateway.channel_manager.im_platforms.slack.slack_connect import SlackChannel, SlackChannelConfig
from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import DingTalkChannel, DingTalkConfig
from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import WhatsAppChannel, \
    WhatsAppChannelConfig
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
