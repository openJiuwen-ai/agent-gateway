# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Optional channel configuration for the standalone Gateway."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from agent_gateway.channel_lifecycle import (
    _changed_optional_channels,
    _is_channel_enabled,
    _stop_channel,
)


logger = logging.getLogger("gateway")

# IM 平台官方 API 域名（仅作为 config.yaml 缺字段时的加载兜底，不在 Config 类里硬编码）
_FEISHU_DEFAULT_API_BASE = "https://open.feishu.cn"
_DINGTALK_DEFAULT_API_BASE = "https://api.dingtalk.com"    # 新版 v1.0 接口域名
_DINGTALK_DEFAULT_OAPI_BASE = "https://oapi.dingtalk.com"
_XIAOYI_DEFAULT_PUSH_URL = "https://hag.cloud.huawei.com/open-ability-agent/v1/agent-webhook"


class OptionalChannelConfigurator:
    """Apply configuration changes for optional IM and protocol channels."""

    def __init__(
        self,
        *,
        channel_manager: Any,
        web_channel: Any,
        agent_server_ext: Any,
        im_inbound: Any,
        im_outbound: Any,
        dummy_bus_cls: Any,
        get_config: Callable[[], dict[str, Any]],
        schedule_agent_prewarm_sync: Callable[..., Any],
        web_proxy_config_cls: Any,
        web_port_manager_cls: Any,
    ) -> None:
        self._channel_manager = channel_manager
        self._web_channel = web_channel
        self._agent_server_ext = agent_server_ext
        self._im_inbound = im_inbound
        self._im_outbound = im_outbound
        self._dummy_bus_cls = dummy_bus_cls
        self._get_config = get_config
        self._schedule_agent_prewarm_sync = schedule_agent_prewarm_sync
        self._web_proxy_config_cls = web_proxy_config_cls
        self._web_port_manager_cls = web_port_manager_cls

        self._last_channels_conf: dict = {}
        self._feishu_channel = None
        self._feishu_task = None
        self._feishu_enterprise_channels: dict[str, Any] = {}
        self._feishu_enterprise_tasks: dict[str, asyncio.Task] = {}
        self._xiaoyi_channel = None
        self._xiaoyi_task = None
        self._dingtalk_channel = None
        self._dingtalk_task = None
        self._telegram_channel = None
        self._telegram_task = None
        self._discord_channel = None
        self._discord_task = None
        self._slack_channel = None
        self._slack_task = None
        self._whatsapp_channel = None
        self._whatsapp_task = None
        self._wecom_channel = None
        self._wecom_task = None
        self._wechat_channel = None
        self._wechat_task = None
        self._ssh_channel = None
        self._ssh_task = None

    def _set_agentos_ssh_key_issuer(
        self,
        issuer,
        *,
        ephemeral_key_ttl_sec: float = 300.0,
    ) -> None:
        """Inject/clear SshChannel as ephemeral key issuer on AgentOSRouter extension."""
        setter = getattr(self._agent_server_ext, "set_key_issuer", None)
        if not callable(setter):
            return
        try:
            setter(issuer, ephemeral_key_ttl_sec=ephemeral_key_ttl_sec)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[App] failed to set AgentOS SSH key issuer: %s", exc)

    async def _apply_feishu_channels(self, conf: dict, changed_channels: set[str]) -> None:
        if "feishu" not in changed_channels:
            return

        feishu_conf = conf.get("feishu") if isinstance(conf, dict) else {}

        # ---- 停止旧 apps 实例（从 channel_manager 查找） ----
        # 先 pop 再停止，避免 _stop_channel 内部 unregister_channel(channel_id)
        # 批量删除后后续 key 访问抛 KeyError
        for ch in self._channel_manager.pop_channels_by_id("feishu"):
            await _stop_channel(
                ch,
                getattr(ch, "start_task", None),
                f"feishu_app[{ch.app_id}]",
                channel_manager=self._channel_manager,
            )
        # 统一注销 adapter（所有 feishu app 共享 "feishu" 标识）
        self._im_inbound.unregister_adapter("feishu")
        self._im_outbound.unregister_adapter("feishu")

        # 单变量置空（不再使用，但保持 nonlocal 兼容）
        self._feishu_channel, self._feishu_task = None, None

        apps = feishu_conf.get("apps") or []
        if not apps:
            logger.info("[App] channels.feishu.apps empty, FeishuChannel disabled")
            return

        from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
            FeishuChannel,
            FeishuConfig,
        )
        for app in apps:
            if not app.get("enabled", True):
                continue
            enabled, reason = _is_channel_enabled(app, ["app_id", "app_secret"])
            if not enabled:
                logger.info("[App] channels.feishu.apps[].%s, skipping", reason)
                continue

            app_id = str(app.get("app_id") or "").strip()
            channel_id = "feishu"

            feishu_config = FeishuConfig(
                enabled=True,
                app_id=app_id,
                app_secret=str(app.get("app_secret") or "").strip(),
                encrypt_key=str(app.get("encrypt_key") or "").strip(),
                verification_token=str(app.get("verification_token") or "").strip(),
                allow_from=app.get("allow_from") or [],
                enable_streaming=bool(app.get("enable_streaming", True)),
                chat_id=str(app.get("chat_id") or "").strip(),
                channel_id=channel_id,
                last_chat_id=str(app.get("last_chat_id") or "").strip(),
                last_open_id=str(app.get("last_open_id") or "").strip(),
                group_digital_avatar=bool(app.get("group_digital_avatar", False)),
                my_user_id=str(app.get("my_user_id") or app.get("my_open_id") or "").strip(),
                bot_name=str(app.get("bot_name") or "").strip(),
                enable_memory=bool(app.get("enable_memory", False)),
                message_merge_window_ms=int(app.get("message_merge_window_ms", 15000)),
                api_base=str(app.get("api_base") or "").strip() or _FEISHU_DEFAULT_API_BASE,
            )
            # 数字分身 adapter（共享 "feishu" key）
            feishu_adapter = None
            if feishu_config.group_digital_avatar:
                from agent_gateway.channel_manager.im_platforms.feishu.feishu_im_adapter import (
                    FeishuIMPlatformAdapter,
                )
                feishu_adapter = FeishuIMPlatformAdapter(
                    my_open_id=feishu_config.my_user_id,
                    bot_name=feishu_config.bot_name,
                )
                self._im_inbound.register_adapter(channel_id, feishu_adapter)
                self._im_outbound.register_adapter(channel_id, feishu_adapter)

            channel = FeishuChannel(feishu_config, self._dummy_bus_cls(), im_platform_adapter=feishu_adapter)
            self._channel_manager.register_channel(channel)
            task = asyncio.create_task(channel.start(), name=f"feishu-{app_id}")
            channel.start_task = task  # 挂到 channel 对象上，不另存 dict
            logger.info("[App] FeishuChannel(app=%s) registered from channels.feishu.apps", app_id)

    async def _apply_feishu_enterprise_channels(
        self,
        conf: dict,
        changed_channels: set[str],
    ) -> None:
        if "feishu_enterprise" not in changed_channels:
            return

        for bot_key, task in list(self._feishu_enterprise_tasks.items()):
            await _stop_channel(
                self._feishu_enterprise_channels.get(bot_key),
                task,
                f"feishu_enterprise[{bot_key}]",
                channel_manager=self._channel_manager,
            )
        for _old_ch in self._feishu_enterprise_channels.values():
            _old_ch_id = getattr(_old_ch, "_channel_id", "") or getattr(_old_ch, "name", "")
            if _old_ch_id:
                self._im_inbound.unregister_adapter(_old_ch_id)
                self._im_outbound.unregister_adapter(_old_ch_id)
        self._feishu_enterprise_channels = {}
        self._feishu_enterprise_tasks = {}

        enterprise_conf = conf.get("feishu_enterprise") if isinstance(conf, dict) else None
        if not isinstance(enterprise_conf, dict):
            logger.info(
                "[App] channels.feishu_enterprise missing or invalid; "
                "FeishuEnterpriseChannel disabled"
            )
            return

        from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
            FeishuChannel,
            FeishuConfig,
        )
        for bot_key, bot_conf_raw in enterprise_conf.items():
            if not isinstance(bot_key, str) or not bot_key.strip():
                continue
            bot_conf = bot_conf_raw if isinstance(bot_conf_raw, dict) else None
            if bot_conf is None:
                logger.info("[App] channels.feishu_enterprise.%s invalid config, skipping", bot_key)
                continue
            enabled, reason = _is_channel_enabled(bot_conf, ["app_id", "app_secret"])
            if not enabled:
                logger.info(
                    "[App] channels.feishu_enterprise.%s.%s, FeishuEnterpriseChannel disabled",
                    bot_key,
                    reason,
                )
                continue

            bot_key = bot_key.strip()
            app_id = str(bot_conf.get("app_id") or "").strip()
            channel_id = f"feishu_enterprise:{app_id}"
            feishu_config = FeishuConfig(
                enabled=True,
                app_id=app_id,
                app_secret=str(bot_conf.get("app_secret") or "").strip(),
                encrypt_key=str(bot_conf.get("encrypt_key") or "").strip(),
                verification_token=str(bot_conf.get("verification_token") or "").strip(),
                allow_from=bot_conf.get("allow_from") or [],
                enable_streaming=bool(bot_conf.get("enable_streaming", True)),
                chat_id=str(bot_conf.get("chat_id") or "").strip(),
                channel_id=channel_id,
                bot_key=bot_key,
                last_chat_id=str(bot_conf.get("last_chat_id") or "").strip(),
                last_open_id=str(bot_conf.get("last_open_id") or "").strip(),
                my_user_id=str(bot_conf.get("my_user_id") or "").strip(),
                bot_name=str(bot_conf.get("bot_name") or "").strip(),
                group_digital_avatar=bool(bot_conf.get("group_digital_avatar", False)),
                enable_memory=bool(bot_conf.get("enable_memory", False)),
                api_base=str(bot_conf.get("api_base") or "").strip() or _FEISHU_DEFAULT_API_BASE,
            )
            feishu_adapter = None
            if feishu_config.group_digital_avatar:
                from agent_gateway.channel_manager.im_platforms.feishu.feishu_im_adapter import (
                    FeishuIMPlatformAdapter,
                )
                feishu_adapter = FeishuIMPlatformAdapter(
                    my_open_id=feishu_config.my_user_id,
                    bot_name=feishu_config.bot_name,
                )
                self._im_inbound.register_adapter(channel_id, feishu_adapter)
                self._im_outbound.register_adapter(channel_id, feishu_adapter)
            channel = FeishuChannel(feishu_config, self._dummy_bus_cls(), im_platform_adapter=feishu_adapter)
            self._channel_manager.register_channel(channel)
            task = asyncio.create_task(channel.start(), name=f"feishu-enterprise-{bot_key}")
            self._feishu_enterprise_channels[bot_key] = channel
            self._feishu_enterprise_tasks[bot_key] = task
            logger.info(
                "[App] registered FeishuChannel(%s) from config.yaml.channels.feishu_enterprise.%s",
                bot_key,
                channel_id,
            )

    async def apply(self, conf: dict) -> None:
        from agent_gateway.channel_manager.web.channel_config_normalizers import (
            _normalize_feishu_conf,
            _normalize_xiaoyi_conf,
        )

        # === 入口归一化（必须在 _should_restart_channel 之前） ===
        if isinstance(conf, dict):
            feishu_raw = conf.get("feishu")
            if isinstance(feishu_raw, dict):
                conf["feishu"] = _normalize_feishu_conf(feishu_raw)
            xiaoyi_raw = conf.get("xiaoyi")
            if isinstance(xiaoyi_raw, dict):
                conf["xiaoyi"] = _normalize_xiaoyi_conf(xiaoyi_raw)
        # ==========================================================

        restart_pending = self._channel_manager.pop_channel_restart_pending()
        changed_channels = _changed_optional_channels(
            self._last_channels_conf,
            conf,
            restart_pending,
        )
        self._last_channels_conf = dict(conf or {})

        await self._apply_feishu_channels(conf, changed_channels)

        await self._apply_feishu_enterprise_channels(conf, changed_channels)

        if "xiaoyi" in changed_channels:
            xiaoyi_conf = conf.get("xiaoyi") if isinstance(conf, dict) else {}

            # ---- 停止旧 apps 实例（从 channel_manager 查找） ----
            # 先 pop 再停止，避免 _stop_channel 内部 unregister_channel(channel_id)
            # 批量删除后后续 key 访问抛 KeyError
            for ch in self._channel_manager.pop_channels_by_id("xiaoyi"):
                await _stop_channel(
                    ch,
                    getattr(ch, "start_task", None),
                    f"xiaoyi_app[{ch.app_id}]",
                    channel_manager=self._channel_manager,
                )

            # 单变量置空（不再使用，但保持 nonlocal 兼容）
            self._xiaoyi_channel, self._xiaoyi_task = None, None

            apps = xiaoyi_conf.get("apps") or []
            if not apps:
                logger.info("[App] channels.xiaoyi.apps empty, XiaoyiChannel disabled")
            else:
                from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
                    XiaoyiChannel,
                    XiaoyiChannelConfig,
                )
                for app in apps:
                    if not app.get("enabled", True):
                        continue
                    enabled, reason = _is_channel_enabled(app, ["ak", "sk", "agent_id"])
                    if not enabled:
                        logger.info("[App] channels.xiaoyi.apps[].%s, skipping", reason)
                        continue

                    api_id = str(app.get("api_id") or "").strip()
                    is_default = app.get("is_default", False) or len(apps) == 1
                    if not api_id and not is_default:
                        logger.warning(
                            "[App] channels.xiaoyi.apps[].api_id required for non-default, skipping"
                        )
                        continue

                    channel_id = "xiaoyi"
                    config = XiaoyiChannelConfig(
                        enabled=True,
                        channel_id=channel_id,
                        mode=str(app.get("mode") or "xiaoyi_channel").strip(),
                        ak=str(app.get("ak") or "").strip(),
                        sk=str(app.get("sk") or "").strip(),
                        agent_id=str(app.get("agent_id") or "").strip(),
                        api_id=api_id,
                        enable_streaming=bool(app.get("enable_streaming", True)),
                        # 以下字段 _normalize_xiaoyi_conf 会自动填充缺省值，
                        # 此处显式读取以兼容直接使用 apps 格式的场景
                        ws_url1=str(app.get("ws_url1") or "").strip(),
                        ws_url2=str(app.get("ws_url2") or "").strip(),
                        uid=str(app.get("uid") or "").strip(),
                        api_key=str(app.get("api_key") or "").strip(),
                        push_id=str(app.get("push_id") or "").strip(),
                        push_url=str(app.get("push_url") or "").strip() or _XIAOYI_DEFAULT_PUSH_URL,
                        file_upload_url=str(app.get("file_upload_url") or "").strip(),
                    )
                    channel = XiaoyiChannel(config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(channel)
                    task = asyncio.create_task(channel.start(), name=f"xiaoyi-{api_id or 'default'}")
                    channel.start_task = task  # 挂到 channel 对象上，不另存 dict
                    logger.info("[App] XiaoyiChannel(api_id=%s) from channels.xiaoyi.apps", api_id or "default")

        if "dingtalk" in changed_channels:
            dingtalk_conf = conf.get("dingtalk") if isinstance(conf, dict) else None
            await _stop_channel(
                self._dingtalk_channel,
                self._dingtalk_task,
                "dingtalk",
                background_wait=True,
                channel_manager=self._channel_manager,
            )
            self._dingtalk_channel, self._dingtalk_task = None, None

            if isinstance(dingtalk_conf, dict):
                enabled, reason = _is_channel_enabled(dingtalk_conf, ["client_id", "client_secret"])
                if not enabled:
                    logger.info("[App] channels.dingtalk.%s, DingTalkChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
                        DingTalkChannel,
                        DingTalkConfig,
                    )
                    dingtalk_config = DingTalkConfig(
                        enabled=True,
                        client_id=str(dingtalk_conf.get("client_id") or "").strip(),
                        client_secret=str(dingtalk_conf.get("client_secret") or "").strip(),
                        allow_from=dingtalk_conf.get("allow_from") or [],
                        api_base=str(dingtalk_conf.get("api_base") or "").strip() or _DINGTALK_DEFAULT_API_BASE,
                        oapi_base=str(dingtalk_conf.get("oapi_base") or "").strip() or _DINGTALK_DEFAULT_OAPI_BASE,
                    )
                    self._dingtalk_channel = DingTalkChannel(dingtalk_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._dingtalk_channel)
                    self._dingtalk_task = asyncio.create_task(self._dingtalk_channel.start(), name="dingtalk")
                    logger.info("[App] DingTalkChannel registered from config.yaml.channels.dingtalk")
            else:
                logger.info("[App] channels.dingtalk missing or invalid, DingTalkChannel disabled")

        if "telegram" in changed_channels:
            telegram_conf = conf.get("telegram") if isinstance(conf, dict) else None
            await _stop_channel(
                self._telegram_channel,
                self._telegram_task,
                "telegram",
                channel_manager=self._channel_manager,
            )
            self._telegram_channel, self._telegram_task = None, None

            if isinstance(telegram_conf, dict):
                enabled, reason = _is_channel_enabled(telegram_conf, ["bot_token"])
                if not enabled:
                    logger.info("[App] channels.telegram.%s, TelegramChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import (
                        TelegramChannel,
                        TelegramChannelConfig,
                    )
                    telegram_config = TelegramChannelConfig(
                        enabled=True,
                        bot_token=str(telegram_conf.get("bot_token") or "").strip(),
                        allow_from=telegram_conf.get("allow_from") or [],
                        parse_mode=str(telegram_conf.get("parse_mode") or "Markdown").strip(),
                        group_chat_mode=str(telegram_conf.get("group_chat_mode") or "mention").strip(),
                    )
                    self._telegram_channel = TelegramChannel(telegram_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._telegram_channel)
                    self._telegram_task = asyncio.create_task(self._telegram_channel.start(), name="telegram")
                    logger.info("[App] TelegramChannel registered from config.yaml.channels.telegram")
            else:
                logger.info("[App] channels.telegram missing or invalid, TelegramChannel disabled")

        if "discord" in changed_channels:
            discord_conf = conf.get("discord") if isinstance(conf, dict) else None
            await _stop_channel(
                self._discord_channel,
                self._discord_task,
                "discord",
                channel_manager=self._channel_manager,
            )
            self._discord_channel, self._discord_task = None, None

            if isinstance(discord_conf, dict):
                enabled, reason = _is_channel_enabled(discord_conf, ["bot_token"])
                if not enabled:
                    logger.info("[App] channels.discord.%s, DiscordChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.discord.discord_connect import (
                        DiscordChannel,
                        DiscordChannelConfig,
                    )
                    discord_config = DiscordChannelConfig(
                        enabled=True,
                        bot_token=str(discord_conf.get("bot_token") or "").strip(),
                        application_id=str(discord_conf.get("application_id") or "").strip(),
                        guild_id=str(discord_conf.get("guild_id") or "").strip(),
                        channel_id=str(discord_conf.get("channel_id") or "").strip(),
                        allow_from=discord_conf.get("allow_from") or [],
                        block_dm=(str(discord_conf.get("block_dm")).lower() in ["true", "1"]) or False,
                    )
                    self._discord_channel = DiscordChannel(discord_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._discord_channel)
                    self._discord_task = asyncio.create_task(self._discord_channel.start(), name="discord")
                    logger.info("[App] DiscordChannel registered from config.yaml.channels.discord")
            else:
                logger.info("[App] channels.discord missing or invalid, DiscordChannel disabled")

        if "slack" in changed_channels:
            slack_conf = conf.get("slack") if isinstance(conf, dict) else None
            await _stop_channel(
                self._slack_channel,
                self._slack_task,
                "slack",
                channel_manager=self._channel_manager,
            )
            self._slack_channel, self._slack_task = None, None

            if isinstance(slack_conf, dict):
                enabled, reason = _is_channel_enabled(slack_conf, ["bot_token", "app_token"])
                if not enabled:
                    logger.info("[App] channels.slack.%s, SlackChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.slack.slack_connect import (
                        SlackChannel,
                        SlackChannelConfig,
                    )
                    reply_in_thread_raw = slack_conf.get("reply_in_thread", True)
                    reply_in_thread = (
                        str(reply_in_thread_raw).strip().lower() in ("true", "1", "yes", "on")
                        if isinstance(reply_in_thread_raw, str)
                        else bool(reply_in_thread_raw)
                    )
                    slack_config = SlackChannelConfig(
                        enabled=True,
                        bot_token=str(slack_conf.get("bot_token") or "").strip(),
                        app_token=str(slack_conf.get("app_token") or "").strip(),
                        allow_from=slack_conf.get("allow_from") or [],
                        allowed_channel_ids=slack_conf.get("allowed_channel_ids") or [],
                        default_channel_id=str(slack_conf.get("default_channel_id") or "").strip(),
                        reply_in_thread=reply_in_thread,
                    )
                    self._slack_channel = SlackChannel(slack_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._slack_channel)
                    self._slack_task = asyncio.create_task(self._slack_channel.start(), name="slack")
                    logger.info("[App] SlackChannel registered from config.yaml.channels.slack")
            else:
                logger.info("[App] channels.slack missing or invalid, SlackChannel disabled")

        if "whatsapp" in changed_channels:
            whatsapp_conf = conf.get("whatsapp") if isinstance(conf, dict) else None
            await _stop_channel(
                self._whatsapp_channel,
                self._whatsapp_task,
                "whatsapp",
                channel_manager=self._channel_manager,
            )
            self._whatsapp_channel, self._whatsapp_task = None, None

            if isinstance(whatsapp_conf, dict):
                bridge_ws_url = str(whatsapp_conf.get("bridge_ws_url") or "ws://127.0.0.1:19600/ws").strip()
                default_jid = str(whatsapp_conf.get("default_jid") or "").strip()
                allow_from = whatsapp_conf.get("allow_from") or []
                enable_streaming = bool(whatsapp_conf.get("enable_streaming", True))
                auto_start_bridge = bool(whatsapp_conf.get("auto_start_bridge", False))
                bridge_command = str(
                    whatsapp_conf.get("bridge_command") or "node scripts/whatsapp-bridge.js"
                ).strip()
                bridge_workdir = str(whatsapp_conf.get("bridge_workdir") or "").strip()
                bridge_env_raw = whatsapp_conf.get("bridge_env") or {}
                bridge_env = bridge_env_raw if isinstance(bridge_env_raw, dict) else {}

                enabled_raw = whatsapp_conf.get("enabled", None)
                if enabled_raw is None:
                    enabled = bool(bridge_ws_url)
                else:
                    enabled = bool(enabled_raw)

                if not enabled:
                    logger.info("[App] channels.whatsapp.enabled = false, WhatsAppChannel disabled")
                elif not bridge_ws_url:
                    logger.info("[App] channels.whatsapp missing bridge_ws_url, WhatsAppChannel disabled")
                else:
                    from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import (
                        WhatsAppChannel,
                        WhatsAppChannelConfig,
                    )
                    whatsapp_config = WhatsAppChannelConfig(
                        enabled=True,
                        enable_streaming=enable_streaming,
                        bridge_ws_url=bridge_ws_url,
                        allow_from=allow_from,
                        default_jid=default_jid,
                        auto_start_bridge=auto_start_bridge,
                        bridge_command=bridge_command,
                        bridge_workdir=bridge_workdir,
                        bridge_env={str(k): str(v) for k, v in bridge_env.items()},
                    )
                    self._whatsapp_channel = WhatsAppChannel(whatsapp_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._whatsapp_channel)
                    self._whatsapp_task = asyncio.create_task(self._whatsapp_channel.start(), name="whatsapp")
                    logger.info("[App] WhatsAppChannel registered from config.yaml.channels.whatsapp")
            else:
                logger.info("[App] channels.whatsapp missing or invalid, WhatsAppChannel disabled")

        if "wecom" in changed_channels:
            wecom_conf = conf.get("wecom") if isinstance(conf, dict) else None
            await _stop_channel(
                self._wecom_channel,
                self._wecom_task,
                "wecom",
                channel_manager=self._channel_manager,
            )
            self._wecom_channel, self._wecom_task = None, None

            if isinstance(wecom_conf, dict):
                enabled, reason = _is_channel_enabled(wecom_conf, ["bot_id", "secret"])
                if not enabled:
                    logger.info("[App] channels.wecom.%s, WecomChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import (
                        WecomChannel,
                        WecomConfig,
                    )
                    wecom_config = WecomConfig(
                        enabled=True,
                        bot_id=str(wecom_conf.get("bot_id") or "").strip(),
                        secret=str(wecom_conf.get("secret") or "").strip(),
                        ws_url=str(wecom_conf.get("ws_url") or "wss://openws.work.weixin.qq.com").strip(),
                        allow_from=wecom_conf.get("allow_from") or [],
                        enable_streaming=bool(wecom_conf.get("enable_streaming", True)),
                        send_thinking_message=bool(wecom_conf.get("send_thinking_message", True)),
                        group_digital_avatar=bool(wecom_conf.get("group_digital_avatar", False)),
                        my_user_id=str(wecom_conf.get("my_user_id") or "").strip(),
                        bot_name=str(wecom_conf.get("bot_name") or "").strip(),
                        enable_memory=bool(wecom_conf.get("enable_memory", False)),
                    )
                    # 数字分身：创建 adapter 并注册到 pipeline
                    wecom_adapter = None
                    if wecom_config.group_digital_avatar:
                        from agent_gateway.channel_manager.im_platforms.wecom.wecom_im_adapter import (
                            WecomIMPlatformAdapter,
                        )
                        wecom_adapter = WecomIMPlatformAdapter(
                            my_user_id=wecom_config.my_user_id,
                            bot_name=wecom_config.bot_name,
                        )
                        self._im_inbound.register_adapter("wecom", wecom_adapter)
                        self._im_outbound.register_adapter("wecom", wecom_adapter)
                    self._wecom_channel = WecomChannel(wecom_config, self._dummy_bus_cls(), im_platform_adapter=wecom_adapter)
                    self._channel_manager.register_channel(self._wecom_channel)
                    self._wecom_task = asyncio.create_task(self._wecom_channel.start(), name="wecom")
                    logger.info("[App] WecomChannel registered from config.yaml.channels.wecom")
            else:
                logger.info("[App] channels.wecom missing or invalid, WecomChannel disabled")

        if "wechat" in changed_channels:
            wechat_conf = conf.get("wechat") if isinstance(conf, dict) else None
            await _stop_channel(
                self._wechat_channel,
                self._wechat_task,
                "wechat",
                channel_manager=self._channel_manager,
            )
            self._wechat_channel, self._wechat_task = None, None

            if isinstance(wechat_conf, dict):
                enabled, reason = _is_channel_enabled(wechat_conf, [])
                if not enabled:
                    logger.info("[App] channels.wechat.%s, WechatChannel disabled", reason)
                else:
                    from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
                        WechatChannel,
                        WechatConfig,
                    )
                    wechat_config = WechatConfig(
                        enabled=True,
                        base_url=str(wechat_conf.get("base_url") or "https://ilinkai.weixin.qq.com").strip(),
                        bot_token=str(wechat_conf.get("bot_token") or "").strip(),
                        ilink_bot_id=str(wechat_conf.get("ilink_bot_id") or "").strip(),
                        ilink_user_id=str(wechat_conf.get("ilink_user_id") or "").strip(),
                        allow_from=wechat_conf.get("allow_from") or [],
                        auto_login=bool(wechat_conf.get("auto_login", True)),
                        qrcode_poll_interval_sec=float(wechat_conf.get("qrcode_poll_interval_sec", 2.0)),
                        long_poll_timeout_sec=int(wechat_conf.get("long_poll_timeout_sec", 45)),
                        backoff_base_sec=float(wechat_conf.get("backoff_base_sec", 1.0)),
                        backoff_max_sec=float(wechat_conf.get("backoff_max_sec", 30.0)),
                        credential_file=str(
                            wechat_conf.get("credential_file") or "~/.wx-ai-bridge/credentials.json"
                        ).strip(),
                        enable_streaming=bool(wechat_conf.get("enable_streaming", True)),
                    )
                    self._wechat_channel = WechatChannel(wechat_config, self._dummy_bus_cls())
                    self._channel_manager.register_channel(self._wechat_channel)
                    self._wechat_task = asyncio.create_task(self._wechat_channel.start(), name="wechat")
                    logger.info("[App] WechatChannel registered from config.yaml.channels.wechat")
            else:
                logger.info("[App] channels.wechat missing or invalid, WechatChannel disabled")

        if "ssh" in changed_channels:
            ssh_conf = conf.get("ssh") if isinstance(conf, dict) else None
            await _stop_channel(
                self._ssh_channel,
                self._ssh_task,
                "ssh",
                channel_manager=self._channel_manager,
            )
            self._ssh_channel, self._ssh_task = None, None
            # Clear issuer whenever SSH channel is torn down / reconfigured.
            self._set_agentos_ssh_key_issuer(None)

            if isinstance(ssh_conf, dict):
                # 南向经 agent client（如 agentos_router -> yuanrong）动态解析。
                enabled, reason = _is_channel_enabled(ssh_conf, ["listen_port"])
                full_cfg = self._get_config()
                gateway_cfg = full_cfg.get("gateway") if isinstance(full_cfg, dict) else {}
                agent_client_cfg = (
                    gateway_cfg.get("agent_client") if isinstance(gateway_cfg, dict) else {}
                )
                client_type = (
                    str(agent_client_cfg.get("type") or "websocket").strip().lower()
                    if isinstance(agent_client_cfg, dict)
                    else "websocket"
                )
                if not enabled:
                    logger.info("[App] channels.ssh.%s, SshChannel disabled", reason)
                elif client_type != "agentos_router":
                    logger.warning(
                        "[App] channels.ssh.enabled=true but gateway.agent_client.type=%s "
                        "(require agentos_router); SshChannel will not start",
                        client_type,
                    )
                else:
                    # SSH is optional and disabled for the normal Desktop path;
                    # keep its cryptography/runtime imports out of Gateway cold start.
                    from agent_gateway.channel_manager.protocol.ssh.ssh_connect import (
                        SshChannel,
                        SshChannelConfig,
                    )
                    from agent_gateway.extensions.auth.ssh_key_registry import (
                        KeyRegistry,
                    )

                    ssh_config = SshChannelConfig.from_dict({**ssh_conf, "enabled": True})
                    self._ssh_channel = SshChannel(
                        ssh_config,
                        self._dummy_bus_cls(),
                        key_registry=KeyRegistry(),
                    )
                    self._channel_manager.register_channel(self._ssh_channel)
                    self._ssh_task = asyncio.create_task(self._ssh_channel.start(), name="ssh")

                    def _on_ssh_task_done(task: asyncio.Task) -> None:
                        try:
                            task.result()
                        except asyncio.CancelledError:
                            return
                        except Exception as exc:  # noqa: BLE001
                            logger.error(
                                "[App] SSH channel failed to start: %s. "
                                "If SSH is enabled, install optional dependency with "
                                "`uv sync --extra ssh` or `pip install \"agent-gateway[ssh]\"`.",
                                exc,
                            )

                    self._ssh_task.add_done_callback(_on_ssh_task_done)
                    if ssh_config.auth.enabled:
                        self._set_agentos_ssh_key_issuer(
                            self._ssh_channel.key_issuer,
                            ephemeral_key_ttl_sec=ssh_config.auth.ephemeral_key_ttl_sec,
                        )
                    logger.info(
                        "[App] SshChannel registered from config.yaml.channels.ssh "
                        "(listen %s:%s -> MessageHandler; southbound via agent client; auth=%s)",
                        ssh_config.listen_host,
                        ssh_config.listen_port,
                        ssh_config.auth.enabled,
                    )
            else:
                logger.info("[App] channels.ssh missing or invalid, SshChannel disabled")

        if "web_proxy" in changed_channels:
            web_proxy_conf = conf.get("web_proxy") if isinstance(conf, dict) else None
            self._web_channel.web_proxy_enabled = False
            self._web_channel.web_proxy_auth_enabled = True
            self._web_channel.web_resolver = None
            self._web_channel.web_runtime_release = None
            previous_watcher = getattr(self._web_channel, "web_registry_watcher", None)
            if previous_watcher is not None:
                await previous_watcher.stop()
                self._web_channel.web_registry_watcher = None
            previous_manager = getattr(self._web_channel, "web_port_manager", None)
            if previous_manager is not None:
                await previous_manager.close_all()
            if isinstance(web_proxy_conf, dict):
                enabled, reason = _is_channel_enabled(web_proxy_conf, [])
                full_cfg = self._get_config()
                gateway_cfg = full_cfg.get("gateway") if isinstance(full_cfg, dict) else {}
                agent_client_cfg = (
                    gateway_cfg.get("agent_client") if isinstance(gateway_cfg, dict) else {}
                )
                client_type = (
                    str(agent_client_cfg.get("type") or "websocket").strip().lower()
                    if isinstance(agent_client_cfg, dict)
                    else "websocket"
                )
                wp_config = self._web_proxy_config_cls.from_dict(web_proxy_conf)
                if not enabled:
                    logger.info("[App] channels.web_proxy.%s, web proxy disabled", reason or "enabled=false")
                elif client_type != "agentos_router":
                    logger.warning(
                        "[App] channels.web_proxy.enabled=true but gateway.agent_client.type=%s "
                        "(require agentos_router); web proxy will not start",
                        client_type,
                    )
                else:
                    web_resolver = getattr(self._agent_server_ext, "resolve_web_endpoint", None)
                    if not callable(web_resolver):
                        logger.warning(
                            "[App] channels.web_proxy.enabled=true but agent_server_ext has no "
                            "resolve_web_endpoint; web proxy will not start"
                        )
                    else:
                        web_release = getattr(
                            self._agent_server_ext, "release_web_endpoint", None
                        )

                        async def _held_web_resolver(
                            user_id: str,
                            agent_type: str,
                            protocol: str,
                            _resolve=web_resolver,
                        ) -> str | None:
                            return await _resolve(
                                user_id, agent_type, protocol, acquire=True
                            )

                        self._web_channel.web_proxy_enabled = wp_config.enabled
                        self._web_channel.web_proxy_auth_enabled = wp_config.auth_enabled
                        self._web_channel.web_proxy_config = wp_config
                        self._web_channel.web_resolver = _held_web_resolver
                        self._web_channel.web_runtime_release = (
                            web_release if callable(web_release) else None
                        )
                        manager = self._web_port_manager_cls(self._web_channel, wp_config)
                        self._web_channel.web_port_manager = manager
                        manager.start()
                        from agent_gateway.cron.factory import (
                            load_cron_store_settings,
                        )
                        from agent_gateway.extensions.agentos.registry_watcher.service import (
                            RegistryWatcherService,
                        )

                        endpoints = list(load_cron_store_settings(full_cfg).endpoints)
                        watcher = RegistryWatcherService(
                            etcd_endpoints=endpoints,
                            on_desired=manager.reconcile,
                        )
                        self._web_channel.web_registry_watcher = watcher
                        await watcher.start()
                        logger.info(
                            "[App] 3rd-agent web proxy: listeners driven by registry_watcher "
                            "(listen %s, auth=%s, etcd=%s)",
                            wp_config.listen_host,
                            "on" if wp_config.auth_enabled else "off",
                            "on" if watcher.enabled else "off",
                        )
            else:
                logger.info("[App] channels.web_proxy missing or invalid, web proxy disabled")

        self._schedule_agent_prewarm_sync(
            "agent-prewarm-sync-after-channel-change",
            delay_seconds=1.0,
        )
