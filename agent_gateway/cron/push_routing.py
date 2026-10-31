from __future__ import annotations

from openjiuwen_protocol.message import EventType, Message

from agent_gateway.cron.dingtalk_routing import (
    is_usable_dingtalk_staff_id,
    resolve_dingtalk_push_metadata,
)


channels_cfg: dict = {}
ch_cfg: dict = {}


def _format_cron_broadcast_text(
    *, job_name: str, text: str, is_placeholder: bool
) -> str:
    # Result body, in-progress placeholders, and [cron] status text are all
    # delivered as-is — no job-name prefix is prepended.
    return str(text or "").strip()


def build_push_message(
    job,
    state,
    *,
    text: str,
    is_placeholder: bool,
    now_fn,
):
    """Build the outbound event for one cron push target."""
    broadcast_text = _format_cron_broadcast_text(
        job_name=job.name,
        text=text,
        is_placeholder=is_placeholder,
    )
    payload_extra = {
        "content": broadcast_text,
        # WebChannel uses this to route AgentOS cron output to the owning
        # browser connection.  Empty preserves legacy single-user fanout.
        "user_id": str(job.user_id or "").strip(),
        "cron": {
            "job_id": job.id,
            "job_name": job.name,
            "run_id": state.run_id,
            "push_at": state.push_at_iso,
            "wake_at": state.wake_at_iso,
            "exec_channel_id": state.exec_channel_id,
            "exec_session_id": state.exec_session_id,
            "is_placeholder": bool(is_placeholder),
            "status": state.status,
        },
    }
    if job.mode == "proactive.tick" and state.manually_triggered:
        # Reuse the frontend's global toast path; retain cron/user metadata
        # for tenant routing and avoid inserting a fake chat message.
        payload_extra["source"] = "proactive_notification"

    channel_id = (job.targets or "").strip()
    if not channel_id:
        return None

    # 企业飞书：优先用作业里绑定的 SessionMap session_id（feishu::chat_id::bot_id::...），
    # 避免多群共用 bot 时误用 config 中的 last_*（最近一条消息的会话）。
    # TUI：不绑定 session_id，否则 TUI 重启后新 session_id 与旧不同，消息会被前端过滤。
    # Web：不绑定 session_id——WebChannel.send 对带 payload.cron 的推送会广播给所有 web 客户端，
    # 绑定旧 session_id 反而会让前端 shouldHandleSessionEvent 因 session 不匹配而丢弃。
    # 关闭 tab/换设备后旧会话再无连接，置空 session_id 让消息进当前活跃会话流。
    metadata: dict | None = None
    msg_session_id: str | None = None
    routing_sid = str(getattr(job, "session_id", None) or "").strip()
    if routing_sid and channel_id != "tui" and channel_id != "web":
        msg_session_id = routing_sid
    if channel_id.startswith("feishu_enterprise:") and routing_sid and "::" in routing_sid:
        parts = routing_sid.split("::")
        if len(parts) >= 3 and parts[0] == "feishu":
            chat_part = str(parts[1] or "").strip()
            if chat_part:
                metadata = {"feishu_chat_id": chat_part}
                if len(parts) >= 6:
                    open_part = str(parts[3] or "").strip()
                    if open_part:
                        metadata["feishu_open_id"] = open_part
                msg_session_id = chat_part

    # 钉钉：优先用作业绑定的发起会话（Issue #2449）。
    # Gateway 内部会话 ID（dingtalk_…）不能当 staffId，此时留空走下方 last_* 兜底。
    if channel_id == "dingtalk" and routing_sid and metadata is None:
        bound = resolve_dingtalk_push_metadata(routing_sid)
        if bound is not None:
            metadata = dict(bound)
            sender = str(bound.get("dingtalk_sender_id") or "").strip()
            if is_usable_dingtalk_staff_id(sender):
                # 保留 msg_session_id 为 job.session_id（可能是内部会话），
                # 仅在 binding 场景用真实 staff 作为发送目标已写入 metadata。
                pass

    # 针对 feishu/xiaoyi/whatsapp/dingtalk：从 config.yaml 取最近一次可回发的平台身份，写入 metadata
    # 这样即使 cron 推送没有 session_id，也能让 Channel.send 正常路由到对应会话。
    if metadata is None:
        try:
            from agent_gateway.common.config import get_config_raw

            cfg = get_config_raw() or {}
            channels_cfg = cfg.get("channels") or {}
            ch_cfg = channels_cfg.get(channel_id) or {}
            if channel_id == "feishu" or channel_id.startswith("feishu:"):
                # V2 多应用：从 apps 列表取对应 app 的 last_*（而非平铺字段）
                target_app_id = str(getattr(job, "app_id", None) or "").strip()
                if not target_app_id:
                    if channel_id.startswith("feishu:") and not channel_id.startswith("feishu_enterprise:"):
                        target_app_id = channel_id.split(":", 1)[1].strip()
                apps = ch_cfg.get("apps") or []
                if isinstance(apps, list):
                    for app in apps:
                        if not isinstance(app, dict):
                            continue
                        if target_app_id and app.get("app_id") != target_app_id:
                            continue
                        if not target_app_id and not app.get("is_default", False):
                            continue
                        last_chat_id = str(app.get("last_chat_id") or "").strip()
                        last_open_id = str(app.get("last_open_id") or "").strip()
                        if last_chat_id or last_open_id:
                            metadata = {
                                "feishu_chat_id": last_chat_id,
                                "feishu_open_id": last_open_id,
                            }
                        break
                # 兜底：如果 apps 列表为空或无匹配，回退到旧平铺字段
                if metadata is None:
                    last_chat_id = str(ch_cfg.get("last_chat_id") or "").strip()
                    last_open_id = str(ch_cfg.get("last_open_id") or "").strip()
                    if last_chat_id or last_open_id:
                        metadata = {
                            "feishu_chat_id": last_chat_id,
                            "feishu_open_id": last_open_id,
                        }
            elif channel_id.startswith("feishu_enterprise:"):
                app_id = channel_id.split(":", 1)[1].strip()
                enterprise_cfg = channels_cfg.get("feishu_enterprise") or {}
                if isinstance(enterprise_cfg, dict) and app_id:
                    for _, bot_cfg in enterprise_cfg.items():
                        if not isinstance(bot_cfg, dict):
                            continue
                        bot_app_id = str(bot_cfg.get("app_id") or "").strip()
                        if bot_app_id != app_id:
                            continue
                        last_chat_id = str(bot_cfg.get("last_chat_id") or "").strip()
                        last_open_id = str(bot_cfg.get("last_open_id") or "").strip()
                        if last_chat_id or last_open_id:
                            metadata = {
                                "feishu_chat_id": last_chat_id,
                                "feishu_open_id": last_open_id,
                            }
                        break
            elif channel_id == "xiaoyi":
                last_session_id = str(ch_cfg.get("last_session_id") or "").strip()
                last_task_id = str(ch_cfg.get("last_task_id") or "").strip()
                if last_session_id or last_task_id:
                    metadata = {
                        "xiaoyi_session_id": last_session_id,
                        "xiaoyi_task_id": last_task_id,
                    }
            elif channel_id == "whatsapp":
                last_jid = str(ch_cfg.get("last_jid") or "").strip()
                if last_jid:
                    metadata = {
                        "whatsapp_jid": last_jid,
                    }
            elif channel_id == "wecom":
                last_chat_id = str(ch_cfg.get("last_chat_id") or "").strip()
                last_user_id = str(ch_cfg.get("last_user_id") or "").strip()
                if last_chat_id or last_user_id:
                    metadata = {
                        "wecom_chat_id": last_chat_id,
                        "wecom_user_id": last_user_id,
                    }
            elif channel_id == "wechat":
                last_user_id = str(ch_cfg.get("last_user_id") or "").strip()
                last_context_token = str(ch_cfg.get("last_context_token") or "").strip()
                if last_user_id:
                    metadata = {
                        "wechat_user_id": last_user_id,
                        "reply_to_user_id": last_user_id,
                    }
                    if last_context_token:
                        metadata["wechat_context_token"] = last_context_token
                        metadata["context_token"] = last_context_token
            elif channel_id == "dingtalk":
                last_sender_id = str(ch_cfg.get("last_sender_id") or "").strip()
                last_conversation_id = str(ch_cfg.get("last_conversation_id") or "").strip()
                last_conversation_type = str(ch_cfg.get("last_conversation_type") or "").strip()
                # 钉钉 send() 依赖 metadata 决定单聊/群聊（conversation_type + conversation_id）。
                # sender_id 作为单聊兜底接收者；群聊以 conversation_id 为主。
                if last_sender_id or last_conversation_id:
                    metadata = {
                        "dingtalk_sender_id": last_sender_id,
                        "dingtalk_chat_id": last_conversation_id,
                        "conversation_id": last_conversation_id,
                        "conversation_type": last_conversation_type or "1",
                    }
        except Exception:
            metadata = None

    if metadata is None:
        metadata = {}
    if channel_id == "dingtalk":
        # 仅用可用的钉钉 staffId / delivery binding 补路由；禁止把 dingtalk_… 内部会话当 staffId。
        if routing_sid and not str(metadata.get("dingtalk_sender_id") or "").strip():
            bound = resolve_dingtalk_push_metadata(routing_sid)
            if bound and is_usable_dingtalk_staff_id(bound.get("dingtalk_sender_id")):
                metadata["dingtalk_sender_id"] = bound["dingtalk_sender_id"]
                if not str(metadata.get("conversation_id") or "").strip():
                    metadata["conversation_id"] = bound.get("conversation_id") or ""
                    metadata["dingtalk_chat_id"] = bound.get("dingtalk_chat_id") or ""
                if not str(metadata.get("conversation_type") or "").strip():
                    metadata["conversation_type"] = bound.get("conversation_type") or "1"
        # 若 metadata 里误塞了内部会话 ID，清掉以免 batchSend 报 staffId.notExisted。
        if not is_usable_dingtalk_staff_id(metadata.get("dingtalk_sender_id")):
            metadata.pop("dingtalk_sender_id", None)
        if not str(metadata.get("conversation_type") or "").strip():
            metadata["conversation_type"] = "1"

    # 获取 group_digital_avatar 和 my_user_id 配置
    _group_digital_avatar = False
    _my_user_id = ""
    if channel_id == "wecom":
        _group_digital_avatar = bool(ch_cfg.get("group_digital_avatar") or False)
        _my_user_id = str(ch_cfg.get("my_user_id") or "").strip()
    elif channel_id == "feishu":
        _group_digital_avatar = bool(ch_cfg.get("group_digital_avatar") or False)
        _my_user_id = str(ch_cfg.get("my_user_id") or "").strip()
    elif channel_id.startswith("feishu_enterprise:"):
        app_id = channel_id.split(":", 1)[1].strip()
        enterprise_cfg = channels_cfg.get("feishu_enterprise") or {}
        if isinstance(enterprise_cfg, dict) and app_id:
            for _, bot_cfg in enterprise_cfg.items():
                if not isinstance(bot_cfg, dict):
                    continue
                bot_app_id = str(bot_cfg.get("app_id") or "").strip()
                if bot_app_id != app_id:
                    continue
                _group_digital_avatar = bool(bot_cfg.get("group_digital_avatar") or False)
                _my_user_id = str(bot_cfg.get("my_user_id") or "").strip()
                break

    if _group_digital_avatar and _my_user_id:
        # 判断定时任务是在群聊还是私聊中创建的
        # 优先使用 job.chat_type（创建时保存的），如果没有则尝试从 session_id 推断
        _is_cron_from_group = job.chat_type == "group"

        # 只有同时满足以下条件才启用 IMOutboundPipeline 路由决策：
        # 1. 开启了 group_digital_avatar
        # 2. 配置了 my_user_id
        # 3. 定时任务是在群聊中创建的（私聊创建的任务直接推送，不走路由决策）
        if _is_cron_from_group:
            # 不在此处硬编码 reply_scope，交由 IMOutboundPipeline 根据内容决定 DM 还是群聊。
            # 只需补充 outbound pipeline 所需的 metadata 前置条件：
            #   - chat_type=group（pipeline 仅对群聊做路由决策）
            #   - reply_candidate_feishu_open_id / reply_candidate_reason（pipeline 需要知道目标用户）
            metadata["chat_type"] = "group"
            if channel_id == "wecom":
                metadata["reply_wecom_user_id"] = _my_user_id
            elif channel_id == "feishu" or channel_id.startswith("feishu_enterprise:"):
                metadata["reply_candidate_feishu_open_id"] = _my_user_id
            metadata["reply_candidate_reason"] = "cron_target_user"
            metadata["reply_target_name"] = _my_user_id
            # 标记为定时任务消息，避免在群聊中重复发送确认消息
            metadata["is_cron_job"] = True

    _msg_app_id = str(getattr(job, "app_id", None) or "").strip() or None
    return Message(
        id=f"cron-push-{state.run_id}-{channel_id}",
        type="event",
        channel_id=channel_id,
        session_id=msg_session_id,
        params={},
        timestamp=now_fn(),
        ok=True,
        payload=payload_extra,
        event_type=EventType.CHAT_FINAL,
        metadata=metadata,
        group_digital_avatar=_group_digital_avatar,
        app_id=_msg_app_id,
    )
