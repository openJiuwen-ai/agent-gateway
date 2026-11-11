# 配置频道

Gateway 是 IM 平台、Web UI、TUI 与 ACP 客户端的统一接入层。频道按需惰性加载：只有 `config.yaml` 的 `channels` 中出现对应条目时，才会初始化该频道及其依赖。

## 配置文件

默认路径：`$JIUWENSWARM_HOME/.jiuwenswarm/config/config.yaml`。文件不存在时 Gateway 告警并使用默认值（不启用任何 IM 频道）。修改保存后，Gateway 会自动重载发生变化的频道配置。

最小配置——显式关闭所有 IM 频道：

```yaml
channels: {}
```

## 支持的频道

| 区域 | 频道 | 配置键 | 私聊 | 群聊 |
| --- | --- | --- | --- | --- |
| 国内 | 小艺 | `xiaoyi` | 支持 | 不支持 |
| 国内 | 飞书 | `feishu` | 支持 | 支持（@机器人） |
| 国内 | 钉钉 | `dingtalk` | 支持 | 支持（@机器人） |
| 国内 | 企业微信 | `wecom` | 支持 | 支持（@机器人） |
| 国内 | 个人微信 | `wechat` | 支持 | 不支持 |
| 海外 | Telegram | `telegram` | 支持 | 支持（@机器人） |
| 海外 | Discord | `discord` | 支持 | 支持（@机器人） |
| 海外 | Slack | `slack` | 支持 | 支持（频道线程） |
| 海外 | WhatsApp | `whatsapp` | 支持 | 不支持 |

飞书与企业微信额外支持群聊数字分身（`group_digital_avatar: true`）。

## 常用配置示例

下面的占位值需要替换为真实凭据，其余字段可保持默认：

```yaml
channels:
  feishu:
    apps:
      - app_id: "cli_xxx"          # 替换为飞书应用 App ID
        app_secret: "xxx"          # 替换为飞书应用 App Secret
        allow_from: []             # 空表示不限制；填写 open_id 可加白名单
        group_digital_avatar: false

  telegram:
    bot_token: "123456:ABC"        # 替换为 BotFather 提供的 token
    allow_from: []
    group_chat_mode: mention       # mention / always

  dingtalk:
    client_id: "xxx"               # 替换为钉钉应用 Client ID
    client_secret: "xxx"           # 替换为钉钉应用 Client Secret
```

## 各频道配置项

| 频道 | 主要配置项 |
| --- | --- |
| `feishu` | `apps[]`：`app_id`、`app_secret`、`encrypt_key`、`verification_token`、`allow_from`、`enable_streaming`、`group_digital_avatar`、`my_user_id`、`bot_name` |
| `xiaoyi` | `apps[]`：`ak`、`sk`、`agent_id`、`api_id`、`enable_streaming` |
| `dingtalk` | `client_id`、`client_secret`、`allow_from` |
| `telegram` | `bot_token`、`allow_from`、`parse_mode`、`group_chat_mode` |
| `discord` | `bot_token`、`application_id`、`guild_id`、`channel_id`、`allow_from`、`block_dm` |
| `slack` | `bot_token`、`app_token`、`allow_from`、`allowed_channel_ids`、`default_channel_id`、`reply_in_thread` |
| `wecom` | `bot_id`、`secret`、`ws_url`、`allow_from`、`enable_streaming`、`group_digital_avatar` |
| `wechat` | `base_url`、`bot_token`、`ilink_bot_id`、`ilink_user_id`、`allow_from`、`auto_login` |
| `whatsapp` | `bridge_ws_url`、`allow_from`、`default_jid`、`enable_streaming`、`auto_start_bridge` |

## 平台凭据获取

各平台的应用创建、权限申请与凭据获取步骤较长，统一维护在外部平台接入文档中：

- [国内频道](https://gitcode.com/openJiuwen/jiuwenswarm/blob/develop/docs/zh/国内频道.md)
- [海外频道](https://gitcode.com/openJiuwen/jiuwenswarm/blob/develop/docs/zh/海外频道.md)

## 验证

1. 启动 Gateway，日志中应出现 `[ExtensionManager] 加载 ...` 以及对应频道的注册日志。
2. 若日志显示 `channels.<name> missing or invalid`，检查配置键名与必填凭据。
3. 若频道未启动，确认该条目没有显式设置 `enabled: false`，且必填凭据非空。

---

## 返回导航

- [返回文档首页](../../README.md)
- [快速开始](../tutorial/01-quick-start.md)
- [本地功能验证](local-validation.md)

## 员工群生命周期事件

绑定 `agent_id` 的飞书应用应订阅机器人入群、退群、群信息更新及 `im.chat.disbanded_v1` 解散事件并发布。生命周期回调在宿主确认后才返回成功；路由缺失、处理失败或超过 3 秒未确认会向 SDK 报错，供平台重试。未绑定员工的频道忽略这些事件。

飞书同步回调运行在同一 app 共享的 SDK 事件循环上，最多 3 秒的确认等待会推迟该 app 后续帧（包括普通消息）的处理与 ACK；不同 app 的循环独立。

飞书生命周期确认失败日志包含应用 ID、事件类型、事件 ID、异常类型及受控原因：`confirmation_timeout`（确认等待超时）、`callback_unavailable`（宿主回调缺失）、`invalid_event_time`（事件时间无效）、`host_failed`（宿主调用异常）、`host_not_accepted`（宿主未接受）或 `delivery_failed`（其他投递异常）。宿主自身抛出的超时归为 `host_failed`。SDK 侧继续接收通用异常，不输出原始宿主异常正文。

钉钉员工应用通过 Stream EVENT 处理器接收已订阅的 `chat_add_member`、`chat_remove_member`、`chat_quit`、`chat_update_title`、`chat_disband`。机器人消息 CALLBACK 保持独立。事件须在钉钉应用侧配置订阅；仅添加机器人或仅有用户 OAuth 不等于事件已接入。Gateway 校验应用绑定、事件标识与时间，透传至既有内部路由，宿主负责成员核验、登记及撤销授权。成员变更本身不证明机器人已退群；未知成员状态不能当作离群。

钉钉宿主确认最多等待 120 秒：Swarm 的在线成员核验包含授权状态、当前 profile 和群机器人名单三次串行 CLI 调用，每次调用的超时为 30 秒；其余预算留给本地及 E2A 处理。超时取消等待并返回 500，供平台重试。忽略的事件记录 debug 日志，非法载荷及确认超时记录 warning 日志，均携带可用的事件类型、事件 ID 和接收应用 ID，不记录事件正文。

上述超时是 Gateway 的宿主等待预算，不代表平台承诺在此期间不重投。两种渠道均按至少一次投递处理：超时取消不会回滚宿主已经持久化的变更，平台重投可能再次到达宿主。宿主必须按稳定的平台事件 ID 及应用／Agent 作用域幂等处理，不能把取消视为事件从未执行。
