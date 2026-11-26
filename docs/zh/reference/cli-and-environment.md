# 命令行与环境变量参考

本文列出 `agent-gateway` 的命令行参数、环境变量、配置文件与运行时目录。

## 命令行参数

入口：`agent-gateway`（Gateway + 频道）与 `agent-gateway-acp`（ACP 桥接）。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-u, --agent-server-url <URL>` | 由 `AGENT_SERVER_URL` 或 HOST/PORT 拼出 | 下游 AgentServer WebSocket URL，优先级最高 |
| `-H, --host <HOST>` | `WEB_HOST` 或 `127.0.0.1` | WebChannel 绑定地址 |
| `-p, --port <PORT>` | `WEB_PORT` 或 `19000` | WebChannel 绑定端口 |
| `--web-path <PATH>` | `WEB_PATH` 或 `/ws` | WebChannel WebSocket 路径 |
| `--name <name>` | 无 | 启动 `instances.yaml` 中的命名实例 |
| `--dotenv <path>` | 无 | 启动时从指定 `.env` 加载环境变量（仅启动阶段生效） |
| `-h, --help` | 无 | 显示帮助并退出 |

## 环境变量

### 下游 AgentServer

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `AGENT_SERVER_URL` | 未设置 | 下游 WebSocket URL，优先级最高 |
| `AGENT_SERVER_HOST` | `127.0.0.1` | 未设置 URL 时用于拼接 `ws://host:port` |
| `AGENT_SERVER_PORT` / `AGENT_PORT` | `18092` | 下游端口 |
| `AGENT_CONNECT_RETRY` | `20` | 下游连接重试次数；目标是回环地址（`127.0.0.1` / `localhost` / `::1`）时强制提升到至少 `240`（约 60s） |
| `AGENT_CONNECT_RETRY_INTERVAL` | `3` | 重试间隔上限（秒）；回环地址按 0.05s 起指数退避，上限 0.25s |

### 上游入口

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEB_HOST` | `127.0.0.1` | WebChannel 绑定地址 |
| `WEB_PORT` | `19000` | WebChannel 端口 |
| `WEB_PATH` | `/ws` | WebChannel WebSocket 路径 |
| `WEB_WS_PERMESSAGE_DEFLATE` | 关闭 | 网关 WebSocket 是否启用 permessage-deflate 帧压缩。压缩需对每帧做 zlib，内网 / 单机部署下 CPU 开销大于带宽收益，因此默认关闭；公网弱网部署可设为 `1` 开启 |
| `GATEWAY_HOST` | `127.0.0.1` | TUI 路由绑定地址 |
| `GATEWAY_PORT` | `19001` | TUI 路由端口；固定默认值，同机并行多实例时需显式分配 |
| `GATEWAY_URL` | 未设置 | 部分扩展使用的 Gateway 对外地址 |
| `AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS` | 开启 | Web（`/ws`）与 ACP（`/acp`）入口是否把网关未登记的方法按原方法名透传给 AgentServer。方法集合归 AgentServer，网关只拦截保留前缀与企业版禁写方法。设为 `0` / `false` / `no` / `off` 恢复“仅静态白名单转发”的严格模式，用于灰度与回滚；`/tui` 不受此开关影响 |
| `A2A_SERVER_ENABLED` | 未启用 | 设为 `1` / `true` 时启用 A2A 入口 |
| `A2A_SERVER_HOST` | `127.0.0.1` | A2A 监听地址 |
| `A2A_SERVER_PORT` | `19100` | A2A 监听端口 |
| `A2A_SERVER_PATH` | `/a2a` | A2A RPC 路径 |
| `A2A_SERVER_CARD_PATH` | `/.well-known/agent-card.json` | Agent Card 路径 |
| `A2A_SERVER_EXTENDED_CARD_PATH` | `/agent/authenticatedExtendedCard` | 扩展 Agent Card 路径 |
| `A2A_SERVER_PROTOCOL_VERSION` | `1.0.0` | A2A 协议版本 |

### 工作区与运行时

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `AGENT_GATEWAY_HOME` | 用户主目录 | 用户主目录；实例根目录为其下的 `.jiuwenswarm`。旧名 `JIUWENSWARM_HOME` |
| `AGENT_GATEWAY_DATA_DIR` | 未设置 | 直接指定实例根目录，优先级高于 `AGENT_GATEWAY_HOME`。旧名 `JIUWENSWARM_DATA_DIR` |
| `JIUWENSWARM_RUNTIME_WORKSPACE_READY` | 未设置 | 内部标记；由桌面端等父进程设置，避免重复初始化工作区 |
| `AGENT_GATEWAY_CONFIG_DIR` | 未设置 | Gateway 专用配置目录；文件名固定为 `config.yaml`，未设置时使用 `<config_dir>/config.yaml`，相对路径按配置目录解析；必须在 Gateway 进程启动前设置。旧名 `JIUWENSWARM_GATEWAY_CONFIG_DIR` |
| `BROWSER_PROFILE_NAME` | `jiuwenclaw` | 浏览器相关功能的 profile 名称 |

### 健康检查 / 心跳

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL` | `config.yaml` 的 `health_check.every`，否则 `60` | 检查间隔（秒） |
| `HEALTH_CHECK_TIMEOUT` | 未设置 | 单次检查超时（秒） |
| `HEALTH_CHECK_RELAY_CHANNEL_ID` / `HEARTBEAT_RELAY_CHANNEL_ID` | `config.yaml` 的 `health_check.target`，否则 `web` | 结果回传频道 |

### 旧名兼容

Gateway 读取环境变量时先读 `AGENT_GATEWAY_*` 新名，未设置时回退到旧名，已有部署无需修改。

| 新名 | 旧名 |
| --- | --- |
| `AGENT_GATEWAY_HOME` | `JIUWENSWARM_HOME` |
| `AGENT_GATEWAY_DATA_DIR` | `JIUWENSWARM_DATA_DIR` |
| `AGENT_GATEWAY_CONFIG_DIR` | `JIUWENSWARM_GATEWAY_CONFIG_DIR` |
| `AGENT_GATEWAY_DOWNLOAD_ASSET_ROOT` | `JIUWENSWARM_DOWNLOAD_ASSET_ROOT` |
| `AGENT_GATEWAY_EDITION` | `JIUWENSWARM_EDITION` |
| `AGENT_GATEWAY_SSL_VERIFY` | `JIUWENSWARM_SSL_VERIFY` |
| `AGENT_GATEWAY_ENABLE_ORIGIN_CHECK` | `JIUWENSWARM_ENABLE_ORIGIN_CHECK` |
| `AGENT_GATEWAY_WS_ALLOWED_ORIGIN_HOSTS` | `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` |
| `AGENT_GATEWAY_LOG_MASKING_PROBE` | `JIUWENCLAW_LOG_MASKING_PROBE`、`JIUWENSWARM_LOG_MASKING_PROBE` |
| `AGENT_GATEWAY_FILE_DOWNLOAD_SECRET` | `JIUWENSWARM_FILE_DOWNLOAD_SECRET` |
| `AGENT_GATEWAY_REQUEST_EXT_FORWARD_HEADERS` | `JIUWENSWARM_REQUEST_EXT_FORWARD_HEADERS`、`JIUWENCLAW_REQUEST_EXT_FORWARD_HEADERS` |
| `AGENT_GATEWAY_SUPERVISOR_PID` | `JIUWENSWARM_SUPERVISOR_PID` |

- 新旧名同时设置且取值不同时：路径类变量（`AGENT_GATEWAY_HOME`、`AGENT_GATEWAY_DATA_DIR`、`AGENT_GATEWAY_CONFIG_DIR`、`AGENT_GATEWAY_DOWNLOAD_ASSET_ROOT`）直接报错；其他变量以新名为准，并在日志中告警一次。空白取值一律视为未设置，会继续回退后面的名字，也不参与冲突比较。
- 与 JiuwenSwarm 的 AgentServer、启动器一起部署时，请继续使用旧名，或把新旧名设为同一个值。它们目前只认旧名，只设新名会导致两边使用不同的数据目录或密钥。
- Gateway 写给 AgentServer 的变量（如 `JIUWENSWARM_RUNTIME_PROFILE`、`JIUWENSWARM_AGENTOS_SANDBOX_CONFIG`）仍使用旧名。

## 配置文件

默认路径：`$AGENT_GATEWAY_DATA_DIR/config/config.yaml`；未设置 `AGENT_GATEWAY_DATA_DIR` 时为 `$AGENT_GATEWAY_HOME/.jiuwenswarm/config/config.yaml`。旧名同样生效，见上文“旧名兼容”。

Gateway 可用 `AGENT_GATEWAY_CONFIG_DIR` 指定独立配置目录；文件名固定为 `config.yaml`，首次缺失时从内置 `config.yaml` 模板初始化，已存在文件不会被覆盖。`logging.yaml` 仍按配置目录读取，不跟随该变量。

该变量必须在 Gateway 进程启动前注入（systemd/drop-in、启动命令或早期 `--dotenv`）。`agent_gateway.common.config` 会在导入时绑定配置路径，不支持在进程运行中动态修改该变量来切换配置文件；请勿在 import 之后再修改，否则可能出现读写路径不一致。

文件缺失时，配置读取回退到 Gateway 自带模板；独立启动会安装该模板，已有配置不会被覆盖。常用配置项：

```yaml
channels: {}          # 仅初始化此处出现的 IM 频道
health_check:
  every: 60           # 健康检查间隔（秒）
  target: web         # 结果回传频道
```

## 运行时目录

| 路径 | 内容 |
| --- | --- |
| `<root>/config/config.yaml` | Gateway / 频道配置 |
| `<root>/profile/agent/sessions/` | 会话数据（由 AgentServer 写入） |
| `<root>/logs/agent/` | Gateway、AgentServer 与汇总日志 |
| `<root>/.gateway.lock` | 工作区单例锁 |
| `<root>/profile/agent/home/cron_jobs.json` | 定时任务存储（Gateway 维护） |

其中 `<root>` = `AGENT_GATEWAY_DATA_DIR`（若设置），否则为 `AGENT_GATEWAY_HOME/.jiuwenswarm`；旧名同样生效。

企业版只将 `<root>/profile/` 挂到共享 PVC/NFS；`config/`、`logs/`、`auth/`、`runtime/`
等系统路径仍位于平铺数据根，并由各 Pod 私有持有，不挂载整个数据根。个人版使用相同
路径。运行日志即使包含业务事件也属于系统运行态；例如 `context_usage.jsonl` 写入
`logs/agent/`，企业版通过 stdout 采集留存。

从早期平铺版本升级时，已知的持久业务目录会迁入 `profile/`；在平台支持时，旧路径会
保留为软链接。系统目录留在原位。未发布的 `domain/infra` 中间布局不属于支持的升级路径。

## 退出行为

| 场景 | 行为 |
| --- | --- |
| 工作区锁已被占用 | 输出 `[App] Another Gateway is already serving this workspace`，以非零状态退出 |
| `--name` 指定的实例不存在或工作区缺失 | 启动前报错并以非零状态退出 |
| `Ctrl+C` | 关闭 WebChannel、频道与后台任务后退出 |

---

## 返回导航

- [返回文档首页](../../README.md)
- [快速开始](../tutorial/01-quick-start.md)
- [单机多实例](../how-to/run-multiple-instances.md)
