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
| `JIUWENSWARM_HOME` | 用户主目录 | 用户主目录；实例根目录为其下的 `.jiuwenswarm` |
| `JIUWENSWARM_DATA_DIR` | 未设置 | 直接指定实例根目录，优先级高于 `JIUWENSWARM_HOME` |
| `JIUWENSWARM_RUNTIME_WORKSPACE_READY` | 未设置 | 内部标记；由桌面端等父进程设置，避免重复初始化工作区 |
| `BROWSER_PROFILE_NAME` | `jiuwenclaw` | 浏览器相关功能的 profile 名称 |

### 健康检查 / 心跳

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL` | `config.yaml` 的 `health_check.every`，否则 `60` | 检查间隔（秒） |
| `HEALTH_CHECK_TIMEOUT` | 未设置 | 单次检查超时（秒） |
| `HEALTH_CHECK_RELAY_CHANNEL_ID` / `HEARTBEAT_RELAY_CHANNEL_ID` | `config.yaml` 的 `health_check.target`，否则 `web` | 结果回传频道 |

## 配置文件

默认路径：`$JIUWENSWARM_DATA_DIR/infra/config/config.yaml`；未设置 `JIUWENSWARM_DATA_DIR` 时为 `$JIUWENSWARM_HOME/.jiuwenswarm/infra/config/config.yaml`。

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
| `<root>/infra/config/config.yaml` | Gateway / 频道配置 |
| `<root>/domain/agent/sessions/` | 会话数据（由 AgentServer 写入） |
| `<root>/infra/logs/agent/` | Gateway、AgentServer 与汇总日志 |
| `<root>/infra/.gateway.lock` | 工作区单例锁 |
| `<root>/domain/agent/home/cron_jobs.json` | 定时任务存储（Gateway 维护） |

其中 `<root>` = `JIUWENSWARM_DATA_DIR`（若设置），否则为 `JIUWENSWARM_HOME/.jiuwenswarm`。

企业版只将 `<root>/domain/` 挂到共享 PVC/NFS，`<root>/infra/` 为各 Pod 的私有目录，
不挂载整个数据根。个人版使用相同路径，两区均为普通子目录。运行日志即使包含业务事件
也属于系统区；例如 `context_usage.jsonl` 写入 `infra/logs/agent/`，企业版通过 stdout
采集留存。

从旧版升级时，Gateway 和 AgentServer 都会在加载配置前检查数据目录。使用同一数据目录的两个进程会等待同一次迁移完成。旧会话和定时任务迁入 `domain/agent/`，旧配置迁入 `infra/config/`，旧组件日志迁入 `infra/logs/agent/`。已有新布局的文件继续使用，同名旧文件有差异时以 `.migrated` 后缀保留。

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
