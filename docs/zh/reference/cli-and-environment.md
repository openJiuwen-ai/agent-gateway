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
| `--enable-cron` | 关闭 | 在 `--mode multiprocess` 中启用受支持的文件模式个人版 Cron 任务；使用账号登录或非默认项目的任务会被拒绝 |
| `--check-cron` | 关闭 | 配合 `--mode multiprocess --enable-cron` 使用：以 JSON 输出只读的 Cron 预检报告并退出 |
| `--southbound-routes-file <path>` | 无 | 配合 `--mode multiprocess` 使用：静态多 AgentServer 试点。该 JSON 文件为每个具名用户固定其回环 AgentServer 及所属 Worker 槽位 |
| `-h, --help` | 无 | 显示帮助并退出 |

### 本地多进程试点

仅用于个人版 IPv4 回环部署。显式使用 `--mode multiprocess --workers 2` 启动两个 Worker；省略 `--mode` 仍走原单进程。北向仅开放试点支持的 Web `/ws`、健康探针和 TUI／ACP 数据操作；南向连接单个 `ws://127.0.0.1:<端口>`。账号管理和可选渠道请继续使用单进程。试点中的 Cron 默认关闭，需显式传入 `--enable-cron`。要接入多个本地 AgentServer 而非一个，请使用下文的静态试点。

### 静态多 AgentServer 试点（仅路由）

```bash
agent-gateway --mode multiprocess --workers 2 --southbound-routes-file routes.json
```

`routes.json` 是一份本地、不含密钥的快照，固定每个具名用户由哪个回环 AgentServer 提供服务。每个 Worker 保留自己的固定槽位，只连接该槽位的目标。连接的具名用户（`user_id` 查询参数，TUI／ACP 上的 `X-User-Id`）决定目标。这里没有默认 AgentServer：用户没有目标的连接，以及与所属连接用户不一致的帧，都会在任何处理器为其保留状态之前以 `1008` 关闭。

文件规则：

- UTF-8 JSON，不超过 16 KiB，必须且只能包含下列全部字段。未知、缺失或重复的字段都会被拒绝。
- `manifest.slots` 必须与 `--workers` 的槽位完全一致：一个 Worker 为 `[0]`，两个为 `[0, 1]`，依此类推。
- `owner_slot` 是该用户的固定亲和规则在此 manifest 下选中的槽位。这与为连接选择 Worker 是同一条规则，因此应计算得出，而不是自行指定：

```python
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


def owner_slot(user_id: str, workers: int) -> int:
    policy = LocalIdentityPolicy()
    affinity = FixedUserHash(WorkerManifest(tuple(range(workers))), policy)
    identity = resolve_local_identity(f"user_id={user_id}".encode(), (), entry="web", policy=policy)
    return affinity.select(identity)
```

- `target_id`、`physical_id`、`user_id`、`ws_url` 在同一文件内各自唯一。`ws_url` 为 `ws://127.0.0.1:<端口>` 或 `ws://[::1]:<端口>`，可带路径，不能含凭据、查询串或片段。`agent_type` 为 `jiuwenswarm`。单个文件最多 32 个目标。

两个 Worker 的示例：

```json
{
  "schema_version": 1,
  "revision": 1,
  "identity_policy": {"contract_id": "local-v1", "domain": "local"},
  "manifest": {"slots": [0, 1], "config_version": 1, "algorithm": "hrw-sha256-v1"},
  "targets": [
    {
      "target_id": "as-a", "physical_id": "as-a", "generation": 1, "user_id": "alice",
      "agent_type": "jiuwenswarm", "owner_slot": 0, "ws_url": "ws://127.0.0.1:8081/agent"
    },
    {
      "target_id": "as-b", "physical_id": "as-b", "generation": 1, "user_id": "bob",
      "agent_type": "jiuwenswarm", "owner_slot": 1, "ws_url": "ws://127.0.0.1:8082/agent"
    }
  ]
}
```

静态 Worker 保证的行为：

- **路由。** 请求只到达其连接用户的目标，不会到达其他任何目标。没有服务发现，没有 Worker 之间的转发，也没有故障转移：目录只能通过以新的 `revision` 重启来变更。
- **会话。** 在一个 Worker 内，一个会话 ID 只属于一个目标，不论它是 AgentServer 自己的 ID，还是 ACP 会话对客户端可见的 ID。指名其他目标会话的帧会在套接字处被拒绝。
- **投递。** 出站帧只到达其归属用户的套接字：依次取请求的用户、帧声明的用户、会话的用户。没有归属用户的帧不予投递。
- **启动。** 某个 Worker 无法连接其任一目标时，整个进程组以 `SOUTHBOUND_UNREACHABLE` 停止，且不开放任何监听。目录或角色是 Worker 无法提供服务的，则以 `SOUTHBOUND_CONFIGURATION` 失败。
- **推送。** AgentServer 的 `send_push` 帧只有在发送它的目标拥有该帧所指的请求 ID 或会话时才会被投递。由其他目标发送的帧，或未指名任何已归属请求或会话的帧，都会被丢弃。

一个 AgentServer 只有一个主动推送槽位，属于它最新的非 RPC WebSocket 连接。AgentServer 是否会把某个推送给到本 Gateway 的连接，取决于该 AgentServer 自身。本试点不承诺未关联任何请求或会话的推送能到达任何用户，无论是同一 Worker 还是其他 Worker 上的用户。

- `--enable-accounts` 将凭据限制在目标 AS 和 Worker 槽位内。`--enable-cron` 按任务归属调度文件任务，并把工具确认回给原 AS；只读预检会拦截没有静态路由的已启用任务。

本版本中静态 Worker 不提供的内容：

- **免账号 Worker 的个人版兼容能力。** 其原生 TUI 请求、个人版 Web 设置与操作，以及心跳代理，都是读取单个 AgentServer 的用户目录来应答的。静态 Worker 的每个具名用户各有一个 AgentServer，因此不安装这些能力，并拒绝相应请求。
- **附件。** 带 `files`、`media_items`、`attachments` 或 `documents` 的 `chat.send` 会被拒绝：每个用户都有私有的 AgentServer 目录，Gateway 无法代用户导入文件。
- **其他智能体与配置写入。** `jiuwenswarm` 以外的 `agent_type`，以及所有 Gateway 配置变更。

### ADX 动态实例试点

在受信的回环部署中使用 `--mode multiprocess --workers 2 --instance-provider-file instances.json`。`InstanceProviderSpec` 文件不含密钥，定义槽位、允许的用户、命名空间、Ingress、注册表及工作目录，并须与现有 ADX 路由配置一致（`gateway.agent_client.type` 为 `adx_router` 或 `agentos_router`）。Ingress 凭据仍来自配置指定的环境变量。试点不可与静态路由、账号服务或 Cron 同时开启。

首个数据请求按用户归属领取或创建实例。写入和主动推送受实例代次约束；在途或结果不确定的请求保留占用，空闲实例按配置阈值回收。回收后不把旧会话迁到新代次。试点不提供生产级租户认证，也未覆盖 Worker 崩溃后发现上代实例。

### Cron 迁移预检

```bash
agent-gateway --mode multiprocess --enable-cron --check-cron --workers 4
```

使用准备上线的 Worker 数：每个用户的任务固定归属其中一个 Worker，归属由该数量决定。命令读取 `<root>/profile/agent/home/cron_jobs.json` 和 Gateway 配置，输出一行 JSON 后退出；不修改任务，也不启动监听、Worker 或调度器。`issues` 按任务 ID 和归属 Worker 列出阻塞项与警告，不包含任务正文或凭据数据。退出码 `0` 表示所有已启用任务均受支持；`1` 表示存在不支持的启用任务，或配置、任务文件无法读取。`agentserver_checked: false` 表示未向 AgentServer 核验模型可用性与项目执行。带 `--enable-cron` 启动时会执行同一检查，存在不支持的启用任务时不开放监听。

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
| `AGENT_GATEWAY_HOME` | 用户主目录 | 用户主目录；实例根目录为其下的 `.jiuwenswarm` |
| `AGENT_GATEWAY_DATA_DIR` | 未设置 | 直接指定实例根目录，优先级高于 `AGENT_GATEWAY_HOME` |
| `AGENT_GATEWAY_RUNTIME_WORKSPACE_READY` | 未设置 | 内部标记；由桌面端等父进程设置，避免重复初始化工作区 |
| `AGENT_GATEWAY_CONFIG_DIR` | 未设置 | Gateway 专用配置目录；文件名固定为 `config.yaml`，未设置时使用 `<config_dir>/config.yaml`，相对路径按配置目录解析；必须在 Gateway 进程启动前设置 |
| `BROWSER_PROFILE_NAME` | `jiuwenclaw` | 浏览器相关功能的 profile 名称 |

### 健康检查 / 心跳

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL` | `config.yaml` 的 `health_check.every`，否则 `60` | 检查间隔（秒） |
| `HEALTH_CHECK_TIMEOUT` | 未设置 | 单次检查超时（秒） |
| `HEALTH_CHECK_RELAY_CHANNEL_ID` / `HEARTBEAT_RELAY_CHANNEL_ID` | `config.yaml` 的 `health_check.target`，否则 `web` | 结果回传频道 |

### 兼容说明

为兼容已有部署，Gateway 仍会读取旧变量名；新部署请使用 `AGENT_GATEWAY_*`。

- 新旧名同时设置且取值不同时：路径类变量直接报错；其他变量以新名为准，并告警一次。空白取值一律视为未设置，会继续回退后面的名字。
- 与 JiuwenSwarm AgentServer 一起部署时，请确保 Gateway 和 AgentServer 使用相同的数据目录、文件下载密钥等共享值。
- 与其他 harness 一起部署时，请用 `AGENT_GATEWAY_DATA_DIR` 显式指定 Gateway 的数据目录。harness 自己使用的 `JIUWENSWARM_*` 变量（例如 agent-core 读取的 `JIUWENSWARM_DATA_DIR`）只设给 harness 进程，不要传给 Gateway。
- Gateway 注入给 AgentServer 的 JWS 专属变量保持原变量名。

## 配置文件

默认路径：`$AGENT_GATEWAY_DATA_DIR/config/config.yaml`；未设置 `AGENT_GATEWAY_DATA_DIR` 时为 `$AGENT_GATEWAY_HOME/.jiuwenswarm/config/config.yaml`。

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

其中 `<root>` = `AGENT_GATEWAY_DATA_DIR`（若设置），否则为 `AGENT_GATEWAY_HOME/.jiuwenswarm`。

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
