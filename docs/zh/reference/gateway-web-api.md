# Gateway 接口文档（Web 前端 ⇄ Gateway）

> **目标**：完整描述 JiuwenSwarm Gateway 对 Web 前端暴露的全部接口——WebSocket JSON-RPC 方法、HTTP 路由与推送事件，覆盖参数校验、错误码、权限、流式契约（出错处理 / 取消 / 完整性）与内部输入输出关联。预期读者为前端开发、SDK 开发与网关维护者。
>
> **基准版本**：本仓 `upstream/develop` @ `0d50e02b0`（2026-11-09），行为已按该版本核对修正。**路径前缀映射**：`gateway/*` → `agent_gateway/*`、`common/*` → `agent_gateway/common/*`、`common/schema/message.py` 中的 `ReqMethod` → `jiuwenswarm/common/schema/message.py`（Swarm 侧解析），`EventType/Mode/Message` → 协议包 `e2a/`；线上 method 始终为字符串。`server/*`、`agents/*`、`channels/web/frontend/*` 引用属 AgentServer / 前端仓，本仓不可核。**未随本次修订整体刷新的行号仍为旧基线 jiuwenswarm `3548d192d` 行号**，仅作定位线索，以代码为最终实现源。

---

## 目录与覆盖范围

**本文档覆盖**：Web 前端（浏览器 ⇄ `/ws`、`/ws/git` 与同端口 HTTP）可触达的主要接口——489 个 WS 方法（其中 7 个企业版专用）、13 组 HTTP 路由、50 个推送事件类型；含参数校验、错误码、权限与流式契约。

**明确不在范围内**（避免误判为遗漏）：

- TUI 专属方法（`session.rewind*`、`session.color_set`、`history.list_turns` 等，注册于 `tui/tui_connect.py`）与 ACP 通道方法（`acp.tool_response` 等，均为透传）；
- Gateway ⇄ AgentServer 的 E2A 内部方法（`file.upload_chunk`、`voice.task.*`、`cron.jobs.sync` 等，见 `docs/zh/reference/e2a-protocol.md`）；
- §10.6 纯转发域（skills/mcp/agents/装备包/plugins/personal_context/a4p/rsi/harness/issue 等 150+ 个方法）已给出逐域逐方法契约（参数/响应/错误码/特殊机制）；`harness.import/export` 为已知未接线方法（§10.6.16 标注）。

**章节导览**：

1. 总体架构（进程拓扑 / 三层分发 / 归一化规则）
2. 传输协议与帧格式（握手 / 帧 / 通用校验 / 限制 / 背压 / 配置开关）
3. 通用契约（错误码 / E2A 映射 / 事件投影与路由 / 鉴权 / 连接生命周期）
4. 单 Agent 对话接口（chat.\* / command.\* / history.get / **流式契约详解**）
5. Agent 集群（Team）模式（mode / team.\* / group.\* / org / swarmflow / team 事件流）
6. Session 会话接口（含生命周期与事件）
7. Project 项目接口（含 project.git.\* 与 /ws/git）
8. 定时任务接口（cron.job.\* / heartbeat.job.\* / schedule.\*）
9. HTTP 接口（auth / file-api / trajectory）
10. 其它 WS 方法族（配置 / 媒体 / 渠道 / 更新 / 权限 / Designer / **纯转发域逐方法契约**）
- 附录 A 方法分发总表 · 附录 B 事件总表 · 附录 C 错误码总表 · 附录 D 限制常量 · **附录 E 端到端样例（新手从这里入手）** · 附录 F 方法级网关处理标注总表（透传 vs 网关处理 + 自动化透传验证）

---

## 1. 总体架构

### 1.1 进程与端口

| 服务 | 进程入口 | 默认监听 | 说明 |
|------|---------|---------|------|
| WebChannel（Gateway 主入口） | `gateway/app_gateway.py` | `127.0.0.1:19000`，WS 路径 `/ws`（`WebChannelConfig`，`gateway/channel_manager/web/web_connect.py:171-178`） | 前端唯一对接面：WS JSON-RPC + 同端口 HTTP + `/ws/git` |
| GatewayServer（辅助路由） | `gateway/app_gateway.py:2398-2403` | `GATEWAY_HOST:GATEWAY_PORT`，默认 `127.0.0.1:19001` | 承载 `/tui`、`/acp` 等辅助 WS 路由，前端通常不直接使用 |
| AgentServer | `server/app_agentserver.py` | Gateway 内部连接 | 业务执行进程；Gateway 经 E2A 协议（`docs/zh/reference/e2a-protocol.md`）与之通信 |

Gateway 与 AgentServer 为双进程拆分布局（架构说明见 `docs/zh/explanation/architecture.md`）。**所有前端请求要么在 Gateway 本地处理，要么经 E2A 转发到 AgentServer**，前端无感知。

### 1.2 接口面分类

| 接口面 | 端点 | 用途 |
|--------|------|------|
| WS JSON-RPC | `ws://<host>:19000/ws` | 绝大多数业务方法（§4-§8、§10） |
| WS Git 订阅 | `ws://<host>:19000/ws/git` | diff 监控订阅与撤销/重做（§7.3） |
| HTTP 认证 | `/api/v1/auth/*` | 华为账号登录/登出/模型目录（§9.1） |
| HTTP 容器文件 | `/file-api/*` | 文件列表/读写/上传下载（仅 AgentOS 多用户部署挂载，§9.2） |
| HTTP 调试轨迹 | `/api/trajectory/*` | 会话观测轨迹查询（§9.3） |
| HTTP MCP OAuth 回调 | `/mcp/oauth/callback` | MCP OAuth 流程落地页 |
| HTTP 诊断代理 | `/api/trajectory/sessions/{sid}/diagnoses/*` | 诊断任务代理到 AgentServer |
| HTTP MCP App 沙箱 | `/api/v1/mcp-app/sandbox`、`/mcp-app-sandbox` | App iframe 沙箱页 |
| HTTP 个人分享 | `/share-api/jobs*` | 会话快照分享（仅个人版） |
| HTTP REST/SSE | `/api/v1/*` | chat completions（SSE）、sessions CRUD、settings/workspace/team mapped 表（仅企业版挂载，与 WS 方法共用统一入站管道） |

### 1.3 请求分发三层路由

一条 `req` 帧进入 `_handle_raw_message`（`web_connect.py:2144-2422`）后按以下顺序分发：

1. **帧级校验**（§2.3）：失败立即回 `res` 错误帧。
2. **本地 handler 优先**：`chat.send` 在 `_HANDLER_BEFORE_CALLBACK_METHODS`（`web_connect.py:104`）中——先执行本地 handler 回 ack，然后**继续**走转发；其余注册了本地 handler 的方法（`session.list`、`cron.job.*` 等，注册表见 `app_web_handlers.py:5628-6706`）在回调未接管时执行。
3. **入站回调（白名单转发）**：`_FORWARD_REQ_METHODS`（`app_web_handlers.py:459-696`）内的方法经 `_norm_and_forward`（`app_gateway.py:2522-2548`）归一化后交 MessageHandler → E2A → AgentServer。其中 `_FORWARD_NO_LOCAL_HANDLER_METHODS`（`app_web_handlers.py:709-924`，如 `team.*`、`skills.*`、`session.switch`）转发后即结束（响应由 AgentServer 异步回写）；其余（如 `chat.interrupt`）转发后本地 handler 仍会补发 ack。
4. **未注册方法**：回 `METHOD_NOT_FOUND`（`web_connect.py:2419-2422`）。

> 2026-11 起 HTTP 入口（企业 `/api/v1` 与 mapped 路由）与 WS 共用统一入站管道 `channel_manager/web/invoke.py`（`dispatch_web_request`）。企业版在该管道前置写保护 `is_enterprise_write_forbidden`（config/models/agents/extensions/plugins/mcp.server/a2a 前缀、`channel.*.set_conf|unbind`、skills 白名单外 → `FORBIDDEN "企业版配置由管理面统一下发"`）；**该写保护当前仅接入 HTTP 入口，WS 主路径未接入**。企业版另将 team 控制面 7 方法从转发白名单剔除（见 §5.2）。

### 1.4 归一化规则（网关改写请求）

`_normalize_gateway_message`（`app_gateway.py:289-323`）在转发前执行：

- `params.content` 复制为 `params.query`（无 `query` 时）；
- **`chat.resume` 被改写为 `chat.interrupt` + `params.intent="resume"`**（`app_gateway.py:295-297`）；
- `chat.send` / `history.get` **无条件视为流式**（`is_stream=True`，`app_gateway.py:300-303`），客户端顶层 `is_stream` 字段仅对 `command.goal` 等方法有意义；
- 登录模型凭据挂载：请求模型属于登录模型目录时，服务端填入 `_model_auth`（无条件先删除客户端伪造的同名键，`common/auth/passthrough.py:27-52`）；凭据过期时先同步续期再转发（上限 20s，`app_gateway.py:262-286`）；
- `session.create` 注入 work_mode 归一化并携带 `_work_mode_explicit` 传输标记（`app_gateway.py:341-384`）。

---

## 2. 传输协议与帧格式

### 2.1 WebSocket 握手

**URL**：`ws://<host>:<port>/ws[?user_id=<username>&session_id=<sid>&mode=<mode>&agent_id=<aid>&app_id=<app>]`

| 检查项 | 条件与默认 | 失败结果 |
|--------|-----------|---------|
| Origin 校验 | 仅当环境变量 `JIUWENSWARM_ENABLE_ORIGIN_CHECK=1` 开启（默认关闭）；白名单为 `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS`（逗号分隔 hostname，含 `none` 可放行无 Origin 客户端）（`common/security/ws_origin.py:12-41`） | HTTP 403 `Forbidden: Origin not allowed`（`web_channel_app.py:153-186`） |
| 握手鉴权 | 由 `AgentOSRouterClient.set_handshake_auth` 注册（`extensions/agentos/agentos_router/router_client.py:548-558`），**仅 AgentOS 多用户部署启用**；token 取 URL `?token=` → `Authorization: Bearer` → `X-Token`（`extensions/agentos/auth/common.py:33-60`）；普通单用户部署不注册钩子，握手放行 | HTTP 401 `Unauthorized`（`web_channel_app.py:195-222`） |
| query `session_id` 合法性 | 若携带必须通过 `validate_session_id`（§2.3） | WS close 1008（`web_connect.py:1372-1386`） |
| path 不匹配（非 `/ws`、`/ws/git`） | — | WS close 1008 `unsupported path`（`web_connect.py:1354-1360`） |

**身份字段**（握手一次读取，整条连接复用）：

- `user_id`：query `?user_id=`（用户名）优先，其次 `X-User-Id` 头（`web_connect.py:433-458`）。该值决定 RoutingKey 的 user_id 维度与多用户隔离（§3.5）；未声明时回退 `remote`（`ip:port`）。
- **登录会话**：`X-Auth-Session` 头优先，其次 cookie `jiuwenswarm_auth`，再次 query `?auth_session=`（鸿蒙 WebView 场景）（`web_connect.py:70-102`），仅握手读取一次。**登录态变化后前端必须重连**（前端 `webClient.reconnect`，`channels/web/frontend/src/services/webClient.ts:213-225`）。
- IAM 认证身份（UUID）只作认证门禁，不参与工作区身份推导（用户目录按 username 布局，`web_connect.py:450-458` 注释）。

**握手成功后**：Gateway 以 query 中的 `app_id`（默认 `default`）/`mode`（默认 `agent`）/`agent_id`（默认 `default`）/`session_id`（缺省合成 `sess_<hex时间戳>_<6hex>`，`web_connect.py:1931-1936`）构造 5 维 `RoutingKey(user_id, channel_id, app_id, AgentRef(mode,id), session_id)` 并注册连接（`web_connect.py:1368-1404`）。AgentServer 就绪时前端立即收到 `connection.ack` 事件（§3.6）。

### 2.2 帧格式

三种帧（TS 定义 `channels/web/frontend/src/types/websocket.ts:9-34`）：

```jsonc
// 请求（前端 → Gateway）
{ "type": "req", "id": "<string>", "method": "<string>", "params": { }, "is_stream": false }

// 响应（Gateway → 前端，与请求 id 一一对应）
{ "type": "res", "id": "<string>", "ok": true, "payload": { } }
{ "type": "res", "id": "<string>", "ok": false, "error": "<人读信息>", "code": "<错误码>" }

// 推送事件（Gateway → 前端）
{ "type": "event", "event": "<事件名>", "payload": { }, "seq": 0, "stream_id": null }
```

- `id`、`method` 必须为字符串，否则 `BAD_REQUEST "invalid request"`；JSON 解析失败回 `BAD_REQUEST "invalid json"`（`web_connect.py:1614-1636`）。
- `params` 非对象时**静默兜底为 `{}`**（`web_connect.py:1637-1638`）——不报错，由各方法的必填校验兜住。
- `ok:false` 时 `error` 缺省 `"request failed"`；`code` 仅失败帧携带（`web_connect.py:367-401`）。
- `seq` / `stream_id` 为**可选保留字段，当前主事件路径不赋值**（§4.9.3），前端不得依赖其排序/去重。
- 顶层 `is_stream` **仅对部分方法生效**：
  - `chat.send` / `history.get`：网关无条件强制流式（§1.4），传不传等效；
  - 白名单转发的其它方法（如 `command.goal` 的 set/resume）：原样透传 AgentServer，并决定其走流式（逐 chunk 事件，正常路径无 res）还是一元（单条 res）分支（`server/agent_ws_server.py:2899`；前端对 goal set/resume 以 fire-and-forget + `is_stream:true` 发送，`webClient.ts:332-365`）；
  - E2A 代理方法（`session.list`、`cron.job.*`、`media.*` 等经 `proxy_unary_request` 的）：代理层恒按一元（`e2a_proxy.py:165` 硬编码 `is_stream=False`），该字段被忽略。

> 💡 **约定**：后文各方法章节的「请求参数」表，均指帧内 `params` 对象的字段；外层信封（`type` / `id` / `method`，可选 `is_stream`）对所有方法一致，不再逐节重复。**响应格式**同样逐方法列出：一元方法见各条目的「同步 res」/「响应 payload」（即 `res` 帧 `payload` 字段的结构）；流式方法（chat 族）的 res 仅为受理 ack，真实输出见 §4.9 事件序列与附录 B 事件字段；完整帧样例见**附录 E**。

### 2.3 通用参数校验

在方法级校验之前，帧级统一执行（`web_connect.py:1623-1689`）：

| 校验项 | 规则 | 失败返回 |
|--------|------|---------|
| `params.mode`、`params.agent_id` | 必须是字符串（Issue #4876：非字符串曾在路由层 hash 崩溃且无响应） | `BAD_REQUEST "params.mode must be a string"` |
| `params.session_id`（显式携带时） | `validate_session_id`（`common/protocol_ids.py:27-43`）：字符串、≤80 字符、正则 `^[A-Za-z0-9_](?:[A-Za-z0-9_.-]{0,78}[A-Za-z0-9_])?$`（首尾限字母数字下划线，中间允许 `.` `-`）、无首尾空白 | `BAD_REQUEST`（error 为具体原因，异常消息不含原始值，防日志注入） |
| swarmflow 控制方法（pause/resume/stop）的 `run_id`/`workflow_run_id` | 两者同传必须相等；`validate_workflow_run_id`：字符串、非空、≤256 字符、无控制字符（`common/protocol_ids.py:55-69`） | `BAD_REQUEST` |
| `chat.send` / `chat.resume` 的 `params.group_id`（显式携带时） | 须通过 `validate_session_id` 且与显式 `session_id` 相等（`web_connect.py:2247-2252`；旧基线该校验在 AgentServer 侧） | `BAD_REQUEST` |
| 未注册 method | — | `METHOD_NOT_FOUND "unknown method: <method>"` |

**空值兜底约定**（跨方法通用）：

- 未显式携带 `session_id` 的请求（如 `memory.compute`、`config.get` 等 keepalive/拉取类）合成临时 id 构造 Message，但**不更新 ws 路由注册**，连接保留原 RoutingKey（`web_connect.py:1655-1723` 注释）。
- 大多数可选参数缺省由**服务端默认值**兜底而非报错（如 `mode` 未知串兜底 `agent.work.normal`，`Mode.from_raw`，`common/schema/message.py:609-643`）；必填参数缺失才回 `BAD_REQUEST`。
- 布尔参数普遍要求严格布尔类型（如 `session.pin` 的 `pinned`、heartbeat 的 `enabled`、`project.git.commit` 的 `stage_all`），传字符串/数字会被拒。
- 分页参数普遍"宽松解析 + 越界钳制"：如 `session.list` limit 默认 20、1–200 之外钳制；`team.history.get` limit 默认 500、1–1000；`cron.job.preview` count 默认 5、夹取 1–50。

**"缺省不携带"型参数的五种缺省机制**（不传该键时请求里就没有它，网关不注入字面默认值；缺省行为按机制分五类）：

| 机制 | 缺省后发生什么 | 典型参数 |
|------|---------------|---------|
| 场景路由 | 该分支不触发，走默认分支 | `chat.send.source`（普通消息，会取消在途流）、`input_mode`、`user_answer` 审批三件套（team plan 结构校验条件是键存在性，缺省即跳过）、`agent_group_name`（首次构建不指定专家团）、`target_team_id`、`skills` |
| 会话状态沿用 | 服务端按会话既有状态/自身解析兜底 | `chat.interrupt` 定位参数（四级定位链 §4.2）、`chat.send` 的 `work_mode/project_dir/project_id`（session 已存在时忽略）、`workspace_dir`（回落 cwd/全局默认）、`team.snapshot.team_name`（取活跃 team） |
| 集合/过滤 | 空集或不过滤 | `files`（无附件）、`trusted_dirs`（空列表）、`cron.job.list.project_id`（全部可见）、`project.*.work_mode`（不过滤） |
| 语义切换 | 缺省本身是一种显式功能 | `session.rename.title`（缺省=查询）、`/file-api` 的 `offset/limit`（缺省=二进制全量流）、`session_id`（不绑定会话校验） |
| 信号上报 | 不上报，仅影响体验/日志 | `session.switch` 的 `previous_session_id/previous_mode/view_id` |

> ⚠️ 两个有实际风险的缺省：`group.member.add.client_request_id`（失去幂等收据，重试仅靠 `ALREADY_EXISTS` 拦截）；AgentServer 重启/重连后 `chat.interrupt` 不带 `mode`（会话归属登记丢失时定位降级到默认 agent，§4.2）。

**类型校验强度分层**（字段类型不符时不一定被拒，按强度分三档）：

| 强度 | 行为 | 典型参数 |
|------|------|---------|
| 严格拒绝 | 回 `BAD_REQUEST`（或专用错误码） | 帧级 `mode`/`agent_id` 非 string；`session_id`/`run_id` 类型与格式；**严格布尔**参数（`session.pin.pinned`、`project.git.*` 布尔旗标、`heartbeat` 的 `enabled`、`session.create` 的 `moa_enabled`/`persist_session`）；`model_selection`（pydantic `extra="forbid"`）；team plan 审批的 `plan_language` ∉{cn,en} |
| 宽松容错 | 不报错，静默兜底/丢弃/钳制 | `params` 非对象→置 `{}`；列表非 dict 项静默跳过（`attachments`/`answers`）；`trusted_dirs` 非字符串项丢弃；分页越界钳制（⚠️ 旧基线的 token_budget 静默丢弃行为在结构化端点已改为原样透传，见 §4.6）；布尔强转参数（`enable_swarmflow`/`overwrite_confirmed`/`team`——传 `"yes"`/`1` 会被当真值）；cron `id` 经 `str()` 强转 |
| 类型严、值宽 | 类型拒绝但值兜底 | `mode`：非 string 帧级拒绝，未知串兜底 `agent.work.normal`（仅 `team` 前缀未知值拒）；`history.get` 的 `cursor` 类型非法走事件流错误 `INVALID_HISTORY_CURSOR` 而非 res |

> ⚠️ **客户端实践**：容错行为是防御性兜底而非契约（静默丢弃比报错更难排查），请严格按本文档参数表的类型传值；各参数的校验强度已逐行标注在"默认/校验"列（"严格布尔""容忍数字字符串""越界钳制"等）。

### 2.4 大小限制与超时

| 限制 | 值 | 位置 |
|------|----|------|
| 前端 → Gateway 单 WS 帧 | **150 MB**（`WEB_WS_MAX_MESSAGE_BYTES`，uvicorn `ws_max_size`；`document.persist` 可携带大 base64） | `common/ws_limits.py:11`、`web_connect.py:697` |
| Gateway ↔ AgentServer 单 E2A 帧 | 8 MB（`AGENT_WS_MAX_MESSAGE_BYTES`）；E2A 单请求载荷超 **4 MB**（`E2A_PAYLOAD_MAX_BYTES`）改走 HTTP bridge 分块 | `common/ws_limits.py:6`、`gateway/routing/agent_http_bridge.py:31` |
| E2A 大文件分块 | 2 MB/块（`FILE_UPLOAD_CHUNK`） | `agent_http_bridge.py:35` |
| WS ping/pong | 间隔 20s、超时 60s | `web_connect.py:683,691-705` |
| Gateway → AgentServer unary 超时 | **Web 渠道无网关侧上限**（走 AgentClient 默认）；仅 TUI 渠道设 25s 默认 / 55s 上限 | `gateway/routing/agent_request_timeout.py:72-108` |
| 前端默认请求超时 | 15s（`DEFAULT_TIMEOUT_MS`，可按请求覆盖） | `channels/web/frontend/src/services/webClient.ts:29` |

### 2.5 背压与连接保护

- 每连接独立 writer 协程 + **512 帧有界队列**，满队列丢帧并告警（`gateway/routing/base_ws_channel.py:29,264-267`）；单帧发送超时 10s 丢该帧（`:305-315`）。
- **写停滞看门狗**：客户端停止读取导致写缓冲越过高水位持续 60s，或 EOF 后 60s 未排空，强制 `transport.abort()` 回收连接（`transport_stall_guard.py:33-103`，修复 CLOSE-WAIT fd 泄漏）。
- 服务器停机时对所有连接 `close(1001, "server shutdown")`（`web_connect.py:722-724`）。

### 2.6 影响接口行为的配置开关

以下环境变量 / 配置项会**改变接口的可用性与行为**，联调与部署时需知晓（大小限制类常量见附录 D）：

| 开关 | 默认 | 影响的接口行为 |
|------|------|---------------|
| `JIUWENSWARM_ENABLE_ORIGIN_CHECK` | 关闭（仅 `=1` 开启） | WS 握手 Origin 校验（§2.1）：开启后 Origin 不在白名单 → 403 |
| `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` | 空 | Origin 白名单（逗号分隔 hostname，含 `none` 可放行无 Origin 客户端） |
| `GATEWAY_HOST` / `GATEWAY_PORT` | `127.0.0.1` / `19001` | GatewayServer（`/tui`、`/acp` 辅助路由）监听地址 |
| `AGENT_CONNECT_RETRY` / `AGENT_CONNECT_RETRY_INTERVAL` | 20 / 3s | 启动时连接 AgentServer 的重试次数与间隔（重试耗尽前转发方法回 `SERVICE_UNAVAILABLE`） |
| `trajectory_ui.enabled`（config） | 关 | `/api/trajectory/*` 全部 503 `TRAJECTORY_DISABLED`（§9.3） |
| `ASR_API_BASE` / `ASR_API_KEY` / `ASR_MODEL_NAME`（env） | 未配置 | `task.asr.transcribe` 未配置/非法 → `ASR_NOT_CONFIGURED` / `ASR_BAD_CONFIG`（§10.2） |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL`（env）→ config `health_check.every` | 60s | 探活周期；`target` / `active_hours` 见 §8.4 |
| `HEARTBEAT_JOBS_*`（env）→ config `heartbeat.jobs.*` | 每 session ≤5、全局 ≤100、interval ≥60s | heartbeat.job 资源限制（§8.2） |
| `gateway.cron.store_backend`（config） | `file`（`~/.jiuwenswarm/agent/home/cron_jobs.json`） | `etcd` 时 cron 任务改存 etcd（HA） |
| `proactive_recommendation.enabled`（config） | — | 主动推荐；开关变化自动创建/删除内置 `proactive-tick-auto` cron 任务（§8.1.6 拒绝面板删除） |
| `JIUWENSWARM_TASK_TEXT_LIMIT`（env） | 4096 字符 | `team.snapshot` 与 `team.task` 事件的 title/content 截断上限 |
| `GIT_PUSH_TIMEOUT_SEC`（env） | 60s | `project.git.push` 的命令超时 |
| 登录开关（部署侧） | 未开启 | `/api/v1/auth/authorize` 等 → 404 `login_disabled`；登录模型凭据链路不可用 |
| 部署形态：单用户 vs AgentOS | 单用户 | 握手鉴权启用与否、`/file-api` 挂载与否、接口数据隔离方式（§3.5） |

---

## 3. 通用契约

### 3.1 错误码体系

错误码出现在 `res` 帧 `code` 字段（WS）或 HTTP 响应体 `code`（HTTP）。**通用协议码**：

| code | 语义 | 典型触发 |
|------|------|---------|
| `BAD_REQUEST` | 请求帧/参数不合法（最高频默认码） | 帧结构非法；`mode`/`agent_id` 非 string；`session_id`/`run_id` 非法；各方法必填缺失、类型/枚举/边界校验失败；E2A 失败响应未携带 code 时的缺省（`e2a_proxy.py:110,220`） |
| `METHOD_NOT_FOUND` | 未注册的 req method（仅主 WS；`/ws/git` 对未知方法回 `BAD_REQUEST`） | `web_connect.py:1762-1764,1803-1806` |
| `INTERNAL_ERROR` | handler 未捕获异常兜底 | `web_connect.py:549-553`（`"handler error: <异常>"`） |
| `SERVICE_UNAVAILABLE` | 依赖不可达 | E2A：AgentServer client 为 None 或 `server_ready=False`（`"AgentServer is unavailable"`）；E2A 转发其他异常；本地服务未注入（cron/heartbeat/health_check 不可用等） |
| `AGENT_SERVER_TIMEOUT` | AgentServer unary 请求超时 | `"AgentServer request timed out"`（`agent_request_timeout.py:11-12`） |
| `NOT_FOUND` | 资源不存在（或无权访问——不泄露存在性） | session/project/cron job/heartbeat job/git watch 不存在；越权访问他人 cron job 也返回 NOT_FOUND |
| `FORBIDDEN` | 权限不足 | heartbeat/cron 归属校验失败；默认项目的破坏性操作（rename/pin/remove）；work 模式项目执行 Git 操作 |
| `CONFLICT` | 状态冲突 | heartbeat job update 冲突；project.create 目录/名称冲突；session.create 对已有 `persist_session` 会话变更 |
| `SESSION_BUSY` | 会话/项目执行中，拒绝写操作 | `/ws/git` discard/redo；`project.remove` 存在运行中会话 |
| `UNSUPPORTED` | 平台能力不支持 | 原生目录/文件选择器在当前平台不可用 |

**领域专用码**（详见各节）：流式错误（`AGENT_SERVER_ERROR` / `AGENT_SERVER_CONNECTION_CLOSED`，§4.9.4）、会话生命周期（`SESSION_ARCHIVED` / `OPERATION_IN_PROGRESS` / `ALREADY_EXISTS`，§6.4）、cron（`CRON_STOP_FAILED` 等，§8）、Git（`PROJECT_NOT_FOUND` / `NO_TURN_TO_DISCARD` / `DIFF_HISTORY_EXPIRED` 等，§7）、ASR（`ASR_*` 前缀族，§10）、HTTP auth（`auth_header_required` / `login_disabled` / `session_expired` 等，§9.1）、文件传输（`USER_MISMATCH` / `FILE_TOO_LARGE` 等，§9.2）、轨迹（`TRAJECTORY_DISABLED` 等，§9.3）。完整清单见附录 C。

### 3.2 E2A 转发错误映射（所有转发型方法的统一外壳）

`proxy_unary_request`（`gateway/routing/e2a_proxy.py:94-222`）对一元（unary）转发方法：

| AgentServer 侧情况 | 前端 res 帧 |
|--------------------|------------|
| client 为 None / `server_ready=False` | `ok:false, error:"AgentServer is unavailable", code:"SERVICE_UNAVAILABLE"` |
| 请求超时 | `ok:false, error:"AgentServer request timed out", code:"AGENT_SERVER_TIMEOUT"` |
| 转发其他异常（连接断等） | `ok:false, error:str(exc), code:"SERVICE_UNAVAILABLE"` |
| AgentServer 返回失败 | `error=payload.error 或 "<method> failed"`；`code=payload.code` 或该方法配置的 `default_error_code`（缺省 `BAD_REQUEST`，如 session.create 用 `SESSION_CREATE_FAILED`） |
| AgentServer 返回成功 | `ok:true, payload` 原样透传 |

`preserve_error_payload=True` 的方法（Git 域、harness.import/export）失败时 payload 整体回写，供前端读取结构化错误明细。

### 3.2.1 网关回复的透传分级

按"AgentServer 内容到达前端的保真程度"分四档（与 §3.2/§3.3 呼应的总览）：

| 档 | 载体 | 行为 |
|----|------|------|
| 完全透传 | 白名单转发 res（**含失败**——payload 整体保留，另把 error/message/code 提取到帧顶层，`web_connect.py:1166-1190`）；E2A 代理成功 res；`preserve_error_payload=True` 域（Git、harness.import/export）的失败 res；全量白名单事件（§3.3 名单） | payload 原样，仅自动补 `session_id`/`request_id` |
| 字段提取 | E2A 代理失败 res——只取 `error`（回退 `"<method> failed"`）与 `code`（回退 default_error_code），**其余失败 payload 字段丢弃**；白名单 res 失败的顶层 error 链 `payload.error` → `payload.message` | 外壳网关构造 |
| 投影改造 | 裁剪档事件（`chat.delta`/`reasoning`/`final` 压成 `{session_id, content}`+白名单键，§3.3） | 内容经改造 |
| 网关合成 | ack shell；本地 handler（cron/heartbeat/updater/config/task.asr/path）；帧级校验错误与 `INTERNAL_ERROR` 兜底；传输层错误；合成事件（cancel 的 `interrupt_result`+`processing_status`、cancelled_tools 转 `tool_result`、`chat.error` 投影 `AGENT_SERVER_*` 族、`connection.ack`、cron 占位推送） | 与 AgentServer 无关 |

> ⚠️ 前端实践：需要读失败响应的**结构化字段**（如 Git 的 `detail`、装备安装的 `pending_connectors`）时，注意区分两类方法——Git/harness 走 preserve_error_payload（payload 在），其余 E2A 代理域失败只有顶层 `error`/`code`。

### 3.3 事件 payload 投影规则

`_build_event_payload`（`web_connect.py:880-944`）把内部 Message 投影为前端事件 payload，分两档：

- **全量透传白名单** `_WEB_FULL_PAYLOAD_EVENT_TYPES`（`web_connect.py:118-176`）+ 前缀 `team.*`、`org.*`、`designer.*`、`a4p.*`、`harness.*`、`personal_context.context.*`（`web_connect.py:1119-1134`）：payload 原样透传（自动补 `session_id`）。控制类事件（`chat.tool_call`、`chat.error`、`todo.updated`、`chat.ask_user_question`、`history.message` 等）都在此列。白名单相对旧基线新增：`chat.client_tool_request`、`chat.llm_call_start/end`、`chat.moa_status`、`client.attachments_changed`、`jiuwenmate.chat.stream`、`prompt_suggestion.ready`（现 `web_connect.py:118-176`）；精确项另加 `video.core.result`、`cron.run.updated`。
- **裁剪档**（`chat.delta`、`chat.reasoning`、`chat.final` 等内容流事件）：压成 `{session_id, content}` + 白名单透传键（`role`、`member_name`、`team_name`、`team_id`、`request_id`、`turn_request_id`、`execution_id`、`source`、`proactive_*`、`timestamp`、`agent_template_name` 等，`web_connect.py:899-925`）；`chat.final` 额外透传 `cron` 块（定时任务结果推送标识，`:926-929`）。
- `chat.*` / `goal.*` / `execution.error` 事件缺 `request_id` 时回填消息 id（前端去重用，`:864-877`）。

### 3.4 事件路由与完整性

出站事件按以下优先级投递（`web_connect.py:946-1241`）：

1. team fan_out 精确路由（V2 `RoutingTarget`，§5.6）；
2. 发起请求的物理连接（`metadata.ws_id`）——发起连接存活时只投给它；**已断开的迟到事件不再丢弃**：回退 session 桶投给重连后的新连接（在途思考/正文/工具进度不丢失，`web_connect.py:1432-1439,1509-1549`；A4P 授权恢复另有新连接改绑特例）；
3. session 桶（同 session 的其它活跃连接，多 tab 场景）；
4. 特例全量广播：`health_check.relay`（探活全局态）、带 `cron` 块的 `chat.final`（按 `payload.user_id` 限定接收者，空 user_id 广播，`:985-1007`）、`proactive_notification`。

**前端不回执、不重排**：事件顺序由单连接 per-ws writer 串行保证；流式完整性见 §4.9.5。

### 3.5 身份、鉴权与多用户隔离

- **单用户部署**：无握手鉴权、无 user_id 隔离；Gateway 与 AgentServer 共享 `~/.jiuwenswarm/` 数据目录（旧基线 `session.list` 等接口在 AgentServer 离线时读本地目录的 fallback 已删除，现统一 E2A，见 §6.1）。
- **AgentOS 多用户部署**：`AgentOSRouterClient` 按连接 user_id（username）把 E2A 请求路由到每用户独立的 AgentServer 运行时（工作区 `/home/agentos/users/<username>`，`extensions/agentos/agentos_router/router_client.py:450-462`）。**数据隔离靠"每用户一个注入目录的 AgentServer"实现**，session/project/team 数据天然只落在该用户目录。
- **越权表现**：跨用户访问通常表现为 `NOT_FOUND`（查不到）；明确拒绝的：cron/heartbeat 归属校验 `FORBIDDEN`/`NOT_FOUND`、`/file-api` 的 `USER_MISMATCH`(403)、group 的 ownership 校验（以 `NOT_FOUND` 隐藏存在性）。
- **登录模型凭据**：`credential_ref` 与路由 user_id 是两套体系；凭据只能来自服务端登录会话（`_auth_session` 无条件覆盖客户端传值，`app_web_handlers.py:4883`）。登出时经 `auth.credentials.update` 撤销 AgentServer 侧凭据（`web_http_auth.py:140-152`）。

### 3.6 连接生命周期

| 阶段 | 行为 |
|------|------|
| 连接建立 | 握手注册 RoutingKey；AgentServer 就绪时发 `connection.ack` 事件：`{session_id, mode:"BUILD", tools:[], protocol_version:"1.0"}`（AgentServer 未就绪不发，`app_web_handlers.py:2162-2193`） |
| 首条请求 | 携带显式 `session_id` 的请求触发 re-register，覆盖占位 RoutingKey（`web_connect.py:1708-1723`） |
| 断开 | 在途消息任务给 5s 宽限收尾后 cancel；writer 冲刷残余帧后清理；**不取消 agent 会话**（关 tab 后端继续执行，与 TUI 不同）；**不清 busy 映射**（`web_connect.py:1481-1514`）；断连钩子清理该 ws 的 session-sharing/GodView 订阅（`app_web_handlers.py:2197-2206`） |
| 重连 | 新 ws_id；前端指数退避重连（1s 起、上限 30s、最大次数限制；`webClient.ts:538-548`），pending 请求全部以 `WS_DISCONNECTED` 拒绝；重连后主要靠 `history.get` 恢复上下文（§4.10） |
| 前端请求超时 | 15s 默认超时以 `REQUEST_TIMEOUT`（可重试）拒绝 pending promise——仅影响前端等待，不影响后端执行 |
| is_stream 请求 | `command.goal` set/resume 等正常路径无 res 的流式请求，前端用 fire-and-forget 发送、不注册 pending（`webClient.ts:332-365`） |

---

## 4. 单 Agent 对话接口

### 4.1 chat.send

> **网关处理**：**网关处理**——归一化 content→query、强制流式、先回 ack + processing_status(true)。
发送一条用户消息，启动一轮 agent 执行。**网关只回受理 ack，全部对话内容经事件流推送**。

**完整请求帧**（外层信封见 §2.2；`is_stream` 可省略——chat.send 恒按流式处理）：

```jsonc
→ { "type": "req", "id": "req_m0abc_1", "method": "chat.send",
     "params": {
       "session_id": "sess_18f3ab2c_9d4e1f",   // 下方参数表的字段均位于 params 内
       "content": "帮我看看当前项目的目录结构",
       "mode": "agent",
       "model_name": "qwen3-max"
     } }
```

**请求参数**（`params` 内字段）：

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `content` 或 `query` | string | 二选一 | 无 `query` 时 `content` 复制为 `query`（`app_gateway.py:292-294`）；空内容不前置拦截，由 runtime 处理 | 消息正文。支持 `@文件名` 引用（网关内联文件内容，有大小上限）与 `@agent-xxx` 智能体提及（网关注入 system-reminder 提示）；`message` 为等价别名（Skill 创建/修改场景约定字段） |
| `session_id` | string | 推荐 | §2.3 格式校验；缺省由网关合成 `sess_<hex时间戳>_<6hex>` | 目标会话。**携带显式 session_id 的请求会把当前连接 re-register 到该会话的路由键上**（`web_connect.py:1708-1723`）——多 tab 场景下事件按会话桶投递 |
| `mode` | string | 否 | 不传默认 `"agent"`（归一为 `agent.work.normal`，`web_connect.py:1710`）；非 string 拒绝；未知串兜底 `agent.work.normal`；`team` 前缀的未知值拒绝（`UNKNOWN_TEAM_MODE`） | 会话模式，决定单 agent / 集群执行路径与路由维度（完整枚举与迁移规则见 §5.1） |
| `agent_id` | string | 否 | `"default"` | RoutingKey `AgentRef.id` 维度；单 agent 模式下通常保持默认 |
| `files` | list[dict] \| dict | 否 | 缺省无附件（不触发外链导入）；项结构见下方「files 项」；带外链的项触发网关 E2A `file.import_url` | 文件附件，**两种形态**：直接发送时为 list（项结构见下）；`media.persist` 落盘后回传 chat.send 时为 dict（`{uploaded_images: [...]}` 等，`media_attachments.py:127-139`）。外链由 **AgentServer** 下载落盘（Gateway 不接触内容），导入失败保留原值由下游报可重试错误（`web_connect.py:586-661`） |
| `attachments` | list[dict] | 否 | 缺省不处理（正文 `@引用` 才生效）；项结构见下方「attachments 项」 | 结构化附件。网关把每项解析为 `@"<path>"` 前缀并入正文，并从正文中剥离对应的 `@` 提及（`message_handler.py:2991-3018,3057-3070`）；优先级高于正文的 `@` 引用（两者同时存在时只处理 attachments） |
| `model_name` | string | 否 | 缺省沿用会话模型选择（`session.selection.set` 持久值），无则用配置默认模型（`team_helpers.py:3395-3399`） | 本轮使用的模型（透传 AgentServer 解析）；与 `session.selection.set` 持久化的选择互补，单轮生效 |
| `input_mode` / `runtime_mode` | string | 否 | 缺省按普通发送（会先取消同 session 在途流）；枚举 `steer` / `follow_up`（大小写不敏感）；两者同传必须一致（否则 `BAD_REQUEST`）；语义见下方「input_mode 详解」 | 会话内补充输入（不新开一轮、不取消在途流） |
| `attach_goal` | bool | 否 | 默认 false；true 时**不取消在途流** | Goal 输出流**纯订阅**：不发送任何内容（query 不入模型、不写 user 历史），goal 非 active 直接受理结束、active 则附着流持续读 goal 事件（`interface_deep.py:18468-18504`，交互详见 §4.6）。用于宿主失去 set 流后重新附着（TUI/SDK 形态）；**Web 前端不使用**（set 流长连接直到终态） |
| `source` | string | 否 | 缺省无来源标记（普通消息，会取消在途流）；枚举见下方「source 枚举」 | 消息来源标记，影响是否取消在途流与路由行为 |
| `request_id` | string | source 为中断恢复型时必填 | — | 中断恢复型 chat.send 的载荷：对应所答问题的关联 id |
| `answers` | list[dict] | source 为中断恢复型时必填 | — | 答案数组，项结构同 §4.4 answers 项 |
| `is_supplement` | bool | 否 | 缺省不传（仅网关 supplement 流程内部合成） | 网关内部合成标记（supplement 切换任务时的新一轮），客户端无需传 |
| `cwd` | string | 否 | 缺省由服务端按会话工作区/全局默认解析（`interface.py:1740-1755`） | 工作目录，供附件路径解析与 agent 环境 |
| `trusted_dirs` | list[string] | 否 | 缺省为空列表 | 可信目录列表 |
| `work_mode` / `project_dir` / `project_id` | string | 否 | 缺省不携带；session 已存在时忽略；会话与项目的绑定校验发生在 `session.create`（§6.1） | 环境参数（`work_mode` 枚举 `work` / `code`） |

**files 项**（两种形态，`web_connect.py:586-661`）：

```jsonc
// 形态一：外链（网关转发 AgentServer 下载落盘，成功后原位替换为形态二）
{ "url": "https://example.com/report.pdf" }        // 或 "uri"
// 形态二：已落盘（media.persist / document.persist / file.import_url 的产物）
{ "path": "/abs/or/rel/path.pdf", "name": "report.pdf", "mime_type": "application/pdf" }
```

- 仅当列表中**存在**带 `url`/`uri` 的项才触发导入请求；纯本地 `path` 项原样透传。
- 导入结果按原 `files` 顺序合并；单用户共享目录部署用覆盖命名，AgentOS 多用户部署用防冲突命名（`overwrite` 语义不同）。
- AgentServer 不可达或导入失败时**保留原样**，由后续 chat 链路返回可重试错误——不阻断整条消息。

**attachments 项**（`message_handler.py:2991-3018`）：

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `path` | string | 是 | — | 文件引用路径（相对 `cwd` 解析）；strip 后为空的项被丢弃 |
| `type` | string | 否 | `"file"` | 附件类型标记 |
| `filename` | string | 否 | 取 `path` 文件名 | 展示名 |

按解析后的 path 去重。

**input_mode 详解**（`runtime/session_input.py:33-70`、`server/runtime/agent_adapter/session_input.py`）：

| 值 | 语义 | 约束 |
|----|------|------|
| `steer` | **转向**：把文本注入正在运行轮次的转向队列，实时影响当前执行，不新开轮 | 接受非空文本，**另可带已上传图片引用**（`media_items`，png/jpeg/webp/gif，引用式无内联数据；有图片引用时允许空文本，其余附件仍拒绝，`runtime/session_input.py:62-98`）；可带 `expected_execution_id` 精确锚定目标执行，该 id 对应的执行已结束/已变更时拒绝（`SESSION_INPUT_TARGET_CHANGED` 语义）；跨会话输入未获准入时 `SESSION_INPUT_QUEUE_REQUIRED` |
| `follow_up` | **补充**：挂在当前轮之后作为后续输入，同样不取消在途流 | 不支持 `expected_execution_id` |
| （未传/未知值） | 按普通 chat.send 处理（会先取消同 session 在途流） | — |

**source 枚举**（`message_handler.py:816-857`）：

| 值 | 语义 | 是否取消在途流 |
|----|------|--------------|
| `proactive_recommendation` | 主动推荐推送产生的消息 | 否 |
| `ask_user_interrupt` | 回答 `chat.ask_user_question` 的中断恢复（+`request_id`+`answers`） | 否 |
| `confirm_interrupt` | 回答确认类中断（含 plan 审批，§4.4） | 否 |
| `permission_interrupt` | 回答工具权限确认中断 | 否 |
| `evolution_interrupt` | 回答技能演进审批中断 | 否 |
| `client_tool_interrupt` | 回答 `chat.client_tool_request`（客户端本地工具执行结果）的中断恢复（+`answers`，无需 `request_id`） | 否 |

**会话配置与装备参数**（均可选，来自 `ChatSendParams` 契约 `common/schema/chat_send.py:33-231`）：

| 参数 | 类型 | 缺省行为 | 说明 |
|------|------|---------|------|
| `skills` | list[string] | 缺省不携带 | 用户选中的 skill 名（如 `/doc` `/review` 或 UI 选择器）；单 Agent 作为 prompt 提示，普通 Team 同时更新 Team 级 Skill 可见性，专家团不允许额外选择 |
| `agent_template_name` | string | 缺省保持当前专家 | 会话期望专家名：非空 load/切换，`""` 卸载当前专家 |
| `agent_group_name` | string | 缺省不携带 | 首次 Team 构建时选择的专家团包；**会话建立后不可切换**；与非空 `skills` 互斥（含从 metadata 继承的场景） |
| `plugin_names` | list[string] | 缺省保持 | 期望插件包名**全集**：差量同步，`[]` 卸载全部插件 |
| `enable_swarmflow` | bool | 缺省沿用会话持久值，首次回退 team 配置（`team_helpers.py:1131-1173` 三级解析） | team 模式 SwarmFlow 编排开关；true 激活编排循环。**与 plan 模式无后端互斥校验，仅前端互斥（§5.7 注）** |
| `swarmflow_budget` | int | 缺省用服务端默认预算 | 编排轮次上限；仅 `enable_swarmflow=true` 时有意义 |
| `swarmflow_worktree` | bool | 缺省保持会话持久值，首次回退 team 配置 | 会话级 worktree 隔离开关；true 时团队配备 worktree manager |
| `team_dispatch_mode` | `"autonomous"` \| `"scheduled"` | 缺省沿用会话持久值，首次回退 team 配置 | 派发模式；**仅会话首请求能改**，详见 §5.7 |
| `team_bootstrap_mode` | `"leader"` \| `"framework"` | 缺省沿用会话持久值 | 团队引导方式；仅 dispatch=scheduled 时合法，详见 §5.7 |
| `target_team_id` | string | 缺省不携带 | 用户消息明确指定的 Team；Owner 走常规路径，组织 Expert 走定向执行 |
| `workspace_dir` | string | 缺省不携带（回落 `cwd`/全局默认） | 工作空间目录；服务端会尝试创建该目录并同时约束 cwd 与沙箱（`interface.py:1740-1755`） |

**同步 res**（`app_web_handlers.py:3848-3854`）：

```json
{ "ok": true, "payload": { "accepted": true, "session_id": "<sid>" } }
```

此后内容全部走事件流（§4.9.1）。**`accepted:true` 只表示网关已受理**，业务成败看事件流终点（`chat.final` / `chat.error` / `chat.interrupt_result`）。

**内部行为**：普通 chat.send 会先取消同 session 在途流（`message_handler.py:4925-4929`）；steer/follow_up/attach_goal/中断恢复型/proactive 不取消。网关先发 `chat.processing_status(is_processing=true)` 再后台消费 E2A 流（`:5272-5311`）。

**错误**：帧级 `BAD_REQUEST`（§2.3）；流启动即失败回 `ok:false` res（`message_handler.py:5436-5455`）；流中途失败发 `chat.error` 事件（§4.9.4）；非法 team 模式发 `chat.error` 事件 `code:"UNKNOWN_TEAM_MODE"`（`message_handler.py:716-764`）。

### 4.2 chat.interrupt

> **网关处理**：**网关处理**——cancel 重构信封 {intent:'cancel', session_id}+定位参数，agent_notify='await'。
中断/暂停/恢复/补充当前轮次。

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `session_id` | string | 是 | — |
| `intent` | string | 否（默认 `"cancel"`） | 枚举：`cancel` / `pause` / `resume` / `supplement` |
| `new_input` | string | supplement 用（缺省空串） | 非空时走"切换任务"语义 |
| `attachments` | list[dict] | 否（缺省无附件） | supplement 附件（结构同 §4.1 attachments 项） |
| `mode` | string | 否（缺省回退该会话已注册的模式；无会话状态时信封不带此键，由 AgentServer 自行定位，`message_handler.py:1357-1377`） | 会话模式（§5.1 枚举）；AgentServer 据此定位正确的 adapter/runtime。team 会话正常流程下可省（网关从会话状态自动回填 team），**重连后立即 interrupt 的场景建议显式携带** |
| `team` | bool | 否（缺省 false） | team 模式提示标志；与 `mode` 为 team 变体二选一即可（`is_team_params` 任一为真即入 team 语境） |
| `work_mode` | string | 否（缺省不携带——仅会话归属查询落空时参与②级兜底定位，见下方四级定位链） | 枚举 `work` / `code` |
| `project_dir` | string | 否（缺省不携带——同上，仅兜底定位时消费） | 项目目录（绝对路径） |
| `trusted_dirs` | list[string] | 否（缺省空列表——不附加可信目录，工具文件访问范围不受额外授权） | 可信目录列表（非字符串项被服务端丢弃） |

以上 `mode` / `team` / `work_mode` / `project_dir` / `trusted_dirs` 五个参数在本方法中均为**透传定位参数**（`message_handler.py:1357-1386`）：不改变中断语义，仅供 AgentServer 定位目标 agent/runtime。

**缺省时的整体默认行为**（`runtime/request.py:521-582`，四级定位链）：① 首选按 `(channel_id, session_id)` 定位——遍历该渠道缓存的全部 agent 实例、逐个询问会话归属（`has_session_runtime`，`agent_manager.py:1402-1433`），**不依赖任何透传参数**（归属关系由此前 chat.send 执行时物化在各 agent 内部）；② 会话查不到且信封带 mode 时才用 mode/work_mode/project_dir 解析查找；③ 再兜底按默认路径查找；④ 全部落空时返回**幂等成功** `interrupt_result{success:true, "当前会话任务已终止"}`——不报错、不误伤。即缺省不携带不会导致未定义行为，最坏情况是幂等 no-op。

**同步 res**：`{accepted:true, session_id, intent?}`（`app_web_handlers.py:3956-3961`）。

**各 intent 语义与取消生效点**（`message_handler.py:4554-4754`）：

- **cancel**（默认）：网关 cancel 该 session 全部流任务并**等待 AgentServer 真实结果**后再转发 `chat.interrupt_result`（`agent_notify="await"`，`message_handler.py:5770-5777`——防亲和漂走时 killed=0 误导 UI），随后合成 `chat.processing_status{is_processing:false}`。向 AgentServer 发 `CHAT_CANCEL`：停掉 session 级流任务（`agent_ws_server.py:2835-2848`）→ Runtime 停止执行、取消 pending todos、暂停 ACTIVE Goal；AgentServer 回包中的 `cancelled_tools` 逐个转成 `chat.tool_result{status:"error"}` 补发前端（避免工具卡片悬转）。AgentServer 侧无 agent 时快速成功（"当前会话任务已终止"）；中断请求本身失败时回 `chat.interrupt_result{success:false, message:"任务终止失败: …"}`。
- **pause / resume**：**不取消任何任务**。AgentServer 的 ReAct 循环在 checkpoint 阻塞/解除。回 `chat.interrupt_result{intent, success, message:"任务已暂停/已恢复/已完成", has_active_task?}`。**pause/resume 后网关不再补发合成 `chat.processing_status`**（服务端权威帧；resume 被服务端当空操作处理，issue 5762）；仅 supplement 合成 `true`、cancel 合成 `false`（`web_connect.py:1586-1602`）。
- **supplement**（带 `new_input`）：① 取消旧流 → ② 发 `chat.interrupt_result{intent:"supplement"}` → ③ 同步向 AgentServer 发 `CHAT_CANCEL{intent:"supplement", new_input}` → ④ 网关合成**新的 chat.send**（`is_supplement:true`，新 request_id `req_<hex>_<原id>`）重新入队。演进审批进行中则排队并回"已加入队列，等待演进完成"。

**可中断性**：流式输出、工具执行、模型生成均可中断；pause 只在 checkpoint 生效。

### 4.3 chat.resume

> **网关处理**：**网关处理**——改写为 chat.interrupt + intent:'resume'（setdefault）。
恢复暂停中的轮次（不新建轮）。**网关归一化将其改写为 `chat.interrupt` + `intent:"resume"`**（`app_gateway.py:295-297`），行为同 §4.2 的 resume 分支。

**同步 res**：`{accepted:true, session_id}`（`app_web_handlers.py:3948-3954`）。

另一类"恢复"是**中断恢复型 chat.send**（`source ∈ {ask_user_interrupt, confirm_interrupt, permission_interrupt, evolution_interrupt}` + `request_id` + `answers[]`），走 chat.send 流式路径且不取消在途流（§4.1）。

### 4.4 chat.user_answer

> **网关处理**：**网关处理**——本地 handler 回 ack 后转发原 params。
回答服务端下发的选择题/确认题（对应 `chat.ask_user_question` 事件，§4.9.2）。

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `session_id` | string | 是 | 目标会话 |
| `request_id` | string | 是 | 所答问题的关联 id，即 `chat.ask_user_question` payload 的 `request_id`；AgentServer 按其前缀分派（`symphony_experience_*` 经验安装、`team_skill_evolve_*`/`skill_evolve_*`/`evolve_simplify_*` 技能演进审批、`call_*` 工具确认等） |
| `answers` | list[dict] | 是 | 答案数组，项结构见下表；与问题按 `question` 文本或顺序对应 |
| `evolution_meta` | dict | 否（缺省不携带） | 演进审批元数据（下发问题时的 `evolutionMeta` 原样回传）。缺省时服务端取空 dict（`evolution_helpers.py:388-392`），分派仍靠 `request_id` 前缀完成，但 `event_kind` 等上下文退化为启发式判定——功能不中断，精度降级 |
| `plan_approval_kind` | string | 否（缺省不携带——team plan 结构校验分支不触发（校验以键存在性为条件），答案按 `request_id` 正常分派） | 计划审批类型（下发问题的 `planApprovalKind` 回传） |
| `plan_content` / `plan_language` | string | 否（缺省不携带——影响同上：不触发 team plan 结构校验，不参与决策） | 计划内容 / 语言（`"cn"`\|`"en"`，计划修订场景回传） |

**answers 项结构**（`channels/web/frontend/src/types/websocket.ts:183-204`）：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `question` | string | ask_user_interrupt 场景必填 | 所答问题的**原始文本**（后端据此把答案归属到对应问题）；其余场景可省 |
| `selected_options` | list[string] | 是 | 选中的选项 `label` 列表（多选时多项） |
| `custom_input` | string | 否 | 自定义补充输入（自由文本作答）；缺省空串——无补充文本，拒绝类反馈退化为默认文案 |
| `card_id` | string | 否 | 卡片 id（多卡片场景定位）；缺省不携带——答案按 `request_id` 归属问题（permission 场景多卡片时才需卡片定位） |

**同步 res**：`{accepted:true, session_id, request_id?}`。转发为**非流式** E2A；AgentServer 按 request_id 前缀分派（技能演进审批、symphony 经验安装、计划审批等），最终 res `{accepted:true, resolved:bool}`（`interface_deep.py:15243-15299`）。演进审批中断型答案会被网关改道为 chat.send。

### 4.5 chat.swarmflow_reply

> **网关处理**：**网关处理**——本地 handler 回 ack 后非流式转发原 params。
回答 SwarmFlow 工作流中的人机交互节点（对应 `chat.ask_user_question` 事件且 `source:"swarmflow_human"`，payload 带 `swarmflowMeta`）。

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `session_id` | string | 是 | 目标会话 |
| `correlation_id` | string | 是 | 人机回合关联 id |
| `answer` | string | 是（非空） | 人的回复正文 |
| `run_id` | string | 否 | 工作流 run；影响投递目标 `swarmflow:<run_id>:<corr>`（缺省 `swarmflow:<corr>`） |

缺参回 `ok:false "missing session_id/correlation_id/answer"`。

**同步 res**：`{accepted:true, session_id}`。非流式转发；AgentServer 构造 HumanAgentMessage 投给 pending human-session future。缺参回 `ok:false "missing session_id/correlation_id/answer"`。此方法不取消现有流、不走演进审批（`message_handler.py:4546-4548` 注释）。

### 4.6 command.goal（目标模式）

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
Goal 是**自驱长目标**：set 后服务端自行循环执行（attempt 轮）直至评估完成/受阻/被控制，宿主只需一条附着流持续接收事件。Web 前端用目标栏（GoalBar）承载，TUI 可用 `/goal` 斜杠（Web 不解析斜杠，必须走结构化方法，`interface_deep.py:14512-14525`）。

**入口分流**（`runtime/service.py:3226-3236`）：

| action | 顶层 `is_stream=false`（Web 常用） | 顶层 `is_stream=true` |
|--------|-----------------------------------|----------------------|
| `get` / `pause` / `clear` | 一元 res（GOAL_CONTROL） | 单帧 `goal.snapshot` 事件流（is_complete） |
| `set` / `resume` | 同样落穿持续流（set/resume 恒走流式执行，`interface.py:3665`） | 持续流：`goal.snapshot` 头帧 → goal 执行事件 → 终态（GOAL_STREAM） |

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `session_id` | string | 是 | 目标会话 |
| `action` | string | 否（默认 `get`） | 枚举：`get` / `set` / `pause` / `resume` / `clear`（**`stop` 未实现**——落到 `invalid_action`，`interface_deep.py:16585-16591`） |
| `objective` | string | set 用（缺省空串——set 得到空目标，set 场景应携带；空/仅空白 → `invalid_objective`） | 目标文本；仅 set 成功写 user 历史（pause/resume/clear/get 均 control-only） |
| `overwrite_confirmed` | bool | 否（默认 false） | 已有 goal 时 set 须 true，否则产出 `goal.confirm_required`；**Web 前端恒带 true**（本地弹窗确认，`webClient.ts:906`——该事件实际只对不带确认位的客户端出现） |
| `token_budget` | int \| null | 否（缺省 null=不设预算） |≤0 → `invalid_objective`"token_budget must be positive" | **结构化端点原样透传**（无字符串/bool 软化：传字符串以 "Goal command error" 失败；bool 因 int 子类被当数值接受） |
| `max_attempts` | int \| null | 否（缺省 null=不设上限） |≤0 → `invalid_objective`"max_attempts must be positive" | 同上，原样透传 |

**Goal 状态机**（4 态，无 failed——失败终态是 blocked；`openjiuwen:harness/goal/manager.py`）：

```
(无 goal) ──set──► ACTIVE(revision=0)
已有 goal 未带 overwrite_confirmed ──► goal.confirm_required（状态不变）
已有 goal + overwrite_confirmed ──► 整体替换（新 goal_id；旧在跑轮 cancel "goal_overwrite"）

ACTIVE ──pause──► PAUSED（不打断在跑 attempt；其迟到评估仍可落地：终态覆盖 PAUSED，CONTINUE 停表保持）
ACTIVE ──attempt 评估 COMPLETE──► COMPLETED   ──评估 BLOCKED/轮异常/hook 阻断──► BLOCKED
ACTIVE ──attempt 评估 CONTINUE──► 保持 ACTIVE 并自驱入队下一 attempt
ACTIVE ──attempt_count ≥ max_attempts──► BLOCKED（evidence="max_attempts_exhausted: N/M"）
ACTIVE ──token_usage.total ≥ token_budget──► BLOCKED（evidence="token_budget_exhausted: used/budget"）
PAUSED/BLOCKED ──resume──► ACTIVE（无在跑 attempt 时 revision+1 并入队新 attempt；不重置计数）
COMPLETED ──resume──► invalid_state（终态不可恢复）；ACTIVE ──resume──► 幂等 "Goal already active."
任意状态 ──clear──► 记录删除（cancel 在跑轮 "goal_clear"，发 goal.updated(goal=null)）
```

**GoalRecord 字段**（`goal.updated`/`goal.snapshot` 的 `goal` 载荷，`openjiuwen:harness/goal/schema.py:210-227`）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `goal_id` | string | `uuid4().hex[:12]` |
| `objective` | string | 目标正文 |
| `status` | string | `active` / `paused` / `completed` / `blocked` |
| `revision` | int | 代际号（set=0；resume 无在跑 attempt 时 +1；pause 不加） |
| `attempt_count` | int | 已开始 attempt 数 |
| `token_usage` | object | `{input_tokens, output_tokens, cached_input_tokens, total_tokens}` |
| `max_attempts` / `token_budget` | int \| null | 上限（未设为 null） |
| `last_assessment` | object \| null | `{status: continue\|complete\|blocked, evidence, remaining_work, next_instruction}` |
| `last_stop_reason` | string \| null | `completed` / `blocked` |
| `time_used_seconds` | int | 已结算活跃秒数 |
| `active_started_at` | string \| null | 当前计时起点（非 active 为 null） |

**set 的执行流程**（`interface_deep.py:18369-18467`）：attach 抢占输出流 → GoalManager.set（校验/建记录/入队首个 attempt/发 goal.updated）→ 立即 yield `goal.snapshot{action:"set", goal}`（is_complete=false，先于 goal.updated 到达）→ 写 objective 的 user 历史（`role=user, extra={goal_id, is_goal_objective_message:true}`；同 session 有并发任务时**推迟到 user→goal 内容边界再落盘**，保证时间序）→ 持续读流直到终态。set/resume 正常路径**永远没有 res**（fire-and-forget，`webClient.ts:877-883`）。

**执行期行为**：每个 attempt 是完整 agent 轮（query 被整体替换为 `<goal_task>` XML：objective + 上轮评估 + 预算提示；系统提示挂载 Goal 工作规则），产出常规 `chat.reasoning/delta/tool_call/tool_result`；attempt 结束由评估器判定（默认 HYBRID：agent 的 `submit_goal_report` 报 COMPLETE/BLOCKED 须 transcript 独立验证，CONTINUE 直接信任；无报告走 transcript 评估）。**`goal.updated` 在每个 attempt 边界发出**（含 CONTINUE），token 用量与 attempt_count 随之刷新；`accumulate_usage`/`begin_attempt` 不单独发事件。预算上限在 attempt 结束评估时判定，仅把 CONTINUE 覆盖为 **BLOCKED**（非 completed）。

**goal 事件与 chat.\* 的交织规则**（前端需容乱序——两事件走不同通道，`useWebSocket.ts:253-254`）：

- user→goal 边界：合成一条空 `chat.final{content:""}` 关闭 user 气泡；
- attempt 边界的中间 `chat.final` 在 goal 仍 ACTIVE 时**降级为** `chat.delta{content, goal_intermediate:true}`（已流式过的正文直接丢弃防重复）；终态后的 final 保持 terminal；
- attempt 之间不插分泡 final；
- `chat.interrupt_result`（cancel）**附带 `goal`（paused 快照）**——cancel 对 ACTIVE goal 先执行 pause（不清记录），供无输出租约时刷新目标栏（`interface_deep.py:15289-15308, 15377-15381`）；
- `execution.error` / goal 路径的 `chat.error` 也附带 `goal` 字段；
- completed 时额外幂等落盘一条 `goal.completed:` JSON 的 assistant 历史卡片（`interface_deep.py:16365-16411`）。

**控制动作语义**：

| action | 语义 | 关键行为 | 失败码 |
|--------|------|---------|--------|
| `get` | 只读快照 | **lock-free peek**——goal 轮持锁时也不阻塞（`manager.py:89-97`） | `goal_manager_not_started` |
| `pause` | 暂停续跑，**不打断在跑 attempt** | 丢排队续跑 work、结算计时；在跑 attempt 的评估仍可提交；非 active 时**先执行 pause 再报 `invalid_state`** 并带回 paused 快照 | `no_goal` / `invalid_state`（非 active） |
| `resume` | 恢复续跑 | 折叠计时、revision+1（无在跑时）、入队新 attempt；**不重置计数** | `no_goal` / `invalid_state`（**仅 PAUSED/BLOCKED 可 resume**；completed 等终态拒绝；已 active 幂等 "Goal already active."） |
| `clear` | **删除记录**（区别于 pause：之后 set 是全新 goal） | settle 计时 → 删 store → cancel 在跑轮 → `goal.updated(goal=null)` | `no_goal` |

**事件 payload**：

| 事件 | payload | 触发 |
|------|---------|------|
| `goal.snapshot` | `{event_type, action, goal, cleared_goal?, message?}`（one-shot 帧含 message 且 is_complete；set/resume 头帧仅 `{action, goal}` 且 is_complete=false） | get/pause/clear 单帧；set/resume 头帧 |
| `goal.updated` | `{event_type, goal}`（clear 后 goal=null） | set/pause/resume/clear 后及**每个 attempt 边界** |
| `goal.confirm_required` | `{event_type, existing_goal, requested_objective}`，is_complete=true | set 遇已有 goal 且未带 overwrite_confirmed（Web 恒带 true 故实际不出现） |

**非流式 res payload**（get/pause/clear，`interface.py:3046-3066`）：`{action, message, goal(权威快照，clear 后 null), record(goal 副本，兼容旧 TUI), cleared_goal?(仅 clear), existing_goal?/requested_objective?(仅 confirm_required), code?(错误时), error?}`；`ok:false` 条件为 `goal_error` **或** `goal_confirm_required`。

**流式各动作序列**：

| 动作 | 成功序列 |
|------|---------|
| get/pause/clear | 单帧 `goal.snapshot{...}` is_complete |
| set | `goal.snapshot(set)` → `goal.updated` → [attempt: chat 流 + 边界 goal.updated]×N → 终态 `goal.updated(completed/blocked)` → `chat.final` → is_complete |
| set（输出租约被 chat 持有） | `goal.snapshot(set)` → `runtime.accepted` → is_complete（goal 正文从原 chat 流继续） |
| resume | 同 set（`goal.snapshot(resume)` 起；已 active 时 `goal.snapshot` → `runtime.accepted` → is_complete） |
| 失败 | 单帧 `chat.error{code, goal}`（one-shot 分支键为 `error`，set/resume 流分支键为 `message`，`interface_deep.py:18416-18421`）is_complete |

**错误码**：`invalid_objective`（objective 空/预算≤0）、`already_exists`（唯一产出 `goal_confirm_required` 的码）、`no_goal`（pause/resume/clear 无目标）、`invalid_state`（pause 非 active / resume 终态）、`invalid_action`（含 stop）、`goal_manager_not_started`、`goal_error`（one-shot 无 code 时兜底）。BLOCKED 的 evidence 前缀（非错误码）：`round_execution_error:` / `max_attempts_exhausted: N/M` / `token_budget_exhausted: used/budget` / hook 阻断文案。

**attach_goal 补充**（chat.send 参数，见 §4.1）：`attach_goal:true` 的 chat.send 是**纯输出流订阅**——不发送任何内容（query 不入模型、不写 user 历史），仅 peek goal：非 active 直接 `runtime.accepted` 结束，active 则附着流持续读 goal 事件（`interface_deep.py:18468-18504`）。用途是宿主失去 set 流后重新附着（TUI/SDK 形态）；**Web 前端不使用**（set 流长连接直到终态）。

### 4.7 command.btw / command.compact / command.mcp / command.workflows

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
均为白名单转发、非流式 res（除非显式 `is_stream`）。以下契约依据 AgentServer 仓 develop@475e3a3b64 核对。

#### command.btw

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `question` | string | 是 | strip 非空；空 → `ok:true {status:"failed", error:"Question is required"}` | 独立单轮 LLM 侧问答，不改历史、无工具 |
| `mode` | string | 否 | 缺省 "agent"，归一 | — |

- **响应**：`{status:"ok", answer}` / `{status:"no_context"}` / `{status:"failed", error}`；异常 `ok:false {status:"failed", error}` **无 code**。

#### command.compact

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：`mode`（缺省 "agent"）。**响应**：`{result:"compressed"|"noop"|…, stats, summary?, compact_summary?}`；成功压缩时推 `context.compression_state` 与 `context.usage` 事件并写历史；异常 `ok:false {error}` 无 code。

#### command.mcp

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `action` | string | 否 | 缺省 "list"；枚举 list/show/add/update/enable/disable/remove(=delete)/list_tools/oauth_callback，非法 → `MCP_BAD_REQUEST` | — |
| `name` | string | 条件 | **show 缺省=列 enabled 项**；enable/disable/remove/delete/list_tools **必填**（缺失 → `MCP_BAD_REQUEST`）；add/update 亦必填 | — |
| `transport` | string | add/update 必填 | stdio/sse/http/streamable-http/streamable_http | — |
| `command` | string | stdio 必填 | 非空；args/cwd/env 可选 | — |
| `url` | string | http 族必填 | 非空；headers/timeout_s 可选 | — |
| `enabled` | bool | 否 | 缺省 true | — |

- **响应**：list `{type:"list", items, cache?}`；show `{type:"detail", item, tool_count}`（两级探测）；add/update 预检+落 config.yaml → `{type:"added"/"updated"}` 或失败 `{type:"add_failed"/"update_failed", name, error}`；enable/disable/remove `{type:..., name, applied, item, error?}`；list_tools `{type:"tools", tools, server_name}`。卡片敏感字段一律脱敏。错误：`MCP_NOT_FOUND`/`MCP_BAD_REQUEST`/`MCP_INTERNAL`。

#### command.workflows

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `action` | string | 否 | 缺省 "list"；枚举 list/get_workflow/get_phase/get_agent | 非法 → `ok:false "unknown action: ..."`（无 code） |
| `workflow_id`（别名 `workflow_run_id`） | string | get_* 必填 | 缺失/不存在 → `ok:false`（无 code） | — |
| `offset` / `limit` | int | 否（list） | offset 缺省 0（0–10,000,000）；limit 缺省 50、**1–200** | run 分页 |
| `phase_offset` / `phase_limit` | int | 否（get_workflow） | 缺省 0 / 20；limit **1–100** | phase 摘要分页 |
| `phase_id` | string | get_phase/get_agent 必填 | 缺失 → `ok:false` | — |
| `agent_offset` / `agent_limit` | int | 否（get_phase） | 缺省 0 / 50；limit **1–200** | — |
| `agent_id` | string | get_agent 必填 | 缺失 → `ok:false` | — |

- **响应**：list `{type:"workflow_run_snapshot", action:"list", session_id, workflows[], total, has_more}`；get_workflow 为 run meta + 分页 phase 摘要；数据源 live → checkpoint 恢复（遗留 `running` 归一 `paused`+`recovered`）；超长字段切分 `_{field}_parts`。

### 4.8 history.get

> **网关处理**：**网关处理**——强制流式、ack 参数回显、不抬升/清 busy。
拉取会话历史。**网关只回受理 ack，数据经 `history.message` 事件流推送**（归一化强制 `is_stream=true`；该流不抬升/不清空会话 busy）。

**请求参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `session_id` | string（必填） | 目标会话 |
| `page_idx` | int | 页码协议（1-based）；**未携带 `cursor` 键时必须提供**——缺失/非法回 `chat.error "invalid page_idx or session history not found"` |
| `cursor` | string \| null | **params 中出现 `cursor` 键即切换游标协议**（首传 `null` 从头拉取）；必须为 null 或 string，否则 `INVALID_HISTORY_CURSOR` |
| `limit` | int | 否；游标协议每批条数，默认 50 |
| `subagent_id` | string | 否；子 agent 视图（缺省返回主会话记录） |

**同步 res**：`{accepted:true, session_id, page_idx?, cursor?, limit?}`（参数回显，`app_web_handlers.py:3977-3988`）。

**事件流**（`agent_ws_server.py:7541-7737`）：每条记录（web 通道按 32KB 分片）一个 `history.message` 事件，payload `{message, session_id, subagent_id, total_pages, page_idx, cursor, next_cursor, has_more, snapshot_id, snapshot_end}`；首批额外先推一条 `todo.updated` 全量快照；结束帧 `history.message{status:"done"}`（is_complete）。错误：游标非法/快照变更 → `history.message{status:"error", code:INVALID_HISTORY_CURSOR|HISTORY_SNAPSHOT_CHANGED}`；会话不存在/页码非法 → `chat.error{error:"invalid page_idx or session history not found"}`。

### 4.9 流式契约（核心）

#### 4.9.1 单轮事件序列

典型单 agent 轮次按时间序：

```
res {accepted:true, session_id}                          # 受理 ack
chat.processing_status {is_processing:true}              # 网关先发
runtime.accepted {request_id, timestamp}                 # runtime 确认受理
chat.reasoning / chat.delta ...                          # 思考与正文（交错）
chat.tool_call → [chat.tool_update] → chat.tool_result   # 工具阶段（可多轮，穿插 delta）
[chat.usage_metadata / chat.usage_summary / context.usage / todo.updated /
 goal.updated / chat.ask_user_question / plan.approval_required / plan.mode_exited]
chat.final {content, session_id}                         # 终轮正文
chat.processing_status {is_processing:false, is_complete:true}
```

**终点三选一**：`chat.final`（成功）/ `chat.error`（失败）/ `chat.interrupt_result`（取消，随后合成 processing_status false）。`chat.ask_user_question` 不是终点——等待用户作答（§4.4）。

#### 4.9.2 payload 关键字段

- 内容流事件（delta/reasoning/final）为裁剪档投影：`{session_id, content}` + 白名单键（`role`、`member_name`、`team_name`、`request_id`、`source` 等，§3.3）。
- 控制事件（tool_call/tool_result/processing_status/error/ask_user_question 等）全量透传。
- `chat.tool_call` 带 `tool_call.arguments` 与 `reasoning_content`；`chat.tool_result` 的 `rendered_result` 即模型实际看到的工具返回文本。
- `context.usage` 事件持久化前端用量到 `context_usage.jsonl`（10MB 轮转 ×3，`web_connect.py:150-153,1808-1860`）。

#### 4.9.3 顺序保证与 seq/stream_id

- 前端事件帧的 `seq`/`stream_id` 为**可选保留字段，当前主路径从不赋值**（`_serialize_frame` 只发 `{type,event,payload}`，`web_connect.py:1878-1918`）；前端**不得**依赖它们排序/去重。
- 顺序由单连接 per-ws writer 串行保证；E2A 内部线（Gateway↔AgentServer）每个 chunk 有 0-based `sequence` 且同请求严格有序（`agent_client.py:608-651`），前端无感知。
- `send_event` API 支持 `seq`/`stream_id` 参数（`web_connect.py:403-431`），当前仅企微 IM 通道消费 `stream_id`。

#### 4.9.4 流式出错处理

- **AgentServer 流中途失败**：网关发 `chat.error {error, code, is_complete:true}`；code 推导：异常自带 code > `TimeoutError→AGENT_SERVER_TIMEOUT` > 连接关闭→`AGENT_SERVER_CONNECTION_CLOSED` > `AGENT_SERVER_ERROR`（`message_handler.py:3810-3846`）。
- **聊天族 unary 失败**（chat.send/resume/user_answer/swarmflow_reply 无 event_type 的失败响应）：统一投影为 `chat.error` 事件（`message_handler.py:3100-3139`）。
- **流启动即异常**：回 `ok:false` res 帧。
- AgentServer 侧任何流式请求有 TurnOutcomeTracker 兜底（chat.final→succeeded、ask_user_question→waiting_user、终错→failed，`agent_ws_server.py:553-601`）；请求级异常最终回写失败 wire（`:2924-2943`）。
- keepalive 空闲帧不下发前端（`message_handler.py:3795-3798`）。

#### 4.9.5 完整性保证

**正常或失败路径下，单轮流保证以 `chat.final`、`chat.error` 或 `chat.interrupt_result`（+processing_status false）之一收尾**。前端物理断连时尾部事件**不再丢弃**——回退 session 桶投给重连后的新连接（§3.4）。流帧合并（coalesce）只作用于相邻同类内容帧、不跨控制帧重排，不破坏该保证。

#### 4.9.6 流帧合并

网关 writer 对队列中**连续的** `chat.delta` / `chat.reasoning` 帧合并：条件为 event 相同且 payload 除 `content` 外全部键值一致，`content` 字符串拼接；**上限 32 帧**（`_STREAM_COALESCE_MAX_FRAMES`）；遇到异构帧立即停止合并、保留原顺序（`web_connect.py:94-95,220-300`）。合并仅是发送侧优化，前端语义不变。

#### 4.9.7 会话忙碌状态（busy）

`chat.processing_status`（`is_processing`）与 `chat.interrupt_result`（pause/supplement/resume 视为忙碌、cancel 视为空闲）共同维护网关 busy 映射（`web_connect.py:1286-1326`）；供 `/ws/git` 写操作与 `project.remove` 预检查询。同 session 多流时仅最后一个流结束才发 processing_status false（`message_handler.py:5031-5039`）。

### 4.10 断线恢复

- **关 tab 不停止执行**：Web 断开不取消 agent 会话；busy 映射保留（防止 discard/redo 与仍在写的 agent 并发）。
- **重连恢复上下文**：`history.get`（主要手段，可还原 `chat.final`/`tool_call`/`tool_result` 等事件型记录）→ `session.get_metadata`（返回 `queued_session_messages` 与 `can_continue_queued_session_messages:true` 时提示用户）→ 用户确认后调 `session.message.continue_queued`（§6.1）释放跨会话信箱队列。
- 暂停中的轮次用 `chat.resume` 恢复。

---

## 5. Agent 集群（Team）模式接口

### 5.1 进入方式与 mode 枚举

team 模式由 `chat.send` / `session.create` 的 `mode` 参数进入。`Mode` 枚举（协议包 `e2a/message.py:689-708`）canonical 值：

| 类别 | canonical | 说明 |
|------|-----------|------|
| 单 agent | `agent.work.normal` / `agent.work.plan` / `agent.code.normal` / `agent.code.plan` | plan 为计划模式 |
| 集群 | `team.work.normal` / `team.work.plan` / `team.code.normal` / `team.code.plan` | `team.design` 是 `mode_matrix.py` 的 canonical 组合值，**不是** `Mode` 枚举成员（`Mode.from_raw("team.design")` 会兜底 `agent.work.normal`） |
| 历史值（静默迁移） | `agent` → `agent.work.normal`；`team` → `team.work.normal`；`code.team` → `team.code.normal`；`team.plan.normal/code` → `team.work.plan`/`team.code.plan`；裸 `plan`/`fast` → `agent.work.normal`（在 `Mode.from_raw` 归一，协议包 `message.py:728-730`） | `DEPRECATION_MAP`，`common/mode_matrix.py:94-105`（行号未变） |
| 非法值 | 未知串兜底 `agent.work.normal`；**以 `team` 开头但不在已知集合 → 拒绝**：`chat.error` 事件 `code:"UNKNOWN_TEAM_MODE"`（`message_handler.py:716-764`） | — |

- Web 前端通常发 `mode="team"` + `work_mode="work"|"code"`，经组合表解析（`mode_matrix.py:164-173`）。
- mode 是 RoutingKey 的 `AgentRef.mode` 维度；team 会话的连接以 `mode="team"` 注册，出站 team 事件据此兜底命中（`web_connect.py:1708-1723`）。
- team 模式 chat.send 额外触发 **GodView 订阅注册**（web 前端作为观察者接收全部 team 事件，`message_handler.py:558-648`）。
- team 会话的 session metadata 携带 `team_name`、`team_template_id`、`session_team_dispatch_mode`、`session_team_bootstrap_mode`（§5.7）。
- 已绑定 team 的会话，chat.send 自动回填 `team_name`/`team_template_id`（`agent_ws_server.py:5131-5161`）。

### 5.2 team.* 方法

均为白名单纯转发（`_FORWARD_NO_LOCAL_HANDLER_METHODS`），由 AgentServer 处理；错误码遵循 §3.2（AgentServer code 透传，缺省 BAD_REQUEST）。**企业版另有 team 控制面 7 方法**（team.create / get / stop / member.join / leave / members.list / quota.get，`app_web_handlers.py:699-707`）：仅企业版注册并转发；个人版从转发白名单剔除，WS 调用回 `METHOD_NOT_FOUND`，HTTP 路由同样按版本过滤（`http_routes/mapped.py:946-973`）。以下参数契约依据 AgentServer 仓 develop@475e3a3b64 核对。

#### team.templates.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无。**响应**：`{templates:[{template_id, display_name, available:true, source:"modes.team.<id>", team_name}]}`（读 config.yaml `modes.team.*`；无配置回退 legacy 单模板）。恒 ok=true。

#### team.bindings.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无。**响应**：`{teams:[<TeamBinding> + 增强字段]}`，按 `updated_at` 降序。TeamBinding：`team_name/template_id/created_at/updated_at/session_ids/last_session_id/legacy/model_selection?`；增强：`template_available/source_template_available/team_config_available/team_config_path/session_count/active_session_id/selectable/disabled_reason("active"|"legacy"|"team_config_missing"|"")`。读取异常按 legacy/缺失降级，不报错。

#### team.binding.create

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `team_name` | string | 是 | 1-64 字符；禁路径分隔符/控制字符；非 `.`/`..` | 超规 → `BAD_REQUEST` |
| `template_id` | string | 是 | 必须在已配置模板集合内 | 未命中 → `NOT_FOUND` |

- **响应**：`{team: <TeamBinding>}`。**错误**：`CONFLICT`"team_name already exists"；删除竞态 → `TEAM_DELETING`。

#### team.binding.generate

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `description` | string | 是 | strip 非空；别名 `prompt` | 用第一个模板生成名 |

- **响应**：`{team, template}`。**错误**：`BAD_REQUEST`（空描述）、`NOT_FOUND`"no team template configured"、`GENERATION_FAILED`、`CONFLICT`"unable to allocate a unique team_name"（`_2.._101` 后缀重试 100 次后）。

#### team.session.bind

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 回退 envelope；session 目录须存在 | 缺失 → `BAD_REQUEST`；不存在 → `NOT_FOUND` |
| `team_name` | string | 是 | binding 与 entity 配置须存在 | entity 缺失 → `NOT_FOUND`"team entity config missing" |
| `mode` | string | 否 | 缺省 "team"，经 resolve 归一 | — |

- **响应**：`{session_id, team_name, team_template_id, mode, team, team_config_path}`。从其它 team 绑定中移出。

#### team.delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `team_name` | string | 是 | strip 非空 | 空缺 → `BAD_REQUEST "team_name is required"` |
| `team` | bool | 条件必填 | 与 mode 为 team 模式二选一（任一为真即入 team 语境），否则 `UNSUPPORTED_MODE "team.delete is only supported for team mode"` | 团队语境标志 |
| `mode` | string | 条件必填 | 为 team 模式变体时入 team 语境 | 团队模式 |
- **响应**：`{team_name, session_ids, failed_session_ids, deleted, recovery_required}`；失败附 `error`/`code`。
- **错误**：`NOT_FOUND`（team 不存在）、`STOP_TIMEOUT`/`DELETE_FAILED`（运行时错误）。

#### team.snapshot（团队看板）

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 缺省回退请求 envelope 的 session_id | 关联会话 |
| `team_name` | string | 否 | 缺省取会话活跃 team | 点名团队（多 team 并发时才需显式） |
- **数据源三级回退**：live（session 级 monitor）→ live-scoped（指定 team 的只读 monitor）→ db（TeamDB）；响应含 `members_source: "live"|"live-scoped"|"db"|"empty"`。
- **响应**：`{members:[{member_id, name, status, execution_status, mode, role, cli_agent}], tasks:[{task_id, team_name, title, content, status, assignee, updated_at}], team_id, members_source}`；title/content 超 4096 字符截断并附 `*_truncated`/`*_original_size`。
- **恒 ok=true**（读失败降级空快照）。

#### team.history.get

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 非空 | 缺失 → `ok:false {error:"session_id is required"}`（无 code） |
| `member_name` | string | 否 | 非空时仅返回该成员相关记录 | — |
| `cursor` / `offset` | int | 否 | 缺省 0；0 ≤ v ≤ total（互为别名） | — |
| `limit` | int | 否 | 缺省 500；**1–1000** | — |
| `max_bytes` | int | 否 | 缺省 2MB；**2048–6MB** | records 消毒并按 max_bytes 分页 |

- **响应**：`{records, session_id, cursor, next_cursor, has_more, total}`；读异常降级空列表。

#### team.mq.publish

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是（envelope） | 取 request.session_id，params 内同名键无效 | — |
| `payload` | dict | 是 | `payload.type` **必须** `"team.external_event"` | 注入活跃 team runtime |

- **响应**：成功 `{published:true}`；失败 `ok:false {error}`（reason：session_id/payload 缺失、invalid_external_event、interact() 返回值），**无 code**。该连接不注册 GodView。

#### team.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 软必填 | 缺省回退请求 envelope 的 session_id；两者皆空 → 返回空列表**不报错** | 关联会话 |
- **响应**：`{session_id, teams: [{team_id, team_name, display_name, state(running|pending|configured), leader_id, organization_id, is_leader, capabilities, agent_group_name, source, is_owner}], default_team_id}`。恒 ok=true（异常降级空列表）。

### 5.3 group.* 方法（群聊组）

**语义区别**：`team.*` 面向 Team 实体/运行时（模板、绑定、看板）；`group.*` 面向**持久化群聊组**——group 的 `group_id` 本身是一个 team 模式的 session，成员是预编译的 teammate 规格。**有 ownership 校验**：`group.json` 的 `user_id`+`channel_id` 必须与请求一致，否则 `NOT_FOUND`（隐藏存在性）；持久化损坏 → `GROUP_CORRUPT`；`status=="deleting"` 时除 delete 外 → `GROUP_BUSY`。

#### group.create

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空；≤200 | 群名 |
| `description` | string | 否 | 缺省 `""`；可空；≤2000 | 描述 |
| `create_token` | string | 否 | 缺省 request_id；≤256；幂等键——同 token 内容不一致 → `CONFLICT` | 幂等令牌 |
| `members` | list[dict] | 否 | 缺省 `[]`；**≤100 项**；每项 key ⊆ {kind, definition_id, member_name, display_name}，多余键 → `BAD_REQUEST` | 成员规格 |
| `project_id` / `project_dir` / `model_name` | string | 否 | 缺省 `""`，透传 session 创建 | 项目/模型 |
| `view_id` | string | 否 | — | commit 上下文 scope |
- **响应**：`{group_id, session_id, team_name, name, description, members, created_at, updated_at, status:"ready"}`；响应投递失败 → `RESPONSE_TOO_LARGE`。
- **错误**：`BAD_REQUEST` / `CONFLICT` / `NOT_FOUND` / `GROUP_BUSY`（deleting 态）。

#### group.get / group.list / group.update / group.delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `group_id` | string | 是（get/update/delete） | = `params.group_id or request.session_id`，经 `validate_session_id`，非法 → `BAD_REQUEST` | 群 id |
| `limit` | int | 否（list） | 缺省 50；**严格 int**（bool 拒）；1–200 | 分页 |
| `offset` | int | 否（list） | 缺省 0；**严格 int**；≥0 | 偏移 |
| `name` | string | 条件（update） | 出现即校验：非空 ≤200 | 新名（update 仅接受 name/description，其它键 → `BAD_REQUEST`） |
| `description` | string | 条件（update） | 出现即校验：可空 ≤2000 | 新描述 |
- **响应**：get/update → 组公开字段；list → `{groups, total, limit, offset}`（按 updated_at 降序）；delete → `{group_id, deleted:true}`（deleting 态下 delete 可执行、其余方法 `GROUP_BUSY`；删除失败回滚 status="ready"）。

#### group.member.add

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `group_id` | string | 是 | 同上 | 群 id |
| `member` | dict | 是 | 须含 member_name；按成员字段白名单校验 | 成员规格 |
| `client_request_id` | string | 否 | 缺省 request_id；≤256 | 幂等收据（⚠️ 缺省失去幂等，重试仅靠 `ALREADY_EXISTS` 拦截） |
- **响应**：`{group_id, member, status:"registered", event_type:"group.member.added"}`。
- **错误**：`CONFLICT`（同收据不同成员）/ `ALREADY_EXISTS`（重名）/ `GROUP_FULL`（≥100）/ `GROUP_RESTART_REQUIRED`（运行中组需冷恢复）/ `MEMBER_REGISTRATION_FAILED` / `MODE_MISMATCH`（roster 遇非 group_chat 后端）。

#### group.history.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `group_id` | string | 是 | 同上 | 群 id |
| `limit` / `offset` | int | 否 | 同 group.list | 分页 |
- **响应**：`{group_id, messages, total, limit, offset, next_offset}`（末页 null）；scope 不匹配 → `GROUP_CORRUPT`。

**group 内 chat.send 约束**（服务端 `group_requests.py:88-109`）：`group_id` 必须等于 `session_id`——该前半校验已上移到**网关帧级**（`web_connect.py:2247-2252`）；`params.mode` 若携带必须为 team 模式（否则 `MODE_MISMATCH "group chat requires team mode"`）；`params.type` 仅允许缺省或 `"group_chat"`（否则 `MODE_MISMATCH "group chat requires group_chat input"`）；chat.send 进组后服务端强制补 `type="group_chat"`。

> ⚠️ **另 5 个方法（group.execution.list/get、group.file.rename/delete、group.history.clear）仅网关侧注册**：Gateway 不解析方法枚举，按字符串正常转发（网关侧透传已由 ST 验证）；但 **AgentServer 当前版本无 handler**（全仓 grep 无对应字符串），南向落入通用 fall-through——**业务契约当前未定义**，待 AgentServer 实现后补全。

### 5.4 org.snapshot

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：`session_id`（缺省回退 envelope）、`team_id`（缺省 ""——按会话解析 team backend）、`organization_id`（缺省 ""，**仅一致性校验**，不匹配 → 空 payload）。
- **响应**（恒 ok=true）：`{organization:{organization_id, display_name, description, owner_team_id, owner_leader_id, leaders[{..., capabilities, team_package_availability}], metadata, unclaimed_task_policy}, tasks(≤200, description 截 280 字符), unclaimed_tasks, pending_reviews(team_id 时 ≤50), stats:{total, open, unclaimed, pending_reviews, by_status}}`；backend 缺失/组织号不匹配/开关关闭 → 空态 `{organization:null, tasks:[], unclaimed_tasks:[], pending_reviews:[], stats:{}}`。WS 实现另有 `organization_enabled()` 开关与历史持久化组织回退。

### 5.5 swarmflow.pause / resume / stop

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **用途**：对一条 SwarmFlow run 执行暂停/恢复/停止。
- **网关前置校验**（§2.3）：`run_id` 与 `workflow_run_id` 同传必须相等且各自合法，否则 `BAD_REQUEST`。
- **AgentServer 侧**：`run_id` 缺失/非字符串 → `ok:false "run_id is required"`；session 无 RUNNING 状态 team runtime → `ok:false "工作流控制不可用，发送一条消息唤醒团队后即可使用。"`（团队休眠时按钮置灰）；执行成功 → `{run_id, status: "paused"|"resumed"|"stopped"}`；run 已终态/进程重启 → 结构化 miss payload（含权威 status）或 `"workflow run not found"`（`agent_ws_server.py:7339-7450`）。

### 5.6 team 会话的流式事件

team 模式 chat.send 仍产生常规 `chat.*` 流（leader 输出直接进主对话流），同时叠加 team 专属事件（均全量 payload 透传，§3.3）：

| 事件 | payload 结构 | 说明 |
|------|-------------|------|
| `team.member` | `{event_type, session_id, event: {type: spawned\|status_changed\|execution_changed\|restarted\|shutdown, member_id, ...}}` | 成员生命周期；spawned 带 `mode/role/name/cli_agent`，status 变更带 `old_status/new_status` |
| `team.task` | `{event_type, session_id, event: {task_id, member_id, status, title?, content?}}` | 看板任务事件；**status 由服务端统一收敛**（created→pending、claimed/started→in_progress、plan_request→planning、completed/verified→completed、submitted_for_review→in_review、cancelled→cancelled 等） |
| `team.message` | `{event_type, session_id, event: {type: "team.message.p2p"\|"team.message.broadcast", message_id, from_member, to_member?, content, protocol}}` | 成员间消息 |

**前端区分单 agent 流与 team 流**：

1. 事件名前缀 `team.` / `org.`；
2. 帧内 `agent_ref`：teammate 输出 → `{mode:"team", id: member_name}`；team.member/task → `{mode:"team", id: team_id}`；team.message → `{mode:"team", id: from_member}`（`team_helpers.py:737-765`）；
3. 内容流帧透传 `team_name`/`team_id`/`member_name`/`role`（teammate 帧 `role:"teammate"`，leader 帧 `role:"leader"`）；
4. teammate 的 reasoning 被丢弃；非 leader 的 `chat.ask_user_question` 被丢弃（`team_helpers.py:4487-4495`）。

**fan_out 多目标投递**：AgentServer 在 team 事件注入 `fan_out_targets`（`team_helpers.py:417-531`）：p2p 消息 → `[godview, mention(收件人), private(发送方)]`；广播 → `[godview, mention_all]`；teammate 输出 → `[godview, private(该 teammate)]`；其余（leader 输出、team.member/task）→ `[godview]`。Gateway 据此把同一事件投给 web 前端（godview）与各成员席位（IM 渠道 mention/private）。team 轮结束以 `chat.processing_status{is_processing:false}` 收尾（同 session 多流时最后一个流退出才广播）。

### 5.7 dispatch_mode / bootstrap_mode（会话级派发配置）

chat.send / session.create 可携带（`common/schema/chat_send.py:219-240`）：

| 参数 | 枚举 | 语义 |
|------|------|------|
| `team_dispatch_mode` | `autonomous`（任务上共享看板由成员认领）/ `scheduled`（leader 把每个任务指派给具名成员） | 会话的 team 建立后固定，**只有会话首请求能改**；缺省沿用会话持久值，首次回退 team 配置（默认 autonomous） |
| `team_bootstrap_mode` | `leader`（leader 用 build_team 自建团队）/ `framework`（运行时直接建通用团队） | **仅 dispatch_mode=scheduled 时合法**，否则请求边界拒绝 |

显式携带合法值时写回 session metadata（`session_team_dispatch_mode` / `session_team_bootstrap_mode`），刷新页面可恢复（`team_helpers.py:1209-1271`）。scheduled 派发下任务验收（`enable_task_verification`）才有效；非 scheduled 强制关闭（`team_manager.py:1816-1827`）。cron 触发的 team 会话不携带这两个键，沿用 team 配置或既有会话 metadata。

> ⚠️ **plan 模式与 SwarmFlow 的互斥只在前端**：前端开启 Plan 前自动关 Swarmflow、反之亦然（`InputArea.tsx:4449,4541-4543`）。**网关与 AgentServer 均无 `mode` 为 plan 变体 + `enable_swarmflow:true` 的组合校验**——同时携带时不会被拒绝，两种机制叠加生效（行为未定义）。协议层的正确约束方式：plan 变体与 `enable_swarmflow:true` 不要同传。

---

## 6. Session 会话接口

### 6.1 本地 handler + E2A 代理方法

#### session.list

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_LIST)；params 原样透传，无额外改动。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `limit` | int | 否 | 缺省 20；1–200 钳制（AgentServer 侧执行） | 分页大小 |
| `offset` | int | 否 | 缺省 0；≥0 | 偏移 |
- **响应**：`{sessions: [SessionInfo], total, limit, offset}`。SessionInfo 字段（`server/runtime/session/session_info.py:19-52`）：`session_id, title, created_at, last_message_at, message_count, mode, team_name, agent_group_name, pinned, pin_order, project_dir, project_id, cron_id, last_user_message_at, model, model_selection, moa_enabled, work_mode, lifecycle_operation, execution_blocked, stop_pending`。
- 归档/临时探针/ephemeral 会话不返回；按 `last_message_at` 倒序。网关为纯 E2A 薄代理（`app_web_handlers.py:3093-3113`）——**旧基线的"AgentServer 离线读本地目录 fallback"已删除**，不可达时回 `SERVICE_UNAVAILABLE`；limit/offset 钳制随 fallback 移到 AgentServer 侧。不支持 project 过滤（项目内会话用 `project.get_sessions`）。
- **错误**：元数据读失败 `INTERNAL_ERROR`；代理层 `SERVICE_UNAVAILABLE`/`AGENT_SERVER_TIMEOUT`。

#### session.create

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_CREATE, 缺省码 SESSION_CREATE_FAILED)；**drop 客户端 session_id**；注入 create_token（缺省 request_id）；经 _inject_session_work_mode 注入归一化后的 project_id/project_dir/work_mode + _work_mode_explicit 传输标记。
- **参数**：

| 参数 | 类型 | 必填 | 默认/校验 |
|------|------|------|----------|
| `mode` | string | 否 | 缺省 `"agent"`，归一化为 canonical（§5.1 枚举）后**持久化到会话 metadata**——`session.list`/`get_metadata` 的 `mode` 字段即它；Web 常发 `mode="team"`+`work_mode` 组合推导；会话登记模式不锁死每轮执行，chat.send 的 mode 仍独立生效 |
| `project_id` / `project_dir` | string | 否 | 项目绑定：两空或 `default`/`default_code` → 默认项目；仅传 id 自动补 dir；同传须与项目记录一致（否则 `BAD_REQUEST`）；project 不存在/隐藏 `NOT_FOUND`；`project_dir` 必须绝对路径 |
| `work_mode` | string | 否 | 按通道推断（web→`work`）；非法值/与项目记录不一致 `BAD_REQUEST`；**绑定后不可变** |
| `title` / `model_name` | string | 否 | 缺省均为 `""`（空标题 / 不指定模型） |
| `model_selection` | dict | 否 | 缺省 null（不设置会话级模型选择）；`ModelSelection` 校验（§6.3） |
| `moa_enabled` / `persist_session` | bool | 否 | 非布尔 `BAD_REQUEST`；`persist_session` 对已存在会话不可变更（`CONFLICT`） |
| `previous_session_id` / `cron_id` | string | 否 | 缺省均为 `""`；前台切换信号 / 定时任务会话 |
| `session_id` | string | **禁止**（web） | 显式传入报错，恢复用 `session.switch`（仅 TUI 兼容路径允许） |

- **响应**：`{sessionId, session_id, projectId, projectDir, workMode, persist_session, moa_enabled, prewarm_hit, prewarm_status}`。
- **错误**：`BAD_REQUEST`/`NOT_FOUND`/`CONFLICT`（SessionProvisionError code）；网关层 default `SESSION_CREATE_FAILED`。
- 无广播事件，仅 res 帧。

#### session.get_metadata

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_GET_METADATA)；params 原样；网关收到响应后**注入 is_processing**（busy 映射）+ cron 活动叠加。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空；缺省回退帧级 session_id | 目标会话 |
- **响应**：metadata.json 全量字段（`session_id, channel_id, user_id, created_at, last_message_at, title, message_count, mode, team_name, team_template_id, project_dir, project_id, persist_session, model, moa_enabled, cron_id, pinned, pin_order, status, work_mode` 等）+ 网关注入的 `is_processing`（busy 映射）+ AgentServer 注入的 `queued_session_messages` / `can_continue_queued_session_messages`（跨会话信箱，§4.10）。
- **错误**：`BAD_REQUEST`（无 session_id）、`NOT_FOUND`、归档会话 `SESSION_ARCHIVED`。

#### session.rename

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_RENAME)；params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空；缺省回退帧级 session_id | 目标会话 |
| `title` | string \| null | 否 | 缺省不携带=查询；空串/纯空白=清除；非空=设置（strip 后截断 200 字符） | 三语义标题 |
- **响应**：`{session_id, title, previous_title}`。会话不存在时**不报错**（自动补建 metadata）；归档会话 `SESSION_ARCHIVED`。

#### session.pin

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_PIN)；params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空 | 目标会话 |
| `pinned` | bool | 是 | **严格布尔**（isinstance bool，字符串拒绝） | 置顶/取消 |
- **响应**：`{pinned, pin_order}`（非置顶恒 0）。幂等；操作后全部置顶会话紧凑重编号 1..N。
- **错误**：`BAD_REQUEST`、`NOT_FOUND`。

#### session.plan_status

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_PLAN_STATUS)；params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空 | 目标会话 |
- **响应**：`{session_id, in_plan: bool}`（只读；团队会话看 metadata.mode + 进程内状态，单 agent 读 live 实例）。

#### session.message.continue_queued

> **网关处理**：**E2A 代理**——proxy_unary_request(SESSION_MESSAGE_CONTINUE_QUEUED)；params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空 | 要恢复排队消息的会话 |
- **用途**：用户确认后恢复投递到该会话的排队跨会话消息（信箱）。
- **响应**：`{target_session_id, released_unknown_count, accepted: true}`。
- **错误**：`INVALID_ARGUMENT` / `HOST_CAPABILITY_UNAVAILABLE` / `NOT_FOUND` 等（SessionMessagingError code）。

### 6.2 白名单转发方法

#### session.switch

> **网关处理**：**透传**——白名单转发，params 原样（仅 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | strip 非空 | 切换目标 |
| `previous_session_id` | string | 否 | 缺省不携带——不上报信号，不影响切换 | 前一前台会话信号 |
| `mode` | string | 否 | 缺省 `"agent.plan"`，归一化 | 会话模式 |
| `previous_mode` | string | 否 | 缺省不携带——不上报 | 前一模式信号 |
| `team` | bool | 否 | 缺省 false | 团队提示标志 |
| `view_id` | string | 否 | 缺省不携带——不上报 | 前台视图 scope |
- **响应**：`{session_id, mode, switched: bool, moa_enabled}`（先 commit 再回响应）。
- **错误**：`BAD_REQUEST`；目标归档 `SESSION_ARCHIVED` 等 SessionProvisionError code。

#### session.fork

> **网关处理**：**透传**——白名单转发，params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `source_session_id` | string | 是 | strip 非空 | 源会话 |
| `target_session_id` | string | 否 | 缺省服务端生成；已存在 → `ALREADY_EXISTS` | 新会话 id |
| `title` | string | 否 | **缺省继承源会话标题** | 新标题 |
| `fork_point` | dict | 否 | `{message_id\|role\|content\|timestamp}` 可选；**缺省不截断**（全量复制） | 截断点 |
| `session_equipment_override` | dict | 否 | 缺省沿用源会话装备 | 装备覆盖 |
| `side_conversation` | — | — | 出现即 `BAD_REQUEST` | 已废弃 |
- **响应**：`{session_id(新), source_session_id, title, moa_enabled}`。
- **错误**：`BAD_REQUEST` / `NOT_FOUND` / `ALREADY_EXISTS`。

#### session.assets.list / register / rename

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **用途**：会话级命名素材表（`@name` 引用），落盘 `<sessions>/<session_id>/session_assets.json`。
- **list**：`{assets: [{asset_id, name, kind(image|video|audio|document), path, source(upload|generated), created_at}]}`。
- **register**：`items`（必填，每项 `{path, name?, source?}`）；按 path 去重、文件必须存在、名称自动清洗（`@`/换行/tab）与去重（`" 2"`/`" 3"` 后缀，≤60 字符）。
- **rename**：`asset_id` + `name`（非空/≤60/无非法字符）。
- **错误**：`INVALID_PARAMS` / `INVALID_NAME` / `ASSET_NOT_FOUND` / `NAME_CONFLICT`。

### 6.3 session.selection.set（模型选择持久化）

> **网关处理**：**E2A 代理**——_register_config_proxy(SESSION_SELECTION_SET)；**A4g 统一 E2A**（企业版也走）。
- **参数**：`session_id`（string，缺省回退帧级）、`model_selection`（dict，必填）。
- **ModelSelection schema**（`common/model_selection.py:10-38`，pydantic `extra="forbid"`）：

| 字段 | 类型 | 必填 | 校验 |
|------|------|------|------|
| `type` | `"model"` \| `"model_group"` | 是 | — |
| `id` | string | 是 | strip 后非空 |
| `route_id` | string \| null | 否 | 仅 `model_group` 时允许；指定组内路由；`type:"model"` 时携带报错；缺省 null（解析模型组默认路由） |

- **响应**：`{model_selection: {type, id, route_id}}`。统一经 E2A 转发目标 AgentServer（A4g 起个人版不再回落网关本地 handler）。
- **错误**：目标为 **cron 会话时拒绝 `model_group`** 选择（"Scheduled task sessions only support a single model"，`BAD_REQUEST`）；解析失败返回**模型域错误码**（`MODEL_SELECTION_NOT_FOUND` 等，经 ModelSelectionError 透传），非通用 BAD_REQUEST；schema 校验失败 → pydantic 异常文本 + `BAD_REQUEST`。

### 6.4 生命周期方法与事件（lifecycle）

Gateway 注册（`lifecycle_handlers.py:164-172`）：`session.archive` / `session.unarchive` / `session.archived.list` / `session.delete` / `project.sessions.archive` / `project.sessions.delete_archived`。

#### session.archive / session.unarchive

> **网关处理**：**E2A 代理**——lifecycle_handlers 代理转发（session_id/session_ids 透传）；单条/批量分支在 AgentServer 侧，网关响应壳仅透传。

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 与 session_ids 二选一 | strip 非空 | 单会话目标 |
| `session_ids` | list[string] | 与 session_id 二选一 | 数组 **1–100** | 批量目标 |
- **响应**：单条 `{session_id, ok:true, archived:true, archived_at, project_id}` / `{session_id, ok:true, restored:true, project_id}`；批量 `{succeeded_count, failed_count, results[]}`。

#### session.delete

> **网关处理**：**E2A 代理**——lifecycle_handlers 代理转发；AgentServer 侧执行停止运行→fence 写入→物理删除目录。

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 与 session_ids 二选一 | strip 非空 | 单条 |
| `session_ids` | list[string] | 与 session_id 二选一 | 1–100 | 批量 |
- **响应**：`{session_id, ok:true, project_id}`；停止运行 → fence 写入 → 物理删除目录。

#### session.archived.list

> **网关处理**：**E2A 代理**——lifecycle_handlers 代理转发；limit 1–200 / offset 钳制在 AgentServer 侧执行。

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `limit` | int | 否 | 缺省 20；**1–200** | 分页 |
| `offset` | int | 否 | 缺省 0；≥0 | 偏移 |
| `work_mode` | string | 否 | 枚举 work\|code | 形态过滤 |
| `keyword` | string | 否 | 缺省不过滤 | 关键词 |
| `project_id` | string | 否 | 缺省不过滤 | 项目过滤 |
- **响应**：归档会话分页。

#### project.sessions.archive / project.sessions.delete_archived

> **网关处理**：**E2A 代理**——lifecycle_handlers 代理转发；批量归档在 AgentServer 侧执行，网关 watcher 轮询完成态并广播事件。

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目目标 |
- **响应**：`{project_id, succeeded_count, failed_count, results[]}`。

- 归档目录为 `agent/sessions_archived/`；unarchive 时所属项目被移除则连带恢复项目。
- **错误**：cron/heartbeat 会话禁止单独归档（`FORBIDDEN`）；忙碌 `SESSION_BUSY`（附 finishing 详情）；归档幂等。
- **事件**（Gateway 合成并广播）：`session.archived` / `session.unarchived` / `session.deleted` / `session.lifecycle.updated` / `project.lifecycle.updated` / `project.removed` / `project.restored`。跨 tab 感知靠 Gateway 的 2s 生命周期轮询 watcher（`lifecycle_handlers.py:27-84`）。
- **guard 准入闸门**（AgentServer 入站）：除白名单（`session.list`、`project.list/info/get_sessions/get_cron_sessions/pinned_sessions`、`chat.cancel`、`session.stop` 等）外所有请求先过 guard——归档会话 `SESSION_ARCHIVED`、已删除 `NOT_FOUND`、生命周期操作进行中 `OPERATION_IN_PROGRESS`（`server/lifecycle.py:389-419`）。

### 6.5 initialize

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
前端连接后可选的首个握手请求（无本地 handler，纯转发）。

- **参数**：

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `protocolVersion` | string | 否 | 缺省 `"0.1.0"` | 协议版本（extra_config） |
| `clientCapabilities` | dict | 否 | 缺省 `{}` | 客户端能力声明 |
- **响应**：`{protocolVersion: 1, agentInfo: {name:"jiuwenswarm", title:"JiuwenSwarm", version}, agentCapabilities: {loadSession:false, promptCapabilities, sessionCapabilities, mcpCapabilities}, authMethods: []}`（`common/e2a/acp/protocol.py:8-32`）。

---

## 7. Project 项目接口

### 7.1 project.* 管理方法

项目记录（projects.json）：`project_id`(32hex)、`name`、`project_dir`、`pinned`、`pin_order`、`hidden`、`work_mode`、`git`。除 `project.remove` 为 Gateway 编排外均 E2A 直转；`project.list/info/get_sessions/get_cron_sessions/pinned_sessions` 免 lifecycle guard。

以下均为 **E2A 代理**（网关构造请求转发；`project.remove` 为网关编排）。逐方法参数表：

#### project.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `filter` | string | 否 | 缺省 `all`；枚举 all\|pinned\|unpinned | 置顶过滤 |
| `work_mode` | string | 否 | 缺省不过滤；非法 `BAD_REQUEST` | 形态过滤 |
| `include_hidden` | bool | 否 | 缺省 false | 含隐藏项目 |
- **响应**：`{projects:[ProjectInfo]}`——默认项目按 work_mode 拆为虚拟项 `default`/`default_code`；置顶区在前，非置顶按最近消息倒序；ProjectInfo 含 `session_count`（仅计非置顶普通 web 会话）、`git{...}`、`execution_blocked`、`stop_pending` 等。

#### project.info

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目 id |
| `include_hidden` | bool | 否 | 缺省 false | 允许命中隐藏项目 |
- **响应**：`{project: ProjectInfo, **ProjectInfo}`（双写）；不存在/隐藏 `NOT_FOUND`。

#### project.get_sessions

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目 id |
| `work_mode` | string | 否 | 缺省不过滤 | 形态过滤 |
| `limit` | int | 否 | 缺省返回全部 | 分页 |
| `offset` | int | 否 | 缺省 0 | 偏移 |
- **响应**：`{sessions, total}`；排除置顶与 cron 会话，按最近用户消息倒序。

#### project.get_cron_sessions

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目 id |
| `cron_id` | string | 否 | 缺省不过滤 | 按任务过滤 |
| `limit` / `offset` | int | 否 | 缺省全部 / 0 | 分页 |
- **响应**：`{sessions, total}`；返回 cron 执行会话（带 `cron_id` 或 `channel_id=="__cron__"`）。

#### project.pinned_sessions

- **参数**：无。**响应**：`{sessions}`；全部置顶会话（跨项目），按 pin_order 升序。

#### project.create

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 非空、无控制字符、无 `<>:"/\\|?*`、非 Windows 保留名、非全点/空格、≤200 字符 | 项目名 |
| `project_dir` | string | 否 | 须**绝对路径**；目录不存在 `PROJECT_DIR_MISSING` | 自定义目录（缺省 `~/.jiuwenswarm/agent/workspace/{work\\|code}/<name>`） |
| `work_mode` | string | 否 | 枚举 work\|code | 形态 |
- **响应**：`{project_id, project_dir, restored, work_mode, git, project}`；目录命中隐藏项目 → 恢复（restored:true）；命中可见项目/同名冲突 `CONFLICT`。

#### project.rename

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 默认项目 `FORBIDDEN` | 目标 |
| `name` | string | 是 | 同 create 的 name 校验 | 新名 |
- **响应**：`{project_id, name, work_mode}`。

#### project.pin

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 默认项目 `FORBIDDEN` | 目标 |
| `pinned` | bool | 是 | **严格布尔** | 置顶/取消 |
- **响应**：`{pinned, pin_order}`；置顶区紧凑重编号。

#### project.remove

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 默认项目 `FORBIDDEN` | 软删除目标 |
- **行为**：立即受理（不再预检运行中会话，AgentServer 隐藏项目并后台停会话，`app_web_handlers.py:3479-3501`）→ 停该项目 cron（失败 `CRON_STOP_FAILED`）→ `{project_id, hidden:true, affected_sessions, stopped_cron_jobs}`；幂等；广播 `project.removed`（按 user_id 收窄）。

#### project.restore

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 恢复目标 |
- **响应**：`{project_id, restored:true, work_mode, affected_sessions}`；未隐藏 `CONFLICT`；同名被占 `PROJECT_NAME_CONFLICT`；广播 `project.restored`。

### 7.2 project.git.*（主 WS 上的 Git 操作）

均为 E2A 直转 + `preserve_error_payload=True`（失败 payload 含结构化 `detail`）；写操作成功后唤醒 `/ws/git` watcher（`mark_dirty`）。**前置校验**（所有方法，AgentServer 侧）：`project_id` 必填；默认项目 `FORBIDDEN`；work 模式项目 `FORBIDDEN`；不存在 `NOT_FOUND`/`PROJECT_NOT_FOUND`（`server/runtime/session/project_git.py:187-216`）。

#### project.git.status / probe

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
- **响应**：GitStatus 快照 `{project_id, project_name, project_dir, work_mode, repo{is_git, repo_root, branch, head, detached, transient, upstream}, working_tree{is_dirty, staged, unstaged, untracked, conflicted}, branches{current, locals, remotes}, generated_at}`。

#### project.git.init

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `initial_branch` | string | 否 | 缺省 `"main"` | 初始分支名 |
- **响应**：同 status。

#### project.git.switch_branch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `branch` | string | 是 | strip 非空 | 目标分支 |
| `require_clean` | bool | 否 | 缺省 false | true 时工作区不干净拒绝切换 |
- **响应**：`{switched:true, previous_branch, current_branch, status}`。

#### project.git.create_branch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `branch` | string | 是 | strip 非空；分支名须合法 | 新分支名 |
| `checkout` | bool | 否 | 缺省 true | 创建后切换 |
| `start_point` | string | 否 | 缺省 HEAD | 起点 |
- **响应**：`{created:true, checked_out, branch, status}`。

#### project.git.commit

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `message` | string | 是 | 非空 | 提交信息 |
| `paths` | list[string] | 与 stage_all 二选一 | — | 指定路径 |
| `stage_all` | bool | 与 paths 二选一 | **严格布尔** | 全量暂存 |
| `amend` | bool | 否 | 缺省 false | 修正上一提交 |
| `no_verify` | bool | 否 | 缺省 false | 跳过钩子 |
- **响应**：`{committed:true, commit_hash, amended, status}`。

#### project.git.push

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `remote` | string | 否 | 缺省 `"origin"` | 远程名 |
| `branch` | string | 条件必填 | detached HEAD 必传 | 分支 |
| `set_upstream` | bool | 否 | 缺省 false；**严格布尔** | 建立上游 |
| `force` | bool | 否 | 缺省 false；**严格布尔**；用 `--force-with-lease` | 强推 |
| `delete` | bool | 否 | 缺省 false；**严格布尔**；与 set_upstream/force 互斥 | 删远程分支 |
- **响应**：`{pushed:true, remote, branch, deleted, upstream_set, status}`；push 超时独立配置（默认 60s，env `GIT_PUSH_TIMEOUT_SEC`）。

#### project.git.diff_status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `session_id` | string | 否 | — | 关联会话（last_turn 定位） |
| `include_files` | bool | 否 | 缺省 false | 附文件列表 |
| `include_hunks` | bool | 否 | 缺省 false（隐含 include_files） | 附 hunk |
| `hunk_paths` | list[string] | 否 | — | 指定路径 |
- **响应**：diff 状态快照；transient 仓库时 `current:null, repo.transient:true` 仍成功。

#### project.git.turn_diff_list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `session_id` | string | 是 | 会话须属于该项目（`SESSION_NOT_FOUND`/`SESSION_NOT_BOUND`/`PROJECT_SESSION_MISMATCH`） | 关联会话 |
| `limit` | int | 否 | 缺省 50 | 条数 |
| `cursor` | string | 否 | — | 分页游标 |
- **响应**：历史 diff 摘要列表。

#### project.git.turn_diff

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | 前置校验见上 | 项目目标 |
| `session_id` | string | 是 | 同上 | 关联会话 |
| `change_set_id` | string | 与 turn_index 二选一 | 未命中 `CHANGE_SET_NOT_FOUND` | 变更集 id |
| `turn_index` | int | 与 change_set_id 二选一 | 未命中 `TURN_DIFF_NOT_FOUND` | 轮次序号 |
| `include_files` / `include_hunks` | bool | 否 | 缺省 false | 附件/hunk |
- **响应**：指定轮次 diff 详情；历史过期 `DIFF_HISTORY_EXPIRED`。

### 7.3 /ws/git 独立协议

`ws://<host>:<port>/ws/git`，与主 WS 同握手鉴权链路，但 **method 集合封闭为 6 个**，未知方法回 `BAD_REQUEST`（而非 METHOD_NOT_FOUND）。事件推送 `seq`/`stream_id` 恒 null。所有写方法带 `preserve_error_payload=True`；**前置 busy 检查**在网关（§4.9.7 busy 映射）。

#### project.git.diff_watch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目目标 |
| `session_id` | string | 是 | strip 非空 | 关联会话（last_turn） |
| `scope` | string | 否 | 仅 `"summary"` | 快照范围 |
| `include_last_turn` | bool | 否 | 缺省 true | 附上一轮 |
- **首响**：`{watch_id, scope:"summary", snapshot:{repo, current, last_turn, revision}}`；变化推 `project.git.diff_changed`。

#### project.git.diff_files_watch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `watch_id` | string | 是 | diff_watch 返回的 id | 订阅 id |
| `source` | string | 是 | 枚举 `current`\|`last_turn` | 数据源 |
- **首响**：`{watch_id, files_scope, revision, files}`（无 hunk）；推 `project.git.diff_files_changed`。

#### project.git.diff_detail_watch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `watch_id` | string | 是 | — | 订阅 id |
| `source` | string | 是 | 枚举 `current`\|`last_turn` | 数据源 |
| `files` | list[string] | 是 | 非空字符串数组 | 订阅文件 |
- **首响**：`{watch_id, detail_scope, revision, files}`（含 hunk，未订阅路径为 null）；推 `project.git.diff_detail_changed`。

#### project.git.diff_unwatch

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `watch_id` | string | 是 | — | 订阅 id |
| `scope` | string | 是 | 枚举 `all`\|`files`\|`detail` | 取消范围 |
- **响应**：`{watch_id, cancelled:true, scope}`（幂等）。

#### project.git.discard_turn_changes / redo_turn_changes

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 是 | strip 非空 | 项目目标 |
| `session_id` | string | 是 | strip 非空 | 关联会话 |
- **前置 busy 检查**：会话忙碌 → `SESSION_BUSY "session is busy; stop the current run before discarding/redoing changes"`。
- **响应（discard）**：`{session_id, turn_index, change_set_id, restored_files[], deleted_files[], errors[], partial}`；错误码 `NO_TURN_TO_DISCARD`/`NOTHING_TO_DISCARD`/`TURN_TARGET_CHANGED`/`DIFF_HISTORY_EXPIRED`/`PARTIAL_RESTORE_FAILED`。
- **响应（redo）**：同构；错误码 `NO_TURN_TO_REDO`/`NOTHING_TO_REDO`/`REDO_HISTORY_MISSING`/`PARTIAL_REDO_FAILED`。

监控过程 Git 命令失败推 `project.git.error` 事件；首次快照失败保留 AgentServer 结构化 code（如 `NOT_GIT_REPOSITORY`）。


---

## 8. 定时任务接口（cron / heartbeat / schedule）

三套体系**相互独立**：`cron.job.*`（通用定时任务，Gateway 本地实现+进程内调度）、`heartbeat.job.*`（心跳续跑任务，AgentServer Rail 运行时持有、Gateway 代理）、`schedule.*`（AutoHarness 演化管线任务，转发 AgentServer）。

### 8.1 cron.job.*（Gateway 本地，注册于 `app_web_handlers.py:5374-5382`）

通用：`cron_controller` 不可用 → `INTERNAL_ERROR "cron not available"`；单 job 操作先过 `_get_owned_cron_job` 鉴权（§8.1.10）。

#### 8.1.1 cron.job.list


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.list", "params": {"project_id": "default"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"jobs": []}}
```


- **参数**：

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `project_id` | string | 否 | 缺省不过滤——返回当前用户全部可见任务；支持虚拟项目 `default`/`default_code`（精确匹配），或空 project_id 的 job 按 work_mode 消歧 | 项目过滤 |
- **响应**：`{jobs: [CronJob]}`。**按连接 user_id 过滤**：带 user_id 的连接只见 `job.user_id` 精确相等的任务；user_id 为空（单用户）返回全部；隐藏项目任务不出现在列表；企业版每个 job 附 `active_session_id`。

#### 8.1.2 cron.job.meta


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.meta", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"modes": ["agent", "team"], "default_mode": "agent", "default_timeout_seconds": 3600, "default_team_timeout_seconds": 3600, "max_timeout_seconds": 259200}}
```


- **参数**：无。
- **响应**：`{modes: [...CRON_JOB_MODES 排序], default_mode: "agent", default_timeout_seconds: 3600, default_team_timeout_seconds: 3600, max_timeout_seconds: 259200(72h)}`（`runtime/cron/models.py:172-185`）。

#### 8.1.3 cron.job.get


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.get", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "name": "每日日报", "enabled": true, "cron_expr": "0 9 * * *", "mode": "agent"}}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | 经 `str()` 强转 | 任务 id |
- **响应**：`{job: CronJob}`。**错误**：`NOT_FOUND "job not found"`（不存在或**不属于当前用户**——不泄露存在性）。

#### 8.1.4 cron.job.create

> **网关处理**：**纯本地**——controller.create_job（校验 + 项目绑定经 AgentServer 解析 + 落 store）；返回 {job:CronJob}；session_id 被覆盖为当前 web 会话。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.create", "params": {"name": "每日日报", "cron_expr": "0 9 * * *", "description": "总结今天的工作", "targets": "web", "mode": "agent"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<hex32>", "name": "每日日报", "enabled": true}}}
```

- **参数**（CronJob 字段，`runtime/cron/models.py:385-439`）：

| 字段 | 类型 | 必填 | 默认/校验/枚举 |
|------|------|------|---------------|
| `name` | string | 是 | trim 非空、≤64 字符 |
| `cron_expr` | string | 是 | 5 字段（min hour dom month dow）或 7 字段 Quartz（秒开头）；5 字段自动补秒/年；`croniter.is_valid` + 以 job 时区当前时间试算 |
| `timezone` | string | 否 | `"Asia/Shanghai"`；IANA 时区，非法 `BAD_REQUEST` |
| `description` | string | 是 | **即运行时 prompt**；trim 非空、≤500 字符；含"每天/每周…"等措辞时归一为 name |
| `enabled` | bool | 否 | `true` |
| `targets` | string | 视配置 | 推送渠道枚举：`web/tui/feishu/whatsapp/wecom/xiaoyi/wechat/dingtalk` 或 `feishu_enterprise:<app_id>` |
| `session_id` | string | 否 | web handler **无条件覆盖为当前 web 会话 id** |
| `mode` | string | 否 | `"agent"`；枚举 `CRON_JOB_MODES`（agent、team、code、三段式、`proactive.tick` 等；legacy 别名归一） |
| `wake_offset_seconds` | int | 否 | 0；≥0（负数归 0） |
| `delete_after_run` | bool | 否 | false（执行一次后自动删除，提醒类） |
| `timeout_seconds` | int | 否 | 3600；60 ≤ v ≤ 259200 |
| `model_name` | string | 否 | AgentServer 默认模型；经 config 自配 → Zen 免费缓存 → 登录免费模型解析；未知模型报错（附前 20 个可用模型） |
| `model_selection` | dict | 否 | ModelSelection 校验 + 解析 |
| `mcp` | list[string] | 否 | strip/去空/去重，不校验存在性（断连降级） |
| `project_id` / `project_dir` / `work_mode` | string | 否 | 项目归属解析（显式 id > (work_mode, dir) > 默认项目）；`work_mode` 枚举 `code/work`，design 拒绝 |
| `id` | string | 否 | `uuid4().hex`（显式指定用） |
| `user_id` / `_auth_session` | — | 服务端注入 | 连接 user_id / 登录会话（客户端传值被覆盖） |

**字段详解**（`runtime/cron/models.py`、`runtime/cron/controller.py`）：

- **`cron_expr`**：5 字段形式为 `分 时 日 月 周`，如 `"0 9 * * *"` = 每天 09:00、`"30 8 * * 1-5"` = 工作日 08:30；7 字段 Quartz 形式以**秒**开头，如 `"0 0 9 * * ? *"`。5 字段自动补 `0` 秒与 `*` 年。校验 = `croniter.is_valid` + 以 job 时区当前时间实际试算下一次触发（试算失败即 `BAD_REQUEST`，能挡住"2 月 30 日"这类语法合法但永不触发的表达式）。
- **`description`**：双重身份——既是面板展示的任务描述，也是**运行时作为 chat.send 正文发送的 prompt**（`scheduler.py:1531-1532`）。含"每天/每周/每月/上午/下午/早上/晚上/凌晨"等措辞且 name 为纯任务名时，会被归一为 name（用户口语式输入的兜底）。
- **`targets`**：执行**结果**的推送渠道（不影响执行本身——执行恒在 AgentServer 完成）：`web` 推送到该用户浏览器（带 `cron` 块的 `chat.final` 事件，§E.10）；`feishu`/`dingtalk` 等推送到对应 IM；`feishu_enterprise:<app_id>` 精确到飞书多应用。controller 未配置 target_channel 且 targets 为空时报错。
- **`mode`**：决定执行形态——`agent` 系（单 agent 流，执行渠道 `__cron__`）/ `team` 系（team 流式执行，结果按 targets 直播）/ `proactive.tick`（不走 chat，触发主动推荐引擎，由配置开关自动创建删除，面板拒绝删除）。
- **`wake_offset_seconds`**：提前唤醒秒数——agent 在 `push_at - offset` 时刻开始执行，到 `push_at` 时刻推送结果（长任务预留执行窗口）。
- **`delete_after_run`**：一次性提醒语义——下一次执行完成后自动删除 job；无下次触发点时 job 标记 `expired:true` 并停用。
- **`model_name`**：解析链 = config.yaml 自配模型 → Zen 免费缓存 → 登录免费模型；别名会解析为 canonical 名；未知模型报错并附前 20 个可用模型名。登录模型需要连接的登录会话有效，否则 `BAD_REQUEST "该模型需要登录华为账号后使用"`。
- **`mcp`**：本轮执行启用的 MCP 服务器名单；只做格式清洗**不校验存在性**（服务器断连时降级运行，不阻断任务）。
- **`project_id` / `project_dir` / `work_mode`**：三者决定任务归属项目与执行目录——显式 `project_id`（校验存在且未隐藏）优先；否则按 `(work_mode, project_dir)` 解析；都缺省归默认项目。匹配不到归默认项目，非绝对路径的 `project_dir` 报错；`work_mode` 传 `design` 拒绝。
- **`timeout_seconds`**：单次执行上限，超时中断；到达上限前 AgentServer 会先收到取消。
- **`session_id`**：仅作 IM 渠道回发身份解析的线索（从绑定的会话找 chat_id）；web 创建时被 handler 无条件覆盖为当前会话，cron 执行**不**复用该会话（每次触发预建独立执行会话，§8.1.10）。

- **响应**：`{job: CronJob}`。**错误**：`BAD_REQUEST`（一切校验异常，含项目执行闸门 `OPERATION_IN_PROGRESS`）、`SERVICE_UNAVAILABLE`（AgentOS 项目绑定解析失败）、`INTERNAL_ERROR`。
- CronJob 响应体（`to_dict`，`runtime/cron/models.py:433-439`；数据结构已 protocol 化到 `e2a/cron_models.py`）：恒有 `id, name, enabled, expired, cron_expr, timezone, wake_offset_seconds, description, targets, created_at, updated_at, project_id, work_mode, user_id, group_id, bot_id`（group/bot 为企业多租户身份，服务端注入）；条件输出 `session_id, chat_type, mode, delete_after_run, timeout_seconds, last_session_id, model_name, model_selection, mcp, app_id, credential_ref, next_run_at`。

#### 8.1.5 cron.job.update


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.update", "params": {"id": "<job_id>", "patch": {"enabled": false}}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "enabled": false}}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转；不存在/非本人 → `NOT_FOUND` | 任务 id |
| `patch` | dict | 是 | 可改字段：name/cron_expr/timezone/description/enabled/wake_offset_seconds/targets/mode/model_name/model_selection/mcp/session_id/project 字段（身份字段不可改——sticky 剥离）；work_mode 单独 patch / 无 marker 的项目字段 patch 被拒 | 补丁 |
- **响应**：`{job}`。cron 变更时重新试算；model 变更时重算凭据绑定。**错误**：`NOT_FOUND`/`BAD_REQUEST`/`SERVICE_UNAVAILABLE`。

#### 8.1.6 cron.job.delete


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.delete", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"deleted": true}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转 | 任务 id |
- **行为**：停用 → 取消在途执行（并发 CHAT_CANCEL）→ 经 E2A 删除该 job 的执行会话（失败抛带 code 的 RuntimeError，如 `SESSION_BUSY`；**企业版跳过会话级联**只删 store 记录）→ 删 store。`proactive.tick` 内置任务拒绝面板删除（`BAD_REQUEST`）。
- **响应**：`{deleted: true}`。

#### 8.1.7 cron.job.toggle


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.toggle", "params": {"id": "<job_id>", "enabled": false}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "enabled": false}}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转；`proactive.tick` 拒绝 | 任务 id |
| `enabled` | bool | 是 | **严格布尔**（None 拒绝） | 目标状态 |
- **响应**：`{job}`（内部即 update enabled）。enable 时复查项目闸门。

#### 8.1.8 cron.job.preview


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.preview", "params": {"id": "<job_id>", "count": 5}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"next": [{"wake_at": "2026-11-11T09:00:00+08:00", "push_at": "2026-11-11T09:00:00+08:00"}]}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转 | 任务 id |
| `count` | int | 否 | 缺省 5；夹取 **1–50** | 预览条数 |
- **响应**：`{next: [{wake_at, push_at}, ...]}`（ISO 时间，以 job 时区计算；一次性任务无下次触发时提前截断）。

#### 8.1.9 cron.job.run_now


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.run_now", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"accepted": true, "run_id": "<job_id>:1760600100", "session_id": "sess_cron_01"}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转 | 任务 id |
- **行为**：**拒绝停用与过期任务**（`enabled=false` → "任务已暂停，无法执行"、`expired` → "任务已过期，无法执行"，`BAD_REQUEST`；`proactive.tick` 例外，`cron/scheduler.py:1519-1533`）；立即预分配**真实执行会话**（复用 wake 阶段，非 proactive 模式响应即带真实 session_id）；`run_id = "<job_id>:<unix秒>"`。企业版手动触发遇审批提问先挂起（复用 pending 会话）。
- **响应**（对齐 chat.send 语义）：`{accepted: true, run_id, session_id}`；企业审批挂起复用时另带 `awaiting_approval: true, reused_pending_session: true`。

#### 8.1.10 cron 权限与内部行为

- **owner**：`CronJob.user_id`（创建者，不可变；空串=agent 工具创建/历史数据）。单 job 操作（get/update/delete/toggle/preview/run_now）全部经 `_get_owned_cron_job`——认证连接访问他人或无主历史 job 一律 `NOT_FOUND`。
- **项目闸门**：隐藏项目不放行（不到点触发、不可手动启用/立即执行）；`project.remove` 级联停用其下全部任务（不按 user_id 过滤），恢复后默认停用。
- **执行模型**：每次触发**预建独立执行会话**（`session.create` 带 `cron_id`、`create_token="cron:<run_id>"`），随后以 `chat.send`（强制流式）发送 job.description；单 agent 模式执行渠道固定 `__cron__`，team 模式走 team 流并把结果按 `targets` 渠道回传。中断类事件（ask_user_question 等）无人可答，立即失败收场。
- **结果推送**：执行完成以带 `cron` 块的 `chat.final` 事件推送（按 `payload.user_id` 限定接收者，支持关 tab/换设备后重连收取；企业版恒按连接 user_id 过滤）；**web 渠道不再推占位"正在执行中"**（占位仅 IM 渠道，`cron/scheduler.py:3393-3399`）。
- **崩溃恢复**：boot 后 60s 宽限期内已过期的 wake 跳过重排（宁丢结果不重复执行）。
- **持久化**：`~/.jiuwenswarm/agent/home/cron_jobs.json`（文件锁）；HA 可选 etcd 后端。

### 8.2 heartbeat.job.*（AgentServer Rail 运行时，Gateway 代理）

与 cron.job.* 的响应壳对齐。Gateway 未绑 AgentServer → `SERVICE_UNAVAILABLE "heartbeat not available"`；AgentServer 心跳调度器未就绪同样 `SERVICE_UNAVAILABLE`。**与 cron 的核心差异**：session 作用域（非 owner 作用域）、禁止修改运行配置字段、run_now 可拒绝、cancel 只停当次运行。

#### job 字段（HeartbeatJob，`agents/harness/code/rails/heartbeat/models.py:341-529`）

| 字段 | 类型 | 必填(create) | 默认/校验/枚举 |
|------|------|------------|---------------|
| `name` | string | 是 | ≤64 字符 |
| `channel_id` / `session_id` | string | 服务端注入 | 固定 `"web"` / 当前会话——**Web RPC 只能绑定当前 session**，跨 session 迁移需独立管理入口 |
| `prompt` | string | 是 | ≤2000 字符（任务提示词） |
| `schedule` | dict | 是 | `{type: "interval"\|"cron"\|"once"}`；interval：`interval_seconds` ≥60；cron：`cron_expr`（同 cron 校验）+ `timezone`；once：`run_at`（Unix **秒**，须未来） |
| `enabled` | bool | 否 | true（严格布尔） |
| `concurrency_policy` | string | 否 | `"skip"`；枚举 `skip/queue/replace` |
| `session_deleted_policy` | string | 否 | `"disable"`；枚举 `disable/completed` |
| `max_runs` | int \| null | 否 | 12；≥1 或 null=不限 |
| **禁止字段** | — | — | `mode/model/model_name/approval/sandbox/worktree/work_mode` 传入即 `BAD_REQUEST`——心跳执行回到原 session 现场，运行配置由原会话决定 |

响应体额外含：`id`（`hb_` 前缀）、`kind:"heartbeat"`、`status`（scheduled/running/completed/expired/disabled）、`next_run_at/last_run_at/run_count`、`metadata.source`（web_rpc 等）、`run_state`。

#### 方法明细

#### heartbeat.job.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `status` | string | 否 | 过滤状态 | 状态过滤 |
| `scope` | string | 否 | 仅 `current`/`all_visible`；**Web 代理固定 allow_all_visible=False，`all_visible` 必然 `FORBIDDEN "heartbeat.jobs.all permission required"`** | 范围 |
- **响应**：`{jobs}`（默认只返回当前会话的任务）。

#### heartbeat.job.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转；`hb_` 前缀 | 任务 id |
- **响应**：`{job}`；session 不匹配 `FORBIDDEN "job belongs to another session"`；不存在 `NOT_FOUND`。

#### heartbeat.job.create

- **参数**：job 字段表（见上方 HeartbeatJob 字段表）——**禁止字段**（`mode/model/model_name/approval/sandbox/worktree/work_mode`）传入即 `BAD_REQUEST`。
- **响应**：`{job}`；资源限制：每 session 活跃 ≤5、全局 ≤100、interval ≥60s。

#### heartbeat.job.update

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转 | 任务 id |
| `patch` | dict | 是 | 仅 name/prompt/schedule/timezone/enabled/concurrency_policy/session_deleted_policy/max_runs | 补丁 |
- **响应**：`{job}`；enable 时 run_count≥max_runs 拒绝；调度变更无未来触发点 `BAD_REQUEST`；冲突 `CONFLICT`。

#### heartbeat.job.delete / toggle / preview / run_now / cancel

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `str()` 强转 | 任务 id |
| `enabled` | bool | toggle 必填 | **严格布尔**（缺/None 拒绝） | toggle 目标状态 |
| `reschedule` | bool | run_now 可选 | 缺省 false | run_now 重排 |
| `pause_schedule` | bool | cancel 可选 | 缺省 false | cancel 时同时停后续调度 |
- **响应**：delete → `{deleted: bool}`（活跃 run 先 cancel，失败 `CONFLICT`）；toggle → `{job}`；preview → `{next: [{run_at(unix秒), iso}, ...]}`；run_now → **可被拒绝** `{accepted: bool, run_id, session_id, reason?}`（reason：session_deleting/job_completed/session_missing/session_busy/previous_run_active/already_queued 等）；cancel → `{job_id, cancelled_run_id, cancel_status: cancelled|not_found|failed|idle, paused, reason?}`（**只中断当次运行**）。

执行语义：`prompt` 在**原 session 现场**以 background 流继续上下文执行（默认超时 300s）；执行结果以普通 chat 事件回流原会话。权限双因子：`(session_id, user_id)`——调用者 user_id 须与会话属主一致，且 job 须属于当前 session。

### 8.3 schedule.*（AutoHarness 演化管线，转发 AgentServer）

首个请求惰性初始化 AutoHarnessService。与 cron.job.* 无共享存储或调度器。**信封规则**：业务错误恒 `ok:true` + payload `{error}`（无 code）；仅异常才 `ok:false {error}`（`server/handlers/schedule.py:152-169`）。以下均为**透传**。

#### schedule.check_config

- **参数**：无。**响应**：`{valid, missing_fields(含 id/key/prompt/section/optional/env_var 元数据), config_path}`；必填 git.user_name（兼作 fork_owner/gitcode.username）/git.user_email；access_token 可由 env 替代；校验器未初始化 → `{valid:false, error}`。

#### schedule.update_config

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `fields` | dict | 是 | 键须为已知 field id；**未知键静默忽略**，success 恒 true | 写入字段 |
- **响应**：`{success:true, updated_fields}`；写 YAML 后重载 base config。

#### schedule.create

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `query` | string | 否 | 缺省 `""`；无空校验 | 查询 |
| `interval_hours` | int | 否 | 缺省 **4**；无类型校验——非数字 → TypeError → ok:false | 间隔 |
| `run_immediately` | bool | 否 | 缺省 false；true 立即触发但 next_run_time 仍 now+interval | 立即执行 |
| `model_name` | string | 否 | 经 resolve_model | 模型 |
| `pipeline` | string | 否 | 缺省 None | 管线 |
- **响应**：`{task_id:"sch_<hex8>", next_run_time(ISO), message:"定时任务已创建"}`。

#### schedule.run

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `query` | string | 否 | 缺省 `""` | 查询 |
| `model_name` | string | 否 | 同 create | 模型 |
| `pipeline` | string | 否 | 同 create | 管线 |
- **响应**：`{task_id, status:"running", message:"一次性任务已创建并开始执行"}`（is_one_time=true、interval_hours=0、立即触发）。

#### schedule.list / status

- **list 参数**：无。**status 参数**：

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `task_id` | string | 否 | 缺省 `""` | 任务 id |
- **响应**：任务 dict + `progress:{summary, stages[], completed_stages, current_stage, failed_stage, log_path}`；不存在 → **ok:true** + `{error:"任务不存在", task_id}`。

#### schedule.logs

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `task_id` | string | 否 | 缺省 `""` | 任务 id |
| `log_type` | string | 否 | 缺省 `"current"`；仅 current/history，其他报错 | 日志类型 |
| `history_index` | int | 否 | 缺省 -1；history 模式 0=最近完成，越界报错 | 历史序号 |
| `offset` | int | 否 | 缺省 0，行偏移续读 | 偏移 |
| `limit` | int | 否 | 缺省 500，最大行数 | 行数 |
- **响应**：current → `{logs, execution_id, type:"current", total_lines, is_running, has_more}`；history → `{logs, execution_id, type:"history", completed_at, status, total_lines, has_more}`；无执行/无历史/任务不存在 → `{error:...}`。

#### schedule.cancel / delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `task_id` | string | 否 | 缺省 `""` | 任务 id |
- **cancel 响应**：`{task_id, status:"cancelled"}`；调度器未初始化 → `{error}`。
- **delete 行为**：running 先 cancel 再连日志删除。**响应**：`{task_id}`；不存在/失败 → `{error, task_id}`。

另有 `issue.watch_once` / `issue.state.list` / `issue.matrix` / `issue.delete` 走同一 handler（§10.6.17）。

### 8.4 health_check.get_conf / set_conf（探活配置）

> **网关处理**：**纯本地**——读探活配置；返回 {every, target, active_hours}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "health_check.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"every": 60, "target": "", "active_hours": null}}
```

`heartbeat.get_conf`/`set_conf` 为**废弃别名**，行为等同 health_check.*。

- **get_conf** 响应：`{every: <秒>, target: <渠道id>, active_hours: {start:"HH:MM", end:"HH:MM"} | null}`。
- **set_conf** 参数：`every`（number，秒，>0 否则 `BAD_REQUEST`）、`target`（string，渠道 id）、`active_hours`（dict，`{start:"HH:MM", end:"HH:MM"}`，须同时含 start/end，否则视为清除）。`target=feishu` 时预检每个 app 有可用推送目标，否则专用错误 `error=code="feishuNoTarget"`。写回 config.yaml `health_check` 段并热重启探活循环。
- 探活结果以 `health_check.relay` 事件全量广播（payload 双键 `health_check`/`heartbeat` 兼容）。

---

## 9. HTTP 接口

除 §9.1-§9.3 三组外，同端口还挂载：MCP OAuth 回调（`GET /mcp/oauth/callback`）、诊断代理（`/api/trajectory/sessions/{sid}/diagnoses/*`）、MCP App 沙箱（`/api/v1/mcp-app/sandbox`）、个人分享（`/share-api/jobs*`，仅个人版）；企业版另挂管理面与 `/api/v1` REST+SSE 面（`/chat/completions`、`/sessions` CRUD、settings/workspace/team mapped 表，`web_channel_app.py:113-167`、`enterprise/plane.py`）——这些面与 WS 方法共用 `invoke.py` 统一入站管道（§1.3）。

### 9.1 /api/v1/auth/*（华为账号登录）

所有 POST 写操作必须带 `X-Jiuwen-Auth: 1` 头（防跨站简单请求），缺失 → 403 `{ok:false, error:{code:"auth_header_required"}}`。会话 id 双通道：`X-Auth-Session` 头或 cookie `jiuwenswarm_auth`（httponly/samesite=lax/path=/，30 天）。错误体统一 `{ok:false, error:{code, message}}`。

| 路由 | 方法 | 请求 | 成功响应 | 主要错误 |
|------|------|------|---------|---------|
| `/api/v1/auth/authorize` | POST | 无 body | `{authorizeUrl, state, claimToken, expiresIn}` | 403 `auth_header_required`；404 `login_disabled`；503+OAuth code |
| `/api/v1/auth/callback` | GET | query: `state, code, error...` | HTML 落地页（BroadcastChannel 通知 + 自动关闭）+ 种 cookie + `X-Auth-Session` 头 | 400 落地页（`oauth_state_invalid` 等） |
| `/api/v1/auth/claim` | POST | `{state, claimToken}` | 未完成 202 `{ok:true, pending:true}`；完成 200 `{ok:true, islogin:true, sessionId, userId, userName, createdAt}` | 403/404/400 |
| `/api/v1/auth/cancel` | POST | `{state, claimToken}` | 200 `{ok:true}`（幂等，不区分 state 是否存在） | 403 |
| `/api/v1/auth/status` | GET | 会话 id | `{islogin, ...}`（无凭据） | — |
| `/api/v1/auth/logout` | POST | 会话 id | `{ok:true, islogin:false}` + 清 cookie + 撤销 AgentServer 凭据 | 403 |
| `/api/v1/auth/models` | GET | 会话 id | `{ok, islogin, whitelistApplied, models:[...]}`（未登录/过期 `models:[]`，不用缓存充数） | — |
| `/api/v1/auth/quota` | GET | 会话 id | 未接 APIG：`{ok:true, available:false, quota:null}`（非错误） | 401 `not_logged_in`/`session_expired`；502 其他 ApigError |

### 9.2 /file-api/*（容器文件，仅 AgentOS 多用户部署挂载）

统一鉴权中间件：先 `authenticate_http`（失败 401）；auth 启用时做 **USER_MISMATCH 校验**——请求声明的 user_id（显式参数/query/`X-User-Id` 头并集）必须 ⊆ token 身份集合，否则 403 `USER_MISMATCH`。user_id 解析优先级：显式参数 > 头 > IAM username；缺失 400。路径穿越防护：上传必须相对路径+禁 `..`+扩展名黑名单；下载/列表必须绝对路径且限定 `/home/agentos` 下（`router_client.py:284-361`）。

| 路由 | 方法 | 关键参数 | 响应 |
|------|------|---------|------|
| `/file-api/list-files` | GET | `dir`（必填）、`session_id`（缺省不携带）、`user_id`（缺省回退 `X-User-Id` 头/IAM 身份，仍无则 400）、`recursive`（默认 false）、`max_depth`（默认 0，仅 `recursive=true` 时生效，int≥0） | `{files:[{name, path, isDirectory, isMarkdown, size, modifiedTime, type}]}`（目录优先、名称不区分大小写排序） |
| `/file-api/list-markdown` | GET | 同上 | 仅 markdown 文件 |
| `/file-api/raw-file` | GET | `path`（必填）、`offset`/`limit`（缺省不携带——携带或 `format=json` 才走单块 JSON）、`format`（默认 `binary`） | json/offset → 单块 `{path,size,offset,chunk_size,data_base64,eof}`；否则二进制流（64KB 块探测 Content-Type） |
| `/file-api/download` | GET/HEAD | `token`（必填）、`inline` | verified_asset token → E2A 分块（512KB）支持 `Range`/HEAD/inline；location token → 流式 attachment。400 `invalid_download_token` |
| `/file-api/file-content` | GET | `path`、`encoding`（默认 utf-8，可 auto/gbk/big5/shift_jis 等） | 全文解码文本（带 `X-Original-Encoding` 头） |
| `/file-api/file-content` | POST | body `{path(仅 .md/.mdx), content, ...}` | `{ok:true}`；400 `only_markdown_supported` 等 |
| `/file-api/upload` | POST multipart | `dir`、`session_id`、`file`（**≤20 个**） | `{ok:true, files:[{filename,path,mime_type,size_bytes}], errors:[...]}`（逐文件收集成败） |
| `/file-api/mkdir` | POST | `path`（必填）、`mode`、`recursive` | `{success, path, created}` |

传输错误码 → HTTP 状态：`UNAUTHORIZED族`→401、`FORBIDDEN族`→403、`NOT_FOUND族`→404、`PAYLOAD_TOO_LARGE/FILE_TOO_LARGE`→413、`LIST_NOT_SUPPORTED/NOT_IMPLEMENTED`→501、其余 502（`container_file_http.py:92-119`）。

### 9.3 /api/trajectory/*（调试轨迹，全 GET）

访问校验（按序）：`trajectory_ui.enabled` 开关与 SQLite reader 均在 AgentServer 侧——网关按其状态映射 503（不可用码族含 `RUNTIME_WARMING/DRAINING/FAILED/QUEUE_FULL`）/ 504（`AGENT_SERVER_TIMEOUT`）；session_id 非法 → 400；会话不存在 → 404 `NOT_FOUND`；mode 非 Agent/Team → 403 `UNSUPPORTED_SESSION_MODE`。所有响应 `Cache-Control: no-store`；先过 Origin/FetchMetadata 校验（cross-site 403 `FORBIDDEN_ORIGIN`）。

| 路由 | 参数 | 响应 |
|------|------|------|
| `GET /api/trajectory/sessions/{sid}/subjects` | `after_revision`（默认 0） | `{schema_version, session_id, items, watermark, store_epoch}` |
| `.../stream-frames` | `since_frame_seq`（0）、`limit`（500，1–2000） | 一页流帧（按提交序） |
| `.../sequences` | `hashes`（逗号分隔 64 位 hex，≤200）、`since_revision` | `{sequences, blobs}` |
| `.../archive` | 无 | `application/zip`（单条目 trajectory.jsonl；≤16MB 内存、更大走临时文件） |
| `.../checkpoints` / `.../usage` | 无 | checkpoint 快照 / 按主体分区用量 |
| `.../subjects/{subject_id}/records` | `since_revision`、`limit`（默认 1000，1–1000） | 记录引用 + 去重 sequences/blobs；404 subject not found |
| `.../traces/{trace_id}/spans/{span_id}/raw` | 路径参数（trace 32 hex、span 16 hex） | 原始 OTLP JSON；404 span not found |

---

## 10. 其它 WS 方法族

以下方法族均为标准 res 壳，参数校验与错误码遵循 §2/§3 通用契约。

### 10.1 配置与模型

**分发契约（A4g/A5）**：`config.get/set/save_all/validate_model`、`models.*`、`model_groups.*`、`session.selection.set` 经 `_register_config_proxy` 注册——**个人版统一经 E2A 在目标 AgentServer 执行，不回落网关本地**；企业版仅 RuntimeRouted/无 client/legacy 共享目录走本地 handler（`app_web_handlers.py:5714-5759`）。`config.moa_status` 与 `models.refresh_status` **恒 E2A**（无本地 handler）。`vendors.*`、`openai_account.*`、`connection.status`、`locale.*` 为网关本地 handler。转发前删客户端传入的 `_auth_session` 键、改注入连接登录会话（`:5735-5746`）。企业版写保护（仅 HTTP 入口）：`config.set/save_all`、`models.replace_all/save/remove/set_active` 等 → `FORBIDDEN "企业版配置由管理面统一下发"`。

#### config.get

> **网关处理**：**E2A 代理**——_register_config_proxy(CONFIG_GET)；**A4g 个人版恒 E2A**；转发前删客户端 _auth_session 键、注入连接登录会话。
- **参数**：无。**响应**：一次性扁平面板快照（全 string 化）：`CONFIG_SET_ENV_MAP` 全键的当前值（默认模型五键 `model_provider/model/api_base/api_key/endpoint_profile`、多模态/搜索/agents/team 扁平键等）+ 元信息 `app_version/runtime_platform/external_cli_agents_supported/cua_backend_available` + YAML 开关族（`setup_guide_enabled`、`rsi_enabled`、MoA 键、`trajectory_ui_*`、实验开关、`enable_free_models` 等）；api_key/token 类经解密；YAML 读失败逐键回默认且**仍 ok=true**。

#### config.set

> **网关处理**：**E2A 代理**——_register_config_proxy(CONFIG_SET)；A4g 同上；legacy 共享目录的外部 CLI 依赖安装门控短路。
- **参数**：扁平键值对象，四族键：env 族（同步 `os.environ` + 持久化 `.env`；`*_provider` 须在枚举、`*_context_window_tokens` 正整数或 256K/1M）、YAML 开关族（`cua_agent_enabled` 须 true/false/null、`permissions_profile` 与 `permissions_enabled` 互斥）、`agents`/`team`（整体替换 `modes.team`）、MoA 键族。
- **响应**：`{updated:[键], applied_without_restart: bool}`——**该布尔表示是否已在 AgentServer 进程内热重载**（`reload_agents_config` 按 scopes={model, multimodal, search, team, permissions, agent_runtime, proactive} 生效），非"安排进程重启"；另可附 `codex_dependency_install`/`external_cli_dependency_installs`/`canonical_config`。
- **错误**：`BAD_REQUEST`（键校验）/ `INTERNAL_ERROR`（team 写回失败、KV cache fail-closed、MoA 保存失败）。

#### config.save_all

> **网关处理**：**E2A 代理**——_register_config_proxy(CONFIG_SAVE_ALL)；A4g 同上。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `config` | object | 否 | 同 config.set 全键 | — |
| `models` | list | 否 | 同 models.replace_all 的条目规则 | 整体替换 models.defaults |
| `login_model_settings` | object | 否 | `{模型名:{context_window: 正整数}}` | 登录模型窗口设置 |
| `agents` / `team` | 顶层 | 否 | 并入 config | — |

- **响应**：`{updated, applied_without_restart, models_count?}` + 可选 install/canonical_config；**单次热更新**；affinity 与新模型不匹配时自动关闭 KV cache affinity。

#### config.validate_model / models.validate（同一实现）

> **网关处理**：**E2A 代理**——_register_config_proxy(CONFIG_VALIDATE_MODEL)；A4g 同上。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `api_base` / `model` | string | 是 | api_base strip 去尾 `/` | — |
| `model_provider` | string | 是 | ProviderType 枚举（大小写归一） | — |
| `api_key` | string | 非 OAuth 厂商必填 | OpenAIAccount provider 免 | — |
| `verify_ssl` | bool | 否 | 缺省保留持久值再 false | — |
| `endpoint_profile` / `reasoning_level` / `vendor_key` | — | 否 | 草稿→host 推断→持久兜底 | — |

- **行为**：发最小 chat completion 探测（timeout 25s、不重试；先 max_tokens=3 失败再 ≤16）。**响应**：`{ok:true, model_provider}`；探测失败 → `LLM_ERROR` + 原始异常文本。

#### config.moa_status

> **网关处理**：**E2A 代理**——_proxy_config_request(CONFIG_MOA_STATUS)；恒 E2A、无本地 handler。
- **参数**：`model_selection`（可选）。**响应**：已配置 `{configured:true, aggregator_model_name, aggregator_alias}`；任何异常（未配置组/成员缺凭据等）→ `{configured:false}`——**服务端实现永不回错误**；`SERVICE_UNAVAILABLE` 仅指 AgentServer 断链（E2A 外壳）。

#### models.list

> **网关处理**：**E2A 代理**——_register_config_proxy / _model_resource_rpc；**A4g 个人版恒 E2A**；转发前删客户端 _auth_session 键、注入连接登录会话。
- **参数**：无。**响应**：`{models:[{model_id, model_name, api_base, api_key(登录模型恒空), has_api_key, model_provider, temperature, reasoning_level(on/off), is_default, is_agentos, is_free, alias, origin_index, vendor_key, plan, endpoint_profile, context_window_tokens, ...}], model_groups:[公开组], active_model}`；E2A 成功后 AgentServer 追加 Zen 免费模型行（按连接登录会话合并）。企业 legacy 本地分支另返回 `model_source:"enterprise"` 且**无 model_groups**。

#### models.replace_all

> **网关处理**：**E2A 代理**——_register_config_proxy / _model_resource_rpc；**A4g 个人版恒 E2A**；转发前删客户端 _auth_session 键、注入连接登录会话。
- **参数**：`models`（list 必填；登录模型与 `is_agentos:true` 条目被剔除且剔后须非空）。条目字段：`model_name`（strip 非空）、`api_key`（新条目必填；OpenAIAccount 例外；带 `origin_index` 的旧条目缺省视为"未改"）、`model_provider`（枚举）、`temperature`（空串→null）、`timeout`（缺省 1800）、`verify_ssl`（缺省 false）、`is_default`、`alias`（列表内唯一且不与 model_name 冲突）、`reasoning_level`（枚举 off/on/minimal/low/medium/high/xhigh/max 且按模型能力校验）、`context_window_tokens`（正整数或 256K/1M）、`vendor_key`/`plan`（plan 枚举 token_plan/coding_plan/custom_api 且必须带 vendor_key）、`endpoint_profile`、`origin_index`（命中原位置时 diff 合并保留 `${VAR}` 占位符）、`model_id`（提供时须已存在）。
- **响应**：`{count, applied_without_restart}`。**错误**：`BAD_REQUEST` / `ModelSelectionError`（code+payload）/ `INTERNAL_ERROR`。

#### models.get / upsert / delete / references

> **网关处理**：**E2A 代理**——_register_config_proxy / _model_resource_rpc；**A4g 个人版恒 E2A**；转发前删客户端 _auth_session 键、注入连接登录会话。
| 方法 | 参数 | 说明 |
|------|------|------|
| `models.get` **E2A 代理** | `model_id`（必填） | 完整条目 + `source/is_agentos/read_only/write_only_fields`；服务端分支 defaults 源非登录模型时**下发明文 api_key**；agentos 源不出 key 且 read_only；`MODEL_SELECTION_NOT_FOUND` |
| `models.upsert` **E2A 代理** | `model`（dict 必填；扁平或嵌套 model_client_config/model_config_obj 均可） | 合并语义（未提交字段保留；api_key 空或未变保留原值、变更则加密）；`model_id` 缺省生成 `mdl_`；`is_default:true` 独占；agentos 模型 → `MODEL_SELECTION_FORBIDDEN`"AgentOS models are read-only" |
| `models.delete` **E2A 代理** | `model_id`（必填） | 先查引用（组路由/session/agents/team/cron）→ 有引用 `MODEL_SELECTION_REFERENCED`（payload 带 references）；agentos → FORBIDDEN；删除时级联剪路由、空组移除 |
| `models.references` **E2A 代理** | `ModelSelection` 本体（整个 params 作 schema） | `{references:[{scope, scope_id}]}`，scope ∈ model_group/session/cron/agent/team；元数据不可读 → `MODEL_RUNTIME_UNAVAILABLE` |

#### model_groups.list / upsert / delete

> **网关处理**：**E2A 代理**——_register_config_proxy / _model_resource_rpc；**A4g 个人版恒 E2A**；转发前删客户端 _auth_session 键、注入连接登录会话。
- **list**：无参数；组公开键 `model_group_id/display_name/enabled/is_default/routes/request_config` + 可选 `routing`（仅三个超时键，其余剥离）。
- **upsert**：`group`（dict；`model_group_id` 缺省生成 `mgp_`、route_id 缺省 `rte_`）；**enabled 组须含至少一条 enabled 路由**否则 `MODEL_GROUP_INVALID`；候选整体校验。
- **delete**：`model_group_id` 必填；引用检查同 models.delete。
- 错误外壳：`ModelSelectionError` → `error+code+payload`；其余 `BAD_REQUEST`。

#### vendors.list / vendors.fetch_models（网关本地）

> **网关处理**：**纯本地**——读静态厂商注册表；返回 {vendors:{token_plan:[...], coding_plan:[...], custom_api:[...], reasoning:{...}}}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "vendors.list", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"vendors": {"token_plan": [], "coding_plan": [], "custom_api": []}}}
```

- **list**：无参数。`{vendors:{token_plan:[...], coding_plan:[...], custom_api:[...], reasoning:{...}}}`——厂商卡片 17 字段（vendor_key/display_name/plan/client_provider/api_base/endpoint_profile/default_model/model_options/...）。
- **fetch_models**：`vendor_key`+`plan`（必填，须命中预设枚举）、`api_key`（需 key 的预设必填，未给走回退）、`modality`（仅 volcengine+custom_api 支持 `image`|`video`，须配 `api_base`）。远端成功 `{models, source:"remote"}`；回退仍 `ok:true`（`source:"preset"|"unavailable"` + reason）；拉取超时 12s。

#### locale.get_conf / set_conf

> **网关处理**：**分流**——legacy 本地 / AgentOS E2A；返回 {preferred_language:'zh'|'en'}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "locale.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"preferred_language": "zh"}}
```

- **get_conf**：无参数 → `{preferred_language:"zh"|"en"}`（缺省/非法归一 zh）。
- **set_conf**：`preferred_language`（必填，strip.lower ∈ {zh, en}）；写回 config.yaml → `{preferred_language}`；非法 → `BAD_REQUEST`。

#### openai_account.auth.* / openai_account.models.list（网关本地）

| 方法 | 参数 | 说明 |
|------|------|------|
| `auth.status` **纯本地** | 无 | `{authenticated, auth_path, has_refresh_token, expires_at, needs_refresh, error, base_url}` |
| `auth.start_login` **纯本地** | 无 | 幂等复用未过期 job；`{status:"pending", login_id, user_code, verification_uri, interval, expires_in, expires_at, auth}`（TTL ≤300s） |
| `auth.pending_login` **纯本地** | 无 | 最新 job（同上形状）；无 job → `{status:"none"}` |
| `auth.poll_login` **纯本地** | `login_id`（必填非空） | 状态机：不存在/过期 `{status:"expired"}` → 未批准 `{status:"pending"}` → 成功 `{status:"authenticated", authenticated:true, auth}`（job 移除） |
| `auth.logout` **纯本地** | 无 | 清空 login job 后登出；`{logged_out, auth}` |
| `models.list` **纯本地** | 无 | `{models:[id...], base_url, auth}`；失败 `MODEL_LIST_ERROR` |

- 错误 payload 三段：`{status:"error", error, code, status_code, relogin_required}`（code 枚举：`openai_account_auth_missing/_refresh_failed/_store_read_failed`、`device_code_*`、`token_exchange_*` 等）。

| `session.selection.set` **E2A 代理** | 见 §6.3 |
| `models.refresh_status` **未实现** | 无本地 handler，恒 E2A；刷新模型状态 |
| `connection.status` **纯本地** | 网关本地；`{agent_ready/连接状态, protocol_version}` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "connection.status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"agent_ready": true, "protocol_version": "1.0"}}
```


### 10.2 媒体与附件

#### media.persist（媒体落盘）

> **网关处理**：**E2A 代理**——proxy_unary_request(MEDIA_PERSIST)；**网关预上传**：>4MB base64 解码后经 E2A FILE_UPLOAD_CHUNK 分块（2MB/块）→ 以 _persisted:true+path 替代原 base64 项；上传失败保留原值。
- **请求参数**：`media_items`（list；仅处理 `type=="image"` 的项，**≤8 张**——AgentServer 落盘在存满 8 张后停止（跳过非图片项继续计数）；chat.send 网关路径为"取前 8 项再过滤"，两处口径有细微差异）。项字段：

| 字段 | 类型 | 必填 | 校验 | 说明 |
|------|------|------|------|------|
| `type` | string | 是 | 须为 `"image"`，其余跳过 | 仅图片落盘 |
| `mimeType` / `mime_type` | string | 是 | lower+strip；表：image/png→.png、image/jpeg→.jpg、image/webp→.webp、image/gif→.gif；表外丢弃 | MIME |
| `base64Data` / `base64_data` | string | 与 path 二选一 | 解码后 ≤10MB；`data:...;base64,` 前缀自动剥离 | 上传正文 |
| `_persisted` + `path` | bool+string | 否 | path 须存在否则丢弃 | 网关预上传大图直接透传 |

- **处理**：图片落盘 `agent/sessions/<sid>/uploads/<安全文件名>`（同名原子去重）；**超过 4MB E2A 预算的**在网关解码后经 E2A 分块上传（2MB/块）替换为 `_persisted:true` 落盘记录；上传失败**保留原 base64** 由下游报可重试错误（网关不重复解码）。
- **响应**：`{content?, query?, media_items, files}`——`media_items` 每项 `{type:"image", filename, mime_type, path, size_bytes}`；`files` 为 **dict**：`{uploaded_images:[{filename, path, mime_type, size_bytes}]}`。
- **错误**：归一化异常 `INTERNAL_ERROR`；企业版项越出租户根 `FORBIDDEN`；超限/非法项**静默丢弃不报错**。

#### media.discard（丢弃未发送副本）

> **网关处理**：**E2A 代理**——proxy_unary_request(MEDIA_DISCARD)；params 原样。
| 参数 | 类型 | 必填 | 校验 | 说明 |
|------|------|------|------|------|
| `path` | string | 是 | 须为**当前会话 uploads 目录直接子文件**：绝对路径、拒 symlink/目录/其他会话/用户原图 | 只删会话上传副本 |

- **响应**：`{deleted: true|false}`（文件已不存在返回 deleted:false——重复删除是成功）。**错误**：`BAD_REQUEST`（path required/must be absolute/outside session uploads/refusing symlink/not a file）/ `INTERNAL_ERROR`（OSError）。

#### document.persist / document.formats

> **网关处理**：**E2A 代理**——proxy_unary_request(DOCUMENT_PERSIST)；同 media.persist 预上传逻辑（_pre_persist_large_documents）。
- **document.persist**：参数 `documents` 与/或 `media_items` 中 `type=="document"`（或 type 缺省且文件名过黑名单）的项——**与 media.persist 差异显著**：单项解码后 ≤**100MB**（media 10MB）、数量上限 **8 条（企业版 20）**、支持本地 `path/original_path` 引用、超限项进 `document_errors:[{index, filename, error}]` **不静默丢**、回显剥离 base64 防超帧、响应恒含 `forbidden_formats`；`files.uploaded_documents:[{filename, path, original_path, mime_type, size_bytes}]`。
- **document.formats**：无参数 → `{forbidden_formats:[29 个后缀]}`（.exe/.dll/.msi/.bat/.ps1/.jar/.app/.dmg/.so/.dylib 等，排序）。

#### task.asr.transcribe（语音转写）

> **网关处理**：**纯本地**——base64 解码 + mime 校验 + ASR API 调用（120s 超时）；返回 {text}；未配置→ok+{text:'',skipped:true}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "task.asr.transcribe", "params": {"audio_base64": "<base64...>", "mime_type": "audio/webm"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"text": "转写文本"}}
```

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `audio_base64` | string | 是 | 空/非 Base64 → `BAD_REQUEST`（"录音内容为空"/"不是有效的 Base64"）；解码后 **≤25MB** → `ASR_AUDIO_TOO_LARGE` | 录音内容 |
| `mime_type` | string | 否 | 缺省 "audio/webm"；剥 `;charset` 后缀、lower；枚举 flac/m4a/mp4/mpeg/mp3/ogg/wav/wave/webm/x-m4a → 表外 `ASR_UNSUPPORTED_FORMAT` | 录音格式 |

- **响应**：`{text}`；**未配置 ASR** → `ok:true + {text:"", skipped:true, reason:"ASR_NOT_CONFIGURED"}`；mime 校验先于解码；超时 120s/连接 15s 不重试。
- **错误码族**：`ASR_NOT_CONFIGURED`(经 skipped 形态)/`ASR_BAD_CONFIG`/`ASR_UNSUPPORTED_FORMAT`/`ASR_AUDIO_TOO_LARGE`/`ASR_TIMEOUT`/`ASR_CONNECTION_ERROR`/`ASR_UPSTREAM_ERROR`/`ASR_INVALID_RESPONSE`/`ASR_AUDIO_DECODE_FAILED`/`ASR_EMPTY_TRANSCRIPT`；意外 `INTERNAL_ERROR`。

#### workspace.*（工作区只读/管理，E2A 代理）

路径规范（全部 `relative_path`）：strip、`\\`→`/`、拒绝 `..`/NUL/绝对路径、`.` 段折叠、空=根。

#### workspace.tree

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `relative_path` | string | 否 | 路径规范见上；空=根 | 相对租户根的目录 |
- **响应**：`{relative_path, entries:[{name, relative_path, is_dir, size_bytes, mtime_ms, ctime_ms, zone, deletable, mime_hint}]}`；不存在 `NOT_FOUND`；路径逃逸 `FORBIDDEN 'path escapes tenant root'`。

#### workspace.preview

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `relative_path` | string | 是 | 空 → `BAD_REQUEST` | 相对租户根文件路径 |
| `max_bytes` | int | 否 | 缺省 512KiB；**硬顶 1MiB**（`min(int(...), 1MiB)`） | 只读前 limit+1 字节判定截断 |
- **响应**：`{relative_path, truncated, content}`——二进制（含 NUL 或可打印占比 <85%）或不可解码 → `content:null`；目录 `BAD_REQUEST`。

#### workspace.usage

- **参数**：无（身份取自请求三元组）。**响应**：`{user_id, group_id, bot_id, used_bytes, limit_bytes, percent, status(ok\|warn\|block), source_policy_id, unlimited}`。

#### workspace.entries.delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `relative_paths` | list[string] | 是 | 非 list → `BAD_REQUEST 'invalid workspace request'` | 逐条独立删除 |
- **响应**：`{results:[{relative_path, ok, error?, freed_bytes?}]}`——可删 zone 仅 session_workspace/session_todo/artifact；错误枚举 forbidden_zone/bad_path/not_found/io_error/delete_too_deep(≥32)/delete_too_many(>5000)；symlink 只删链接不跟随。

#### file.import_url / file.upload_chunk（附件外链导入与分块上传）

> **网关处理**：**E2A 代理**——_file_proxy(FILE_IMPORT_URL)；params 原样。
| 方法 | 参数 | 说明 |
|------|------|------|
| `file.import_url` **E2A 代理** | `files`（list[dict] 必填；项含 `url`/`uri`、可选 `name/filename` 缺省取 URL basename 再缺省 "download"）、`overwrite`（bool 缺省 false——false 落盘 stem-N 去重；网关内部调用时 overwrite=非 AgentOS 客户端） | 下载落 `get_agent_workspace_dir()`（UA jiuwenswarm-agent、60s 超时）；响应 `{files:[原项 ∪ {path, filename, size, mime_type}]}`，失败项 `{**原项, error}` 整体仍 ok=true；企业 OBS 分支：1..20 项、图片 ≤10MiB、配额超 `WORKSPACE_QUOTA_EXCEEDED` |
| `file.upload_chunk` **E2A 代理** | `upload_id`（必填）、`target_rel_path`（必填，`\`→`/`、禁前导 `/` 与 `..`）、`resolved_path`（续传锚点，首块响应返回）、`data`（base64 必填，空报错）、`final`（bool 缺省 false） | 首块 O_EXCL 原子创建（stem-1..999 去重）、续块 append；越出用户目录 `BAD_REQUEST "path_outside_workspace"`；响应 `{path(绝对), size(累计), upload_id, final}`。**浏览器 WS 通道 512KiB/块**（画布素材）；网关侧 >4MB 经 E2A 以 2MB/块顺序上传 |

> `file.read_upload_chunk` 为**纯内部方法**（仅 `/file-api/raw-file` 读取 uploads 目录使用，非对外 WS 方法）：参数 `path`（绝对、parent 须为 `agent/sessions/<sid>/uploads`）、`offset`、`limit`（≤512KiB）；响应 `{data_base64, offset, chunk_size, size, eof, name, mime_type}`。

### 10.3 IM 渠道配置

**公共契约**：`channel.*` 配置族均为**网关本地 handler**（不经转发）；set_conf 链路 = `cm.set_conf`（内存整体替换 + 热重建 Channel 实例，失败回滚并标记重启待定）→ 合并写 config.yaml → 清 Agent 配置缓存；**写盘失败仅告警、响应仍成功**。企业版写保护：`channel.*.set_conf`/`unbind` 在 **HTTP 入口**经 invoke 拦截；绑定方法另有 **handler 内自检**（WS/HTTP 均拦）：feishu/dingtalk 的 bind/unbind/revoke 与 **welink 全部 7 个方法（含 get_bindings）** → `FORBIDDEN "企业版配置由管理面统一下发"`；`channel.feishu.get_bindings` 企业版放行。

#### channel.get

> **网关处理**：**纯本地**——读 cm.enabled_channels；返回 {channels:[{channel_id}]}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.get", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"channels": [{"channel_id": "feishu"}, {"channel_id": "web"}]}}
```

- **参数**：无。**响应**：`{channels:[{channel_id}]}`（当前已注册 Channel 实例 id）。

#### get_conf / set_conf（18 个）

- **get_conf**：无参数 → `{config: <渠道配置体>}`。feishu/xiaoyi 返回前归一化为 `{apps:[...]}` 并补默认值（旧平铺格式转单 app）；其余七渠道原样返回。⚠️ **读路径不脱敏**（`_mask_sensitive` 无调用点）——app_secret/sk 原样返回。
- **set_conf**：params 即渠道配置体（dict 必填）。**telegram/whatsapp/discord/slack/wecom/wechat 为整体替换**（无 schema 校验，wechat 另有数值边界）；**feishu/dingtalk 为按 app 合并保留**（未发送的 secret 不被默认空值覆盖；dingtalk 新 app 强制 `allow_from`、client_id 重复拒绝、空 client_secret 视为不变；dingtalk 缺 `apps` 键时退回整体替换）。响应 `{config: <写后读回>}`。
- **各渠道配置体关键字段**（核心 5-10 个）：

| 渠道 | 关键字段（默认） |
|------|----------------|
| feishu | `app_id/app_secret`、`encrypt_key/verification_token`、`allow_from(["0.0.0.0/0"])`、`enabled(true)/is_default`、`name`、`enable_streaming(true)`、`my_user_id/bot_name`、`group_digital_avatar/enable_memory(false)` |
| xiaoyi | `ak/sk`、`agent_id/api_id/app_id/uid/api_key/push_id`、`ws_url1/ws_url2`（华为 HAG）、`push_url/file_upload_url`、`enable_streaming(true)`、`phone_tools_enabled(false)` |
| telegram | `enabled(false)`、`bot_token`、`allow_from`、`parse_mode("Markdown")`、`group_chat_mode("mention")` |
| dingtalk | app 级 `client_id/client_secret/persistent_agent_id/robot_code/allow_from`（空列表=放行所有）/`enabled`（缺省"有凭证即启用"）；共享段 `api_base/oapi_base` + 文件族 `max_download_size/download_timeout/send_file_allowed/enable_file_download/workspace_dir` |
| whatsapp | `enabled(false)`、`bridge_ws_url("ws://127.0.0.1:19600/ws")`、`allow_from/default_jid`、`auto_start_bridge(false)`、`bridge_command/bridge_workdir/bridge_env` |
| discord | `enabled(false)`、`bot_token/application_id/guild_id/channel_id`、`block_dm(false)`、`allow_from` |
| slack | `enabled(false)`、`bot_token/app_token`（Socket Mode）、`allow_from/allowed_channel_ids/default_channel_id`、`reply_in_thread(true)` |
| wecom | `enabled(false)`、`bot_id/secret`、`ws_url`、`enable_streaming/send_thinking_message(true)`、`message_merge_window_ms(15000)`、文件族 `max_download_size(100MB)` 等 |
| wechat | `enabled(false)`、`base_url`、`bot_token/ilink_bot_id/ilink_user_id`、`auto_login(true)`、`qrcode_poll_interval_sec(2.0)`、`long_poll_timeout_sec(45)`、`backoff_base_sec(1.0)/backoff_max_sec(30.0)`、`credential_file` |

- **channel.wechat.set_conf 数值边界**（写盘前校验，`BAD_REQUEST`）：`qrcode_poll_interval_sec` 0.1–3600、`long_poll_timeout_sec` 1–600（**必须整数**）、`backoff_base_sec/backoff_max_sec` 0.1–3600 + 跨字段 **max ≥ base**（两者同时出现才校验）；bool 显式排除、NaN/inf 拒绝。

#### channel.wechat.get_login_ui / unbind

> **网关处理**：**纯本地**——读微信登录 UI 状态；返回 {phase, message, qr, credentials, ...}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wechat.get_login_ui", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"phase": "idle", "message": "", "qr": null}}
```

- **get_login_ui**：无参数 → `{phase, message, qr, credentials, credentials_source, error, updated_at}`（qr 为二维码结构或 null）。
- **unbind**：无参数 → 删本地凭据文件 + 置空 bot_token/ilink_bot_id/ilink_user_id → `{config}`；企业版 HTTP 入口 FORBIDDEN。

#### 绑定方法（local_only，不经转发）

| 方法 | 参数 | 说明 |
|------|------|------|
| `channel.feishu/dingtalk.bind_agent` **本地**（local_only） | `app_id`/`client_id` + `app_secret`/`client_secret` + `config_id`（均必填 strip 非空）；dingtalk 另可 `robot_code` | 经同事服务 CONNECTION_ATTACH（120s 超时）取 agent_id → 合并写盘热生效；响应 `{binding:{app_id\|client_id, persistent_agent_id}}`；锁忙 `CONFLICT "application binding is busy"` |
| `channel.feishu/dingtalk.unbind_agent` **本地**（local_only） | `app_id`/`client_id`（必填） | 移除该 app 的 persistent_agent_id 绑定；`{unbound:true}`（**目标不存在也返回 unbound:true**） |
| `channel.feishu.get_bindings` **本地** | 无 | `{agent_ids:[...]}`（启用且带绑定的去重列表；企业版放行） |
| `channel.feishu.revoke` **本地** | `persistent_agent_id`（必填） | 匹配绑定的 app 置 `enabled:false`（不删条目）；未命中 `NOT_FOUND "colleague is not bound to Feishu"` |
| `channel.welink.get_bindings` **本地** | `config_id` 或 `persistent_agent_id`（可选过滤） | `{cli_ready, assistant_ready, channel_enabled, persistent_agent_id, assistant_credential_id, assistant_account, account, bindings[], agent_ids[]}`；⚠️ 企业版也拦截 |
| `channel.welink.login_start` **本地** | `config_id` 或 `persistent_agent_id`（二选一必填）、`account("")`、`cli_path("welink-cli")`、`env("pro")` | `{session_id, status:"pending", qr_url, qr_text, output, error, account}`；同 agent 同账号幂等复用 |
| `channel.welink.login_status` **本地** | `session_id`（必填；`NOT_FOUND "login session not found"`） | 刷新后 success 且未保存 → **自动落盘**共享 reader 登录并热生效；失败 `{status:"failed", error}` |
| `channel.welink.assistant_login_start` **本地** | `account` + `config_id`/`persistent_agent_id`（均必填） | `{session_id, status, qr_payload, error, account, kind:"assistant"}` |
| `channel.welink.assistant_login_status` **本地** | `session_id`（必填） | 扫码成功 → 落盘 assistant 凭据并**仅本次响应回显 AK/SK 一次**；失败 `{status:"failed"}` |
| `channel.welink.assistant_credential_save` **本地** | `config_id`/`persistent_agent_id` + `account` + `assistant_credential_id` + `assistant_secret`（均必填 strip） | 保存后响应**不回显 secret**；`{status:"success", kind:"assistant", ...}` |
| `channel.welink.revoke` **本地** | `config_id`/`persistent_agent_id`（必填） | 按 bindings 匹配删除（**保留共享 reader 与其他助手**）；`{revoked:true}`；未命中 `NOT_FOUND "colleague is not bound to WeLink"` |

### 10.4 更新与维护

**公共**：`path.set` 与 `updater.download/upgrade/reset_source/set_conf` 命中企业版写保护（HTTP 入口 invoke 拦截 → `FORBIDDEN "企业版配置由管理面统一下发"`）。

**UpdateStatus 字段**（`common/updater.py:97-135`，四个状态方法共用）：`current_version`、`current_build_date`、`latest_version`、`state`、`has_update`、`install_mode`、`release_notes`、`published_at`、`source_type`、`asset_name`、`matched_asset`、`download_url`、`downloaded_path`、`downloaded_bytes/total_bytes`、`error`、`checked_at`、`installing`、`restart_command`；`get_status` 附加 `platform`/`platform_supported`。**state 十值枚举**：`idle / checking / downloading / upgrading / error / disabled / up_to_date / update_available / downloaded / restarting`。

#### updater.get_status / check / download / upgrade

> **网关处理**：**纯本地**——读 Updater 状态快照；返回 UpdateStatus（含 platform）。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.get_status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"current_version": "0.1.0", "latest_version": "", "state": "idle", "has_update": false, "platform": "darwin", "platform_supported": true}}
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `get_status` | 无 | UpdateStatus 快照（含 platform 两字段） |
| `check` | `manual`（bool 可选缺省 false） | 触发检查；无更新 → `up_to_date`（附 latest_version/release_notes）；有更新 → `update_available`（desktop 已有安装包则 `downloaded`）；**异常不报错**——state=error + error 文案 |
| `download` | 无 | 幂等（已在 downloading/upgrading 直接返回）；无可用更新 → state=error "No update available..."；desktop 模式置 downloading、pip 直接置 upgrading 后台执行 |
| `upgrade` | 无 | **仅 pip 模式**：置 restarting+installing=true，成功后 3s SIGTERM 自杀重启；desktop → state=error "Desktop upgrades are handled by the desktop app" |

#### updater.get_conf / reset_source / set_conf


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip", "repo_owner": "openJiuwen", "repo_name": "agent-gateway", "access_token": "****"}}
```


#### updater.get_conf / reset_source


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip", "repo_owner": "openJiuwen", "repo_name": "agent-gateway", "access_token": "****"}}
```


- **参数**：均无。get_conf → `{enabled, desktop_release_api_type, release_api_type, install_mode, repo_owner, repo_name, release_api_url, asset_name_pattern(=windows 模式), asset_name_pattern_windows/_macos/_linux, timeout_seconds, pypi_mirror, access_token(掩码)}`；reset_source 重置为默认源后返回同结构。

#### updater.set_conf


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.set_conf", "params": {"enabled": true, "timeout_seconds": 30}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip", "timeout_seconds": 30}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `enabled` | bool | 否 | 缺省不改 | 更新开关 |
| `repo_owner` / `repo_name` / `release_api_url` / `asset_name_pattern` / `asset_name_pattern_windows` / `_macos` / `_linux` / `release_api_type` / `pypi_mirror` | string | 否 | strip；缺省不改 | 更新源配置 |
| `timeout_seconds` | int | 否 | `max(5, int(v))`；非整数 → `BAD_REQUEST` | 请求超时 |
- **响应**：写回后的 get_conf。⚠️ `release_api_type` 不做枚举校验，非法值下次 check 才报 `Unsupported release_api_type`。

#### memory.compute / memory.forbidden.get/set / hooks.list / commands.list


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "memory.compute", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"rss_mb": 256.0, "total_mb": 16384.0, "available_mb": 8192.0}}
```


| 方法 | 参数 | 说明 |
|------|------|------|
| `memory.compute` **纯本地** | 无 | `{rss_mb, total_mb, available_mb}`（psutil 缺失回退 /proc/meminfo）；兼作连接 keepalive（不更新路由注册） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "memory.compute", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"rss_mb": 256.0, "total_mb": 16384.0, "available_mb": 8192.0}}
```

| `memory.forbidden.get/set` **E2A 代理** | get 无参；set 透传 params（非对象 `BAD_REQUEST`） | 分流：AgentOS → E2A；legacy/无 client → 本地读写 config `memory.forbidden_memory_definition` 段 |
| `hooks.list` **纯本地** | 无（E2A 分支固定发 `{}`） | 分流：legacy/无 client → 本地读 config.yaml；其余 → E2A。本地响应 `{events:[{name, total_hooks, matchers:[...]}], disable_all_hooks, source:"config.yaml"}`（覆盖全部 17 个 HookEvent） |
| `commands.list` **纯本地** | `work_mode`（string 可选——按命令 `available_modes` 过滤） | 纯本地静态元数据（刻意不转发）：`{commands:[{name, usage, example, kind:"built-in", takesArgs, scope, execution, requires_session, available_modes, description(中文), description_i18n:{zh,en}}]}` |

#### path.get / set / select_directory / select_files / select_file

> **网关处理**：**分流**——legacy 本地读 browser 配置 / AgentOS E2A；返回 {chrome_path, headless, decision_*...}。

**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.get", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"chrome_path": "/usr/bin/google-chrome", "headless": true, "decision_mode": "llm"}}
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `path.get` **纯本地** | 无 | 分流：非 legacy → E2A；legacy → 本地读 browser 配置 → `{chrome_path, headless(true), decision_mode(llm\|shadow\|hybrid), decision_provider, decision_model, decision_display_name, decision_api_base, jev_key_configured}` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.get", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"chrome_path": "/usr/bin/google-chrome", "headless": true, "decision_mode": "llm"}}
```

| `path.set` **纯本地** | `chrome_path`（必填 string）、`headless`（缺省 true，非 bool 回退 true）、`decision_mode/provider/model/api_base/display_name`（仅非 None 时写） | 分流同上；成功后清 Agent 配置缓存并重启浏览器运行时；decision_mode/provider 非法 `BAD_REQUEST` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.set", "params": {"chrome_path": "/usr/bin/google-chrome", "headless": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"chrome_path": "/usr/bin/google-chrome", "headless": true}}
```

| `path.select_directory` **纯本地** | `initial_dir`（可选 string） | **legacy → 网关进程原生目录选择器**（异常 `UNSUPPORTED`）；**AgentOS → E2A 在注入 workspace 内解析**：空 initial_dir → `UNSUPPORTED "...enter the project name and absolute path manually"`（前端回落手动输入）；越界 `BAD_REQUEST`；不存在 → `{path:null, cancelled:true}` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_directory", "params": {"initial_dir": "/Users/me/project"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"path": "/Users/me/project/src", "cancelled": false}}
```

| `path.select_files` **纯本地** | `initial_dir`（可选）、`allow_multiple`（bool 缺省 true，仅影响前端） | legacy → 原生选择器；AgentOS → E2A 枚举注入目录：排序跳过 `.`/`_` 前缀、仅普通文件、**上限 200 条**；图片（png/jpg/jpeg/webp/gif/jfif）≤4MB 附 base64 预览 → `{files:[{path, filename, size, mime_type, kind:image\|document, base64?}], cancelled:false}` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_files", "params": {"initial_dir": "/Users/me/project", "allow_multiple": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"files": [{"path": "/Users/me/project/a.py", "kind": "document"}], "cancelled": false}}
```

| `path.select_file` **纯本地** | `initial_dir`/`initial_path`/`title`（均可选 string） | **纯本地**原生选择器（无 E2A 分支）；不可用 `UNSUPPORTED`；`{path, cancelled}` |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_file", "params": {"initial_dir": "/Users/me/project"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"path": "/Users/me/project/main.py", "cancelled": false}}
```


#### external_cli.*（8 个，全部本地）

#### external_cli.detect


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.detect", "params": {"cli_agent": "claude"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "claude", "status": "ok", "path": "/usr/local/bin/claude", "version": "1.0.0"}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `cli_agent` | string | 是 | strip+lower | 目标 agent（claude/codex/自定义） |
| `cli_path` | string | 否 | strip | 显式 CLI 路径 |
| `use_builtin` | bool | 否 | `is True` 或字符串 `'true'` | 优先内置托管 CLI |
- **响应**：`{cli_agent, status: ok\|warning\|unavailable\|missing\|unsupported, path, version, reference_version, message, reason?}`。

#### external_cli.validate_paths


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.validate_paths", "params": {"paths": ["/usr/local/bin/claude", "/usr/local/bin/codex"]}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"results": {"/usr/local/bin/claude": true, "/usr/local/bin/codex": false}}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `paths` | array | 是 | 非 array → `BAD_REQUEST`；**截取前 50 条** | 待验证路径列表 |
- **响应**：`{results: {<path>: true\|false\|null}}`——true=存在，false=确认缺失，null=不可判定。

#### external_cli.prune_path_histories


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.prune_path_histories", "params": {"cli_agent": "claude", "remove_paths": ["/old/path"]}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"histories": ["/usr/local/bin/claude"]}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `cli_agent` | string | 是 | 空 → `BAD_REQUEST 'cli_agent is required'` | 目标 agent |
| `remove_paths` | array | 是 | 非 array → `BAD_REQUEST`；**截取前 50 条** | 待剪枝路径 |
- **响应**：`{histories: [<剩余路径>]}`——服务端与磁盘历史求交集删除。

#### external_cli.install / install_status / codex_install_status / custom_agent.remove


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.install", "params": {"cli_agent": "claude"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "claude", "status": "running", "phase": "preparing"}}
```


| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `cli_agent` | string | 是（install/status/remove） | install/status 须在已知清单（claude/codex+托管运行时）内，否则 `BAD_REQUEST 'unsupported external cli agent'` | 目标 agent |
- **响应**：install → 19 字段安装快照 + `runtime`（**仅桌面冻结运行时可装**：pip 运行时 → `UNSUPPORTED`）；install_status → 同快照；codex_install_status → codex 快照（无 runtime 键）；custom_agent.remove → `{cli_agent}`（未安装 `BAD_REQUEST 'custom agent not found'`）。

### 10.5 权限接口

> **网关处理**：**E2A 代理**——本节全部 11 个方法均纯 E2A 转发（网关构造请求，params 透传；权限权威在目标 AgentServer 用户目录，Gateway 无本地 fallback）。

#### permissions.tools.get / set / update / update_user / delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 缺省回退帧级 | 关联会话 |
| `tool` / `tools` | string \| list[string] | get/set/update 是 | 工具名或列表 | 目标工具 |
| `enabled` | bool | set 是 | — | 启停 |
| `config` | dict | update 是 | — | 工具配置 |
| `user_id` | string | update_user/delete 是 | — | 目标用户 |
- **响应**：get → `{tools:[{name, enabled, config, ...}]}`；set/update/delete → `{ok:true}`；错误 `BAD_REQUEST`/`NOT_FOUND`。

#### permissions.rules.get / create / update / delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 缺省回退帧级 | 关联会话 |
| `rule_id` | string | update/delete 是 | — | 规则 id |
| `rule` | dict | create/update 是 | 规则结构（pattern/action/permissions 等） | 规则定义 |
- **响应**：get → `{rules:[...]}`；create/update → `{rule}`；delete → `{deleted:true}`。

#### permissions.approval_overrides.get / delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 缺省回退帧级 | 关联会话 |
| `tool` | string | 是 | — | 目标工具 |
| `user_id` | string | 否 | — | 目标用户 |
- **响应**：get → `{overrides:[...]}`；delete → `{deleted:true}`。

#### permissions.owner_scopes.get / set

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 缺省回退帧级 | 关联会话 |
| `scopes` | dict | set 是 | 数字分身 owner 范围声明结构 | 范围定义 |
- **响应**：get → `{scopes}`；set → `{ok:true}`。A5 起统一经 E2A（`app_web_handlers.py:6346-6374`）。

### 10.6 纯转发域逐方法契约

以下方法在 `_FORWARD_REQ_METHODS` 白名单中、无本地 handler，经 E2A 转发 AgentServer（`app_web_handlers.py:459-696`），传输层契约同 §3.2（成功透传、失败透传 payload.code 缺省 BAD_REQUEST）。**注意两域特例**：a4p 转发带网关前置检查（`AGENT_NOT_READY` / `A4P_WEB_SESSION_REQUIRED`，`app_web_handlers.py:5826-5859`）；rsi.\* 一律由网关注入 `session_id`（`rsi.artifact.download` 另注入 `_download_user_id`）。

其中 §10.6.1-§10.6.5（skills）与 §10.6.9（专家团/装备包）为**逐参数契约**（参数表含类型/必填/默认与校验/说明 + 响应字段 + 错误码），依据 AgentServer 仓 develop `475e3a3b64` 的 handler 实现逐行核对——这些小节内的行号引用相对该仓 `server/` 目录。布尔参数普遍为 Python 真值判断（`bool(params.get(...))`）：WS 路径无字符串布尔强转，**传字符串 "false" 会被当作 true**；严格布尔与宽松字符串解析的例外已在参数行逐处标注。另有 11 个白名单在册方法（`session.assets.remove`、`skills.enterprise/source` 系列、`skills.update/updates.check`）因钉定协议包缺枚举成员**当前不可达**（帧级 METHOD_NOT_FOUND，附录 F.1），无接口章节。

#### 10.6.1 skills 基础

**公共契约**（本小节全部方法）：

- 分发：WS/HTTP 均经 `_SKILL_ROUTES`（`interface.py:1279-1320`）路由到 SkillManager，`params` 原样透传（`dict(request.params)`，`interface.py:2542`）；HTTP 南向仅对 `with_installed`/`refresh_marketplaces` 做查询串字符串布尔强转（`handlers/skills.py:18-31`），**WS 路径无任何字符串布尔强转**。
- 错误返回双形态：handler 抛 `SkillRpcError(code, msg)` → `ok:false + {error, message, code}`（`interface.py:2648-2659`）；抛普通 `ValueError`（缺参/非法参数）→ `ok:false` **无 code**（`interface.py:2650-2652`）；大量业务失败不占错误码通道：`ok:true + {success:false, detail, detail_key?}`。
- `name` 通用校验 `_safe_path_name`（`skill_manager.py:675-689`）：strip 非空；拒绝 `.`/`..`、含 `/`、含 `\`、绝对路径（含 Windows 形式）；**无长度上限**。
- 布尔陷阱：除标注"严格布尔"外，布尔参数一律 `bool(params.get(...))` Python 真值判断——**传字符串 "false" 会被当作 true**。

#### skills.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `refresh_marketplaces` | bool（真值） | 否 | 缺省 false；WS 无字符串强转 | 为真时先对已启用 marketplace 执行 git clone/pull（单源失败仅告警不中断，`skill_manager.py:9191-9220`）；企业版跳过同步 |
| `with_installed` | bool（真值） | 否 | 缺省 false | 为真时在同一响应内联附带 `plugins`（即 skills.installed 结果），避免二次请求（`skill_manager.py:1113-1115`） |

- **响应**：`{skills:[卡片], plugins?}`。卡片字段（`skill_manager.py:6889-7004`）：frontmatter 全字段 + `name/description/version/tags/allowed_tools/source/display_name/installed/enabled/is_builtin/is_builtin_source/proprietary/has_evolutions/skill_type/display_name_zh|en/description_zh|en` + 安装账本身份键（`installation_id/source_type/source_id/origin/installed_at/removable/sync_status/consistency` 等）；SkillPack 卡另含 `member_count/requested_enabled/config/blocked_members`（`skillpack.py:312-333`）。
- **特殊**：每次列举自动补登记手动拷入 skills 目录的本地技能（`skill_manager.py:1100-1106`）；企业版 builtin/marketplace 扫描为空（`:1110-1111`）。

#### skills.installed

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无（传入被忽略）。
- **响应**：`{plugins:[...], skills:[...]}`——**含两个数组**。`plugins[]`（`skill_manager.py:1176-1184`）：`{plugin_name, marketplace, spec, installed_at, git_commit, enabled, skills:[{name, version}]}`（version 读 `.archive` current_version，损坏为 null；仅含 marketplace 来源，历史字符串 spec 回 null）；`skills[]`（`:1186-1190`）为 prebuilt/user/builtin 的安装账本 DTO（`_skill_installation_dto`，`:12111-12125`）。

#### skills.get

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 缺失 → 无 code 错误"缺少参数: name"；`_safe_path_name` | 技能名。定位顺序：workspace 精确名 → frontmatter 规范名 → 容器包成员 → 市场未装副本 → 白名单内置（`skill_manager.py:7567-7670`） |
| `version` | string \| null | 否 | 缺省/None/**空串**均视同 null → 读 workspace 副本；非空 strip 后只读 `.archive` 版本副本，**不回退** | 找不到 → `SKILL_VERSION_NOT_FOUND` |

- **响应**（`skill_manager.py:1289-1343`）：frontmatter 全字段 + `content`（正文，本地相对图片改写为受控预览 URL，`skill_content_images.py:170-228`）+ `file_path/source/display_name/enabled/config{enabled}/version/skill_type` 等；`skill_type=="skillpack"` 时追加投影 `member_count/requested_enabled/config/blocked_members` + `skillpack{workflow_graph, members}`。
- **错误**：`SKILL_NOT_FOUND` / `SKILL_VERSION_NOT_FOUND` / `SKILL_VERSION_CONTENT_INVALID` / `SKILL_INVALID_METADATA`（SkillPack 校验失败）。

#### skills.versions.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 + `_safe_path_name` | 仅解析本地 skills 目录（`skill_manager.py:1357`） |

- **响应**（`archive_store.py:162-228`）：`{success, name, default_version, versions:[{version, is_default, source(缺省 "skillhub"), available, created_at, updated_at}]}`；`available` = 版本目录含 SKILL.md 或任一 `*.md`；按 `created_at` 降序；默认版本不在列表中时（索引损坏）报 `SKILL_VERSION_CONTENT_INVALID`。只读 `.archive`。

#### skills.files.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 + `_safe_path_name` | 技能名（仅 workspace 副本） |
| `version` | 任意 | 条件拒绝 | **键存在且值非 None（含空串）即拒绝**："skills.files.list 不接受 version 参数"（`skill_manager.py:1369-1370`） | 显式不支持版本查询 |

- **响应**（`skill_files.py:101-148`）：`{name, files:[{path(POSIX 相对), type:"file"|"directory", size, mime_type}]}`；隐藏 `.archive/`、跳过 symlink 与越根条目，按路径小写排序。
- **错误**：仅 `SKILL_NOT_FOUND`（不安全 name 走无 code 的 ValueError——**本方法不会触发 SKILL_UNSAFE_PATH**）。

#### skills.files.get

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 + `_safe_path_name` | 技能名（仅 workspace 副本） |
| `path` | string | 是 | strip 非空；安全校验（`skill_files.py:158-203`）：拒绝空、绝对路径、`..`、首段 `.archive`、越根、symlink（文件或任一父目录）、目录、硬链接（nlink>1） | 文件相对路径 |
| `version` | 任意 | 条件拒绝 | 同 files.list | — |

- **响应**（`skill_manager.py:1421-1452`）：`{name, path, type:"file", mime_type, size}` + 二选一——文本（mime `text/*` 或后缀白名单 ≤512KB 且 utf-8 可解码）→ `{encoding:"utf-8", content}`；否则 → `download_url`（600s 有效，绑定会话）。
- **错误**：`SKILL_UNSAFE_PATH` / `SKILL_NOT_FOUND`（超 512KB 不报错，静默走 download_url）。

#### skills.rebuild

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 + `_safe_path_name` | 技能名 |
| `version` | string \| null | **键必填** | 键缺失 → "缺少参数: version（本地无版本 Skill 请显式传 null）"；None=workspace 重建；**空串报错**"version 为空时请传 null"；非空=指定 `.archive` 版本副本（必须已存在） | 重建目标 |
| `user_intent` | string \| null | 否 | 缺省 null；strip | 用户重建意图，拼入 Agent prompt |
| `language` | string | 否 | 缺省 "cn"；**空串也回 "cn"**；无枚举校验 | 重建指令语言 |

- **两阶段**：handler 返回 followup 载荷后，分发层在 RPC 内**同步静默执行 Agent 改写**（临时会话 `skills-rebuild-*`、非用户历史；非默认版本先备份再切换，`interface.py:2715-2813`），最终响应替换为 **`{success:true}`**（`interface.py:2633`）。
- **错误**：SkillPack → `SKILL_OPERATION_UNSUPPORTED`；内置 → `SKILL_BUILTIN_READ_ONLY`；无/无效 evolutions.json 或无可应用经验 → `SKILL_REBUILD_FAILED`；version=null 但已有产品版本 → `SKILL_REBUILD_FAILED`"该 Skill 已有产品版本，请传入 version"。
- **副作用**：归档旧版本；**经验消费后清空 evolutions.json**（workspace 与版本副本双侧，`skill_manager.py:7380,7423-7432`）；默认版本同步回 workspace；完成后刷新 skill rails。

#### skills.toggle

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 空 → `{success:false, detail:"缺少参数: name"}`；`_safe_path_name` | 技能名 |
| `enabled` | **bool（严格）** | 是 | `isinstance(enabled, bool)` 不满足即拒（HTTP 也不强转）——**字符串 "true" 被拒** | 目标启停状态 |
| `dry_run` | bool（真值） | 否 | 缺省 false；**字符串 "false" 为真 → 进入探测模式** | 不落任何状态，仅返回关联技能包 |
| `origin` | string | 否 | 缺省 null；strip | 精确匹配安装账本（单机版 prebuilt 拦截定位；企业版写 plugin enabled）——文档早期版本漏列 |

- **响应**：dry_run → `{success, name, enabled, parent_skillpacks:[包名]}`；正常 → `{success, name, enabled, config{enabled}, detail:"配置已更新；下次 reload / rebuild / 新会话后执行面生效。"}`；SkillPack 另附投影。
- **错误/特殊**：单机版 prebuilt → `{success:false, error_code:"prebuilt_not_toggleable"}`（`skill_manager.py:1532-1552`）；**含缺失/损坏成员的技能包拒绝启用**（disabled 成员不阻塞，`:1594-1602`）；SkillPack 元数据损坏 → `SKILL_INVALID_METADATA`；清单锁忙 → `code:"SKILL_INVENTORY_BUSY"`（`:308-313`）。成功后**重建 agent 实例**（`interface.py:2578-2592`）。

#### skills.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `spec` | string | 是 | 含 `@` 时 `rsplit("@",1)` 两侧均须过 `_safe_path_name`（`skill_manager.py:2602-2626`） | `plugin_name@marketplace`；**裸名命中内置目录或 `<name>@builtin` → 转内置安装**（`:2605-2615, 2628-2629`） |
| `force` | bool（真值） | 否 | 缺省 false；**字符串 "false" 为真 → 覆盖重装** | 目标目录已存在时：false → `{success:false, detail:"skill X 已存在"}`；true → 删除重装 |

- **响应**：`{success:true}` 或 `{success:false, detail}`（spec 格式错/名称空/市场未找到/clone 失败/缺 SKILL.md 等，均无独立错误码）。
- **副作用**：clone/pull 市场仓、登记 installed_plugins（name/marketplace/version/commit/source/installed_at）、刷新索引；成功后**重建 agent 实例**。

#### skills.uninstall

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 空 → `{success:false, detail:"缺少参数: name"}`；`_safe_path_name` 失败时回退从 origin 尾段提取再校验 | 技能名 |
| `origin` | string | 否 | 缺省 null；strip | 企业版按 origin 精确删除同名多条中的指定一条——文档早期版本漏列 |

- **响应**：普通路径 `{success:true, restorable:bool}`；企业 entity 路径 `{success:true, name}`——**两种形态字段不一致**（`skill_manager.py:4971,5069`）。
- **错误/特殊**：prebuilt（及企业版 builtin）→ `{success:false, error_code:"prebuilt_not_removable"}`；源码内置 → `{success:false, detail:"内置技能不允许删除"}`（无码）；被有效 SkillPack 引用 → `code:"SKILL_REFERENCED_BY_PACK"` + `referencing_skillpacks[]`（`:4974-4985`）；锁忙 → `SKILL_INVENTORY_BUSY`。
- **副作用**：内置校验通过后先做**成员备份**到包内 `_member_backup/`（`_` 前缀使扫描跳过；失败仅告警，restorable=false，`skillpack.py:505-523`）；成功后仅轻量刷新 skill rails、**不重建实例**（`interface.py:2609-2615`）。

#### 10.6.2 skills 导入 / 知识转技能 / 市场源

#### skills.pack_member.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `pack` | string | 是 | strip 非空 + `_safe_path_name`（`skill_manager.py:675-689`） | 技能包名，须为已存在的 skillpack/容器包（`:5144-5145`） |
| `name` | string | 是 | 同上 | 成员技能名；其卸载备份须在 `<pack>/_member_backup/`（容器包 `<pack>/skills/_member_backup/`，`:5147-5149`） |

- **响应**：成功 `{success:true, name, pack}`（`:5173`）；失败一律 `{success:false, detail}`（缺参/非法名/包不存在/无备份/恢复失败），**无 SKILL_* 码**。
- **特殊**：恢复是**无条件覆盖**——目标已存在直接 rmtree 再 copytree，无 force/确认参数（`:5151-5155`）；成功后补登记本地技能并刷新索引。

#### skills.import_local

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `download_token` | string | 与 `path` **二选一** | 同传或全空 → `SKILL_INVALID_PACKAGE`"download_token 与 path 必须且只能提供一个"（`:5190-5196`）；token 经校验+过期检查+会话绑定（`_session_id` 由分发层注入）；指向文件须为 `.zip`/`.skill`/`.skill.zip` | `send_file_to_user` 签发的短期令牌导入 |
| `path` | string | 与 `download_token` 二选一 | 本地分支须**绝对路径**（支持 `~` 展开、拒 symlink 与黑名单根）；URL 分支**仅 HTTPS + host 白名单**（默认 `*.obs.*.myhuaweicloud.com`，env `IMPORT_LOCAL_ALLOWED_DOWNLOAD_HOSTS` 覆盖，`:8347-8361`）、`allow_redirects=False`、超时默认 60s（env `IMPORT_LOCAL_REMOTE_TIMEOUT`） | 本地绝对路径（zip/skill 包、单 SKILL.md、含 SKILL.md 目录）或 https 归档 URL |
| `force` | bool（真值） | 否 | 缺省 false；**字符串 "false" 为真 → 覆盖**（⚠️ 与 import_upload 的宽松解析不同） | 同名已存在时是否覆盖；不覆盖 → `SKILL_IMPORT_OVERWRITE_REQUIRED` |
| `checksum_sha256` | string | 否 | 缺省 ""；仅 URL 分支校验（小写 hex 比对，`:6443-6447`） | 不匹配 → `{success:false, detail:"下载文件校验失败（SHA256 不匹配）"}`（**无 code**） |

- **响应**：成功 `{success:true, skill:{name, description, version, skill_type, source, workspace_path}}`（version 新装为 None）。
- **错误**：`SKILL_DOWNLOAD_TOKEN_INVALID` / `SKILL_INVALID_PACKAGE`（坏 zip/无 SKILL.md）/ `SKILL_UNSAFE_PATH`（zip slip/symlink）/ `SKILL_FILE_TOO_LARGE`（解包配额：总 100MB、5000 文件、单文件 32MB）/ `SKILL_IMPORT_OVERWRITE_REQUIRED` / `SKILL_BUILTIN_READ_ONLY` / `SKILL_NAME_CONFLICT`（企业版覆盖 prebuilt）。成功后**重建 agent 实例**（`interface.py:2577-2608`）。

#### skills.import_upload

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `path` | string | 是 | strip 非空（缺 → `SKILL_INVALID_PACKAGE`）；`resolve(strict=True)` 须存在且为普通文件；扩展名**仅 `.zip`**（`.skill`/`.skill.zip` 显式拒绝）；**>50MB → `SKILL_FILE_TOO_LARGE`** | HTTP 上传产物临时路径（`/file-api/skills/upload-temp` 保留待后续 RPC） |
| `overwrite` | bool | 否 | 缺省 false；**宽松字符串解析**："1"/"true"/"yes"/"on" 小写后才为真（`:5564-5570`）——"false" 为假（与 force 不同） | 同名已存在且 false → `SKILL_ALREADY_EXISTS`（注意：本方法冲突码是 ALREADY_EXISTS，区别于 import_local 的 OVERWRITE_REQUIRED） |

- **响应**：同 import_local；origin 固定 `"file-api:upload"`。

#### skills.create_from_knowledge

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `link` | string | 与 `file_path` **二选一** | 同传或全空 → `SKILL_KNOWLEDGE_INPUT_CONFLICT`（`:5405-5416`）；无格式校验 | 知识来源链接（阶段二走 `skill-omni-creation`） |
| `file_path` | string | 与 `link` 二选一 | 须存在且为文件（否则 `SKILL_INVALID_PACKAGE`）；**>50MB → `SKILL_FILE_TOO_LARGE`** | 本地知识文档绝对路径（阶段二走 `skill-creator-router`） |
| `skill_description` | string | 否 | 缺省 "" | 注入 prompt 的补充约束（不得当作 Skill 名称使用） |
| `skill_name` | string | 否 | 缺省 "" | 注入 prompt 的期望名称（未被占用则必须采用）——**软约束**；硬约束是下方早检 |

- **两阶段**：阶段一 followup 载荷对客户端不可见——分发层同步静默跑 Agent（临时会话 `skills-knowledge-*`、`log_as_user=False`）后以 finalize 结果替换响应。最终成功：`{success:true, skill:{name, description, version(None), skill_type, source("local"), workspace_path, content(SKILL.md 全文)}}`。
- **错误**：`SKILL_KNOWLEDGE_INPUT_CONFLICT`；**同名早检**（候选 = skill_name → 文件名 stem → URL 末段，`:5582-5619`）命中 → `{success:false, code:"SKILL_ALREADY_EXISTS", skill_name}`，**不提供覆盖路径**；产物无 SKILL.md → `SKILL_INVALID_PACKAGE`；元数据非法 → `SKILL_INVALID_METADATA`。

#### skills.marketplace.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无。
- **响应**：`{marketplaces:[{name, url, enabled(记录缺省 true), install_location, last_updated}]}`。⚠️ `install_location` 服务端只读不写（除非 state.json 手工携带，恒为 null）；`last_updated` 仅由 toggle 写入（UTC ISO）。

#### skills.marketplace.add

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | falsy 检查（不预 strip）+ `_safe_path_name`；重名 → `{success:false, detail:"marketplace 已存在"}` | 源名称 |
| `url` | string | 是 | falsy 检查后**原样存储，无格式/协议校验**（仅启用时经 git clone 实际验证） | git 仓库 URL |

- **响应**：`{success:true}`；**新增源默认 `enabled=false`**（"避免未经确认就触发远程同步"）。

#### skills.marketplace.remove

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | falsy + `_safe_path_name`；不存在 → `{success:false, detail}` | 源名称 |
| `remove_cache` | bool（真值） | 否 | 缺省 true；**字符串 "false" 为真 → 删缓存** | 删除 `<marketplace_dir>/<name>` 本地缓存（失败仅告警，cache_removed=false） |

- **响应**：`{success:true, name, cache_removed:bool}`。**只删源记录与缓存，不卸载从该源安装的技能**。

#### skills.marketplace.toggle

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | falsy + `_safe_path_name`；不存在 → `{success:false, detail}` | 源名称 |
| `enabled` | **bool（严格）** | 是 | `isinstance(bool)` 否则报"缺少参数: enabled (bool)"——**字符串被拒** | 目标状态 |

- **响应**：启用 → `{success, name, enabled:true, detail:"已启用并执行 git pull|clone"}`（失败则 success:false）；禁用 → `{success, name, enabled:false, cache_removed, detail}`。
- **特殊**：启用时缓存已存在则 pull、否则按 url clone，成功才置 enabled 并刷新 `last_updated`；禁用**删缓存但不卸载已安装 skill**。

#### 10.6.3 skills 在线源（online_search / skillnet / clawhub）

#### skills.online_search.search

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `query` | string | 是 | strip 非空；**缺省回退读 `q` 别名**（`skill_manager.py:2963`） | 搜索词；空 → `{success:false, partial:false, items:[], sources:[], detail:"缺少参数: query"}` |
| `limit` | int（`int()` 强转，容忍数字串） | 否 | 缺省 20；非整数报错；**夹取 1–50** | 聚合后截断条数，同时作各子源 limit |
| `cache_mode` | 任意 | 否 | 缺省=直连；**仅精确 "prefer_cache" 生效且只透传给 teamskillshub 子调用**，其他值静默忽略 | 走目录缓存 |
| `refresh` | bool（真值） | 否 | 缺省 false；**字符串 "false" 为真** | 强制刷新缓存（仅 teamskillshub） |

- **响应**：`{success, partial(恒存在), query, items:[归一化卡片], sources:[{source, status:success|error|skipped, count, detail_key?, detail?}], cache?}`。归一化卡片（`:2786-2814`）：`source, name, display_name, description, identifier, version, author, is_team_skill, native_score, category, updated_at, source_rank, fusion_score(RRF K=60), matched_sources[], exact_match, matched_source_count`；clawhub 卡另含 `owner_handle`。
- **重要**：聚合源恒定为 **teamskillshub（必参与）+ clawhub（配置了 token 才参与，否则 sources 记 `status:"skipped"` + `tokenNotConfigured`）——skillnet 不在聚合搜索内**（`:2983-3012`）。单源超时 30s（超时 → `detail_key:"skills.onlineSearch.sourceTimeout"`）。

#### skills.online_search.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `source` | string | 否 | 缺省 "teamskillshub"；枚举 `teamskillshub \| swarmskillshub \| clawhub \| skillnet`（casefold），其余 → `detail_key:"unsupportedSource"`；swarmskillshub 与 teamskillshub 走同一安装 | 目标源 |
| `identifier` | string | 是 | strip；**回退链 `identifier → asset_id → slug → url`** | 语义随 source：hub asset_id / clawhub slug / skillnet url |
| `force` | bool（真值） | 否 | 缺省 false | 透传目标源 |
| `version` / `market_url` / `tag` / `mirror_url` | 任意 | 否 | **仅 `is not None` 时透传**；`market_url` 仅 teamskillshub、`tag` 仅 clawhub、`mirror_url` 仅 skillnet | market_url 覆盖 hub base_url（缺省取 env `TEAM_SKILLS_HUB_BASE_URL`/内置默认） |
| `display_name` / `owner_handle` | string | 否 | strip 后**非空才透传**；`owner_handle` 仅 clawhub | 展示名/发布者 |
| `url` | string | 否 | **缺省用 identifier**；仅 skillnet | 技能地址 |
| `plan_id` / `confirmed` / `expected_skill_type` | 任意 | 否 | 仅键存在时透传（teamskillshub 的 SkillPack 事务/守卫）——文档早期版本漏列 | 事务提交分支 |

- **响应**：即目标源安装方法响应原样。⚠️ **teamskillshub 路径抛出的 SkillRpcError（如 `SKILL_NAME_CONFLICT`）会以 `ok:false + {error, message, code}` 穿透**——本方法并非严格"无 RPC code"。

#### skills.skillnet.search

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `q` | string | 是 | strip 非空 | 搜索词 |
| `mode` | string | 否 | **非法值（含缺省）静默回 "keyword"，不报错**（`:3136-3140`） | `keyword \| vector` |
| `limit` / `page` / `min_stars` | int（`int()` 强转） | 否 | 缺省不携带；转整数失败报错；**无钳制**（区别于 online_search 的 1–50） | 透传 skillnet-ai |
| `threshold` | float（`float()` 强转） | 否 | 缺省不携带 | vector 模式相似度阈值 |
| `sort_by` / `category` | string | 否 | 仅真值（非空串）才携带 | 排序字段/分类 |

- **响应**：`{success, query, count, skills:[{skill_name, skill_description, author, stars, skill_url, category}]}`。异常 → `detail=str(exc)`（空则 `detail_key:"skills.skillNet.errors.searchFailedFallback"`）。

#### skills.skillnet.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `url` | string | 是 | strip 非空 | 技能地址（通常 GitHub） |
| `force` | bool（真值） | 否 | 缺省 false | 覆盖安装 |
| `mirror_url` | string \| null | 否 | 缺省 null；**空串视为未提供**；非空须 http(s) + netloc + 长度 ≤2048，否则 `detail_key:"invalidMirrorUrl"` | 镜像下载基址 |

- **响应**：`{success:true, pending:true, install_id}`（uuid hex）——**异步**；任务表为模块级共享 dict（跨 SkillManager 实例可见，**进程重启即丢**）；完成 hook 重载 Agent，hook 失败 → `detail_key:"reloadRequired"`（"技能已安装，请手动刷新页面生效"——实际已落盘）。

#### skills.skillnet.install_status

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `install_id` | string | 是 | strip 非空 | install 返回的 id |

- **响应四形态**：过期 → `{success:false, detail:"安装会话已过期…", detail_key:"sessionExpired"}`；pending → `{success:true, status:"pending"}`（**无 skill 字段**）；failed → `{success:false, status:"failed", detail, detail_key?, detail_params?, code?}`；done → `{success:true, status:"done", skill:{name, source:"skillnet"}}`。

#### skills.skillnet.evaluate

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `url` | string | 是 | strip 非空 | 评估目标 |

- **响应**：`{success:true, evaluation:<skillnet-ai 结果原样>}`。**同步执行**（线程）；LLM 配置复用 `react.model_client_config`（缺 api_key → `detail_key:"evaluateNoApiKey"`；skillnet-ai 未装 → `skillnetAiMissing`）。

#### skills.clawhub.get_token

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无。**响应**：`{success:true, token:<掩码>, has_token}`——掩码规则：长度 ≤8 全 `*`，否则前4+`*`+后4（`:9914-9921`）；**响应永不回传明文**。

#### skills.clawhub.set_token

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `token` | string | 否 | 缺省 ""；strip；**空串/纯空白 = 清除** | ⚠️ 持久化为 state 明文（掩码仅用于响应，`:9909-9912`） |

- **响应**：`{success:true, token:<掩码>}`。

#### skills.clawhub.search

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `q` | string | 是 | strip 非空 | 搜索词 |
| `limit` | int（`int()` 强转） | 否 | **缺省不发送**（用服务端默认）；非整数报错；无钳制 | 透传 query param |

- **前置**：须先 set_token，否则 `detail_key:"tokenNotConfigured"`。**响应**：`{success, query, count, skills:[{slug, display_name, summary, version, updated_at, owner_handle}]}`（HTTP 超时 30s；错误 `detail_key`：`httpError`/`searchFailed`）。

#### skills.clawhub.download

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `slug` | string | 是 | strip 非空 + `_safe_path_name`（失败仅 detail，**无 detail_key**） | **兼作本地目录名与登记名**（`skill.name == slug`） |
| `owner_handle` | string | 条件 | 缺省 ""；非空才发送 | slug 多发布者时服务端要求（本地不校验） |
| `display_name` | string | 否 | 缺省 ""；非空生效 | 目录展示名（解析顺序：参数 > SKILL.md name > slug） |
| `version` / `tag` | 任意 | 否 | 真值才作为 query param 发送（version 缺省=服务端 latest） | 版本/标签 |
| `force` | bool（真值） | 否 | 缺省 false | 覆盖本地与镜像目录 |

- **响应**：`{success:true, skill:{name(=slug), display_name, source:"clawhub"}}`；**HTTP 超时 120s**。预检已安装（非 force）→ `detail_key:"skillAlreadyInstalled"`；其余 `skillMdNotFound`/`parseSkillFailed`/`httpError`/`downloadFailed`。

#### 10.6.4 skills Hub（teamskillshub / swarmskillshub）

**公共契约**：市场 HTTP 超时默认 60s（env `TEAM_SKILLS_HUB_TIMEOUT`；publish 上传固定 120s、delete 60s）；base_url 解析 `market_url → env TEAM_SKILLS_HUB_BASE_URL → 内置默认`；`raise SkillRpcError` 的 `SKILLHUB_*/SKILL_*` 码出现在 `ok:false` 响应顶层 `payload.code`（`interface.py:2647-2660`），其余失败为 `{success:false, detail, detail_key?}`；`cache_mode="prefer_cache"` 命中时响应附 `cache` sidecar（`{state: fresh|stale|miss|error, refreshing, updated_at, complete, has_more}`）。

#### skills.teamskillshub.info

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `asset_id` | string | 是 | strip 非空 | Hub 资产 ID |
| `version` | string | 是 | strip 非空 | 查询参数 |
| `market_url` | string | 否 | 缺省回落 env/内置默认 | 覆盖本次请求的 base_url |

- **响应**：`{success, asset_id, version, data}`——`data` 为 Hub `/artifacts/{asset_id}` 返回**原样透传**（无白名单，"data 结构由 Hub 决定"仅对本方法成立）。

#### skills.teamskillshub.init

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | `_safe_path_name` | 新技能目录名 |
| `path` | string | 否 | 缺省 "."；禁 `~`、禁绝对路径；resolve 后必须仍在 skills 根内且为已存在目录 | 父目录（仅 skills 目录内相对路径） |
| `skill_type` | string | 否 | 缺省 "teamskills"；别名 `plugin_type`/`type`；枚举 `teamskills \| skill` | teamskills 模板含 `kind: team-skill` 与 roles(role_01/02) |
| `force` | bool（真值） | 否 | 缺省 false；**字符串 "false" 为真 → 清空重建** | 目标目录非空时：false 拒绝；true 先 rmtree 再重建 |

- **响应**：`{success:true, path}`（目标目录绝对路径）。失败一律 `{success:false, detail}` 无码。

#### skills.teamskillshub.validate

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `path` | string | 是 | **必须绝对本地路径**；禁 URL 协议、禁 symlink（含目录内遍历）、禁黑名单根；须存在且为目录；须含 SKILL.md 且 frontmatter 有非空 name+description（name 另过 `_validate_skill_name` ≤128 等） | 待校验的技能根目录 |
| `skill_type` | string | 否 | 别名 `plugin_type`/`type`；枚举 `teamskills \| skill`；缺省按 frontmatter `kind=="team-skill"` 推断 | teamskills 时执行 roles 契约校验：roles 非空 list、每项含非空字符串 id、有效 id ≥2、不重复 |

- **响应**：成功 `{success, path, skill_file, skill_type, name, warnings}`（**warnings 恒为 []**）；roles 失败 `{success:false, detail, errors:[逐条明细]}`。

#### skills.teamskillshub.pack

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `path` | string | 是 | 同 validate 的绝对路径安全校验 | 技能根目录（zip 名 = 目录名） |
| `skill_name` | string | 否 | 缺省取 SKILL.md frontmatter name；**name/description 任一为空 → `code:"SKILL_INVALID_METADATA"`** | 打包标识 |
| `version` | string | 否 | 缺省 "1.0.0" | 写入 plugin.yaml |
| `display_name` | string | 否 | 缺省 SKILL.md display_name → skill_name | 市场展示名 |
| `output` | string | 否 | 缺省 "out"；相对路径落在 skill_root/output，绝对路径直接 resolve（自动创建） | 输出目录 |

- **响应**：`{success:true, path(zip 绝对路径), checksum_sha256}`。

#### skills.teamskillshub.search

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `q` | string | 否 | 缺省 ""（全量）；非空才上送 | 关键词 |
| `page_size` | int（`int()` 强转，别名 `limit`） | 否 | 缺省 20；**夹取 1–100**；非整数报错 | 每页条数 |
| `page` | int | 否 | 缺省 1；≥1 | 页码 |
| `skill_type` | string | 否 | 别名 `plugin_type`；**本地无枚举校验**（Hub 侧定义；team={swarmskill,teamskills,…}、single={skill,skillpack}，未知类型项被丢弃） | 类型过滤 |
| `author` | string | 否 | 缺省 ""；非空作 `publisher_name` | 按发布者过滤 |
| `order_by` | string | 否 | 缺省 install_count；**本地无枚举校验**（透传，Hub 侧校验） | 排序字段 |
| `desc` | 任意 → bool | 否 | 缺省 true；**唯一做字符串布尔解析**："1"/"true"/"yes"/"on" 才为真 | 降序开关 |
| `cache_mode` / `refresh` | — | 否 | 仅 "prefer_cache" 生效（`search_publisher_id` 非空时禁用）；refresh 仅 prefer_cache 下有意义 | 目录缓存 |
| `search_asset_id` / `search_asset_type` / `search_publisher_id` | string | 否 | 缺省 ""；非空作查询参数——文档早期版本漏列 | 精确过滤 |
| `market_url` | string | 否 | 同 info | 覆盖 base_url |

- **响应**：`{success, query, count, skills:[{asset_id, name, display_name, summary, version, author, category, native_score, updated_at, plugin_type, is_team_skill, skill_type}]}` + 可选 `cache`。失败 `detail_key:"skills.teamskillshub.errors.searchFailed"`（无 code）。

#### skills.teamskillshub.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `asset_id` | string | 是 | strip 非空 | Hub 资产 ID |
| `version` | string | 否 | 缺省（None/空）不带 version → 取远端最新 | 指定版本；产物身份不符 → `SKILL_INVALID_PACKAGE` |
| `force` | bool（真值） | 否 | 缺省 false；**本期无效**（`_ = force`，"仅保留兼容字段，本期禁止用于删除覆盖"，`:4271-4272`） | 即使 true 也不得覆盖同名本地 Skill |
| `display_name` | string | 否 | 缺省取 skill_name | 仅新安装写入登记 |
| `output` | string | 否 | 缺省 ""=默认 skills 目录；非空 resolve（**相对路径按进程 CWD 解析**）；命中时跳过登记与镜像复制 | 自定义安装根 |
| `market_url` | string | 否 | 同上 | 覆盖 base_url |
| `plan_id` | string | 否 | 需 `confirmed:true` 且 `SKILLPACK_PRODUCTIZATION_ENABLED=true`，否则拒绝 | SkillPack 事务提交分支 |
| `expected_skill_type` | string | 否 | `=="skillpack"` 时对非 pack 包拒绝 | 内部守卫 |

- **响应**：新装 `{success:true, skill:{name, display_name, source:"teamskillshub", asset_id, path}}`；**幂等命中（同 asset 重复安装）返回的 skill 无 `display_name`**。
- **幂等/冲突**：同 asset 已装 → 直接成功；dest 存在但 asset 不同 → `SKILL_NAME_CONFLICT`"本地已存在同名 Skill"。
- **错误**：`SKILLHUB_INSTALL_FAILED`（download_url 缺失/格式错/无版本号）、`SKILL_INVALID_PACKAGE`（无 SKILL.md/身份不一致）、`SKILL_OPERATION_UNSUPPORTED`（skillpack 未启用/output 冲突）、`SKILL_INVALID_METADATA`、下载 host 白名单（env `TEAM_SKILLS_HUB_ALLOWED_DOWNLOAD_HOSTS`，默认 OBS 域）、锁忙 `skills.inventoryBusy`。

#### skills.teamskillshub.publish

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `token` / `system_token` | string | **二选一（XOR）** | 同传或全不给 → "请且仅请提供一种鉴权"；**不回落环境变量**（区别于 recommend）；system_token → `X-System-Token`，否则 `Authorization: Bearer` | Hub 凭证 |
| `version` | string | 是 | strip 非空 | 发布版本号 |
| `path` / `file` | string | **二选一** | file 须为存在的 `.zip`；path 须为含 SKILL.md 的目录；上传前统一经 build_asset_package 规范化 | 目录或 zip 包 |
| `skill_id` | string | 否 | 缺省不带上送 | 更新场景传已有资产 ID |
| `version_desc` | string | 否 | 缺省 "" | 版本描述 |
| `force` | bool（真值） | 否 | 缺省 false | **双重语义**：跳过本地"与 last_published_version 同版本"冲突（否则 `SKILL_PUBLISH_VERSION_CONFLICT`）；并以 force 字段上送 Hub |
| `market_url` | string | 否 | 同上 | 覆盖 base_url |

- **响应**：`{success:true, skill_id, name, version}`（skill_id 取 Hub 返回 asset_id/plugin_id 或回落入参）；成功后本地写 `remote_asset_id`/`last_published_version`。上传超时 **120s**；兜底 `detail_key:"publishFailed", code:"SKILLHUB_PUBLISH_FAILED"`。

#### skills.teamskillshub.delete

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `skill_id` | string | 是 | strip 非空 | 资产 ID |
| `token` / `system_token` | string | **二选一（XOR）** | 同 publish（不回落环境变量） | 凭证 |
| `version` | string | 否 | 缺省 "all" | 删除指定版本或全部 |
| `market_url` | string | 否 | 同上 | 覆盖 base_url |

- **响应**：`{success:true, skill_id, version}`；失败 `detail_key:"deleteFailed"` 无 code（超时 60s）。

#### skills.swarmskillshub.recommend

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `top_k` | int（别名 `limit`） | 否 | 缺省 10；**夹取 1–500**；非整数报错 | 推荐数量 |
| `user_id` / `request_id` / `category_id` | string | 否 | 缺省 ""；request_id 响应优先回填 Hub 返回值 | 个性化上下文；空 user_id 走匿名 TopK |
| `timestamp` | 任意 | 否 | 仅非 None 才写入 body——文档早期版本漏列 | 透传 |
| `plugin_type` | string | 否 | 别名 `skill_type`；逗号分隔多值去重，`teamskills`→`swarmskill` 归一 | 类型过滤 |
| `cache_mode` / `refresh` | — | 否 | 仅 "prefer_cache"；**显式上下文（user_id/request_id/timestamp/publisher_id/凭证任一）且无 session 时绕过缓存直连**；私有缓存仅进程内 TTL≤60s，公共匿名走磁盘 | 缓存 |
| `token` / `system_token` | string | 否 | **有环境变量回落**（`TEAM_SKILLS_HUB_SYSTEM_TOKEN` 优先于 `TEAM_SKILLS_HUB_USER_TOKEN`，区别于 publish/delete）；**完全无凭证不报错，匿名 POST** | 凭证 |
| `language` / `locale` | string | 否 | 仅参与缓存 key，不上送——文档早期版本漏列 | — |
| `market_url` | string | 否 | 同上 | 覆盖 base_url |

- **响应**：`{success, request_id, user_id, source, category_id, plugin_type, count, skills, items}`（**skills 与 items 是同一列表**）；每项字段：`asset_id, name, display_name, summary, short_desc, version, latest_version, updated_at, plugin_type, skill_type, tags, score, publisher_name, author, install_count, like_count, view_count, icon_uri, category_id, category_name`。失败 `detail_key:"recommendFailed"` 无 code。

#### skills.swarmskillshub.detail

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `asset_id` | string | 是 | `_safe_path_name`；非法 → `code:"SKILLHUB_DETAIL_FAILED"` | 查询的资产 ID |
| `market_url` / `token` | — | **不可用** | **不读取 params**（"普通页面不得覆盖 market_url / token"）；base_url 用后端配置、鉴权用服务端 env 凭证 | 统一服务端凭证 |

- **响应**：`{success:true, asset_id, version, data}`——`data` 为**固定 26 字段白名单**（asset_id/version/asset_type/plugin_type/name/display_name/short_desc/detail_desc/icon_uri/publisher_*/tags/category_*/certification/changelog/install_count/view_count/like_count/star_count/review_count/average_rating/create_time/update_time/review_summary/review_sections），**非整体透传**（"data 结构由 Hub 决定"对本方法不成立）。
- **detail_desc 回填**：Hub 详情缺失时下载公开产物提取 README/SKILL.md 正文，**上限 200×1024 字符（截断不失败，仅作用于回填正文）**；回填失败静默 ""。错误：`SKILLHUB_DETAIL_NOT_FOUND`（资产不存在/无公开版本）/ `SKILLHUB_DETAIL_FAILED`（detail 截 300 字符，其余方法 500）。

#### 10.6.5 skills 检索 / 演化 / 图谱 / 经验

#### skills.retrieval.status

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：无（`_session_profile` 为服务端注入，客户端传入的同名键被覆盖）。
- **响应字段**（`skill_manager.py:2087-2119`）：`enabled, index_enabled(=enabled), mode(auto|flat), candidate_scale(small|large), estimated_candidate_tokens, candidate_budget_tokens, effective_strategy(legacy|small_full|large_flat|indexed|indexed_stale), layout, index_state, index_required(恒 false), index_recommended(恒 false), build_supported, build_status(缺省 idle), build_stage, build_progress(float), build_message, build_error, build_id, build_logs(⚠️ 实际恒 []——状态链路以 include_logs=False 加载，"末 50 条"为死逻辑), index_exists, fresh, indexed_count, installed_count, installed_enabled_count, searchable_count, tool_name(恒 "skill_index"), profile_scope, profile_session_id, profile_model_name, pinned_index_revision, profile_recovery_error`。

#### skills.retrieval.index_build

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `force` | bool \| string | 否 | 缺省 false；**宽松字符串解析**："1"/"true"/"yes"/"on" 为真，其余字符串（含 "false"）为假——与 bool() 真值语义不同 | 强制重建（即使索引新鲜） |

- **响应**：`{success, background(=success), mode, effective_strategy, build_status(缺省 "running"), build_id, detail}`。
- **分支**：检索未启用 → `{success:false, error_code:"skill_retrieval_disabled", effective_strategy:"legacy"}`；候选规模 small → `{success:false, error_code:"skill_index_not_needed"}`；可用技能为空 → core 层 `empty_inventory`（**该 code 不透出**）；已在构建 → 幂等成功复用。

#### skills.retrieval.index_cancel

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `build_id` | string | 否 | strip 后空则置 None；**缺省=取消当前活跃构建**；与活跃 id 不一致 → `success:false + "Build id mismatch..."`（core 码 build_id_mismatch 不透出） | 指定构建 |

- **响应**：`{success, mode, build_status, build_id, detail}`；无运行构建也幂等成功（`build_status:"idle"`）。取消不删除已发布索引；开关关闭后仍可作清理调用。

#### skills.retrieval.search

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `query` | string | 是 | strip 非空 | 检索关键词，走 Agent 同款 `skill_index` 工具（内部固定 limit 10） |

- **响应**：`{success(=无 error), mode, query, result(工具输出), detailed_output(诊断 dict)}`；空 query → `{success:false, mode, detail:"query must not be empty", matches:[]}`（无 error_code）。

#### skills.retrieval.tree

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：忽略全部。**响应恒定**：`{success:false, mode, unsupported:true, tree_supported:false, nodes:[], detail:"Browse the Skill taxonomy through skill_index list operations."}`。

#### skills.evolution.status / get / save

#### skills.evolution.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 + `_safe_path_name`；**技能不存在不报错**（返回 `exists:false`） | 技能名 |
- **响应**：`{name, exists}`。

#### skills.evolution.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 同上 | 技能名 |
- **响应**：`{name, exists, valid, skill_id(缺省回落 name), version(缺省/非法回落 "1.0.0"), updated_at, entries[]}`——entry 含 `id, source, timestamp, context, change{section, action, content, target, ...}, applied, score, usage_stats{...}`；无法解析项**静默跳过**；文件损坏 → `exists:true, valid:false, detail`。

#### skills.evolution.save

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 同上 | 技能名 |
| `entries` | array | 是 | 必须为数组；**每项须为对象、`id` 非空字符串、`change.content` 为字符串**；`action ∈ {append, merge, replace, skip}`（非 skip 时 `section` 须 ∈ 八个合法段），其余字段补默认（source="unknown"、score=0.6）——**整体覆盖写入** evolutions.json | 条目列表 |
- **响应**：`{success:true, name, entry_count, updated_at}`；**双写版本副本可回滚**（任一步失败双侧回滚）；成功后轻量刷新 skill rails。
- **错误**：技能不存在 → `SKILL_NOT_FOUND`；内置 → `SKILL_BUILTIN_READ_ONLY`；SkillPack → `SKILL_OPERATION_UNSUPPORTED`。

#### skills.graph.build / status / get / cancel

#### skills.graph.build

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `force` | bool \| string | 否 | 缺省 false；**宽松字符串解析**："1"/"true"/"yes"/"on" 为真，其余字符串（含 "false"）为假 | 强制重建（绕过指纹复用） |
- **响应**：`{success:true, background:true, build_status:"running", graph_dir, build_log(末 80 条), build_progress{stage,label,percent,status}, llm_token_usage, build_error?}`；后台单任务守卫（已在构建 → 复用返回 running）。

#### skills.graph.status

- **参数**：无。**响应**：`GraphStatus.to_dict`（`success, graph_dir, exists, stale, skill_count, changed_count, added_count, removed_count, resume_available, checkpoint_dir, detail`）+ build 日志三件套；构建失败时 detail 被 build_error 覆盖。

#### skills.graph.get

- **参数**：无。**响应**：成功 `{success:true, graph_dir, graph_manifest, orchestration_min_edge_confidence, skills[](已剔除禁用), graph{nodes, edges, pack_nodes, pack_edges}, diagnostics}`；缺失 `{success:false, graph_dir, detail:"技能总谱不存在或不完整，请先构建总谱。", error}`。

#### skills.graph.cancel

- **参数**：无（**force 被忽略**）。**响应**：无任务 → `{success:false, cancelled:false, build_status:"idle", detail}`；成功 → `{success:true, cancelled:true, build_status:"cancelled", detail:"已取消…缓存和 checkpoint 会保留。"}`。

#### skills.experience.list / request

#### skills.experience.list

- **参数**：无。**响应**：`{success, enabled, candidates:[{recipe_id, recipe_version, name, applicability, structure[[source,target,relation]...], execution_count, success_count, success_rate}]}`；**三分支**：SkillPack 产品化开启 → `{success:false, reason:"scoped_skillpack_tool_required"}`；flow 未启用 → `{success:true, enabled:false, candidates:[]}`。

#### skills.experience.request

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `recipe_id` | string | 是 | strip 非空 | 保留候选的 Recipe id |
| `recipe_version` | int \| 数字字符串 | 是 | 接受 int 或可 `int()` 的 str；**bool 显式拒绝**；`version ≥ 1` | Recipe 不可变版本号 |
- **params 白名单**：仅 `{recipe_id, recipe_version}`（+服务端注入 `_session_id/_channel_id`）；任何多余键（如 `reason`）→ `{success:false, reason:"invalid_parameters"}`。其余 reason：`invalid_recipe_version`/`invalid_recipe`/`scoped_skillpack_tool_required`/`flow_disabled`/`candidate_not_found`。成功 `{success:true, request_id("symphony_experience_"+sha256[:20]), candidate}`；**经审批提示流再次发出**。

#### 10.6.6 assets.publish.\*（资产发布）

**公共约束**（`server/runtime/marketplace/asset_publish_api.py`，下称 api）：

- 仅 `channel_id=="web"`（否则 `WEB_CHANNEL_REQUIRED`）；params 禁止键 `path/scope/workspace/hub_url/market_url/publisher_id/user_id` → `INVALID_PARAMETERS`（api:253-257）。
- `auth`：`{oauth_provider ∈ {gitcode, github}, access_token}`；token 非空 str ≤8192 且可打印 ASCII（api:227-242）→ 违规 `AUTH_REQUIRED`。例外：`local_status` 不需要；`describe` 在 auth 缺省或 token 为空时走**本地回退**（records 空）。
- `kind` 枚举 `{skill, skillpack, agent_template, agent_group, plugin, mcp}` → 非法 `INVALID_KIND`（api:260, 311）。
- `local_id`：非空 ≤256、非 `.`/`..`、不含 `/ \ \0` → 否则 `RESOURCE_NOT_FOUND`（api:32-41）。
- 错误外壳：`ok:false + {code, error, can_submit:false, errors:[{code, field}]}`；未归类异常 → `PUBLISH_UNAVAILABLE`（agent_ws_server.py:10357-10362）。
- 限额：draft TTL **1800s**、每 scope draft ≤64（`draft_limit`）、artifact ≤512MB（`artifact_limit`）、上传前复核 ≤50MB（`file_too_large`）（asset_publish_service.py:39-41, 375-381）。

#### assets.publish.describe

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `kind` | string | 是 | 枚举六值 | 资产类型 |
| `local_id` | string | 是 | `_safe_id` | 本地已安装资产标识（服务端解析安装根） |
| `auth` | dict | 条件 | 缺省/token 空 → 本地模式 | skillpack 本地模式仍会查 hub：401/403 → `AUTH_REQUIRED`，其余 → `SKILLPACK_HUB_CHECK_FAILED`（api:283-294） |

- **响应**：`{kind, local_id, defaults, can_publish, hub_url, errors, records, identity_verified:false}`；`defaults={asset_name, version, display_name, description, tags, visibility:"public"}`，version 非 `x.y.z`/7 位 hex 回退 `1.0.0`。非 skillpack 的 `can_publish` 恒 true。

#### assets.publish.prepare

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `kind` / `local_id` / `auth` | — | 是 | 见公共约束 | — |
| `metadata` | dict | 是 | **白名单键** `asset_name/version/display_name/description/tags/version_desc/visibility/dependency_sources`，多余键 → `INVALID_METADATA`；`asset_name` 非空无首尾空白、`version` 须 `x.y.z` 或 7 位 hex（违规 → `INVALID_PACKAGE`） | 资产元数据 |
| `target_asset_id` | string | 否 | 非空无首尾空白 | 更新已有 Hub 资产 |
| `force` | 任意（truthy） | 否 | 缺省 false | 跳过同版本冲突检查 |

- **响应**：`{draft_id, expires_at, kind, package_name, version, target_asset_id, checksum_sha256(=artifact_sha256), size_bytes, files, excluded, normalizations, dependencies, can_submit, warnings:[], errors:[{code:"VERSION_CONFLICT", field:"version"}]?}`。skillpack 独立路径：强制 `visibility:"public"` 且禁 force（`SKILLPACK_REQUIRES_IMMUTABLE_PUBLIC_VERSION`）、成员 ≤32、目录白名单（SKILL.md/skillpack.lock.json/.archive）。

#### assets.publish.commit

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `draft_id` | string | 是 | scope 内查无 → `not_found` | prepare 产物 |
| `request_id` | string | 是 | 非空 ≤4096（skillpack ≤256）；**幂等键**——同 request_id 复用同 operation，绑定不同 draft → `request_conflict` | 幂等收据 |
| `auth` | dict | 是 | 见公共约束 | 凭据仅存于上传任务内存 |

- **响应**：即 status 记录结构（见下）。错误：`draft_expired` / `target_conflict`（同 target+version 已有进行中操作）/ `queue_full`（每 scope 活跃 ≥32）。

#### assets.publish.status

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `operation_id` | string | 是 | 查无 → `not_found` | commit 返回 |
| `auth` | dict | 是 | skillpack 先 resume 续传 | — |

- **响应**：`{operation_id, draft_id, execution_status(queued|uploading|completed|failed|unknown), created_at, updated_at, kind, local_id, package_name, version, target_asset_id, result?, error?}`；`result={asset_id, kind, package_name, version, publish_result, visibility, deduplicated}`；`error={code, http_status?, outcome_unknown?, retry_after?, message?}`。

#### assets.publish.records

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：`kind` + `local_id` + `auth`（按凭据派生 scope 隔离）。**响应**：`{records:[同 status 记录], identity_verified:false}`（created_at 倒序）。

#### assets.publish.local_status

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
- **参数**：`kind` + `local_id`（**无 auth**；资产缺失 → `RESOURCE_NOT_FOUND`）。**响应**：`{state}`，枚举 `published | pending | unpublished | unknown`（跨 scope 聚合 completed 记录）。

#### 10.6.7 mcp.\*

**公共契约**（WS 层 `agent_ws_server.py:10368+`，下称 aws；registry `server/runtime/mcp/registry.py`，下称 reg）：

- 12 个方法 WS/HTTP 双落点近乎复制；**`mcp.oauth.start` 仅 WS 层可达**（HTTP 南向 dispatch 缺项）；`wait_auth` 的 host-OAuth 分支仅 WS 层。
- 所有卡片/详情响应经 `_mask_sensitive_fields`（aws:9512-9527）：key 含 api_key/token/authorization/secret 等 12 个子串或值含 `bearer `/`api-key `/`secret-` 的非容器值替换为 `"***"`，dict/list 递归。
- 响应 payload 首字段恒为 `type`；错误码族 `MCP_INTERNAL / MCP_BAD_REQUEST / MCP_NOT_FOUND / MCP_UNREACHABLE / MCP_AUTH_FAILED` + CLI 结构化码 `MCP_RUNTIME_MISSING / MCP_INSTALL_NETWORK / MCP_CLI_INCOMPLETE / MCP_NAME_CONFLICT / MCP_CREDENTIALS_INVALID / MCP_INSTALL_SCRIPT_DEP`（reg:72-85, 1085-1128）。

#### mcp.list

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `filter` | string | 否 | 缺省 `builtin`；strip+lower；∉{builtin, local} **静默兜底 builtin** | builtin=预置目录；local=已连接预置+全部自定义 |
| `query` | string | 否 | 缺省 ""；casefold 分词 **AND** 匹配 name/display_name/description/category/tags | 搜索词 |
| `cache_mode` | string | 否 | 仅精确 `"prefer_cache"` 且 **query 为空**时走目录缓存并附 `cache`；其他值静默忽略 | 缓存策略 |
| `refresh` | bool | 否 | **严格 `is True`**——字符串 "true" 不生效 | 强刷缓存 |

- **响应**：`{type:"list", items:[卡片], cache?}`；卡片含 `id/name/package_name/display_name/description/category/integration_state/connection_state/has_bundled_skills/icon/source/installed/version/tags`。Hub 不可达（非缓存路径）→ 仅返回本地卡片不报错。

#### mcp.show

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 与 name 二选一（id 优先） | 两者皆空 → `MCP_BAD_REQUEST` | Hub asset id |
| `name` | string | 与 id 二选一 | 同上 | 运行时包名 |

- **响应**：`{type:"detail", item:{...含 tools/skills/mcp_spec/cli_spec_present/bundled_skills（脱敏）}}`；tools 空时临时连接回填。错误：`{type:"show_failed", code:"MCP_NOT_FOUND"}`。

#### mcp.install

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | strip 非空；包名校验拒绝 `.`/`..`/路径；与 builtin 冲突拒绝 | Hub asset id |

- **响应**：已装幂等早退 `{id, name, installed:true}`；新装 `{type:"installed", item, connect:{...}}`——**连接失败不致命**，嵌入 connect 子对象（type ∈ connected/auth_required/credentials_required/connect_failed/cancelled；内嵌 connect 探测失败**不回滚安装**）。⚠️ Hub 资产不存在落 `MCP_INTERNAL`（非 MCP_NOT_FOUND）。

#### mcp.uninstall

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | strip；空 → `MCP_NOT_FOUND`；未知 id **不报错**：`{removed:false, already_removed:true}` | asset id 或包名 |

- **响应**：`{type:"uninstalled", item:{id, name, removed[, already_removed]}, applied, error?}`；live 注销 15s 超时 → `applied:false` 附 error；清凭据+刷 skill rails 尽力而为。

#### mcp.connect

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 → `MCP_BAD_REQUEST` | 包名或自定义 MCP 名 |

- **响应（两阶段哨兵）**：`connected` / `auth_required`（CLI 型含 auth_url/step_index/steps_total/auth_domain/command/install；host-OAuth 型含 auth_pending/host_oauth/credential_kind）/ `credentials_required`（含 required_tokens/credential_kind/title/description/doc_url 等）/ `cancelled`（与 cancel 竞态）。
- **错误**：探测失败 → `{type:"connect_failed", code:"MCP_UNREACHABLE"}`（先写 connecting、探测通过才 flip connected，失败回滚）；KeyError → `MCP_NOT_FOUND`；CliConnectError → 结构化 code+runtime+install_cmd。

#### mcp.wait_auth

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 | — |
| `step_index` | int | 否 | 缺省 0；`int()` 强转，非数字报错；负数 clamp 0 | 多步 CLI 起始步 |

- **hold-open 轮询**：CLI 型 **200 次 × 3s ≈ 10 分钟**；host-OAuth 型 200 次 × **1s**（仅 WS 层）；每轮先查 cancel 标记；多步采用新 step_index 防死循环。
- **响应**：`connected` / `cancelled`；超时/失败 → `{type:"connect_failed", code:"MCP_AUTH_FAILED"}`。

#### mcp.cancel_connect / mcp.disconnect / mcp.delete_custom

#### mcp.cancel_connect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 → `MCP_BAD_REQUEST` | 包名 |
- **响应**：`{type:"cancelled", name}`（幂等；绝不碰已 connected）。

#### mcp.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 → `MCP_BAD_REQUEST` | 包名 |
- **响应**：`{type:"disconnected", name, applied, item(脱敏), error?}`；KeyError → `MCP_NOT_FOUND`。自定义 MCP 不删记录仅回 registered+抹凭据；市场 MCP 全量清理。

#### mcp.delete_custom

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip；builtin → `MCP_BAD_REQUEST "cannot be deleted"` | 自定义 MCP 名 |
- **响应**：`{type:"deleted", name, applied, item:{name, removed:true, was_connected}, error?}`；无 state 记录 → `MCP_NOT_FOUND`。

#### mcp.register_custom

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip；与 builtin 包冲突 → `MCP_NAME_CONFLICT`；**其余所有 params 键原样作为 config 透传** | 自定义 MCP 名 |
| `transport` | string | 是 | 枚举 {stdio, sse, http, streamable-http, streamable_http}（lower；http/streamable_http 归一化 streamable-http） | 传输类型 |
| `command` | string | stdio 必填 | 空 → "stdio transport requires command" | 可含整条命令 |
| `args` | list/str | 否 | list 原样；str 单元素；空 → 按空白拆分 command | 启动参数 |
| `url` | string | 远程必填 | 空 → "remote transport requires url" | sse/http/streamable-http |
| `headers` / `env` | dict | 否 | 键值强转 str | — |
| `timeout_s` | number | 否 | >0 才写入，int 截断 | 超时 |

- **响应**：`{type:"registered", name, item(脱敏)}`；仅持久化 state=registered 不连接；编辑已连接实例会先移除旧 live 实例。

#### mcp.save_credentials

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip 非空 | — |
| `tokens` | dict | 是 | **非空 dict**；value 为 None 静默跳过（不计入 saved_keys）、其余强转 str | Form-B `${VAR}` 占位符填值 |

- **响应**：`{type:"credentials_saved", name, saved_keys:[排序后非 None 键]}`；token 永不回显；不触发重载（需再调 connect）。

#### mcp.oauth.start（仅 WS 层）

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | strip；**仅白名单 provider（如 canva）支持 host OAuth**，其余 → `MCP_AUTH_FAILED "does not support host OAuth"` | host-OAuth 远程 MCP |
| `callback_url` | string | 是 | 必须 loopback 基址：http + host 127.0.0.1 + 有端口 + path 空/"/" + 无 query/fragment | 本地回调基址 |
- **响应**：`{type:"auth_required", name, auth_required:true, auth_url, flow, redirect_uri}`；flow TTL 600s。

#### 10.6.8 agents.\*（子 agent 配置）

**公共契约**（`server/handlers/agents.py` + `server/runtime/agent_config_service.py`，下称 acs）：所有失败为 `ok:false + {error}` **无 code**；`workspace_dir` 缺省 cwd；合并优先级 project > user > local > builtin；**不在参数字段集内的键被静默丢弃**（agents.py:203-207）。

#### agents.list / agents.get

#### agents.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `workspace_dir` | string | 否 | 缺省 cwd | 定位 project/local agent 目录 |
- **响应**：`{agents: [AgentDefinition...]}`——含被 shadow 的同名项，按 source 排序 builtin→local→user→project。

#### agents.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 缺省 `""` → 查不到报错 | agent 名 |
| `workspace_dir` | string | 否 | 缺省 cwd | — |
- **响应**：`{agent: AgentDefinition}`（仅活跃未 shadow 版本）；不存在 → `"Agent 不存在: {name}"`。

#### agents.create

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 正则 `^[a-zA-Z0-9_-]{3,50}$`；同名 builtin 拒绝 | strip 后校验 |
| `description` | string | 是 | 无校验 | LLM 生成输入之一 |
| `prompt` | string | 条件必填 | schema 必填；`generate=true` 且 LLM 成功时服务端覆写 | system prompt |
| `location` | string | 是 | 枚举 `user\|project\|local`，非法 ValueError | 写入目标目录 |
| `generate` | 任意（truthy） | 否 | **默认 true**；需 name+description 均非空才触发 LLM，失败回退请求值 | LLM 生成 when_to_use/prompt |
| `model/tools/color/permission_mode/memory_scope/disallowed_tools/when_to_use/max_iterations/skills` | — | 否 | tools 缺省 `["*"]` | 其余字段 |

- **响应**：`{agent, generated, applied, reload_error}`；成功后写 `react.subagents.<name>.enabled=true` 并热 reload（失败 `applied:false`+`reload_error`）。

#### agents.update / delete / enable / disable / tools_list

#### agents.update

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 定位键（**不可改**）；不存在/builtin → 报错 | agent 名 |
| `description`/`when_to_use`/`prompt`/`model`/`tools`/`color`/`permission_mode`/`memory_scope`/`disallowed_tools`/`max_iterations`/`skills` | — | 否 | None=不改（**不含 name/location**；未知键静默丢弃） | 更新字段 |
| `generate` | 任意（truthy） | 否 | 缺省 **false**；需 name+description | LLM 生成 |
- **响应**：`{agent, generated, applied, reload_error}`；builtin 拒绝"不能修改内置 agent"。

#### agents.delete

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | 缺省 `""` | agent 名 |
| `workspace_dir` | string | 否 | 缺省 cwd | — |
- **响应**：`{ok, applied, reload_error}`——**目标不存在返回 `ok:true` + 内层 `payload.ok:false`**（不报错）；builtin 拒绝；成功后从 config 移除条目。

#### agents.enable / agents.disable

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是 | `str()` 强转+strip；空 → `BAD_REQUEST "agent name is required"` | agent 名（enabled 由方法名固定） |
- **响应**：`{name, enabled, applied, reload_error}`；builtin 拒绝。

#### agents.tools_list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `workspace_dir` | string | 否 | 缺省 cwd（实际未用于枚举——工具表为静态） | — |
- **响应**：`{tools:[{name, internal_name, description, group}], groups, disallowed_for_subagents}`。

#### 10.6.9 agent_groups / agent_templates / plugin_packages（装备包）

**公共契约**（三族共用 `extension_package_manager.py`，下称 EPM；分发入口 `interface.py:2978-3159`）：

- **主键**：`id` 为主键、`name` 为兼容别名——**别名仅对 show/file.\* 与 agent_groups.uninstall 生效**；`agent_templates.update/delete/uninstall` 与两族 install/uninstall 的实际操作**只认 `id`**。
- **名称安全** `_reject_package_name`（EPM:112-140）：拒绝空、`.`/`..`、点开头隐藏名、含 `/`/`\`、绝对路径。
- **文件预览**（file.read）：白名单 `.md/.mdx/.json/.py/.pdf`；路径任一段不得以 `.` 开头、拒绝 `..`/绝对路径/symlink 段；文本 ≤1MiB 且经敏感信息脱敏；**本地 PDF → 600s `download_url`（无 1MiB 限制）；Hub ZIP 内 PDF → base64 data URL（≤1MiB）**；非 UTF-8 文本返回占位串 `[二进制文件，大小 N bytes]`（不报错）。
- **导入限额**（import_local）：解压文件数 ≤10000、未压缩总量 ≤512MiB、路径深度 ≤32 段；拒绝绝对/`..` 成员、symlink 与特殊文件；path 须绝对路径、存在、非 symlink、不含 `://`（不支持 URL）。
- **布尔**：`include_team_compatibility` **严格 `is True`**（字符串 "true" 无效）；`refresh`/`force` 为 bool() 真值。

#### agent_groups.list（专家团列表）

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `filter` | string | 否 | 缺省不过滤；枚举 `builtin`（source==builtin）/`local`（source==local 或 builtin/hub 且已装）/`builtin+hub`（source∈{builtin,hub}）/`mine`（source==local 或已装）；其它值忽略；**filter∈{local,mine} 时不请求 Hub** | 来源过滤 |
| `query` | string | 否 | 缺省不过滤；空格分词 AND 匹配 name/displayName/description/category/tags（casefold） | 关键词 |
| `cache_mode` | string | 否 | 缺省=直连 Hub 分页搜索（≤100 页）；**仅 "prefer_cache" 且 query 为空时走缓存**（TTL 600s） | 目录缓存 |
| `refresh` | bool（真值） | 否 | 缺省 false | 仅 prefer_cache 模式生效，强制重载 |

- **响应**：`{agentGroups:[卡片]}` + 可选顶层 `cache`。本地卡（EPM:2609-2726）：`id, name, displayName, displayDescription, category, tags, source, installed, avatar, memberCount, members[{id, agentTemplateId, displayName, role, avatar}], skills[{id, displayName}], capabilities{canUse, canInstall, canUninstall, canPreviewFiles, canEdit, canPublish}`；Hub 卡同构但 `source:"hub", installed:false, members:[], skills:[]`。Hub 拉取失败降级仅返回本地卡。

#### agent_groups.show

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `name` 兼容别名；解析顺序 local→built_in→resources | 专家团包 ID |

- **响应**：`{group: 卡片}` = list 卡 + 详情字段：`version, updatedAt(manifest mtime), details(README/description), persona, leaderId(恒 "leader"), quickInputs, disabledRuntimes[]`。不存在 → not found；Hub 身份不匹配 → `HUB_ASSET_NOT_FOUND`。

#### agent_groups.file.list / file.read

| 方法 | 参数 | 说明 |
|------|------|------|
| `file.list` **透传** | `id` | 响应 `{tree:[...]}`：目录 `{path(尾"/"), type:"dir", children[]}`、文件 `{path, type:"file", size, previewable}`；隐藏点文件与 symlink 排除；Hub 包先映射 runtime id，本地无定义时下载 ZIP 预览（LRU 8 份进程缓存） |
| `file.read` **透传** | `id` + `path`（缺省 ""） | 响应文本 `{path, content}`（脱敏）/ PDF `{path, content:null, download_url}`；规则见公共契约"文件预览" |

#### agent_groups.create

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `_reject_package_name`（违规 → `AGENT_GROUP_NAME_INVALID`） | 包 ID |
| `name` | string | 是 | 非空 strip；casefold 全库查重 → `AGENT_GROUP_DUPLICATE` | 展示名 |
| `description` / `persona` | string | 是 | 非空 | persona 写入 manifest instruction |
| `leaderId` | string | 是 | 非空 + 包名校验；缺失 → `AGENT_GROUP_LEADER_REQUIRED`；须为已存在的 Team 兼容专家模板 | 领队模板 ID |
| `memberIds` | list[string] | 是（必须为 list） | 缺失/非 list → `AGENT_GROUP_MEMBERS_REQUIRED`；逐项校验（见错误码总表） | 成员模板 ID 列表 |
| `skills` | list[string] | 否（但必须为 list） | 每项须为工作区已存在 skill；去重 | 复制进包 `skills/` |
| `quickInputs` | list[string] | 否 | — | 双语化 {zh,en} |
| `tags` | list[{zh, en, id?}] | 否 | zh/en 非空 | — |
| `category` | string | 否 | 缺省 "" | — |
| **禁止字段** | — | — | `model/mcps/pluginNames/apiKey/apiBase/path` 任一存在即报 `unsupported agent_group field` | — |

- **响应**：`{id: package_id}`；创建后为**未安装**定义（installed=false, source=local）。
- **成员模板校验**：找不到 → `AGENT_GROUP_MEMBER_NOT_FOUND`；package_type 错 → `AGENT_GROUP_TEMPLATE_INVALID`；Team 加载失败 → `AGENT_GROUP_MEMBER_INCOMPATIBLE`；成员含 AGENT.md → `AGENT_GROUP_MEMBER_AGENT_MD`。

#### agent_groups.import_local

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `path` | string | 是 | 绝对路径、存在、非 symlink、不含 `://`；支持目录/`.zip`/`.tar`/`.tar.gz`；导入限额见公共契约 | 本地包位置 |

- **响应**：`{id: package_id}`，**随后立即自动 install**（⚠️ 与 templates.import_local 相反）；组特有校验：manifest package_type=agent_group、展示名查重、manifest name=目录名（不符时自动改名规范化）、`load_agent_group_package` 通过。

#### agent_groups.install / uninstall

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `install` | `id` | 本地无定义时下载 Hub 资产校验后安装（installed=true）。**无 connector 门、无超时**（区别于模板/插件）。响应 `{}`；not found → `AGENT_GROUP_REQUEST_FAILED` 兜底 |
| `uninstall` | `id`（`name` 兼容别名） | **先删定义、后停运行 Team**（仅停 organization/expert Team；文件删除失败则不停任何运行时）；仅查 local/built_in。响应 `{}` |

#### agent_templates.list（专家/模板列表）

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `filter` / `query` / `cache_mode` / `refresh` | — | 否 | 语义同 agent_groups.list | 同上 |
| `include_team_compatibility` | bool | 否 | **严格 `is True`**（字符串 "true" 无效）；仅给**本地卡**附加 `teamCompatible:{leader:bool, member:bool}`（member=false 当存在 AGENT.md） | Team 兼容标记 |

- **响应**：`{templates:[卡片]}` + 可选 `cache`。本地卡（EPM:788-803）：`id, displayName, displayDescription, category, tags, source, avatar, installed, connection_state, [teamCompatible], [runtime:{provider_name}]`；Hub 卡：`id, packageName, displayName, ..., source:"hub", installed:false, connection_state:"disconnected", version`。`connection_state` 语义（EPM:681-704）：无 connector → installed?connected:disconnected；否则各 MCP 状态 AND——任一 connecting→connecting、全 connected→connected、否则 disconnected。

#### agent_templates.show

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | `name` 别名；Hub id 先查状态店映射 | 模板 ID |

- **响应**：`{template: 卡片}`（本地 EPM:820-879）：`id, displayName, displayDescription, source, avatar, version, details(=description), tags, skills[], tools[], rails[], mcps[], persona, installed, connection_state, pending_connectors[], quickInputs[], runtime?`；Hub 未装卡另含 `packageName`（details 空时回读公共 README）。

#### agent_templates.file.list / file.read

- 参数与响应同 agent_groups.file.\*（Hub ZIP 预览共用同一 LRU 缓存与校验）。

#### agent_templates.create

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `id` | string | 是 | 包名校验（违规为普通 ValueError，**非 AGENT_GROUP_\* 码**） | 模板 ID |
| `name` / `description` / `persona` | string | 是 | 非空 | persona 写 `persona/{id}.md` |
| `skills` | list[string] | **是（必须为 list，可为空）** | 非 list 即拒"missing or invalid skills"；每项须为已存在 skill | 技能清单 |
| `mcps` | list[string] | 否 | 缺省 []；去重 | connector 名单 |
| `quickInputs` / `tags` | list | 否 | 缺省 [] | 同 groups |
| `runtime` | `{provider_name, config}` | 否 | 必须为 dict；`provider_name ∈ {claudecode, codex}`；**设定时禁止 connectors**（mcps 非空即拒）并自动附加"三方Harness/External Harness"标签 | 外部 Harness 运行时 |

- **响应**：**`{}`**（⚠️ 与 groups.create 返回 `{id}` 不同——分发层兜底分支不回传 id）。

#### agent_templates.update / delete

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `update` | `id`（只认 id）+ 同 create 的全部可选字段（全量覆写 persona/skills/manifest） | **仅 local 包可改**（marketplace source≠local 拒绝"only local packages can be edited"）；分发层先卸载运行会话中的模板实例使更新立即生效。响应 `{}` |
| `delete` | `id` | **仅 local 包可删**；永久 rmtree + 删 marketplace 条目；先卸载运行实例。响应 `{}` |

#### agent_templates.import_local

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `path` | string | 是 | 同 groups.import_local 全套校验 | 本地包位置 |

- 模板特有校验：**必须含 README.md**；persona 须为 `{"dir":"./persona"}` 形态且目录内至少一个 `.md`；skill dir 须包内相对。
- **响应**：`{id: package_id}`，**不自动安装**（与 groups 相反）。

#### agent_templates.install / uninstall

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `install` | `id` | **connector 就绪门**（EPM:3220-3243）：manifest 声明的 connector 任一未 connected → `ok:false + {error, pending_connectors:[...]}`（**响应体而非异常**，不落 marketplace）；通过后 installed=true；**整体 120s 超时**（超时报"安装超时，请稍后重试"）。企业隐藏/白名单门在安装前。external harness 包另校验：manifest 不得含 tools/rails/subagents |
| `uninstall` | `id`（name 别名仅用于运行实例定位） | 先卸载运行会话实例；仅查 local/built_in；connector MCP 记录不回收。**响应**：声明过 connector → `{"notice":"本装备依赖的 connector 仍保持连接，可在 MCP 管理页断开"}`，否则 `{}` |

#### plugin_packages.*

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。

- 与 agent_templates **同构**：list/show/file.\*/install/uninstall 共用同一分发与 gated 安装路径（`interface.py:3042-3097`，install 同 connector 门 + 120s）；`create` 差异：无 `persona`/`quickInputs`/`tags`/`runtime` 字段，其余校验同模板。

#### 错误码总表（装备包域）

| code | 触发条件 |
|------|---------|
| `AGENT_GROUP_NAME_INVALID` | id/名为空、`.`/`..`、隐藏名、含分隔符、绝对路径 |
| `AGENT_GROUP_DUPLICATE` | package_id 撞 local/built_in/resources；展示名 casefold 撞已有组 |
| `AGENT_GROUP_LEADER_REQUIRED` / `AGENT_GROUP_MEMBERS_REQUIRED` | leaderId 缺失或空白 / memberIds 缺失或非 list |
| `AGENT_GROUP_MEMBER_INVALID` / `_RESERVED` / `_DUPLICATE` | 成员项空或非字符串 / 等于 "leader" / 重复 |
| `AGENT_GROUP_LEADER_MEMBER_CONFLICT` | memberId == leaderId |
| `AGENT_GROUP_MEMBER_NOT_FOUND` / `_TEMPLATE_INVALID` / `_INCOMPATIBLE` / `_AGENT_MD` | 成员模板解析不到 / package_type 非 agent_template / Team 加载失败 / 成员含 AGENT.md |
| `AGENT_GROUP_REQUEST_FAILED` | 组 8 方法任何无 code 异常的兜底 |
| `AGENT_GROUP_NOT_INSTALLED` / `AGENT_GROUP_UNAVAILABLE` | Team 装配路径（组未安装/不可用） |
| `HUB_ASSET_NOT_FOUND` | Hub 资产不存在（同时失效 Hub 目录缓存） |

#### 10.6.10 plugins / extensions

#### plugins.*（SkillManager）

| 方法 | 参数 | 说明 |
|------|------|------|
| `plugins.list` **透传** | 无 | `{plugins:[{plugin_name, marketplace, spec("name@marketplace" 或裸名), version, installed_at, git_commit, skills, enabled(缺省 true)}]}` |
| `plugins.install` **透传** | `spec`（必填：`plugin@marketplace` / 本地路径（`/`、`./`、`../`、`~` 前缀）/ http(s) URL）、`force`（默认 false） | 透传 skills 安装：本地/URL 走 import_local，marketplace 走 skills.install（裸名仅允许内置）；成功后置 enabled=true + **重建 Agent 实例**；业务失败 `ok:true + {success:false, detail}` |
| `plugins.uninstall` **透传** | `name`（必填）、`origin`（可选，透传 skills.uninstall 按来源精确删除） | 先清 `_disabled_plugins/<name>` 缓存；prebuilt（企业版含 builtin）→ `{success:false, error_code:"prebuilt_not_removable"}` |
| `plugins.enable` / `disable` **透传** | `name`（必填） | 只改状态不动物理目录；成功 `{success:true, detail:"插件 X 已启用/已禁用，请执行 /reload-plugins 使其生效"}`；未找到 `{success:false, detail:"未找到插件: X"}` |
| `plugins.reload` **透传** | 无 | 按 enabled 把技能目录移入/移出 `_disabled_plugins` 后重建 Agent 实例；`{success:true, plugins_count, disabled_count, skills_count, detail}` |

#### extensions.*（RailManager）

| 方法 | 参数 | 说明 |
|------|------|------|
| `extensions.list` **透传** | 无 | `{extensions:[{name, class_name, enabled, description, priority}]}` |
| `extensions.import` **透传** | `folder_path`（必填：目录须存在、**目录名须 ASCII 英文标识符**、同名拒绝、须含 `rail.py` 且内容 import DeepAgentRail/AgentRail 可编译） | 复制进扩展目录，返回新扩展对象（enabled=false，priority 缺省 50）；错误 `ok:false {error}` |
| `extensions.delete` **透传** | `name`（必填；不存在报错） | 删除目录与注册状态；`{deleted:true, name}` |
| `extensions.toggle` **透传** | `name`（必填）、`enabled`（**缺省 false**，非必填——文档早期版本有误） | 对 live agent 热注册/注销（无实例时报"DeepAgent 实例未设置"）；返回更新后的扩展对象 |

#### 10.6.11 personal_context.runtime.*（采集与使用开关）

**公共契约**（`server/personal_context/ws_handler.py`，下称 WS；host_api.py 下称 HOST；Core 在 openjiuwen 包）：

- 实际 **26 个方法**（runtime 9 + fetch 12 + context 5）。
- 错误出口（WS:381-412）：WS 层形状校验（缺参/非字符串/非 bool/非对象/流式缺 is_stream）→ `BAD_REQUEST`；Host/Core 语义校验 → **Core 数值码 `154000`-`154006`** + `status` 状态名 + 本地化消息；其余 → `INTERNAL_ERROR`。注意：`not_configured` 在 Host 写路径默认落 **154000**（仅 fetch.run_* 经 reason 改写落 154001）。
- **投影配置**（开关/写方法标准响应）：`{master_enabled, collection_enabled, agent_use_enabled, strategy_profile, max_pages_per_directory, max_subdirectories_per_directory, model_index, model_id, fetch_services[]}`——fetch_services 每项去 `credentials`，**凭据永不回传**；未配置投影额外含 `configured:false`。
- status 七态：`CREATED/CONFIGURED/STARTING/RUNNING/STOPPING/STOPPED/FAILED`。

### runtime.*（9 个）

#### runtime.status

- **参数**：无（params 忽略）。**响应**：Status 全字段（`configured, collection_enabled, agent_use_enabled, state, pipeline_running, pipeline_queue_size, fetch_service_states{}, fetch_service_errors{}, fetch_run_progress{}, context_root, context_ready, last_error`）。

#### runtime.start_collection / stop_collection

- **参数**：无。**响应**：投影配置。start 未配置时**创建初始配置**（master/collection=true、agent_use=false）；stop 未配置返回未配置投影不报错；活动采集轮中改配置 → `154001 service_running`。

#### runtime.start_agent_use / stop_agent_use

- **参数**：无。**响应**：投影配置。未配置 → `154000 not_configured`；仅翻内存标志不启停运行时，幂等。

#### runtime.set_master_enabled

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `enabled` | bool | 是 | **严格 bool**（非 bool → BAD_REQUEST） | 总开关 |
- **响应**：投影配置。级联两子开关，之后子开关独立切换不反写总开关。

#### runtime.get_config

- **参数**：无。**响应**：已配置→投影配置（无 `configured` 键）；未配置→含 `configured:false` 的默认投影。

#### runtime.patch_config

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `patch` | dict | 是 | **仅允许 3 键** `collection_enabled/agent_use_enabled/strategy_profile`（strategy ∈ rules\|balanced\|agent）；未知键 → `154000` | 局部补丁 |
- **响应**：更新后的投影配置。活动轮 → `154001 service_running`；热重启失败自动回滚。

#### runtime.select_model

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `model_index` | int | 是 | 非 int/负数 → `154000 parameter_invalid`；越界/不可用 → `154000 model_invalid` | 按 models.list 下标 |
- **响应**：投影配置（含稳定 `model_id`，格式 provider:model_name）。

### fetch.*（12 个）

**service 对象字段**（create_service 的 `service`）——完整字段表见 §10.6.11 原始位置（此处仅列方法级参数）。

#### fetch.list_services

- **参数**：无。**响应**：`{services:[投影 service]}`；未配置 → 空列表。

#### fetch.create_service

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service` | dict | 是 | 非 object → **BAD_REQUEST**；字段校验见 service 对象字段表 | 新服务定义 |
- **响应**：创建后的投影 service。错误：未配置 → `154000 not_configured`；service_id 重复 → `duplicate_service`；同 provider ≥20 → `service_limit`；github/gitcode 传 credentials → `credentials_invalid`；未授权 → `authorization_required`。

#### fetch.delete_service

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string | 是 | 空/缺 → **BAD_REQUEST** | 目标服务 |
- **响应**：`{ok:true}`。错误：未知 → `154000 unknown_service`；活动轮 → `154001 service_running`；**保留已发布 Context 文件**。

#### fetch.patch_service

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string | 是 | 空/缺 → **BAD_REQUEST** | 目标服务 |
| `patch` | dict | 是 | **仅允许 4 键** `interval_seconds/max_items_per_run/source/time_range` | 局部补丁 |
- **响应**：更新后的投影 service。热更新失败回滚。

#### fetch.start_service / stop_service

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string | 是 | 空/缺 → **BAD_REQUEST** | 目标服务 |
- **响应**：`{ok:true}`。**只切换未来调度开关**（enabled 位），不影响进行中轮次。

#### fetch.run_all / run_one

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string | run_one 是 | 空/缺 → **BAD_REQUEST** | 指定服务；run_all 无参 |
- **响应**：`{state:'accepted', service_ids, runs:[{service_id, run_id}]}`（异步）。错误：未配置 → `154001 not_configured`；采集关 → `154001 collection_disabled`；目标忙 → `154001`。

#### fetch.stop_run

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string | 是 | 空/缺 → **BAD_REQUEST** | 目标服务 |
- **响应**：`{ok:true}`。对已知服务**幂等**（无活动任务静默成功）；未知 → `154001`。

#### fetch.get_run_status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `service_id` | string \| null | 否 | 给值时须非空，否则 `154000 parameter_invalid` | 过滤单服务 |
| `run_id` | string \| null | 否 | **须伴随 service_id 且非空**，否则 `154000 parameter_invalid` | 定位单轮 |
- **响应**三形态：单条 run 记录 / `{service_id, runs:[当前+历史 ≤5]}` / `{services:[...]}`；未配置返回空列表**不报错**。

#### fetch.get_authorization_status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `provider` | string | 是 | 空/缺 → **BAD_REQUEST**；strip+casefold | github/gitcode/feishu |
- **响应**：github/gitcode 实时 PAT 验证 → `{provider, state: not_authorized\|authorized\|authorization_failed, account, error}`（未存 PAT 返回 not_authorized **不报错**）；feishu 设备流状态。

#### fetch.authorize_provider

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `provider` | string | 是 | 空/缺 → **BAD_REQUEST** | github/gitcode/feishu |
| `credentials` | dict \| null | 否 | 非对象非 null → **BAD_REQUEST**；github 须恰 `{token}`、gitcode 须恰 `{pat}`；**feishu 必须 null** | PAT 直传 |
| `reauthorize` | bool | 否 | 缺省 false；**严格 bool** | 热替换凭据 |
- **响应**：github/gitcode → `{provider, state:'authorized', account, ...}`；feishu → 设备流 OAuth 挑战。凭据永不回传。

### context.*（5 个）

#### context.stream_graph / stream_tree

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `root_id` | string \| null | 否 | 给值须非空，否则 **BAD_REQUEST**；缺省 null 时根节点入选第 1 层；显式 root_id 时根不出现在 nodes、从子层开始 | 根节点 ID（`page:` 形式） |
| `depth` | int | 否 | 缺省 3；`type is not int`（**拒 bool**）且 1≤depth≤10，否则 **BAD_REQUEST** | BFS 层数 |
- **必须 `is_stream:true`**（否则 BAD_REQUEST）。**响应事件序列**：`start → nodes×N（每批 200）→ edges×N → end`。
- node：`{id:'page:<相对路径>', kind, subkind, label, path, service_id:null, has_children}`；edge：`{source, target, kind: contains\|references}`（references 仅 graph）。Context 未就绪 → `{context_ready:false, nodes:[], edges:[]}`。

#### context.search_pages

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `query` | string | 是 | 空/缺/纯空白 → **BAD_REQUEST** | 检索词 |
- **响应**：`{results:[{node_id, title, path, snippet}]}`——BM25F 加权（title 4.0/path 3.0/headings 2.0/body 1.0），最多 10 条，snippet ≤240 字符；未就绪返回空列表**不报错**。

#### context.get_node

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `node_id` | string | 是 | **`page:` 前缀** + POSIX 相对 + `.md` 后缀，禁 `\\`/绝对/`..`；`source:` 前缀提示改用 get_source | 页面节点 ID |
- **响应**：`{node_id, title, path, markdown}`；未就绪/缺失 → `154002`。

#### context.get_source

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `source_id` | string | 是 | 正则 `^src_[0-9a-f]{32}$` | 原子源稳定 ID |
- **响应**：`{source_id, title, source_type, locator, provider, service_id, first_seen, last_seen}`。

#### 10.6.14 a4p.*（预授权）

**公共契约**（`agents/harness/common/a4p/rpc.py`，下称 RPC；authorizer.py 下称 AUTH）：错误 payload=`{error, code}`（缺省 BAD_REQUEST）；授权类方法要求 A4P 已启用（否则 `A4P_DISABLED`）且请求带交互式 Web 路由（否则 `A4P_WEB_SESSION_REQUIRED`，RPC:122-128）；`pending` 在关闭时返回 `{ok:true, pending:null}` 不报错。

**两阶段授权事件**：`a4p.authorization_request`（requestId/kind/uiContext/mandate/signingOptions/originalActions/preparedActionIndexes）→ complete/reject/reprepare → `a4p.authorization_terminated`。**超时 180s**（`DEFAULT_AUTHORIZATION_TIMEOUT_SECONDS=180`，runtime.py:49——注意早期文档写 300s 有误）；超时推 `A4P_AUTHORIZATION_TIMEOUT` 按拒绝结算、取消推 `A4P_AUTHORIZATION_CANCELLED`；禁用/改配取消在途授权推 `A4P_DISABLED` / `A4P_CONFIG_CHANGED`。

#### a4p.config.get / a4p.config.update

> **网关处理**：**E2A 代理**——_forward_a4p_to_agent；**前置检查** AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED 网关即拒；params 原样。
| 方法 | 参数 | 说明 |
|------|------|------|
| `config.get` **E2A 代理** | 无（**不接受任何配置字段**，session_id 被剥离） | `{enabled: <落盘 bool>}` |
| `config.update` | `enabled`（**必填且仅此一键**，值必须 bool） | `{enabled}`（落盘值）；热重配失败 → `A4P_CONFIG_APPLY_FAILED`（已保存）；update 会取消在途授权（`A4P_CONFIG_CHANGED`） |

#### a4p.authorization.reprepare

> **网关处理**：**E2A 代理**——_forward_a4p_to_agent；前置检查同上；params 原样。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `requestId` | string | 是 | strip 非空；**未知键一律拒绝**（仅允许 session_id/requestId/selectedActionIndexes） | 待改选授权请求 |
| `selectedActionIndexes` | int[] | 是 | 非空 list；每项 int 且 0≤i<len(original_actions)；无重复 | 改选动作索引 |

- **响应**：`{ok, pending:<快照>}`；与已 prepared 索引一致时幂等返回；否则生成新 requestId 作废旧请求并重推 authorization_request。
- **错误**：`A4P_INVALID_SELECTION` / `A4P_NOT_PENDING` / `A4P_ROUTE_MISMATCH` / `A4P_REPREPARE_FAILED` / `A4P_DISABLED`。

#### a4p.authorization.complete / reject / pending

> **网关处理**：**E2A 代理**——_forward_a4p_to_agent；前置检查同上；params 原样。
| 方法 | 参数 | 说明 |
|------|------|------|
| `complete` | `requestId`（必填；**未知键拒绝**，仅 requestId/session_id） | `{ok:true, requestId}`——签发签名 mandate 给等待中的 Agent；`A4P_NOT_PENDING` / `A4P_ROUTE_MISMATCH` / `A4P_AUTHORIZATION_INVALID` |
| `reject` | `requestId`（必填）、`reason`（可选，缺省 "User rejected A4P authorization"；不校验未知键） | `{ok:true, requestId}`；`A4P_NOT_PENDING` / `A4P_ROUTE_MISMATCH` |
| `pending` | 无 | `{ok:true, pending:<快照>\|null}`——取该路由**最新一条**待审；快照含 requestId/kind/mandate/signingOptions/uiContext/originalActions/preparedActionIndexes/replacesRequestId/repreparing |

#### 10.6.15 rsi.*（训练）

**公共契约**（`server/rsi/rsi_handlers.py`，下称 RH；services.py 下称 SVC）：网关注入的 `session_id` 转为私有 `_rsi_session_id` 用于事件回推；响应 `{ok:true, payload}` / `{ok:false, error, code}`；推送事件 `rsi.training.status.changed`（含 old/new_status，FAILED 附 failure_reason）/ `rsi.training.progress` / `rsi.training.tree.delta`。**任务状态机**：CREATED→{QUEUED,TERMINATED}；QUEUED→{RUNNING,PAUSED,TERMINATED}；RUNNING→{COMPLETED,FAILED,PAUSED,TERMINATED}；PAUSED→{QUEUED,TERMINATED}；非法迁移 → `TASK_STATE_CONFLICT`。

#### rsi.dataset.validate

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `input_file` | string | 是 | 别名 dataset_path | 数据集路径 |
| `scenario` | string | 是 | `HARNESS\|ARTIFACT`（大写归一） | 非法 → `SCENARIO_NOT_SUPPORTED` |
| `artifact_type` | string | 条件必填 | scenario=ARTIFACT 时必填 `PAPER\|PROGRAM`；HARNESS 不接受 | 产物类型 |

- **响应**：`{valid, sample_count, errors:[{reason, code}]}`。错误：`PATH_INVALID` / `DATASET_INVALID`。

#### rsi.task.create

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `scenario` | string | 是 | `HARNESS\|ARTIFACT` | — |
| `artifact_type` | string | 条件必填 | ARTIFACT 必填 `PAPER\|PROGRAM`；HARNESS 携带报错 | — |
| `name` | string | 是 | 1–100 字符 | 实验名称 |
| `model_refs` | object | 是 | `optimizer` 必填；HARNESS 另必填 `tester` | 模型引用 |
| `input_file` | string | 条件必填 | HARNESS 必填（别名 dataset_path） | 数据集 |
| `artifact_path` | string | 条件必填 | PROGRAM 必填；PAPER 与 `optimization_instruction` 至少其一；HARNESS 不接受 | 产物路径 |
| `optimization_instruction` | string | 否 | 仅 PAPER 可填；空串归一 None | 优化指令 |
| `max_iterations` | int | 否 | 正整数，**缺省 1**；PROGRAM 未填时读 bundle task.json 同名键 | 迭代上限 |
| `web_proxy` | string | 否 | 仅 ARTIFACT+PAPER；http(s) URL、≤512 字符、须含端口、无 path/query/fragment | 评测代理 |
| `training_options` | object | 否 | HARNESS 专属（亦可平铺顶层）：domain(general\|office)、execution_mode(仅 local，auto→local)、**evaluation_method**(script_based\|exact_match\|llm_as_judge，默认 script_based)、max_epochs(默认1)、batch_size(默认1)、max_issue_attempts(默认8)、max_repair_rounds(默认3)、sibling_candidate_count(必须=1)、rollout_concurrency(默认1) | 训练选项 |

- **响应**：`{task_id:"rsi-<uuid8>", status:"CREATED"}`。错误：`INVALID_HARNESS`（无可用活动 Harness 配置）、`UNSUPPORTED_PARAMETER`（training_options 越界）、`DATASET_INVALID`/`PATH_INVALID`。

#### rsi.task.list / get / delete

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `list` | `scenario?`（缺省不过滤）、`artifact_type?`（HARNESS 过滤时携带报错） | `{tasks:[{task_id,name,scenario,artifact_type,status,created_at}]}`（created_at 倒序） |
| `get` | `task_id`（必填 strip 非空） | `{task_id,name,scenario,artifact_type,status, config:{model:{optimizer,tester},input_file,max_iterations,optimization_instruction,artifact_path,web_proxy_configured}, progress:{iteration,total_iterations,score,baseline}, best_artifact:{artifact_id,name,adopted}\|null, usage?, failure_reason?}`——usage 仅在有记录时、failure_reason 仅 FAILED 时返回 |
| `delete` | `task_id`（必填） | `{ok:true}`；`TASK_STATE_CONFLICT` 四分支：任务持有保留 Harness 版本 / QUEUED·PAUSED 且产物未释放 / RUNNING·PAUSED 不可删 / 在用产物未消费不可删；QUEUED 先 cancel 再删 |

#### rsi.training.start / pause / resume / terminate

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `start` | `task_id` | `{status:"QUEUED"}`；QUEUED 幂等短路；单队列并发=1；**PAUSED 任务须走 resume**（否则 TASK_STATE_CONFLICT） |
| `pause` | `task_id` | `{status}`——QUEUED→立即 PAUSED；RUNNING→返回当前 RUNNING（迁移待 Provider 确认）；ARTIFACT 场景不支持 → `SCENARIO_NOT_SUPPORTED` |
| `resume` | `task_id` | `{status:"QUEUED"}`；运行中暂停且 Provider 支持 → 续跑，否则全新 run；仅 PAUSED 可 resume；引擎指纹不匹配 → `RESUME_MISMATCH` |
| `terminate` | `task_id` | `{status}`——QUEUED/CREATED→立即 TERMINATED；RUNNING/PAUSED 有钩子→待确认、无钩子（生产引擎）→立即 TERMINATED；终态任务拒绝 |

#### rsi.report.get / usage.get / tree.get

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。

| 方法 | 参数 | 说明 |
|------|------|------|
| `report.get` | `task_id` | Provider 报告；未运行兜底 `{status, best_score, baseline, metrics:{eval_passed,eval_total,pruned_count,iterations}, usage, best_artifact, report_summary:"", markdown:null}` |
| `usage.get` | `task_id` | `{usage:{tokens:{input,output,cache_hit},cost_estimate,call_count}, per_iteration:[...], usage_by_node:{...}}`；无记录且无快照 → `TASK_NOT_FOUND` |
| `tree.get` | `task_id` | `{nodes:[{node_id,iteration,parent_id,type,adopted,score,description,snapshot_artifact_id,failure_reason,changes,extra}], depth, iteration}` |

#### rsi.artifact.download / files.list / files.get

> **网关处理**：**E2A 代理**——_register_harness(RSI_ARTIFACT_DOWNLOAD)；**注入 session_id** + 按 user_id 注入 _download_user_id。

| 方法 | 参数 | 说明 |
|------|------|------|
| `download` | `task_id`（必填）、`artifact_id`（可选，**缺省=best 产物**） | 文件 → `{path, kind(harness_plugin\|artifact_package), is_best, filename, is_directory:false, download_url, download_token}`（短时 HTTP 凭证）；**目录 → `{path, is_directory:true}`（不含下载链接）**；best/缺省下载即置 active_ref_released 放行后续 delete。`ARTIFACT_NOT_FOUND` / `PATH_INVALID` |
| `files.list` | `task_id`（必填，格式 `^[A-Za-z0-9][A-Za-z0-9_-]*$`）、`path` **或** `artifact_id`（**二选一，同给报错**；artifact_id 只读定位不释放在用产物） | `{root, initial_path, files:[{name,path,isDirectory,size,type}]}`；仅 artifact/run/snapshots/.rsi_artifact_views 四目录公开；zip >256MB → `BAD_REQUEST` |
| `files.get` | `task_id`、`path`（必填，限公开目录内） | `{path, name, size, type, encoding(text\|base64 按后缀), content}`；预览上限 10MB |

#### rsi.harness.install / versions.list / rollback

> **网关处理**：**E2A 代理**——_register_harness(RSI_*)；**注入 session_id**（网关→连接会话）。

| 方法 | 参数 | 说明 |
|------|------|------|
| `install` | `task_id`（必填；仅 HARNESS+COMPLETED 且 publication_status=published） | `{installation_id, task_id, node_id, harness_name, sha256, status:"ACTIVE", already_active, hot_load}`；同 task+同 sha256 幂等；installation_id=`rsi-harness-<sha256[:16]>`。`RSI_HARNESS_NOT_READY/NOT_PUBLISHED/INVALID/INSTALL_CONFLICT/INSTALL_FAILED` |
| `versions.list` | 无（忽略入参） | `{active_installation_id, versions:[{installation_id,task_id,node_id,harness_name,sha256,installed_at,is_active,is_initial,available}]}`——**不暴露本地路径** |
| `rollback` | `installation_id`（必填；已是 active 时幂等 rolled_back:false） | 安装记录 + `{from_installation_id, rolled_back:true}`；存在非终态任务 → `TASK_STATE_CONFLICT`；**校验目标内容 hash 未变化** |

#### 10.6.16 harness.*（扩展包管理）

**公共契约**：错误码映射 `_harness_error_code`——消息含 "already active/exists" → `CONFLICT`、"not found" → `NOT_FOUND`、`"native"` → `BAD_REQUEST`、兜底 `BAD_REQUEST`；缺 `package_id` → `BAD_REQUEST "missing package_id"`；其他异常 → `INTERNAL_ERROR`（`server/handlers/extensions.py:17-27, 306-452`）。

| 方法 | 参数 | 说明 |
|------|------|------|
| `harness.packages`（wire 名 `harness.packages.get`） **E2A 代理** | 无 | `{packages:[{id, extension_name, runtime_path, config_path, created_at, is_active, version_label, description}], native_version:{id:"native", extension_name:"Native Agent", is_active}, active_package_ids, last_updated}` |
| `harness.packages.scan` **E2A 代理** | 无 | 同上；扫描 `runtime_extensions/*/*/harness_config.yaml`，保留既有元数据、过滤已下线 active id，持久化 harness-packages.json |
| `harness.activate` **E2A 代理** | `package_id`（必填） | 已激活幂等成功（响应路径字段为空串+“扩展已处于激活状态”）；成功 `{activated_package_id, extension_name, runtime_path, config_path, loaded_resources, message:"扩展已热生效（agent 模式），加载资源: N 项"}`；无 agent 实例 → “扩展已标记为激活”；堆叠激活并广播同 channel 实例 |
| `harness.deactivate` **E2A 代理** | `package_id`（必填） | 未激活幂等；成功 `{deactivated_package_id, extension_name, message}` |
| `harness.delete` **E2A 代理** | `package_id`（必填；`"native"` 双重拒绝 `BAD_REQUEST "Cannot delete native agent version"`） | 活动包先卸载+广播再 rmtree；`{deleted_package_id, extension_name, switched_to_native, message}` |
| `harness.import` **E2A 代理** | `file_content`（base64）/ `uploaded_path` 二选一 | 网关：>4MB 且 AgentServer ready → FILE_UPLOAD_CHUNK 分块上传（2MB/块）后仅以 `uploaded_path` 转发并剔除 `file_content`，上传失败 → `SERVICE_UNAVAILABLE`；≤4MB 直接转发（`preserve_error_payload=True`）。另有 multipart 专用入口 `POST /api/v1/harness/packages/actions/import-file`（字段 `file`，>50MB 拒绝）。⚠️ **AgentServer 侧无分发分支（现状核实仍成立）**：协议枚举存在（`harness.packages.import`），jiuwenswarm dispatch 仅注册 get/scan/activate/deactivate/delete；业务实现 `import_package`（zip 校验/Zip-Slip 防护/harness_config.yaml 位置校验/重名 CONFLICT/注册未激活包）已具备但未接线——**响应形态未定义** |
| `harness.export` **E2A 代理** | `package_id`（必填） | 网关直转（`preserve_error_payload=True`）；⚠️ **AgentServer 无分发分支，响应未定义**（REST 摘要宣称的 download_url/token 无下游处理方支撑）；业务实现 `export_package`（native 拒绝/产物 zip 到 temp/exports）已具备但未接线 |

#### 10.6.17 issue.*（GitCode issue 自动处理）

**公共契约**（`server/handlers/schedule.py:152-169`）：**恒 `ok:true`**——业务错误（缺 token/任务不存在等）放 payload 的 `{error}` 而 ok 仍为 true；仅异常才 `ok:false {error}` 且**无 code 字段**。token 回退链：params → env `GITCODE_ACCESS_TOKEN` → base config。

#### issue.watch_once

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `access_token` | string | 否 | 回退 env/base config；缺失 → `{error:"缺少 GitCode Access Token…"}` | — |
| `owner` + `repo_name` 或 `repo` | string | 三选一 | `repo` 支持 "owner/repo" 或 URL；全缺省用 base config；无法解析报错 | 目标仓库 |
| `max_issues` | int | 否 | 缺省 1；非数字回退 1；**夹取 1–5**；指定 issue_numbers 时覆盖为其数量 | 拉取上限 |
| `issue_numbers` | int/list/str | 否 | 别名 `issues`/`issue`/`numbers`；逗号（含中文）分隔正整数去重 | 指定编号 |
| `labels` | str/list | 否 | 默认 `("auto-harness",)` | — |
| `exclude_labels` | str/list | 否 | 默认 `("blocked","wontfix","needs-discussion")` | — |
| `per_page` | int | 否 | 缺省 20；夹取 1–100 | 分页 |
| `pipeline` | string | 否 | 默认 META_EVOLVE_PIPELINE | — |
| `comment_on_start` / `dry_run` | bool（`bool()` 强转） | 否 | 缺省 false；dry_run 条目不入库 | — |
| `start_interval_seconds` | number | 否 | 缺省 0；≤0 且 max_issues>1 自动抬到 1.2（防 worktree 冲突） | 启动间隔 |
| `max_auto_difficulty` | string | 否 | 默认 "medium"；枚举 low<medium<high<unclear，超限记 needs_human | 难度上限 |
| `model_name` | string | 否 | ⚠️ 被 resolve 后 `del` 丢弃，对行为无效 | — |

- **响应**：`{owner, repo, fetched, started:[...], skipped:[{issue, status:"skipped"|"needs_human", reason}], reconciled:[...]}`。跳过条件：已关闭/缺必需 label/终态或在途/已有关联 PR/难度超限。

#### issue.state.list / issue.matrix / issue.delete

> **网关处理**：**透传**——params 原样转发（仅附录 F.2 通用归一化）。
| 方法 | 参数 | 说明 |
|------|------|------|
| `state.list` | 无 | `{issues:[<记录> + task_status?, progress?, pr_url?]}`；任务已删 → `task_status:"task_deleted"`；含轻量终态对账 |
| `matrix` | `access_token`/`repo` 同上、`force_refresh`（默认 false）、`page`（缺省 1，下限 1；**非数字直接 ok:false**，区别于 watch_once 的容错）、`per_page`（**缺省 50，夹取 1–100**）、`labels`（空=不过滤、小写匹配，仅作用于返回不入库） | `{owner, repo, total, added, removed, updated, difficulty_counts, matrix:[{number, title, body(截 200), labels, difficulty, updated_at, first_seen, last_analyzed}], has_more, page, per_page, cached, labels_filter}`（缓存按 owner/repo 持久化） |
| `delete` | `issue_numbers`（解析同 watch_once）、`delete_completed`（默认 false，命中 success/pr_created/completed/completed_without_pr）、`delete_failed`（默认 false，命中 failed/cancelled） | `{deleted:[{issue, task_id}], rejected:[{issue, reason:"任务正在执行中…"}]}`（running 态拒绝，任务残留例外放行） |

### 10.7 Designer 画布（执行图编排）

13 个方法，均为 E2A 代理（`designer_handlers.py:9-23`，注册于 `app_web_handlers.py:5829-5832`），错误码遵循 §3.2 通用契约：

| 方法 | 说明 |
|------|------|
| `designer.workspace.create` / `designer.workspace.get` **E2A 代理** | 画布工作区创建 / 读取 |
| `designer.workspace.session.create` / `designer.workspace.session.compose` **E2A 代理** | 在工作区内创建 / 组合画布会话 |
| `designer.graph.get` / `designer.graph.list` / `designer.graph.save` / `designer.graph.patch` **E2A 代理** | 执行图读写（`designer.graph.bootstrap` / `designer.graph.chat` 尚未挂到 Web 路由） |
| `designer.media.capabilities` **E2A 代理** | 媒体能力查询 |
| `designer.run.start` / `designer.run.get` / `designer.run.pause` / `designer.run.cancel` **E2A 代理** | 画布运行控制（`designer.run.choose_output` 已删除） |

推送事件（全量 payload，附录 B）：`designer.run.updated` / `designer.node.updated` / `designer.graph.updated` / `designer.leader.activity`。**路由**：designer 帧走通用三级路由（发起连接 ws_id 钉死 → A4P 改绑 → session 桶；旧基线"不按 ws_id 钉死"的特例已删除，`web_connect.py:1509-1543`）。

### 10.8 新增方法族（topic_starters / prompt_suggestion / client / mcp_app / 会话素材）

以下方法在基线后新增，依据 AgentServer 仓 develop@475e3a3b64 核对。**转发包装**：topic_starters.\* 与 prompt_suggestion.\* 经网关 `_forward_recommendation` 薄包装——RPC 名映射为 `action` 字段经 `recommendation.job` 转发（`topic_starters.control` 的客户端 `action` 改写为 `control_action`；连接登录会话注入 `auth_session`，`app_web_handlers.py:5550-5586`）；`recommendation.job` 免会话存活校验。client.\*/mcp_app.\*/session.assets.\*/initialize/proactive.feedback 为纯转发。

#### topic_starters.status / select_model / profile / restore_direction / control / get / foreground / event

#### topic_starters.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `include_api_key` | bool | 否 | 仅 `is True` 才回显密钥 | 是否含专用模型 api_key |
- **响应**：配置状态 + 模型信息 + `model_config` + `enabled`；`status ∈ ready\|missing_configuration\|invalid_configuration`。

#### topic_starters.select_model

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `model` | string | 与 configuration 二选一 | 须为可用 OpenAI 型模型 id；**空串=清空并关闭** | 选择目录内模型 |
| `configuration` | dict | 与 model 二选一 | 字段 model/provider/api_base/api_key/vendor_key/plan/endpoint_profile 均须 str；provider=='OpenAI'、model 非空 ≤100、api_base 须 http(s):// 且 ≤512、api_key ≤2048、context_window_tokens 正整数 | 独立 OpenAI 兼容直连配置 |
- **响应**：完整 status payload；清空时推 `topic_starters.control_changed{enabled:false}`；非法 `BAD_REQUEST 'invalid recommendation model configuration'`。

#### topic_starters.profile

- **参数**：无。**响应**：`{units:[{unit_id, summary, sources}], semantic_version, permanent_directions:[{direction_id, summary, since}]}`。

#### topic_starters.restore_direction

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `direction_id` | string | 是 | 必须命中现存 permanent direction | 恢复被永久屏蔽的方向 |
- **响应**：恢复后的完整 profile payload；未命中 `BAD_REQUEST 'no such permanent direction'`。

#### topic_starters.control

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `action` | string | 是 | 枚举 enable/disable/delete | 开关/删除推荐数据 |
| `include_history` | bool | 否 | 缺省 false；**须为 bool** | enable/disable 时是否保留历史 |
- **响应**：status payload；并推送 `topic_starters.control_changed{enabled: <bool>}`。

#### topic_starters.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 缺省回退信封 session_id；空 → `BAD_REQUEST` | 访问会话 |
| `policy_id` | string | 否 | ''/v4→旧历史道；v5→prepare 道；v6→产品道；其余 `BAD_REQUEST` | 选数据道 |
| `context_scope` | string | 条件 | v5 只许 `global_new_chat`（或缺省）；v6 须 `global_new_chat` 或 `project:<id>` 且与 project_id 一致 | 范围 |
| `project_id` | string \| null | 条件 | v5 必须 null；v6 须匹配 scope | 项目范围 |
| `manual_retry` | bool | 否 | 仅 `is True` 触发 | 产品道手动重试 |
| `slate_id` | string | 否 | — | 拉取指定 slate |
| `model_name` | string | 否（仅 v4 道） | 存在连接类字段即拒 `invalid_model_request_fields` | v4 道模型选择 |
- **响应**：v5/v6 道 `{available, session_id, policy_id, (context_scope, project_id), ...slate 字段}`；不可用 `{available:false, reason: prepare_service_unavailable\|topic_service_unavailable\|topic_starter_unavailable}`。

#### topic_starters.foreground

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `context_scope` | string | 否 | 仅 `global_new_chat` 触发调度 | 前台到访信号 |
| `visit_id` | string | 否 | 兜底信封 session_id，再兜底 'foreground' | 访问标识 |
- **响应**：`{scheduled: bool}`。

#### topic_starters.event

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 兜底信封 session_id | 会话 |
| `slate_id` | string | 是 | 非空 | 曝光批次 |
| `event_type` | string | 是 | 枚举 dynamic_exposure/topic_click/not_interested/post_exposure_direct/page_left/session_bound | 事件类型 |
| `policy_id` | string | 是 | v3/v4（legacy）、v5、v6 | 数据道 |
| `selection_mode` | string | 条件 | v3/v4 ∈ {static_fallback, personalized_dynamic, generic_dynamic}；v5/v6 ∈ {personalized_dynamic, mixed_dynamic, generic_dynamic} | 选择模式 |
| `topic_id` | string | 条件 | topic_click/not_interested/session_bound 须与 payload.card_revision_id 一致 | 卡片 |
| `profile_version` | — | v3/v4 必填 | 必须存在 | 画像版本 |
| `payload` | dict | v5/v6 必填 | 须含 manifest_id、generation_batch_id（非空）；context_scope/project_id 约束同 get；exposure 需 topics 列表 | 事件体 |
- **响应**：`{recorded: bool}`。

#### prompt_suggestion.get / prompt_suggestion.event

#### prompt_suggestion.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 兜底信封 session_id；空 → `BAD_REQUEST` | 取该会话最新一次成功回答的未消费建议 |
- **响应**：`{enabled, suggestions:[{session_id, request_id, suggestion_id, text}]}`。

#### prompt_suggestion.event

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `event_type` | string | 是 | 枚举 input_changed/shown/accepted/ignored/suppressed | 事件类型 |
| `session_id` | string | input_changed 必填 | 网关与服务端双重校验非空 | 会话 |
| `suggestion_id` | string | outcome 事件必填 | 须为该会话已生成的建议 | 建议 id |
| `method` | string | accepted 必填 | 枚举 click\|tab | 接受方式 |
| `time_to_accept_ms` | int | accepted 必填 | 非负整数 | 耗时 |
| `reason` | string | ignored/suppressed 必填 | ignored ∈ {dismissed, sent_other, left_session, typed}；suppressed 仅 timing | 原因 |
- **响应**：input_changed → `{cancelled: bool}`；总开关关闭 → `{recorded: false, reason: 'disabled'}`；其余 → `{recorded: bool}`（一次性记录）。

#### client.attach / renew / list / conversation.messages（客户端宿主挂载）

#### client.attach

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 须为请求者拥有的现存会话，否则 `NOT_FOUND_OR_FORBIDDEN` | 目标会话 |
| `plugin_id` | string | 是 | 正则 `^[a-z0-9][a-z0-9_-]{0,63}$` | 应用插件 id |
| `resource` | dict | 是 | key/display_name/kind/host_vendor 均为非空 str；key 不得为路径/URL | 资源稳定标识 |
| `capabilities` | dict | 是 | JSON ≤64KB；supported_tools 须为字符串列表且 ⊆ 插件已定义工具 | 宿主可执行能力 |
- **响应**：`{attachment_id, session_id, plugin_id, pending:[...], lease_seconds:120.0, renew_every_seconds:30.0}`。
- **错误**：`BAD_REQUEST` / `INVALID_ARGUMENT` / `NOT_FOUND_OR_FORBIDDEN` / `CONFLICT`（会话已属其他应用）/ `PLUGIN_UNAVAILABLE` / `UNAVAILABLE`。

#### client.renew

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `attachment_id` | string | 是 | 须与本 socket（channel_id+ws_id）及 session_id 三者匹配 | 续约 |
| `session_id` | string | 是 | 同上 | 会话 |
- **响应**：`{lease_seconds: 120.0, renew_every_seconds: 30.0}`；失配 `NOT_ATTACHED 'no such attachment: attach again'`。

#### client.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 否 | 兜底信封 session_id；标记 is_current | 过滤 |
- **响应**：`{attachments:[{attachment_id, display_name, kind, host_vendor, session_id, is_current}]}`。

#### client.conversation.messages

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `target_session_id` | string | 是 | 须为请求者拥有的现存会话 | 读取其他已挂客户端会话的最新消息页 |
- **响应**：`{messages:[...]}`——history.get 第 1 页的倒序公开记录，剥离 reasoning* 字段（保留 `has_reasoning` 标记）。

#### mcp_app.list_tools / read_resource / call_tool（MCP Apps 沙箱）

#### mcp_app.list_tools

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `server` | string | 是 | 非空；须为已配置且可连接的 MCP 服务名 | MCP 服务名 |
- **响应**：`{tools:[raw tools/list dump]}`（保留 _meta/schema/annotations）；`MCP_NOT_FOUND`/`MCP_BAD_REQUEST`/`MCP_INTERNAL`。

#### mcp_app.read_resource

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `server` | string | 是 | 同上 | MCP 服务名 |
| `uri` | string | 是 | **必须 `ui://` 开头**，否则 `MCP_BAD_REQUEST` | UI 资源 URI |
- **响应**：raw `ReadResourceResult` dump（contents + `_meta.ui` 的 csp/permissions）。

#### mcp_app.call_tool

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `server` | string | 是 | 同上 | MCP 服务名 |
| `tool` | string | 是 | 非空；须存在于该服务的 tools/list | 工具名 |
| `arguments` | dict \| null | 否 | 非 dict → `MCP_BAD_REQUEST`；缺省 `{}` | 工具入参 |
- **响应**：raw `CallToolResult` dump；工具 `_meta.ui.visibility` 不含 'app' → `MCP_APP_FORBIDDEN`。

#### initialize / session.assets.* / proactive.feedback

#### initialize

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `protocolVersion` | string | 否 | 缺省 '0.1.0' | 协议版本 |
| `clientCapabilities` | dict | 否 | 缺省 `{}` | 客户端能力声明 |
- **响应**：ACP 能力 payload：`{protocolVersion:1, agentInfo:{name:'jiuwenswarm', title:'JiuwenSwarm', version}, agentCapabilities:{loadSession:false, promptCapabilities, sessionCapabilities, mcpCapabilities}, authMethods:[]}`。

#### session.assets.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 缺省回退信封 session_id | 会话 id |
- **响应**：`{assets:[{asset_id('asset_'+8hex), name, kind(image\|video\|audio\|document), path, source(upload\|generated), created_at, client_id?, name_history?}]}`；错误 `INVALID_PARAMS`。

#### session.assets.register

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 同上 | 会话 |
| `items` | list | 是 | 每项 `path` 必须为存在的绝对文件路径（缺失→该项 created=null）；`source` ∈ {upload, generated}（非法回落 upload）；`client_id` ≤60 幂等键（同 client_id 已存在则幂等返回原素材）；`name` 缺省取 stem/非法自动洗牌/冲突追加 -N | 批量注册 |
- **响应**：`{assets:[全部], created:[与 items 平行的资产或 null]}`——upload 按 client_id、generated 按 path 幂等去重。

#### session.assets.rename

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `session_id` | string | 是 | 同上 | 会话 |
| `asset_id` | string | 是 | 不存在 → `ASSET_NOT_FOUND` | 目标素材 |
| `name` | string | 是 | 非空 ≤60、无 `@\r\n\t`；大小写不敏感冲突 → `NAME_CONFLICT` | 新名称 |
- **响应**：`{assets:[全部]}`；旧名写入 name_history。

#### proactive.feedback

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `rec_id` | string | 是 | 非空 | 推荐条目 id |
| `feedback_type` | string | 是 | 枚举 explicit_like\|explicit_dislike | 反馈类型 |
| `rec_type` | string | 否 | 兜底键 proactive_type | 推荐类型元数据 |
| `rec_target` | string | 否 | 兜底键 proactive_target | 推荐目标元数据 |
- **响应**：`{status:'feedback_recorded', rec_id}`；缺参 `ok:false {error}` **无 code**。

### 10.9 jiuwenmate.*（企业协同入口，46 个 Web 白名单 + 1 个本地）

**可达性注记**：46 个方法在网关 `_JIUWENMATE_FORWARD_METHODS` 白名单（`app_web_handlers.py:409-457`），params 原样透传；**服务端实现位于 AgentServer `releases/0.2.9.beta2` 发布线**（develop 线当前不含 `server/jiuwenmate/`），以下契约按该线核对（`server/jiuwenmate/web_adapter.py`，下称 WA）。业务错误统一 `PersistentAgentError(message, code)`（缺省 `BAD_REQUEST`）。另有 10 个**内部 E2A 方法不在 Web 白名单**（inbound/ack/group.response 等，由网关主动发起，见本节末）。`jiuwenmate.colleague.delete` 为网关本地编排（local_only）。

**colleague 域（同事/数字分身）**：

**colleague 域（同事/数字分身）**（域内方法均**透传**）：

#### jiuwenmate.colleague.ensure_default

- **参数**：无。**响应**：`{colleague: wire\|null}`（不存在返回 null 不报错）。

#### jiuwenmate.colleague.list

- **参数**：无。**响应**：`{colleagues:[wire]}`。

#### jiuwenmate.colleague.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
- **响应**：`{colleague: wire}`；`NOT_FOUND 'colleague not found'`。

#### jiuwenmate.colleague.create

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `settings` | dict | 是 | `name` str ≤64 必填；`description` ≤2000；`persona` ≤20000；`tags` list[str] ≤64 项；`category` str ≤64 缺省 'custom'；`avatar_url` ≤2048；`group_reply_mode` ∈ {passive,adaptive}；`permission_profile` ∈ {default,automatic,full_access}；`capabilities{model str ≤512, skills/connectors/plugins/knowledge/starter_prompts 各 list}`；**未知字段报错** | 同事设置 |
| `template_id` | string | 否 | 非 str 报错 | 模板 id |
| `source_template_id` | string | 否 | — | 来源模板 |
| `unique_source` | bool | 否 | 仅 `is True` 生效 | 唯一来源约束 |
- **响应**：`{colleague: wire}`；模板不存在 `TEMPLATE_NOT_FOUND`。

#### jiuwenmate.colleague.settings.get / settings.update

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `settings` | dict | settings.update 是 | 结构同 create | 更新设置 |
- **响应**：get → `{settings}`；update → `{colleague: wire}`；`NOT_FOUND`。

#### jiuwenmate.colleague.publish / set_enabled

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `enabled` | bool | set_enabled 是 | **严格 bool**（非 bool 报错） | 启停 |
- **响应**：publish → `{version}`；set_enabled → `{colleague: wire}`。

#### jiuwenmate.colleague.delete（网关本地编排）

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | `agent_id` 即 config_id | 同事 agent id |
| `mode` | string | 是 | 枚举 unbind\|purge（unbind=hide 保数据；purge=清数据） | 删除模式 |
- **流程**：E2A `colleague.connections` 列连接 → feishu/welink/dingtalk 三平台摘除绑定 → E2A `colleague.remove`。**响应**：`{deleted: true}`。

**conversation / chat 域**（域内方法均**透传**）：`conversation.list`（`agent_id`）→ `{conversations:[...]}`；`conversation.rename`/`pin`（`agent_id`+`conversation_id`+`title`/`pinned`）、`conversation.delete`（软删归档）——不存在 `NOT_FOUND "conversation not found"`；`chat.send`（`agent_id`+`conversation_id`+`text`+`message_id` 均必填，会话须已打开）→ `{request, conversation}`，流式片段经 `jiuwenmate.chat.stream` **事件**推给 watch 连接；`chat.history`（`agent_id`+`conversation_id`）→ `{requests:[...]}`。

**request / permission / sharing 域**（域内方法均**透传**）：`request.get`/`request.abort`（`request_id` 必填；abort 可带 `reason`）；`permission.decide`（`approval_id`+`decision ∈ approve|reject` 必填，另可 `conversation_id`/`tool_call_id`/`agent_id`；跨群共享需 owner 专批 `FORBIDDEN`）；`sharing.list`（`agent_id`+`mark_read` 仅 `is True` 生效）；`sharing.decide`（`agent_id`+`approval_id`+`decision`，未命中 `NOT_FOUND`/`CONTINUATION_NOT_READY`）；`sharing.revoke`（`agent_id`+`grant_id`）。

**knowledge / models / activity**（域内方法均**透传**）：`knowledge.list`（无参 → `{pages:[{name,label}]}`）；`models.list`（无参 → 模型目录含 vendor_key，无连接凭据）；`activity.get`（无参 → `{activity: report}`）。

**channel 域（群记录）**（域内方法均**透传**）：

**channel 域（群记录）**（域内方法均**透传**）：

#### jiuwenmate.channel.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
- **响应**：`{channels:[{id, name, platform, coverage, artifacts, token_usage, last_response_minutes}]}`（按最近活跃倒序）。

#### jiuwenmate.channel.messages

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `group_id` | string | 是 | strip 非空 | 群 id |
| `cursor` | string | 否 | 非空时分页 | 游标 |
- **响应**：`{status, next_cursor, coverage, coverage_reason, messages:[{id, sender 字段, text, status, received_at, attachments}]}`；无归档 → `{messages:[], status:'empty', next_cursor:null}`。

#### jiuwenmate.channel.trace

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `group_id` | string | 是 | strip 非空 | 群 id |
- **响应**：`{started_at: iso|null, rows:[...]}`；无 Session → `{started_at:null, rows:[]}`。

**agent_mail 域**（统一前置 `agent_id`；域内方法均**透传**）：

**agent_mail 域**（统一前置 `agent_id`；域内方法均**透传**）：

#### jiuwenmate.agent_mail.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同事存在性校验 NOT_FOUND | 目标同事 |
- **响应**：`{agent_mail:{status, cli_version, ...}}`；无 runtime → `{status:'unavailable', cli_version:null}`。

#### jiuwenmate.agent_mail.auth.begin

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{authorization:{status, cli_version, operation_id, auth_url, frontend_opens_auth}}`；错误 `AGENT_MAIL_RUNTIME_ERROR`（auth_url 须 https 且 qq.com/tencent.com 域 ≤4096）。

#### jiuwenmate.agent_mail.auth.poll / auth.cancel

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `operation_id` | string | 是 | strip 非空 | 操作 id |
- **响应**：`{agent_mail:{status...}}`（状态机：authorizing→connected）。

#### jiuwenmate.agent_mail.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{agent_mail:{status...}}`。

**dingtalk_auth 域**（统一前置 `agent_id`+`connection_key`，域内方法均**透传**；

**dingtalk_auth 域**（统一前置 `agent_id`+`connection_key`，域内方法均**透传**；绑定解析失败 → `NOT_FOUND 'DingTalk connection is not bound to this employee'`）：

#### jiuwenmate.dingtalk_auth.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 即 config_id | 目标同事 |
| `connection_key` | string | 是 | strip 非空 | 绑定 key |
- **响应**：`{dingtalk_auth:{status, cli_version[, group_monitor]}}`（connected 时附 group_monitor）。

#### jiuwenmate.dingtalk_auth.begin

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
- **响应**：`{authorization:{...}}`；锁忙 `CONFLICT 'DingTalk application binding is busy'`；CLI 侧 `DINGTALK_CLI_APP_NOT_CONFIGURED` 等。

#### jiuwenmate.dingtalk_auth.poll / cancel

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
| `operation_id` | string | 是 | poll 变更 `CONFLICT 'DingTalk authorization operation changed'` | 操作 id |
- **响应**：`{dingtalk_auth:{...}}`。

#### jiuwenmate.dingtalk_auth.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上；允许已保存但停用的绑定 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
- **响应**：`{dingtalk_auth:{...}}`。

**welink_office 域**（前置 `agent_id`；域内方法均**透传**）：

**welink_office 域**（前置 `agent_id`；域内方法均**透传**）：

#### jiuwenmate.welink_office.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | get_colleague 校验 | 目标同事 |
- **响应**：`{welink_office:{status: unavailable\|not_connected\|connected, account?, email?, reason?}}`。

#### jiuwenmate.welink_office.connect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `email` | string | 是 | 邮箱正则、≤320、不以 `-` 开头 | 绑定邮箱 |
- **响应**：连接后的 status。错误：`WELINK_NOT_BOUND` / `WELINK_MAIL_DISCOVERY_FAILED`。

#### jiuwenmate.welink_office.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{welink_office: status}`（enabled=0）。

**jiuwenmate.schedule.\***（7 个，均**透传**；区别于 §8.3 的 schedule.*）：

#### jiuwenmate.schedule.list / create / update / set_enabled / delete / run / history

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `name` | string | 是（create/update） | strip 非空 | 任务名 |
| `prompt` | string | 是（create/update） | strip 非空 | 提示词 |
| `cron_expr` | string | 否 | 缺省 '' | cron 表达式 |
| `timezone` | string | 否 | 缺省 'Asia/Shanghai' | 时区 |
| `schedule_kind` | string | 否 | 缺省 'cron' | 调度类型 |
| `interval_seconds` | int | 否 | — | 间隔秒 |
| `run_at` / `starts_at` / `ends_at` | string | 否 | — | 时间点 |
| `agent_id` | string | create 必填 | strip 非空 | 归属同事 |
| `plan_id` | string | update/set_enabled/delete/run/history 必填 | strip 非空 | 计划 id |
| `enabled` | bool | set_enabled 必填 | — | 启停 |
- **响应**：list → `{plans:[...]}`；create/update → `{plan}`；run → `{request_id}`；history → report；delete → `{deleted:true}`。

**内部 E2A 方法不在 Web 白名单**（inbound/ack/group.response 等，由网关主动发起，见本节末）。`jiuwenmate.colleague.delete` 为网关本地编排（local_only）。

**colleague 域（同事/数字分身）**：

**colleague 域（同事/数字分身）**（域内方法均**透传**）：

#### jiuwenmate.colleague.ensure_default

- **参数**：无。**响应**：`{colleague: wire\|null}`（不存在返回 null 不报错）。

#### jiuwenmate.colleague.list

- **参数**：无。**响应**：`{colleagues:[wire]}`。

#### jiuwenmate.colleague.get

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
- **响应**：`{colleague: wire}`；`NOT_FOUND 'colleague not found'`。

#### jiuwenmate.colleague.create

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `settings` | dict | 是 | `name` str ≤64 必填；`description` ≤2000；`persona` ≤20000；`tags` list[str] ≤64 项；`category` str ≤64 缺省 'custom'；`avatar_url` ≤2048；`group_reply_mode` ∈ {passive,adaptive}；`permission_profile` ∈ {default,automatic,full_access}；`capabilities{model str ≤512, skills/connectors/plugins/knowledge/starter_prompts 各 list}`；**未知字段报错** | 同事设置 |
| `template_id` | string | 否 | 非 str 报错 | 模板 id |
| `source_template_id` | string | 否 | — | 来源模板 |
| `unique_source` | bool | 否 | 仅 `is True` 生效 | 唯一来源约束 |
- **响应**：`{colleague: wire}`；模板不存在 `TEMPLATE_NOT_FOUND`。

#### jiuwenmate.colleague.settings.get / settings.update

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `settings` | dict | settings.update 是 | 结构同 create | 更新设置 |
- **响应**：get → `{settings}`；update → `{colleague: wire}`；`NOT_FOUND`。

#### jiuwenmate.colleague.publish / set_enabled

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `enabled` | bool | set_enabled 是 | **严格 bool**（非 bool 报错） | 启停 |
- **响应**：publish → `{version}`；set_enabled → `{colleague: wire}`。

#### jiuwenmate.colleague.delete（网关本地编排）

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | `agent_id` 即 config_id | 同事 agent id |
| `mode` | string | 是 | 枚举 unbind\|purge（unbind=hide 保数据；purge=清数据） | 删除模式 |
- **流程**：E2A `colleague.connections` 列连接 → feishu/welink/dingtalk 三平台摘除绑定 → E2A `colleague.remove`。**响应**：`{deleted: true}`。

**conversation / chat 域**（域内方法均**透传**）：`conversation.list`（`agent_id`）→ `{conversations:[...]}`；`conversation.rename`/`pin`（`agent_id`+`conversation_id`+`title`/`pinned`）、`conversation.delete`（软删归档）——不存在 `NOT_FOUND "conversation not found"`；`chat.send`（`agent_id`+`conversation_id`+`text`+`message_id` 均必填，会话须已打开）→ `{request, conversation}`，流式片段经 `jiuwenmate.chat.stream` **事件**推给 watch 连接；`chat.history`（`agent_id`+`conversation_id`）→ `{requests:[...]}`。

**request / permission / sharing 域**（域内方法均**透传**）：`request.get`/`request.abort`（`request_id` 必填；abort 可带 `reason`）；`permission.decide`（`approval_id`+`decision ∈ approve|reject` 必填，另可 `conversation_id`/`tool_call_id`/`agent_id`；跨群共享需 owner 专批 `FORBIDDEN`）；`sharing.list`（`agent_id`+`mark_read` 仅 `is True` 生效）；`sharing.decide`（`agent_id`+`approval_id`+`decision`，未命中 `NOT_FOUND`/`CONTINUATION_NOT_READY`）；`sharing.revoke`（`agent_id`+`grant_id`）。

**knowledge / models / activity**（域内方法均**透传**）：`knowledge.list`（无参 → `{pages:[{name,label}]}`）；`models.list`（无参 → 模型目录含 vendor_key，无连接凭据）；`activity.get`（无参 → `{activity: report}`）。

**channel 域（群记录）**（域内方法均**透传**）：

**channel 域（群记录）**（域内方法均**透传**）：

#### jiuwenmate.channel.list

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
- **响应**：`{channels:[{id, name, platform, coverage, artifacts, token_usage, last_response_minutes}]}`（按最近活跃倒序）。

#### jiuwenmate.channel.messages

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `group_id` | string | 是 | strip 非空 | 群 id |
| `cursor` | string | 否 | 非空时分页 | 游标 |
- **响应**：`{status, next_cursor, coverage, coverage_reason, messages:[{id, sender 字段, text, status, received_at, attachments}]}`；无归档 → `{messages:[], status:'empty', next_cursor:null}`。

#### jiuwenmate.channel.trace

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | strip 非空 | 同事 agent id |
| `group_id` | string | 是 | strip 非空 | 群 id |
- **响应**：`{started_at: iso|null, rows:[...]}`；无 Session → `{started_at:null, rows:[]}`。

**agent_mail 域**（统一前置 `agent_id`；域内方法均**透传**）：

**agent_mail 域**（统一前置 `agent_id`；域内方法均**透传**）：

#### jiuwenmate.agent_mail.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同事存在性校验 NOT_FOUND | 目标同事 |
- **响应**：`{agent_mail:{status, cli_version, ...}}`；无 runtime → `{status:'unavailable', cli_version:null}`。

#### jiuwenmate.agent_mail.auth.begin

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{authorization:{status, cli_version, operation_id, auth_url, frontend_opens_auth}}`；错误 `AGENT_MAIL_RUNTIME_ERROR`（auth_url 须 https 且 qq.com/tencent.com 域 ≤4096）。

#### jiuwenmate.agent_mail.auth.poll / auth.cancel

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `operation_id` | string | 是 | strip 非空 | 操作 id |
- **响应**：`{agent_mail:{status...}}`（状态机：authorizing→connected）。

#### jiuwenmate.agent_mail.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{agent_mail:{status...}}`。

**dingtalk_auth 域**（统一前置 `agent_id`+`connection_key`，域内方法均**透传**；

**dingtalk_auth 域**（统一前置 `agent_id`+`connection_key`，域内方法均**透传**；绑定解析失败 → `NOT_FOUND 'DingTalk connection is not bound to this employee'`）：

#### jiuwenmate.dingtalk_auth.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 即 config_id | 目标同事 |
| `connection_key` | string | 是 | strip 非空 | 绑定 key |
- **响应**：`{dingtalk_auth:{status, cli_version[, group_monitor]}}`（connected 时附 group_monitor）。

#### jiuwenmate.dingtalk_auth.begin

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
- **响应**：`{authorization:{...}}`；锁忙 `CONFLICT 'DingTalk application binding is busy'`；CLI 侧 `DINGTALK_CLI_APP_NOT_CONFIGURED` 等。

#### jiuwenmate.dingtalk_auth.poll / cancel

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
| `operation_id` | string | 是 | poll 变更 `CONFLICT 'DingTalk authorization operation changed'` | 操作 id |
- **响应**：`{dingtalk_auth:{...}}`。

#### jiuwenmate.dingtalk_auth.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上；允许已保存但停用的绑定 | 目标同事 |
| `connection_key` | string | 是 | 同上 | 绑定 key |
- **响应**：`{dingtalk_auth:{...}}`。

**welink_office 域**（前置 `agent_id`；域内方法均**透传**）：

**welink_office 域**（前置 `agent_id`；域内方法均**透传**）：

#### jiuwenmate.welink_office.status

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | get_colleague 校验 | 目标同事 |
- **响应**：`{welink_office:{status: unavailable\|not_connected\|connected, account?, email?, reason?}}`。

#### jiuwenmate.welink_office.connect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
| `email` | string | 是 | 邮箱正则、≤320、不以 `-` 开头 | 绑定邮箱 |
- **响应**：连接后的 status。错误：`WELINK_NOT_BOUND` / `WELINK_MAIL_DISCOVERY_FAILED`。

#### jiuwenmate.welink_office.disconnect

| 参数 | 类型 | 必填 | 默认/校验 | 说明 |
|------|------|------|----------|------|
| `agent_id` | string | 是 | 同上 | 目标同事 |
- **响应**：`{welink_office: status}`（enabled=0）。

**jiuwenmate.schedule.\***（7 个，均**透传**；区别于 §8.3 的 schedule.*）：`list/create/update/set_enabled/delete/run/history`——字段：`name`+`prompt`（str 必填）、`cron_expr`（缺省 ""）、`timezone`（缺省 Asia/Shanghai）、`schedule_kind`（缺省 cron）、`interval_seconds`（int 可选）、`run_at/starts_at/ends_at`（可选）；create 需 `agent_id`、操作类需 `plan_id`、set_enabled 需 `enabled`（bool）。

**内部 E2A 方法（不在 Web 白名单，网关主动发起；对前端不可达）**：`colleague.connections`/`colleague.remove`（删除编排用）、`connection.attach`、`feishu.prepare`、`group.response`（群成员校验应答）、`delivery.ack`（投递回执：stage="send" + outcome ∈ delivered/failed/unknown/pending）、`thinking_notice.ack`、`feishu/dingtalk/welink.inbound`（归一化事件投递，响应恒 `{disposition: accepted|rejected|retryable}`）。

> ⚠️ `jiuwenmate.dingtalk.binding_check`：白名单在册但**已废弃**（beta2 起由 `connection.attach` 替代，AgentServer 侧 handler 为死代码）；`jiuwenmate.chat.stream` 是事件类型而非请求方法；`jiuwenmate.artifact.list` 服务端存在但未进 Web 白名单。

---

## 附录 A. 方法分发总表

按处理位置归类（"本地"= Gateway Web 层 handler；"E2A 代理"= 本地 handler 内调 `proxy_unary_request`；"白名单转发"= 无本地 handler，经 MessageHandler 转发，响应由 AgentServer 回写）：

| 处理位置 | 方法 |
|---------|------|
| 本地（纯本地逻辑） | `memory.compute`、`hooks.list`、`commands.list`、`path.*`、`updater.*`、`health_check.*`（+heartbeat 别名）、`channel.*` 配置族、`task.asr.transcribe`、`external_cli.*`、`openai_account.*`、`cron.job.*`（9 个）、`project.remove`（Gateway 编排）、`chat.send/interrupt/resume/user_answer/swarmflow_reply`（空 ack shell）、`history.get`（ack shell）、`connection.status`、`topic_starters.*`、`prompt_suggestion.*` |
| E2A 代理（本地 handler + 转发） | `session.list/create/get_metadata/rename/pin/plan_status/message.continue_queued`、`session.selection.set`、`project.list/info/get_sessions/get_cron_sessions/create/rename/pin/restore/pinned_sessions`、`project.git.*`（11 个）、`media.persist/discard`、`document.persist/formats`、`heartbeat.job.*`（10 个，经 HeartbeatControllerProxy）、`permissions.*`（tools/rules/approval_overrides/owner_scopes）、`a4p.*`（6 个，含 authorization.pending）、`harness.*`、`rsi.*`、`designer.*`（13 个，§10.7）、`workspace.tree/preview/usage/entries.delete`、`file.import_url`、`file.upload_chunk`、`config.get/set/save_all`、`models.*`、`model_groups.*`、`vendors.*`、`channel.get`、`memory.forbidden.get/set`（分流） |
| 白名单转发（无本地 handler） | `initialize`、`session.switch/fork`、`session.assets.list/register/rename`、`team.*`（templates.list/bindings.list/binding.create/binding.generate/session.bind/delete/snapshot/history.get/mq.publish/list）、`group.*`、`org.snapshot`、`swarmflow.pause/resume/stop`、`command.goal/btw/compact/mcp/workflows`、`chat.*`（上述空 ack 之外的转发主体）、`history.get`（数据主体）、`schedule.*`、`issue.*`、`skills.*`（40+）、`assets.publish.*`、`mcp.*`、`agents.*`、`agent_groups.*`、`agent_templates.*`、`plugins.*`、`plugin_packages.*`、`extensions.*`、`personal_context.*`、`proactive.feedback` |

`/ws/git` 封闭方法集（6 个）：`project.git.diff_watch/diff_files_watch/diff_detail_watch/diff_unwatch/discard_turn_changes/redo_turn_changes`（§7.3）。

> 方法级「透传 / 网关处理 / 不可达」标注与自动化透传验证见**附录 F**。

当前全集 489 个 WS 方法（个人版 482；企业版另注册 team 控制面 7 个，见 §5.2）。相对旧基线的新增域：`jiuwenmate.*`（46 个转发 + 1 个本地）、`client.*`、`mcp_app.*`、`group.execution.*` / `group.file.*` / `group.history.clear`、`skills.source.*` / `skills.update` / `skills.updates.check`（企业版另有 `skills.enterprise.*`）、`channel.feishu|dingtalk.bind_agent/unbind_agent`、`channel.feishu.get_bindings/revoke`、`channel.welink.*`（7 个，均 local_only 不经转发）。应用插件（extensions registry）还可动态注册额外方法，插件禁用时统一回 `APPLICATION_PLUGIN_DISABLED`。

## 附录 B. 事件类型总表（EventType，协议包 `e2a/message.py`，共 50 个）

| 事件 | 载荷档 | 说明 |
|------|--------|------|
| `hello` | — | 遗留枚举项，网关代码无生产/消费方，前端不应依赖 |
| `connection.ack` | 全量 | 连接建立确认 `{session_id, mode:"BUILD", tools:[], protocol_version:"1.0"}` |
| `chat.delta` / `chat.reasoning` | 裁剪 | 正文/思考流片段（可合并 §4.9.6） |
| `chat.final` | 裁剪(+cron) | 终轮正文；带 `cron` 块时为定时任务结果推送（按 user_id 限定广播） |
| `chat.tool_call` / `chat.tool_update` / `chat.tool_result` | 全量 | 工具调用生命周期 |
| `chat.usage_metadata` / `chat.usage_summary` / `context.usage` | 全量 | 用量 |
| `chat.error` | 全量 | 流式错误终点 `{error, code?, is_complete:true}` |
| `chat.interrupt_result` | 全量 | 中断结果 `{intent, success, message, new_input?, merged_input?, paused_task?, has_active_task?}` |
| `chat.processing_status` | 全量 | 会话忙碌状态 `{is_processing, current_task?, is_complete?}` |
| `chat.ask_user_question` | 全量 | 选择题/确认题 `{request_id, questions[{question, header, options[{label, description?, value?}], multi_select?}], source?, swarmflowMeta?}` |
| `chat.subtask_update` / `chat.subagent_activity` | 全量 | 子任务/子 agent 进度 |
| `chat.symphony_status` / `chat.message_updated` / `chat.retract` / `chat.file` / `chat.media` / `chat.session_result` / `chat.evolution_status` | 全量 | 状态推送/消息修订/撤回/文件/媒体/跨会话结果/自演进 |
| `chat.client_tool_request` | 全量 | AgentServer 请求客户端执行本地工具（Office 加载项/浏览器等）；前端以 chat.send `source:"client_tool_interrupt"` + `answers` 回传结果恢复轮次 |
| `client.attachments_changed` | 全量 | 客户端附件变化通知 |
| `history.message` | 全量 | 历史分片 `{message, cursor/next_cursor, has_more, status: done\|error, ...}` |
| `todo.updated` | 全量 | 待办 |
| `goal.snapshot` / `goal.updated` | 全量 | Goal 快照与状态刷新（每个 attempt 边界发 goal.updated，见 §4.6）；`chat.interrupt_result` 与 `execution.error` 亦附带 `goal` 字段 |
| `goal.confirm_required` | 全量 | set 遇已有 goal 且未带 `overwrite_confirmed`：`{existing_goal, requested_objective}`，is_complete（Web 恒带确认位故实际不出现，§4.6） |
| `plan.approval_required` / `plan.mode_exited` | 全量 | 计划审批/退出 |
| `runtime.accepted` / `execution.error` | 全量 | runtime 受理/执行错误 |
| `team.member` / `team.task` / `team.message` | 全量（前缀） | 集群事件（§5.6） |
| `session.archived/unarchived/deleted` / `session.lifecycle.updated` / `session.message.updated` | 全量 | 会话生命周期（§6.4）；`session.message.updated` 按 owner 定向 |
| `project.lifecycle.updated` / `project.removed` / `project.restored` | 全量 | 项目生命周期（按 user_id 收窄广播） |
| `project.git.diff_changed/diff_files_changed/diff_detail_changed/error` | — | /ws/git 推送（§7.3） |
| `context.compression_state` | 全量 | 压缩进度 |
| `health_check.relay` | 全量 | 探活结果（全量广播，payload 双键 health_check/heartbeat） |
| `workflow.updated` / `designer.*` / `a4p.*` / `harness.*` / `personal_context.context.*` / `rsi.*` / `security.alert` / `proactive_recommendation` | 全量（前缀） | 各域推送 |

## 附录 C. 错误码总表（按域分组）

**通用协议码**见 §3.1。域专用码：

| 域 | code |
|----|------|
| 流式（chat.error 事件） | `AGENT_SERVER_ERROR`、`AGENT_SERVER_TIMEOUT`、`AGENT_SERVER_CONNECTION_CLOSED`、`UNKNOWN_TEAM_MODE` |
| 会话生命周期 | `SESSION_ARCHIVED`、`OPERATION_IN_PROGRESS`、`ALREADY_EXISTS`、`SESSION_CREATE_FAILED`、`SESSION_BUSY`（git 写操作/project.remove） |
| 会话资产 | `INVALID_PARAMS`、`INVALID_NAME`、`ASSET_NOT_FOUND`、`NAME_CONFLICT` |
| 会话信箱 | `INVALID_ARGUMENT`、`HOST_CAPABILITY_UNAVAILABLE`、`NOT_FOUND_OR_FORBIDDEN` |
| 项目 | `PROJECT_DIR_MISSING`、`PROJECT_NAME_CONFLICT`、`PROJECT_RESOLVE_FAILED` |
| Git | `PROJECT_NOT_FOUND`、`NOT_GIT_REPOSITORY`、`NO_TURN_TO_DISCARD/REDO`、`NOTHING_TO_DISCARD/REDO`、`TURN_TARGET_CHANGED`、`DIFF_HISTORY_EXPIRED`、`CHANGE_SET_NOT_FOUND`、`TURN_DIFF_NOT_FOUND`、`SESSION_NOT_FOUND`、`SESSION_NOT_BOUND`、`PROJECT_SESSION_MISMATCH`、`PARTIAL_RESTORE_FAILED`、`PARTIAL_REDO_FAILED`、`REDO_HISTORY_MISSING` |
| 历史 | `INVALID_HISTORY_CURSOR`、`HISTORY_SNAPSHOT_CHANGED` |
| cron | `CRON_STOP_FAILED`、`DELETE_FAILED`、`STOP_TIMEOUT`、`UNSUPPORTED_MODE`、`GENERATION_FAILED` |
| heartbeat | `CONFLICT`（删除/更新冲突）、`FORBIDDEN`（session/user 归属） |
| group | `GROUP_FULL`、`GROUP_CORRUPT`、`GROUP_BUSY`、`GROUP_RESTART_REQUIRED`、`MEMBER_REGISTRATION_FAILED`、`MODE_MISMATCH` |
| swarmflow | `SWARMFLOW_SESSION_BUDGET_EXHAUSTED` |
| goal | `invalid_objective`（objective 空/预算≤0）、`already_exists`（唯一触发 goal_confirm_required 的码）、`no_goal`、`invalid_state`（pause 非 active / resume 终态）、`invalid_action`（含未实现的 stop）、`goal_manager_not_started`、`goal_error`（one-shot 兜底）；blocked 的 evidence 前缀：`round_execution_error:` / `max_attempts_exhausted: N/M` / `token_budget_exhausted: used/budget`（§4.6） |
| 会话输入 | `SESSION_INPUT_TARGET_CHANGED` 等 |
| 模型/更新 | `MODEL_LIST_ERROR`、`AGENT_NOT_READY`、`CONFIG_SYNC_FAILED` |
| ASR | `ASR_NOT_CONFIGURED`、`ASR_BAD_CONFIG`、`ASR_UNSUPPORTED_FORMAT`、`ASR_AUDIO_TOO_LARGE`、`ASR_TIMEOUT`、`ASR_CONNECTION_ERROR`、`ASR_UPSTREAM_ERROR`、`ASR_INVALID_RESPONSE`、`ASR_EMPTY_TRANSCRIPT` |
| 平台 | `UNSUPPORTED`（选择器等） |
| skills | `SKILL_NOT_FOUND / SKILL_INVALID_PACKAGE / SKILL_INVALID_METADATA / SKILL_RESERVED_PATH / SKILL_IMPORT_OVERWRITE_REQUIRED / SKILL_BUILTIN_READ_ONLY / SKILL_ALREADY_EXISTS / SKILL_NAME_CONFLICT / SKILL_UNSAFE_PATH / SKILL_FILE_TOO_LARGE / SKILL_OPERATION_UNSUPPORTED / SKILL_VERSION_NOT_FOUND / SKILL_VERSION_CONTENT_INVALID / SKILL_DOWNLOAD_TOKEN_INVALID / SKILL_KNOWLEDGE_INPUT_CONFLICT / SKILL_REBUILD_FAILED / SKILL_PUBLISH_VERSION_CONFLICT / SKILLHUB_INSTALL_FAILED / SKILLHUB_PUBLISH_FAILED / SKILLHUB_DETAIL_NOT_FOUND / SKILLHUB_DETAIL_FAILED / SKILL_INVENTORY_BUSY（清单锁忙）/ SKILL_REFERENCED_BY_PACK（卸载被技能包引用）；另有非 SKILL_ 前缀的 error_code：prebuilt_not_toggleable / prebuilt_not_removable` |
| 资产发布 | `WEB_CHANNEL_REQUIRED / AUTH_REQUIRED / INVALID_KIND / INVALID_PARAMETERS / INVALID_METADATA / INVALID_PACKAGE / RESOURCE_NOT_FOUND / PUBLISH_UNAVAILABLE`（+ store/build 映射码如 `draft_limit / artifact_limit / VERSION_CONFLICT`） |
| MCP | `MCP_INTERNAL / MCP_BAD_REQUEST / MCP_NOT_FOUND / MCP_UNREACHABLE / MCP_AUTH_FAILED`；CLI 结构化：`MCP_RUNTIME_MISSING / MCP_INSTALL_NETWORK / MCP_CLI_INCOMPLETE / MCP_NAME_CONFLICT / MCP_CREDENTIALS_INVALID / MCP_INSTALL_SCRIPT_DEP` |
| agents 装备包 | `AGENT_GROUP_NAME_INVALID / AGENT_GROUP_DUPLICATE / AGENT_GROUP_LEADER_REQUIRED / AGENT_GROUP_MEMBER_*`（INVALID/CONFLICT/RESERVED/DUPLICATE/NOT_FOUND/INCOMPATIBLE/AGENT_MD 共 7 个）/ `AGENT_GROUP_TEMPLATE_INVALID / AGENT_GROUP_MEMBERS_REQUIRED / AGENT_GROUP_REQUEST_FAILED（组方法无 code 异常兜底）/ AGENT_GROUP_NOT_INSTALLED / AGENT_GROUP_UNAVAILABLE / HUB_ASSET_NOT_FOUND`（逐码触发条件见 §10.6.9 错误码总表） |
| personal_context | **Core 数值串**：`154000`（配置/参数）、`154001`（状态）、`154002`（文件/图）、`154003-154006`（采集/处理/写入/超时）——payload 附加 `status` 状态名 |
| a4p | `A4P_NOT_PENDING / A4P_INVALID_SELECTION / A4P_ROUTE_MISMATCH / A4P_REPREPARE_FAILED / A4P_DISABLED / A4P_CONFIG_APPLY_FAILED / A4P_AUTHORIZATION_INVALID / A4P_WEB_SESSION_REQUIRED / A4P_AUTHORIZATION_TIMEOUT / A4P_AUTHORIZATION_CANCELLED` |
| rsi | `SCENARIO_NOT_SUPPORTED / PATH_INVALID / DATASET_INVALID / INVALID_HARNESS / MODEL_NOT_FOUND / MODEL_CONFIG_INVALID / UNSUPPORTED_PARAMETER / TASK_NOT_FOUND / TASK_STATE_CONFLICT / ARTIFACT_NOT_FOUND / RESUME_MISMATCH / RESUME_INPUT_CHANGED / RSI_HARNESS_NOT_READY / RSI_HARNESS_NOT_PUBLISHED / RSI_HARNESS_INVALID / RSI_HARNESS_INSTALL_CONFLICT / RSI_HARNESS_INSTALL_FAILED` |
| HTTP auth | `auth_header_required`(403)、`login_disabled`(404)、`not_logged_in`(401)、`session_expired`(401)、`oauth_state_invalid`、`oauth_access_denied`、`oauth_callback_failed`、`oauth_callback_missing_params`、`oauth_callback_replayed` |
| file-api | `UNAUTHORIZED`、`AUTH_REQUIRED`(401)；`FORBIDDEN`、`FORBIDDEN_DIR`、`FORBIDDEN_EXTENSION`、`FORBIDDEN_PATH`、`USER_MISMATCH`(403)；`INVALID_PATH`、`MISSING_FILE_PATH`、`MISSING_SESSION_ID`、`MISSING_FILE_CONTENT`、`ONLY_MARKDOWN_SUPPORTED`、`BAD_REQUEST`(400)；`INSTANCE_NOT_FOUND`、`FILE_NOT_FOUND`、`NOT_FOUND`(404)；`PAYLOAD_TOO_LARGE`、`FILE_TOO_LARGE`(413)；`LIST_NOT_SUPPORTED`、`NOT_IMPLEMENTED`(501)；其余 502 |
| trajectory | `TRAJECTORY_DISABLED`(503)、`TRAJECTORY_QUERY_FAILED`/`TRAJECTORY_REQUEST_FAILED`/`SESSION_LOOKUP_FAILED`(500)、`FORBIDDEN_ORIGIN`/`UNSUPPORTED_SESSION_MODE`(403)、`METHOD_NOT_ALLOWED`(405) |
| E2A 上传 | `UPLOAD_FAILED` |

> ⚠️ **注意**：部分 AgentServer 侧错误响应**不携带 code**（如 command.btw/compact、schedule.*、swarmflow_reply 缺参），前端只能依赖 `ok:false` + `error` 文本。

## 附录 D. 关键限制常量速查

| 常量 | 值 | 位置 |
|------|----|------|
| `SESSION_ID_MAX_LEN` | 80（正则见 §2.3） | `common/protocol_ids.py:15-21` |
| `WORKFLOW_RUN_ID_MAX_LEN` | 256 | `common/protocol_ids.py:20` |
| `WEB_WS_MAX_MESSAGE_BYTES` | 150 MB | `common/ws_limits.py:11` |
| `AGENT_WS_MAX_MESSAGE_BYTES` / `E2A_PAYLOAD_MAX_BYTES` | 8 MB / 4 MB（分块 2 MB） | `common/ws_limits.py:6`、`agent_http_bridge.py:31,35` |
| `AGENT_WS_SEND_BUDGET_BYTES` | 6 MB（AgentServer 链路发送预算，新增） | `common/ws_limits.py:7` |
| 流帧合并上限 | 32 帧（仅 chat.delta/reasoning） | `web_connect.py:94-95` |
| 出站队列 / 单帧发送超时 / 写停滞 abort | 512 帧 / 10s / 60s | `base_ws_channel.py:29`、`transport_stall_guard.py:33` |
| 断连后在途任务宽限 | 5s | `web_connect.py:1456-1462` |
| history 分页 | 默认 50/片；chat.final 分片 ≤32KB | `server/wire_truncate.py:33,39-42` |
| team history | limit 默认 500（1–1000）、max_bytes 默认 2MB（2048–6MB） | `server/wire_truncate.py:43-47` |
| cron job | name ≤64、description ≤500、timeout 60s–72h、preview count 1–50 | `runtime/cron/models.py` |
| heartbeat job | name ≤64、prompt ≤2000、interval ≥60s、每 session 活跃 ≤5、全局 ≤100、执行超时 300s | `agents/harness/code/rails/heartbeat/models.py` |
| ASR 音频 | ≤25 MB | `task_asr.py:17` |
| file-api 上传 | ≤20 个文件 | `container_file_http.py:25` |
| trajectory | stream-frames limit ≤2000、sequences ≤200、records limit ≤1000、archive ≤16MB 内存 | `trajectory_http.py:50-65` |
| 前端默认请求超时 | 15s（可按请求覆盖） | `webClient.ts:29` |

---

## 附录 E. 端到端调用样例

以下样例基于代码逐帧核对（字段与结构均为实际线上行为），`→` 为客户端发送、`←` 为服务端下发；`...` 表示省略的重复帧。req id 采用前端实际格式 `req_<base36时间戳>_<序号>`（`webClient.ts:596-600`）。

### E.1 连接建立

```jsonc
// 握手：ws://127.0.0.1:19000/ws?user_id=alice&session_id=sess_18f3ab2c_9d4e1f
// （user_id 为用户名；session_id 可省略，由服务端合成占位 id）

// AgentServer 就绪时，连接成功后立即收到：
← { "type": "event", "event": "connection.ack",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "mode": "BUILD",
                  "tools": [], "protocol_version": "1.0" } }
```

### E.2 单 Agent 对话完整轮（含工具调用）

```jsonc
→ { "type": "req", "id": "req_m0abc_1", "method": "chat.send",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f",
                 "content": "帮我看看当前项目的目录结构",
                 "mode": "agent",
                 "model_name": "qwen3-max" } }

// 1) 受理 ack（业务成败看事件流终点，不看这个 res）
← { "type": "res", "id": "req_m0abc_1", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f" } }

// 2) 网关先发忙碌状态
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f",
                  "is_processing": true, "is_complete": false } }

// 3) runtime 受理（timestamp 为执行机毫秒钟，前端据此回填乐观气泡）
← { "type": "event", "event": "runtime.accepted",
     "payload": { "event_type": "runtime.accepted", "request_id": "req_m0abc_1",
                  "timestamp": 1760600000000 } }

// 4) 思考流与正文流交错（裁剪档 payload：session_id + content；相邻同类帧网关可能合并）
← { "type": "event", "event": "chat.reasoning",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "content": "用户需要目录结构，我先列出文件。" } }
← { "type": "event", "event": "chat.delta",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "content": "我先查看项目目录。" } }

// 5) 工具调用（全量 payload）
← { "type": "event", "event": "chat.tool_call",
     "payload": { "event_type": "chat.tool_call", "session_id": "sess_18f3ab2c_9d4e1f",
                  "tool_call": { "id": "call_01", "name": "list_files",
                                 "arguments": { "path": "." } } } }
← { "type": "event", "event": "chat.tool_result",
     "payload": { "event_type": "chat.tool_result", "session_id": "sess_18f3ab2c_9d4e1f",
                  "tool_name": "list_files", "tool_call_id": "call_01",
                  "status": "success", "result": "...", "rendered_result": "src/\ndocs/\n..." } }

// 6) 终轮正文与收尾
← { "type": "event", "event": "chat.final",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f",
                  "content": "项目结构如下：src/ 是主代码，docs/ 是文档 ..." } }
← { "type": "event", "event": "chat.usage_summary",
     "payload": { "event_type": "chat.usage_summary", "session_id": "sess_18f3ab2c_9d4e1f", ... } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f",
                  "is_processing": false, "is_complete": true } }
```

### E.3 流式出错

```jsonc
// AgentServer 中途异常时，流以 chat.error 收尾（is_complete:true）
← { "type": "event", "event": "chat.error",
     "payload": { "event_type": "chat.error", "session_id": "sess_18f3ab2c_9d4e1f",
                  "error": "AgentServer connection closed",
                  "code": "AGENT_SERVER_CONNECTION_CLOSED", "is_complete": true } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "is_processing": false } }
```

### E.4 取消（chat.interrupt）

```jsonc
→ { "type": "req", "id": "req_m0abd_2", "method": "chat.interrupt",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f", "intent": "cancel" } }

← { "type": "res", "id": "req_m0abd_2", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f", "intent": "cancel" } }

// cancel 等 AgentServer 真实结果后转发 interrupt_result，随后网关合成忙碌 false：
← { "type": "event", "event": "chat.interrupt_result",
     "payload": { "event_type": "chat.interrupt_result", "session_id": "sess_18f3ab2c_9d4e1f",
                  "intent": "cancel", "success": true, "message": "任务已取消" } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "is_processing": false } }

// 随后 AgentServer 侧确认：被中断工具逐个补发 error 结果（卡片不再悬转）
← { "type": "event", "event": "chat.tool_result",
     "payload": { "event_type": "chat.tool_result", "session_id": "sess_18f3ab2c_9d4e1f",
                  "tool_call_id": "call_02", "status": "error", "error": "[Interrupted]" } }
```

### E.5 暂停与恢复

```jsonc
→ { "type": "req", "id": "req_m0abe_3", "method": "chat.interrupt",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f", "intent": "pause" } }
← { "type": "res", "id": "req_m0abe_3", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f", "intent": "pause" } }
← { "type": "event", "event": "chat.interrupt_result",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "intent": "pause",
                  "success": true, "message": "任务已暂停", "has_active_task": true } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "is_processing": false, "paused": true } }   // 服务端权威帧（网关不再为 pause 合成 true）

// 恢复（chat.resume 在网关被改写为 chat.interrupt + intent=resume，行为等价）：
→ { "type": "req", "id": "req_m0abf_4", "method": "chat.resume",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f" } }
← { "type": "res", "id": "req_m0abf_4", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f" } }
← { "type": "event", "event": "chat.interrupt_result",
     "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "intent": "resume",
                  "success": true, "message": "任务已恢复" } }
```

### E.6 选择题交互（ask_user_question → user_answer）

```jsonc
// 服务端下发问题（本轮未结束，等待作答）
← { "type": "event", "event": "chat.ask_user_question",
     "payload": { "event_type": "chat.ask_user_question", "session_id": "sess_18f3ab2c_9d4e1f",
                  "request_id": "call_confirm_7",
                  "questions": [ { "question": "检测到未提交的修改，如何处理？",
                                   "header": "处理方式",
                                   "options": [ { "label": "提交", "description": "git commit 后继续" },
                                                { "label": "放弃", "description": "还原修改" } ],
                                   "multi_select": false } ] } }

→ { "type": "req", "id": "req_m0ac0_5", "method": "chat.user_answer",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f", "request_id": "call_confirm_7",
                 "answers": [ { "question": "检测到未提交的修改，如何处理？",
                                "selected_options": [ "提交" ] } ] } }
← { "type": "res", "id": "req_m0ac0_5", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f",
                  "request_id": "call_confirm_7" } }
// 之后流继续，最终以 chat.final 收尾
```

### E.7 Team 集群会话（scheduled 派发）

```jsonc
→ { "type": "req", "id": "req_m0ac1_6", "method": "chat.send",
     "params": { "session_id": "sess_team_01", "content": "调研竞品并输出报告",
                 "mode": "team", "work_mode": "work",
                 "team_dispatch_mode": "scheduled" } }   // 首请求固定派发模式并落盘 metadata
← { "type": "res", "id": "req_m0ac1_6", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_team_01" } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_team_01", "is_processing": true, "is_complete": false } }

// 成员生成（agent_ref 标识来源）
← { "type": "event", "event": "team.member",
     "payload": { "event_type": "team.member", "session_id": "sess_team_01",
                  "agent_ref": { "mode": "team", "id": "team_report" },
                  "event": { "type": "spawned", "member_id": "m_researcher",
                             "mode": "build", "role": "teammate", "name": "研究员" } } }

// 看板任务（status 由服务端收敛：created→pending、claimed/started→in_progress、completed→completed）
← { "type": "event", "event": "team.task",
     "payload": { "event_type": "team.task", "session_id": "sess_team_01",
                  "agent_ref": { "mode": "team", "id": "team_report" },
                  "event": { "task_id": "task_01", "member_id": "m_researcher",
                             "status": "pending", "title": "竞品调研" } } }

// 成员间消息
← { "type": "event", "event": "team.message",
     "payload": { "event_type": "team.message", "session_id": "sess_team_01",
                  "agent_ref": { "mode": "team", "id": "m_researcher" },
                  "event": { "type": "team.message.p2p", "message_id": "msg_01",
                             "from_member": "m_researcher", "to_member": "m_writer",
                             "content": "调研数据已整理", "protocol": "..." } } }

// teammate 输出（role=teammate + member_name 区分；teammate 的 reasoning 被丢弃不下发）
← { "type": "event", "event": "chat.delta",
     "payload": { "session_id": "sess_team_01", "content": "调研完成，共 3 家竞品 ...",
                  "role": "teammate", "member_name": "研究员",
                  "team_name": "team_report", "team_id": "team_report",
                  "agent_ref": { "mode": "team", "id": "m_researcher" } } }

// leader 汇总 → 终轮 → 空闲
← { "type": "event", "event": "chat.final",
     "payload": { "session_id": "sess_team_01", "role": "leader",
                  "content": "报告已完成，摘要如下 ..." } }
← { "type": "event", "event": "chat.processing_status",
     "payload": { "session_id": "sess_team_01", "is_processing": false, "is_complete": true } }
```

### E.8 创建会话与列表

```jsonc
→ { "type": "req", "id": "req_m0ac2_7", "method": "session.create",
     "params": { "mode": "agent", "project_id": "default", "work_mode": "work",
                 "title": "新会话", "model_selection": { "type": "model", "id": "qwen3-max" } } }
← { "type": "res", "id": "req_m0ac2_7", "ok": true,
     "payload": { "sessionId": "sess_18f3ac9_77e2a1", "session_id": "sess_18f3ac9_77e2a1",
                  "projectId": "default", "projectDir": "", "workMode": "work",
                  "persist_session": false, "moa_enabled": false,
                  "prewarm_hit": false, "prewarm_status": "miss" } }

→ { "type": "req", "id": "req_m0ac3_8", "method": "session.list", "params": { "limit": 20, "offset": 0 } }
← { "type": "res", "id": "req_m0ac3_8", "ok": true,
     "payload": { "sessions": [ { "session_id": "sess_18f3ac9_77e2a1", "title": "新会话",
                                   "created_at": 1760600000.0, "last_message_at": 1760600000.0,
                                   "message_count": 0, "mode": "agent", "team_name": "",
                                   "pinned": false, "pin_order": 0, "project_id": "default",
                                   "project_dir": "", "cron_id": "", "model": "qwen3-max",
                                   "model_selection": { "type": "model", "id": "qwen3-max" },
                                   "moa_enabled": false, "work_mode": "work",
                                   "execution_blocked": false, "stop_pending": false } ],
                  "total": 1, "limit": 20, "offset": 0 } }
```

### E.9 历史拉取（游标协议）

```jsonc
// params 中出现 "cursor" 键即切换游标协议；首批额外先推 todo.updated 快照
→ { "type": "req", "id": "req_m0ac4_9", "method": "history.get",
     "params": { "session_id": "sess_18f3ab2c_9d4e1f", "cursor": null, "limit": 50 } }
← { "type": "res", "id": "req_m0ac4_9", "ok": true,
     "payload": { "accepted": true, "session_id": "sess_18f3ab2c_9d4e1f",
                  "cursor": null, "limit": 50 } }
← { "type": "event", "event": "todo.updated", "payload": { "session_id": "sess_18f3ab2c_9d4e1f", "todos": [] } }
← { "type": "event", "event": "history.message",
     "payload": { "event_type": "history.message", "session_id": "sess_18f3ab2c_9d4e1f",
                  "message": { "role": "assistant", "content": "项目结构如下：..." },
                  "cursor": null, "next_cursor": "cur_a1", "has_more": true,
                  "snapshot_id": "snap_01", "snapshot_end": false } }
// ... 继续以 next_cursor 请求，直到：
← { "type": "event", "event": "history.message",
     "payload": { "event_type": "history.message", "session_id": "sess_18f3ab2c_9d4e1f",
                  "message": { "role": "user", "content": "帮我看看当前项目的目录结构" },
                  "cursor": "cur_a2", "next_cursor": null, "has_more": false,
                  "snapshot_id": "snap_01", "snapshot_end": true, "status": "done" } }
```

### E.10 定时任务

```jsonc
// 创建
→ { "type": "req", "id": "req_m0ac5_10", "method": "cron.job.create",
     "params": { "name": "每日日报", "cron_expr": "0 9 * * *",
                 "description": "总结今天的工作并生成日报", "targets": "web",
                 "mode": "agent", "project_id": "default", "timeout_seconds": 1800 } }
← { "type": "res", "id": "req_m0ac5_10", "ok": true,
     "payload": { "job": { "id": "c9f1e2d3a4b54768901234567890abcd", "name": "每日日报",
                           "enabled": true, "expired": false, "cron_expr": "0 9 * * *",
                           "timezone": "Asia/Shanghai", "wake_offset_seconds": 0,
                           "description": "总结今天的工作并生成日报", "targets": "web",
                           "created_at": 1760600000.0, "updated_at": 1760600000.0,
                           "mode": "agent", "timeout_seconds": 1800,
                           "project_id": "default", "work_mode": "work",
                           "model_name": "qwen3-max", "user_id": "alice" } } }

// 预览未来 5 次触发
→ { "type": "req", "id": "req_m0ac6_11", "method": "cron.job.preview",
     "params": { "id": "c9f1e2d3a4b54768901234567890abcd", "count": 5 } }
← { "type": "res", "id": "req_m0ac6_11", "ok": true,
     "payload": { "next": [ { "wake_at": "2026-10-17T09:00:00+08:00",
                              "push_at": "2026-10-17T09:00:00+08:00" }, ... ] } }

// 立即执行（返回真实预分配会话）
→ { "type": "req", "id": "req_m0ac7_12", "method": "cron.job.run_now",
     "params": { "id": "c9f1e2d3a4b54768901234567890abcd" } }
← { "type": "res", "id": "req_m0ac7_12", "ok": true,
     "payload": { "accepted": true, "run_id": "c9f1e2d3...:1760600100",
                  "session_id": "sess_cron_c9f1e2_01" } }

// 执行完成后，结果以带 cron 块的 chat.final 推送（按 user_id 限定接收连接，支持换设备收取）
← { "type": "event", "event": "chat.final",
     "payload": { "session_id": "sess_cron_c9f1e2_01", "content": "今日日报：...",
                  "cron": { "job_id": "c9f1e2d3...", "run_id": "c9f1e2d3...:1760600100",
                            "push_at": "...", "current_time": "..." },
                  "user_id": "alice" } }
```

### E.11 /ws/git 订阅

```jsonc
// 独立连接 ws://127.0.0.1:19000/ws/git（同握手鉴权；method 集合封闭为 6 个）

→ { "type": "req", "id": "g1", "method": "project.git.diff_watch",
     "params": { "project_id": "p32hex...", "session_id": "sess_18f3ab2c_9d4e1f",
                 "scope": "summary", "include_last_turn": true } }
← { "type": "res", "id": "g1", "ok": true,
     "payload": { "watch_id": "w_a1", "scope": "summary",
                  "snapshot": { "project_id": "p32hex...", "session_id": "sess_18f3ab2c_9d4e1f",
                                "repo": { "is_git": true, "repo_root": "/path", "branch": "main",
                                          "head": "abc1234", "transient": false,
                                          "repo_is_parent_of_project": false },
                                "current": { "is_dirty": true, ... },
                                "last_turn": null,
                                "revision": "gitdiff:1760600000:init" } } }

// 文件变化后推送（seq/stream_id 恒为 null，不携带）
← { "type": "event", "event": "project.git.diff_changed",
     "payload": { "watch_id": "w_a1", "revision": "gitdiff:1760600123:...", ... } }

// 会话忙碌时撤销本轮修改会被拒：
→ { "type": "req", "id": "g2", "method": "project.git.discard_turn_changes",
     "params": { "project_id": "p32hex...", "session_id": "sess_18f3ab2c_9d4e1f" } }
← { "type": "res", "id": "g2", "ok": false,
     "error": "session is busy; stop the current run before discarding/redoing changes",
     "code": "SESSION_BUSY" }
```

### E.12 常见错误响应

```jsonc
// 帧结构 / 参数类型
← { "type": "res", "id": "req_x", "ok": false, "error": "params.mode must be a string", "code": "BAD_REQUEST" }
← { "type": "res", "id": "req_x", "ok": false, "error": "invalid json", "code": "BAD_REQUEST" }

// session_id 非法（正则 ^[A-Za-z0-9_](?:[A-Za-z0-9_.-]{0,78}[A-Za-z0-9_])?$，≤80）
← { "type": "res", "id": "req_x", "ok": false, "error": "invalid session_id", "code": "BAD_REQUEST" }

// 未知方法
← { "type": "res", "id": "req_x", "ok": false, "error": "unknown method: foo.bar", "code": "METHOD_NOT_FOUND" }

// AgentServer 不可达（转发型方法）
← { "type": "res", "id": "req_x", "ok": false, "error": "AgentServer is unavailable", "code": "SERVICE_UNAVAILABLE" }

// AgentServer 超时（unary）
← { "type": "res", "id": "req_x", "ok": false, "error": "AgentServer request timed out", "code": "AGENT_SERVER_TIMEOUT" }

// handler 异常兜底
← { "type": "res", "id": "req_x", "ok": false, "error": "handler error: ...", "code": "INTERNAL_ERROR" }

// 资源不存在 / 越权（cron job 不存在或不属于当前用户，均不泄露存在性）
← { "type": "res", "id": "req_x", "ok": false, "error": "job not found", "code": "NOT_FOUND" }
```

### E.13 HTTP 接口样例

```bash
# ── 华为账号登录（浏览器） ──────────────────────────────
# 1) 发起登录（必须带防跨站头）
curl -X POST http://127.0.0.1:19000/api/v1/auth/authorize -H "X-Jiuwen-Auth: 1"
# → 200 {"authorizeUrl":"https://...","state":"st_...","claimToken":"tk_...","expiresIn":300}

# 2) 浏览器跳转 authorizeUrl 完成账号授权 → 回调 /api/v1/auth/callback?state=...&code=...
#    落地页通过 BroadcastChannel 通知同源页并自动关闭；cookie jiuwenswarm_auth 已种下

# 3) 轮询认领（未完成 202 / 完成 200）
curl -X POST http://127.0.0.1:19000/api/v1/auth/claim -H "X-Jiuwen-Auth: 1" \
     -H "Content-Type: application/json" -d '{"state":"st_...","claimToken":"tk_..."}'
# → 200 {"ok":true,"islogin":true,"sessionId":"...","userId":"...","userName":"alice","createdAt":...}

# 4) 查询登录模型目录
curl http://127.0.0.1:19000/api/v1/auth/models --cookie "jiuwenswarm_auth=..."
# → {"ok":true,"islogin":true,"whitelistApplied":true,"models":[{...}]}

# ── 容器文件（仅 AgentOS 部署，token 鉴权） ─────────────
curl "http://127.0.0.1:19000/file-api/list-files?dir=project/src&user_id=alice" \
     -H "Authorization: Bearer <token>"
# → {"files":[{"name":"main.py","path":"/home/agentos/users/alice/project/src/main.py",
#              "isDirectory":false,"isMarkdown":false,"size":1024,"modifiedTime":...,"type":"file"}]}

curl -X POST http://127.0.0.1:19000/file-api/upload -H "Authorization: Bearer <token>" \
     -F "dir=project/uploads" -F "session_id=sess_..." -F "file=@report.pdf"
# → {"ok":true,"files":[{"filename":"report.pdf","path":"...","mime_type":"application/pdf","size_bytes":...}],"errors":[]}

# ── 调试轨迹 ───────────────────────────────────────────
curl "http://127.0.0.1:19000/api/trajectory/sessions/sess_18f3ab2c_9d4e1f/subjects?after_revision=0"
# → {"schema_version":1,"session_id":"...","items":[...],"watermark":42,"store_epoch":...}
```

### E.14 最小 WS 客户端（浏览器 JS）

```js
const ws = new WebSocket('ws://127.0.0.1:19000/ws?user_id=alice');
let seq = 0;
const nextId = () => `req_${Date.now().toString(36)}_${++seq}`;

function request(method, params) {
  return new Promise((resolve, reject) => {
    const id = nextId();
    const onMessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === 'res' && msg.id === id) {
        ws.removeEventListener('message', onMessage);
        msg.ok ? resolve(msg.payload) : reject(Object.assign(new Error(msg.error), { code: msg.code }));
      }
    };
    ws.addEventListener('message', onMessage);
    ws.send(JSON.stringify({ type: 'req', id, method, params }));
  });
}

ws.addEventListener('message', (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.type === 'event') {
    switch (msg.event) {
      case 'chat.delta': /* 流式正文 */ break;
      case 'chat.final': /* 终轮 */ break;
      case 'chat.error': /* 错误终点（code 见附录 C） */ break;
      case 'chat.processing_status': /* 忙碌状态 */ break;
    }
  }
});

ws.addEventListener('open', async () => {
  // chat.send 的 res 只是受理 ack，正文全部走事件流
  await request('chat.send', { session_id: 'sess_18f3ab2c_9d4e1f',
                               content: '你好', mode: 'agent' });
});
```

> ⚠️ 样例中的 `...` 省略号与注释仅为可读性；线上帧均为紧凑 JSON，且事件帧**不含** `seq`/`stream_id`（§4.9.3）。各事件的完整字段以附录 B 与代码为最终实现源。

---

## 附录 F. 方法级网关处理标注总表（透传 vs 网关处理）

> 依据网关 `upstream/develop@0d50e02b0` 的转发注册表、帧级校验与归一化实现汇总。**全量透传结论由 `st_test/testcases_gateway/test_gateway_forward_passthrough.py`（267 个用例，真实网关进程 + Stub AgentServer）逐方法验证**：纯透传方法按本文档参数约束调用后，E2A 信封 `params` 逐键原样到达 AgentServer。
> 形态说明：个人版 WS 方法全集 **482**（企业版另注册 team 控制面 7 个 = 489）；`/ws/git` 封闭协议 6 个另计（§7.3）。

### F.1 分发类别与网关处理一览（个人版）

| 类别 | 数量 | 方法 | 网关处理 |
|---|---|---|---|
| **纯透传** | 231（含下行 5 个下游未实现方法；其中 jiuwenmate.\* 46 个的服务端实现在 AgentServer `releases/0.2.9.beta2` 发布线——develop 线不含 server/jiuwenmate，契约按该发布线核对） | `_FORWARD_NO_LOCAL_HANDLER_METHODS` 全集：skills.\*、mcp.\*、agents.\*、装备包三族、plugins/extensions、personal_context.\*、a4p.\*、rsi.\*、team.\*、group.\*、org.snapshot、swarmflow.\*、command.\*、initialize、session.switch/fork、session.assets.\*、assets.publish.\*、jiuwenmate.\*、client.\*、mcp_app.\* 等 | **无**：params 逐键原样转发（仅 F.2 通用归一化）；`is_stream` 透传客户端顶层值 |
| 网关注册、AgentServer 未实现 | 7 | group.execution.list/get、group.file.rename/delete、group.history.clear（含于纯透传 231）；client.conversations（含于纯透传）；models.refresh_status（E2A 代理） | Gateway 按字符串正常转发（网关侧透传已由 ST 验证）；**AgentServer 当前版本无对应 handler**，南向落通用 fall-through——业务契约未定义 |
| 派发等同纯透传 | 13 | issue.\*（4）+ schedule.\*（9）——白名单在册但未列入 no-local 名单、无本地 handler | 无（ST 已验证 params 相等） |
| 白名单 + 网关处理 | 6 | chat.send / chat.interrupt / chat.resume / chat.user_answer / chat.swarmflow_reply / history.get | 见 F.3 逐方法改写 |
| E2A 代理 | 128 | session.\*、project.\*、project.git.\*、media.\*、document.\*、heartbeat.job.\*、permissions.\*、a4p.\*、harness.\*、rsi.\*、designer.\*（13）、config 面板（config.\*/models.\*/model_groups.\*）、session.selection.set、workspace.\*、file.import_url/upload_chunk 等（注册本地 handler 内构造 E2A 请求） | 见 F.4 网关侧处理点 |
| 纯本地 | 68 | cron.job.\*（9）、updater.\*（7）、channel.get + 九渠道 get_conf/set_conf（19）+ wechat 登录/解绑（2）、path.\*（5）、external_cli.\*（8）、task.asr.transcribe、memory.compute、memory.forbidden.get/set、hooks.list、commands.list、locale.\*、openai_account.\*（6）、health_check/heartbeat.get_conf/set_conf（4）、connection.status、vendors.\*（2） | 网关内执行不触达 AgentServer；path.select_\*/path.get/set、hooks.list、memory.forbidden、locale 带 legacy 本地 / AgentOS→E2A 分流 |
| 网关薄包装转发 | 10 | topic_starters.\*（8）、prompt_suggestion.\*（2） | RPC 名改写为 `action` 字段经 `recommendation.job` 转发（注入 auth_session）；与纯本地/纯透传均不同 |

> **逐方法标注**：全量 489 个方法的分类与处理要点见 **F.6 逐方法标注总表**（程序化生成，与注册表同口径）。
| **不可达** | 11 | session.assets.remove、skills.enterprise.{list,install,uninstall,source.providers,source.search}、skills.source.{providers,search,install}、skills.update、skills.updates.check | Gateway 字符串透传；Swarm Front 对未注册方法返回 `METHOD_NOT_FOUND`，ST 用例锁定该集合以提醒契约同步 |
| 企业版专用 | 7 | team.create / team.get / team.stop / team.member.join / leave / team.members.list / team.quota.get | 个人版从白名单与注册表双双剔除 → `METHOD_NOT_FOUND`；HTTP 路由同步过滤 |
| /ws/git 封闭协议 | 6 | project.git.diff_watch / diff_files_watch / diff_detail_watch / diff_unwatch / discard_turn_changes / redo_turn_changes | 独立分发：网关 busy 预检 + 转发 + `preserve_error_payload` |
| local_only（注册但不转发） | 14 | channel.feishu/dingtalk.bind_agent/unbind_agent、channel.feishu.get_bindings/revoke、channel.welink.\*（7）、jiuwenmate.colleague.delete | 网关本地执行；企业版拦截为 **handler 内自检**（WS/HTTP 均拦，区别于 invoke 的 HTTP-only 写保护）：绑定/撤销与 welink 全部方法 → FORBIDDEN（feishu.get_bindings 放行） |

### F.2 全部转发方法共用的网关归一化（帧进网关即发生）

| 改写点 | 规则 | 影响 |
|---|---|---|
| content→query | params 无 `query` 且有 `content` 时复制 content 为 query | 全部转发方法（含纯透传——传 content 即附带 query） |
| is_stream | chat.send / history.get 恒 true；其余透传客户端顶层值 | 全部 |
| session_id | 显式携带则帧级格式校验（≤80 正则）并 re-register 路由；缺省沿用连接注册会话 | 全部 |
| params 非对象 | 静默置 `{}` | 全部 |
| 帧级校验 | `params.mode`/`agent_id` 必须 string；`group_id` 须等于显式 session_id；swarmflow 的 run_id/workflow_run_id 同传须相等 | 全部（失败 `BAD_REQUEST`，不转发） |
| 登录凭据 | `_model_auth` 无条件先删客户端同名键；登录模型时服务端注入凭据、过期先同步续期（≤20s） | 全部转发 |

### F.3 网关处理方法（6 个）的逐方法改写

| 方法 | 网关处理 | 到达 AgentServer 的形态 |
|---|---|---|
| chat.send | 本地 handler 先回 ack `{accepted:true}`；先发 `chat.processing_status(true)`；归一化 content→query；强制流式；后台消费流事件 | method=chat.send，params=发送值+query 复制，is_stream=true |
| chat.interrupt | 本地 handler 回 ack；cancel 分支**重构信封** `{intent:"cancel", session_id}` + 透传定位参数（mode/team/trusted_dirs/work_mode/project_dir）；`agent_notify="await"` 等真实结果再转发 interrupt_result | method=chat.interrupt，params=重构值 |
| chat.resume | 归一化改写为 chat.interrupt + `intent:"resume"`（setdefault 语义，不覆盖显式值） | method=chat.interrupt |
| chat.user_answer | 本地 handler 回 ack；转发原 params | 原样 |
| chat.swarmflow_reply | 本地 handler 回 ack；非流式转发原 params | 原样 |
| history.get | 本地 handler 回 ack（参数回显）；强制流式；不抬升/不清 busy | 原样 + is_stream=true |

### F.4 E2A 代理与纯本地的逐方法网关处理

> 已合并至正文各方法的 `> **网关处理**` 标注行（含 E2A 组装方式、参数改动、本地处理与返回值）。

### F.5 透传保证的自动化验证

`st_test/testcases_gateway/test_gateway_forward_passthrough.py`（267 用例，方法清单从网关注册表动态导入保证与代码同步）：

1. 纯透传 244 个方法逐方法参数化——断言 stub 收到的 E2A 信封 `params` 与发送**逐键相等**、`channel=web`、`is_stream` 语义正确；
2. chat 族 6 方法断言 F.3 的文档化改写（cancel 重构与定位参数透传、resume 的 intent setdefault、强制流式）；
3. rsi.session_id 注入断言（未携带时注入连接会话 id）；
4. 帧级拒绝负例（mode 非字符串 / session_id 非法 / group_id 不匹配 / swarmflow run_id 不一致 → `BAD_REQUEST` 且不触达 AgentServer）；未知方法与个人版 team 控制面 → `METHOD_NOT_FOUND`；
5. 不可达集合硬校验（11 方法；协议包补枚举后测试失败以提醒同步本文档）。

运行：`python -m pytest st_test/testcases_gateway --no-cov`。

### F.6 逐方法标注总表（全量 489 个，程序化生成）

> 由网关转发注册表 + 协议枚举 + 注册方法清单程序化生成（与 ST 同口径）；分类口径与 F.1 一致，要点列为该方法**特有**的处理（通用归一化见 F.2）。`模型 M 属多类时按最高优先级归类：不可达 > 企业专用 > AgentServer 未实现 > 网关处理 > 透传 > local_only > 薄包装 > 纯本地 > E2A 代理`。

| 方法 | 网关处理分类 | 要点 / 改写 |
|------|-------------|------------|
| `a4p.authorization.complete` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `a4p.authorization.pending` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `a4p.authorization.reject` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `a4p.authorization.reprepare` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `a4p.config.get` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `a4p.config.update` | E2A 代理（本地 handler 构造请求） | 前置检查 AGENT_NOT_READY / A4P_WEB_SESSION_REQUIRED |
| `acp.tool_response` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.file.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.file.read` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.import_local` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.show` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_groups.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.file.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.file.read` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.import_local` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.show` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agent_templates.update` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.disable` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.enable` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.tools_list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `agents.update` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.commit` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.describe` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.local_status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.prepare` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.records` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `assets.publish.status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `channel.dingtalk.bind_agent` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.dingtalk.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.dingtalk.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.dingtalk.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.dingtalk.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.dingtalk.unbind_agent` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.discord.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.discord.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.discord.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.discord.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.feishu.bind_agent` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.feishu.get_bindings` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.feishu.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.feishu.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.feishu.revoke` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.feishu.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.feishu.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.feishu.unbind_agent` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.get` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.get", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"channels": [{"channel_id": "feishu"}, {"channel_id": "web"}]}}
```

| `channel.slack.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.slack.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.slack.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.slack.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.telegram.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.telegram.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.telegram.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.telegram.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.wechat.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wechat.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.wechat.get_login_ui` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wechat.get_login_ui", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"phase": "idle", "message": "", "qr": null}}
```

| `channel.wechat.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wechat.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.wechat.unbind` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wechat.unbind", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false, "bot_token": ""}}}
```

| `channel.wecom.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wecom.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.wecom.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.wecom.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.welink.assistant_credential_save` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.assistant_login_start` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.assistant_login_status` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.get_bindings` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.login_start` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.login_status` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.welink.revoke` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `channel.whatsapp.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.whatsapp.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.whatsapp.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.whatsapp.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `channel.xiaoyi.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.xiaoyi.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": false}}}
```

| `channel.xiaoyi.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "channel.xiaoyi.set_conf", "params": {"enabled": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"config": {"enabled": true}}}
```

| `chat.interrupt` | 网关处理（白名单+本地 handler） | cancel 分支重构信封 {intent:'cancel', session_id}+定位参数；agent_notify='await' |
| `chat.resume` | 网关处理（白名单+本地 handler） | 改写为 chat.interrupt + intent:'resume'（setdefault） |
| `chat.send` | 网关处理（白名单+本地 handler） | 归一化 content→query；强制流式；先回 ack + processing_status(true) |
| `chat.swarmflow_reply` | 网关处理（白名单+本地 handler） | 本地 handler 回 ack 后非流式转发原 params |
| `chat.user_answer` | 网关处理（白名单+本地 handler） | 本地 handler 回 ack 后转发原 params |
| `client.attach` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `client.conversation.messages` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `client.conversations` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `client.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `client.renew` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `command.btw` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `command.compact` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `command.goal` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `command.mcp` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `command.workflows` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `commands.list` | 纯本地 | 网关内执行，不触达 AgentServer |
| `config.get` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `config.moa_status` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `config.save_all` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `config.set` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `config.validate_model` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `connection.status` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "connection.status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"agent_ready": true, "protocol_version": "1.0"}}
```

| `cron.job.create` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.create", "params": {"name": "每日日报", "cron_expr": "0 9 * * *", "description": "总结今天的工作", "targets": "web", "mode": "agent"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<hex32>", "name": "每日日报", "enabled": true}}}
```

| `cron.job.delete` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.delete", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"deleted": true}}
```

| `cron.job.get` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.get", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "name": "每日日报", "enabled": true, "cron_expr": "0 9 * * *", "mode": "agent"}}}
```

| `cron.job.list` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.list", "params": {"project_id": "default"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"jobs": []}}
```

| `cron.job.meta` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.meta", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"modes": ["agent", "team"], "default_mode": "agent", "default_timeout_seconds": 3600, "default_team_timeout_seconds": 3600, "max_timeout_seconds": 259200}}
```

| `cron.job.preview` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.preview", "params": {"id": "<job_id>", "count": 5}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"next": [{"wake_at": "2026-11-11T09:00:00+08:00", "push_at": "2026-11-11T09:00:00+08:00"}]}}
```

| `cron.job.run_now` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.run_now", "params": {"id": "<job_id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"accepted": true, "run_id": "<job_id>:1760600100", "session_id": "sess_cron_01"}}
```

| `cron.job.toggle` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.toggle", "params": {"id": "<job_id>", "enabled": false}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "enabled": false}}}
```

| `cron.job.update` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "cron.job.update", "params": {"id": "<job_id>", "patch": {"enabled": false}}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"job": {"id": "<job_id>", "enabled": false}}}
```

| `designer.graph.get` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.graph.list` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.graph.patch` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.graph.save` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.media.capabilities` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.run.cancel` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.run.get` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.run.pause` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.run.start` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.workspace.create` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.workspace.get` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.workspace.session.compose` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `designer.workspace.session.create` | E2A 代理（本地 handler 构造请求） | E2A 代理（timeout_seconds 可配） |
| `document.formats` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `document.persist` | E2A 代理（本地 handler 构造请求） | >4MB base64 先经 E2A 分块预上传（path/_persisted 替代） |
| `extensions.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `extensions.import` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `extensions.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `extensions.toggle` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `external_cli.codex_install_status` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.codex_install_status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "codex", "status": "idle", "phase": "idle"}}
```

| `external_cli.custom_agent.remove` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.custom_agent.remove", "params": {"cli_agent": "my_agent"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "my_agent"}}
```

| `external_cli.detect` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.detect", "params": {"cli_agent": "claude"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "claude", "status": "ok", "path": "/usr/local/bin/claude", "version": "1.0.0"}}
```

| `external_cli.install` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.install", "params": {"cli_agent": "claude"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "claude", "status": "running", "phase": "preparing"}}
```

| `external_cli.install_status` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.install", "params": {"cli_agent": "claude"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"cli_agent": "claude", "status": "running", "phase": "preparing"}}
```

| `external_cli.prune_path_histories` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.prune_path_histories", "params": {"cli_agent": "claude", "remove_paths": ["/old/path"]}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"histories": ["/usr/local/bin/claude"]}}
```

| `external_cli.validate_paths` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "external_cli.validate_paths", "params": {"paths": ["/usr/local/bin/claude", "/usr/local/bin/codex"]}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"results": {"/usr/local/bin/claude": true, "/usr/local/bin/codex": false}}}
```

| `file.import_url` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |
| `file.upload_chunk` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |
| `group.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.execution.get` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `group.execution.list` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `group.file.delete` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `group.file.rename` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `group.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.history.clear` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `group.history.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.member.add` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `group.update` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `harness.activate` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `harness.deactivate` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `harness.delete` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `harness.export` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `harness.import` | E2A 代理（本地 handler 构造请求） | >4MB base64 先经 E2A 分块预上传（path/_persisted 替代） |
| `harness.packages` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `harness.packages.scan` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `health_check.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "health_check.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"every": 60, "target": "", "active_hours": null}}
```

| `health_check.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "health_check.set_conf", "params": {"every": 120, "target": "feishu"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"every": 120, "target": "feishu", "active_hours": null}}
```

| `heartbeat.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "heartbeat.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"every": 60, "target": "", "active_hours": null}}
```

| `heartbeat.job.cancel` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.create` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.delete` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.get` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.list` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.meta` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.preview` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.run_now` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.toggle` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.job.update` | E2A 代理（本地 handler 构造请求） | 经 HeartbeatControllerProxy 单通道转发 |
| `heartbeat.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "heartbeat.set_conf", "params": {"every": 120, "target": "feishu"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"every": 120, "target": "feishu", "active_hours": null}}
```

| `history.get` | 网关处理（白名单+本地 handler） | 强制流式；ack 参数回显；不抬升/清 busy |
| `hooks.list` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |
| `initialize` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `issue.delete` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `issue.matrix` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `issue.state.list` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `issue.watch_once` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `jiuwenmate.activity.get` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.agent_mail.auth.begin` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.agent_mail.auth.cancel` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.agent_mail.auth.poll` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.agent_mail.disconnect` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.agent_mail.status` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.channel.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.channel.messages` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.channel.trace` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.chat.history` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.chat.send` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.create` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.delete` | local_only 本地 | 网关本地执行不经转发；企业版 handler 内自检拦截（feishu.get_bindings 放行） |
| `jiuwenmate.colleague.ensure_default` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.get` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.publish` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.set_enabled` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.settings.get` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.colleague.settings.update` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.conversation.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.conversation.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.conversation.pin` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.conversation.rename` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.dingtalk_auth.begin` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.dingtalk_auth.cancel` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.dingtalk_auth.disconnect` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.dingtalk_auth.poll` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.dingtalk_auth.status` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.knowledge.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.models.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.permission.decide` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.request.abort` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.request.get` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.create` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.history` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.run` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.set_enabled` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.schedule.update` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.sharing.decide` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.sharing.list` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.sharing.revoke` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.welink_office.connect` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.welink_office.disconnect` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `jiuwenmate.welink_office.status` | **透传** | params 逐键原样（仅 F.2 通用归一化）（服务端在 releases/0.2.9.beta2 线） |
| `locale.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "locale.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"preferred_language": "zh"}}
```

| `locale.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "locale.set_conf", "params": {"preferred_language": "zh"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"preferred_language": "zh"}}
```

| `mcp.cancel_connect` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.connect` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.delete_custom` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.disconnect` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.oauth.start` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.register_custom` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.save_credentials` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.show` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp.wait_auth` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp_app.call_tool` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp_app.list_tools` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `mcp_app.read_resource` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `media.discard` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `media.persist` | E2A 代理（本地 handler 构造请求） | >4MB base64 先经 E2A 分块预上传（path/_persisted 替代） |
| `memory.compute` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "memory.compute", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"rss_mb": 256.0, "total_mb": 16384.0, "available_mb": 8192.0}}
```

| `memory.forbidden.get` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `memory.forbidden.set` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `model_groups.delete` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `model_groups.list` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `model_groups.upsert` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.delete` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.get` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.list` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.references` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.refresh_status` | 网关可达、**AgentServer 未实现** | 南向落通用 fall-through，契约未定义 |
| `models.replace_all` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.upsert` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `models.validate` | E2A 代理（本地 handler 构造请求） | A4g：个人版恒 E2A；登录会话 id 随请求注入 |
| `openai_account.auth.logout` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.auth.logout", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"logged_out": true}}
```

| `openai_account.auth.pending_login` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.auth.pending_login", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"status": "pending", "login_id": "<id>"}}
```

| `openai_account.auth.poll_login` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.auth.poll_login", "params": {"login_id": "<id>"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"status": "authenticated", "authenticated": true}}
```

| `openai_account.auth.start_login` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.auth.start_login", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"status": "pending", "login_id": "<id>", "user_code": "XXXX-XXXX", "verification_uri": "https://...", "interval": 5, "expires_in": 300}}
```

| `openai_account.auth.status` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.auth.status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"authenticated": false, "auth_path": "~/.openai/auth.json", "has_refresh_token": false, "needs_refresh": false}}
```

| `openai_account.models.list` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "openai_account.models.list", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"models": ["gpt-4o", "gpt-4o-mini"], "base_url": "https://api.openai.com"}}
```

| `org.snapshot` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `path.get` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.get", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"chrome_path": "/usr/bin/google-chrome", "headless": true, "decision_mode": "llm"}}
```

| `path.select_directory` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_directory", "params": {"initial_dir": "/Users/me/project"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"path": "/Users/me/project/src", "cancelled": false}}
```

| `path.select_file` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_file", "params": {"initial_dir": "/Users/me/project"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"path": "/Users/me/project/main.py", "cancelled": false}}
```

| `path.select_files` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.select_files", "params": {"initial_dir": "/Users/me/project", "allow_multiple": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"files": [{"path": "/Users/me/project/a.py", "kind": "document"}], "cancelled": false}}
```

| `path.set` | 纯本地 | legacy 本地 / AgentOS 走 E2A 分流 |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "path.set", "params": {"chrome_path": "/usr/bin/google-chrome", "headless": true}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"chrome_path": "/usr/bin/google-chrome", "headless": true}}
```

| `permissions.approval_overrides.delete` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.approval_overrides.get` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.owner_scopes.get` | E2A 代理（本地 handler 构造请求） | A4g/A5：统一 E2A |
| `permissions.owner_scopes.set` | E2A 代理（本地 handler 构造请求） | A4g/A5：统一 E2A |
| `permissions.rules.create` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.rules.delete` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.rules.get` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.rules.update` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.tools.delete` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.tools.get` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.tools.set` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.tools.update` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `permissions.tools.update_user` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `personal_context.context.get_node` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.context.get_source` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.context.search_pages` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.context.stream_graph` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.context.stream_tree` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.authorize_provider` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.create_service` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.delete_service` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.get_authorization_status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.get_run_status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.list_services` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.patch_service` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.run_all` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.run_one` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.start_service` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.stop_run` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.fetch.stop_service` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.get_config` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.patch_config` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.select_model` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.set_master_enabled` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.start_agent_use` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.start_collection` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.stop_agent_use` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `personal_context.runtime.stop_collection` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.import_local` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.show` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugin_packages.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.disable` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.enable` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.reload` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `plugins.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `proactive.feedback` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `project.create` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.get_cron_sessions` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.get_sessions` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.git.commit` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.create_branch` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.diff_status` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.init` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.probe` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.push` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.status` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.switch_branch` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.turn_diff` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.git.turn_diff_list` | E2A 代理（本地 handler 构造请求） | preserve_error_payload=True（失败 payload 整体保留） |
| `project.info` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.list` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.pin` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.pinned_sessions` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.remove` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.rename` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.restore` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.sessions.archive` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `project.sessions.delete_archived` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `prompt_suggestion.event` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `prompt_suggestion.get` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `rsi.artifact.download` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id；按 user_id 注入 _download_user_id |
| `rsi.artifact.files.get` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.artifact.files.list` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.dataset.validate` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.harness.install` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.harness.rollback` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.harness.versions.list` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.report.get` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.task.create` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.task.delete` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.task.get` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.task.list` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.training.pause` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.training.resume` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.training.start` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.training.terminate` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.tree.get` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `rsi.usage.get` | E2A 代理（本地 handler 构造请求） | 网关注入 session_id |
| `schedule.cancel` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.check_config` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.create` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.delete` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.list` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.logs` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.run` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.status` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `schedule.update_config` | **透传（白名单在册、未列 no-local 名单，无本地 handler）** | params 逐键原样；业务错误恒 ok:true（§10.6.17/§8.3） |
| `session.archive` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.archived.list` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.assets.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `session.assets.register` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `session.assets.remove` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `session.assets.rename` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `session.create` | E2A 代理（本地 handler 构造请求） | drop 客户端 session_id；注入 work_mode 归一化+_work_mode_explicit+create_token |
| `session.delete` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.fork` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `session.get_metadata` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.list` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.message.continue_queued` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.pin` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.plan_status` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.rename` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `session.selection.set` | E2A 代理（本地 handler 构造请求） | A4g/A5：统一 E2A |
| `session.switch` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `session.unarchive` | E2A 代理（本地 handler 构造请求） | 网关构造 E2A 请求，params 透传 |
| `skills.clawhub.download` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.clawhub.get_token` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.clawhub.search` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.clawhub.set_token` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.create_from_knowledge` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.enterprise.install` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.enterprise.list` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.enterprise.source.providers` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.enterprise.source.search` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.enterprise.uninstall` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.evolution.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.evolution.save` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.evolution.status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.experience.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.experience.request` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.files.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.files.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.graph.build` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.graph.cancel` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.graph.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.graph.status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.import_local` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.import_upload` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.installed` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.marketplace.add` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.marketplace.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.marketplace.remove` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.marketplace.toggle` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.online_search.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.online_search.search` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.pack_member.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.rebuild` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.retrieval.index_build` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.retrieval.index_cancel` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.retrieval.search` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.retrieval.status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.retrieval.tree` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.skillnet.evaluate` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.skillnet.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.skillnet.install_status` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.skillnet.search` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.source.install` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.source.providers` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.source.search` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.swarmskillshub.detail` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.swarmskillshub.recommend` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.info` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.init` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.install` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.pack` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.publish` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.search` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.teamskillshub.validate` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.toggle` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.uninstall` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `skills.update` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.updates.check` | **不可达** | 白名单在册但协议枚举缺失 → 帧级 METHOD_NOT_FOUND |
| `skills.versions.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `swarmflow.pause` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `swarmflow.resume` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `swarmflow.stop` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `task.asr.transcribe` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "task.asr.transcribe", "params": {"audio_base64": "<base64...>", "mime_type": "audio/webm"}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"text": "转写文本"}}
```

| `team.binding.create` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.binding.generate` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.bindings.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.create` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.delete` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.get` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.history.get` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.member.join` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.member.leave` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.members.list` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.mq.publish` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.quota.get` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.session.bind` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.snapshot` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `team.stop` | **企业版专用** | 个人版从白名单与注册表剔除 → METHOD_NOT_FOUND |
| `team.templates.list` | **透传** | params 逐键原样（仅 F.2 通用归一化） |
| `topic_starters.control` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.event` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.foreground` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.get` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.profile` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.restore_direction` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.select_model` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `topic_starters.status` | 网关薄包装转发 | RPC 名改写为 action 经 recommendation.job 转发；注入 auth_session |
| `updater.check` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.check", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"current_version": "0.1.0", "latest_version": "0.2.0", "state": "update_available", "has_update": true, "install_mode": "pip"}}
```

| `updater.download` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.download", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"state": "downloading", "downloaded_bytes": 0, "total_bytes": 10485760}}
```

| `updater.get_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.get_conf", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip", "repo_owner": "openJiuwen", "repo_name": "agent-gateway", "access_token": "****"}}
```

| `updater.get_status` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.get_status", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"current_version": "0.1.0", "latest_version": "", "state": "idle", "has_update": false, "platform": "darwin", "platform_supported": true}}
```

| `updater.reset_source` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.reset_source", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip"}}
```

| `updater.set_conf` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.set_conf", "params": {"enabled": true, "timeout_seconds": 30}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"enabled": true, "release_api_type": "gitcode", "install_mode": "pip", "timeout_seconds": 30}}
```

| `updater.upgrade` | 纯本地 | 网关内执行，不触达 AgentServer；企业版写保护（set_conf/unbind） |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "updater.upgrade", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"state": "restarting", "installing": true, "restart_command": "pip install --upgrade agent-gateway"}}
```

| `vendors.fetch_models` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "vendors.fetch_models", "params": {"vendor_key": "openai", "plan": "coding_plan", "api_key": "sk-..."}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"models": ["gpt-4o", "gpt-4o-mini"], "source": "remote"}}
```

| `vendors.list` | 纯本地 | 网关内执行，不触达 AgentServer |


**请求/返回帧**：

```jsonc
→ {"type": "req", "id": "req_x", "method": "vendors.list", "params": {}}
← {"type": "res", "id": "req_x", "ok": true, "payload": {"vendors": {"token_plan": [], "coding_plan": [], "custom_api": []}}}
```

| `workspace.entries.delete` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |
| `workspace.preview` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |
| `workspace.tree` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |
| `workspace.usage` | E2A 代理（本地 handler 构造请求） | E2A 代理；路径规范校验在服务端 |



---

## 返回导航

- [返回文档首页](../../README.md)
- [返回项目首页](../../../README.zh.md)

