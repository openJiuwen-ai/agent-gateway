# Gateway 接口文档（Web 前端 ⇄ Gateway）

> **目标**：完整描述 JiuwenSwarm Gateway 对 Web 前端暴露的全部接口——WebSocket JSON-RPC 方法、HTTP 路由与推送事件，覆盖参数校验、错误码、权限、流式契约（出错处理 / 取消 / 完整性）与内部输入输出关联。预期读者为前端开发、SDK 开发与网关维护者。
>
> **基准版本**：本仓 `upstream/develop` @ `0d50e02b0`（2026-11-09），行为已按该版本核对修正。**路径前缀映射**：`gateway/*` → `agent_gateway/*`、`common/*` → `agent_gateway/common/*`、`common/schema/message.py`（ReqMethod/EventType/Mode/Message）→ 协议包 `openjiuwen_protocol/`（钉定 rev `4c5f3e9eb`）；`server/*`、`agents/*`、`channels/web/frontend/*` 引用属 AgentServer / 前端仓，本仓不可核。**未随本次修订整体刷新的行号仍为旧基线 jiuwenswarm `3548d192d` 行号**，仅作定位线索，以代码为最终实现源。

---

## 目录与覆盖范围

**本文档覆盖**：Web 前端（浏览器 ⇄ `/ws`、`/ws/git` 与同端口 HTTP）可触达的主要接口——489 个 WS 方法（其中 7 个企业版专用）、13 组 HTTP 路由、50 个推送事件类型；含参数校验、错误码、权限与流式契约。

**明确不在范围内**（避免误判为遗漏）：

- TUI 专属方法（`session.rewind*`、`session.color_set`、`history.list_turns` 等，注册于 `tui/tui_connect.py`）与 ACP 通道方法（`acp.tool_response` 等）；
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
- 附录 A 方法分发总表 · 附录 B 事件总表 · 附录 C 错误码总表 · 附录 D 限制常量 · **附录 E 端到端样例（新手从这里入手）**

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
| 宽松容错 | 不报错，静默兜底/丢弃/钳制 | `params` 非对象→置 `{}`；列表非 dict 项静默跳过（`attachments`/`answers`）；`trusted_dirs` 非字符串项丢弃；`token_budget`/`max_attempts` 类型不合法（如 float）**静默丢弃**（`interface_deep.py:14690-14698`）；分页越界钳制；布尔强转参数（`enable_swarmflow`/`overwrite_confirmed`/`team`——传 `"yes"`/`1` 会被当真值）；cron `id` 经 `str()` 强转 |
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

恢复暂停中的轮次（不新建轮）。**网关归一化将其改写为 `chat.interrupt` + `intent:"resume"`**（`app_gateway.py:295-297`），行为同 §4.2 的 resume 分支。

**同步 res**：`{accepted:true, session_id}`（`app_web_handlers.py:3948-3954`）。

另一类"恢复"是**中断恢复型 chat.send**（`source ∈ {ask_user_interrupt, confirm_interrupt, permission_interrupt, evolution_interrupt}` + `request_id` + `answers[]`），走 chat.send 流式路径且不取消在途流（§4.1）。

### 4.4 chat.user_answer

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
| `token_budget` | int | 否（缺省 null=不设预算上限） | token 预算；容忍数字字符串、bool 拒绝（`interface_deep.py:14690-14698`）；≤0 → `invalid_objective` |
| `max_attempts` | int | 否（缺省 null=不设次数上限） | attempt 次数上限；解析同上 |

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
| `pause` | 暂停续跑，**不打断在跑 attempt** | 丢排队续跑 work、结算计时；在跑 attempt 的评估仍可提交 | `no_goal` / `invalid_state`（非 active） |
| `resume` | 恢复续跑 | 折叠计时、revision+1（无在跑时）、入队新 attempt；**不重置计数** | `no_goal` / `invalid_state`（completed）；已 active 幂等成功 |
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

均为白名单转发、非流式 res（除非显式 `is_stream`）：

| 方法 | 关键参数 | 响应要点 |
|------|---------|---------|
| `command.btw` | `session_id`（必填）、`question`（必填非空）、`mode`（缺省 `"agent"`） | 独立单轮 LLM 侧问答，不改历史。`{status:"ok", answer}`；空 question 回 `{status:"failed", error:"Question is required"}` |
| `command.compact` | `session_id`（必填）、`mode`（缺省 `"agent"`） | 手动上下文压缩。res `{result:"compressed"\|"noop", stats, summary?, compact_summary?}`；过程中推 `context.compression_state` 与 `context.usage` 事件（`agent_ws_server.py:7798-8003`） |
| `command.mcp` | `action`（缺省 `"list"`：`list`/`show`/`add`…）、`name`（仅 show 读取，缺省不携带） | MCP 服务器管理；list/show 响应敏感字段一律脱敏；add 先预检再落 config.yaml |
| `command.workflows` | `action`（`list`（默认）/`get_workflow`/`get_phase`/`get_agent`）、`workflow_id`、`session_id`、四组分页参数（run limit 默认 50 上限 200；phase 默认 20 上限 100；agent 默认 50 上限 200） | SwarmFlow run 快照；无 live 时从 checkpoint 恢复（遗留 `running` 归一为 `paused`+`recovered`）；超长字段切分为 `_{field}_parts`（`server/wire_truncate.py:12-18`） |

### 4.8 history.get

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

team 模式由 `chat.send` / `session.create` 的 `mode` 参数进入。`Mode` 枚举（协议包 `openjiuwen_protocol/message.py:689-708`）canonical 值：

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

均为白名单纯转发（`_FORWARD_NO_LOCAL_HANDLER_METHODS`），由 AgentServer 处理；错误码遵循 §3.2（AgentServer code 透传，缺省 BAD_REQUEST）。**企业版另有 team 控制面 7 方法**（`team.create` / `team.get` / `team.stop` / `team.member.join` / `team.member.leave` / `team.members.list` / `team.quota.get`，`app_web_handlers.py:699-707`）：仅企业版注册并转发；个人版从转发白名单剔除，WS 调用回 `METHOD_NOT_FOUND`，HTTP 路由同样按版本过滤（`http_routes/mapped.py:946-973`）。

#### team.templates.list

- **用途**：列出 `config.yaml` `modes.team.*` 配置的团队模板。
- **参数**：无。
- **响应**：`{templates: [{template_id, display_name, available:true, source, team_name}]}`（`common/team_template_config.py:83-128`）。

#### team.bindings.list

- **用途**：列出所有 Team 实体（binding store + entity store + 遗留 session 扫描合并），供前端 team 选择器。
- **参数**：无必填。
- **响应**：`{teams: [...]}`，按 `updated_at` 降序。每项：`team_name / template_id / created_at / updated_at / session_ids / last_session_id / legacy / model_selection` + 增强字段 `template_available / source_template_available / team_config_available / team_config_path / session_count / active_session_id / selectable / disabled_reason`（`"active"|"legacy"|"team_config_missing"|""`）（`agent_ws_server.py:5233-5305`）。

#### team.binding.create

- **参数**：`team_name`（string，必填；1-64 字符、禁路径分隔符/控制字符）、`template_id`（string，必填）。
- **响应**：`{team: <TeamBinding>}`。
- **错误**：模板不存在 `NOT_FOUND`；entity/binding 已存在 `CONFLICT`。

#### team.binding.generate

- **用途**：按自然语言描述生成 team 名并创建（默认第一个模板）。
- **参数**：`description`（string，必填；或别名 `prompt`）。
- **响应**：`{team, template}`。重名自动加 `_2.._100` 后缀重试，仍失败 `CONFLICT "unable to allocate a unique team_name"`。
- **错误**：`GENERATION_FAILED`（名称生成失败）、`NOT_FOUND`（无模板）、`CONFLICT`。

#### team.session.bind

- **用途**：把已存在 session 绑定到某 Team（会从其它 team 绑定中移出）。
- **参数**：`session_id`（string，必填）、`team_name`（string，必填）、`mode`（string，可选，默认 `"team"`，经 `resolve_agent_request_mode` 归一）。
- **响应**：`{session_id, team_name, team_template_id, mode, team, team_config_path}`。
- **错误**：缺 session/team `BAD_REQUEST`；session、binding、entity 配置缺失 `NOT_FOUND`；team 删除进行中拒绝。

#### team.delete

- **用途**：删除 Team 及其全部持久化 session。
- **参数**：`team_name`（string，必填）；且 params 须表明 team 语境（`team: bool` 为真或 `mode` 为 team 模式），否则 `UNSUPPORTED_MODE`。
- **响应**：`{team_name, session_ids, failed_session_ids, deleted, recovery_required}`。
- **错误**：`NOT_FOUND`（team 不存在）、`STOP_TIMEOUT`/`DELETE_FAILED`（运行时错误）。

#### team.snapshot（团队看板）

- **用途**：拉取成员列表 + 任务看板快照（刷新恢复、team 切换时）。
- **参数**：`session_id`（string，必填；缺省回退请求 envelope 的 session_id，`agent_ws_server.py:6495`）、`team_name`（string，可选，缺省取会话活跃 team；多 team 并发时才需显式指定）。
- **数据源三级回退**：live（session 级 monitor）→ live-scoped（指定 team 的只读 monitor）→ db（TeamDB）；响应含 `members_source: "live"|"live-scoped"|"db"|"empty"`。
- **响应**：`{members: [{member_id, name, status, execution_status, mode, role, cli_agent}], tasks: [{task_id, team_name, title, content, status, assignee, updated_at}], team_id, members_source}`。title/content 超 4096 字符截断并附 `*_truncated`/`*_original_size`（`team_monitor_handler.py:60,119-133`）。
- **恒 ok=true**（读失败降级空快照）。

#### team.history.get

- **用途**：team 会话历史分页（避免与 history.get 并发竞争）。
- **参数**：`session_id`（string，必填）、`member_name`（string，可选，仅该成员相关记录）、`cursor`/`offset`（int，默认 0）、`limit`（int，默认 500，1–1000）、`max_bytes`（int，默认 2MB，2048–6MB）。
- **响应**：`{records, session_id, cursor, next_cursor, has_more, total}`；records 消毒并按 max_bytes 分页。

#### team.mq.publish

- **用途**：向活跃 team runtime 注入外部事件（绕过 chat.send）。
- **参数**：`session_id`（string，取自请求 envelope，无需在 params 重复携带）、`payload`（dict，必填，且 `payload.type` **必须**为 `"team.external_event"`）。
- **响应**：成功 `{published:true}`；失败 `{error:<reason>}`。
- 该方法的连接**不注册 GodView**（短生命周期发布连接不是观察者）。

#### team.list

- **用途**：列出**一个 session** 当前归属的全部 team（team 选择器）。
- **参数**：`session_id`（string，必填；缺省回退请求 envelope 的 session_id，`agent_ws_server.py:6785`）。
- **响应**：`{session_id, teams: [{team_id, team_name, display_name, state(running|pending|configured), leader_id, organization_id, is_leader, capabilities, agent_group_name, source, is_owner}], default_team_id}`。恒 ok=true（异常降级空列表）。

### 5.3 group.* 方法（群聊组）

**语义区别**：`team.*` 面向 Team 实体/运行时（模板、绑定、看板）；`group.*` 面向**持久化群聊组**——group 的 `group_id` 本身是一个 team 模式的 session，成员是预编译的 teammate 规格（`server/runtime/group_service.py`）。**有 ownership 校验**：`group.json` 的 `user_id`+`channel_id` 必须与请求一致，否则 `NOT_FOUND`（隐藏存在性）。

| 方法 | 关键参数 | 响应 / 错误 |
|------|---------|------------|
| `group.create` | `name`（≤200）、`description`（≤2000）、`members`（≤100 项，每项仅允许 `{kind, definition_id, member_name, display_name}`）、`create_token`（幂等键，默认 request_id）、`project_id/project_dir/model_name` | `{group_id, session_id, team_name, name, description, members, created_at, updated_at, status}`；同 token 内容不一致 `CONFLICT` |
| `group.list` / `group.get` | `limit`（默认 50，1–200）/`offset` | `{groups, total, limit, offset}`，按 updated_at 降序 |
| `group.update` | `name`/`description`（**仅接受这两个字段**，其他字段 `BAD_REQUEST`） | 更新后的 group |
| `group.member.add` | `member`（dict，必填）、`client_request_id`（string，≤256，缺省不携带——无幂等收据） | `{group_id, member, status:"registered", event_type:"group.member.added"}`；错误：`GROUP_FULL`/`ALREADY_EXISTS`/`CONFLICT`/`GROUP_RESTART_REQUIRED`/`MEMBER_REGISTRATION_FAILED` |
| `group.history.get` | `limit`/`offset` | `{group_id, messages, total, limit, offset, next_offset}`；数据损坏 `GROUP_CORRUPT` |
| `group.delete` | `group_id` | `{group_id, deleted:true}`；删除中再操作 `GROUP_BUSY`；停 team 失败 `DELETE_FAILED` |

**group 内 chat.send 约束**：`group_id` 必须等于 `session_id`——该校验已上移到**网关帧级**（`web_connect.py:2247-2252`，含 `validate_session_id(group_id)`，否则 `BAD_REQUEST`）；`params.mode` 必须为 team 模式（否则 `MODE_MISMATCH "group chat requires team mode"`）；`params.type` 必须为 `"group_chat"`（后两者在 AgentServer 侧，`server/runtime/group_requests.py:88-108`）。另新增转发方法：`group.execution.list/get`、`group.file.rename/delete`、`group.history.clear`。

### 5.4 org.snapshot

- **用途**：右侧信息框——组织（organization）+ 任务池状态。组织由 session 绑定的 Team backend 唯一决定。
- **参数**：`session_id`（string，缺省回退请求 envelope 的 session_id）、`team_id`（string，缺省空串——按会话解析 team backend）、`organization_id`（string，缺省空串，仅一致性校验）。
- **响应**：`{organization: {organization_id, display_name, description, owner_team_id, owner_leader_id, leaders, metadata, unclaimed_task_policy}, tasks(≤200), unclaimed_tasks, pending_reviews(≤50), stats: {total, open, unclaimed, pending_reviews, by_status}}`。
- backend 缺失或 organization_id 不匹配 → 空 payload `{organization: null, tasks: [], ...}`；恒 ok=true（`agent_ws_server.py:6817-6971`）。

### 5.5 swarmflow.pause / resume / stop

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

- **参数**：`limit`（int，默认 20，1–200 钳制，AgentServer 侧执行）、`offset`（int，默认 0，≥0）。不支持 project 过滤（项目内会话用 `project.get_sessions`）。
- **响应**：`{sessions: [SessionInfo], total, limit, offset}`。SessionInfo 字段（`server/runtime/session/session_info.py:19-52`）：`session_id, title, created_at, last_message_at, message_count, mode, team_name, agent_group_name, pinned, pin_order, project_dir, project_id, cron_id, last_user_message_at, model, model_selection, moa_enabled, work_mode, lifecycle_operation, execution_blocked, stop_pending`。
- 归档/临时探针/ephemeral 会话不返回；按 `last_message_at` 倒序。网关为纯 E2A 薄代理（`app_web_handlers.py:3093-3113`）——**旧基线的"AgentServer 离线读本地目录 fallback"已删除**，不可达时回 `SERVICE_UNAVAILABLE`；limit/offset 钳制随 fallback 移到 AgentServer 侧。
- **错误**：元数据读失败 `INTERNAL_ERROR`；代理层 `SERVICE_UNAVAILABLE`/`AGENT_SERVER_TIMEOUT`。

#### session.create

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

- **参数**：`session_id`（string，必填，缺省回退帧级）。
- **响应**：metadata.json 全量字段（`session_id, channel_id, user_id, created_at, last_message_at, title, message_count, mode, team_name, team_template_id, project_dir, project_id, persist_session, model, moa_enabled, cron_id, pinned, pin_order, status, work_mode` 等）+ 网关注入的 `is_processing`（busy 映射）+ AgentServer 注入的 `queued_session_messages` / `can_continue_queued_session_messages`（跨会话信箱，§4.10）。
- **错误**：`BAD_REQUEST`（无 session_id）、`NOT_FOUND`、归档会话 `SESSION_ARCHIVED`。

#### session.rename

- **参数**：`session_id`（string，必填；缺省回退帧级 session_id）、`title`（string \| null，缺省不携带）。**三语义**：`title` 不传 → 查询；空串/纯空白 → 清除；非空 → 设置（strip 后截断 200 字符）。
- **响应**：`{session_id, title, previous_title}`。会话不存在时**不报错**（自动补建 metadata）；归档会话 `SESSION_ARCHIVED`。

#### session.pin

- **参数**：`session_id`（string，必填）、`pinned`（bool，必填，严格布尔）。幂等；操作后全部置顶会话紧凑重编号 1..N。
- **响应**：`{pinned, pin_order}`（非置顶恒 0）。
- **错误**：`BAD_REQUEST`、`NOT_FOUND`。

#### session.plan_status

- **参数**：`session_id`（string，必填）。
- **响应**：`{session_id, in_plan: bool}`（只读；团队会话看 metadata.mode + 进程内状态，单 agent 读 live 实例）。

#### session.message.continue_queued

- **用途**：用户确认后恢复投递到该会话的排队跨会话消息（信箱）。
- **参数**：`session_id`（string，必填）。
- **响应**：`{target_session_id, released_unknown_count, accepted: true}`。
- **错误**：`INVALID_ARGUMENT` / `HOST_CAPABILITY_UNAVAILABLE` / `NOT_FOUND` 等（SessionMessagingError code）。

### 6.2 白名单转发方法

#### session.switch

- **用途**：切换/恢复会话（前台目标切换 + Runtime 恢复）。
- **参数**：`session_id`（string，必填）、`previous_session_id`（string，缺省不携带——不上报前一前台会话信号，不影响切换本身）、`mode`（string，默认 `"agent.plan"`）、`previous_mode`（string，缺省不携带——同前，不上报）、`team`（bool，缺省 false——不作为团队提示）、`view_id`（string，缺省不携带——不上报前台视图 scope）。
- **响应**：`{session_id, mode, switched: bool, moa_enabled}`（先 commit 再回响应）。
- **错误**：`BAD_REQUEST`；目标归档 `SESSION_ARCHIVED` 等 SessionProvisionError code。

#### session.fork

- **用途**：**复制（分叉）一个已持久化会话**——新 session_id，可选在某 fork_point 截断；支持装备覆盖。
- **参数**：`source_session_id`（string，必填）、`target_session_id`（string，缺省由服务端生成新 id，已存在 → `ALREADY_EXISTS`）、`title`（string，**缺省继承源会话标题**，`session_ops_service.py:391-394`）、`fork_point`（dict，`{message_id|role|content|timestamp}` 可选，**缺省不截断**——全量复制历史）、`session_equipment_override`（dict，缺省沿用源会话装备）；`side_conversation` 已废弃，出现即 `BAD_REQUEST`。
- **响应**：`{session_id(新), source_session_id, title, moa_enabled}`。
- **错误**：`BAD_REQUEST` / `NOT_FOUND` / `ALREADY_EXISTS`。

#### session.assets.list / register / rename

- **用途**：会话级命名素材表（`@name` 引用），落盘 `<sessions>/<session_id>/session_assets.json`。
- **list**：`{assets: [{asset_id, name, kind(image|video|audio|document), path, source(upload|generated), created_at}]}`。
- **register**：`items`（必填，每项 `{path, name?, source?}`）；按 path 去重、文件必须存在、名称自动清洗（`@`/换行/tab）与去重（`" 2"`/`" 3"` 后缀，≤60 字符）。
- **rename**：`asset_id` + `name`（非空/≤60/无非法字符）。
- **错误**：`INVALID_PARAMS` / `INVALID_NAME` / `ASSET_NOT_FOUND` / `NAME_CONFLICT`。

### 6.3 session.selection.set（模型选择持久化）

- **参数**：`session_id`（string，缺省回退帧级）、`model_selection`（dict，必填）。
- **ModelSelection schema**（`common/model_selection.py:10-38`，pydantic `extra="forbid"`）：

| 字段 | 类型 | 必填 | 校验 |
|------|------|------|------|
| `type` | `"model"` \| `"model_group"` | 是 | — |
| `id` | string | 是 | strip 后非空 |
| `route_id` | string \| null | 否 | 仅 `model_group` 时允许；指定组内路由；`type:"model"` 时携带报错；缺省 null（解析模型组默认路由） |

- **响应**：`{model_selection: {...}}`；校验/解析失败 `error + code`（缺省 `BAD_REQUEST`）。统一经 E2A 转发目标 AgentServer（A4g 起个人版不再回落网关本地 handler）。

### 6.4 生命周期方法与事件（lifecycle）

Gateway 注册（`lifecycle_handlers.py:164-172`）：`session.archive` / `session.unarchive` / `session.archived.list` / `session.delete` / `project.sessions.archive` / `project.sessions.delete_archived`。

| 方法 | 参数 | 响应 |
|------|------|------|
| `session.archive` / `session.unarchive` | `session_id` 或 `session_ids`（数组 1–100） | `{session_id, ok:true, archived:true, archived_at, project_id}` / `{session_id, ok:true, restored:true, project_id}`；批量为 `{succeeded_count, failed_count, results[]}` |
| `session.delete` | `session_id`（单条）或 `session_ids` | `{session_id, ok:true, project_id}`；停止运行 → fence 写入 → 物理删除目录 |
| `session.archived.list` | `limit`(1–200)/`offset`/`work_mode`/`keyword`/`project_id` | 归档会话分页 |
| `project.sessions.archive` / `project.sessions.delete_archived` | `project_id` | `{project_id, succeeded_count, failed_count, results[]}` |

- 归档目录为 `agent/sessions_archived/`；unarchive 时所属项目被移除则连带恢复项目。
- **错误**：cron/heartbeat 会话禁止单独归档（`FORBIDDEN`）；忙碌 `SESSION_BUSY`（附 finishing 详情）；归档幂等。
- **事件**（Gateway 合成并广播）：`session.archived` / `session.unarchived` / `session.deleted` / `session.lifecycle.updated` / `project.lifecycle.updated` / `project.removed` / `project.restored`。跨 tab 感知靠 Gateway 的 2s 生命周期轮询 watcher（`lifecycle_handlers.py:27-84`）。
- **guard 准入闸门**（AgentServer 入站）：除白名单（`session.list`、`project.list/info/get_sessions/get_cron_sessions/pinned_sessions`、`chat.cancel`、`session.stop` 等）外所有请求先过 guard——归档会话 `SESSION_ARCHIVED`、已删除 `NOT_FOUND`、生命周期操作进行中 `OPERATION_IN_PROGRESS`（`server/lifecycle.py:389-419`）。

### 6.5 initialize

前端连接后可选的首个握手请求（无本地 handler，纯转发）。

- **参数**：`protocolVersion`（string，默认 `"0.1.0"`）、`clientCapabilities`（dict，默认 `{}`）。
- **响应**：`{protocolVersion: 1, agentInfo: {name:"jiuwenswarm", title:"JiuwenSwarm", version}, agentCapabilities: {loadSession:false, promptCapabilities, sessionCapabilities, mcpCapabilities}, authMethods: []}`（`common/e2a/acp/protocol.py:8-32`）。

---

## 7. Project 项目接口

### 7.1 project.* 管理方法

项目记录（projects.json）：`project_id`(32hex)、`name`、`project_dir`、`pinned`、`pin_order`、`hidden`、`work_mode`、`git`。除 `project.remove` 为 Gateway 编排外均 E2A 直转；`project.list/info/get_sessions/get_cron_sessions/pinned_sessions` 免 lifecycle guard。

| 方法 | 关键参数 | 响应 / 说明 |
|------|---------|------------|
| `project.list` | `filter`（`all`（默认）/`pinned`/`unpinned`）、`work_mode`（缺省不过滤；非法 `BAD_REQUEST`）、`include_hidden`（默认 false） | `{projects: [ProjectInfo]}`；默认项目按 work_mode 拆为虚拟项 `default`/`default_code`；置顶区在前，非置顶按最近消息倒序。ProjectInfo 含 `session_count`（仅计非置顶普通 web 会话）、`git{...}`、`execution_blocked`、`stop_pending` 等 |
| `project.info` | `project_id`（必填）、`include_hidden` | `{project: ProjectInfo, **ProjectInfo}`（双写）；不存在/隐藏 `NOT_FOUND` |
| `project.get_sessions` | `project_id`（必填）、`work_mode`（缺省不过滤）、`limit`（缺省返回全部）、`offset`（默认 0） | `{sessions, total}`；排除置顶与 cron 会话，按最近用户消息倒序 |
| `project.get_cron_sessions` | `project_id`、`cron_id`（可选过滤）、`limit`/`offset` | `{sessions, total}`；返回 cron 执行会话（带 `cron_id` 或 `channel_id=="__cron__"`） |
| `project.pinned_sessions` | 无参数 | `{sessions}`；全部置顶会话（跨项目），按 pin_order 升序 |
| `project.create` | `name`（必填，校验：非空、无控制字符、无 `<>:"/\|?*`、非 Windows 保留名、非全点/空格、≤200 字符）、`project_dir`（可选，须绝对路径；目录不存在 `PROJECT_DIR_MISSING`）、`work_mode` | `{project_id, project_dir, restored, work_mode, git, project}`；**冲突语义**：目录命中隐藏项目 → 恢复（`restored:true`）；命中可见项目 `CONFLICT`；同名冲突 `CONFLICT`。默认目录 `~/.jiuwenswarm/agent/workspace/{work\|code}/<name>` |
| `project.rename` | `project_id`、`name` | `{project_id, name, work_mode}`；默认项目 `FORBIDDEN` |
| `project.pin` | `project_id`、`pinned`（严格布尔） | `{pinned, pin_order}`；默认项目 `FORBIDDEN`；置顶区紧凑重编号 |
| `project.remove` | `project_id`（默认项目 `FORBIDDEN`） | **软删除**（Gateway 编排）：**立即受理，不再预检运行中会话**（AgentServer 隐藏项目并后台停会话，`app_web_handlers.py:3479-3501`）→ 停该项目 cron（失败 `CRON_STOP_FAILED`）→ `{project_id, hidden:true, affected_sessions, stopped_cron_jobs}`；幂等；广播 `project.removed` 事件（按 user_id 收窄） |
| `project.restore` | `project_id` | `{project_id, restored:true, work_mode, affected_sessions}`；未隐藏 `CONFLICT`；同名被占 `PROJECT_NAME_CONFLICT`；广播 `project.restored` |

### 7.2 project.git.*（主 WS 上的 Git 操作）

均为 E2A 直转 + `preserve_error_payload=True`（失败 payload 含结构化 `detail`）；写操作成功后唤醒 `/ws/git` watcher（`mark_dirty`）。**前置校验**（所有方法）：`project_id` 必填；默认项目 `FORBIDDEN`；work 模式项目 `FORBIDDEN`；不存在 `NOT_FOUND`/`PROJECT_NOT_FOUND`（`server/runtime/session/project_git.py:187-216`）。

| 方法 | 关键参数 | 响应 |
|------|---------|------|
| `project.git.status` / `probe` | `project_id` | GitStatus 快照：`{project_id, project_name, project_dir, work_mode, repo{is_git, repo_root, branch, head, detached, transient, upstream}, working_tree{is_dirty, staged, unstaged, untracked, conflicted}, branches{current, locals, remotes}, generated_at}` |
| `project.git.init` | `project_id`、`initial_branch`（默认 `"main"`） | 同 status |
| `project.git.switch_branch` | `branch`（必填）、`require_clean`（默认 false） | `{switched:true, previous_branch, current_branch, status}` |
| `project.git.create_branch` | `branch`（必填）、`checkout`（默认 true）、`start_point`（可选） | `{created:true, checked_out, branch, status}` |
| `project.git.commit` | `message`（必填非空）、`paths`（与 stage_all 互斥）、`stage_all`（严格布尔）、`amend`、`no_verify` | `{committed:true, commit_hash, amended, status}` |
| `project.git.push` | `remote`（默认 `"origin"`）、`branch`（detached HEAD 必传）、`set_upstream`/`force`/`delete`（严格布尔，delete 与前两者互斥） | `{pushed:true, remote, branch, deleted, upstream_set, status}`；force 用 `--force-with-lease`；push 超时独立配置（默认 60s） |
| `project.git.diff_status` | `session_id`、`include_files`、`include_hunks`（隐含 files）、`hunk_paths` | diff 状态快照；transient 仓库时 `current:null, repo.transient:true` 仍成功 |
| `project.git.turn_diff_list` | `session_id`（必填）、`limit`（默认 50）、`cursor` | 历史 diff 摘要列表；会话须属于该项目（`SESSION_NOT_FOUND`/`SESSION_NOT_BOUND`/`PROJECT_SESSION_MISMATCH`） |
| `project.git.turn_diff` | `session_id`（必填）、`change_set_id` 或 `turn_index`（二选一）、`include_files`/`include_hunks` | 指定轮次 diff 详情；未命中 `CHANGE_SET_NOT_FOUND`/`TURN_DIFF_NOT_FOUND`；历史过期 `DIFF_HISTORY_EXPIRED` |

### 7.3 /ws/git 独立协议

`ws://<host>:<port>/ws/git`，与主 WS 同握手鉴权链路，但 **method 集合封闭为 6 个**，未知方法回 `BAD_REQUEST`（而非 METHOD_NOT_FOUND）。事件推送 `seq`/`stream_id` 恒 null。

| method | params | 首次响应 / 后续事件 |
|--------|--------|--------------------|
| `project.git.diff_watch` | `project_id`、`session_id`、`scope`（仅 `"summary"`）、`include_last_turn`（默认 true） | 快照 `{watch_id, scope:"summary", snapshot:{repo, current, last_turn, revision}}`；变化推 `project.git.diff_changed` |
| `project.git.diff_files_watch` | `watch_id`（必填）、`source`（`current`\|`last_turn`） | `{watch_id, files_scope, revision, files}`（无 hunk）；推 `project.git.diff_files_changed` |
| `project.git.diff_detail_watch` | `watch_id`、`source`、`files`（非空字符串数组） | `{watch_id, detail_scope, revision, files}`（含 hunk，未订阅路径为 null）；推 `project.git.diff_detail_changed` |
| `project.git.diff_unwatch` | `watch_id`、`scope`（`all`\|`files`\|`detail`） | `{watch_id, cancelled:true, scope}`（幂等） |
| `project.git.discard_turn_changes` | `project_id`、`session_id` | 撤销本轮代码修改。**前置 busy 检查**：会话忙碌 → `SESSION_BUSY "session is busy; stop the current run before discarding/redoing changes"`。成功 `{session_id, turn_index, change_set_id, restored_files[], deleted_files[], errors[], partial}`；错误码 `NO_TURN_TO_DISCARD`/`NOTHING_TO_DISCARD`/`TURN_TARGET_CHANGED`/`DIFF_HISTORY_EXPIRED`/`PARTIAL_RESTORE_FAILED` |
| `project.git.redo_turn_changes` | 同上 | 重做；错误码 `NO_TURN_TO_REDO`/`NOTHING_TO_REDO`/`REDO_HISTORY_MISSING`/`PARTIAL_REDO_FAILED` |

监控过程 Git 命令失败推 `project.git.error` 事件；首次快照失败保留 AgentServer 结构化 code（如 `NOT_GIT_REPOSITORY`）。

---

## 8. 定时任务接口（cron / heartbeat / schedule）

三套体系**相互独立**：`cron.job.*`（通用定时任务，Gateway 本地实现+进程内调度）、`heartbeat.job.*`（心跳续跑任务，AgentServer Rail 运行时持有、Gateway 代理）、`schedule.*`（AutoHarness 演化管线任务，转发 AgentServer）。

### 8.1 cron.job.*（Gateway 本地，注册于 `app_web_handlers.py:5374-5382`）

通用：`cron_controller` 不可用 → `INTERNAL_ERROR "cron not available"`；单 job 操作先过 `_get_owned_cron_job` 鉴权（§8.1.10）。

#### 8.1.1 cron.job.list

- **参数**：`project_id`（string，缺省不过滤——返回当前用户全部可见任务；支持虚拟项目 `default`/`default_code`——精确匹配，或空 project_id 的 job 按 `work_mode` 消歧，老 job 在 `default` 下兜底可见）。
- **响应**：`{jobs: [CronJob]}`。**按连接 user_id 过滤**：带 user_id 的连接只见 `job.user_id` 精确相等的任务；user_id 为空（单用户）返回全部。

#### 8.1.2 cron.job.meta

- **参数**：无。**响应**：`{modes: [...CRON_JOB_MODES 排序], default_mode: "agent", default_timeout_seconds: 3600, default_team_timeout_seconds: 3600, max_timeout_seconds: 259200(72h)}`（`runtime/cron/models.py:175-188`）。

#### 8.1.3 cron.job.get

- **参数**：`id`（string，必填）。**响应**：`{job: CronJob}`。**错误**：`NOT_FOUND "job not found"`（不存在或**不属于当前用户**——不泄露存在性）。

#### 8.1.4 cron.job.create

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
- CronJob 响应体（`to_dict`，`runtime/cron/models.py:433-439`；数据结构已 protocol 化到 `openjiuwen_protocol/cron_models.py`）：恒有 `id, name, enabled, expired, cron_expr, timezone, wake_offset_seconds, description, targets, created_at, updated_at, project_id, work_mode, user_id, group_id, bot_id`（group/bot 为企业多租户身份，服务端注入）；条件输出 `session_id, chat_type, mode, delete_after_run, timeout_seconds, last_session_id, model_name, model_selection, mcp, app_id, credential_ref, next_run_at`。

#### 8.1.5 cron.job.update

- **参数**：`id`（string，必填）+ `patch`（dict，必填）。可更新：name/cron_expr/timezone/description/enabled/wake_offset_seconds/targets/mode/model_name/model_selection/mcp/session_id/project 字段。cron 变更时重新试算；model 变更时重算凭据绑定。
- **响应**：`{job}`。**错误**：`NOT_FOUND`/`BAD_REQUEST`/`SERVICE_UNAVAILABLE`。

#### 8.1.6 cron.job.delete

- **参数**：`id`（string，必填）。**行为**：停用 → 取消在途执行（并向 AgentServer 发 CHAT_CANCEL）→ 经 E2A 删除该 job 的执行会话（失败抛带 code 的 RuntimeError，如 `SESSION_BUSY`）→ 删 store。`proactive.tick` 内置任务拒绝面板删除（`BAD_REQUEST`）。
- **响应**：`{deleted: true}`。

#### 8.1.7 cron.job.toggle

- **参数**：`id`（string，必填）、`enabled`（bool，必填，None 拒绝）。enable 时复查项目闸门。`proactive.tick` 拒绝。
- **响应**：`{job}`。

#### 8.1.8 cron.job.preview

- **参数**：`id`（string，必填）、`count`（int，默认 5，夹取 1–50）。
- **响应**：`{next: [{wake_at, push_at}, ...]}`（ISO 时间，以 job 时区计算；一次性任务无下次触发时提前截断）。

#### 8.1.9 cron.job.run_now

- **参数**：`id`（string，必填）。**行为**：**拒绝停用与过期任务**（`enabled=false` → "任务已暂停，无法执行"、`expired` → "任务已过期，无法执行"，`BAD_REQUEST`；`proactive.tick` 例外，`cron/scheduler.py:1519-1533`）；立即预分配**真实执行会话**（复用 wake 阶段，非 proactive 模式响应即带真实 session_id）；`run_id = "<job_id>:<unix秒>"`。企业版手动触发遇审批提问先挂起（复用 pending 会话）。
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

| 方法 | 参数 | 响应 / 特殊行为 |
|------|------|----------------|
| `heartbeat.job.list` | `status?`（string）、`scope?`（string，仅 `current`/`all_visible`） | `{jobs}`。**Web 代理固定 `allow_all_visible=False`，`scope=all_visible` 必然 `FORBIDDEN "heartbeat.jobs.all permission required"`**；默认只返回当前会话的任务 |
| `heartbeat.job.meta` | 无 | `{limits(8 项资源限制), schedule_types, concurrency_policies, session_deleted_policies, statuses, sources, run_count_semantics, deprecated_fields}` |
| `heartbeat.job.get` | `id`（string，必填） | `{job}`；session 不匹配 `FORBIDDEN "job belongs to another session"`；不存在 `NOT_FOUND` |
| `heartbeat.job.create` | job 字段（dict，不允许未知字段，结构见上方字段表） | `{job}`；资源限制：每 session 活跃 ≤5、全局 ≤100、interval ≥60s |
| `heartbeat.job.update` | `id`（string，必填）+ `patch`（dict，必填，仅 name/prompt/schedule/timezone/enabled/concurrency_policy/session_deleted_policy/max_runs） | `{job}`；enable 时 run_count≥max_runs 拒绝；调度变更无未来触发点 `BAD_REQUEST`；冲突 `CONFLICT` |
| `heartbeat.job.delete` | `id`（string，必填） | `{deleted: bool}`；活跃 run 先 cancel，cancel 失败 `CONFLICT` |
| `heartbeat.job.toggle` | `id`（string，必填）+ `enabled`（bool，必填，严格布尔） | `{job}`（内部即 update enabled） |
| `heartbeat.job.preview` | `id`（string，必填）+ `count`（int，默认 5） | `{next: [{run_at(unix秒), iso}, ...]}` |
| `heartbeat.job.run_now` | `id`（string，必填）+ `reschedule`（bool，默认 false） | **可被拒绝**：`{accepted: bool, run_id, session_id, reason?}`；accepted=false 的 reason：`session_deleting/job_completed/session_missing/session_busy/previous_run_active/already_queued/replacement_pending/job_disabled_during_replace` 等；终态任务不能被 run_now 绕过 |
| `heartbeat.job.cancel` | `id`（string，必填）+ `pause_schedule`（bool，默认 false） | **只中断当前这一次运行**（区别于 delete）；`pause_schedule=true` 时同时停用后续调度。`{job_id, cancelled_run_id, cancel_status: cancelled\|not_found\|failed\|idle, paused, reason?}` |

执行语义：`prompt` 在**原 session 现场**以 background 流继续上下文执行（默认超时 300s）；执行结果以普通 chat 事件回流原会话。权限双因子：`(session_id, user_id)`——调用者 user_id 须与会话属主一致，且 job 须属于当前 session。

### 8.3 schedule.*（AutoHarness 演化管线，转发 AgentServer）

首个请求惰性初始化 AutoHarnessService。与 cron.job.* 无共享存储或调度器。**响应无 code 字段**（失败仅 `{error}`）。

| 方法 | 关键参数 | 响应 |
|------|---------|------|
| `schedule.check_config` | 无 | `{valid, missing_fields, config_path}` |
| `schedule.update_config` | `fields: {"git.user_name": ...}` | `{success, updated_fields}` |
| `schedule.create` | `query`、`interval_hours`（默认 4）、`run_immediately`（默认 false）、`model_name`（缺省用服务端默认模型）、`pipeline`（缺省用默认管线） | `{task_id: "sch_<hex8>", next_run_time, message}` |
| `schedule.run` | `query`、`model_name?`、`pipeline?` | `{task_id, status:"running", message}`（一次性任务） |
| `schedule.list` / `status` | 无 / `task_id` | `{tasks}` / 任务 dict（不存在 `{error:"任务不存在"}`） |
| `schedule.logs` | `task_id`、`log_type`（current/history）、`history_index`、`offset`、`limit`（默认 500） | 日志 dict |
| `schedule.cancel` / `delete` | `task_id` | `{task_id, status:"cancelled"}` / `{task_id}` |

另有 `issue.watch_once` / `issue.state.list` / `issue.matrix` / `issue.delete` 走同一 handler。

### 8.4 health_check.get_conf / set_conf（探活配置）

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

| 方法 | 说明 |
|------|------|
| `config.get` / `config.set` / `config.save_all` / `config.validate_model` / `config.moa_status` | 配置面板读写（A4g：个人版统一经 E2A 在目标 AgentServer 执行，不回落网关本地；企业版仅 RuntimeRouted/无 client/legacy 共享目录走本地 handler）；`config.set` 变更环境变量后可能安排进程重启（响应说明是否热更新）；moa_status 经 E2A 在目标 AgentServer 检查，不可用回 `SERVICE_UNAVAILABLE`。新增本地/代理方法：`models.refresh_status`、`connection.status`、`workspace.tree/preview/usage/entries.delete` |
| `models.list/get/replace_all/validate/upsert/delete/references`、`model_groups.list/upsert/delete` | 模型与模型组管理（配置代理，经 `_ConfigReq` 转发） |
| `session.selection.set` | 见 §6.3 |
| `vendors.list` / `vendors.fetch_models` | 模型厂商列表 / 拉取厂商模型目录 |
| `openai_account.auth.*`（status/start_login/pending_login/poll_login/logout）、`openai_account.models.list` | OpenAI 账号登录态管理 |
| `locale.get_conf` / `locale.set_conf` | 语言配置 |
| `channel.get` | 渠道配置概览 |

### 10.2 媒体与附件

#### media.persist（媒体落盘）

- **请求参数**：`media_items`（list；仅处理 `type=="image"` 的项，**最多前 8 项**，单项 base64 解码后 **≤10MB**——超限项静默丢弃不报错）。项字段：

| 字段 | 说明 |
|------|------|
| `type` | 须为 `"image"`，其它值跳过 |
| `mimeType` / `mime_type` | 须在支持表内（常见 image/png、image/jpeg 等），否则丢弃 |
| `base64Data` / `base64_data` | base64 正文，可带 `data:...;base64,` 前缀（自动剥离） |
| `_persisted` + `path` | 大图专用：HTTP bridge 已落盘的记录直接透传（网关不重复解码），path 不存在则丢弃 |

- **处理**：图片落盘到 `<sessions>/<session_id>/uploads/`；**超过 E2A 4MB 限制的**在网关侧解码后经 E2A 分块上传（§2.4），避免压内部 WS 帧。
- **响应 payload**：回传 normalize 后的 `{content?, query?, media_items?, files?}`——`media_items` 每项变为 `{type:"image", filename, mime_type, path, size_bytes}`，并同步生成 `files.uploaded_images: [{filename, path, mime_type, size_bytes}]`（注意此时 `files` 为 **dict** 而非 chat.send 的 list）。失败 `INTERNAL_ERROR`。
- **归属**：media.persist 现为网关薄 E2A 代理（超 4MB 预算的 base64 先经 E2A 分块上传再转发，不再是独立 HTTP bridge）；项级归一化（仅 image、前 8 项、单项 ≤10MB、mime 表）在 AgentServer 侧落盘时执行，同规则在 chat.send 路径保留于 `channel_manager/web/media/normalizer.py`。

#### media.discard（丢弃未发送副本）

- **请求参数**：`path`（string 必填，缺失 `BAD_REQUEST "path is required"`）。只删**当前会话 uploads 目录内**的普通文件。
- **响应 payload**：删除结果（`{ok/deleted}` 形态以 `discard_session_upload` 实现为准）；路径非法 `BAD_REQUEST`、IO 异常 `INTERNAL_ERROR`。

#### document.persist / document.formats

- `document.persist`：文档附件落盘，请求/响应与 media.persist 同构（`persist_and_parse_documents` normalize 后回传 `content/query/files` 等关心的键）；受上传格式黑名单约束。大文档同样走 HTTP bridge 分块。
- `document.formats`：返回文档上传黑名单格式列表。

#### task.asr.transcribe（语音转写）

- **请求参数**：`audio_base64`（必填）、`mime_type`（默认 `audio/webm`；枚举 flac/m4a/mp4/mpeg/mp3/ogg/wav/wave/webm/x-m4a）；解码后 **≤25MB**。
- **响应 payload**：`{"text": "<转写文本>"}`。
- **错误码族**：`ASR_NOT_CONFIGURED` / `ASR_BAD_CONFIG` / `ASR_UNSUPPORTED_FORMAT` / `ASR_AUDIO_TOO_LARGE` / `ASR_AUDIO_DECODE_FAILED` / `ASR_TIMEOUT` / `ASR_CONNECTION_ERROR` / `ASR_UPSTREAM_ERROR` / `ASR_INVALID_RESPONSE` / `ASR_EMPTY_TRANSCRIPT`；意外异常 `INTERNAL_ERROR`。**未配置 ASR 时返回 `ok:true + {text:"", skipped:true, reason:"ASR_NOT_CONFIGURED"}`**（不报错）。

### 10.3 IM 渠道配置

`channel.{feishu,xiaoyi,telegram,dingtalk,whatsapp,discord,slack,wecom,wechat}.get_conf/set_conf`；`channel.wechat.get_login_ui/unbind`。各渠道配置结构 normalization 在网关侧完成后写回。

### 10.4 更新与维护

| 方法 | 说明 |
|------|------|
| `updater.get_status` / `check` / `download` / `upgrade` | 更新状态机操作，响应均为下方 **UpdateStatus** 快照 |
| `updater.get_conf` / `reset_source` / `set_conf` | 更新源配置读写；响应为 `{enabled, desktop_release_api_type, release_api_type, install_mode, repo_owner, repo_name, release_api_url, asset_name_pattern(含 _windows/_macos/_linux), timeout_seconds, pypi_mirror, access_token(掩码 ****)}`（`common/updater.py:131-154`） |
| `health_check.get_conf` / `set_conf`（+`heartbeat.*` 废弃别名） | 见 §8.4 |
| `memory.compute` | **响应 payload**：`{rss_mb, total_mb, available_mb}`（本地执行，兼作连接 keepalive，不更新路由注册） |
| `memory.forbidden.get` / `memory.forbidden.set` | 禁止记忆（记忆黑名单）定义读写，**分流**：AgentOS 部署走 E2A 代理（get 无参数、set 透传 params）；legacy 单用户本地执行——get 返回 config `memory.forbidden_memory_definition` 段，set 写回后响应 `{ok:true}`（params 非对象 `BAD_REQUEST`）（`app_web_handlers.py:5909-5948`） |
| `hooks.list` / `commands.list` | 钩子 / 斜杠命令清单 |
| `path.get` / `set` / `select_directory` / `select_files` / `select_file` | 本机路径选择（原生选择器不可用回 `UNSUPPORTED`） |
| `external_cli.detect` / `install` / `install_status` 等 | 外部 CLI（Claude/Codex 等）依赖安装状态机（`install` 返回后台安装任务句柄，`install_status` 轮询进度与日志） |

**UpdateStatus 响应字段**（`common/updater.py:93-129`，四个状态方法共用）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `current_version` / `latest_version` | string | 当前 / 最新版本 |
| `state` | string | `idle` / `checking` / `downloading` / `upgrading` / `error` / `disabled` |
| `has_update` | bool | 是否有可用更新 |
| `install_mode` | string | `pip` / `desktop` |
| `release_notes` / `published_at` | string | 发行说明 / 发布时间 |
| `source_type` / `asset_name` / `matched_asset` | string | 更新源类型 / 资产名模式 / 命中的资产 |
| `download_url` / `downloaded_path` | string | 下载地址 / 已下载文件路径 |
| `downloaded_bytes` / `total_bytes` | int | 下载进度 |
| `error` / `checked_at` | string / float | 错误信息 / 最近检查时间 |
| `installing` | bool | 安装中 |
| `restart_command` | string | 升级后重启命令 |
| `platform` / `platform_supported` | string / bool | 平台 / 是否支持自动更新（`get_status` 附加） |

`check` 的 `manual:true` 触发手动检查；`download` 无可用更新时 `state:"error"` + "No update available..."；`upgrade` 非 pip 安装时由桌面端接管（`state:"error"` 说明文案）。

### 10.5 权限接口

`permissions.tools.get/set/update/update_user/delete`、`permissions.rules.get/create/update/delete`、`permissions.approval_overrides.get/delete`：**纯 E2A 转发**（权限状态唯一权威在目标 AgentServer 用户目录，Gateway 无本地 fallback）。`permissions.owner_scopes.get/set`：数字分身 owner 范围声明（A5 起同样统一经 E2A，`app_web_handlers.py:6346-6374`）。

### 10.6 纯转发域逐方法契约

以下方法在 `_FORWARD_REQ_METHODS` 白名单中、无本地 handler，经 E2A 转发 AgentServer（`app_web_handlers.py:408-599`），传输层契约同 §3.2（成功透传、失败透传 payload.code 缺省 BAD_REQUEST）。**注意两域特例**：a4p 转发带网关前置检查（`AGENT_NOT_READY` / `A4P_WEB_SESSION_REQUIRED`，`app_web_handlers.py:5826-5859`）；rsi.\* 一律由网关注入 `session_id`（`rsi.artifact.download` 另注入 `_download_user_id`）。

#### 10.6.1 skills 基础

公共机制：zip 安全限额——上传包 ≤50MB、解压总量 ≤100MB、文件数 ≤5000、单文件 ≤32MB、路径深度 ≤20（`skill_manager.py:138-142`）；多数 handler 业务失败不占错误码通道，返回 `ok:true` + `{success:false, detail, detail_key?}`；安装/导入/切换类方法成功后服务端**重建 agent 实例**（`interface.py:2158-2187`）。公共错误码：`SKILL_NOT_FOUND / SKILL_INVALID_PACKAGE / SKILL_INVALID_METADATA / SKILL_RESERVED_PATH / SKILL_IMPORT_OVERWRITE_REQUIRED / SKILL_BUILTIN_READ_ONLY / SKILL_ALREADY_EXISTS / SKILL_NAME_CONFLICT / SKILL_UNSAFE_PATH / SKILL_FILE_TOO_LARGE / SKILL_OPERATION_UNSUPPORTED / SKILL_VERSION_NOT_FOUND / SKILL_VERSION_CONTENT_INVALID / SKILL_DOWNLOAD_TOKEN_INVALID / SKILL_KNOWLEDGE_INPUT_CONFLICT / SKILL_REBUILD_FAILED`。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `skills.list` | `refresh_marketplaces`（bool，默认 false）、`with_installed`（bool，默认 false） | `{skills:[卡片], plugins?}`；卡片含 name/description/version/tags/allowed_tools/source/installed/enabled/is_builtin/has_evolutions/skill_type 等 | refresh 时先 clone/pull 市场源 |
| `skills.installed` | 无 | `{plugins:[{plugin_name, marketplace, spec, installed_at, git_commit, enabled, skills:[{name,version}]}]}` | — |
| `skills.get` | `name`（必填）、`version`（缺省 null 读 workspace 副本） | 详情：frontmatter 全字段 + `content`（正文，图片改写为受控预览 URL）+ file_path/enabled/config；SkillPack 追加 skillpack 结构 | 版本相关错误码显式拒绝 `version` 回退 |
| `skills.versions.list` | `name`（必填） | `{success, name, default_version, versions:[{version, is_default, available, created_at, updated_at}]}`（只读 `.archive`） | — |
| `skills.files.list` | `name`（必填）；**显式拒绝 `version`** | `{name, files:[{path, type, size, mime_type}]}`（隐藏 `.archive/`） | `SKILL_UNSAFE_PATH` |
| `skills.files.get` | `name`、`path`（必填，拒绝绝对路径/`..`/symlink）；拒绝 `version` | 文本（白名单后缀 ≤512KB）→ `{content, encoding:"utf-8"}`；二进制 → `{download_url}`（600s 有效） | `SKILL_UNSAFE_PATH` |
| `skills.rebuild` | `name`（必填）、`version`（**键必填**，null=workspace / 非空=版本副本）、`user_intent`（缺省 null）、`language`（默认 "cn"） | 两阶段：返回 followup 载荷后服务端静默执行 Agent 改写，最终 `{success:true}` | `SKILL_REBUILD_FAILED`；经验消费后清 evolutions.json |
| `skills.toggle` | `name`、`enabled`（必填）、`dry_run`（默认 false） | `{success, name, enabled, config}`；dry_run 返回 parent_skillpacks | SkillPack 含损坏成员时拒绝启用 |
| `skills.install` | `spec`（必填，`plugin_name@marketplace`；裸名匹配内置转内置安装）、`force`（默认 false） | `{success:true}`；失败 `{success:false, detail}` | — |
| `skills.uninstall` | `name`（必填） | `{success, restorable}`；内置技能拒绝删除 | 成员备份到包内 `_/` 目录供 pack_member.install 恢复 |

#### 10.6.2 skills 导入 / 知识转技能 / 市场源

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `skills.pack_member.install` | `pack`、`name`（均必填） | `{success, name, pack}` | 从包内备份恢复已卸载成员 |
| `skills.import_local` | `download_token` / `path`（**二选一**，同传报错；path 支持本地绝对路径或 http(s) 归档 URL）、`force`（默认 false）、`checksum_sha256`（仅 URL 导入时校验，缺省不校验） | `{success, skill:{name, description, version, skill_type, source, workspace_path}}` | `SKILL_DOWNLOAD_TOKEN_INVALID`；文件传输不经 WS（先 HTTP 上传拿 path） |
| `skills.import_upload` | `path`（必填，HTTP 上传产物的临时路径）、`overwrite`（默认 false，容忍 "1/true/yes/on"） | 同 import_local | `SKILL_FILE_TOO_LARGE`（>50MB）/ `SKILL_ALREADY_EXISTS` |
| `skills.create_from_knowledge` | `link` / `file_path`（**二选一**，同传/全空报错；file ≤50MB）、`skill_description`（缺省 ""）、`skill_name`（缺省 ""，未被占用时必须采用） | 两阶段：followup 后服务端静默跑 Agent 生成并安装 | `SKILL_KNOWLEDGE_INPUT_CONFLICT` / `SKILL_ALREADY_EXISTS` |
| `skills.marketplace.list` | 无 | `{marketplaces:[{name, url, enabled, install_location?, last_updated?}]}` | — |
| `skills.marketplace.add` | `name`、`url`（必填） | `{success}` | **新增源默认禁用** |
| `skills.marketplace.remove` | `name`（必填）、`remove_cache`（默认 true） | `{success, name, cache_removed}` | — |
| `skills.marketplace.toggle` | `name`、`enabled`（必填） | 启用时同步 clone/pull；禁用**删缓存但不卸载已装技能** | — |

#### 10.6.3 skills 在线源（online_search / skillnet / clawhub）

公共：无 RPC code，失败均为 `ok:true` + `{success:false, detail, detail_key}`；clawhub 需先 `set_token`。

| 方法 | 关键参数 | 响应要点 | 特殊 |
|------|---------|---------|------|
| `skills.online_search.search` | `query`（必填）、`limit`（默认 20，夹取 1–50）、`cache_mode`（缺省直连，仅 "prefer_cache" 走目录缓存）、`refresh`（默认 false，刷新缓存） | `{success, partial?, query, items:[归一化卡片], sources:[{source,status,count}], cache?}` | 多源并发（单源超时 30s）+ RRF 融合去重 |
| `skills.online_search.install` | `source`（默认 "teamskillshub"：teamskillshub\|swarmskillshub\|clawhub\|skillnet）、`identifier`（必填）、`force`（默认 false）、`version`/`market_url`/`display_name`/`owner_handle`/`tag`/`url`/`mirror_url`（缺省按目标源透传规则） | 即目标源安装方法的响应 | 转发壳 |
| `skills.skillnet.search` | `q`（必填）、`mode`（默认 "keyword"：keyword\|vector）、`limit`/`page`/`min_stars`（缺省不携带——用源默认）、`threshold`（缺省不过滤）、`sort_by`（缺省不传）、`category`（缺省不传） | `{success, query, count, skills:[{skill_name, ...}]}` | — |
| `skills.skillnet.install` | `url`（必填）、`force`（默认 false）、`mirror_url`（缺省 null，须 http(s)） | **异步**：`{success, pending:true, install_id}` | 轮询 install_status；任务表在内存，进程重启丢失 |
| `skills.skillnet.install_status` | `install_id`（必填） | `{success, status: pending\|done\|failed, skill?}` | — |
| `skills.skillnet.evaluate` | `url`（必填） | `{success, evaluation}` | 同步执行 |
| `skills.clawhub.get_token` | 无 | `{success, token(掩码), has_token}` | 永不回传明文 |
| `skills.clawhub.set_token` | `token`（缺省 ""，空串即清除） | `{success, token(掩码)}` | — |
| `skills.clawhub.search` | `q`（必填）、`limit`（缺省用源默认；非整数报错） | `{success, query, count, skills:[{slug, display_name, ...}]}` | detail_key 区分 tokenNotConfigured/httpError/searchFailed |
| `skills.clawhub.download` | `slug`（必填，兼作本地登记名）、`owner_handle`（缺省 ""，slug 多发布者时必需）、`version`（缺省 latest）、`tag`（缺省不传）、`force`（默认 false） | `{success, skill:{name, display_name, source:"clawhub"}}` | 120s 超时 |

#### 10.6.4 skills Hub（teamskillshub / swarmskillshub）

公共：市场超时默认 60s（env `TEAM_SKILLS_HUB_TIMEOUT`）；`token`/`system_token` 二选一（publish/delete 不走环境变量回落）。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `skills.teamskillshub.info` | `asset_id`、`version`（必填）、`market_url`（缺省用配置 base_url） | `{success, asset_id, version, data}` | data 结构由 Hub 决定 |
| `skills.teamskillshub.init` | `name`（必填）、`path`（默认 "."，仅 skills 目录内相对路径）、`skill_type`（默认 "teamskills"）、`force`（默认 false，目标非空时清空重建） | `{success, path}` | 脚手架模板 |
| `skills.teamskillshub.validate` | `path`（必填）、`skill_type`（缺省按 frontmatter 推断） | `{success, path, skill_file, skill_type, name, warnings}` 或 `{success:false, errors}` | roles 校验明细 |
| `skills.teamskillshub.pack` | `path`（必填）、`skill_name`（缺省取 SKILL.md name）、`version`（默认 "1.0.0"）、`display_name`（缺省 SKILL.md display_name → skill_name）、`output`（默认 "out"） | `{success, path, checksum_sha256}` | `SKILL_INVALID_METADATA` |
| `skills.teamskillshub.search` | `q`（缺省 ""=全量）、`page_size`（默认 20，夹取 1–100）、`page`（默认 1）、`skill_type`（缺省不过滤）、`author`（缺省 ""）、`order_by`（默认 install_count）、`desc`（默认 true）、`cache_mode`（缺省直连）、`refresh`（默认 false） | `{success, query, count, skills:[{asset_id, name, ...}]}` | — |
| `skills.teamskillshub.install` | `asset_id`（必填）、`version`（缺省取远端最新）、`force`（默认 false，**仅兼容保留，本期禁止覆盖**）、`display_name`（缺省取 skill_name）、`output`（缺省 ""=默认 skills 目录） | `{success, skill:{name, source:"teamskillshub", asset_id, path}}`；同 asset 幂等成功 | `SKILLHUB_INSTALL_FAILED` / `SKILL_NAME_CONFLICT`（同名不同 asset 即使 force 也拒绝） |
| `skills.teamskillshub.publish` | `token`/`system_token`（二选一）、`version`（必填）、`path`/`file`（二选一）、`skill_id`（缺省 null，更新场景传已有资产 ID）、`version_desc`（缺省 ""）、`force`（默认 false，跳过同版本冲突） | `{success, skill_id, name, version}` | `SKILL_PUBLISH_VERSION_CONFLICT`（同版本未 force） |
| `skills.teamskillshub.delete` | `skill_id`（必填）、`token`/`system_token`、`version`（默认 **"all"**） | `{success, skill_id, version}` | — |
| `skills.swarmskillshub.recommend` | `top_k`（默认 10，夹取 1–500）、`user_id`/`request_id`/`category_id`（缺省 ""——匿名推荐上下文，Hub 走冷启动）、`plugin_type`（缺省不传，可逗号分隔多值）、`cache_mode`（缺省直连）、`refresh`（默认 false） | `{success, request_id, count, skills, items}` | 缺凭证匿名 POST |
| `skills.swarmskillshub.detail` | `asset_id`（必填；**market_url/token 不可覆盖**） | `{success, asset_id, version, data}`（正文回填上限 200KB） | `SKILLHUB_DETAIL_NOT_FOUND/FAILED` |

#### 10.6.5 skills 检索 / 演化 / 图谱 / 经验

| 方法 | 关键参数 | 响应要点 | 特殊 |
|------|---------|---------|------|
| `skills.retrieval.status` | 无 | 检索目录状态：`{enabled, mode, effective_strategy, index_state, build_status, build_progress, build_logs(末 50 条), ...}` | 只读 |
| `skills.retrieval.index_build` | `force`（默认 false） | 后台构建；未启用 → `{success:false, error_code:"skill_retrieval_disabled"}` | 经 status 轮询 |
| `skills.retrieval.index_cancel` | `build_id`（缺省取消当前活跃构建） | `{success, build_status}` | 幂等 |
| `skills.retrieval.search` | `query`（必填非空） | `{success, mode, query, result, detailed_output}` | 走 skill_index 工具 |
| `skills.retrieval.tree` | 忽略 | 恒 `{success:false, unsupported:true}` | 已关闭的旧 RPC |
| `skills.evolution.status` / `get` / `save` | `name`（必填）；save 另需 `entries`（数组，每项 id 非空 + change.content 为字符串） | status `{name, exists}`；get 规范化文档；save `{success, entry_count, updated_at}` | 内置技能 `SKILL_BUILTIN_READ_ONLY`；save 双写版本副本可回滚 |
| `skills.graph.build` / `status` / `get` / `cancel` | build/cancel 仅 `force`（默认 false）；status/get 无参 | build `{success, background:true, build_status:"running"}`；get 缺失 → `{success:false, detail:"...请先构建总谱"}` | 后台单任务守卫 |
| `skills.experience.list` / `request` | list 无参；request `recipe_id`/`recipe_version`（**params 白名单**，多余键 reason=invalid_parameters） | list `{success, enabled, candidates}`；request `{success, request_id, candidate}` | request 经审批提示流再次发出 |

#### 10.6.6 assets.publish.\*（资产发布）

公共（`asset_publish_api.py:243-346`）：仅 web 通道（否则 `WEB_CHANNEL_REQUIRED`）；params **禁止** `path/scope/workspace/hub_url/market_url/publisher_id/user_id`（→ `INVALID_PARAMETERS`）；除 local_status 外均需 `auth`（`oauth_provider ∈ {gitcode, github}` + `access_token` ≤8192，否则 `AUTH_REQUIRED`）；`kind ∈ {skill, agent_template, agent_group, plugin, mcp}`（非法 `INVALID_KIND`）；未归类异常 `PUBLISH_UNAVAILABLE`；本地资产仅接受服务端解析的已安装根（`RESOURCE_NOT_FOUND`）。

| 方法 | 关键参数 | 响应要点 |
|------|---------|---------|
| `describe` | `kind`、`local_id`（必填）、`auth` | `{defaults:{asset_name, version, display_name, description, tags, visibility}, records, can_publish, hub_url}` |
| `prepare` | + `metadata`（必填，白名单键：asset_name/version/display_name/description/tags/version_desc/visibility/dependency_sources）、`target_asset_id`、`force` | 两阶段第一步：`{draft_id, expires_at, checksum_sha256, size_bytes, files, warnings, errors:[{code:"VERSION_CONFLICT"}]?}` |
| `commit` | `draft_id`、`request_id`（幂等键）、`auth` | 两阶段第二步，异步上传；响应即 status 记录结构 |
| `status` | `operation_id`、`auth` | `{execution_status: queued\|uploading\|completed\|failed\|unknown, result?, error?}` |
| `records` | `kind`、`local_id`、`auth` | `{records:[...], identity_verified:false}`（按凭据 scope 隔离） |
| `local_status` | `kind`、`local_id`（**无 auth**） | `{state: published\|pending\|unpublished\|unknown}`（跨 scope 聚合） |

#### 10.6.7 mcp.\*

公共：响应带 `type` 字段（list/detail/installed/connected/auth_required/...）；错误码 `MCP_INTERNAL / MCP_BAD_REQUEST / MCP_NOT_FOUND / MCP_UNREACHABLE / MCP_AUTH_FAILED` + CLI 结构化码（`MCP_RUNTIME_MISSING / MCP_INSTALL_NETWORK / MCP_CLI_INCOMPLETE / MCP_NAME_CONFLICT / MCP_CREDENTIALS_INVALID / MCP_INSTALL_SCRIPT_DEP`）；所有卡片/详情**敏感字段一律脱敏**（`agent_ws_server.py:8406-8422`）。

| 方法 | 关键参数 | 响应要点 | 特殊 |
|------|---------|---------|------|
| `mcp.list` | `filter`（默认 "builtin"：builtin\|local，非法值兜底 builtin）、`query`（缺省 ""=全量，分词 AND 匹配）、`cache_mode`（缺省直连，仅 "prefer_cache" 生效且无 query 时附 cache 状态）、`refresh`（默认 false） | `{type:"list", items:[卡片], cache?}` | Hub 不可达回退本地列表 |
| `mcp.show` | `id` 或 `name`（必填） | `{type:"detail", item:{...含 tools/skills/mcp_spec(脱敏)}}` | 已连接但 tools 空时临时连接回填 |
| `mcp.install` | `id`（Hub asset id，必填） | `{type:"installed", item, connect:{...}}`（安装后立即连接） | 同步 hold-open；连接失败不回滚安装 |
| `mcp.uninstall` | `id`（必填） | `{type:"uninstalled", item, applied, error?}` | 卸载含内置 skills 与凭据 |
| `mcp.connect` | `name`（必填） | `connected`（含脱敏 item）/ `auth_required`（两阶段哨兵，含 auth_url/step_index）/ `credentials_required`（含 required_tokens）/ `connect_failed`（**ok:false**） | CLI 型两阶段：第一阶段 ok:true + auth_required → 调 `mcp.wait_auth`；live 探测失败回滚（`MCP_UNREACHABLE`） |
| `mcp.wait_auth` | `name`（必填）、`step_index`（默认 0） | hold-open 轮询（200 次×3s ≈ 10 分钟）→ connected / cancelled / 超时 `MCP_AUTH_FAILED` | 单次最终响应 |
| `mcp.cancel_connect` | `name`（必填） | `{type:"cancelled", name}` | 幂等；绝不碰已 connected |
| `mcp.disconnect` | `name`（必填） | `{type:"disconnected", name, applied, item, error?}` | 保留包/自定义定义 |
| `mcp.register_custom` | `name`（必填）、`transport`（必填：stdio\|sse\|streamable-http）、`command`（stdio）/ `url`（远程）、`headers/env/timeout_s` | `{type:"registered", name, item}` | 不连接——随后须调 `mcp.connect` |
| `mcp.delete_custom` | `name`（必填） | `{type:"deleted", name, applied, item}` | 自定义 MCP 终极删除 |
| `mcp.save_credentials` | `name`、`tokens`（dict 必填非空） | `{type:"credentials_saved", name, saved_keys}` | token 永不回显 |

#### 10.6.8 agents.\*（子 agent 配置）

数据模型 `AgentDefinition`：`name/description/prompt/source(builtin\|user\|project\|local)/model/tools/color/permission_mode/memory_scope/shadowed_by/enabled/when_to_use/max_iterations/skills`；合并优先级 project > user > local > builtin。除注明外错误为 `ok:false {error}` 无 code。

| 方法 | 关键参数 | 响应要点 |
|------|---------|---------|
| `agents.list` | `workspace_dir`（可选，缺省 cwd） | `{agents:[AgentDefinition]}` |
| `agents.get` | `name`（必填） | `{agent:{...}}` |
| `agents.create` | `name`（必填，`^[a-zA-Z0-9_-]{3,50}$`）、`description`/`prompt`/`location`（user\|project\|local）必填、`generate`（**默认 true**：LLM 生成 when_to_use/prompt，失败回退）、其余模型字段可选 | `{agent, generated, applied, reload_error}`；写 `<location>/agents/<name>.md` 并热 reload |
| `agents.update` | `name` + 全可选字段（None=不改）、`generate`（默认 false） | 同 create |
| `agents.delete` | `name` | `{ok, applied, reload_error}`；builtin 拒绝 |
| `agents.enable` / `disable` | `name`（必填） | `{name, enabled, applied, reload_error}`；builtin 拒绝 |
| `agents.tools_list` | `workspace_dir` | `{tools:[{name, internal_name, description, group}], groups, disallowed_for_subagents}` |

#### 10.6.9 agent_groups / agent_templates / plugin_packages（装备包）

公共：`id` 为主键（`name` 兼容别名）；列表 `filter` 枚举 `builtin / builtin+hub / mine / local`；安装失败包装 `{error}`，Hub 资产不存在 `code:"HUB_ASSET_NOT_FOUND"`；文件预览仅 `.md/.mdx/.json/.py/.pdf`（文本 ≤1MiB、脱敏、防穿越/symlink、PDF 返回 600s 下载链接）。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `agent_groups.list` / `show` | list：`filter`（枚举 builtin\|builtin+hub\|mine\|local，缺省不过滤）、`query`（缺省不过滤）、`cache_mode`（缺省直连）、`refresh`（默认 false）；show：`id`（必填） | `{agentGroups:[卡片]}` / `{group}`（show 含 version/details/persona/members/skills/capabilities） | — |
| `agent_groups.file.list` / `file.read` | `id`（+read 的 `path`） | `{tree:[...]}` / `{path, content}`（PDF → download_url） | — |
| `agent_groups.create` | `id`、`name/description/persona`（必填非空）、`leaderId`（必填）、`memberIds`（list，可空）、`skills`（须已存在）、`quickInputs/tags/category`；**禁止** `model/mcps/pluginNames/apiKey/apiBase/path` | `{id: package_id}` | `AGENT_GROUP_*` 结构化码族（NAME_INVALID/DUPLICATE/LEADER_REQUIRED/MEMBER_INVALID 等 12 个，`extension_package_manager.py:3535-3592`） |
| `agent_groups.import_local` | `path`（必填：本地**绝对**路径的目录或 zip/tar，解压 ≤512MiB/≤10000 文件/深度 ≤32） | `{id}`；**导入后立即 install** | — |
| `agent_groups.install` / `uninstall` | `id` | `{}` | 组安装**无 connector 门、无超时**（对比模板/插件） |
| `agent_templates.list` / `show` | list：`filter/query/include_team_compatibility`；show：`id` | `{templates:[卡片]}` / `{template}`（含 mcps/pending_connectors/connection_state/persona/runtime） | connection_state 由 connector 的 MCP 状态聚合 |
| `agent_templates.file.list` / `file.read` | 同 groups | 同 groups | — |
| `agent_templates.create` / `update` | `id`、`name/description/persona`（必填）、`skills`（必填）、`mcps/quickInputs/tags/runtime`；runtime 设定时禁止 connectors | `{}` | 仅 local 包可 update/delete |
| `agent_templates.delete` | `id` | `{}` | 先卸载运行中实例 |
| `agent_templates.import_local` | `path`（同上；须含 README.md 与 persona 目录） | `{id}`；**导入但不安装** | — |
| `agent_templates.install` | `id` | `{}` | **connector 就绪门**：manifest 声明的 connector 任一未 connected → `ok:false {error, pending_connectors:[...]}`；120s 超时 |
| `agent_templates.uninstall` | `id` | `{}`（+connector 保留 notice） | — |
| `plugin_packages.*` | 与 agent_templates 同构（create 无 persona/quickInputs/tags/runtime） | `{packages}` / `{package}` / `{}` | install/uninstall 与模板同契约（connector 门 + 120s） |

#### 10.6.10 plugins / extensions

| 方法 | 关键参数 | 响应要点 | 特殊 |
|------|---------|---------|------|
| `plugins.list` | 无 | `{plugins:[{plugin_name, marketplace, spec, version, installed_at, git_commit, skills, enabled}]}` | — |
| `plugins.install` | `spec`（必填：marketplace spec 或本地路径/URL）、`force` | 透传 skills 安装结果 | 成功后重建 agent 实例 |
| `plugins.uninstall` | `name` | 透传 skills 卸载结果 | — |
| `plugins.enable` / `disable` | `name`（必填） | `{success, detail:"...请执行 /reload-plugins 使其生效"}` | 物理目录移动延迟到 reload |
| `plugins.reload` | 无 | `{success, plugins_count, disabled_count, skills_count, detail}` | 按 enabled 移动目录 |
| `extensions.list` | 无 | `{extensions:[{name, class_name, enabled, description, priority}]}` | — |
| `extensions.import` | `folder_path`（必填：目录须含 rail.py 且内容可编译） | 新扩展对象（enabled=false） | 复制到 extensions 目录 |
| `extensions.delete` | `name`（必填） | `{deleted:true, name}` | — |
| `extensions.toggle` | `name`、`enabled`（必填 bool） | 更新后的扩展对象 | live agent 热注册/注销 |

#### 10.6.11 personal_context.runtime.*（采集与使用开关）

公共：实现于 `server/personal_context/`（Host 主目录 `~/.jiuwenswarm/.personal_context`）；业务错误码为 **Core 数值串**：`154000`（配置/参数类，reason 细分 config_invalid/parameter_invalid/duplicate_service/service_limit/unknown_service/model_invalid/authorization_*/credentials_invalid/not_configured）、`154001`（状态类：state_invalid/service_running/collection_disabled/not_configured）、`154002`（文件/图读取类）、`154003-154006`（采集/处理/写入/超时）——payload 附加 `status`（状态名）与本地化消息；参数 ValueError → `BAD_REQUEST`。开关类方法响应均为**投影配置** `{master_enabled, collection_enabled, agent_use_enabled, strategy_profile, max_pages_per_directory, max_subdirectories_per_directory, fetch_services[](已去凭据), model_index, model_id}`。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `runtime.status` | 无（params 忽略） | 有界状态快照：`{configured, collection_enabled, agent_use_enabled, state(CREATED…FAILED 七态), pipeline_running, fetch_service_states{}, fetch_run_progress{}, context_ready, last_error}` | 只读，不启动运行时 |
| `runtime.start_collection` / `stop_collection` | 无 | 投影配置 | start 未配置时**创建初始配置**；stop 未配置返回未配置投影不报错；stop 先写盘再停 |
| `runtime.start_agent_use` / `stop_agent_use` | 无 | 投影配置 | 未配置 → `154001 not_configured`；只翻内存标志不启停运行时，幂等 |
| `runtime.set_master_enabled` | `enabled`（bool，必填） | 投影配置 | 总开关级联两个子开关；**之后子开关可独立切换不反写总开关** |
| `runtime.get_config` | 无 | 已配置→投影配置（无 configured 字段）；未配置→含 `configured:false` 与默认值 | — |
| `runtime.patch_config` | `patch`（dict，必填；**仅允许** `collection_enabled/agent_use_enabled/strategy_profile`，strategy ∈ rules\|balanced\|agent） | 更新后的投影配置 | 未知字段 `154000`；活动轮次中 `154001 service_running`；热重启失败自动回滚 |
| `runtime.select_model` | `model_index`（int，必填，非负；按 `models.list` 下标） | 投影配置（含稳定 `model_id`，格式 provider:model_name） | 越界 → `154000 model_invalid` |

#### 10.6.12 personal_context.fetch.*（采集服务）

service 对象字段：`service_id`（≤约 500 字符）、`provider`（`local_files|github|gitcode|feishu|browser_bookmarks|zhihu_reader|toutiao_reader|rss_feed`）、`enabled`（bool）、`interval_seconds`（默认 10800，0<v≤31536000）、`max_items_per_run`（null，1..10000）、`time_range`（`{mode:"all"}` | `{mode:"recent", recent_days}` | `{mode:"fixed", start_at, end_at}`）、`source`（按 provider：github `{owner, repo, resources?}`、feishu `{mode:account|wiki_space, ...}`、rss_feed `{feed_url}` 等）。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `fetch.list_services` | 无 | `{services:[投影 service]}`；未配置 → 空列表 | — |
| `fetch.create_service` | `service`（dict，必填） | 创建后的投影 service | 同 provider ≥20 → `154000 service_limit`；github/gitcode 须先授权（`authorization_required`）；热应用失败回滚 |
| `fetch.delete_service` | `service_id`（必填） | `{ok:true}` | 活动轮次 → `154001 service_running`；保留已发布 Context 文件 |
| `fetch.patch_service` | `service_id` + `patch`（**仅允许** interval_seconds/max_items_per_run/source/time_range） | 更新后的投影 service | 服务跑动中拒绝 |
| `fetch.start_service` / `stop_service` | `service_id`（必填） | `{ok:true}` | 只切换**未来调度开关**（enabled 位），不影响进行中轮次；立即执行用 run_one |
| `fetch.run_all` | 无 | `{state:"accepted", service_ids, runs:[{service_id, run_id}]}`（异步） | 采集关闭 → `154001 collection_disabled`；无 enabled 服务拒绝 |
| `fetch.run_one` | `service_id`（必填） | 同上单元素 | — |
| `fetch.stop_run` | `service_id`（必填） | `{ok:true}` | **幂等**——无活动任务静默成功 |
| `fetch.get_run_status` | `service_id`?、`run_id`?（run_id 须伴随 service_id） | 三形态：按服务分组 / 单服务 / 单条 record（含进度、错误、created_node_count；历史每服务保留 5 条） | run 过期 → `154001` |
| `fetch.get_authorization_status` | `provider`（必填） | github/gitcode：`{provider, state: not_authorized\|authorized\|authorization_failed, account?, verification_url, expires_at, error}`（已存 PAT 实时验证）；feishu 设备流状态 | github 未配置返回 not_authorized **不报错** |
| `fetch.authorize_provider` | `provider`（必填）、`credentials`（github 须恰 `{token}`、gitcode 须恰 `{pat}`；feishu 必须 null）、`reauthorize`（默认 false） | github/gitcode：PAT 直传验证存盘；feishu：设备流 OAuth（`{state: authorizing\|authorized, authorization_step, verification_url, ...}`） | PAT 无效 `154000 credentials_invalid`；reauthorize=true 热替换该 provider 全部服务凭据；凭据永不回传 |

#### 10.6.13 personal_context.context.*（已发布上下文）

事件 `personal_context.context.start/nodes/edges/end` 全量 payload 透传（§3.3）。

| 方法 | 关键参数 | 响应要点 | 错误码/特殊 |
|------|---------|---------|------------|
| `context.stream_graph` / `stream_tree` | `root_id`（缺省 null=从根的子层开始）、`depth`（默认 3，1..10）；**必须 `is_stream:true`** | 事件序列 start → nodes×N（每批 200）→ edges×N → end；node `{id:"page:<相对路径>", kind, label, path, has_children}`、edge `{source, target, kind: contains\|references}`（tree 仅 contains） | graph 与 tree 的差异仅在边类型；纯文件读不启动运行时 |
| `context.search_pages` | `query`（必填非空） | `{results:[{node_id, title, path, snippet}]}（最多 10 条，BM25F 加权）` | — |
| `context.get_node` | `node_id`（必填，须 `page:` 前缀） | `{node_id, title, path, markdown}` | `source:` 前缀提示改用 get_source |
| `context.get_source` | `source_id`（必填，`^src_[0-9a-f]{32}$`） | `{source_id, title, source_type, locator, provider, service_id, first_seen, last_seen}` | — |

#### 10.6.14 a4p.*（预授权）

公共：网关前置 `AGENT_NOT_READY`（AgentServer 未就绪）与 `A4P_WEB_SESSION_REQUIRED`（WS 路由表无该 session 的 Web authorizer 路由）；配置类方法在 A4P 关闭时仍可用；两阶段授权流：`a4p.authorization_request` 事件（payload 含 requestId/kind/mandate/signingOptions/originalActions/preparedActionIndexes）→ reprepare/complete/reject → `a4p.authorization_terminated` 事件（终码 `A4P_AUTHORIZATION_TIMEOUT`(默认 300s)/`A4P_AUTHORIZATION_CANCELLED`/`A4P_REPREPARE_FAILED`）。

| 方法 | 关键参数 | 响应要点 | 错误码 |
|------|---------|---------|--------|
| `a4p.config.get` | 无（**不接受任何配置字段**） | `{enabled: bool}` | — |
| `a4p.config.update` | `enabled`（bool，必填，仅此一键） | `{enabled}`（落盘值） | `A4P_CONFIG_APPLY_FAILED`（已保存但热重配失败） |
| `a4p.authorization.reprepare` | `requestId`（必填）、`selectedActionIndexes`（int[]，必填：非空、0≤i<len(original_actions)、无重复） | `{ok, pending:<快照>}`；旧 requestId 作废并重新推送 | `A4P_INVALID_SELECTION` / `A4P_NOT_PENDING` / `A4P_ROUTE_MISMATCH` / `A4P_REPREPARE_FAILED` / `A4P_DISABLED` |
| `a4p.authorization.complete` | `requestId`（必填） | `{ok:true, requestId}`（签发签名 mandate 给 Agent） | `A4P_NOT_PENDING` / `A4P_ROUTE_MISMATCH` / `A4P_AUTHORIZATION_INVALID` |
| `a4p.authorization.reject` | `requestId`（必填）、`reason`（缺省 "User rejected..."） | `{ok:true, requestId}` | 同上 |
| `a4p.authorization.pending` | 无 | `{ok:true, pending:<快照>\|null}` | A4P 关闭时返回 `{ok, pending:null}` 不报错 |

#### 10.6.15 rsi.*（训练）

公共：错误语义 RsiError → `{error, code}`；网关注入 `session_id`（推送事件 `rsi.training.status.changed/progress/tree.delta` 据此路由回发起会话）。任务状态机：CREATED→QUEUED→RUNNING→COMPLETED/FAILED/PAUSED/TERMINATED（每步触发推送）。

| 方法 | 关键参数 | 响应要点 | 错误码 |
|------|---------|---------|--------|
| `rsi.dataset.validate` | `input_file`（必填，别名 dataset_path）、`scenario`（必填：HARNESS\|ARTIFACT）、`artifact_type`（ARTIFACT 必填：PAPER\|PROGRAM） | `{valid, sample_count, errors:[{reason, code}]}` | `SCENARIO_NOT_SUPPORTED` / `PATH_INVALID` / `DATASET_INVALID` |
| `rsi.task.create` | `name`（1–100 字符）、`scenario`、`model_refs:{optimizer, tester?}`、HARNESS 另需 `input_file`；PROGRAM 需 `artifact_path`；PAPER 需 artifact_path 或 `optimization_instruction`；`max_iterations`（默认 1）；HARNESS `training_options`（execution_method/max_epochs 等） | `{task_id:"rsi-<uuid8>", status:"CREATED"}` | `INVALID_HARNESS` / `MODEL_NOT_FOUND` / `UNSUPPORTED_PARAMETER` 等 |
| `rsi.task.list` | `scenario`（缺省不过滤：HARNESS\|ARTIFACT）、`artifact_type`（缺省不过滤；HARNESS 过滤时携带报错） | `{tasks:[六字段投影]}`（created_at 倒序） | — |
| `rsi.task.get` | `task_id`（必填） | `{config, progress, best_artifact, usage?, failure_reason?}` | `TASK_NOT_FOUND` |
| `rsi.task.delete` | `task_id`（必填） | `{ok:true}` | `TASK_STATE_CONFLICT`（RUNNING/PAUSED 不可删；QUEUED 先 cancel） |
| `rsi.training.start` / `resume` | `task_id`（必填） | `{status:"QUEUED"}`（单队列并发=1；QUEUED 幂等短路） | `TASK_STATE_CONFLICT`（resume 仅 PAUSED 可用） |
| `rsi.training.pause` | `task_id`（必填） | QUEUED→立即 PAUSED；RUNNING→返回当前 RUNNING（待 Provider 确认迁移） | `TASK_STATE_CONFLICT` |
| `rsi.training.terminate` | `task_id`（必填） | QUEUED→TERMINATED；RUNNING/PAUSED→保持当前态待确认（生产引擎无钩子时立即 TERMINATED） | 终态任务拒绝 |
| `rsi.report.get` / `rsi.usage.get` / `rsi.tree.get` | `task_id`（必填） | 报告（best_score/metrics/usage）/ 用量（tokens/cost/按轮次/按节点）/ 演进树 `{nodes:[{node_id, iteration, parent_id, type, adopted, score, ...}], depth}` | `TASK_NOT_FOUND` |
| `rsi.artifact.download` | `task_id`（必填）、`artifact_id`（缺省=best 产物） | 文件 → `{path, kind, filename, download_url, download_token}`（短时 HTTP 凭证，与聊天附件同机制）；目录 → `{path, is_directory:true}`（改走 files.list） | `ARTIFACT_NOT_FOUND` / `PATH_INVALID`（产物路径限任务目录内）；**下载即消费**——放行后续 delete |
| `rsi.artifact.files.list` | `task_id`、`path`（必填） | `{root, files:[{name, path, isDirectory, size, type}]}`（仅 artifact/run/snapshots/.rsi_artifact_views 四目录公开；zip 产物展开到内容寻址缓存） | zip >256MB 拒绝 |
| `rsi.artifact.files.get` | `task_id`、`path`（必填） | `{path, size, type, encoding: text\|base64, content}`（预览上限 10MB） | — |
| `rsi.harness.install` | `task_id`（必填） | `{installation_id, harness_name, sha256, status:"ACTIVE", already_active, hot_load}`（不可变版本目录 + 原子替换 + 热加载广播） | `RSI_HARNESS_NOT_READY/NOT_PUBLISHED/INVALID/INSTALL_CONFLICT/INSTALL_FAILED`；幂等 |
| `rsi.harness.versions.list` | 无（忽略参数） | `{active_installation_id, versions:[{installation_id, task_id, sha256, is_active, is_initial, available}]}`（**不暴露本地路径**） | — |
| `rsi.harness.rollback` | `installation_id`（必填） | 安装记录 + `{from_installation_id, rolled_back}` | 存在非终态任务时 `TASK_STATE_CONFLICT`；**校验目标内容 hash 未变化** |

#### 10.6.16 harness.*（扩展包管理）

⚠️ **`harness.import` / `harness.export` 在 AgentServer 侧无分发分支**（`HARNESS_PACKAGES_IMPORT/EXPORT` 枚举存在、网关正常转发并带 >4MB 分块上传与结构化错误透传，但 `agent_ws_server.py` 分发链无 handler）——调用会落入通用 unary/chat 运行时，**响应形态未定义**；业务实现 `import_package/export_package` 存在于 `AutoHarnessService` 但无 E2A 接线。其余方法错误码由消息映射：already active/exists → `CONFLICT`、not found → `NOT_FOUND`、其余 `BAD_REQUEST`。

| 方法 | 关键参数 | 响应要点 |
|------|---------|---------|
| `harness.packages`（wire 名 `harness.packages.get`） | 无 | `{packages:[{id, extension_name, is_active, version_label, ...}], native_version, active_package_ids}` |
| `harness.packages.scan` | 无 | 同上（扫描 runtime_extensions 并持久化） |
| `harness.activate` | `package_id`（必填） | `{activated_package_id, message}`；已激活幂等成功（`CONFLICT` 语义但成功）；热加载广播 |
| `harness.deactivate` | `package_id`（必填） | `{deactivated_package_id, message}`；未激活幂等 |
| `harness.delete` | `package_id`（必填；`"native"` 拒绝） | `{deleted_package_id, switched_to_native, message}`；活动包先卸载再删 |
| `harness.import` | `file_content`（base64；>4MB 网关先分块上传再以 `uploaded_path` 转发） | **无 AgentServer 处理方，未定义** |
| `harness.export` | `package_id` | **无 AgentServer 处理方，未定义** |

#### 10.6.17 issue.*（GitCode issue 自动处理）

公共：响应恒 `ok:true`（异常 `ok:false` 仅 `{error}` 无 code）；token 回退链 params → env `GITCODE_ACCESS_TOKEN` → base_config。

| 方法 | 关键参数 | 响应要点 |
|------|---------|---------|
| `issue.watch_once` | `access_token`?、`owner`+`repo_name` 或 `repo`?、`max_issues`（默认 1，夹取 1–5）、`issue_numbers`?、`labels`（默认 auto-harness）、`exclude_labels`、`dry_run`（默认 false）、`comment_on_start`、`max_auto_difficulty`（默认 medium） | `{owner, repo, fetched, started:[...], skipped:[...], reconciled:[...]}`（跳过条件：已关闭/缺 label/终态/已有关联 PR/难度超限） |
| `issue.state.list` | 无 | `{issues:[{...记录, task_status?, progress?, pr_url?}]}`（轻量对账） |
| `issue.matrix` | `force_refresh`（默认 false）、`page`（默认 1）、`per_page`（默认 50，夹取 1–100）、`labels`? | `{total, added, removed, updated, difficulty_counts, matrix:[...], cached}`（缓存按 owner/repo 持久化） |
| `issue.delete` | `issue_numbers`?、`delete_completed`/`delete_failed`（默认 false） | `{deleted:[...], rejected:[{issue, reason:"任务正在执行中..."}]}`（running 态拒绝删除） |

另有 `proactive.feedback`（点赞/点踩反馈，经 E2A 写 recommendation.json 的 feedback_buffer）；`schedule.*` 见 §8.3。

### 10.7 Designer 画布（执行图编排）

13 个方法，均为 E2A 代理（`designer_handlers.py:9-23`，注册于 `app_web_handlers.py:5829-5832`），错误码遵循 §3.2 通用契约：

| 方法 | 说明 |
|------|------|
| `designer.workspace.create` / `designer.workspace.get` | 画布工作区创建 / 读取 |
| `designer.workspace.session.create` / `designer.workspace.session.compose` | 在工作区内创建 / 组合画布会话 |
| `designer.graph.get` / `designer.graph.list` / `designer.graph.save` / `designer.graph.patch` | 执行图读写（`designer.graph.bootstrap` / `designer.graph.chat` 尚未挂到 Web 路由） |
| `designer.media.capabilities` | 媒体能力查询 |
| `designer.run.start` / `designer.run.get` / `designer.run.pause` / `designer.run.cancel` | 画布运行控制（`designer.run.choose_output` 已删除） |

推送事件（全量 payload，附录 B）：`designer.run.updated` / `designer.node.updated` / `designer.graph.updated` / `designer.leader.activity`。**路由**：designer 帧走通用三级路由（发起连接 ws_id 钉死 → A4P 改绑 → session 桶；旧基线"不按 ws_id 钉死"的特例已删除，`web_connect.py:1509-1543`）。

---

## 附录 A. 方法分发总表

按处理位置归类（"本地"= Gateway Web 层 handler；"E2A 代理"= 本地 handler 内调 `proxy_unary_request`；"白名单转发"= 无本地 handler，经 MessageHandler 转发，响应由 AgentServer 回写）：

| 处理位置 | 方法 |
|---------|------|
| 本地（纯本地逻辑） | `memory.compute`、`hooks.list`、`commands.list`、`path.*`、`updater.*`、`health_check.*`（+heartbeat 别名）、`channel.*` 配置族、`task.asr.transcribe`、`external_cli.*`、`openai_account.*`、`cron.job.*`（9 个）、`project.remove`（Gateway 编排）、`chat.send/interrupt/resume/user_answer/swarmflow_reply`（空 ack shell）、`history.get`（ack shell）、`connection.status`、`topic_starters.*`、`prompt_suggestion.*` |
| E2A 代理（本地 handler + 转发） | `session.list/create/get_metadata/rename/pin/plan_status/message.continue_queued`、`session.selection.set`、`project.list/info/get_sessions/get_cron_sessions/create/rename/pin/restore/pinned_sessions`、`project.git.*`（11 个）、`media.persist/discard`、`document.persist/formats`、`heartbeat.job.*`（10 个，经 HeartbeatControllerProxy）、`permissions.*`（tools/rules/approval_overrides/owner_scopes）、`a4p.*`（6 个，含 authorization.pending）、`harness.*`、`rsi.*`、`designer.*`（13 个，§10.7）、`workspace.tree/preview/usage/entries.delete`、`file.import_url`、`file.upload_chunk`、`config.get/set/save_all`、`models.*`、`model_groups.*`、`vendors.*`、`channel.get`、`memory.forbidden.get/set`（分流） |
| 白名单转发（无本地 handler） | `initialize`、`session.switch/fork`、`session.assets.list/register/rename`、`team.*`（templates.list/bindings.list/binding.create/binding.generate/session.bind/delete/snapshot/history.get/mq.publish/list）、`group.*`、`org.snapshot`、`swarmflow.pause/resume/stop`、`command.goal/btw/compact/mcp/workflows`、`chat.*`（上述空 ack 之外的转发主体）、`history.get`（数据主体）、`schedule.*`、`issue.*`、`skills.*`（40+）、`assets.publish.*`、`mcp.*`、`agents.*`、`agent_groups.*`、`agent_templates.*`、`plugins.*`、`plugin_packages.*`、`extensions.*`、`personal_context.*`、`proactive.feedback` |

`/ws/git` 封闭方法集（6 个）：`project.git.diff_watch/diff_files_watch/diff_detail_watch/diff_unwatch/discard_turn_changes/redo_turn_changes`（§7.3）。

当前全集 489 个 WS 方法（个人版 482；企业版另注册 team 控制面 7 个，见 §5.2）。相对旧基线的新增域：`jiuwenmate.*`（46 个转发 + 1 个本地）、`client.*`、`mcp_app.*`、`group.execution.*` / `group.file.*` / `group.history.clear`、`skills.source.*` / `skills.update` / `skills.updates.check`（企业版另有 `skills.enterprise.*`）、`channel.feishu|dingtalk.bind_agent/unbind_agent`、`channel.feishu.get_bindings/revoke`、`channel.welink.*`（7 个，均 local_only 不经转发）。应用插件（extensions registry）还可动态注册额外方法，插件禁用时统一回 `APPLICATION_PLUGIN_DISABLED`。

## 附录 B. 事件类型总表（EventType，协议包 `openjiuwen_protocol/message.py` + 网关 shim `common/protocol_compat.py`，共 50 个）

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
| skills | `SKILL_NOT_FOUND / SKILL_INVALID_PACKAGE / SKILL_INVALID_METADATA / SKILL_RESERVED_PATH / SKILL_IMPORT_OVERWRITE_REQUIRED / SKILL_BUILTIN_READ_ONLY / SKILL_ALREADY_EXISTS / SKILL_NAME_CONFLICT / SKILL_UNSAFE_PATH / SKILL_FILE_TOO_LARGE / SKILL_OPERATION_UNSUPPORTED / SKILL_VERSION_NOT_FOUND / SKILL_VERSION_CONTENT_INVALID / SKILL_DOWNLOAD_TOKEN_INVALID / SKILL_KNOWLEDGE_INPUT_CONFLICT / SKILL_REBUILD_FAILED / SKILL_PUBLISH_VERSION_CONFLICT / SKILLHUB_INSTALL_FAILED / SKILLHUB_PUBLISH_FAILED / SKILLHUB_DETAIL_NOT_FOUND / SKILLHUB_DETAIL_FAILED` |
| 资产发布 | `WEB_CHANNEL_REQUIRED / AUTH_REQUIRED / INVALID_KIND / INVALID_PARAMETERS / INVALID_METADATA / INVALID_PACKAGE / RESOURCE_NOT_FOUND / PUBLISH_UNAVAILABLE`（+ store/build 映射码如 `draft_limit / artifact_limit / VERSION_CONFLICT`） |
| MCP | `MCP_INTERNAL / MCP_BAD_REQUEST / MCP_NOT_FOUND / MCP_UNREACHABLE / MCP_AUTH_FAILED`；CLI 结构化：`MCP_RUNTIME_MISSING / MCP_INSTALL_NETWORK / MCP_CLI_INCOMPLETE / MCP_NAME_CONFLICT / MCP_CREDENTIALS_INVALID / MCP_INSTALL_SCRIPT_DEP` |
| agents 装备包 | `AGENT_GROUP_NAME_INVALID / AGENT_GROUP_DUPLICATE / AGENT_GROUP_LEADER_REQUIRED / AGENT_GROUP_MEMBER_*`（INVALID/CONFLICT/RESERVED/DUPLICATE/NOT_FOUND/INCOMPATIBLE/AGENT_MD 共 7 个）/ `AGENT_GROUP_TEMPLATE_INVALID / AGENT_GROUP_MEMBERS_REQUIRED / AGENT_GROUP_REQUEST_FAILED / HUB_ASSET_NOT_FOUND` |
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

## 返回导航

- [返回文档首页](../../README.md)
- [返回项目首页](../../../README.zh.md)
