# 认证与鉴权说明

本文解释 Agent Gateway 的认证与鉴权体系：谁在哪个入口出示什么凭据、凭据如何存储与流转、以及 Gateway 用哪些规则防止客户端伪造身份。模块路径均相对 `agent_gateway/` 包。

> **English**：[../../en/explanation/authentication.md](../../en/explanation/authentication.md)

---

## 总览

Gateway 的鉴权分五层，各层独立生效、互不替代：

| 层 | 保护对象 | 机制 | 主要实现 |
| --- | --- | --- | --- |
| 信道接入 | WebSocket / HTTP 入口 | Origin 校验、IAM 握手鉴权、SSH 公钥校验、IM 应用凭据 + 用户白名单 | `channel_manager/`、`common/security/`、`extensions/auth/` |
| 用户身份 | 登录态 | 华为账号 OAuth2 授权码 + PKCE、加密会话存储 | `auth/` |
| 模型凭据 | 免费模型调用 | 请求级透传 + 占位句柄 + AgentServer 登记表注入 | `auth/passthrough.py`、`auth/login_credentials.py` |
| 数据面 | 文件上传 / 下载 | HMAC 签名的自包含令牌 | `common/file_download_token.py` |
| 内部通道 | Gateway ↔ AgentServer | 共享密钥头 | `routing/agent_client.py` |

企业版另有出站凭据存储（`common/secrets/`）与管理面 mTLS，见[企业版凭据管理](#企业版凭据管理)。

---

## 登录用户身份

### 登录流程（OAuth2 授权码 + PKCE）

登录功能由远端配置开关：Gateway 启动时拉取一次配置（默认 `https://aigw.openjiuwen.com/v1/config`，可用 `JIUWENSWARM_CONFIG_URL` 覆盖；显式设为空或 `off` 即彻底关闭登录，登录路由返回 404）。配置同时下发 OAuth 参数与 APIG 接入点。

流程由 `auth/account_kit.py`（`AccountKitFlow`）与 `auth/service.py`（`AuthService`，进程级单例）实现：

1. 客户端 `POST /api/v1/auth/authorize`，Gateway 生成 PKCE S256 的 `code_verifier` / `code_challenge` 与随机 `state`（有效期 10 分钟），返回华为账号授权页 URL。
2. 浏览器完成授权后回调。回调可能落在本机（`GET /api/v1/auth/callback`），也可能落在 ECS 交换服务上——此时发起方轮询认领，`state` 中内嵌 claim 指纹防止他人冒领。
3. Gateway 用授权码换 `id_token` + `refresh_token`。换 token 请求发往配置下发的交换服务（由它补 `client_secret`，本地不持有 secret）。`id_token` 的 JWT payload 只在本地解析出 `user_id` 与过期时间，**Gateway 不验签**——验签由 APIG 侧的自定义认证器完成。
4. 会话建立后，发起方凭 `{state, claimToken}` 调 `POST /api/v1/auth/claim` 认领会话。

### 会话存储与生命周期

`auth/session_store.py`（`AuthSessionStore`）：

- **存储**：内存 + 加密落盘 `~/.jiuwenswarm/infra/auth/sessions.json`（受 `JIUWENSWARM_DATA_DIR` 影响）。加密一律 AES-256-GCM，密钥为本机 `.install_key`（32 字节、权限 0600、每机一把，损坏时留档重建）。存档含 `refresh_token`，明文落盘等于交出账号额度。
- **多进程共享**：Gateway 写、AgentServer 读。按存档 mtime 判断变化即重载；写操作前强制重读一次，防止用陈旧内存快照覆盖另一进程的更新。
- **绑定**：`session_id`（随机 24 字节 urlsafe）关联 `user_id`（华为账号 openid）。**同一账号只保留一个会话**，重新登录会顶掉旧会话。
- **生命周期**：会话 TTL 30 天；`id_token` 约 1 小时一换，剩余不足 5 分钟时后台续期（按 user_id 加锁合并并发续期）；登出即删除会话。

### HTTP 会话标识

登录态标识同时走两条通道（`channel_manager/web/web_http_auth.py`）：

- Cookie `jiuwenswarm_auth`（httponly、samesite=lax、30 天）——浏览器场景。
- 请求头 `X-Auth-Session`——桌面 WebView / TUI 场景，优先于 cookie。

登录、认领、取消、登出等 POST 路由必须带 `X-Jiuwen-Auth: 1` 头，用于拦截跨站简单请求。WebSocket 连接在**握手时读一次**会话标识（头 → cookie → query），之后整条连接沿用这份值；消息体里的同名键一律剔除（见[安全设计原则](#安全设计原则)）。

### APIG 模型通道

登录后可用的免费模型经 APIG 接入：`id_token` 以 `Authorization: Bearer` 出示，验签由 APIG 自定义认证器完成。模型列表用 `id_token` 实时发现（非配置出来），经远端配置的白 / 黑名单过滤后缓存 30 分钟；列表条目的 `api_key` 恒为空串，真实凭据按请求注入（见下节）。

---

## 免费模型凭据链路

免费模型的凭据全程不落配置、不落日志，链路如下：

```text
浏览器登录（OAuth2 + PKCE）
   │ id_token + refresh_token
   ▼
AuthSessionStore（AES-256-GCM 落盘）
   │ Gateway 转发时服务端回填 params._model_auth
   │   = { api_base, api_key = id_token, credential_ref }
   ▼
AgentServer 登记表（进程内存，句柄 → 真 token）
   │ httpx 钩子在请求发出前注入 Authorization: Bearer <id_token>
   ▼
APIG（自定义认证器验签）
```

### 占位句柄

模型配置里的 `api_key` 是稳定占位值 `jiuwen-login:<ref>`。`ref` 是凭据句柄：以会话存储主密钥派生的密钥对 `jiuwen-login-credential:<user_id>` 做 HMAC-SHA256 取前 32 位 hex。**句柄按账号而非会话计算**——重新登录后句柄不变，正在跑的请求能等到新 token；且句柄是单向 HMAC，出现在配置、日志、落盘文件中均无泄露风险。

### 请求级透传

`auth/passthrough.py` 的 `normalize_model_auth()` 在 Gateway 转发每条消息前执行：

1. **无条件删除**客户端传入的 `_model_auth` 键——否则前端自己塞一份就能指定"用哪个地址、带哪个头"。
2. 仅当同时满足：有会话 id、APIG 配置存在、请求的模型是登录来源、登录会话可用——才由服务端回填 `{api_base, api_key, credential_ref}`。
3. 放置超过 1 小时的会话在转发前先同步续期（上限 20 秒），避免恢复使用后第一条消息 401。

### AgentServer 侧登记与注入

`auth/login_credentials.py`（在 AgentServer 进程执行）：

- 收到带 `_model_auth` 的请求即登记 / 刷新句柄；登记表纯进程内存，2 小时未被使用即清除。
- 模型配置只含占位值 `jiuwen-login:<ref>`；httpx 请求钩子在请求发出前把它替换为登记表中的真 token。**fail-closed**：句柄无效、或目标地址不在登记时的接入点之下，直接删除 `Authorization` 头放行——占位值与真 token 都绝不外发到陌生地址。

### 续期与撤销

- AgentServer 后台每 60 秒扫描：最近 20 分钟在用、且离过期不足 15 分钟的句柄，经 server_push 发 `auth.credential.refresh` 事件请 Gateway 续期（重试间隔 2 分钟）。
- Gateway 按句柄反查会话、续期，把新 token 经 `auth.credentials.update` 推回；会话已登出或续期失败则推 `{revoked: true}`，AgentServer 撤销登记。登出时 Gateway 同样主动推送撤销。

### 会话属主

IM / cron 渠道的请求没有登录会话，但对话可能在用免费模型。`auth/session_owners.py` 记录"对话会话 → 凭据句柄"（明文落盘 `session_owners.json`，TTL 30 天、上限 2000 条，句柄本身单向无泄露风险）：只在 Gateway 自己挂上凭据时记录，不收调用方传值。之后无登录会话的请求按属主找回凭据；找不到时向客户端下发"需要登录"提示，而不是放行。

---

## 渠道接入鉴权

### Web 渠道（Origin 校验与握手鉴权）

WebChannel 的 WebSocket 升级经过两道可选闸门（`channel_manager/web/web_channel_app.py`）：

- **Origin 校验**（`common/security/ws_origin.py`）：`JIUWENSWARM_ENABLE_ORIGIN_CHECK=1` 启用（默认关）。允许的 hostname 列表由 `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS`（逗号分隔）给出，列表含 `none` 时才放行无 Origin 请求；不匹配返回 403。轨迹 HTTP 旁路同规则，且 `Sec-Fetch-Site: cross-site` 直接 403。
- **IAM 握手鉴权**：南向为 AgentOS 路由时，握手请求的 token 交 agent-os IAM 校验（见下），失败返回 401。

### AgentOS IAM 鉴权

`extensions/agentos/agentos_router/agentos_authenticator.py` 把 token 提交 IAM `POST /api/v1/auth/verify`。token 来源优先级：query `?token=` → `Authorization: Bearer` → `X-Token` 头。Gateway **只消费响应中的 `valid`（身份合法性）**，不消费资源级 `authorized`。IAM 鉴权未启用时回落信任握手头 `X-User-Id`。该认证器实现 `extensions/auth/credential_authenticator.py` 的 `CredentialAuthenticator` 接口，经 `set_handshake_auth()` 挂到 Web / TUI 渠道，同一接口也为将来的 API-KEY、SSH 证书预留。

### SSH 渠道（临时密钥）

用户先经 Web / TUI 连上 Gateway 后，可切换到 SSH 渠道（`channel_manager/protocol/ssh/`）。切换时 Gateway 签发一把**短命临时密钥**（`extensions/auth/ssh_key_issuer.py`，默认 ssh-ed25519、TTL 300 秒），公钥指纹连同用户身份注册进内存注册表（`ssh_key_registry.py`，惰性过期）。SSH 服务器关闭密码认证、只接受公钥：指纹已登记且用户名一致才放行；连接的用户身份**只取自注册表命中结果**，不信任客户端声明的用户名。配置在 `channels.ssh.auth`（`enabled` 默认 `false`）。

### Web proxy 频道

三方 Agent Web UI 代理（`channel_manager/protocol/web_proxy/`）按 agent 类型独立监听端口，入口 URL 携带 `?user_id=&token=`。token 交给 IAM 校验；query 声称的 `user_id` 与 token 身份不符返回 403。首跳带 token 的请求 302 重定向到去 token 的同路径并种 httponly cookie（默认 15 分钟空闲过期）。IAM 相关头与 cookie 在转发进 agent 容器前一律剥除。

### IM 渠道（应用凭据与用户白名单）

主要 IM 渠道均用官方 SDK 长连接，以"应用级凭据 + SDK 内建校验"替代 webhook 验签：

- **飞书**：`app_id` / `app_secret` + `encrypt_key` / `verification_token` 建 WebSocket 长连接，事件解密与令牌比对在 lark-oapi SDK 内完成。
- **钉钉**：`client_id` / `client_secret` 建 Stream 长连接；出站 REST 用 appKey/appSecret 换 `x-acs-dingtalk-access-token`。
- **企业微信**：`bot_id` / `secret` 建 WebSocket 长连接。
- **Telegram / Discord / Slack 等**：bot token 长连接或轮询。

应用凭据之上是**用户白名单**：`config.yaml` 中 `channels.<platform>.allow_from` 列出允许的发送者 ID，空列表 = 放行所有人，不匹配的消息直接丢弃（Telegram 回复无权限提示，其余静默）。钉钉额外加固：`allow_from` 格式非法时该应用拒绝启动，而非当成空表放行所有人。IM 消息的 `user_id` 由 Gateway 从平台签名事件中提取，发送者没有注入身份参数的通道。

### Gateway ↔ AgentServer 内部通道

E2A WebSocket 默认不鉴权（本机进程边界）。设置环境变量 `JIUWENSWARM_GATEWAY_INTERNAL_TOKEN` 后，Gateway 建连时携带 `X-Jiuwen-Gateway-Internal` 共享密钥头，AgentServer 据此放行 gateway 专有动作（如 MCP OAuth 回调）。**载荷里的 `channel_id` 字段可伪造，不能作权限判据**。异常断线后客户端保留目标 URI 与鉴权头用于按需重连；显式断开则清除，不再自动重连。

---

## 文件下载令牌

`common/file_download_token.py` 用 HMAC-SHA256 签名的自包含令牌（`base64url(payload).hex(hmac)`）授权文件下载 / 上传，跨进程无需共享内存即可校验：

- **密钥**：环境变量 `JIUWENSWARM_FILE_DOWNLOAD_SECRET`（≥32 字符）优先；否则自动生成并写入工作区 `config/.file_download_secret`（权限 0600）。
- **形态**：普通下载 `{path, sid, exp?}`；Skill 正文图片默认 600 秒过期；受管资产令牌额外绑定 `asset_id` / `size` / `digest`；上传令牌的 `path` 限定在用户目录根之下。
- **校验**：常量时间 HMAC 比对；无 `exp` 的载荷仅接受两个精确的历史 schema（其余一律拒绝）；可选 `session_id` 必须与 `sid` 严格相等。

URL 形态 `/file-api/download?token=...&user_id=...`。注意该 query token 是文件位置令牌，与 IAM 凭据无关——IAM 校验路径显式不接受它。

---

## MCP OAuth 回调

Gateway 侧只有回调入口：WebChannel 端口上的 `GET /mcp/oauth/callback` 把 query 原样转发给 AgentServer（经 E2A `command.mcp` 的 `oauth_callback` 动作，由内部共享密钥头放行，超时 25 秒）。授权码交换、token 缓存与刷新都在 AgentServer 侧完成；回调失败渲染为 400 落地页，不会把失败显示成"授权成功"。

---

## 企业版凭据管理

### A2A 出站凭据

A2A 出站 Agent 的领域记录只存凭据引用 `a2a/outbound/<agent_id>.api_key`，真实凭据进分层 SecretStore（`common/secrets/`）：逻辑键经 `secret_registry.yaml` 路由到 env / file / db，默认后端为 JSON 文件；存储值带 `ENC:v1` 信封，AES-256-GCM 主密钥来自 `JIUWEN_SECRET_MASTER_KEY` 或 `~/.jiuwenswarm/config/.master_key`（0600）。调用时按远端 Agent Card 声明的 security scheme 把凭据注入 header / query / cookie（apiKey、bearer、basic；oauth2 / OIDC 按 bearer 处理），卡片声明需要凭据但没有配置则拒绝调用。落库记录递归脱敏；对外投影只暴露 `has_credential`，不暴露引用本身。

### 管理面 link mTLS 与密钥体系

企业版管理面路由（`/api/v1/*` 模板、配置同步等）由 link mTLS 守卫（`common/security/link_mtls.py`）：模式 off / observe / enforce 由环境变量决定，**默认 off 即无应用层鉴权**，enforce 模式要求 TLS 对端证书 scope 与 binding 头匹配，失败 403。`enterprise/config/manager_config/security/` 另备有一套签密原语（X25519 信封加密 + Ed25519 签名，Gateway / Manager 两端逐字节一致），为配置下发的签名加密预留；当前同步路径按"无签密"处理，传输安全依赖 link mTLS。

---

## 安全设计原则

- **服务端专有键先删后填**：`_model_auth`、`_auth_session` 等参数键在转发前无条件剔除客户端传值，再由服务端按连接属性与会话存储回填。客户端无法通过消息体指定"用哪个地址、带哪个头、以谁的身份"。
- **身份只在信道建立时识别一次**：WebSocket 握手读一次会话标识并挂到连接；SSH 连接身份只认注册表命中结果；IM 发送者身份由 Gateway 从平台签名事件提取。
- **凭据不进配置与日志**：模型配置只见占位句柄；会话存档加密落盘；认证上下文的 repr 强制脱敏；A2A 出站记录落库前递归脱敏。
- **fail-closed**：句柄失效或目标地址不符时删除 `Authorization` 头而非放行占位值；验签失败统一按失败处理；密钥指纹不匹配返回未找到。
- **不回落"当前唯一登录用户"**：没有会话 id 就是未登录，绝不自动借用本机其他登录态——否则无 cookie 的请求可以冒用他人凭据。
- **令牌最小暴露面**：下载 / 上传令牌自包含且短命；web proxy 首 302 即把 token 从 URL 挥发掉换成 cookie。

## 环境变量与配置项

| 变量 / 配置 | 作用 | 默认 |
| --- | --- | --- |
| `JIUWENSWARM_CONFIG_URL` | 远端配置（登录开关、OAuth 参数、APIG 接入点、模型白 / 黑名单）；设为空或 `off` 关闭登录 | 正式环境地址 |
| `JIUWENSWARM_DATA_DIR` | 数据根目录，决定 `infra/auth/` 会话目录位置 | `~/.jiuwenswarm` |
| `JIUWENSWARM_SSL_VERIFY` | 出网请求 TLS 校验开关 | 开 |
| `JIUWENSWARM_ENABLE_ORIGIN_CHECK` | WebSocket / HTTP Origin 校验开关（`1` 启用） | 关 |
| `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` | 允许的 Origin hostname（逗号分隔；含 `none` 时放行无 Origin 请求） | 空 |
| `JIUWENSWARM_FILE_DOWNLOAD_SECRET` | 文件令牌 HMAC 密钥（≥32 字符） | 自动生成 `.file_download_secret` |
| `JIUWENSWARM_GATEWAY_INTERNAL_TOKEN` | Gateway → AgentServer 内部共享密钥头 | 未设置 |
| `JIUWEN_SECRET_MASTER_KEY` | 企业版 SecretStore AES 主密钥 | `~/.jiuwenswarm/config/.master_key` |
| `channels.<platform>.allow_from` | IM 渠道发送者白名单（`config.yaml`） | 空 = 放行所有人 |
| `channels.ssh.auth.*` | SSH 渠道鉴权开关与临时密钥参数（TTL 默认 300 秒） | `enabled: false` |

命令行参数与运行时目录的总表见[命令行与环境变量](../reference/cli-and-environment.md)。

## 非目标

- 不在本地验证 `id_token` 签名——验签归 APIG 自定义认证器。
- 不管理 IAM token 的签发与轮换——token 生命周期归 agent-os IAM，Gateway 只透传与校验。
- 不实现 MCP OAuth 的授权码交换与 token 缓存——归 AgentServer。
- 会话存储不跨机器同步——`.install_key` 每机一把，登录态是本机的。
- 不替代 AgentServer 的权限、工具与沙箱控制。

---

## 返回导航

- [返回文档首页](../../README.md)
- [架构说明](architecture.md)
- [E2A 协议参考](../reference/e2a-protocol.md)
- [配置渠道](../how-to/configure-channels.md)
