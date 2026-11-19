# Runtime 企业修复说明

本分支针对 PR65 的确定性遗留问题做最小修复，不改变会话共享/恢复权限策略，也不改变存量租户 ID 编码。

## 行为变化

- 企业模式下，所有需要 Runtime 会话路由的请求均使用可信 `channel_context.routing` 与用户身份镜像。`params`、`agent_ref` 等不再作为企业租户身份兜底；可信身份缺失返回 `FORBIDDEN`。个人版原有兜底保持。范围是 `identity_from_envelope` 覆盖的全部会话方法（`session.*`、`history.*`、`path.*`、`chat.*` 等）；不经过会话路由的 routeless 管理请求（启动期 `initialize`、`agent.reload_config` 等）与反向 RPC 回包仍按其既有路径处理，不要求三元组。
- 企业聊天准入缺少模块或 `instance_agent_resource` 仓储时返回 `SERVICE_UNAVAILABLE`，不再跳过 enabled、expiry、match 策略。
- 显式 `GATEWAY_AGENT_CLIENT_MODE=runtime` 时，Runtime 扩展缺失/加载失败不再回落 WebSocket。与配置选择的 AgentOS/ADX 冲突、重复注册南向客户端均启动失败。
- Runtime 能力使用显式 `agent_client_mode = "runtime"` 标记，Web 配置分支不依赖类名字符串。
- router 客户端类型统一由 `agent_client_mode.ROUTER_CLIENT_TYPES` 维护，Runtime 冲突校验与 SSH 渠道准入共用，新增 router 类型只改一处。
- 同一扩展实例重复注册南向客户端保持幂等；不同客户端扩展重复注册仍启动失败。
- HTTP 与 WebSocket 均按可信 bot 查询 `packages_for_bot`，分别编码为 HTTP 头和信封字段；客户端提供的专家包名单不作为来源。企业出站缺少可信 bot 时记 warning 后按空名单继续：下游空名单拒绝受控专家包、只放行用户自有包，因此这是失败关闭而非放行。
- Runtime Manager 返回无业务错误码的 HTTP429（包括空体、非JSON体）时允许路由重试；明确的 fatal 业务码仍优先。
- 反向 RPC 的精确 session/JSON-RPC ID 匹配优先；兼容 fallback 仅接受唯一候选，歧义时拒绝且保留所有其他会话记录。歧义拒绝不按 Pod 去重放行：JSON-RPC ID 仅在会话内唯一，同一 Pod 上的不同会话仍无法确定回包归属。错误信息携带 `jsonrpc_id` 与候选数量，日志列出候选 Pod 便于定位 `session_id` 口径漂移。

## 部署注意

企业 runtime 部署必须装配资源仓储并提供完整可信 group/bot/user 身份。此前依赖“缺仓储放行”或业务参数填身份的异常配置会被拒绝，这是有意的安全收紧。准入仓储缺失时所有 `chat.*` 会在第一跳返回 `SERVICE_UNAVAILABLE`，且没有降级开关：若 Gateway 存储装配失败或晚于首个聊天请求（多副本冷启动、DB 连接慢），现象是聊天整体不可用，需按启动日志 `enterprise Gateway storage wiring failed` / `Agent admission repository is unavailable` 排查后重启。未新增 Gateway 对所有会话操作的资源准入策略，也未宣称本分支解决了下游会话所有权检查。

企业版非 Web 入口（IM 管道、cron、CLI/TUI、Gateway 内部构造）必须在进站时写入可信 `channel_context.routing`（`group_id`/`bot_id`）与 `user_id`，否则第一条会话方法就会返回 `FORBIDDEN`。这是本次身份收敛的部署前提，不是运行期可回落项。

## 未纳入

- `chat.swarmflow_reply`：核对 AgentServer `jiuwenswarm` develop `21119c03d305c3e26a59a532e172edeb0ac39eb6` 的 HTTP 注册与专用路径声明，未找到该方法端点。不能在 Gateway 单边猜 URL；需 AgentServer 配套实现及契约测试。
- 业务幂等/跨Pod去重、origin TTL、Pod锁淘汰、统一deadline、保活退避、最终租户ID/迁移、权限矩阵，仍独立跟踪。
- 未进行真实企业Pod联调；HTTP429使用 `httpx.MockTransport` 测试，仓储和包查询以外部依赖替身测试。

## 验证

使用 Python 3.13，通过 `uv sync --frozen --extra test` 创建锁定依赖环境。定向测试可运行：

```bash
JIUWENSWARM_HOME="$PWD/.test-home" \
JIUWENSWARM_DATA_DIR="$PWD/.test-home/.jiuwenswarm" \
.venv/bin/python -W ignore::SyntaxWarning -m pytest --no-cov -q \
  tests/unit_tests/extensions \
  tests/unit_tests/gateway/test_agent_client_mode.py \
  tests/unit_tests/gateway/test_runtime_client_assembly.py \
  tests/unit_tests/gateway/test_app_gateway_acp.py \
  tests/unit_tests/gateway/test_app_web_handlers.py \
  tests/unit_tests/gateway/test_package_allowlist_forward.py \
  tests/unit_tests/gateway/test_enterprise_web_config.py
```

隔离目录用于避免测试模块导入阶段写真实用户运行目录。`SyntaxWarning` 过滤用于现有第三方 pysbd 的非法转义警告，不修改依赖或关闭业务异常断言。
