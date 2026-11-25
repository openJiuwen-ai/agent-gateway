# Gateway 单进程重构边界

PR #108 将 `a8f694a7fb8c1b60438bdbdc5807bd3d14cb63f6` 重建到 develop
`7712e35ed0f65ceaadb6b03beaba91c1c4282502`。默认仍为单进程，不启用 Worker 池、
Control/Acceptor、IPC、fd 交接、用户亲和或 Cron 分片，不改变 HTTP 连接策略与配置持久化格式。

## P0/P1：启动与进程生命周期

`startup/bootstrap.py` 提供显式的环境、第三方日志、Gateway 日志与工作区准备阶段。
导入 helper 不执行这些阶段；入口仍按原顺序调用。父进程设置的
`JIUWENSWARM_RUNTIME_WORKSPACE_READY` 继续跳过重复准备，不恢复已删除的媒体能力迁移。

`startup/lifecycle.py` 保留原工作区 OS 锁：重复实例以状态码 1 退出，正常和异常退出释放锁。
SIGINT/SIGTERM 设置同一个退出事件，恢复已安装的信号处理器并等待遥测清理，包括部分信号
安装失败的路径。监督进程重启码 75 与无监督进程重启语义不变。日志保留默认路径、母仓
handler 复用、脱敏和级别更新；显式日志目录不能静默回退或在初始化后切换。

测试位于 `tests/unit_tests/gateway/` 下的 `test_startup_bootstrap.py`、
`test_startup_lifecycle.py`、`test_logging_directory.py`、
`test_gateway_main_contract.py` 和 `test_gateway_shutdown_contract.py`。

## P2-A：监听生命周期

`transport/uvicorn_listener.py` 拥有 Web HTTP/WebSocket 框架服务，
`transport/websocket_listener.py` 拥有 TUI/ACP 监听资源。构造不绑定端口，启动等待真正 ready，
停止等待完整关闭；关闭后的句柄不能重启。适配器不接管进程信号。

WebChannel 仍构建原 FastAPI 应用并拥有业务状态及清理，`start()` 仍长期运行；协议监听的
start 在绑定后返回。装配通过 `WebChannel.wait_ready()` 使用原五秒就绪边界，取消辅助
等待者不取消业务 owner。启动失败不能发布假 ready；关闭码 1001、带保护的 HTTP/WebSocket
协议与压缩默认值保持不变。

WebSocket 监听任务被取消时先关闭已绑定资源，原任务仍保持取消状态；其他等待者收到带原始
原因、可由 `except Exception` 捕获的 `RuntimeError`，不会被无关的 `CancelledError` 取消。
只有 ready 前的普通异常计为启动失败；ready 后的故障由 `wait_closed()` 报告，不改写已成功的
启动结果。已关闭的句柄仍拒绝再次 `start()`。

测试包括 `test_uvicorn_listener.py`、`test_websocket_listener.py`、
`test_gateway_listener_lifecycle.py` 及对应进程 ST，覆盖归档、活连接、取消、绑定失败和端口复用。

## P2-B：协议运行时

`routing/protocol_runtime.py` 拥有 ProtocolRuntime、ProtocolRuntimeConfig、RouteConfig、
ACP bridge 及请求、会话和连接索引。`app_gateway` 与 `routing.gateway_server` 的
GatewayServer、GatewayServerConfig 兼容导出仍是同一类对象的别名。

保留最新 develop 的 TUI 请求关联、会话切换、断线宽限、重连与路由隔离。客户端请求 ID 映射为
唯一内部上游 ID，响应时恢复；旧连接的迟到结果不能投递到替代窗口。新模块和测试使用当前
`e2a` 包，不使用旧 `openjiuwen_protocol` 路径。冷导入不初始化旧入口或提前加载 ACP。

测试包括 `test_protocol_runtime_import_contract.py`、`test_tui_request_correlation.py`、
`test_tui_lifecycle_routing.py` 与原 `test_app_gateway_acp.py` 兼容测试。

## P2-C：单进程装配

`app_gateway._run()` 将配置保存工厂、重启动作、预处理、连接及启动计时依赖显式传给
`application/single.py`。

- `services.py` 创建同一份 client、MessageHandler、ChannelManager 和公共服务，保留扩展
  选择、企业存储初始化与清理、provider 绑定及 AgentOS 配置轮询服务。
- client 选择保持当前 agent-client 模式校验与扩展客户端直连语义；文件型 Cron 迁移使用
  AgentServer 模型目录，不回退本地目录。
- `web.py` 装配原 WebChannel、handler、插件、Git watcher、推荐回调和首次请求门闩，不绑定监听。
- `tui_acp.py` 在 Web ready 后创建 TuiChannel、ACP 队列与唯一 ProtocolRuntime，通道注册后才订阅 client。
- `inbound.py`、`connection.py`、`configuration.py` 保留预处理、重试/就绪和配置 helper；
  旧导入兼容，patch 私有变量时应指向实际定义模块。
- 入站预处理保留 `app_id` 与当前 Web 本地方法路由：强制转发优先，已注册本地 handler
  留在本地，拒绝前缀不外发，未知 Web 方法默认仍可透传。
  `/acp` 复用同一套判定：拒绝前缀回 `METHOD_NOT_ALLOWED`，未登记方法按原方法名透传，
  透传前经过企业版禁写判定；`/tui` 仍只转发已登记方法。
- `single.py` 在 dispatch/Cron 前启动 Web，保留配置轮询启停顺序和 TUI/ACP 装配前的原让步阶段。
  Web 就绪预算默认与进程冷启动预算一致，可用 `GATEWAY_WEB_READY_TIMEOUT_SECONDS` 覆盖。
  `0` 表示不等待，仅已经 ready 的监听可以通过检查。

抽取后，私有 monkeypatch 目标应指向实际定义模块：

| 旧 `app_gateway` 目标 | 新目标 |
| --- | --- |
| `configure_gateway_telemetry` | `agent_gateway.application.services.configure_gateway_telemetry` |
| `_agent_prewarm_enabled` / `_AGENT_PREWARM_EXCLUDED_CHANNELS` | `agent_gateway.application.single` |
| `_stop_web_runtime` / `_wait_for_web_channel_listening` | `agent_gateway.application.connection` |
| `_reject_removed_agent_client_type` | `agent_gateway.application.services` |
| `_build_acp_route_binding` / `_build_route_config_map` | `agent_gateway.application.tui_acp` |
| 入站预处理 helper | `agent_gateway.application.inbound` |

运维检索日志时，TUI/ACP 后台任务名为 `tui-acp-protocol-runtime`。

可选渠道先于单进程装配抽取：先由旧入口完整调用，本阶段再将相同调用迁至 single。装配测试位于
`test_application_assembly_contract.py`，真实进程停启/锁测试位于
`st_test/testcases_gateway/test_gateway_single_lifecycle.py`。
正常退出可清理不意味着任意早期启动失败都具备事务式清理。

## P2-D：可选渠道生命周期

`application/optional_channels.py::OptionalChannels` 拥有 IM、SSH、A2A、Web Proxy 配置、
实例和任务。host 显式传入依赖，保留 SDK 延迟导入、单渠道失败隔离及 revision 感知重试。
退出分三段：配置/重试任务、A2A、TUI/Web 后的其他渠道；等待后台取消任务结束。
预热仍属于 host，WebChannel.stop 仍拥有 Web Proxy 清理。

`test_optional_channels_lifecycle.py` 验证资源归属、重试与清理。
`test_gateway_optional_channels_lifecycle.py` 通过真实进程验证装配抽取前后的 host 启停边界。
AgentOS Web Proxy 使用当前 runtime Cron 配置 helper，测试不再替换缺失的导入。
真实账号、SDK 和冻结包仍需单独验收。

## 验证与回退

使用每个 worktree 独立的新 `.venv` 和 develop 未修改的锁文件。先记录基线，再验证每个
阶段的定向与全量测试。五个 PR 按启动/监听、协议运行时、可选渠道、单进程装配、
凭据/配置副作用顺序合入。每阶段同时包含调用方、测试和中英文文档；下一 PR 基于实际
合入后的 develop 重建并复跑验证。命令保留仓库 pytest 选项：

```bash
uv sync --frozen --extra test
uv run --frozen pytest tests --no-cov
uv run --frozen pytest st_test/testcases_gateway --no-cov
```

collection 前将 `JIUWENSWARM_HOME`、`JIUWENSWARM_DATA_DIR` 指向临时目录。
进程 ST 通过 `AGENT_GATEWAY_BIN`、`ST_GATEWAY_CONFIG_SRC` 选择当前 Python 和仓库模板。
外部账号/模型、真实母仓集成、冻结包和非 macOS 平台是独立门禁，skipped 不代表这些能力通过。

实际命令、结果、跳过/失败原因、源码/锁摘要与包来源保存在仓库外，不复用历史通过数字。
最终树须与验证参考树一致，五阶段线性排列，不含原大提交或额外 merge。
回退前停止当前进程，按依赖逆序撤回并验证工作区锁与端口复用。
本次不迁移存储 schema 或活连接。
