# Gateway 单进程重构边界

PR #108 将 `a8f694a7fb8c1b60438bdbdc5807bd3d14cb63f6` 重建到 develop
`84baf62290d8598fbcb9b8240c2035e64479da6d`。默认仍为单进程，不启用 Worker 池、
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
- `web.py` 装配原 WebChannel、handler、插件、Git watcher、推荐回调和首次请求门闩，不绑定监听。
- `tui_acp.py` 在 Web ready 后创建 TuiChannel、ACP 队列与唯一 ProtocolRuntime，通道注册后才订阅 client。
- `inbound.py`、`connection.py`、`configuration.py` 保留预处理、重试/就绪和配置 helper；
  旧导入兼容，patch 私有变量时应指向实际定义模块。
- `single.py` 在 dispatch/Cron 前启动 Web，保留配置轮询启停顺序和 TUI/ACP 装配前的原让步阶段。

仅 P2-C 中间提交暂留内联可选渠道闭包，下一阶段 P2-D 再抽取。装配测试位于
`test_application_assembly_contract.py`，真实进程停启/锁测试位于
`st_test/testcases_gateway/test_gateway_single_lifecycle.py`。
正常退出可清理不意味着任意早期启动失败都具备事务式清理。

## P2-D：可选渠道生命周期

`application/optional_channels.py::OptionalChannels` 拥有 IM、SSH、A2A、Web Proxy 配置、
实例和任务。single 显式传入依赖，保留 SDK 延迟导入、单渠道失败隔离及 revision 感知重试。
退出分三段：配置/重试任务、A2A、TUI/Web 后的其他渠道；等待后台取消任务结束。
预热仍属于 host，WebChannel.stop 仍拥有 Web Proxy 清理。

`test_optional_channels_lifecycle.py` 验证资源归属、重试与清理。真实账号、SDK 和冻结包仍需
单独验收；继承的 AgentOS Web Proxy 路径引用了缺失的 settings helper，受控测试不能证明
真实代理启动成功。

## P3-A：本地凭据访问

`CredentialAccess`、`CredentialUpdateSink` 暴露最小不可变值，repr 不含 token。
`assemble_services` 注入延迟 AuthService provider、LocalCredentialAccess，以及绑定同一
MessageHandler 的 sink。登录 HTTP handler 与 Web/TUI/ACP 入站共享依赖。
查找前删除调用方提供的 `_model_auth`，注入路径不能再次执行旧全局注入。
保留过期 token 20 秒刷新等待、仍可用 token 回退，以及业务用户与模型登录身份的区别。

未绑定的旧 helper 与 `register_auth_routes(app)` 保持兼容。退出登录先删除本地会话，再经原
AS 链路尽力通知，不承诺多 AS 可靠吊销。测试包括 `test_local_credential_access.py`、
`test_credential_pipeline_contract.py` 和 `test_auth_dependency_injection.py`。

## P3-B：Cron 与会话凭据绑定

登录来源的 Cron 模型在创建/更新时绑定凭据引用，执行时仅按该引用取凭据，不能按同名模型
借用账号。原 job 格式、调度和投递不变。`SessionLoginBindings` 适配现有
`session_owners.json` 持久化/TTL 与目录检查；保留非 Web 回退范围和登录提示，未登录 Web
请求不能借用记录的账号。绑定先于 MessageHandler 转发，公共凭据接口先于 Cron 调度接入。
未绑定的旧对象保持兼容；这是账号绑定，不是 Worker 归属。

测试包括 `test_session_login_bindings.py`、`test_cron_credential_access.py`、
`test_credential_network_contract.py` 和既有 Cron/登录回归。

## P3-C：配置保存副作用

`application/config_saved_effects.py::ConfigSavedEffectCoordinator` 协调原配置快照、AS reload、
evolution 缓存、预热、浏览器重启、proactive Cron 与主题同步副作用。
`_build_config_saved_callback` 保留薄工厂兼容，`sync_saved_topic_recommendation` 仍可从入口
导入。重试时序、重启决策、请求上下文和后台错误处理不变。

个人版 Web/TUI 仍由目标 AgentServer 持久化并 reload；远端保存成功不能调用协调器或二次
reload。协调器不写配置，不替代 OptionalChannels/CM 更新链。
测试包括 `test_config_saved_effects.py` 与使用真实工厂的 `test_config_save_legacy_worker.py`。

## 验证与回退

使用每个 worktree 独立的新 `.venv` 和 develop 未修改的锁文件。先记录基线，再验证每个
中间提交的定向测试。最终命令保留仓库 pytest 选项：

```bash
uv sync --frozen --extra test
uv run --frozen pytest tests --no-cov
uv run --frozen pytest st_test/testcases_gateway --no-cov
```

collection 前将 `JIUWENSWARM_HOME`、`JIUWENSWARM_DATA_DIR` 指向临时目录。
进程 ST 通过 `AGENT_GATEWAY_BIN`、`ST_GATEWAY_CONFIG_SRC` 选择当前 Python 和仓库模板。
外部账号/模型、真实母仓集成、冻结包和非 macOS 平台是独立门禁，skipped 不代表这些能力通过。

实际命令、结果、跳过/失败原因、源码/锁摘要与包来源保存在仓库外，不复用历史通过数字。
最终树须与验证参考树一致，8 个提交线性排列，不含原大提交或额外 merge。
回退前停止当前进程，按依赖逆序撤回并验证工作区锁与端口复用。
本次不迁移存储 schema 或活连接。
