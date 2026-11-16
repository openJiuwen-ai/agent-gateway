# Gateway 单进程启动兼容基线

P0/P1保护默认单进程行为，提取初始化与生命周期边界，并提供显式日志目录；P2-A进一步分离两个监听适配器。尚未增加 Control、Acceptor 或 Worker 进程，也未改变聊天、登录、Cron 或 HTTP 策略。

## 基线与验证组合

| 对象 | 提交 |
| --- | --- |
| agent-gateway 官方 develop | `cad980e61996d8e1d31bb3270a00af014f5bb8ea` |
| jiuwenswarm 官方 develop | `d54c14d053dfd8578f516794ab481959816e28cc` |
| 该 jiuwenswarm 锁定的 Gateway | `54e5330d3ea6be972aa38de4c42a898ea73fed37` |

分别验证 Gateway 独立安装、jiuwenswarm 原锁定组合、jiuwenswarm 加候选 Gateway。候选组合用独立虚拟环境替换 Gateway，显式记录包来源；不改母仓依赖文件或 lock。

Python 3.12.14/macOS arm64 是本轮运行环境。Windows/Linux 和桌面冻结安装包需在各自环境验收，不能由源码检查推断通过。

## 保留的契约

- `agent-gateway`、`python -m agent_gateway.app_gateway`、母仓 Gateway 入口保持原参数与默认值。
- dotenv、第三方日志、Gateway 日志、工作区准备、运行环境应用的调用顺序保持不变。父进程 READY 标志仍跳过工作区重复准备。
- `startup.bootstrap` 本身不执行初始化；旧入口仍在原阶段调用这些函数。
- 唯一工作区锁仍使用原 `GatewayLock`，重复启动退出码为 1，生命周期结束后释放锁。
- SIGINT/SIGTERM 转换为同一 shutdown event，退出时恢复旧 handler 并清理 telemetry。部分信号注册失败也恢复此前已注册 handler。
- supervised restart 退出码 75、原 crash 约定 70 及原非托管 restart 行为保持。
- `setup_gateway_logger()` 默认目录、母仓 handler 复用、脱敏和热更新语义不变。
- 新的 `log_directory` 参数供后续角色装配使用：所有文件写入显式目录；不能静默回退共享目录，不能在同一进程内换到另一目录。

## P2-A：监听资源边界

- `transport.uvicorn_listener.UvicornListener`持有19000的框架server和生命周期；`transport.websocket_listener.WebSocketListener`持有19001的监听资源。构造不bind，start等待真实ready，stop等待关闭完成；同一handle关闭后不复活。
- WebChannel仍构建原FastAPI应用、保留业务状态与清理，`start()`仍是长期协程。`ProtocolRuntime.start()`仍在绑定后返回，TUI/ACP的ACK和路由仍由原类处理。
- 编排通过`WebChannel.wait_ready()`等待真实监听，不再读取Uvicorn私有字段；5秒ready边界保持。取消辅助等待不取消业务所有者。
- 监听适配器不接管进程信号；正常停止等待框架shutdown，启动失败不假ready，已取得的监听资源必须收束。Web业务关闭码1001和原guarded HTTP/WS配置保持。
- 原HTTP keep-alive、登录、Cron、南向路由及配置/存储格式不变。P2-A不是整个P2完成，也不支持多Worker或fd交接。
- 新增回归覆盖真实HTTP完整归档、活WS、启动失败/取消、端口重绑及同workspace停启；实际结果、候选范围和审查修复见母仓`F_20261112_1414_gateway-p2-a-listeners`，与下文P0/P1历史结果分开记录。

## 回归入口

定向门禁覆盖 dotenv/CLI 参数、锁、信号、启动 trace、日志热更新/脱敏和真实 Gateway 停启。业务保护复用原登录、取消、分享、Cron、ACP、Web binding 测试；逐项对照原版与候选版的用例及结果。

```bash
uv sync --frozen --extra test
uv run --frozen --extra test python -m pytest tests/unit_tests/gateway tests/auth tests/integration/gateway --no-cov
```

运行 pytest 前将 `JIUWENSWARM_HOME` 和 `JIUWENSWARM_DATA_DIR` 指向本轮临时目录，防止 collection 阶段导入入口时操作真实用户数据。ST 显式选择当前 Python 和仓库 config 模板：

```bash
AGENT_GATEWAY_BIN="$PWD/.venv/bin/python -m agent_gateway.app_gateway" \
ST_GATEWAY_CONFIG_SRC="$PWD/agent_gateway/resources/config.yaml" \
.venv/bin/python -m pytest st_test/testcases_gateway --no-cov
```

真实母仓 ST 位于 `st_test/testcases_integration`；环境必须安装指定版本 workswarm，模型聊天默认跳过。必须记录实际运行的 AS/Gateway 来源，不以 skipped 作为功能通过。

## 本轮环境限制

- 原始广测在第三方 `pysbd` 导入时因 invalid escape 的 SyntaxWarning 被 pytest 的 error 规则升级而中止。后续前后对照命令追加 `-W 'ignore:invalid escape sequence:SyntaxWarning'`；生产依赖和 pytest 配置未更改。
- 母仓 frozen 安装在 `openjiuwen-computeruse` 的原生构建失败。限定范围的集成环境使用 `uv sync --frozen --no-install-package openjiuwen-computeruse`；此结果不覆盖 Computer Use 或完整发行包验收。
- 早期广测命令使用 `-o addopts=''` 时误清除了仓库的 `--asyncio-mode=auto`，产生 98 failed 和 58 errors。该结果属于执行配置错误，不能归因于旧业务代码。最终对照显式保留 `--asyncio-mode=auto`；原始记录保留用于追溯。
- 母仓原 pin、Gateway develop 原版及入口改造版的真实 AS 契约均为 4 passed、1 模型聊天 skipped；母仓启动/端口/dotenv 为 34 passed。旧 pin→候选→旧 pin 同工作区停启和 `config.get` 回退探针通过。
- 母仓新 develop 已改变部分 domain/infra 路径，Gateway develop 仍保留分区。依赖升级需单独验收实际配置/数据路径；简单连接测试通过不能证明所有部署配置兼容。

## 证据与回退

本轮 Gateway 单元/auth/集成目录对照：原版 2358 passed、2 skipped，候选 2385 passed、2 skipped；新增 27 项通过，无用例丢失、新增失败或新增跳过。独立转发 ST 两版各 274 passed；新增真实停启 ST 2 passed；最终 wheel 安装后转发冒烟 4 passed。各套件有重叠，数量不能相加作为去重后的覆盖总数。默认日志路径的全套转发对照在日志接口补丁前完成；补丁后补跑日志/热更新/启动 27 项、真实 AS 集成与 wheel 冒烟。

每次结果应记录源码 SHA、依赖 lock 摘要、实际包来源、平台/Python、安装组合、命令、JUnit 结果和失败/跳过理由。日志及真实配置不入库。此次证据保存于执行报告指向的工作区外目录。

P1 没有存储格式变更。回退按依赖顺序撤回入口接线与 helper，保留原数据；先停止当前 Gateway，再启动旧版本。维护窗口验证锁释放与工作区复用，不承诺运行任务或活连接在线迁移。

代码合入、显式启用多进程和切换默认是三个独立门禁。P0/P1 完成不表示多进程业务或容量已经验收。

## P2-B：19001 协议运行时提取

`ProtocolRuntime`、`ProtocolRuntimeConfig`、`RouteConfig` 及直接协议辅助逻辑现在定义在
`agent_gateway.routing.protocol_runtime`。旧 `agent_gateway.app_gateway` 继续导出同一份类，
不是另建一套协议实现。原请求/会话/连接索引、ACP bridge 与空闲收尾、TUI 注册/注销、
ACK 和消息循环仍属于该协议对象；监听继续委托 P2-A 的 `WebSocketListener`。

仅导入新协议模块不运行旧入口、不准备工作区、不解析 CLI dotenv、不绑定端口或启动 Auth/Cron。
为隔离既有 ACP CLI 的导入副作用，渠道包的 `AcpChannel`、`AcpChannelConfig` 改为按需导出；
原包级导入用法保留。旧应用入口仍在原环境准备阶段后加载 ACP。私有 idle 常量的测试 patch
应定位实际定义模块，不能修改旧入口别名并期待新模块全局变量变化。

新导入契约及真实随机端口测试位于 `tests/unit_tests/gateway/test_protocol_runtime_import_contract.py`；
原 `test_app_gateway_acp.py` 继续从旧入口导入，验证消费者兼容。安装包需在源码目录外验证实际
site-packages 来源。ST 应设置 `ST_GATEWAY_CONFIG_SRC` 指向仓内配置模板，避免宿主配置回退。

本批仍是默认 single，不实现用户亲和、fd 交接或 Worker 池，不改变 HTTP 策略、登录和 Cron 归属。

旧 `GatewayServer` / `GatewayServerConfig` 名称在 `app_gateway` 和
`routing.gateway_server` 中仅作为兼容别名保留，与新类为同一对象；内部装配使用新名称。

## P2-C：Single 装配边界

`app_gateway._run()`委托`application/single.py`，显式传入配置保存factory、重启动作、消息预处理回调和启动时间信息。`main()`、CLI、环境初始化、组锁和信号入口不变。新模块不反向导入旧入口，也不在导入时创建运行服务。

- `application/services.py`创建同一组南向client、MessageHandler、ChannelManager及原Cron/健康检查等引用，保留扩展选型、企业存储注入和provider绑定。
- `application/web.py`创建原WebChannel，安装原handler、插件、git watcher、推荐推送及首次Web请求门闩，不绑定监听。
- `application/tui_acp.py`在Web优先就绪之后创建TuiChannel、ACP入站队列和唯一ProtocolRuntime；保留TUI出站委托，client订阅发生在Web/TUI注册之后。
- `application/single.py`协调原阶段和退出；可选渠道旧配置、重试和生命周期闭包暂留此处，P2-D尚未实施。
- `application/inbound.py`、`connection.py`、`configuration.py`保存原消息预处理、重试/就绪及配置加载helper。旧helper保留兼容导出，私有模块变量需patch真实定义位置。

原配置保存`_on_config_saved`仍在旧入口源码中，供已有消费者与后续管理接口分批迁移；不是新建Control服务。Web实际监听后才启动CM dispatch和Cron，再保留原让步阶段并装配TUI/ACP。仍默认single，没有Worker池、fd交接或HTTP连接策略变化。

装配契约测试：`tests/unit_tests/gateway/test_application_assembly_contract.py`。包含冷导入、旧入口显式委托、Web启动顺序/绑定失败、单份服务接线、企业存储顺序、客户端选型、TUI订阅顺序、Web首次请求门闩及服务退出顺序。正常停启不代表任意早期启动失败均有事务式清理。

## P2-D 可选渠道生命周期

`application/optional_channels.py::OptionalChannels`收口原IM、SSH、A2A与Web Proxy的配置/实例/task管理。`single.py`通过显式依赖创建它，按原时点启动A2A、安装配置callback并延迟应用初始渠道配置；保留SDK惰性import、单渠道失败隔离及revision失效重试。

退出分三阶段：配置初始化/retry → A2A → TUI/Web之后的其他渠道；prewarm仍由single协调，Web Proxy资源最终仍由WebChannel.stop释放。DingTalk后台取消等待task由组件跟踪并join。不改协议、登录或Cron，不引入多Worker。

生命周期行为测试位于`tests/unit_tests/gateway/test_optional_channels_lifecycle.py`；真实平台账号与冻结发行包需单独验收。既有AgentOS Web Proxy分支导入缺失配置函数的问题仍遗留；替身重配置测试不等于真实启用验证。


