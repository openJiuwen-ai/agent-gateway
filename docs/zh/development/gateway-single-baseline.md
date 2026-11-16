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
