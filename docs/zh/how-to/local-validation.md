# Gateway 本地功能验证指南

在本地用 Mock AgentServer 模拟上下游接口，验证 `agent-gateway` 的基本功能（启动、下游连接、上游 WebSocket 往返）。

- **下游（South）**：AgentServer WebSocket，本指南用 Mock 服务代替真实 AgentServer。
- **上游（North）**：Gateway 内置的 `/tui` WebSocket 路由（帧格式与真实客户端一致）。

> 如需最短的首次运行路径，请先阅读[快速开始](../tutorial/01-quick-start.md)。本文侧重完整验证步骤与排错。

## 0. 前置条件

- Python 3.11 ~ 3.13
- [uv](https://docs.astral.sh/uv/)
- 可访问 gitcode.com（依赖含 git 源）

## 1. 安装

```bash
cd agent-gateway
uv sync
uv run agent-gateway --help
```

依赖说明：`pyproject.toml` 中已声明全部三方依赖；本包不依赖 `workswarm`，可独立安装运行。

若 `uv sync` 因网络失败，可先配置镜像：

```bash
export UV_DEFAULT_INDEX=https://pypi.org/simple
uv sync
```

## 2. 配置

### 2.1 目录隔离（推荐）

```bash
export JIUWENSWARM_HOME="$PWD/.local-home"
```

- 工作区为 `$JIUWENSWARM_HOME/.jiuwenswarm`，首次启动会自动初始化（默认技能、cron 任务文件等），无需手工创建。
- 配置文件默认路径：`$JIUWENSWARM_HOME/.jiuwenswarm/infra/config/config.yaml`。
- 多实例隔离可用 `JIUWENSWARM_DATA_DIR`（部署侧 `--name` 用的就是这个）。

### 2.2 端口与下游地址

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `AGENT_SERVER_URL` | 由 HOST/PORT 拼出 | 下游 AgentServer WebSocket URL，优先级最高 |
| `AGENT_SERVER_HOST` / `AGENT_SERVER_PORT` | `127.0.0.1` / `18092` | 未设置 URL 时用它拼 `ws://host:port` |
| `WEB_HOST` / `WEB_PORT` / `WEB_PATH` | `127.0.0.1` / `19000` / `/ws` | WebChannel（uvicorn）监听地址 |
| `GATEWAY_PORT` | `19001` | `/tui` 路由监听端口 |
| `AGENT_CONNECT_RETRY` / `AGENT_CONNECT_RETRY_INTERVAL` | `20` / `3` | 下游连接重试次数 / 间隔（秒） |

命令行参数 `--agent-server-url` / `--host` / `--port` / `--web-path` 可覆盖对应环境变量。

### 2.3 config.yaml（可选）

文件不存在时 Gateway 会告警并使用默认值，最小验证无需创建。如需显式关闭所有 IM 频道：

```yaml
channels: {}
health_check:
  every: 60
  target: web
```

仅当 `channels` 中出现对应条目时，才会惰性加载 feishu / dingtalk / telegram / discord / slack / wecom 等频道及其依赖。

## 3. 启动下游 Mock AgentServer

示例脚本已随仓提供：[examples/mock-agent-server/mock_agent_server.py](../../../examples/mock-agent-server/mock_agent_server.py)。它连接后回 `connection.ack`，对请求回 Echo，并支持流式分片。

启动（终端 A）：

```bash
uv run python examples/mock-agent-server/mock_agent_server.py 19501
# 输出 MOCK_READY ws://127.0.0.1:19501
```

## 4. 启动 Gateway

终端 B：

```bash
export JIUWENSWARM_HOME="$PWD/.local-home"
export AGENT_SERVER_HOST=127.0.0.1 AGENT_SERVER_PORT=19501
export WEB_HOST=127.0.0.1 WEB_PORT=19500 GATEWAY_HOST=127.0.0.1 GATEWAY_PORT=19502

uv run agent-gateway --port 19500
```

就绪标志（日志）：

- 未出现 `[App] Another Gateway is already serving this workspace` —— 工作区单例锁获取成功
- `[ExtensionManager] 加载 ...` —— 扩展加载
- `Uvicorn running on http://127.0.0.1:19500` —— WebChannel 就绪
- `[Cron] loaded N job(s) from ...` / `background_cleanup: started ...` —— cron 与后台清理启动

## 5. 上游功能验证

客户端示例已随仓提供：[examples/mock-agent-server/gw_client.py](../../../examples/mock-agent-server/gw_client.py)。它经 `/tui` 路由发送 `chat.send`，并打印 Gateway 回推的事件。

```bash
uv run python examples/mock-agent-server/gw_client.py ws://127.0.0.1:19502/tui
```

期望输出（顺序）：

1. `connection.ack`（Gateway 对上游的握手确认）
2. `{"type":"res","id":"req-chat","ok":true,"payload":{"accepted":true,"session_id":"sess_test"}}`
3. `chat.processing_status`（is_processing=true）
4. 3 条 `chat.final`，内容为 Mock 的 `流式-1 / 流式-2 / 流式-3(完)`
5. `chat.processing_status`（is_complete=true）

同时终端 A 应出现 `MOCK_RECV chat.send {...}`，说明上游 → Gateway → 下游链路打通。

再验证一条下游 RPC（`session.list`）：

```bash
uv run python - <<'EOF'
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:19502/tui") as ws:
        await ws.send(json.dumps({"type": "req", "id": "req-list", "method": "session.list", "params": {}}))
        while True:
            frame = json.loads(await ws.recv())
            if frame.get("type") == "res" and frame.get("id") == "req-list":
                print(json.dumps(frame, ensure_ascii=False)); break

asyncio.run(main())
EOF
```

期望：`{"type":"res","id":"req-list","ok":true,"payload":{"sessions":[],"current_branch":"HEAD"}}`，且 Mock 端出现 `MOCK_RECV session.list {}`。

## 6. 停止与清理

- 两个终端 `Ctrl+C`
- 清理本地工作区：`rm -rf .local-home`

## 7. 契约 ST（自动化回归）

### 7.1 独立契约（不依赖 workswarm）

仓库 `st_test/testcases_gateway/` 是 gateway 独立契约 ST：用内置 `StubAgentServer` 顶替真实 AgentServer，不依赖 workswarm、沙箱、元戎，可用于 PR 门禁。

```bash
uv sync --group dev          # 需要 pytest / pytest-cov
uv run python -m pytest st_test/testcases_gateway --no-cov
```

覆盖的契约：

- `/ws` 首帧 `connection.ack`，未知方法在 gateway 本地被拒绝且不下发南向
- 非流式转发：`session.list` 经 E2A 下发并按原 payload 回收
- 流式转发：`chat.send` 的南向分片转成 `chat.delta` / `chat.final` 事件帧
- 南向掉线：返回 `SERVICE_UNAVAILABLE`，不挂死

每个用例使用独立的临时 `JIUWENSWARM_HOME` 与随机 `WEB_PORT` / `GATEWAY_PORT`，可与其他用例并行。默认使用 PATH 中的 `agent-gateway` 入口，也可显式指定：

```bash
AGENT_GATEWAY_BIN="python -m agent_gateway.app_gateway" uv run python -m pytest st_test/testcases_gateway --no-cov
```

### 7.2 workswarm 兼容（真实进程）

`st_test/testcases_integration/` 验证 gateway 与真实 jiuwenswarm 的兼容性：用例自行拉起 `jiuwenswarm-agentserver` 和 `agent-gateway` 两个真实进程，只通过 `/ws` 黑盒验证，不引用 workswarm 内部模块。

```bash
# 环境里需同时安装 workswarm（默认 develop）与本仓
pip install -e /path/to/jiuwenswarm
pip install -e .
python -m pytest st_test/testcases_integration --no-cov
```

覆盖：`connection.status` 的 `agent_ready`、`config.get` 配置透传、会话 `create → list → delete` 生命周期、定时任务 `cron.job.create → get → delete` 落库核对。

真实模型对话默认跳过（外部依赖，避免 PR 门禁抖动），需要时开启：

```bash
# 临时工作区会从 ~/.jiuwenswarm/infra/config 复制模型配置；
# 可用 ST_WORKSWARM_CONFIG_SRC 指定其他配置目录。
ST_WORKSWARM_CHAT=1 python -m pytest st_test/testcases_integration --no-cov
```

`ST_WORKSWARM_HOME` 可指定复用已有工作区，默认每轮建临时目录并隔离。

## 8. 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `ModuleNotFoundError: No module named 'agent_gateway'` | 依赖未安装完全，重新 `uv sync`；或检查是否绕过了项目 venv |
| 启动即报 “Another Gateway is already serving this workspace” | 同一 `JIUWENSWARM_HOME` 已有 Gateway 在跑（`.gateway.lock`），换目录或先停旧进程 |
| 日志出现 `等待 connection.ack 超时` | Mock 未发送 `connection.ack`，`server_ready` 会一直为 False，部分代理入口返回不可用 |
| 个人版访问 `/api/v1/health` 返回 404 | `/api/v1` 系列路由只在企业版挂载；个人版仅在 WebChannel 端口暴露 `/ws`。探活请连 `/ws` 并读首帧 `connection.ack` |
| 下游指向不可达地址时启动卡约 60s 后退出 | 回环地址（`127.0.0.1` / `localhost` / `::1`）会把重试次数强制提升到至少 240 次（约 60s），仍失败则进程非 0 退出。验证“掉线降级”应先连通再断开南向 |
| 同机并行多个 Gateway 报端口占用 | `WEB_PORT`（19000）与 `/tui` 的 `GATEWAY_PORT`（19001）都是固定默认值，多实例需逐个分配 |
| `st_test/testcases_integration` 全部 skip | 环境里没有 `jiuwenswarm-agentserver`；先安装 workswarm develop（`pip install -e /path/to/jiuwenswarm`） |
| `st_test/testcases_integration` 启动 180s 超时 | 真实 AgentServer 首次启动要初始化工作区/MCP 预置包，机器较慢时可能超过默认等待；查看临时工作区下的 `agentserver.log` 与 `gateway-*.log` |
| 端口占用 | 调整 `WEB_PORT` / `GATEWAY_PORT` / Mock 端口 |
| IM 频道未启动 | 属于正常行为：仅 `channels` 中显式启用的频道才会拉起 |
| `uv sync` 很慢或超时 | 首次需拉取 openjiuwen 等 git 源；确认可访问 gitcode，或配置 `UV_DEFAULT_INDEX` 镜像 |

## 附录 A：复用已有 venv 运行

若本地已有安装 gateway 依赖的 venv，可直接复用，无需另建环境：

```bash
JIUWENSWARM_HOME=/tmp/gw-home \
AGENT_SERVER_HOST=127.0.0.1 AGENT_SERVER_PORT=19501 \
WEB_PORT=19500 GATEWAY_PORT=19502 \
PYTHONPATH=/path/to/agent-gateway \
/path/to/venv/bin/python -m agent_gateway.app_gateway --port 19500
```

注意：该 venv 需已安装 gateway 的全部依赖。

---

## 返回导航

- [返回文档首页](../../README.md)
- [快速开始](../tutorial/01-quick-start.md)
- [命令行与环境变量参考](../reference/cli-and-environment.md)
