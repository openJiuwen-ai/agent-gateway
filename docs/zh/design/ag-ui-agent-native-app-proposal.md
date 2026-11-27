# 面向 Agent Native App 的 AG-UI 接入方案

## 价值点

当前，智能体原生 App 若想接入 JiuwenSwarm 的 Agent 能力，需要按照 JiuwenSwarm 的内部 E2A 协议经过 Gateway 发起调用。

引入 AG-UI 标准化协议，可以让独立/第三方 Web/HTML 应用通过标准协议接入 JiuwenSwarm 的智能体能力，降低对内部 E2A 协议和执行框架的耦合。

核心价值包含：
| 核心优势 | 当前现状 | 对 App 小组的直接收益 |
| --- | --- | --- |
| **让 Agent 调用 App 功能形成标准闭环** | 当前可由 App 解析 Agent 返回 JSON 后执行业务动作 | 通过 AG-UI 的标准化工具声明、调用事件和结果回传，不同应用场景工具可复用同一机制 |
| **App 业务状态标准化注入 Agent** | App 自行将场景状态和增量组织为输入 | 通过 AG-UI 标准 state/context 字段传入最新业务状态与上下文 |
| **降低未来引入对接第三方 App 的适配成本** | 第三方 App 接入 JiuwenSwarm 时仍需适配其专有请求与事件。 | 对已支持 AG-UI 的第三方 App，可复用标准交互层接入 App Market，减少针对 JiuwenSwarm 的专有 E2A 协议适配。 |


## 架构设计

```text
第三方/独立 App（新增 AG-UI Client）
    | 
    | AG-UI HTTP POST / SSE：标准化请求与事件
    ↓
Agent Gateway
    └─ 新增 AG-UI 适配：标准请求/事件 ↔ 内部 E2A 协议
    |
    | 内部 E2A 协议
    ↓
JiuwenSwarm AgentServer
    └─ AG-UI 会话复用现有 Agent 执行链，提供 App 工具、业务状态的上下文装配

既有客户端 ── 原有 WebSocket/E2A 入口 ── Gateway ── JiuwenSwarm AgentServer
```

**App 可使用标准 AG-UI 交互契约，Gateway 新增适配，JiuwenSwarm 提供执行能力。**

## 模块设计

按两仓当前实现划分模块及修改范围，Gateway 负责协议适配，JiuwenSwarm 复用现有执行链提供宿主能力。

### agent-gateway

协议模块位于 `agent_gateway/channel_manager/protocol/ag_ui/`。

| 模块 / 修改位置 | 功能 |
| --- | --- |
| HTTP/SSE 入口（`http.py`） | 提供 `/api/ag-ui/run` 入口，处理认证、CORS、Run 生命周期、流式响应和断线取消。 |
| 请求适配（`adapter.py`） | 校验消息增量与完整工具结果批次，将 AG-UI 消息、工具和应用上下文转换为 E2A 请求参数。 |
| 宿主调用（`backend.py`） | 复用 E2A 完成会话创建、历史查询、执行请求和定向取消。 |
| 事件映射（`events.py`） | 将宿主文本、客户端工具请求及执行状态转换为 AG-UI 文本、工具和 Run 生命周期事件。 |
| 映射与并发管理（`store.py`） | 通过 SQLite 保存会话、消息、Run 和工具等待映射，并实现同线程串行与重复 Run 拒绝。 |
| Web 通道集成（`web_channel_app.py`、`app_web_handlers.py`、`message_handler.py`） | 在既有 Web 服务中挂载 AG-UI 路由并接通宿主事件流，保留原有客户端接入路径。 |

### JiuwenSwarm

以下路径相对于 `jiuwenswarm/`，各模块在现有实现上扩展。

| 模块 / 修改位置 | 功能 |
| --- | --- |
| 会话创建与模式校验（`runtime/session_provisioner.py`、`runtime/client_tools.py`） | 将 AG-UI 接入限定为普通 Work 会话，并校验频道一致性及不支持的执行模式。 |
| App 工具装配与结果校验（`runtime/client_tools.py`） | 动态装配 Agent 的 App 工具、保留既有后端工具，并校验工具结果与完整等待批次一致。 |
| 单轮上下文注入（`agents/harness/common/rails/runtime_prompt_rail.py`） | 在模型调用时注入本轮应用状态与上下文，支持工具恢复后的最新状态且不写入持久对话。 |
| Agent 执行与工具恢复（`server/runtime/agent_adapter/interface.py`、`interface_deep.py`） | 复用现有 Agent 执行链和原生检查点完成工具等待与恢复，并输出可靠的执行状态。 |
| 交互与权限约束（`runtime/client_tools.py` 及相关权限、提问和 Plan rails） | 保留已有授权规则，同时屏蔽不支持的宿主交互入口并拒绝需要新增人工确认的操作。 |
| 取消与执行清理（`runtime/service.py`、`server/runtime/agent_adapter/interface_deep.py`） | 按执行 ID 定向停止任务并清理等待点，在执行停止及清理持久化后确认取消结果。 |
| 历史关联与 E2A 传输（`server/runtime/session/`、`server/control/history_service.py`、`server/wire_truncate.py` 等） | 保存并传递执行与工具调用关联信息，供 Gateway 核对历史、恢复工具等待和判断执行结果。 |
