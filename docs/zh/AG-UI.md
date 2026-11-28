# AG-UI 接入指南

## 1. Gateway 的职责

应用通过 Gateway 的 `POST /api/ag-ui/run` 发送消息，以 SSE 接收文本和工具调用。Gateway 将 AG-UI 请求转换成 [E2A](reference/e2a-protocol.md)，交给 AgentServer 执行。入口与 WebChannel 共用 HTTP 服务，无需另开端口。

Gateway 负责认证、会话绑定、输入校验与事件转换；AgentServer 负责模型和后端工具执行，应用负责执行自己声明的工具。

Gateway 按调用身份和 `threadId` 关联宿主会话，`runId` 标识一次运行。同一会话的后续请求继续使用原 `threadId`，每轮使用新的 `runId`。

同一线程不能并发运行。Gateway 将会话绑定、消息引用和待返回工具批次保存在工作区的 `agent/.checkpoint/ag_ui.sqlite3`。

## 2. 启用入口

需要部署包含 AG-UI 路由的 agent-gateway，以及支持客户端工具和 `execution.status` 的 JiuwenSwarm。

| Gateway 环境变量 | 用途 |
| --- | --- |
| `AG_UI_ENABLED=1` | 开启接口，默认关闭 |
| `AG_UI_ALLOWED_ORIGINS` | 允许的浏览器 Origin，多个值用逗号分隔 |
| `AG_UI_PROJECT_ID` | 可选，指定宿主项目 |
| `AG_UI_MODEL_NAME` | 可选，指定模型 |
| `AG_UI_ROLE_INSTRUCTIONS` | 可选，设置应用角色指令 |

按[快速开始](tutorial/01-quick-start.md)配置下游连接后，设置上述环境变量并启动 Gateway：

```bash
export AG_UI_ENABLED=1
export AG_UI_ALLOWED_ORIGINS=http://localhost:3000
uv run agent-gateway --port 19000
```

入口复用 WebChannel 的认证；未配置认证时使用匿名身份。有 Origin 的请求必须命中允许列表，`null` Origin 会被拒绝。身份与权限由服务端确定，不能通过应用状态覆盖。

## 3. 发起对话

请求使用 `Content-Type: application/json` 和 `Accept: text/event-stream`。下面是首轮请求体：

```json
{
  "threadId": "counter-thread",
  "runId": "run-1",
  "messages": [{"id": "user-1", "role": "user", "content": "将计数器设为 3"}],
  "tools": [{
    "name": "set_counter",
    "description": "设置计数器",
    "parameters": {"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]}
  }],
  "state": {"counter": 0},
  "context": []
}
```

后续请求保留已同步消息的 ID、角色和顺序，再追加一条文本用户消息，或一整批工具结果；这两类新增输入不能混在同一轮。应用需保存流中生成的 assistant 消息和工具调用，不能只发送最新一句话。

Gateway 校验历史后，仅将新增输入转换为 E2A `chat.send`，不会重复发送整段对话。

`state` 和 `context` 是本轮临时应用数据，在执行及工具恢复时提供给模型，不写入持久对话。`forwardedProps` 映射为宿主 metadata，不自动注入模型，也不能改变身份、渠道或权限。仅更新状态不能启动一轮运行。

## 4. 应用工具闭环

`tools` 声明本轮可用的应用工具。宿主保留已有后端工具；应用工具不能与其重名，也不会复制到子 Agent。每轮重新声明工具，省略或传空数组会移除本接入此前注册的工具。

工具执行按以下步骤完成：

1. 接收 `TOOL_CALL_START / ARGS / END`，保存工具调用 ID、名称和完整参数。
2. 在应用侧执行该批工具，更新应用状态。
3. 使用新的 `runId`，在消息历史后追加全部 `role=tool` 结果，以 `toolCallId` 对应调用 ID，并携带最新 `state`。
4. Gateway 根据保存的工具调用 ID 与宿主中断对应关系，将结果转换为宿主原生中断恢复输入，模型继续回答。

必须一次返回整个等待批次。缺失、重复或未知结果会被拒绝；不能在等待工具时直接追加用户消息。工具结果目前只支持文本。即使下一轮不再声明某工具，已发出的调用仍可返回结果。

## 5. 结束、错误与恢复

流包含 `RUN_STARTED`、文本消息事件、应用工具事件，以及 `RUN_FINISHED` 或 `RUN_ERROR`。等待应用工具也会结束当前运行，应用完成工具后需发起下一轮；它不等于整个对话已经完成。

Gateway 根据宿主 `execution.status` 判断执行成功或进入工具等待，不能仅凭文本结束或连接关闭判断成功。请求校验错误在开始流之前以 HTTP 错误返回，流内失败通过 `RUN_ERROR` 表达。

断线或超时会触发 Gateway 对当前宿主执行的定向取消。取消未确认时，线程仍保持占用，避免重复执行。已经持久化的工具等待点可用于后续恢复，但不支持断线续传 SSE 或自动重放在途执行。重开线程时 Gateway 会核对宿主历史，历史不匹配需处理后再继续。

## 6. 当前边界

当前实现支持 AG-UI 1.0 的文本对话与应用工具闭环，根会话使用普通 Work 模式。后端工具、MCP、技能及同步子 Agent 按宿主配置可用。

不开放 Code、Team、Plan、Goal、后台派发和宿主交互审批界面，也不支持 AG-UI `resume` 输入。需要询问用户时使用普通文本；工具执行若需要新的人工授权，会返回拒绝结果，已有权限规则仍生效。

本文依据两侧当前源码整理，不代表已完成真实模型、浏览器应用与 Gateway 的端到端联调。
