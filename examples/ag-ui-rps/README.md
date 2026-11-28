# 猜拳俱乐部 · AG-UI HTML App

纯 HTML/CSS/JavaScript 示例，无需前端依赖或构建。玩家点击石头、剪刀或布，由真实 AI 对手调用 `play_rps` 出拳，浏览器判分并把结果回传给 AI 点评。支持连续回合、比分、对局记录和新开一局。

## 启动

先启动 Gateway 及支持 App 工具装配、单轮上下文和执行状态的 AgentServer，接入方案见 [AG-UI 接入指南](../../docs/zh/AG-UI.md)。在 **Gateway 进程**设置：

```bash
export AG_UI_ENABLED=1
export AG_UI_ALLOWED_ORIGINS=http://127.0.0.1:19000
# 可选：按部署选择模型和项目
# export AG_UI_MODEL_NAME=your-model
# export AG_UI_PROJECT_ID=your-project
```

仅更新 Gateway 不会自动为宿主提供 App 工具能力。宿主需要配置可用模型，并支持客户端工具装配、run_context 和 execution.status；本示例不使用本地随机数伪装 AI 对手。

在仓库根目录启动静态服务：

```bash
python3 -m http.server 19000 --bind 127.0.0.1 --directory examples/ag-ui-rps
```

浏览器打开 **http://127.0.0.1:19000**，填写实际 Gateway 地址（默认 `http://127.0.0.1:19500/api/ag-ui/run`，对应本仓本地验证示例的 WebChannel 端口），点击手势开始。默认无需 token；若部署要求 Bearer 认证，在页面填写。凭据仅保留在当前页面内存中，不写入本地存储。

请通过 HTTP 访问，不要直接打开 `index.html`：Gateway 拒绝 `Origin: null`。如果改用 `http://localhost:19000` 或其他端口，也须把该完整 Origin 加入 `AG_UI_ALLOWED_ORIGINS`（多个 Origin 用逗号分隔）。HTTPS 页面应连接 HTTPS Gateway，避免浏览器阻止混合内容。

### 出现 Failed to fetch

静态服务器只提供页面，仍需要单独运行 Gateway 和真实 AgentServer。页面的 Gateway 地址须使用 **WebChannel 的 HTTP 端口**，不是静态服务端口，也不是 `/tui` 所在的 WebSocket 端口。

环境变量必须在启动 Gateway 的终端设置；修改后需要重启 Gateway。若 AgentServer 已监听 19501，可在停止旧 Gateway 后运行：

```bash
AG_UI_ENABLED=1 AG_UI_ALLOWED_ORIGINS=http://127.0.0.1:19000 \
uv run agent-gateway --agent-server-url ws://127.0.0.1:19501 --host 127.0.0.1 --port 19500
```

可用以下命令检查浏览器跨域预检（不执行模型或工具）：

```bash
curl -i -X OPTIONS http://127.0.0.1:19500/api/ag-ui/run \
  -H 'Origin: http://127.0.0.1:19000' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: content-type'
```

预期返回 `204` 和 `Access-Control-Allow-Origin: http://127.0.0.1:19000`。连接失败表示地址或监听有问题；`403 ORIGIN_NOT_ALLOWED` 表示白名单不匹配；`404` 表示该端口未提供 AG-UI 入口，需检查 `AG_UI_ENABLED=1` 和端口。修正后刷新页面或点“新开一局”。

## 对局流程

1. 玩家点击手势，浏览器把选择保存在私有 `pendingMove` 中。发送给 AI 的消息、state 和 context 均不包含本轮玩家手势。
2. AI 收到当前回合、已完成历史和比分，调用 `play_rps({round, move})`。AI 可以参考过去回合，但不能提前看到当前选择。
3. 页面接收标准 HTTP/SSE 工具事件，等 `RUN_FINISHED` 确认完整批次后才执行。App 校验工具名、参数、回合、手势及一次出拳限制，揭晓双方选择并按规则更新比分。
4. 将全部 tool messages 和最新 state 一起提交到新的 Run；该 Run 不附加新 user message。AI 收到结果后简短点评，然后玩家可开始下一回合。

胜负与比分由 App 决定，模型输出的文字和 state 事件不能改分。重复 toolCallId 在当前页面内幂等；不同调用 ID 在同一回合重复出拳会被拒绝，拒绝结果也回传给 AI。AI 不出拳、网络中断、超时或协议错误会停止当前对局；点“新开一局”使用新 threadId，页面不自动重试不完整运行。

刷新或新开一局会清空内存中的历史和执行记录，不恢复旧线程。此示例面向本地体验，不提供持久化或防止玩家修改浏览器的机制。页面在整条工具回传链结束前锁定下一回合。

## 验证

使用 Node.js 20+ 内置测试工具，无需 npm install：

```bash
node --test examples/ag-ui-rps/game.test.mjs
```

覆盖九种出拳组合、参数拒绝、重复调用、SSE 分块、断流不执行工具，以及完整的工具回传与下一回合。测试模拟 AG-UI 流，真实模型仍需使用上述宿主部署验证。
