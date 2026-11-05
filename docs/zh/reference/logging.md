# 日志参考

本页说明 Gateway 的日志级别配置与热路径日志契约。

## 日志文件与配置

网关组件日志写入 `gateway.log`（滚动文件）；组件按 logger 名划分，另产出 `channel.log`、`agent_server.log`、`full.log`、`permissions.log`。

日志级别在 `config.yaml` 的 `logging` 段配置：

| 键 | 作用 | 默认 |
| --- | --- | --- |
| `level` | 所有组件的基础级别 | `INFO` |
| `console_level` | 控制台（stderr）输出级别 | 同 `level` |
| `gateway` | 网关组件日志文件级别 | 同 `level` |
| `channel` | 渠道组件日志文件级别 | 同 `level` |
| `agent_server` | AgentServer 组件日志文件级别 | 同 `level` |
| `full` | 汇总日志 `full.log` 的级别（不含网关组件） | 同 `level` |

环境变量 `LOG_LEVEL` 只覆盖**控制台**级别，不改变组件日志文件的级别。

## 热路径日志契约

网关的请求派发与流式转发是每请求 / 每 chunk 级别的热路径。为避免日志本身成为吞吐瓶颈，这些路径上的日志已收敛到 **DEBUG**。默认 `INFO` 下：

- **仍保留 INFO**：每条流式任务的「Stream 正常完成 / 被取消」一条（含 `total_chunks`）；错误与异常（ERROR / EXCEPTION）；南向 AgentServer 发送失败（WARNING）；以及登录失效提示（同一 session 60 秒内最多一条）。
- **降为 DEBUG**：请求入站与派发（`dispatch`）、E2A 出入站方向行（`[E2A][norm]` / `[E2A][out]`）、流式任务启动与逐 chunk 进度、`is_processing=false` 补发、ws 注册、`send()` 的逐帧诊断等。

排障需要这些明细时，把 `config.yaml` 中 `logging.gateway` 调为 `DEBUG`。注意 `LOG_LEVEL` 只影响控制台，不会打开组件日志文件的 DEBUG 记录；DEBUG 会恢复全部热路径日志，峰值流量下日志量很大，建议只在排障窗口临时开启。

### 接线现状（独立部署）

`setup_gateway_logger` 只配置 `gateway` 这棵 logger 树。当前仅 `app_gateway` 使用
`logging.getLogger("gateway")`；`message_handler`、`routing/agent_client`、
`web/web_connect`、`routing/base_ws_channel` 等模块使用 `getLogger(__name__)`
（即 `agent_gateway.*` 名），独立部署下尚未挂接到该树。因此：

- 这些模块的日志不会写入 `gateway.log` / 组件控制台，`logging.gateway` 调为
  `DEBUG` 目前只恢复 `app_gateway` 一处的热路径明细；「每条流的完成 / 取消
  INFO」等同样暂不可见。
- 该接线缺口为拆仓遗留问题；修复后本契约按上文完整生效。共同安装（母仓
  `setup_logger` 已配置根 logger 树）的场景不受此影响，级别契约照常可用。

## 相关

- [命令行与环境变量](cli-and-environment.md)
- [E2A 协议](e2a-protocol.md) 的日志建议一节
