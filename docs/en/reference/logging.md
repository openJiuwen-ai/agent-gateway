# Logging Reference

This page describes the Gateway's log level configuration and the hot-path logging contract.

## Log files and configuration

Gateway component logs are written to `gateway.log` (rotating file); components are split by logger name and also produce `channel.log`, `agent_server.log`, `full.log`, and `permissions.log`.

Log levels are configured in the `logging` section of `config.yaml`:

| Key | Purpose | Default |
| --- | --- | --- |
| `level` | Base level for every component | `INFO` |
| `console_level` | Console (stderr) output level | Same as `level` |
| `gateway` | Gateway component log file level | Same as `level` |
| `channel` | Channel component log file level | Same as `level` |
| `agent_server` | AgentServer component log file level | Same as `level` |
| `full` | Level for the aggregate `full.log` (excludes the gateway component) | Same as `level` |

The `LOG_LEVEL` environment variable only overrides the **console** level; it does not change component log file levels.

## Hot-path logging contract

Request dispatch and streaming fan-out in the gateway run per request and per chunk. So that logging itself does not become a throughput bottleneck, logs on these paths are converged to **DEBUG**. At the default `INFO` level:

- **Still INFO**: one "Stream completed / cancelled" line per streaming task (with `total_chunks`); errors and exceptions (ERROR / EXCEPTION); southbound AgentServer send failures (WARNING); and the login-expired hint (at most once per session per 60 seconds).
- **Demoted to DEBUG**: inbound requests and dispatch, E2A direction lines (`[E2A][norm]` / `[E2A][out]`), streaming task start and per-chunk progress, `is_processing=false` follow-ups, ws registration, per-frame `send()` diagnostics, and similar.

When troubleshooting needs this detail, set `logging.gateway` to `DEBUG` in `config.yaml`. Note that `LOG_LEVEL` only affects the console and does not enable DEBUG records in component log files; DEBUG restores all hot-path logging, which is very voluminous under peak traffic — enable it only for a troubleshooting window.

### Wiring status (standalone deployment)

`setup_gateway_logger` configures only the `gateway` logger tree. Today only
`app_gateway` uses `logging.getLogger("gateway")`; modules such as
`message_handler`, `routing/agent_client`, `web/web_connect`, and
`routing/base_ws_channel` use `getLogger(__name__)` (i.e. `agent_gateway.*`)
and are not yet attached to that tree in a standalone deployment. Therefore:

- Logs from those modules do not reach `gateway.log` or the component console;
  setting `logging.gateway` to `DEBUG` currently restores only the `app_gateway`
  hot-path detail, and the per-stream completion/cancellation INFO lines are
  likewise not visible yet.
- This wiring gap is a repository-split legacy; once it is fixed, the contract
  above applies in full. Co-installed deployments (where the mother repo's
  `setup_logger` already configures the root logger tree) are unaffected and
  can use the level contract as-is.

## See also

- [CLI and environment variables](cli-and-environment.md)
- The logging guidance section of the [E2A protocol](e2a-protocol.md)
