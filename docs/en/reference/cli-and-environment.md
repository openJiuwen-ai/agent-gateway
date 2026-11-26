# CLI and Environment Reference

This page lists the `agent-gateway` command-line options, environment variables, configuration file, and runtime directories.

## CLI options

Entry points: `agent-gateway` (gateway + channels) and `agent-gateway-acp` (ACP bridge).

| Option | Default | Description |
| --- | --- | --- |
| `-u, --agent-server-url <URL>` | Built from `AGENT_SERVER_URL` or HOST/PORT | Downstream AgentServer WebSocket URL, highest priority |
| `-H, --host <HOST>` | `WEB_HOST` or `127.0.0.1` | WebChannel bind host |
| `-p, --port <PORT>` | `WEB_PORT` or `19000` | WebChannel bind port |
| `--web-path <PATH>` | `WEB_PATH` or `/ws` | WebChannel WebSocket path |
| `--name <name>` | none | Start a named instance from `instances.yaml` |
| `--dotenv <path>` | none | Load environment variables from the given `.env` at startup (startup phase only) |
| `-h, --help` | none | Show help and exit |

## Environment variables

### Downstream AgentServer

| Variable | Default | Description |
| --- | --- | --- |
| `AGENT_SERVER_URL` | unset | Downstream WebSocket URL, highest priority |
| `AGENT_SERVER_HOST` | `127.0.0.1` | Used to build `ws://host:port` when the URL is unset |
| `AGENT_SERVER_PORT` / `AGENT_PORT` | `18092` | Downstream port |
| `AGENT_CONNECT_RETRY` | `20` | Downstream connection retries; forced to at least `240` (about 60s) when targeting a loopback address (`127.0.0.1` / `localhost` / `::1`) |
| `AGENT_CONNECT_RETRY_INTERVAL` | `3` | Retry interval ceiling in seconds; loopback addresses back off exponentially from 0.05s up to 0.25s |

### Upstream entry points

| Variable | Default | Description |
| --- | --- | --- |
| `WEB_HOST` | `127.0.0.1` | WebChannel bind host |
| `WEB_PORT` | `19000` | WebChannel port |
| `WEB_PATH` | `/ws` | WebChannel WebSocket path |
| `WEB_WS_PERMESSAGE_DEFLATE` | Off | Enable permessage-deflate frame compression on the gateway WebSocket. Compression zlib-compresses every frame; on loopback / LAN deployments the CPU cost outweighs the bandwidth saving, so it is off by default. Public-network deployments can set it to `1` to enable |
| `GATEWAY_HOST` | `127.0.0.1` | TUI route bind host |
| `GATEWAY_PORT` | `19001` | TUI route port; a fixed default, assign a distinct port when running several instances on one host |
| `GATEWAY_URL` | unset | Gateway public URL used by some extensions |
| `AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS` | On | Whether the Web (`/ws`) and ACP (`/acp`) entry points forward methods the Gateway has not registered to AgentServer under their original name. AgentServer owns the method set; the Gateway only blocks reserved prefixes and enterprise write-protected methods. Set to `0` / `false` / `no` / `off` to restore the strict "static allowlist only" mode for staged rollout and rollback; `/tui` is not affected by this switch |
| `A2A_SERVER_ENABLED` | disabled | Set to `1` / `true` to enable A2A ingress |
| `A2A_SERVER_HOST` | `127.0.0.1` | A2A bind host |
| `A2A_SERVER_PORT` | `19100` | A2A port |
| `A2A_SERVER_PATH` | `/a2a` | A2A RPC path |
| `A2A_SERVER_CARD_PATH` | `/.well-known/agent-card.json` | Agent Card path |
| `A2A_SERVER_EXTENDED_CARD_PATH` | `/agent/authenticatedExtendedCard` | Extended Agent Card path |
| `A2A_SERVER_PROTOCOL_VERSION` | `1.0.0` | A2A protocol version |

### Workspace and runtime

| Variable | Default | Description |
| --- | --- | --- |
| `AGENT_GATEWAY_HOME` | user home | User home; the instance root is `.jiuwenswarm` under it. Legacy name: `JIUWENSWARM_HOME` |
| `AGENT_GATEWAY_DATA_DIR` | unset | Directly sets the instance root; takes priority over `AGENT_GATEWAY_HOME`. Legacy name: `JIUWENSWARM_DATA_DIR` |
| `JIUWENSWARM_RUNTIME_WORKSPACE_READY` | unset | Internal marker; set by parent processes such as the desktop app to skip duplicate workspace initialization |
| `AGENT_GATEWAY_CONFIG_DIR` | unset | Gateway-only config directory; the file name is always `config.yaml`, defaulting to `<config_dir>/config.yaml`; relative paths resolve against the config directory; must be set before the Gateway process starts. Legacy name: `JIUWENSWARM_GATEWAY_CONFIG_DIR` |
| `BROWSER_PROFILE_NAME` | `jiuwenclaw` | Profile name for browser-related features |

### Health check / heartbeat

| Variable | Default | Description |
| --- | --- | --- |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL` | `health_check.every` from `config.yaml`, else `60` | Check interval in seconds |
| `HEALTH_CHECK_TIMEOUT` | unset | Timeout per check in seconds |
| `HEALTH_CHECK_RELAY_CHANNEL_ID` / `HEARTBEAT_RELAY_CHANNEL_ID` | `health_check.target` from `config.yaml`, else `web` | Channel that receives the result |

### Legacy names

Gateway reads the `AGENT_GATEWAY_*` name first and falls back to the legacy name when it is unset, so existing deployments need no change.

| New name | Legacy name |
| --- | --- |
| `AGENT_GATEWAY_HOME` | `JIUWENSWARM_HOME` |
| `AGENT_GATEWAY_DATA_DIR` | `JIUWENSWARM_DATA_DIR` |
| `AGENT_GATEWAY_CONFIG_DIR` | `JIUWENSWARM_GATEWAY_CONFIG_DIR` |
| `AGENT_GATEWAY_DOWNLOAD_ASSET_ROOT` | `JIUWENSWARM_DOWNLOAD_ASSET_ROOT` |
| `AGENT_GATEWAY_EDITION` | `JIUWENSWARM_EDITION` |
| `AGENT_GATEWAY_SSL_VERIFY` | `JIUWENSWARM_SSL_VERIFY` |
| `AGENT_GATEWAY_ENABLE_ORIGIN_CHECK` | `JIUWENSWARM_ENABLE_ORIGIN_CHECK` |
| `AGENT_GATEWAY_WS_ALLOWED_ORIGIN_HOSTS` | `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` |
| `AGENT_GATEWAY_LOG_MASKING_PROBE` | `JIUWENCLAW_LOG_MASKING_PROBE`, `JIUWENSWARM_LOG_MASKING_PROBE` |
| `AGENT_GATEWAY_FILE_DOWNLOAD_SECRET` | `JIUWENSWARM_FILE_DOWNLOAD_SECRET` |
| `AGENT_GATEWAY_REQUEST_EXT_FORWARD_HEADERS` | `JIUWENSWARM_REQUEST_EXT_FORWARD_HEADERS`, `JIUWENCLAW_REQUEST_EXT_FORWARD_HEADERS` |
| `AGENT_GATEWAY_SUPERVISOR_PID` | `JIUWENSWARM_SUPERVISOR_PID` |

- When a new and a legacy name are both set to different values, path variables (`AGENT_GATEWAY_HOME`, `AGENT_GATEWAY_DATA_DIR`, `AGENT_GATEWAY_CONFIG_DIR`, `AGENT_GATEWAY_DOWNLOAD_ASSET_ROOT`) fail at startup; for other variables the new name wins and a warning is logged once. Blank values always count as unset, so the lookup falls back to the next name and blank values are not compared for conflicts.
- When deployed together with the JiuwenSwarm AgentServer and launcher, keep using the legacy names or set both names to the same value. They only read the legacy names today, so setting only the new name makes the two sides use different data directories or secrets.
- Variables that Gateway writes for the AgentServer (such as `JIUWENSWARM_RUNTIME_PROFILE` and `JIUWENSWARM_AGENTOS_SANDBOX_CONFIG`) keep their legacy names.

## Configuration file

Default path: `$AGENT_GATEWAY_DATA_DIR/config/config.yaml`; when `AGENT_GATEWAY_DATA_DIR` is unset, `$AGENT_GATEWAY_HOME/.jiuwenswarm/config/config.yaml`. The legacy names still work; see “Legacy names” above.

Gateway can point at a dedicated config directory with `AGENT_GATEWAY_CONFIG_DIR`; the file name is always `config.yaml`. A missing file is initialized from the bundled `config.yaml` template, and an existing file is never overwritten. `logging.yaml` is still read from the config directory and does not follow this variable.

Set this variable before the Gateway process starts (a systemd drop-in, the launch command, or an early `--dotenv`). `agent_gateway.common.config` binds the config path at import time, so changing the variable while the process runs is not supported and can make reads and writes disagree.

If the file is missing, configuration reads fall back to Gateway’s bundled template. A standalone start installs that template without overwriting existing configuration. Common settings:

```yaml
channels: {}          # Only IM channels present here are initialized
health_check:
  every: 60           # Health check interval in seconds
  target: web         # Channel that receives the result
```

## Runtime directories

| Path | Content |
| --- | --- |
| `<root>/config/config.yaml` | Gateway / channel configuration |
| `<root>/profile/agent/sessions/` | Session data (written by the AgentServer) |
| `<root>/logs/agent/` | Gateway, AgentServer, and combined logs |
| `<root>/.gateway.lock` | Workspace singleton lock |
| `<root>/profile/agent/home/cron_jobs.json` | Cron job store (maintained by the gateway) |

`<root>` is `AGENT_GATEWAY_DATA_DIR` when set, otherwise `AGENT_GATEWAY_HOME/.jiuwenswarm`; the legacy names still work.

Enterprise deployments mount only `<root>/profile/` on shared PVC/NFS storage; system
paths such as `config/`, `logs/`, `auth/`, and `runtime/` stay in the flat data root and
remain Pod-local. Do not mount the entire data root. Personal deployments use the same
paths as ordinary directories. Runtime logs, including `context_usage.jsonl`, belong in
`logs/agent/` and are collected through stdout in enterprise deployments.

On upgrade from the earlier flat layout, known durable business directories are moved
under `profile/`; their former paths are kept as symlinks when the platform permits.
System directories remain in place. The unreleased intermediate `domain/infra` layout is not a supported upgrade path.

## Exit behavior

| Scenario | Behavior |
| --- | --- |
| Workspace lock already held | Prints `[App] Another Gateway is already serving this workspace` and exits non-zero |
| `--name` instance missing or workspace absent | Fails before startup with a non-zero exit |
| `Ctrl+C` | Shuts down WebChannel, channels, and background tasks, then exits |

---

## Navigation

- [Documentation home](../../README.md)
- [Quick Start](../tutorial/01-quick-start.md)
- [Multiple instances](../how-to/run-multiple-instances.md)
