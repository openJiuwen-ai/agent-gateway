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
| `--enable-cron` | off | Enable the supported file-backed personal Cron jobs in `--mode multiprocess`. Jobs that use account login or a non-default project are refused |
| `--check-cron` | off | With `--mode multiprocess --enable-cron`: print the read-only Cron preflight report as JSON and exit |
| `--southbound-routes-file <path>` | none | With `--mode multiprocess`: the static multi-AgentServer trial. The JSON file fixes each named user's loopback AgentServer per Worker slot. It cannot be combined with `--enable-accounts` or `--enable-cron` |
| `-h, --help` | none | Show help and exit |

### Local multiprocess trial

Use `--mode multiprocess --workers 2` to start two Workers on loopback in the personal edition. The default remains single-process. The trial serves Web `/ws`, health probes and data operations over TUI/ACP, and connects to a single `ws://127.0.0.1:<port>` AgentServer. Keep using single mode for account management and optional channels. Cron stays off in the trial unless `--enable-cron` is passed. To serve several local AgentServers instead of one, use the static trial below.

### Static multi-AgentServer trial (routing only)

```bash
agent-gateway --mode multiprocess --workers 2 --southbound-routes-file routes.json
```

`routes.json` is a local, non-secret snapshot that fixes which loopback AgentServer serves each named user. Each Worker keeps its fixed slot and talks only to the targets of that slot. The named user of a connection (the `user_id` query parameter, or `X-User-Id` on TUI/ACP) selects the target. There is no default AgentServer: a connection whose user has no target, and a frame that does not agree with the user of its connection, are closed with `1008` before any handler keeps state for them.

Rules of the file:

- UTF-8 JSON of at most 16 KiB, with every field shown below. Unknown, missing and duplicate fields are refused.
- `manifest.slots` is exactly the slots of `--workers`: `[0]` for one Worker, `[0, 1]` for two, and so on.
- `owner_slot` is the slot the user's fixed affinity selects for that manifest. It is the same rule that picks the Worker of a connection, so compute it instead of choosing it:

```python
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


def owner_slot(user_id: str, workers: int) -> int:
    policy = LocalIdentityPolicy()
    affinity = FixedUserHash(WorkerManifest(tuple(range(workers))), policy)
    identity = resolve_local_identity(f"user_id={user_id}".encode(), (), entry="web", policy=policy)
    return affinity.select(identity)
```

- `target_id`, `physical_id`, `user_id` and `ws_url` are unique per file. `ws_url` is `ws://127.0.0.1:<port>` or `ws://[::1]:<port>`, with an optional path and without credentials, query or fragment. `agent_type` is `jiuwenswarm`. A file holds at most 32 targets.

Example for two Workers:

```json
{
  "schema_version": 1,
  "revision": 1,
  "identity_policy": {"contract_id": "local-v1", "domain": "local"},
  "manifest": {"slots": [0, 1], "config_version": 1, "algorithm": "hrw-sha256-v1"},
  "targets": [
    {
      "target_id": "as-a", "physical_id": "as-a", "generation": 1, "user_id": "alice",
      "agent_type": "jiuwenswarm", "owner_slot": 0, "ws_url": "ws://127.0.0.1:8081/agent"
    },
    {
      "target_id": "as-b", "physical_id": "as-b", "generation": 1, "user_id": "bob",
      "agent_type": "jiuwenswarm", "owner_slot": 1, "ws_url": "ws://127.0.0.1:8082/agent"
    }
  ]
}
```

What a static Worker guarantees:

- **Routing.** A request reaches the target of the user of its connection, and nothing else. There is no discovery, no forwarding between Workers and no failover: the directory changes only by a restart with a new `revision`.
- **Sessions.** Inside a Worker, a session id belongs to one target, whether it is the AgentServer's own id or the client-visible id of an ACP session. A frame that names the session of another target is refused at the socket.
- **Delivery.** An outbound frame reaches the sockets of its owner only: the user of its request, else the user it declares, else the user of its session. A frame with no owner is not delivered.
- **Startup.** A Worker that cannot connect one of its targets stops the whole group with `SOUTHBOUND_UNREACHABLE`, and no listener opens. A directory or role a Worker cannot serve fails with `SOUTHBOUND_CONFIGURATION`.
- **Pushes.** An AgentServer `send_push` frame is delivered only when the target that sent it owns the request id or the session the frame names. A frame sent by another target, or one that names no owned request or session, is dropped.

An AgentServer owns one unsolicited push slot, which belongs to its newest non-RPC WebSocket connection. Whether an AgentServer sends a given push to the connection of this Gateway is a property of that AgentServer. This trial does not claim that a push naming no request or session reaches any user, on the same Worker or on another.

What a static Worker does not serve in this release:

- **Accounts and Cron.** `--southbound-routes-file` cannot be combined with `--enable-accounts` or `--enable-cron`.
- **The personal compatibility of the account-free Worker.** Its native TUI requests, personal Web settings and operations, and its heartbeat proxy are answered from the user directory of one AgentServer. A static Worker has one per named user, so it installs none of them and refuses those requests.
- **Attachments.** `chat.send` with `files`, `media_items`, `attachments` or `documents` is refused: each user has a private AgentServer directory, so the Gateway cannot import files for the user.
- **Other agents and configuration writes.** `agent_type` other than `jiuwenswarm`, and every Gateway configuration change.

### Preview Cron migration

```bash
agent-gateway --mode multiprocess --enable-cron --check-cron --workers 4
```

Use the Worker count you plan to start: each user's jobs belong to one fixed Worker, chosen from that count. The command reads `<root>/profile/agent/home/cron_jobs.json` and the Gateway configuration, prints one JSON report, and exits. It changes no job and starts no listener, Worker or scheduler. `issues` lists each blocking reason and warning by job ID and owner slot, without task text or credential data. Exit status `0` means every enabled job is supported; `1` means an enabled job is unsupported, or the configuration or job file could not be read. `agentserver_checked: false` means model availability and project execution were not checked against AgentServer. A start with `--enable-cron` runs the same check and opens no listener while an enabled job is unsupported.

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
| `AGENT_GATEWAY_HOME` | user home | User home; the instance root is `.jiuwenswarm` under it. |
| `AGENT_GATEWAY_DATA_DIR` | unset | Directly sets the instance root; takes priority over `AGENT_GATEWAY_HOME`. |
| `AGENT_GATEWAY_RUNTIME_WORKSPACE_READY` | unset | Internal marker; set by parent processes such as the desktop app to skip duplicate workspace initialization |
| `AGENT_GATEWAY_CONFIG_DIR` | unset | Gateway-only config directory; the file name is always `config.yaml`, defaulting to `<config_dir>/config.yaml`; relative paths resolve against the config directory; must be set before the Gateway process starts. |
| `BROWSER_PROFILE_NAME` | `jiuwenclaw` | Profile name for browser-related features |

### Health check / heartbeat

| Variable | Default | Description |
| --- | --- | --- |
| `HEALTH_CHECK_INTERVAL` / `HEARTBEAT_INTERVAL` | `health_check.every` from `config.yaml`, else `60` | Check interval in seconds |
| `HEALTH_CHECK_TIMEOUT` | unset | Timeout per check in seconds |
| `HEALTH_CHECK_RELAY_CHANNEL_ID` / `HEARTBEAT_RELAY_CHANNEL_ID` | `health_check.target` from `config.yaml`, else `web` | Channel that receives the result |

### Compatibility

For existing deployments, Gateway still reads the legacy variable names; new deployments should use `AGENT_GATEWAY_*`.

- When both names are set to different values, path variables fail at startup; other variables use the new name and log one warning. Blank values count as unset and fall back to the next name.
- When deployed together with the JiuwenSwarm AgentServer, make sure both sides use the same data directory, file download secret, and other shared values.
- When deployed with another harness, set Gateway's data directory explicitly with `AGENT_GATEWAY_DATA_DIR`. Set any `JIUWENSWARM_*` variables the harness itself uses (for example `JIUWENSWARM_DATA_DIR`, which agent-core reads) only for the harness process, not for Gateway.
- JWS-specific variables that Gateway injects into the AgentServer keep their original names.

## Configuration file

Default path: `$AGENT_GATEWAY_DATA_DIR/config/config.yaml`; when `AGENT_GATEWAY_DATA_DIR` is unset, `$AGENT_GATEWAY_HOME/.jiuwenswarm/config/config.yaml`.

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

`<root>` is `AGENT_GATEWAY_DATA_DIR` when set, otherwise `AGENT_GATEWAY_HOME/.jiuwenswarm`.

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
