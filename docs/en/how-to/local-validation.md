# Local Validation Guide

Validate `agent-gateway` locally with a mock AgentServer: startup, downstream connection, and the upstream WebSocket round trip.

- **Downstream (South)**: AgentServer WebSocket; this guide uses a mock service instead of a real AgentServer.
- **Upstream (North)**: the gateway's built-in `/tui` WebSocket route (same frame format as real clients).

> For the shortest first-run path, see [Quick Start](../tutorial/01-quick-start.md). This document focuses on complete validation steps and troubleshooting.

## 0. Prerequisites

- Python 3.11 ~ 3.13
- [uv](https://docs.astral.sh/uv/)
- `gitcode.com` reachable (dependencies include git sources)

## 1. Install

```bash
cd agent-gateway
uv sync
uv run agent-gateway --help
```

All third-party dependencies are declared in `pyproject.toml`; the package does not depend on `workswarm` and can be installed standalone.

If `uv sync` fails because of network issues, configure a mirror first:

```bash
export UV_DEFAULT_INDEX=https://pypi.org/simple
uv sync
```

## 2. Configuration

### 2.1 Workspace isolation (recommended)

```bash
export JIUWENSWARM_HOME="$PWD/.local-home"
```

- The workspace is `$JIUWENSWARM_HOME/.jiuwenswarm` and is initialized on first start (default skills, cron files, and so on); no manual setup is required.
- Default config path: `$JIUWENSWARM_HOME/.jiuwenswarm/config/config.yaml`.
- Use `JIUWENSWARM_DATA_DIR` for data isolation between instances (this is what `--name` uses on the deployment side).

### 2.2 Ports and downstream address

| Variable | Default | Description |
|---|---|---|
| `AGENT_SERVER_URL` | derived from HOST/PORT | Downstream AgentServer WebSocket URL, highest priority |
| `AGENT_SERVER_HOST` / `AGENT_SERVER_PORT` | `127.0.0.1` / `18092` | Used to build `ws://host:port` when the URL is unset |
| `WEB_HOST` / `WEB_PORT` / `WEB_PATH` | `127.0.0.1` / `19000` / `/ws` | WebChannel (uvicorn) listen address |
| `GATEWAY_PORT` | `19001` | `/tui` route listen port |
| `AGENT_CONNECT_RETRY` / `AGENT_CONNECT_RETRY_INTERVAL` | `20` / `3` | Downstream connect retries / interval in seconds |

The CLI options `--agent-server-url` / `--host` / `--port` / `--web-path` override the corresponding environment variables.

### 2.3 config.yaml (optional)

When the file is missing, the gateway warns and uses defaults, so it is not required for a minimal validation. To explicitly disable all IM channels:

```yaml
channels: {}
health_check:
  every: 60
  target: web
```

A channel (feishu / dingtalk / telegram / discord / slack / wecom, and so on) is loaded lazily only when its entry exists under `channels`.

## 3. Start the mock AgentServer

The example ships with the repository: [examples/mock-agent-server/mock_agent_server.py](../../../examples/mock-agent-server/mock_agent_server.py). It replies with `connection.ack` on connect, echoes requests, and supports streamed chunks.

Start it (terminal A):

```bash
uv run python examples/mock-agent-server/mock_agent_server.py 19501
# Output: MOCK_READY ws://127.0.0.1:19501
```

## 4. Start the gateway

Terminal B:

```bash
export JIUWENSWARM_HOME="$PWD/.local-home"
export AGENT_SERVER_HOST=127.0.0.1 AGENT_SERVER_PORT=19501
export WEB_HOST=127.0.0.1 WEB_PORT=19500 GATEWAY_HOST=127.0.0.1 GATEWAY_PORT=19502

uv run agent-gateway --port 19500
```

Ready markers in the log:

- No `[App] Another Gateway is already serving this workspace`: workspace singleton lock acquired
- `[ExtensionManager] 加载 ...`: extensions loaded
- `Uvicorn running on http://127.0.0.1:19500`: WebChannel is up
- `[Cron] loaded N job(s) from ...` / `background_cleanup: started ...`: cron and background cleanup started

## 5. Verify the upstream path

The example client ships with the repository: [examples/mock-agent-server/gw_client.py](../../../examples/mock-agent-server/gw_client.py). It sends `chat.send` through the `/tui` route and prints events pushed back by the gateway.

```bash
uv run python examples/mock-agent-server/gw_client.py ws://127.0.0.1:19502/tui
```

Expected output, in order:

1. `connection.ack` (gateway handshake confirmation to the client)
2. `{"type":"res","id":"req-chat","ok":true,"payload":{"accepted":true,"session_id":"sess_test"}}`
3. `chat.processing_status` (`is_processing=true`)
4. Three `chat.final` frames carrying `流式-1 / 流式-2 / 流式-3(完)`
5. `chat.processing_status` (`is_complete=true`)

Terminal A should also print `MOCK_RECV chat.send {...}`, proving the client → gateway → AgentServer path works.

Then verify a downstream RPC (`session.list`):

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

Expected: `{"type":"res","id":"req-list","ok":true,"payload":{"sessions":[],"current_branch":"HEAD"}}`, and the mock side prints `MOCK_RECV session.list {}`.

## 6. Stop and clean up

- Press `Ctrl+C` in both terminals
- Remove the local workspace: `rm -rf .local-home`

## 7. Contract ST (automated regression)

### 7.1 Standalone contracts (no workswarm)

`st_test/testcases_gateway/` is the gateway's standalone contract ST: it replaces the real AgentServer with the built-in `StubAgentServer` and does not depend on workswarm, sandboxes, or YuanRong, so it can gate pull requests.

```bash
uv sync --group dev          # needs pytest / pytest-cov
uv run python -m pytest st_test/testcases_gateway --no-cov
```

Covered contracts:

- First `/ws` frame is `connection.ack`; unknown methods are rejected locally and never forwarded downstream
- Unary proxying: `session.list` is sent as an E2A request and its payload returned unchanged
- Streaming proxying: `chat.send` downstream chunks become `chat.delta` / `chat.final` event frames
- Downstream loss: requests return `SERVICE_UNAVAILABLE` instead of hanging

Each case uses its own temporary `JIUWENSWARM_HOME` and random `WEB_PORT` / `GATEWAY_PORT`, so cases can run in parallel. The `agent-gateway` entry on PATH is used by default; you can override it:

```bash
AGENT_GATEWAY_BIN="python -m agent_gateway.app_gateway" uv run python -m pytest st_test/testcases_gateway --no-cov
```

### 7.2 workswarm compatibility (real processes)

`st_test/testcases_integration/` validates the gateway against a real jiuwenswarm: each run starts a real `jiuwenswarm-agentserver` plus `agent-gateway` and verifies everything as a black box through `/ws`, without importing workswarm internals.

```bash
# The environment needs both workswarm (develop by default) and this repository
pip install -e /path/to/jiuwenswarm
pip install -e .
python -m pytest st_test/testcases_integration --no-cov
```

Covered: `agent_ready` from `connection.status`, config passthrough via `config.get`, the session `create → list → delete` lifecycle, and cron job persistence through `cron.job.create → get → delete`.

Real-model chat is skipped by default (external dependency, keeps the PR gate stable). Enable it when needed:

```bash
# The temporary workspace copies model config from ~/.jiuwenswarm/config;
# point ST_WORKSWARM_CONFIG_SRC elsewhere to use a different config directory.
ST_WORKSWARM_CHAT=1 python -m pytest st_test/testcases_integration --no-cov
```

`ST_WORKSWARM_HOME` reuses an existing workspace; by default each run creates an isolated temporary one.

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `ModuleNotFoundError: No module named 'agent_gateway'` | Dependencies are incomplete; run `uv sync` again, or check that you are not bypassing the project venv |
| Startup reports "Another Gateway is already serving this workspace" | Another gateway is running with the same `JIUWENSWARM_HOME` (`.gateway.lock`); use another directory or stop the old process |
| Log shows `等待 connection.ack 超时` | The mock did not send `connection.ack`, so `server_ready` stays false and some proxy entries are unavailable |
| `/api/v1/health` returns 404 in the personal edition | The `/api/v1` routes are mounted only in the enterprise edition; the personal edition exposes just `/ws` on the WebChannel port. Probe readiness by connecting to `/ws` and reading the first `connection.ack` |
| Startup stalls for ~60s then exits when downstream is unreachable | For loopback addresses (`127.0.0.1` / `localhost` / `::1`) retries are forced to at least 240 (about 60s); the process then exits non-zero. To test the degraded path, connect first and then drop the downstream |
| Port conflicts when running several gateways on one host | `WEB_PORT` (19000) and the `/tui` `GATEWAY_PORT` (19001) are fixed defaults; assign a distinct port per instance |
| All `st_test/testcases_integration` cases are skipped | `jiuwenswarm-agentserver` is missing; install workswarm develop first (`pip install -e /path/to/jiuwenswarm`) |
| `st_test/testcases_integration` times out after 180s | The first real AgentServer start initializes the workspace and MCP presets and can be slow; check `agentserver.log` and `gateway-*.log` in the temporary workspace |
| Port already in use | Change `WEB_PORT` / `GATEWAY_PORT` / the mock port |
| IM channels not started | Expected: only channels explicitly enabled under `channels` are started |
| `uv sync` is slow or times out | The first run pulls git sources such as openjiuwen; make sure gitcode is reachable or configure `UV_DEFAULT_INDEX` |

## Appendix A: Run with an existing venv

If a venv with the gateway's dependencies already exists, reuse it instead of creating a new one:

```bash
JIUWENSWARM_HOME=/tmp/gw-home \
AGENT_SERVER_HOST=127.0.0.1 AGENT_SERVER_PORT=19501 \
WEB_PORT=19500 GATEWAY_PORT=19502 \
PYTHONPATH=/path/to/agent-gateway \
/path/to/venv/bin/python -m agent_gateway.app_gateway --port 19500
```

The venv must already have the gateway's dependencies.

---

## Navigation

- [Documentation home](../../README.md)
- [Quick Start](../tutorial/01-quick-start.md)
- [CLI and environment reference](../reference/cli-and-environment.md)
