# Gateway single-process startup compatibility baseline

P0/P1 protects the default single-process runtime, extracts explicit startup and lifecycle boundaries, and adds an explicit log directory option. P2-A also separates the two listener adapters. Neither batch introduces Control, Acceptor or Worker processes or changes chat, login, Cron, or HTTP behavior.

## Source combinations

| Repository | Commit |
| --- | --- |
| agent-gateway official develop | `cad980e61996d8e1d31bb3270a00af014f5bb8ea` |
| jiuwenswarm official develop | `d54c14d053dfd8578f516794ab481959816e28cc` |
| Gateway pinned by that jiuwenswarm commit | `54e5330d3ea6be972aa38de4c42a898ea73fed37` |

Verify standalone Gateway, the mother's pinned combination, and the mother with the candidate Gateway in separate environments. Record the actual imported package locations and direct URLs. Do not change the mother's lockfile merely to run a candidate comparison.

The current validation host is macOS arm64 with Python 3.12.14. Other platforms and frozen desktop packages require their own validation.

## Preserved contracts

- Existing CLI/module/mother entrypoints, arguments and defaults remain compatible.
- Environment loading, third-party logging, Gateway logging, workspace preparation and runtime environment application retain their original order. The parent's READY marker still skips duplicate workspace preparation.
- Importing `startup.bootstrap` does not execute the stages. The legacy entrypoint still calls them at its original import boundaries.
- The existing workspace OS lock remains authoritative. Duplicate instances exit with code 1; normal and exceptional exits release ownership.
- SIGINT/SIGTERM request the same shutdown event; signal handlers are restored and telemetry cleanup runs. Partial signal installation also restores handlers already installed.
- Supervised restart code 75, crash convention 70 and unsupervised restart behavior remain intact.
- Default logging paths, mother-handler reuse, redaction and level updates remain intact.
- Explicit `log_directory` is available for future role assembly. All file handlers use that directory, with no silent shared-directory fallback or in-process switch to another directory.

## P2-A listener resource boundaries

- `transport.uvicorn_listener.UvicornListener` owns the 19000 framework server and lifecycle; `transport.websocket_listener.WebSocketListener` owns the 19001 listener resources. Construction does not bind; start waits for actual readiness; stop waits for complete shutdown. Closed handles cannot restart.
- WebChannel still builds the original FastAPI app and owns business state and cleanup. Its `start()` remains long-running. `ProtocolRuntime.start()` still returns after binding; the original class retains TUI/ACP ACKs and routing.
- Orchestration uses public `WebChannel.wait_ready()` rather than Uvicorn internals. The existing five-second readiness boundary remains; cancelling an auxiliary waiter does not cancel the business owner.
- Adapters do not own process signals. Normal shutdown awaits the framework; startup failures cannot publish false readiness or retain acquired listener resources. Business close code 1001 and the original guarded HTTP/WS configuration remain intact.
- HTTP keep-alive, login, Cron, southbound routing and configuration/storage formats remain unchanged. P2-A is not the complete P2 phase and provides neither a Worker pool nor fd handoff.
- Added regression tests exercise complete HTTP archives, active WebSockets, startup failure/cancellation, port rebinding and same-workspace restarts. Actual candidate results and review fixes are recorded separately in the mother's `F_20261112_1414_gateway-p2-a-listeners`, not in the P0/P1 historical results below.

## Validation

```bash
uv sync --frozen --extra test
uv run --frozen --extra test python -m pytest tests/unit_tests/gateway tests/auth tests/integration/gateway --no-cov
AGENT_GATEWAY_BIN="$PWD/.venv/bin/python -m agent_gateway.app_gateway" \
ST_GATEWAY_CONFIG_SRC="$PWD/agent_gateway/resources/config.yaml" \
.venv/bin/python -m pytest st_test/testcases_gateway --no-cov
```

Set `JIUWENSWARM_HOME` and `JIUWENSWARM_DATA_DIR` to temporary locations before starting pytest, including collection. Real mother compatibility cases live in `st_test/testcases_integration`; model chat is opt-in and a skip is not a passing capability.

The initial broad run stopped while importing third-party `pysbd`, whose invalid-escape SyntaxWarning is promoted to an error by the repository configuration. Comparative runs use command-local `-W 'ignore:invalid escape sequence:SyntaxWarning'`, without changing dependencies or pytest configuration.

The mother's frozen installation failed building native `openjiuwen-computeruse` resources. Limited integration environments omit that package with `--no-install-package openjiuwen-computeruse`; they do not validate Computer Use or a complete distribution.

An early broad command accidentally removed the repository's `--asyncio-mode=auto` when clearing `addopts`, producing 98 failures and 58 errors. This was a validation-command error, not evidence of existing business defects. Final comparisons explicitly retain `--asyncio-mode=auto`; the original records remain available for audit. The mother's develop has also changed parts of its domain/infra layout while Gateway develop retains the partitioned layout; dependency upgrades need explicit configuration/data-path validation.

Real AS compatibility for the old pin, original Gateway develop, and refactored entrypoint each passed 4 cases with 1 model-chat skip. Mother startup/port/dotenv tests passed 34 cases. A same-workspace old-pin → candidate → old-pin sequence successfully started, served `config.get`, and stopped at each step.

## Evidence and rollback

The Gateway unit/auth/integration comparison passed 2358 cases with 2 skips on the original and 2385 cases with 2 skips on the candidate. All 27 added cases passed, with no missing tests, new failures or new skips. Standalone forwarding ST passed 274 cases on each version; the added process lifecycle ST passed 2 cases; the final installed wheel passed 4 forwarding smoke cases. Suites overlap and their counts must not be summed as unique coverage. The full forwarding comparison preceded the logging API patch; that patch was followed by 27 logging/hot-reload/startup cases, real AS integration and wheel smoke checks.

Record source SHAs, lock hashes, imported package provenance, Python/platform, commands, JUnit outcomes and skip/failure reasons outside the repository. No logs, credentials or actual user configuration belong in a commit.

P1 changes no storage schema. Revert entrypoint wiring and helpers in reverse dependency order. Stop the current process before starting the previous version; validate lock release and workspace reuse. This does not migrate live connections or running jobs.

Merging code, enabling multiprocess mode and changing the default remain separate gates. P0/P1 validation does not establish multiprocess business correctness or capacity.

## P2-B: extracting the 19001 protocol runtime

`ProtocolRuntime`, `ProtocolRuntimeConfig`, `RouteConfig`, and their direct protocol helpers now live in
`agent_gateway.routing.protocol_runtime`. The old `agent_gateway.app_gateway` path re-exports the same
classes, not a second implementation. Request/session/client indexes, the ACP bridge and idle finalization,
TUI registration/unregistration, ACKs, and message handling remain in the protocol object. The P2-A
`WebSocketListener` still owns its listener lifecycle.

Importing the new runtime does not run the application entrypoint, prepare a workspace, parse CLI dotenv,
bind ports, or start Auth/Cron. The channel package loads `AcpChannel` and `AcpChannelConfig` exports on
demand to isolate the existing ACP CLI import side effect; package-level imports remain supported. The old
application still imports ACP after its original environment preparation stage. Tests patching the private idle
constant must target the actual defining module rather than an unrelated entrypoint alias.

`tests/unit_tests/gateway/test_protocol_runtime_import_contract.py` checks import compatibility and real random-port
protocol behavior. The original ACP tests retain legacy imports. Validate wheels outside the source directory and
check their site-packages provenance. Set `ST_GATEWAY_CONFIG_SRC` to the repository configuration template for ST
instead of falling back to host configuration.

This batch retains default single mode. It does not implement affinity, fd handoff, or a Worker pool, and does not
change HTTP policy or login/Cron ownership.

The old `GatewayServer` / `GatewayServerConfig` names remain compatibility aliases in
`app_gateway` and `routing.gateway_server`; they reference the same classes, not wrappers.
Internal assembly uses the canonical names.

## P2-C: single-process composition boundaries

`app_gateway._run()` delegates to `application/single.py` with explicit configuration-save, restart, preprocessing, and startup-timing inputs. The existing CLI, environment preparation, workspace lock, signal entrypoint, and default single mode remain unchanged. Application modules do not import the legacy entrypoint or construct live services at import time.

- `application/services.py` assembles one original client, MessageHandler, ChannelManager, Cron and related service graph, retaining extension selection, enterprise storage ordering and provider binding.
- `application/web.py` wires the original WebChannel, handlers, plugins, Git watcher, recommendation callbacks and first-request gate without binding a listener.
- `application/tui_acp.py` wires TuiChannel, the original ACP inbound queue and one ProtocolRuntime after Web readiness. TUI outbound delegation is unchanged; the client subscribes only after Web/TUI registration.
- `application/single.py` coordinates the original stages and shutdown. Existing optional-channel configuration/retry/lifecycle closures remain here temporarily; P2-D is not implemented.
- `application/inbound.py`, `connection.py` and `configuration.py` own existing preprocessing, connection/readiness and configuration helpers. Legacy helper imports remain compatible; private module globals must be patched at their defining module.

The original `_on_config_saved` body remains discoverable in the legacy entrypoint and is passed through a callback factory, not a new Control service. Web binds before dispatch/Cron startup, followed by the existing yield and TUI/ACP setup. No Worker pool, fd handoff or HTTP connection-policy change is enabled.

`tests/unit_tests/gateway/test_application_assembly_contract.py` covers cold imports, explicit delegation, Web stage order/bind failure, single service instances, enterprise storage order, client selection, TUI subscription order, Web first-request gating and service shutdown. Normal shutdown does not imply transactional cleanup for every early startup failure.

