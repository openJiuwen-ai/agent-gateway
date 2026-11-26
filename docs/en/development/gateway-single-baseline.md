# Gateway single-process refactoring boundaries

PR #108 is rebuilt from `a8f694a7fb8c1b60438bdbdc5807bd3d14cb63f6`
on develop `7712e35ed0f65ceaadb6b03beaba91c1c4282502`. The default remains
single-process. This refactoring does not enable Worker pools, Control/Acceptor
processes, IPC, fd handoff, user affinity or Cron sharding, and does not change
HTTP connection policy or persistent configuration formats.

## P0/P1: startup and process lifecycle

`startup/bootstrap.py` exposes explicit environment, third-party logging,
Gateway logging and workspace preparation stages. Importing the helper does not
run these stages; the entrypoint calls them in the original order. The parent's
`JIUWENSWARM_RUNTIME_WORKSPACE_READY` marker still avoids duplicate preparation.
The removed media-capability migration is not restored.

`startup/lifecycle.py` retains the authoritative workspace OS lock. Duplicate
instances exit with code 1; normal and exceptional exits release the lock.
SIGINT/SIGTERM request one shutdown event; installed handlers are restored and
telemetry cleanup is awaited, including partial signal-installation failure.
Supervised restart code 75 and unsupervised restart behavior remain unchanged.
Logging retains default paths, mother-handler reuse, redaction and level updates;
an explicit log directory cannot silently fall back or switch after initialization.

Tests: `test_startup_bootstrap.py`, `test_startup_lifecycle.py`,
`test_logging_directory.py`, `test_gateway_main_contract.py` and
`test_gateway_shutdown_contract.py` under `tests/unit_tests/gateway/`.

## P2-A: listener lifecycle

`transport/uvicorn_listener.py` owns the Web HTTP/WebSocket framework server;
`transport/websocket_listener.py` owns the TUI/ACP listener resources.
Construction does not bind, start waits for readiness, and stop awaits closure.
Closed handles cannot restart. Adapters do not own process signals.

WebChannel still builds the original FastAPI application and owns business
state and cleanup. Its `start()` remains long-running; protocol start returns
after binding. Orchestration uses `WebChannel.wait_ready()` with the existing
five-second boundary, without cancelling the owner when a waiter is cancelled.
Startup failure cannot publish false readiness. Close code 1001, guarded
HTTP/WebSocket protocols and compression defaults are preserved.

WebSocket listener task cancellation closes bound resources and keeps the owner
task cancelled. Other waiters receive a catchable `RuntimeError` with the original
cause, rather than an unrelated `CancelledError`. Only ordinary failures before
readiness count as startup errors. Failures after readiness are reported by
`wait_closed()` without changing the successful startup result; closed handles
still reject another `start()`.

Tests: `test_uvicorn_listener.py`, `test_websocket_listener.py`,
`test_gateway_listener_lifecycle.py` and the corresponding process lifecycle ST.
They cover archives, live connections, cancellation, bind failure and port reuse.

## P2-B: protocol runtime

`routing/protocol_runtime.py` owns ProtocolRuntime, ProtocolRuntimeConfig,
RouteConfig, the ACP bridge and request/session/connection indexes. The old
GatewayServer and GatewayServerConfig exports from `app_gateway` and
`routing.gateway_server` remain aliases to the same classes.

The latest develop TUI correlation, session switching, disconnection grace,
reconnection and routing isolation remain intact. Client request IDs are mapped
to unique internal upstream IDs and restored on replies; an old connection must
not deliver a late result into a replacement window. New modules and tests use
the current `e2a` package, not the retired `openjiuwen_protocol` import path.
Cold imports do not initialize the legacy entrypoint or eagerly load ACP.

Tests: `test_protocol_runtime_import_contract.py`,
`test_tui_request_correlation.py`, `test_tui_lifecycle_routing.py` and the existing
`test_app_gateway_acp.py` compatibility suite.

## P2-C: single-process assembly

`app_gateway._run()` delegates explicit save-factory, restart, preprocessing,
connection and startup-timing inputs to `application/single.py`.

- `services.py` constructs one client, MessageHandler, ChannelManager and shared
  service graph, preserving extension selection, enterprise storage initialization
  and teardown, provider binding and the AgentOS configuration updater.
- Client selection retains the current agent-client mode validation and direct
  extension-client behavior; file-backed Cron migration uses the AgentServer model
  catalog instead of the local catalog.
- `web.py` wires the original WebChannel, handlers, plugins, Git watcher,
  recommendation callbacks and first-request gate without binding a listener.
- `tui_acp.py` constructs TuiChannel, the ACP queue and one ProtocolRuntime after
  Web readiness. Client subscription follows channel registration.
- `inbound.py`, `connection.py` and `configuration.py` retain preprocessing,
  retry/readiness and configuration helpers. Legacy helper imports remain
  compatible; patch private globals at their defining modules.
- Inbound preprocessing preserves `app_id` and the current Web local-method route:
  forced forwarding wins, registered local handlers stay local, denied prefixes
  never leave the Gateway, and unknown Web methods remain forwardable by default.
  `/acp` reuses the same decision: denied prefixes answer `METHOD_NOT_ALLOWED`,
  every remaining method, registered or not, passes the enterprise write-protection
  check first; unregistered methods are then forwarded under their original name,
  and `/tui` still forwards registered methods only.
- `single.py` starts Web before dispatch/Cron and preserves configuration-updater
  start/stop ordering and the existing yield before TUI/ACP assembly.
  The Web readiness budget defaults to the process cold-start budget and can be
  overridden with `GATEWAY_WEB_READY_TIMEOUT_SECONDS`. A value of `0` does not
  wait: only a listener that is already ready passes the check.

Private monkeypatch targets follow the defining module after this extraction:

| Legacy `app_gateway` target | Canonical target |
| --- | --- |
| `configure_gateway_telemetry` | `agent_gateway.application.services.configure_gateway_telemetry` |
| `_agent_prewarm_enabled` / `_AGENT_PREWARM_EXCLUDED_CHANNELS` | `agent_gateway.application.single` |
| `_stop_web_runtime` / `_wait_for_web_channel_listening` | `agent_gateway.application.connection` |
| `_reject_removed_agent_client_type` | `agent_gateway.application.services` |
| `_build_acp_route_binding` / `_build_route_config_map` | `agent_gateway.application.tui_acp` |
| inbound preprocessing helpers | `agent_gateway.application.inbound` |

For operational log searches, the TUI/ACP background task is named
`tui-acp-protocol-runtime`.

Optional channels are extracted before single-process assembly. The legacy
entrypoint owns them first; this stage transfers the same calls to single.
Assembly tests are in
`test_application_assembly_contract.py`; real process restart/lock tests are in
`st_test/testcases_gateway/test_gateway_single_lifecycle.py`. Normal shutdown
does not imply transactional cleanup for every early startup failure.

## P2-D: optional-channel lifecycle

`application/optional_channels.py::OptionalChannels` owns IM, SSH, A2A and Web
Proxy configuration, instances and tasks. The host supplies explicit
dependencies and retains lazy SDK imports, per-channel failure isolation and
revision-aware retries. Shutdown has three boundaries: configuration/retry work,
A2A, then other channels after TUI/Web. Background cancellation waiters are
joined. Prewarming remains in the host; WebChannel.stop owns Web Proxy cleanup.

`test_optional_channels_lifecycle.py` checks ownership, retries and cleanup.
The process test `test_gateway_optional_channels_lifecycle.py` checks the host's
startup and shutdown boundaries before and after assembly extraction. AgentOS
Web Proxy uses the current runtime Cron settings helper without a test-only
replacement. Real accounts/SDKs and frozen packages still need separate acceptance.

## P3-A: local credential access

`CredentialAccess` and `CredentialUpdateSink` expose minimal immutable values;
tokens are excluded from repr. `assemble_services` injects a lazy AuthService
provider, LocalCredentialAccess and a `MessageHandlerCredentialUpdateSink` bound
to its exact MessageHandler. Login HTTP handlers and Web/TUI/ACP preprocessing
share these dependencies; `MessagePreprocessor.with_credential_access()` and
`MessageHandler.set_credential_access()` are the new injection seams.
Model classification also uses the injected service. Self-configured models do
not read login sessions or start refresh work.
Caller-supplied `_model_auth` is removed with an audit warning before lookup;
the injected path must not subsequently use legacy global injection. The 20-second expired-token
refresh wait, usable-token fallback and distinction between business user IDs
and model login identities remain unchanged.

Legacy unbound helpers and `register_auth_routes(app)` remain compatible.
Logout deletes the local session before best-effort notification through the
existing AS transport; this is not reliable multi-AS revocation.
Tests: `test_local_credential_access.py`, `test_credential_pipeline_contract.py`
and `test_auth_dependency_injection.py`.

## P3-B: Cron and session credential bindings

Login-sourced Cron selections bind a credential reference at create/update;
execution resolves only that reference, not a matching model name. Existing job
formats, scheduling and delivery are unchanged. `SessionLoginBindings` adapts
the existing `session_owners.json` persistence/TTL and catalog checks. Non-Web
fallback scope and login hints remain unchanged; an unauthenticated Web request
cannot borrow the recorded account. Bindings precede MessageHandler forwarding,
and the shared credential port precedes Cron scheduling.
`CronScheduler.set_credential_access()`,
`CronController.set_credential_access()` and the `MessageHandler` constructor argument
`session_login_bindings` are the new injection seams. `MessageHandler` requires an
explicit binding port and no longer falls back to the global session-owner store.
These are account bindings, not Worker ownership.

Tests: `test_session_login_bindings.py`, `test_cron_credential_access.py`,
`test_credential_network_contract.py` and existing Cron/login regressions.

## P3-C: saved-configuration effects

`application/config_saved_effects.py::ConfigSavedEffectCoordinator` coordinates
the original snapshot, AS reload, prewarm, browser restart,
proactive Cron and topic synchronization effects. `_build_config_saved_callback`
remains a compatible thin factory and still accepts the unused legacy
`message_handler` argument; `sync_saved_topic_recommendation` remains
importable from the entrypoint. Retry timing, restart decisions, request context
and background error handling remain unchanged.

Personal Web/TUI saves still persist and reload inside the target AgentServer.
A successful remote save must not call the coordinator or reload again. The
coordinator does not write configuration or replace OptionalChannels/CM updates.
Tests: `test_config_saved_effects.py` and the real-factory
`test_config_save_legacy_worker.py`.

## Verification and rollback

Use a fresh per-worktree `.venv` with develop's unchanged lockfile. Record the
baseline before rebuilding. Merge the five PRs in this order: startup/listeners,
protocol runtime, optional channels, single assembly, credentials/config effects.
Every stage includes its callers, tests and bilingual documentation, and must
pass the full commands below independently before publication. Rebase the next
PR onto the actual merged develop revision and repeat validation before merging.
Commands, retaining the repository's pytest options, are:

```bash
uv sync --frozen --extra test
uv run --frozen pytest tests --no-cov
uv run --frozen pytest st_test/testcases_gateway --no-cov
```

Set `JIUWENSWARM_HOME` and `JIUWENSWARM_DATA_DIR` to temporary directories before
collection. Process ST should use the current Python and repository configuration
via `AGENT_GATEWAY_BIN` and `ST_GATEWAY_CONFIG_SRC`. External accounts/models,
real mother integration, frozen packages and non-macOS platforms are separate
gates; skips do not establish those capabilities.

Record actual commands, outcomes, skip/failure reasons, source/lock identifiers
and package provenance outside the repository; do not reuse historical counts.
Verify the final tree against the validated reference tree and ensure the five
stages are linear, without the original large commit or extra merge commits.
Stop the running process before rollback; revert in reverse dependency order
and validate workspace lock/port reuse. No storage-schema or live-connection
migration is introduced.
