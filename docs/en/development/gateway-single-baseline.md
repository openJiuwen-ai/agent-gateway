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
