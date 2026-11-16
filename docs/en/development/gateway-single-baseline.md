# Gateway single-process startup compatibility baseline

P0/P1 protects the default single-process runtime, extracts explicit startup and lifecycle boundaries, and adds an explicit log directory option. This batch introduces Control, Acceptor or Worker processes or changes chat, login, Cron, or HTTP behavior.

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

