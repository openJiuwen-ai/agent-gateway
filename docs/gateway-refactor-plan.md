# Gateway refactor plan

## Goal

Reduce the concentration of behavior in `message_handler.py` and
`app_web_handlers.py` while preserving the public RPC behavior and existing
import paths. The repository currently has broad, singleton-oriented modules
with little characterization coverage, so changes must be incremental and
measurable.

## Principles

1. Move behavior only after a characterization test exists.
2. Keep one domain per module: mode/state, streaming, non-streaming, cron push,
   evolution approval, and notification/error handling.
3. Preserve legacy import paths with explicit re-exports.
4. Run compile checks and tests after every extraction.
5. Keep refactors behavior-neutral unless a test exposes a defect.

## Current baseline

- Channel configuration handlers now use a shared registration function for
  the standard channel set. `_send_rpc_error` keeps error response shape in one
  place.
- Route order remains: `health_check`, `heartbeat`, `feishu`, `xiaoyi`,
  `telegram`, `dingtalk`, `whatsapp`, `discord`, `slack`, `wecom`, `wechat`, and
  WeChat UI/unbind.
- Telegram intentionally registers before DingTalk. DingTalk and
  Feishu/Xiaoyi are not yet folded into the standard spec because their
  persistence and cache-clearing timing differs.
- Remaining extraction hotspots are `message_handler.py`, `app_gateway.py`,
  `web_connect.py`, `cron/scheduler.py`, and `feishu_connect.py`.
- The next shared pipeline candidate is Feishu/Xiaoyi: both normalize by
  `app_id`, retain sensitive fields, and persist configuration, but cache
  cleanup timing must be preserved.

## Phases

### Phase 0 - Safety net

- Record current test and compile status.
- Add pure-model and payload-normalization tests before moving code.
- Treat `uv.lock` and unrelated working-tree changes as user state.

### Phase 1 - Message-handler value models

- Extract `ChannelMode`, `channel_mode_from_str`, `ChannelControlState`,
  `NewSessionCancelParams`, `ModeChangeCancelParams`, A2UI fallback, and legacy
  health-relay normalization into `agent_gateway/message_handler/models.py`.
- Keep the same symbols importable from `message_handler.py`.
- Verify enum behavior, defaults, required fields, and fallback payloads.

### Phase 2 - MessageHandler split

Extract in this order:

1. Session/control state.
2. Streaming forwarding pipeline.
3. Non-streaming forwarding pipeline.
4. Cron push.
5. Evolution approval.
6. Error and notification helpers.

Each extraction must keep `MessageHandler` as the facade until callers are
migrated deliberately. Do not duplicate mutable state; pass a typed context or
owner object into extracted collaborators.

### Phase 3 - Web handler split

Split `_register_web_handlers` by RPC domain:

1. External CLI.
2. OpenAI account authorization.
3. Updater.
4. Session and project.
5. Path and file.
6. Locale and config.
7. Channel configuration.
8. Cron.
9. Heartbeat.
10. Permissions and harness.

Each domain module should receive a shared registration context instead of
directly capturing dozens of closure variables.

### Phase 4 - Continuous checks

Run:

```sh
uv run python -m compileall -q agent_gateway
uv run pytest -q --no-cov -x
```

After each phase, compare public symbol exports, RPC route tables, and test
coverage. Add behavioral tests before replacing closure state or changing
control flow.

## Completion criteria

- No single function concentrates unrelated RPC domains.
- `MessageHandler` coordinates collaborators instead of implementing all
  pipeline stages directly.
- Legacy imports continue to work.
- All tests and static checks pass on Windows and Linux CI.
