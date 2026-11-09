# Authentication and Authorization

This page explains the authentication and authorization model of Agent Gateway: who presents which credential at which entry point, how credentials are stored and flow, and which rules keep clients from forging identities. Module paths are relative to the `agent_gateway/` package.

> **中文**：[../../zh/explanation/authentication.md](../../zh/explanation/authentication.md)

---

## Overview

Gateway authorization has five layers; each is independent and none substitutes for another:

| Layer | Protects | Mechanism | Implementation |
| --- | --- | --- | --- |
| Channel access | WebSocket / HTTP entry points | Origin checks, IAM handshake auth, SSH public-key auth, IM app credentials + user allowlists | `channel_manager/`, `common/security/`, `extensions/auth/` |
| User identity | Login state | Huawei account OAuth2 authorization code + PKCE, encrypted session store | `auth/` |
| Model credentials | Free-model calls | Per-request passthrough + placeholder handle + AgentServer registry injection | `auth/passthrough.py`, `auth/login_credentials.py` |
| Data plane | File upload / download | HMAC-signed self-contained tokens | `common/file_download_token.py` |
| Internal channel | Gateway ↔ AgentServer | Shared-secret header | `routing/agent_client.py` |

The enterprise edition adds an outbound credential store (`common/secrets/`) and management-plane mTLS; see [Enterprise credential management](#enterprise-credential-management).

---

## User identity

### Login flow (OAuth2 authorization code + PKCE)

Login is gated by a remote config fetched once at startup (default `https://aigw.openjiuwen.com/v1/config`, overridable via `JIUWENSWARM_CONFIG_URL`; setting it to empty or `off` disables login entirely and the auth routes return 404). The config also carries the OAuth parameters and the APIG endpoints.

The flow is implemented in `auth/account_kit.py` (`AccountKitFlow`) and `auth/service.py` (`AuthService`, a process-level singleton):

1. The client calls `POST /api/v1/auth/authorize`; Gateway generates PKCE S256 `code_verifier` / `code_challenge` plus a random `state` (valid 10 minutes) and returns the Huawei account authorization URL.
2. The browser authorizes and calls back. The callback may land on the local machine (`GET /api/v1/auth/callback`) or on an ECS exchange service — in that case the initiator polls to claim it, and `state` embeds a claim fingerprint so nobody else can claim the code.
3. Gateway exchanges the authorization code for an `id_token` + `refresh_token`. The exchange request goes to the configured exchange service (which adds the `client_secret`; the secret never lives on this machine). The JWT payload is parsed locally only to extract `user_id` and expiry — **Gateway does not verify the signature**; verification happens in the APIG custom authenticator.
4. The initiator claims the session with `{state, claimToken}` via `POST /api/v1/auth/claim`.

### Session store and lifecycle

`auth/session_store.py` (`AuthSessionStore`):

- **Storage**: in-memory plus an AES-256-GCM encrypted file `~/.jiuwenswarm/infra/auth/sessions.json` (affected by `JIUWENSWARM_DATA_DIR`). The key is the per-machine `.install_key` (32 bytes, mode 0600, rebuilt on corruption). The archive contains the `refresh_token`; storing it in plain text would hand over the account.
- **Multi-process sharing**: Gateway writes, AgentServer reads. Reloads follow the archive mtime; every write re-reads first so one process never overwrites another's update with a stale snapshot.
- **Binding**: `session_id` (random 24-byte urlsafe) maps to `user_id` (Huawei account openid). **One session per account** — logging in again evicts the old session.
- **Lifecycle**: session TTL is 30 days; the `id_token` rotates roughly hourly and is refreshed in the background when less than 5 minutes remain (concurrent refreshes for the same user are merged under a lock); logout deletes the session.

### HTTP session identifiers

The login identifier travels over two channels simultaneously (`channel_manager/web/web_http_auth.py`):

- Cookie `jiuwenswarm_auth` (httponly, samesite=lax, 30 days) — browsers.
- Header `X-Auth-Session` — desktop WebView / TUI, takes precedence over the cookie.

POST routes such as login, claim, cancel, and logout require an `X-Jiuwen-Auth: 1` header to block cross-site simple requests. A WebSocket connection resolves the session identifier **once at handshake** (header → cookie → query) and keeps that value for the connection's lifetime; the same key inside message bodies is always stripped (see [Security principles](#security-principles)).

### APIG model channel

Free models available after login are reached through APIG: the `id_token` is presented as `Authorization: Bearer`, and signature verification is done by the APIG custom authenticator. The model list is discovered live with the `id_token` (not configured), filtered by the remote config allow/deny lists, and cached for 30 minutes; listed entries always carry an empty `api_key` — the real credential is injected per request (next section).

---

## Free-model credential chain

Credentials for free models never land in config files or logs:

```text
Browser login (OAuth2 + PKCE)
   │ id_token + refresh_token
   ▼
AuthSessionStore (AES-256-GCM on disk)
   │ Gateway refills params._model_auth server-side before forwarding
   │   = { api_base, api_key = id_token, credential_ref }
   ▼
AgentServer registry (process memory, handle → real token)
   │ httpx hook injects Authorization: Bearer <id_token> before the request leaves
   ▼
APIG (custom authenticator verifies the signature)
```

### Placeholder handle

The `api_key` in model config is a stable placeholder `jiuwen-login:<ref>`. The `ref` is a credential handle: the first 32 hex characters of HMAC-SHA256 over `jiuwen-login-credential:<user_id>`, keyed by a key derived from the session-store master key. **The handle is per account, not per session** — after a re-login the handle stays the same, so in-flight requests receive the new token; and being a one-way HMAC, it is safe to appear in config, logs, and on-disk files.

### Per-request passthrough

`normalize_model_auth()` in `auth/passthrough.py` runs before Gateway forwards each message:

1. **Unconditionally delete** any client-supplied `_model_auth` key — otherwise a front end could stuff its own value and dictate "which endpoint, which headers".
2. Only when all hold — there is a session id, APIG config exists, the requested model is login-sourced, and the login session is live — does the server refill `{api_base, api_key, credential_ref}`.
3. A session idle for over an hour is synchronously refreshed before forwarding (capped at 20 seconds) so the first message after a pause doesn't fail with 401.

### AgentServer-side registry and injection

`auth/login_credentials.py` (executed in the AgentServer process):

- Receiving a request carrying `_model_auth` registers/refreshes the handle; the registry is pure process memory, evicted after 2 hours of disuse.
- Model config only ever holds the placeholder `jiuwen-login:<ref>`; an httpx request hook swaps it for the real token from the registry just before the request goes out. **Fail-closed**: if the handle is unknown, or the target URL is outside the registered endpoint, the `Authorization` header is deleted outright — neither the placeholder nor the real token ever leaks to a foreign address.

### Refresh and revocation

- Every 60 seconds the AgentServer scans for handles used within the last 20 minutes that expire within 15 minutes, and sends `auth.credential.refresh` via server_push to ask Gateway to refresh (retry interval 2 minutes).
- Gateway maps the handle back to a session, refreshes it, and pushes the new token via `auth.credentials.update`; if the session is logged out or refresh fails, it pushes `{revoked: true}` and the AgentServer drops the registration. Logout proactively pushes the same revocation.

### Session owners

IM / cron requests have no login session, yet their conversation may use a free model. `auth/session_owners.py` records "conversation session → credential handle" (plain-text `session_owners.json`, TTL 30 days, capped at 2000 entries; handles are one-way so plain text is safe): entries are written only when Gateway itself attached a credential, never from caller-supplied values. Later requests without a login session recover the credential via the owner; if that fails, the client receives a "login required" hint instead of a forged value passing through.

---

## Channel access auth

### Web channel (origin checks and handshake auth)

The WebChannel WebSocket upgrade passes two optional gates (`channel_manager/web/web_channel_app.py`):

- **Origin check** (`common/security/ws_origin.py`): enabled with `JIUWENSWARM_ENABLE_ORIGIN_CHECK=1` (off by default). Allowed hostnames come from `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` (comma-separated); requests without an Origin are only allowed if the list contains `none`; anything else gets 403. The trajectory HTTP side path follows the same rule and rejects `Sec-Fetch-Site: cross-site` outright.
- **IAM handshake auth**: when the southbound side is an AgentOS router, the handshake token is verified by agent-os IAM (below); failure returns 401.

### AgentOS IAM auth

`extensions/agentos/agentos_router/agentos_authenticator.py` submits the token to IAM `POST /api/v1/auth/verify`. Token priority: query `?token=` → `Authorization: Bearer` → `X-Token` header. Gateway **consumes only `valid` in the response (identity validity)**, not `authorized` (resource-level). When IAM auth is disabled it falls back to trusting the `X-User-Id` handshake header. The authenticator implements the `CredentialAuthenticator` interface from `extensions/auth/credential_authenticator.py`, is attached to Web / TUI channels via `set_handshake_auth()`, and the same interface reserves room for future API-key and SSH-certificate methods.

### SSH channel (ephemeral keys)

After connecting via Web / TUI, a user can switch to the SSH channel (`channel_manager/protocol/ssh/`). On switch, Gateway issues a **short-lived ephemeral key** (`extensions/auth/ssh_key_issuer.py`; ssh-ed25519 by default, TTL 300 seconds) and registers the public-key fingerprint together with the user identity in an in-memory registry (`ssh_key_registry.py`, lazily expired). The SSH server disables password auth and accepts public keys only: the fingerprint must be registered and the username must match; the connection identity **comes only from the registry hit**, never from the client-declared username. Configured under `channels.ssh.auth` (`enabled` defaults to `false`).

### Web proxy channel

The third-party Agent Web UI proxy (`channel_manager/protocol/web_proxy/`) listens on one port per agent type; the entry URL carries `?user_id=&token=`. The token is verified by IAM; a query-declared `user_id` that disagrees with the token identity gets 403. The first hop with a token is 302-redirected to the same path without it, planting an httponly cookie (idle expiry defaults to 15 minutes). IAM-related headers and cookies are stripped before anything is forwarded into the agent container.

### IM channels (app credentials and user allowlists)

Major IM channels use official SDK long connections, replacing webhook signature checks with "app-level credentials + SDK-internal verification":

- **Feishu**: `app_id` / `app_secret` plus `encrypt_key` / `verification_token` establish a WebSocket long connection; event decryption and token comparison happen inside the lark-oapi SDK.
- **DingTalk**: `client_id` / `client_secret` establish a Stream long connection; outbound REST exchanges appKey/appSecret for `x-acs-dingtalk-access-token`.
- **WeCom**: `bot_id` / `secret` establish a WebSocket long connection.
- **Telegram / Discord / Slack etc.**: bot-token long connections or polling.

Above the app credentials sits the **user allowlist**: `channels.<platform>.allow_from` in `config.yaml` lists allowed sender IDs; an empty list means everyone, and non-matching messages are dropped (Telegram replies with a no-permission notice; others stay silent). DingTalk is stricter: an malformed `allow_from` refuses to start that app rather than treating it as empty. The IM message `user_id` is extracted by Gateway from the platform-signed event — senders have no channel through which to inject identity parameters.

### Gateway ↔ AgentServer internal channel

The E2A WebSocket has no auth by default (a local process boundary). Setting `JIUWENSWARM_GATEWAY_INTERNAL_TOKEN` makes Gateway attach the `X-Jiuwen-Gateway-Internal` shared-secret header, which the AgentServer uses to admit gateway-only actions (such as the MCP OAuth callback). **The `channel_id` field inside payloads can be forged and is never a basis for authorization.** After an abnormal disconnect the client keeps the target URI and auth headers for reconnect-on-demand; an explicit disconnect clears them and no auto-reconnect happens.

---

## File download tokens

`common/file_download_token.py` authorizes file downloads / uploads with HMAC-SHA256-signed self-contained tokens (`base64url(payload).hex(hmac)`), verifiable across processes without shared memory:

- **Secret**: the `JIUWENSWARM_FILE_DOWNLOAD_SECRET` environment variable (≥32 chars) takes precedence; otherwise one is generated into the workspace `config/.file_download_secret` (mode 0600).
- **Shapes**: plain download `{path, sid, exp?}`; skill content images expire after 600 seconds by default; managed-asset tokens additionally bind `asset_id` / `size` / `digest`; upload tokens constrain `path` under the user-directory root.
- **Validation**: constant-time HMAC comparison; payloads without `exp` are accepted only under two exact legacy schemas (anything else is rejected); an optional `session_id` must equal `sid` exactly.

The URL shape is `/file-api/download?token=...&user_id=...`. Note this query token is a file-location token, unrelated to IAM credentials — the IAM verification path explicitly refuses to accept it.

---

## MCP OAuth callback

Gateway only hosts the callback entry point: `GET /mcp/oauth/callback` on the WebChannel port forwards the query verbatim to the AgentServer (via the E2A `command.mcp` `oauth_callback` action, admitted by the internal shared-secret header, 25-second timeout). Authorization-code exchange and token caching/refresh all happen on the AgentServer side; a failed callback renders a 400 landing page instead of a fake "authorization succeeded".

---

## Enterprise credential management

### A2A outbound credentials

An A2A outbound agent's domain record stores only the credential reference `a2a/outbound/<agent_id>.api_key`; the real credential lives in the layered SecretStore (`common/secrets/`): logical keys are routed by `secret_registry.yaml` to env / file / db, with a JSON file as the default backend; stored values carry an `ENC:v1` envelope with an AES-256-GCM master key from `JIUWEN_SECRET_MASTER_KEY` or `~/.jiuwenswarm/config/.master_key` (0600). At call time the credential is placed into headers / query / cookies per the security scheme declared by the remote Agent Card (apiKey, bearer, basic; oauth2 / OIDC treated as bearer); a card that declares required credentials with none configured refuses the call. Persisted records are sanitized recursively; the public projection exposes only `has_credential`, never the reference itself.

### Management-plane link mTLS and key system

Enterprise management-plane routes (`/api/v1/*` templates, config sync) are guarded by link mTLS (`common/security/link_mtls.py`): modes off / observe / enforce are chosen by environment variables, and **the default off means no application-layer auth**; enforce mode requires the TLS peer-certificate scope to match the binding header, otherwise 403. `enterprise/config/manager_config/security/` additionally provides a signing-and-encryption primitive set (X25519 envelope encryption + Ed25519 signatures, byte-identical on the Gateway and Manager sides), reserved for signed/encrypted config distribution; the current sync path treats traffic as unsigned and unencrypted, relying on link mTLS for transport security.

---

## Security principles

- **Server-only keys are stripped, then refilled**: `_model_auth`, `_auth_session`, and similar parameter keys are unconditionally stripped of client-supplied values before forwarding, then refilled by the server from connection properties and the session store. A client cannot dictate "which endpoint, which headers, whose identity" through a message body.
- **Identity is resolved once, at channel setup**: the WebSocket handshake reads the session identifier once and pins it to the connection; SSH connection identity comes only from the registry hit; IM sender identity is extracted by Gateway from the platform-signed event.
- **Credentials stay out of config and logs**: model config only ever shows placeholder handles; session archives are encrypted on disk; auth-context reprs are redacted; A2A outbound records are sanitized before persistence.
- **Fail-closed**: an unknown handle or a mismatched target deletes the `Authorization` header instead of leaking the placeholder; verification failures are uniformly treated as failures; key fingerprints that don't match read as not-found.
- **No fallback to "the only logged-in user"**: without a session id there is no login, ever — borrowing another local login state would let cookie-less requests impersonate someone else's credentials.
- **Minimal token exposure**: download / upload tokens are self-contained and short-lived; the web proxy's first 302 evaporates the token from the URL into a cookie.

## Environment variables and config

| Variable / config | Purpose | Default |
| --- | --- | --- |
| `JIUWENSWARM_CONFIG_URL` | Remote config (login switch, OAuth parameters, APIG endpoints, model allow/deny lists); empty or `off` disables login | production endpoint |
| `JIUWENSWARM_DATA_DIR` | Data root; determines the `infra/auth/` session directory | `~/.jiuwenswarm` |
| `JIUWENSWARM_SSL_VERIFY` | TLS verification for outbound requests | on |
| `JIUWENSWARM_ENABLE_ORIGIN_CHECK` | WebSocket / HTTP origin check switch (`1` enables) | off |
| `JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS` | Allowed Origin hostnames (comma-separated; `none` admits requests without Origin) | empty |
| `JIUWENSWARM_FILE_DOWNLOAD_SECRET` | HMAC secret for file tokens (≥32 chars) | auto-generated `.file_download_secret` |
| `JIUWENSWARM_GATEWAY_INTERNAL_TOKEN` | Gateway → AgentServer internal shared-secret header | unset |
| `JIUWEN_SECRET_MASTER_KEY` | Enterprise SecretStore AES master key | `~/.jiuwenswarm/config/.master_key` |
| `channels.<platform>.allow_from` | IM sender allowlist (`config.yaml`) | empty = allow everyone |
| `channels.ssh.auth.*` | SSH channel auth switch and ephemeral-key parameters (TTL defaults to 300 seconds) | `enabled: false` |

See [CLI and environment](../reference/cli-and-environment.md) for the full command-line and runtime-directory reference.

## Non-goals

- No local `id_token` signature verification — that belongs to the APIG custom authenticator.
- No IAM token issuance or rotation — token lifecycle belongs to agent-os IAM; Gateway only forwards and verifies.
- No MCP OAuth authorization-code exchange or token caching — that belongs to the AgentServer.
- Session store does not sync across machines — `.install_key` is per-machine; login state is local.
- Not a replacement for AgentServer-side permissions, tools, and sandbox control.

---

## Back to navigation

- [Back to documentation home](../../README_EN.md)
- [Architecture](architecture.md)
- [E2A protocol](../reference/e2a-protocol.md)
- [Configure channels](../how-to/configure-channels.md)
