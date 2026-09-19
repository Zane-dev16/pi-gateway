# Quickstart

Get Pi Gateway running against one platform in about five minutes.

For background on how the gateway is put together, see
[docs/architecture.md](architecture.md); for all configuration knobs,
[docs/configuration.md](configuration.md).

## Prerequisites

- A pi installation with a model/provider configured. The gateway reuses the
  host pi agent loop directly (DEC-023), so turns cannot run until pi can
  reach a provider.
- Node.js 26+ at runtime (the SQLite driver is native).

## 1. Install

Install as a pi package (no manual `npm ci` needed — `pi install` runs it
for you):

```sh
pi install npm:@irellzane/pi-gateway
```

Or from git (same code, no registry):

```sh
pi install git:github.com/Zane-dev16/pi-gateway
```

For local development see [CONTRIBUTING.md](../CONTRIBUTING.md):

```sh
git clone https://github.com/Zane-dev16/pi-gateway
cd pi-gateway
pi install . -l       # links the checkout into pi
npm run build        # type check; must exit clean (dev only)
```

Full requirements and the `PI_HOME` layout:
[docs/installation.md](installation.md).

## 2. Configure the platform list + secrets

The extension boots exactly the platforms named in `PI_GATEWAY_PLATFORMS`
(comma-separated, case-insensitive, deduplicated; unknown names stay
absent). Unset or empty resolves to zero platforms — the gateway starts
with no adapters, unchanged (DEC-072):

```sh
export PI_GATEWAY_PLATFORMS="telegram,matrix"
```

Every adapter declares its required secrets in a manifest (spec 04 §4), and
each listed platform still passes its `requiresEnv` gate at boot: listed but
uncredentialed stays a loud disable naming the missing secret, never a
silent skip.

The Telegram adapter (Bot API long-polling, the polling transport shape;
DEC-024) requires `TELEGRAM_BOT_TOKEN`:

```sh
export TELEGRAM_BOT_TOKEN="123456:ABC..."
export TELEGRAM_ALLOWED_USERS="your-telegram-user-id"
```

The Matrix adapter (long-poll sync; DEC-072/073) requires
`MATRIX_HOMESERVER` plus `MATRIX_ACCESS_TOKEN` — or the `MATRIX_USER_ID` +
`MATRIX_PASSWORD` login pair instead:

```sh
export MATRIX_HOMESERVER="https://matrix.org"
export MATRIX_ACCESS_TOKEN="syt_..."
export MATRIX_ALLOWED_USERS="@you:matrix.org"
```

Provide the secrets through your shell environment or the profile's `.env`
store under `PI_HOME`. Authorization is deny-by-default (spec 06 §2), so
without the sender allowlist an adapter accepts no one.

## 3. Run the gateway

The gateway runs as a pi extension (`extensions/pi-gateway.ts`, loaded via
the `pi.extensions` package manifest) inside a long-lived pi process — there
is no separate gateway CLI. Boot pi in RPC mode with auto-start on
(pi 0.84.4):

```sh
PI_GATEWAY_AUTO_START=1 PI_GATEWAY_PLATFORMS="telegram,matrix" pi --mode rpc
```

On `session_start` the extension resolves the platform list into hosted
adapters (DEC-072; matrix binds the real CS-API HTTP transport, DEC-073),
composes the lifecycle (DEC-058: boot fingerprint, PID file and runtime
lock, `state.db` open/repair, embedded services), and starts it. The same
lifecycle is available manually in chat via `/gateway start [home]` →
`/gateway stop`, with `/gateway` reporting status.

What "enabled" looks like:

- The session notice reads
  `gateway running — home=<PI_HOME> platforms=[telegram,matrix]`
  (`platforms=[none]` means the list was empty or unset.)
- The log records `platform adapter <name> connected` per adapter, then
  `gateway READY`.
- A listed-but-uncredentialed platform logs
  `platform adapter <name> DISABLED: <MISSING_SECRET>` at ERROR with reason
  code `adapter_disabled` — check that line rather than wondering why a
  platform never comes up.

Stop with `/gateway stop` (or by ending the session, which tears the
gateway down on `session_shutdown`): shutdown drains ingress, lets active
turns finish, flushes delivery obligations, and exits (spec 08 §1.2).

## 4. Say hello

Message your bot from an allowed user. A turn runs through the two-level busy
guard and the two-layer turn lease (DEC-004/005); streaming edits a draft
message in place where the platform supports it, then seals the final
response.

Useful first commands in chat:

- `/help`: commands, derived from the single central registry (spec 07 §1)
- `/status`: adapters, worker-pool depth, lease table, delivery backlog

## 5. Verify state on disk

Everything lands under your profile home (default `~/.pi`, override with
`PI_HOME`):

```
~/.pi/
├── state.db              ← the one SQLite substrate (WAL)
├── logs/agent.log        ← catch-all activity log
├── logs/errors.log       ← WARNING+ with reason codes
└── gateway_state.json    ← runtime status snapshot
```

## Next steps

- Add more platforms: [docs/platforms.md](platforms.md)
- Run it as a service, update safely, read logs:
  [docs/operations.md](operations.md)
- Anything acting up: [docs/troubleshooting.md](troubleshooting.md)

## See also

- [docs/installation.md](installation.md): deployment details
- [docs/configuration.md](configuration.md): secrets, policies, allowlists
- [README.md](../README.md): project hub
