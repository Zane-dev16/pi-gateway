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

## 2. Configure one platform

Every adapter declares its required secrets in a manifest (spec 04 §4). The
Telegram adapter requires `TELEGRAM_BOT_TOKEN` (Bot API long-polling, the
polling transport shape; DEC-024).

Provide the secret through your shell environment or the profile's `.env`
store under `PI_HOME`:

```sh
export TELEGRAM_BOT_TOKEN="123456:ABC..."
```

Then set a sender allowlist. Authorization is deny-by-default (spec 06 §2),
so only listed users can drive the bot:

```sh
export TELEGRAM_ALLOWED_USERS="your-telegram-user-id"
```

## 3. Run the gateway

```sh
pi gateway run
```

This composition root (DEC-058) records a boot fingerprint, claims the PID
file and runtime lock, opens or repairs `state.db`, starts the embedded
services (cron, handoff), and connects your configured
adapters. A missing secret disables an adapter loudly: check the log for the
`adapter_disabled` reason rather than wondering why a platform never comes up.

Stop with `Ctrl-C`: shutdown drains ingress, lets active turns finish, flushes
delivery obligations, and exits (spec 08 §1.2).

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
