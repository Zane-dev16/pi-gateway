# Configuration

How Pi Gateway is configured: where behavior config and secrets live, how
authorization is expressed, and what each platform manifest declares.
Environment and layout basics are in [docs/installation.md](installation.md).

## Two kinds of input

Behavior comes from the config file in the profile home. Secrets come from
the `.env` store, and adapters also read well-known env vars for
authorization knobs (below).

Config is read once at boot per watcher/service. There is no live reload in
v0.1 (DEC-013). Restart the gateway to apply
changes; see [docs/operations.md](operations.md) for the takeover handshake
that makes this cheap.

## `PI_HOME` and profiles

- Default home: `~/.pi` (Windows: `%LOCALAPPDATA%\pi`); override with the
  `PI_HOME` env var (spec 01 §6).
- Profiles each resolve to their own home; the override must be installed
  before any project import, which every entrypoint does.
- Under multiplex (several profiles in one process), each inbound turn runs
  under a per-turn scope pair: a home redirect plus a secret scope, reset in
  reverse order even on error. Env reads on scoped paths fail closed: a
  scoped miss returns the declared default instead of falling back to the
  raw process environment, which could leak another profile's credentials
  or allowlists (spec 06 §3, DEC-003/009). CI gates this with
  `npm run check:secrets`.

## Platform manifests

Each platform adapter declares its data in a manifest (spec 04 §4.2):

| Field            | Meaning                                                            |
| ---------------- | ------------------------------------------------------------------ |
| `transportShape` | `polling` \| `ws` \| `webhook`: the conformance family it inherits |
| `requiresEnv`    | secrets required to connect (missing ⇒ loud disable, not silent skip) |
| `optionalEnv`    | opt-in features (e.g. `TELEGRAM_REACTIONS`)                        |
| `capabilities`   | async delivery, native chunking, command prefix, streaming probes  |
| rate/trust data  | per-platform rate tiers and, for HTTP ingress, signature schemes and limits (DEC-017) |

Example: the Telegram manifest requires `TELEGRAM_BOT_TOKEN` and runs the
polling shape. Unique credentials are guarded by token locks: a second
instance connecting with the same bot token is refused with a fatal error
naming the holding profile and PID (spec 06 §5).

## Authorization

Deny by default, with per-decision reason codes logged (spec 06 §2). The
principal knobs, per platform `{P}`:

| Env var                      | Effect                                                        |
| ---------------------------- | ------------------------------------------------------------- |
| `{P}_ALLOWED_USERS`          | platform-wide sender allowlist (`*` wildcard honored)          |
| `{P}_GROUP_ALLOWED_CHATS`    | group chat-ID allowlist (checked before the no-user guard)     |
| `{P}_ALLOW_ALL_USERS`        | explicit opt-out allowing all senders for that platform        |
| `{P}_ALLOW_BOTS`             | `mentions` \| `all`: admit bot senders (#4466 parity)          |
| `GATEWAY_ALLOWED_USERS`      | global allowlist across platforms                             |
| `GATEWAY_ALLOW_ALL_USERS`    | global allow-all opt-out                                      |

Policies per chat type (spec 06 §2.2):

- `dm_policy`: `open` | `allowlist` | `disabled` | `pairing`
- `group_policy`: `open` | `allowlist` | `disabled`

Adapters may default to `pairing` (e.g. a DM pairing handshake with a code,
lockout after 5 failed approvals, approval written into the allowlist; spec
06 §2.4). A message that arrives under an adapter's `open` policy is not
trusted as authorization: the gateway trusts adapter decisions only when the
effective policy is `allowlist` (the #34515 fail-open fix). Webhook
platforms additionally verify HMAC signatures (±300s replay window for
Svix-style schemes) and may require IP-range allowlists on non-loopback
binds (spec 06 §8, DEC-017).

## Gateway behavior knobs

- Busy behavior is set by the registry, not configurable per platform:
  `dispatch | reject | interrupt_then_dispatch`, default `reject`. Queuing a
  recognized slash command is always wrong (spec 07 §1.4, DEC-005).
- The worker pool is bounded (10 workers); correctness comes from leases,
  not thread counts (spec 01 §2.1).
- Delivery obligations cap at 3 attempts / 24h staleness / 7d retention /
  500 rows; timeout-classified send failures are not retried inside platform
  ladders (DEC-046/053/054).

## Precedence notes

- Platform env vars and config-file extras are bridged first-writer-wins:
  under multiplex, a scoped read prefers the profile's own values and never
  falls through to the process env on a miss (spec 06 §2, #72348 parity).
- Allowlists union across platform ∪ group-user ∪ global scopes.
- Unknown policy values normalize to the safe default (deny) with a warning.

## See also

- [docs/platforms.md](platforms.md): per-surface secrets and shapes
- [docs/architecture.md](architecture.md): how these inputs flow
- [docs/troubleshooting.md](troubleshooting.md): denials, locks, scopes
- [README.md](../README.md): project hub
