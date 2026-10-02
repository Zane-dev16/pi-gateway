---
name: verify-pi-gateway
description: "Drive pi-gateway the way a user does — Telegram chat turns plus the pi-gateway CLI — and prove behavior with transcripts, DB state, and logs. Reach for it whenever a task changes gateway behavior and needs proof against the real app."
---

# Verify pi-gateway

Pi-gateway turns chat messages into serialized agent turns on one SQLite
database. Primary surface is **Telegram chat** (first production adapter);
secondary surface is the **`pi-gateway` CLI** (`run|status|stop`). No web UI.

Repo root in every command below is `/root/pi-gateway/pi-gateway`
(`BIN=$REPO/bin/pi-gateway`). Never drive the live `~/.pi` home.

## Launch

Each run gets a disposable home so parallel runs never share state:

```sh
RUN_ID=$$  # or any unique token
HOME_DIR=/tmp/pi-verify-$RUN_ID
scripts/control-pi-gateway launch --home "$HOME_DIR"
```

What it does: starts `pi --mode rpc` with `PI_GATEWAY_AUTO_START=1` and
`PI_HOME="$HOME_DIR"` in the background (cwd is the repo so pi loads the
local `./extensions` entry; stdin stays open via `tail -f /dev/null` or pi
exits on EOF), waits up to 60s for `gateway READY` in the launch log, then
prints `HOME_DIR`, `GATEWAY_PID`, and the log path. Ready = doctor says
HEALTHY. Teardown is under Cleanup below. This is README step 3, the real
production boot — it composes the same `gateway-run` root the standalone
binary uses.

Live-Telegram variant (needs a bot token; platform-less otherwise):

```sh
PI_GATEWAY_PLATFORMS=telegram \
TELEGRAM_BOT_TOKEN="<token>" TELEGRAM_ALLOWED_USERS="<numeric-id>" \
scripts/control-pi-gateway launch --home "$HOME_DIR"
```

Without `TELEGRAM_BOT_TOKEN` the gateway starts with `platforms=[none]` —
fine for lifecycle proof, useless for chat proof. Two gateways sharing one
bot token fight (Bot API 409 Conflict); live-chat runs are exclusive.

Known base defect (not this skill's to fix): bare-node
`bin/pi-gateway run` dies in TypeScript strip-only mode
(`platform-hosting.ts` chain uses parameter properties; open blocker #3 in
`BLOCKERS.local.md`). `bin/pi-gateway status|stop` still work — status
only sees standalone (`gateway.pid`) homes, never extension-launched ones.

## Doctor

Read-only. Run first whenever anything looks off:

```sh
scripts/control-pi-gateway doctor --home "$HOME_DIR"
```

Checks: `gateway_state.json` says `running` and its pid is alive, the
launch log contains `gateway READY`, and `state.db` opens with the
expected tables. Any failure means the instance is not worth driving —
relaunch instead of debugging against it. Never run doctor against `~/.pi`;
it only reads, but a stale state file there lies.

## Drive

All driving goes through the helper. Never kill by process name.

```sh
# Standalone bin surface (stdout + exit code are the assertions; sees only
gateway.pid homes, not extension-launched ones)
scripts/control-pi-gateway cli --home "$HOME_DIR" -- status
BIN=/root/pi-gateway/pi-gateway/bin/pi-gateway  # same binary the helper uses

# Headless Telegram turn (no token, no network): real adapter engine against
# TelegramBotApiFake + ManualPollingClock via the in-repo world fixture
scripts/control-pi-gateway headless --scenario send-receive

# Read-only DB peek (sessions, messages, obligations, leases)
scripts/control-pi-gateway db --home "$HOME_DIR" \
  "SELECT COUNT(*) FROM messages"

# Live Telegram chat over CDP (Chrome on loki, test chat #8979458891)
scripts/control-pi-gateway cdp-targets   # list open tabs via ssh, check CDP alive
```

Live-chat recipe: open `https://web.telegram.org/a/#8979458891` in the
loki Chrome profile, send `/help` or `/status` (deterministic registry
builtins, no model call) or a plain prompt, and read the bot reply in the
chat. Prefer these stable handles: the chat URL fragment `#8979458891`,
the message text `/help`, and the reply text quoting a known builtin
(`status`, `new`, `stop`). Chat proof needs `TELEGRAM_ALLOWED_USERS` to
contain the sender id or the adapter silently drops the message.

## Evidence

Proof artifacts survive cleanup under
`.agents/skills/verify-pi-gateway/artifacts/<feature>/` (gitignored):

- Launch proof: launch log lines (`gateway READY`, `platforms=[none]`),
  `gateway_state.json` before/after, exit reason `planned_stop`
  (`<feature>/launch.txt`, `<feature>/final-state.txt`).
- DB proof: the read-only second view (`<feature>/db.txt` — row counts or
  the inserted message row, never just the action log).
- Log proof: the matching launch-log lines (`gateway READY`, adapter lines)
  (`<feature>/launch.txt`; the extension path tees `[pi-gateway]` lines to
  the launch log, not to a `logs/` dir).
- Live-chat proof: CDP screenshot + visible reply text
  (`<feature>/reply.png`, `<feature>/reply.txt`); headless proof: the
  vitest output (`<feature>/headless.txt`).

Standards: exercise the real user path (extension boot via `pi --mode rpc`,
real Telegram shapes via the fake server, real chat via CDP) — not internal
setters or test-only endpoints. Capture the action AND the resulting state,
not just the final screen. Verify side effects (rows in `messages`,
`delivery_obligations`, `gateway_state.json` transitions) alongside what the
user sees.
Mocks only at the existing production boundary (TelegramBotApiFake for the
Bot API). A `platforms=[none]` run proves lifecycle only; say so on the
artifact. Some dry paths still touch the network — headless runs must show
zero `fetch` to non-local hosts (the fake proves this by construction).

## Cleanup

```sh
scripts/control-pi-gateway cleanup --home "$HOME_DIR"
```

SIGTERMs the gateway pid from `gateway_state.json` (graceful drain to
`planned_stop`), waits for exit, reaps only this run's launcher children
(`pkill -P` on the recorded launcher pid — never by process name), then
removes `$HOME_DIR` and its sibling launch log. Never touches `~/.pi`,
never removes `artifacts/`. Copy DB/log evidence into `artifacts/` BEFORE
cleanup — the DB lives inside the removed home. Run cleanup after every
failed iteration too so broken attempts don't strand processes. After
cleanup, confirm the evidence still exists at the named location — a
cleanup that eats the proof fails the run.

## Helpers

One script, executable, no reverse-engineering required:

- `scripts/control-pi-gateway launch --home <dir>` — background
  `pi --mode rpc` auto-start, wait for READY, print HOME/PID/log.
- `scripts/control-pi-gateway doctor --home <dir>` — read-only health.
- `scripts/control-pi-gateway cli --home <dir> -- <args>` — standalone
  `pi-gateway` argv passthrough (`status`, `stop`, …; gateway.pid homes
  only).
- `scripts/control-pi-gateway db --home <dir> "<sql>"` — read-only query.
- `scripts/control-pi-gateway headless --scenario send-receive` —
  offline Telegram turn via vitest world fixture.
- `scripts/control-pi-gateway cdp-targets` — CDP health over ssh.
- `scripts/control-pi-gateway cleanup --home <dir>` — stop + remove home,
  keep artifacts.

Isolation rule: one home per run, one run per bot token for live chat.
Headless and CLI runs are parallel-safe; live-chat runs are exclusive.
Never drive an instance this skill did not start.
