# Installation

How to install Pi Gateway, where its state lives, and how to keep it updated.
For a five-minute first run, start with [docs/quickstart.md](quickstart.md).

## Requirements

- A pi installation with a working model/provider; the gateway reuses the
  host pi agent loop directly, never a re-implementation (DEC-023)
- Node.js 26+ at runtime (better-sqlite3 13.x is the native SQLite driver,
  installed automatically by `pi install`)
- OS: Linux and macOS fully supported; Windows supported with OS-parity
  semantics (detached spawns, tree-kill takeover, no-`ps` forensics;
  spec 01 §2.3, spec 08 §1.3)

## Install as a pi package (recommended)

```sh
pi install npm:@irellzane/pi-gateway
```

Or from git (same code, no registry):

```sh
pi install git:github.com/Zane-dev16/pi-gateway
```

`pi install` resolves `package.json` and runs `npm install` for you, then
loads the extension at `extensions/pi-gateway.ts` (DEC-058) via the
`pi.extensions` manifest. After install, `pi gateway run` and the in-pi
`/gateway` slash command are available. See `pi list` / `pi remove`
/ `pi update` in `pi/packages.md`. If you installed from git, `pi update`
does not move the pinned ref — use `pi install git:github.com/Zane-dev16/pi-gateway@<new-ref>` to update.

## Install from source (development)

For contributors or local iteration, clone and link the checkout so pi
loads it from disk:

```sh
git clone https://github.com/Zane-dev16/pi-gateway
cd pi-gateway
pi install . -l        # link current checkout into pi (writes .pi/settings.json)
npm ci                # only needed for direct tsc/vitest without pi
npm run build         # tsc --noEmit must be clean
```

Optional self-checks before first run (also enforced in CI, see
[CONTRIBUTING.md](../CONTRIBUTING.md)):

```sh
npm test                    # full behavior-contract suite
npm run check:layering      # downward-only dependency rule (spec 01 §5.3)
npm run check:secrets       # fail-closed secret-scope grep gate (DEC-003/009)
```

## Where state lives: `PI_HOME`

Every path resolves through a single home accessor (spec 01 §6). The
resolution order is: context-local override → `PI_HOME` env var → platform
default (`~/.pi` on POSIX, `%LOCALAPPDATA%\pi` on Windows). The override is
installed before any project import, and all state reads/writes go through it
rather than hardcoded paths.

One home owns:

```
<PI_HOME>/
├── state.db                # the single SQLite substrate (WAL mode)
├── gateway_state.json      # runtime status snapshot (spec 08 §4)
├── state/gateway.heartbeat # loop-liveness heartbeat, rewritten every 30s
├── config + .env store     # behavior config + secrets (see configuration doc)
├── logs/                   # agent.log, errors.log, gateway.log, update.log
├── cron/  kanban/          # embedded-service state and locks
└── pending_messages/       # shutdown-flush recovery files (spec 08 §1.3)
```

`PI_HOME` is also the profile boundary: a second profile with its own home
never trips the first one's duplicate-instance guard, and under multiplex each
profile's secrets and allowlists are isolated by the fail-closed secret scope
(spec 06 §3, DEC-003).

## Running as a service

`pi gateway run` is a long-lived foreground process designed to sit under any
service manager. Semantics that matter for unit files (spec 08 §1–§2):

- `SIGTERM`/`SIGINT` trigger a graceful drain (stop ingress → finish turns →
  release leases → flush → close DB). Give the unit a stop timeout that
  exceeds the drain budget.
- A planned takeover (`--replace`) makes the old process exit 0 on purpose,
  so `Restart=on-failure` does not flap-fight the replacer.
- An unexpected external signal (container restart, OOM, bare `kill`) must
  never be persisted as a clean stop; the next boot distinguishes the two
  (#42675 parity).
- Exit code 75 means the loop-liveness watchdog restarted the process on
  purpose (service-restart code); supervisors should simply restart.
- A frozen drain cannot wedge forever: an OS-thread shutdown watchdog
  hard-exits at drain-budget + 60s after dumping all thread stacks
  (spec 08 §1.3).

## Deployment kinds and updates

The updater does not fight deployment models it does not own (spec 08 §5):

| Install kind              | Update behavior                                                            |
| ------------------------- | -------------------------------------------------------------------------- |
| pi package (`npm:`/`git:`) | `pi update --extensions` / `pi install <source>@<ref>` (pi reconciles)     |
| git checkout (linked dev) | in-place pull, the only in-place kind                                      |
| docker / nix / apt        | prints the right external command and exits 1 before mutating anything     |

Updates themselves are transactional: plan → snapshot → apply →
restart-per-kind → verify → receipt. A mixed-version fleet fails verification
with exit 1 rather than reporting healthy (spec 08 §8). Receipts land in
`logs/update_receipts/`. Details and operator procedures:
[docs/operations.md](operations.md).

## Uninstall

Stop the gateway (graceful drain), then remove the package:

```sh
pi remove npm:@irellzane/pi-gateway        # or pi remove git:github.com/Zane-dev16/pi-gateway
# for a linked checkout: pi remove ./pi-gateway  then rm -rf the clone
```

State under `PI_HOME` (`state.db`, logs, snapshots) is user data; delete it
only if you do not need session history, pairing grants, or cron jobs.

## See also

- [docs/quickstart.md](quickstart.md): first run
- [docs/configuration.md](configuration.md): config, secrets, profiles
- [docs/operations.md](operations.md): signals, health, updates
- [README.md](../README.md): project hub
