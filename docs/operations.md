# Operations

Running Pi Gateway day to day: lifecycle, health, logs, takeover, and the
update pipeline. Spec 08 (Operations) specifies this behavior with parity
anchors; the citations below refer to that document and the DEC log.

## Starting and stopping

```sh
pi gateway run             # foreground; Ctrl-C = graceful drain
pi gateway run --replace   # take over a running instance (takeover handshake)
```

Startup order is binding (spec 01 §3, DEC-058): profile override before
imports → config load → boot-code fingerprint → duplicate-instance guard →
PID file + runtime lock → `state.db` reconcile → cron → supervised watchers →
manifest-driven adapters → signal handlers. A service may start degraded, but
degradation is always loud: a missing platform secret disables that adapter
instead of skipping it.

Stopping drains in order: stop ingress → active turns finish within the grace
window → release leases → flush delivery obligations, token rollups, and
pending messages → close DB → exit. The process exits non-zero only on failed
flushes (spec 08 §1.2).

## Signals and exit codes

| Signal / event                   | Behavior                                                 |
| -------------------------------- | -------------------------------------------------------- |
| `SIGTERM` / `SIGINT`             | Graceful drain (twice = fast exit)                        |
| Takeover `SIGTERM` (`--replace`) | Planned: exit 0 so supervisors don't flap-fight           |
| Unexpected external signal       | Never persisted as a clean stop (#42675 parity)           |
| `SIGUSR1`                        | Service-restart drain: exit 75 (supervisor recycles)      |
| Wedged drain                     | OS-thread watchdog: stack dump + exit 1 ≤ drain+60s       |
| Loop liveness lost (3 probes)    | All-thread dump + exit 75 (spec 08 §1.3)                  |

Exit-code discipline: `0` planned, `1` generic crash/watchdog, `75`
service-restart.

## Duplicate instances and `--replace`

Two gateways under one home would double-tick cron and corrupt delivery
state, so the guard is PID-file-scoped to the profile home (spec 01 §3.2).
`--replace` writes a takeover marker naming the old PID, snapshots its child
processes, and SIGTERMs it with a bounded wait of at most 10 seconds before
forcing. The old process recognizes a covered takeover and exits 0. If the
marker can't be used, startup fails without griefing an unrelated process.

## Health and status

- `gateway_state.json` under `PI_HOME` is rewritten on every status
  transition. It records pid, argv, start time, state
  (`starting | running | draining | stopped …`), active agent count,
  per-platform connection state, and the `code_sha`/`code_version` stamps the
  update pipeline verifies against (spec 08 §4).
- `state/gateway.heartbeat` is rewritten every 30s. A stale mtime means the
  process is alive but the loop is frozen, which is the external monitor's
  signal.
- `/status` (chat command) reports adapters up or disabled with reason,
  worker-pool depth, pending slots, lease-table size, and the delivery
  backlog.
- Health endpoints are read-only over the same snapshot; monitoring must
  never poke adapters (no side effects).

## Logs

Under `PI_HOME/logs/` (spec 08 §3):

| File               | Contents                                                                |
| ------------------ | ----------------------------------------------------------------------- |
| `agent.log`        | INFO+ catch-all: all agent/tool/session activity                         |
| `errors.log`       | WARNING+ with reason codes (authz denials, lease loss, retries); the first triage stop |
| `gateway.log`      | gateway-scoped records only                                              |
| `update.log`       | stdout/stderr mirror of update runs                                      |
| `update_receipts/` | one JSON receipt per update run + `latest.json`                          |

All handlers rotate (~5 MiB), share one redacting formatter, and every
denial/retry/fallback logs a machine-parsable reason code.

## Shutdown backstops

Three independent backstops (spec 08 §1.3):

- Flush serializes pending messages and unflushed agent history to recovery
  files under `pending_messages/` before memory is discarded; the next boot
  replays them.
- Forensics writes a <10 ms context probe plus a detached diagnostic walk, so
  an invisible killer (container/OOM) leaves evidence.
- The watchdog hard-exits a wedged drain after dumping all thread stacks.

Locks and the PID file are always released before a hard exit.

## Updates

The update pipeline is transactional (spec 08 §8):

```
plan → snapshot → apply → restart-per-kind → verify → receipt
```

- The plan step inventories runtimes and refuses non-in-place kinds
  (docker/nix/apt) with the right external command and exit 1 before mutating
  anything.
- The snapshot step writes per-profile state snapshots (identical critical
  set, 1 GiB per-file cap, keep policy floors at 1) for file-loss recovery,
  not code rollback.
- Restart is fleet-wide and drain-first: every gateway unit on the host
  restarts, not just the invoking profile. Surviving siblings on stale code
  were historically the largest repeat-bug class.
- Verify reads each live gateway's stamped `code_sha`; any provably stale
  gateway fails the run as `partial` with exit 1. Mixed-version is never
  "healthy".
- Receipts are written on every terminal path, including refusals and
  exceptions.

During updates the updater may pause gateways with a reversible drain marker
(pause-for-update) instead of tree-killing anything (spec 08 §10).

## Operational rules worth knowing

- There is no live config reload (DEC-013). SIGHUP is not a reload signal;
  restart via `--replace` to apply changes. Update runs install hangup
  protection so package children survive terminal loss (DEC-042).
- Process identity is never guessed from argv substrings. Only canonical,
  parser-derived matchers count (spec 08 §9).
- Token locks: a unique credential (e.g. one bot token) can be held by
  exactly one adapter. Contention is a fatal connect error naming the holder
  profile and PID (spec 06 §5).
- Cron is bound by inactivity (default 600s), not wall-clock, so active jobs
  may run for hours (spec 07 §5.2).
- Multiplex keeps per-profile secret scopes fail-closed; scope hygiene is
  asserted even when a turn body raises (spec 06 §3).

## See also

- [docs/installation.md](installation.md): service installation
- [docs/troubleshooting.md](troubleshooting.md): failure modes
- [docs/architecture.md](architecture.md): why these mechanisms exist
- [README.md](../README.md): project hub
