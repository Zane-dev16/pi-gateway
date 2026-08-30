# Architecture

Pi Gateway reproduces the Hermes Gateway architecture exactly: one long-lived
process, one shared SQLite substrate, capability at the edges, invariants at
the core. Citations like "02 §5, DEC-004" refer to the normative spec set
([../README.md](../README.md)) and its decision log.

## The one-process rule

Everything lives inside the gateway process (spec 01 §1.2): platform adapters,
the runner, the embedded services (cron, kanban, handoff, hooks, plugins), and
sole ownership of `state.db`. Clients (CLI, TUI, dashboard) are consumers of
the same database and command registry, never co-owners; killing a client can
never take down the messaging gateway (spec 01 §2.3).

## Layers

Dependencies flow downward only; `scripts/check-layering.mjs` enforces this
(spec 01 §5.3):

```
entrypoints      'pi gateway run' composition root (DEC-058)
pi_platforms     adapters + kit + conformance (never import the runner)
pi_embedded      cron, kanban, handoff, hooks, update
pi_gateway       guards, streaming, obligations, registry, security
pi_agent_core    worker pool, agent runner, cache, alternation repair
pi_state         schema, leases, messages, usage, WAL ladder
pi_home          PI_HOME accessor (zero deps)
```

## A turn, end to end

```
MessageEvent → [L1 adapter guard] → [L2 runner guard] → [turn lease ×2]
    → worker pool (bounded 10) → agent core ⇄ stream consumer
    → delivery obligations → audited egress doors → drain boundary
```

1. L1 guard (in the adapter): a per-conversation slot installed
   synchronously before any task spawn; busy messages merge into a single
   pending slot or bypass as registry-classified commands (spec 03 §2).
2. L2 guard (in the runner): busy behavior comes from the one central
   command registry, `dispatch | reject | interrupt_then_dispatch`, never a
   per-command if-chain (spec 07 §1, DEC-005). FIFO overflow beyond the
   pending cap (32) is handed to a fresh drain task at the drain boundary.
3. Turn lease in two cooperating layers (spec 02 §5, DEC-004): an in-process
   registry with generation-scoped release, plus a cross-process DB row
   keyed on the compression-lineage root, not the raw session id, resolved
   in the same write transaction. TTL 300s; bounded wait (≤1800s);
   dead-PID reclaim; losing the lease mid-turn is a first-class
   `turn_lease` condition, not a crash.
4. Worker pool and agent core: the synchronous pi agent loop runs on a
   bounded pool; per-conversation prompt-cache stability and strict role
   alternation (repaired pre-request only, never at persist time) are
   invariants (DEC-015).
5. Stream consumer: prefix-stable draft edits against the platform's native
   draft message, sealed at `finish(final_text)`, which is authoritative.
   Every public egress door carries the seal check: one audited chokepoint
   property, enforced by mutation tests (spec 04 §5, DEC-006).
6. Delivery obligations: outbound sends obligate first, then attempt, with
   CAS-guarded transitions `pending → attempting → delivered | failed |
   abandoned` and caps of 3 attempts / 24h stale / 7d retention / 500 rows
   (DEC-053/054). `/status` surfaces the backlog (spec 08 §4).

## Core invariants

| Invariant                                        | Where                                        |
| ------------------------------------------------ | -------------------------------------------- |
| Per-conversation prompt-cache stability          | agent loop contract (spec 05)                |
| Strict message-role alternation, pre-request only | agent core (DEC-015)                        |
| Two-guard ordering (L1 → L2)                     | spec 03 §2                                   |
| Two-layer turn lease, lineage-root key           | spec 02 §5, DEC-004                          |
| Stream contract (4 invariants) + seal chokepoint | spec 04 §5, DEC-006                          |
| Fail-closed secret scoping under multiplex       | spec 06 §3, DEC-003/009                      |
| One central command registry; everything derives | spec 07 §1, DEC-005                          |
| Byte-exact `api_content` replay for cache hits   | `messages.api_content` sidecar (spec 02 §7)  |
| One shared SQLite substrate; no client writes    | spec 01 §1.2                                 |

## The substrate

One `state.db` per profile, SQLite in WAL mode with a guarded fallback ladder
for exotic filesystems (spec 02 §1). Declarative schema with automatic column
reconcile. Lease and compression-lock tables; the delivery ledger; token
usage coalesced through a background writer that never blocks a turn
(spec 02 §7, DEC-011).

## Embedded services

All supervised inside the gateway process, each with a tick loop and a
lock/claim story (spec 01 §4):

| Service            | Tick     | Isolation                                                        |
| ------------------ | -------- | ---------------------------------------------------------------- |
| Cron ticker        | ~60s     | tick lock; inactivity timeout 600s default, not a wall clock     |
| Kanban dispatcher  | ~60s     | machine-global singleton lock; auto-block after repeated failures |
| Kanban notifier    | ~5s      | in-process board subscriptions                                   |
| Handoff watcher    | 2s poll  | atomic DB row claim; re-bind + replay through the normal guards (DEC-008) |
| Hook registry      | event    | observer events never block; command hooks are decision-bearing (DEC-014) |
| Plugin loader      | boot     | idempotent discovery; plugins never modify core files            |

## Startup and shutdown

Startup order is binding (spec 01 §3): profile override before imports →
config → boot-code fingerprint → duplicate-instance guard / `--replace`
takeover → PID file + runtime lock → `state.db` reconcile → cron → supervised
watchers → manifest-driven adapters (missing secret ⇒ loud disable) → signal
handlers. Shutdown drains in reverse: stop ingress, finish turns, release
leases, flush obligations and pending messages, close DB. Three independent
backstops (flush-to-file, forensics probe, watchdog hard-exit) cover data,
evidence, and liveness respectively (spec 08 §1.2–§1.3).

## Client/server split

The messaging gateway is spawned detached and survives client churn; `serve`
backends die with their app by design (spec 01 §2.3). All client surfaces
speak JSON-RPC/REST to the same process.

## Fidelity and the decision log

Deviations from Hermes behavior require a logged DEC before implementation
(DEC-026). The spec set and decision log (DEC-001…069) live at
[../09-open-questions.md](../../09-open-questions.md).

## See also

- [docs/platforms.md](platforms.md): the adapter census
- [docs/adding-a-platform.md](adding-a-platform.md): extend the edges
- [docs/operations.md](operations.md): run and update it
- [README.md](../README.md): project hub
