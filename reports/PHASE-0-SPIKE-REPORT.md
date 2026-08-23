# SPIKE-REPORT.md — Phase 0 Runtime Spike Verification

**Verified:** 2026-08 (independent verifier pass; reports cross-checked, not trusted)
**Verdict: PASS** — `npx tsc --noEmit` exit 0; full suite **46/46 passed (4 files)**,
stable across verifier runs (~17.7 s each).

## Driver chosen

- **TypeScript on Node v26.7.0** (native type-stripping lets child processes import the
  real `.ts` modules directly — no logic replicas), runner **vitest v4**, driver
  **better-sqlite3** (synchronous, WAL-capable; sanity-proven below).
- Cross-process coordination: OS child processes (argv/stdin JSON-lines +
  marker-file polling) **and** worker threads, both importing production modules;
  event-based sync (`waitFor`/collectors), injected clocks for timing-sensitive
  contracts. Wall-clock sleeps-as-sync avoided.

## The three proofs

| Proof | Files | Contracts |
| --- | --- | --- |
| **Turn lease** (02 §5 / DEC-004) | `spike/lease/turn-lease-registry.ts`, `db-turn-lease.ts`, `child-driver.mjs`; tests `spike/tests/lease.spike.test.ts` (**12**) | Layer 1: generation-scoped stale-unwind rejection, timeout fail-closed + FIFO handoff, bounded idle-only eviction, rebind aliasing/blocking. Layer 2: lineage-root keying (segments share slot; fork/unrelated don't), TTL expiry w/ holder-scoped refresh, dead-PID reclaim w/ doubt-protection, `should_abort` bail-out. Cross-process: two-OS-process round-trip, **SIGKILL mid-hold → liveness reclaim ≪TTL** (asserted >240 s before expiry), **8-contender race (4 threads + 4 procs) → exactly one winner**, 20 ms probe timeline proves sole ownership every phase, lineage-root contention across processes. |
| **Stream consumer** (04 §5.2 / DEC-006) | `spike/stream/gateway-stream-consumer.ts`, `fake-relay-adapter.ts`; tests `spike/tests/stream.spike.test.ts` (**8**) | Happy path (prefix chains, ONE draft id per turn, seal carries final **byte-exactly**); **non-prefix frame mutation DETECTED** (`prefixViolations` recorded w/ prev/next evidence, lane disabled, final repaired via edit path); interim-beside-sealed reconciles BY EDIT (never plain second send); **`_interim_send` popped at BOTH doors** via single audited chokepoint (audit sequence `[send, send_for_platform, send]`, marker never on wire); finish exactly-once under double-finish + late-straggler race; failed seal degrades to plain delivery (final never swallowed); bare-finish legacy + no-stream turn delivers nothing; throttle on injected clock. |
| **WAL ladder** (02 §1.1) | `spike/wal/core.ts`, `child_runner.ts`; tests `spike/tests/wal.spike.test.ts` (**22**) | Ladder order traces for every branch: fresh→WAL; already-WAL kept with zero set-pragmas under live sibling opener; silent-refusal fallback + ERROR dedup once per label; raised locking-protocol (message *and* code shapes) → guarded DELETE via `busy_timeout=0` setter then restore; `requireWal` → `WalUnsupportedError`, no downgrade; race-to-WAL & unreadable-probe refuse downgrade; transient EIO retried exactly twice (50 ms pauses); #70055 vuln gate (fresh→DELETE, existing WAL never downgraded, indeterminate left alone); write ladder terminal SQLITE_BUSY after jittered patience; deterministic two-band jitter schedule. **Two OS processes**: A holds BEGIN IMMEDIATE, B collides then lands via ladder — 11 rows verified from third connection, `integrity_check=ok`, **byte-exact payloads, zero lost commits**; snapshot reader sees only committed snapshots (5 or 55, never partial); bare BEGIN rides out ≥2 s hold via `busy_timeout`. |

Plus `spike/sanity.test.ts` (**4**): better-sqlite3 opens WAL (sidecar materializes),
BEGIN IMMEDIATE/ROLLBACK control, multi-byte emoji TEXT+BLOB byte-exact round-trip,
two concurrent connections on one DB.

## Test counts

- **46 contracts total**: lease 12 · stream 8 · wal 22 · sanity 4.
- **Races**: 8-way exactly-one-winner (mixed threads/processes, probe-audited),
  double-finish + late straggler, two-process WAL writer/contender + snapshot
  reader + blocking BEGIN, two-connection SQLite writes.
- **Mutation-style detection**: non-prefix frame guard (recorded violation evidence);
  trace-vocabulary order assertions kill any ladder reordering; generation check
  kills identity-free release; stale-release no-op vs newer owner.
- **Byte-exact round-trips**: UTF-8 byte-compare in stream seals/edits; WAL
  byteLength+code-unit identity incl. combining marks, astral `\u{10FFFF}`,
  lone surrogates pinned to U+FFFD mapping, CJK/RTL mix, ~200 KB value across
  independent connections, payloads through contended cross-process commits.

## Acceptance criteria vs roadmap Phase 0 (10-implementation-roadmap.md)

1. **Host pi agent-loop reuse, directly** — ✅ feasible. `docs/sdk.md` exists at
   `/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/docs/sdk.md` and states
   an embedding surface: `createAgentSession()` (+`ModelRuntime.create()`,
   `SessionManager.inMemory()`) returning `AgentSession` with
   `prompt()/steer()/followUp()/subscribe(event)` — "embed pi in other applications".
   Gateway can drive one `AgentSession` per conversation without wrapping or
   re-implementing the loop (DEC-023 upheld; deep integration = Phase 1).
2. **Concurrency topology (01 §2.1)** — ✅ spike-level: async ingress ↔ sync store on
   worker threads AND OS processes both exercise the same module; generation tokens +
   structured cancellation reachable (`should_abort` bails mid-wait without consuming
   budget; generation-scoped release).
3. **SQLite driver capabilities** — ✅ WAL, `busy_timeout` honored, immediate-vs-deferred
   transaction control, byte-exact TEXT round-trips (incl. BLOB parity) — sanity + WAL
   suites.
4. **Cross-process primitives (08 §2)** — ✅ mostly: PID liveness (`process.kill(pid,0)`)
   drives pre-TTL reclaim; file-lock semantics survive two-process contention with
   integrity_check ok; killed-holder recovery proven. *Residual:* SIGHUP→SIG_IGN
   installability not exercised in this spike.
5. **JSON-RPC over stdio feasibility** — ✅ stdio scope: both harnesses run real
   line-delimited JSON protocols between parent and child processes with bounded
   deadlines, clean reaping, no fd-leak symptoms across repeated runs. (ws transport
   untested — client-surface concern.)
6. **Windows posture (Q20)** — ✅ spike core uses no POSIX-only primitive; PID liveness
   via signal-0 probe is portable. *Note:* SIGKILL appears only in the throwaway test
   harness (reclaim proof shape); ported suites will need a documented Windows-gated
   variant. No PTY/TTY need surfaced in any of the three proofs.

## Failures / deviations

- None blocking. `npx tsc --noEmit`: 0 errors (the previously reported
  `stream.spike.test.ts` error is gone).
- One of three verifier suite runs printed vitest `Errors: 1` while still passing
  46/46 (exit 0 on all runs; did not recur) — transient teardown noise consistent
  with spawned OS processes/workers; watch when suites port to Phases 1–2.
- Footprint compliant: `git status` shows only allowed additions
  (`.gitignore`, `PROGRESS.md`, `package.json`+lockfile, `tsconfig.json`,
  `vitest.config.ts`, `spike/**`); tracked `LICENSE` unmodified; nothing outside
  `spike/` touched within the repo. `package-lock.json` is a natural install artifact.
- All implementation files carry dense Hermes `file:symbol` anchor citations
  (turn_lease.py, hermes_state.py, stream_consumer.py, relay/adapter.py, run.py,
  platforms/base.py); no code vendored.
