# Phase 5 Report — Embedded Services, Update Pipeline, Receipts

**Verifier report.** Scope verified against roadmap §Phase 5 exit criteria
(`/root/pi-gateway/10-implementation-roadmap.md` L351–L389). Trust-nothing policy:
every claim below was re-derived from the actual tests and sources, not from the
workstream reports.

---

## 1. Mechanical gates (exact)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0**, clean |
| `node scripts/check-layering.mjs` | **OK** — downward-only holds across src/, exit 0 |
| `node scripts/check-secret-scope.mjs` | **OK** — no forbidden fallback shapes across 237 files, exit 0 |
| `npx vitest run` (FULL) | **1597 tests: 1596 passed, 1 failed** (138 files: 137 passed, 1 failed), ~311s |
| Baseline delta | 1204 baseline + **393 new** pi_embedded tests = 1597 |

**The single failure is a load flake, not a regression:** Phase-4
`src/pi_gateway/delegation/two-process.test.ts` ("SIGKILL between claim and ack ⇒ boot
restore replays EXACTLY once") hit its 300s timeout under full-suite parallelism, then
**passed in isolation in 1.45s** (2/2). Root cause: the suite grew by ~33% including
several real-child-process suites; on 4 CPUs this real-process test starves when run in
the full pack. No code change implicated; the test passes at Phase-4 commit state too.
Recommended follow-up for a later phase (not fixed here — outside assigned footprint):
serialize the real-child-process suites (`maxConcurrency` / file-level `sequence`) or
raise that file's timeout.

## 2. Footprint

`git status`: the ONLY change in the repo is untracked **`src/pi_embedded/`**
(64 source files ≈ 12 318 LOC + 35 test files, 399 `it()` blocks). Zero tracked-file
modifications; package.json/tsconfig/vitest.config/lifecycle/spec dirs untouched.

Six of seven workstreams delivered; **the update pipeline workstream returned `null`
and shipped nothing** (see §4c/f).

## 3. Exit checklist

| # | Criterion | Verdict | Evidence |
| --- | --- | --- | --- |
| a | Cron bounds edges: catchup clamp, one-shot grace, true-inactivity — ALL injected-clock; EMFILE-vs-contention tick lock; DEC-012 constructor assertion | **PASS** | `schedule.test.ts` "catchup grace clamp [120s,7200s] — both bounds" (min floor 2m⇒120, max ceiling 8h⇒7200, passthrough, unknown-cadence floor); "once grace semantics" (inside window keeps original run_at, just-outside skips, permanently ineligible after run) with injected anchor. `inactivity.test.ts`: idle trips EXACTLY at limit, active job survives HOURS of logical time, runaway fires on idleness AFTER activity stops, limit=0 disables watchdog — all via `CronClock`/manual clocks (13 test files use injected clocks; zero wall-sleep timing assertions). `tick-lock.test.ts`: THE #87644 fork — lock errnos classify CONTENTION, fd-exhaustion NEVER classifies as contention (errno or wrapped wording), EMFILE fault throws TickLockAcquisitionError (never masquerades as healthy skip), real second-ticker observes contention skip shape, retry ladder doubles capped at 15min. `executor.test.ts`: construction plan emits `skip_memory=false` explicitly, violation throws `/skip_memory=False/`, and memory hooks FIRE end-to-end (DEC-012). |
| b | Handoff E2E replays full CLI transcript onto destination through NORMAL pipeline | **PASS** | `handoff/e2e-handoff.test.ts`: composes REAL L1 guard + REAL GatewayAgentRunner over scripted provider + fake transport; seeded 3-turn CLI session → pending row → one watcher tick → wire observation proves every prior turn IN ORDER plus synthetic handoff notice reached the model request; reply egress lands on destination; busy destination takes the NORMAL busy ladder (merged into pending slot, never a rival turn); pipeline failure ⇒ failed row with error payload. Backed by `queue.test.ts` (DEC-008 CAS protocol, atomic claim inside BEGIN IMMEDIATE, error[:500] truncation parity) and a real two-process claim race suite. |
| c | Update drill on TWO-PROFILE host ⇒ fleet version matrix; stale gateway FAILS run (partial + exit 1) AND receipt written for that failure | **FAIL — not built** | No `src/pi_embedded/update/` exists; repo-wide grep finds no receipts module, no latest.json writer, no snapshot/apply/verify/restart code anywhere. Roadmap scope item 7 (08 §5–§8, §10) is entirely missing. |
| d | Every embedded service degrades loudly per-service without blocking the rest (01 §3 stage-9 semantics) | **PASS with gap** | Per-service optional-stage entries exist and are tested: kanban service returns `degraded:true` + loud warning naming restart scope on invalid pinned slug/openBoard failure, clean skip when env-disabled (`service.test.ts` L70–154); hooks `startupEmbeddedExtensions` NEVER throws — hook-discovery failure degrades loudly while plugins still discover+load (`plugin-discovery.test.ts` L405); delegation-watcher `boot()` on closed store degrades LOUDLY, broken store never crashes tick (`watcher.test.ts` L191–219); cron scheduler exposes degraded_start reason_code (`scheduler.ts` L470–502). **Gap:** the shared lifecycle runner's stage bodies `stageCronScheduler`/`stageEmbeddedWatchers` (`src/pi_gateway/lifecycle/lifecycle.ts` L446–456) remain Phase-4 placeholders ("lands Phase 5") — services carry the pattern but none is wired through the ten-stage runner, so cross-service non-blocking is proven within each service's entry, not end-to-end via the real lifecycle. Wiring was not done by any workstream and is outside this verifier's footprint. |
| e | Idle-gate race + ownership matrix rows COMPLETE the Phase-4 rail | **PASS** | `ownership.test.ts`: full decision table — live-parent PINNED self-delivery, idle-end retarget (current session, no rebind), lineage-tip retarget across compression between dispatch/completion, stale-intermediate accepted only when own verified tip equals target, unrelated route released (route-owns-lineage invariant), unknown parent terminally dropped fail-closed, retry arms (DB unavailable releases with visible attempt burn; compression mid-rotation retries; ENDED continuation retries). `watcher.test.ts` ★IDLE-GATE RACE: waits untouched while busy, fires as NEW forged turn exactly once after idle end; retargeted busy target gates on RESOLVED session; fan-out coalescing per parent; loop timing via injected clock; stop() breaks in-flight sleep. `crash-recovery.test.ts`: dead generation rows recovered + delivered EXACTLY once across restarts, interplaying with the Phase-4 rail restore-on-boot. |
| f | Receipts on EVERY path incl. refusals; process matchers parser-derived only (adversarial argv matrix green) | **FAIL — not built** | Same missing update pipeline: there are no receipts, no canonical process matchers, no adversarial argv matrix in the tree. Nothing to verify. |

Supplementary verification beyond the checklist:

- **Kanban** (07 §6): board slug hard boundary pinned via env; reclaim→promote→claim
  tick shape; highest-priority-first with live maxSpawn cap; lost-claim-race skips
  silently (exactly-one-owner); breaker auto-blocks after exact-N consecutive spawn
  failures across ticks; reference TTL 15min; **3 concurrent OS processes race one card
  ⇒ exactly one winner** (`claim-atomicity.two-process.test.ts`).
- **Hooks/plugins** (07 §7): REQUIRED decision triples all proven end-to-end through
  REAL discovery — deny blocks (+ default blocked message names TYPED token),
  handled replaces (silent-success degradation path included), rewrite mutates and
  re-resolves ONE hop only (rewritten command NOT re-intercepted even when it would
  deny); first-decisive-verdict-wins ordering; membership gate short-circuits unknown
  commands; observer containment; frozen-era plugin.yaml loads via real scanner.
- **Approvals bridge** (07 §8): `resolve()` as THE resolution primitive (FIFO pop(0),
  count-authoritative, request_id targeting, resolve_all); waiter release paths never
  strand (settle exactly-once, unregisterNotify signals all, clearSession denies +
  releases immediately); coalescing matches (command, patternKeys) exactly; replay-safe
  snapshots; slash arg parsing incl. verbatim uncased deny reason capped at 280.
- **Handoff watcher ops**: poll interval + startup delay constants exported from
  `index.ts` and used by tests; CLI stub-row `ensureSessionRow`; two-process pending-row
  contention suite present.

### Counts

| Metric | Count |
| --- | --- |
| New contract tests (`it()` blocks) | **393 passing / 399 declared** (6 are `it.todo`/skips in two-process drivers) — all green |
| Real child-process / cross-process race suites | 5 dedicated files (cron two-process tick-lock, handoff two-process claim, kanban 3-process claim race, delegation crash-recovery, kanban sqlite-board) + Phase-4 rail suites still green |
| Injected-clock timing suites | 13 test files use manual/injected clocks; **zero wall-sleep-based timing assertions** in bounds/grace/backoff contracts |
| Mutations | No mutation-testing harness exists in this repo (Phase-2's seal suite was the mutation gate; Phase 5 has none mandated by the roadmap exit row) — races instead covered by real multi-process contracts above |

## 4. Blocked threads / gaps (precise)

1. **UPDATE PIPELINE MISSING (blocks Phase-5 exit criteria c and f).** Roadmap scope
   item 7 — plan (deployment-kind aware) → snapshot (file-set identical, 1 GiB cap,
   keep=1) → apply (ZIP fallback only on argv/git-classified error; dirty-tree refusal
   ×2) → restart-per-kind (fleet-wide, drain-first SIGUSR1, per-unit isolation) →
   verify (settled window; stale gateway ⇒ partial + exit 1) → receipts on EVERY path
   incl. refusals + latest.json + bounded prune → canonical parser-derived process
   matchers (08 §9) → pause-for-update (08 §10) → SIGHUP→SIG_IGN hangup protection
   during update runs (DEC-013). The workstream reported `null`; no code landed.
   This is the sole hard FAIL of the phase.
2. **Lifecycle wiring gap (criterion d).** Services implement and test optional-stage
   semantics individually, but `lifecycle.ts` stages 7/8 bodies still log "lands
   Phase 5". Cross-service degrade-through-the-runner is therefore unproven.
   Fix belongs to whoever owns `pi_gateway/lifecycle` + a small integration test;
   it is mechanical given the per-service entries already conform.
3. **Full-suite real-process starvation.** Phase-4 `two-process.test.ts` times out at
   300s under full-suite load, passes in isolation in 1.45s. Needs suite-level
   serialization of child-process files or a raised timeout; left untouched (outside
   footprint).
4. Minor: 6 of 399 new `it()` declarations are skipped/todo placeholders inside
   two-process driver files — enumerated, not hidden failures.

## 5. Proposed DECs (collected; none implemented)

- **DEC-039 (proposed):** TickLock sidecar realization — Hermes' `.tick.lock` flock is
  realized as a SQLite-sidecar acquisition (`<cronDir>/.tick.lock.db`) because the TS
  runtime lacks a portable flock seam; errno classification (#87644 fork) preserved
  verbatim. Smallest Hermes-consistent behavior; needs ratification since the mechanism
  (not the semantics) diverges.
- **DEC-040 (proposed):** embedded services start via their own optional-stage-shaped
  entries in Phase 5; wiring them into `lifecycle.ts` stage 7/8 bodies is deferred to a
  follow-up integration pass (services already conform to StageEvent/degraded-start
  classification). Records the current state honestly rather than claiming stage-body
  integration.
- **DEC-041 (proposed):** full-suite execution must serialize real-child-process spec
  files (4-CPU hosts starve them under parallel load) — CI-stability decision, no
  behavior impact.

## Verdict

**Phase 5 = CONDITIONAL PASS — 5 of 7 exit-criterion rows PASS (a, b, d-with-gap, e,
plus supplementary kanban/hooks/approvals verification); criteria (c) and (f) FAIL:
the update pipeline + receipts subsystem was never built.** Gates: tsc clean,
layering clean, secret-scope clean, 1596/1597 suite (single load-flake, passes in
isolation). The phase cannot be declared complete until the update drill and
receipts-on-every-path rows have real implementations and green contracts.

---

## Completion — Final Verification (update subsystem landed)

**Date:** 2026-08-24 · **Verifier:** independent trust-nothing pass over the
completed Phase 5 footprint (`src/pi_embedded/update/**`, 24 files ≈ 5 008 LOC).

### Mechanical gates — both runs measured

| Gate | Run 1 | Run 2 |
| --- | --- | --- |
| `npx tsc --noEmit` | exit 0 | — (static, re-checked once) |
| `node scripts/check-layering.mjs` | OK (downward-only holds) | — |
| `node scripts/check-secret-scope.mjs` | OK (258 files under src/) | — |
| `npx vitest run` FULL | **1694/1694 passed, 153 files, 51.4s** | **1694/1694 passed, 153 files, 51.5s** |

Delta vs pre-update verified baseline (1610/1610 over 143 files): **+84 tests /
+10 test files**, exactly the update suite (64 pure contracts + 20 two-process
contracts). Zero failures, zero skips, byte-identical counts across the two
DEC-041-discipline runs (~51s each — the old real-process starvation is gone;
DEC-041 serialization works).

### Exit-criteria table — final

| # | Criterion | Verdict | Evidence |
| --- | --- | --- | --- |
| a | Cron bounds edges (catchup clamp, one-shot grace, true-inactivity; injected clock; #87644 tick lock; DEC-012 memory gate) | **PASS** | unchanged from interim report §3 |
| b | Handoff E2E through NORMAL pipeline | **PASS** | unchanged |
| c | Update drill on TWO-PROFILE host ⇒ fleet matrix; stale gateway FAILS run (partial + exit 1) AND receipt for THAT failure | **PASS** (was FAIL) | `update/pipeline-two-process.test.ts`: REAL git tree + TWO spawned units; stale arm asserts `outcome=partial`, `exitCode=1`, error names staleness, fleet matrix marks default=current / work=stale, `latest.json` pointer reads `outcome:"partial"`; all-current arm exits 0 with success pointer and pulled code verifiably on disk |
| d | Embedded services degrade loudly without blocking (01 §3 stage-9) | **PASS** | stage wiring per DEC-040 landed in commit 8b4f7f5; per-service entries unchanged-green |
| e | Idle-gate race + ownership matrix completes Phase-4 rail | **PASS** | unchanged |
| f | Receipts on EVERY path incl. refusals; parser-derived matchers only (adversarial argv matrix green) | **PASS** (was FAIL) | receipts: success/refusal(zip-package, dirty-tree)/partial/escaped-exception paths each persist exactly one idempotent receipt + atomic `latest.json` pointer (`writeJsonAtomically` temp→rename); bounded prune keeps 20, floors keep≥1. Matchers: token-based subcommand extraction, value-flag set DERIVED from single `TOP_LEVEL_OPTION_SPECS` table (#91869 class proven), `/proc` enumeration numeric-only, mimic matrix (`vim my-gateway-run-notes.txt`, `pi --profile gateway status`, fake basenames…) rejected while the one real unit matches |

Supplementary (all green): snapshot caps — 1 GiB-class skip WITH reason,
zeroed-SQLite guard, post-copy `quick_check` round-trip on real WAL db, keep=1
prune floored, pruning suppressed when protected files skipped, fleet parity of
snapshot ids across profiles (#66140); apply — real fast-forward pull,
diverged-tree fails GIT-classified with NO zip fallback on POSIX and local
commit survives, dep-install failures never clobber (#87304 rationale),
double dirty refusal up-front + TOCTOU with staging-artifact filter under
`--untracked-files=all`, `.git`/`.env`/`node_modules` preserved through the
two-phase swap, dist/ graft rides the swap; restart — fleet SIGUSR1 drain on
real spawned units, wedged unit stopped-after-window then fail-closed
incomplete with healthy unit still drained, verdict table
`_restart_phase_failure_is_incomplete` ported verbatim; verify — settle ~2s
ONLY after actual restarts on injected clock, unknown never fails; hangup —
two-exec-hop SIGHUP survival, `/proc/<pid>/status` SigIgn bit evidence for
non-Node children, unwrapped control dies BY SIGHUP, window absorption +
idempotent restore (DEC-042).

### Footprint audit

`git status --porcelain --untracked-files=all`: **only** modified
`PROGRESS.md` (protocol bookkeeping log entry) + untracked
`src/pi_embedded/update/**`. No other tracked file touched. Wall-clock reads
exist ONLY in `clock.ts` (the seam); stage logic is injection-clean.

### Blocked threads

**None.** Interim report gaps resolved: (1) update subsystem built and green;
(2) lifecycle wiring landed per DEC-040; (3) full-suite starvation fixed by
DEC-041 serialization (311s flaky → 51s ×2 stable); (4) zero skipped/todo tests
remain.

### Proposed DECs

None required — no divergence from Hermes semantics found during verification.
DEC-039..042 (sidecar tick lock, stage-wiring debt, suite serialization,
hangup protection) were logged pre/post implementation and are all accepted in
`09-open-questions.md`.

### Cosmetic notes (non-blocking, left untouched)

- `pipeline-two-process.test.ts`: duplicated JSDoc block above `makeDrill`
  (stale comment left above its replacement).
- `latest.json` torn-write safety is by-construction (temp+rename) and
  exercised via terminal-path assertions; crash-mid-write itself has no unit
  test (not unit-testable without fault injection).

## Final verdict

**Phase 5 = COMPLETE PASS.** All six exit-criteria rows (a)–(f) measured PASS.
Gates: tsc clean, layering clean, secret-scope clean, **1694/1694 over 153
files — twice consecutively**. Footprint exactly as scoped. No blocked threads,
no new DECs proposed.
