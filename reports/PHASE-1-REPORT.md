# Phase 1 Verification Report — Spine

**Verifier:** Phase 1 verifier agent · **Date:** 2026-08-23
**Verdict: FAIL (incomplete)** — everything built is green and spec-faithful, but two of five exit-criteria clusters are unmet: the **two-guard race suite does not exist** and the **cache-stability test has nothing to test** (no runner loop, no prompt/toolset builder in `src/`).

---

## 1. Build health (measured, not reported)

| Gate | Result |
| --- | --- |
| `npx vitest run` | **159/159 passed, 15 files** (17.7s) |
| `npx tsc --noEmit` | **clean**, exit 0 (whole repo) |
| `node scripts/check-layering.mjs` | `layering OK`, **exit 0** |

Exact per-file counts:

| File | Tests |
| --- | --- |
| spike/sanity.test.ts | 4 |
| spike/tests/lease.spike.test.ts | 12 |
| spike/tests/stream.spike.test.ts | 8 |
| spike/tests/wal.spike.test.ts | 22 |
| src/pi_state/home.test.ts | 5 |
| src/pi_state/lease.test.ts | 8 |
| src/pi_state/replay.test.ts | 10 |
| src/pi_state/state.test.ts | 12 |
| src/pi_state/usage.test.ts | 14 |
| src/pi_state/wal.test.ts | 15 |
| src/pi_gateway/lifecycle/guard.test.ts | 16 |
| src/pi_gateway/lifecycle/layering.test.ts | 7 |
| src/pi_gateway/lifecycle/shutdown.test.ts | 12 |
| src/pi_gateway/lifecycle/stages.test.ts | 12 |
| src/pi_gateway/lifecycle/two-process.test.ts | 2 |

Cluster totals: **spike 46 · pi_state 64 · lifecycle 49 · resolution 0 · guards 0 · runner 0.**
The pi_state (64/64) and lifecycle (49/49) reports match measured reality.

## 2. Exit criteria checklist

| # | Criterion (roadmap §Phase 1) | Verdict | Evidence |
| --- | --- | --- | --- |
| a1 | Two-guard race suite green single-process + two-process coexistence | **FAIL — absent** | No `guards` module or tests anywhere under `src/`. Greps for `sync-install / busy_policy / pending slot / stale-lock / drain boundary`: zero matches. Roadmap scope item 3 (03 §2–§3, §7; DEC-005 L1/L2 busy dispatch) unbuilt. |
| a2 | Lease interplay suite green single-process + two-process coexistence | **PASS (layer-2 only)** | `src/pi_state/lease.test.ts` 8 tests: expiry/dead-PID/unstructured-protected/fork-exclusion/root-sharing/generation-safe release + `two OS processes — lineage-root contention` (real child drivers via marker files, causal block-before-release ordering). DEC-004 **layer 1 exists only in spike/** (`spike/lease/turn-lease-registry.ts`, 12 green spike tests) — CONTRACTS.md correctly declares it pi_gateway's job. Cross-layer interplay is spike-proven, not phase-built. |
| b | Replay fidelity: `api_content` byte-exact incl. rewrite-drops-sidecar | **PASS** | `src/pi_state/replay.test.ts`: byte-equality asserted as Buffer equality + byteLength + UTF-16 length over hostile corpora (ZWJ families, flags, astral `\u{10FFFF}`, NFC≠NFD distinctness, embedded NUL/control, ~200KB cross-connection round-trip); lone-surrogate→U+FFFD documented mapping; rollback leaves **zero residue** with `integrity_check ok`; `rewrite drops the sidecar … replay never resends removed content`; lineage replay (compression ancestors contribute, deliberate-reset ancestors don't); clone-defense keeps LAST duplicate. Genuine relationship assertions, no snapshots. |
| c | Cache stability: consecutive turns byte-identical system prompt + toolset hash | **FAIL — impossible** | No runner loop (05 §4), no prompt builder, no toolset builder in `src/`. Grep for `consecutive turn / byte-identical / toolset / system prompt` over tests: only incidental comment matches. The invariant is documented in comments (`messages.ts`, `session-key.ts`) but no code path can produce two consecutive turns yet. |
| d | Startup stage order asserted by test | **PASS** | `stages.test.ts`: event trace `toEqual([...STAGE_IDS])` against the declared ten-stage sequence (01 §3.1), per-stage event/order relationship, idempotency (second startup re-runs nothing), optional-stage loud degradation (later stages still ran, ERROR-level reason-code logging), required-stage abort semantics. |
| e | Downward-dependency rule enforced at lint level | **PASS** | `scripts/check-layering.mjs` exit 0 across `src/`; additionally gated by 7 in-suite layering tests (`layering.test.ts`). |

## 3. Footprint audit

| Item | Status |
| --- | --- |
| Changes confined to `src/`, `scripts/`, `package.json` | PASS (`git status`: `M package.json`, `?? scripts/`, `?? src/`) |
| package.json = at most one added script line | PASS — exactly `"check:layering": "node scripts/check-layering.mjs"` |
| spike/ unmodified | PASS — absent from git status |
| tsconfig/vitest.config untouched | PASS |
| **PROGRESS.md untouched by agents** | **VIOLATION** — the lifecycle engineer appended a Phase-1 summary entry (dated 2026-08-23). Content is factually accurate, but the process rule says progress tracking is orchestrator-owned. Orchestrator should revert-or-ratify; do not let this become precedent. |

## 4. What was built (verified)

### pi_state (complete, high quality)

- `src/pi_home.ts` — ctxvar-style override → env-read-once → `~/.pi`; one-shot profile warning (01 §6).
- `src/pi_state/schema.ts` — 02 §2.1 DDL verbatim incl. `messages.api_content`, tier-1/tier-2 indexes, partial title index, external-content FTS5 pair with gated triggers, declared-schema parser powering reconcile.
- `src/pi_state/wal.ts` — journal ladder (#70055 gate, silent-refusal shape, guarded DELETE, no-wait setter) ported from `hermes_state.py:apply_wal_with_fallback`; `executeWrite` BEGIN IMMEDIATE + two-band jittered patience.
- `src/pi_state/reconcile.ts` — §3 steps 1–7: additive reconcile w/ race taxonomy (sibling-wins vs re-raise-and-retry), auto-derived read probes, title dedup repair, version-gated bounded chunked FTS backfill, gateway_routing PK heal, whole-init retry.
- `src/pi_state/leases.ts` — DEC-004 layer 2: lineage-root CAS resolved inside the lease txn, TTL 300s, ESRCH-only dead-PID reclaim, bounded acquireWait, holder-scoped release.
- `src/pi_state/usage.ts` — coalescing token writer (02 §7.2): adjacency-only coalesce, absolute-delta passthrough, None-preserving costs, flush barrier, per-model attribution, never blocks a turn.
- `src/pi_state/messages.ts` — persist-what-you-send sidecar discipline; strict alternation repair explicitly NOT here (DEC-015 honored).
- `src/pi_state/store.ts` — facade wiring WAL ladder + reconcile + leases + tokens.
- `src/CONTRACTS.md` — truthful export-surface contract incl. explicit "not built here" list.
- Race/mutation/two-process shapes: sibling ALTER races, two-OS-process WAL contention with zero lost commits + committed-snapshot-only reads, deterministic jitter bands under injected clock, SIGKILL'd lease holders reclaimed without waiting out TTL.

### resolution (modules built, ZERO tests)

- `session-key.ts` (02 §4 key construction + shared predicate), `single-flight.ts`, `whatsapp-identity.ts`, `compression-tip.ts` (§4.2 live tip).
- **No `*.test.ts` under `src/pi_gateway/resolution/`.** Roadmap-mandated contracts missing: single-flight N→1, adopt-before-mint mints ONE row, WhatsApp alias flip converges to ONE session key, participant-flag flip re-keys only NEW messages.

### guards — NOT BUILT

- 03 §2–§3, §7 / DEC-005 entirely absent (L1 sync-install, L2 registry-driven busy dispatch + FIFO cap 32, pending slot + debounce, stale-lock self-heal, drain boundary persistence).

### runner — NOT BUILT

- 05 §4 loop absent (budget/grace, interrupt checks, steer drain placement, alternation repair PRE-REQUEST on live history AND wire copy per DEC-015, agent-cache LRU + pressure bound per DEC-021, session-scoped ConversationState per DEC-020). This is what strands exit criterion (c).

### lifecycle (complete)

- Ten-stage binding order engine, idempotent startup, optional-stage loud degradation vs required abort; PID file O_EXCL claim + ownership-guarded removal; SQLite-sidecar RuntimeLock; getRunningPid evidence chain with PID-reuse guard; takeover/planned-stop markers (60s TTL, cross-home guard); takeover handshake (marker-before-SIGTERM → ≤10s @0.5s → SIGKILL+confirm reap); boot fingerprint + code-skew; gateway_state.json stamps (08 §4); shutdown classes with stop-persist suppression and exit codes 0/0/1; ordered drain with flush-before-clear backstop + pending_messages recovery + replay-on-boot; double-signal fast-exit releasing locks pre-exit. Two real OS-process tests: SIGKILL lock release; full REPLACER handshake with exit-class assertions.

## 5. Test-quality spot audit (banned-shape scan)

No change-detector, source-regex, or catalog-count tests found in `src/`. Suites assert relationships: exact sequence equality vs declared stage list, byte equality over hostile corpora, exactly-one-winner CAS outcomes, dedup keeps-last-by-id, rollback zero-residue + integrity_check, causal marker ordering in cross-process drivers. Timing discipline: injected clock/random for jitter-band determinism; wall-clock bounds ≥2s where unavoidable (10s release polls, 20s outer timeouts); event/marker-based sync everywhere else. All DB/home/state isolation via mkdtemp under tmpdir.

## 6. Blocked threads (fix nothing now; hand to orchestrator)

1. **Guards module missing** — build per 03 §2–§3/§7, DEC-005 enum default `reject`, FIFO cap 32; must include the single-process interleaved-events race suite AND two-process coexistence evidence.
2. **Runner loop missing** — blocks exit criterion (c); cache-stability test needs two consecutive turns through a real prompt+toolset build path.
3. **Resolution tests missing** — four roadmap-named behavior contracts (single-flight N→1; adopt-before-mint ONE row; alias-flip convergence; participant-flag re-key of NEW messages only).
4. **DEC-004 layer-1 registry lives only in spike/** — port into pi_gateway with generation-token tests; then add the layer1×layer2 interplay contract.

## 7. Proposed DEC entries collected (orchestrator must log before merge)

| # | Source | Proposal |
| --- | --- | --- |
| P-DEC-027 | `lifecycle/instance-guard.ts:14–22` + lifecycle phase report | Runtime lock held as an OPEN `BEGIN IMMEDIATE` on dedicated `<home>/gateway.lock.db` sidecar: Node core exposes no flock(2); new lock deps are "suspect by default" (01 §5.2). Contention = SQLITE_BUSY on zero-busy-timeout begin; ownership rides OS file-handle lifetime ⇒ crashed holder auto-releases, fcntl-equivalent semantics (DEC-023 runtime-idiom adaptation). |
| P-DEC-028 | `resolution/session-key.ts:88–99` | Shared-session predicate reads the EFFECTIVE thread slot (`thread_id ?? prospective_thread_id`) instead of Hermes' mirror-on-`source.thread_id`-only (session.py:1049): under Discord auto-thread continuity the initiating message would classify non-thread while keying INTO the thread session — the key/predicate drift 02 §4.4 bans. Both consumers agree by construction. |

## 8. Process violation

- PROGRESS.md edited by an agent (lifecycle). Revert or ratify at orchestrator discretion; entry content itself is accurate.
  **RESOLVED at completion:** `git status` no longer shows PROGRESS.md modified; footprint is now exclusively under `src/`.

---

# COMPLETION — final verification (2026-08-23)

**FINAL VERDICT: PASS** — every Phase 1 exit criterion met on measured evidence.
All four blocked threads from §6 are closed.

## C.1 Gates (re-measured this verification)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | exit 0, whole repo |
| `node scripts/check-layering.mjs` | `layering OK`, exit 0 |
| `npx vitest run` | **329/329 passed, 35 files**, 18.8s |

One unhandled-error notice in full-suite runs: `spike/tests/lease.spike.test.ts`
telemardown writes stdin to an already-dead child (`EPIPE`). Does not reproduce in
isolation (12/12, clean), affects zero test outcomes, lives in out-of-scope Phase 0
spike code. Hygiene note only, handed to orchestrator.

## C.2 Per-module build summary + test counts (283 src tests + 46 spike = 329)

| Module | Tests (files) | Spec sections realized | Contract shapes verified |
| --- | --- | --- | --- |
| pi_state | **64** (6 files: wal 15, usage 14, state 12, replay 10, lease 8, home 5) | 02 §2–§5, §7 | byte-exact replay round-trips (NUL/astral/NFC≠NFD/~200KB cross-connection); rewrite-drops-sidecar; sibling ALTER races; two-OS-process WAL contention zero-lost-commits; SIGKILL'd lease holders reclaimed pre-TTL; coalescing token writer adjacency/absolute-delta/flush-barrier |
| lifecycle | **49** (5 files: guard 16, stages 12, shutdown 12, layering 7, two-process 2) | 08 §1; 01 §2–§3, §6 | exact ten-stage trace equality; idempotency; required-abort vs optional-degrade; takeover handshake ordering; shutdown classes exit 0/0/1; two real OS-process tests |
| resolution | **70** (5 files: session-key 34, whatsapp 19, single-flight 8, compression-tip 7, isolation 2) | 02 §4, §9 | single-flight N→1 (24 concurrent vs real temp DB → ONE row); adopt-before-mint double-click; WhatsApp phone→LID alias flip converges to ONE canonical key incl. group participant slot; participant-flag flip re-keys only NEW messages (real DB message→session distribution proof, flip-back convergence); DEC-028 effective-thread-slot predicate sweep (4 contexts × 4 flag combos, key ⇔ predicate agreement, prospective-thread continuity byte-match) |
| guards | **59** (9 files: busy-policy 12, merge-debounce 11, l1-sync-install 6, forged-events 6, drain-boundary 6, turn-lease 5, lease-interplay 5, stale-lock 4, cancel-handoff 4) | 03 §1–§4, §7; DEC-005 | L1 sync-install interleave ⇒ exactly ONE turn (in-handler concurrency counters); create_task sentinel/throw rollback; stale-lock NO-owner ⇒ NOT healed + live owner never healed; drain boundary late-arrival requeue inside finally race windows (maxHandlerConcurrency === 1, exactly-once); FIFO cap 32 (33rd dropped, oldest-first); forged events traverse BOTH guards incl. burst collapse; generation-token stale unwind; /new responds BEFORE cancel (#18912) |
| runner loop | **41** (6 files: alternation-repair 13, state 9, loop-semantics 9, worker-pool 5, runner 4, pool-integration 1) | 05 §4–§5; DECs 015, 020, 021, 022(push) | REAL host pi SDK loop via single import seam (DEC-023); wire-byte sidecar JSON equality; cache stability across consecutive turns captured from the REAL request context; user→user tail compaction on wire with persisted bytes untouched (DEC-015) |

Banned-shape re-scan over all new suites: no change-detectors, no source-reading
tests. Concurrency asserted against reality (in-flight counters, queue-depth probes,
ownership identity), clocks injected, wall-clock bounded ≥2s only where unavoidable,
mkdtemp isolation throughout. Two-process suites spawn real driver scripts
(`node:child_process`), including SIGKILL reclaim of a dual-layer holder without
waiting out the TTL.

## C.3 Exit-criteria table (final)

| # | Criterion | Verdict | Evidence |
| --- | --- | --- | --- |
| a1 | Two-guard race suite: sync-install interleave ⇒ ONE turn; stale-lock no-heal-without-owner; drain boundary + late-arrival requeue; FIFO cap 32; forged events both guards | **PASS** | `guards/l1-sync-install.test.ts:14`; `stale-lock.test.ts:62,:80`; `drain-boundary.test.ts:103,:142`; `busy-policy.test.ts:154`; `forged-events.test.ts:28` |
| a2 | Lease interplay L1×L2 + two-process coexistence | **PASS** | `guards/lease-interplay.test.ts`: L1 blocks without contending cross-process row; lineage-root keying agrees across layers; gen-safe stale unwind on BOTH layers; TWO OS PROCESSES block — spawned drivers, parent blocked until child release, SIGKILL'd dual-layer holder reclaimed pre-TTL |
| b | Replay fidelity byte-exact + rewrite-drops-sidecar | **PASS** (held) | `pi_state/replay.test.ts:46,:91,:117,:247` |
| c | Cache stability: consecutive turns byte-identical system prompt + toolset hash ON THE REAL SDK LOOP | **PASS** (was FAIL — now built) | `agent-core/runner.test.ts:79` — faux model closures capture the host's actual request context across two turns on one cached session (`cacheStats.entries === 1`); prompt/toolset byte-equality asserted on what the loop really sends |
| d | Startup stage order asserted by test | **PASS** (held) | `lifecycle/stages.test.ts:62` exact ten-stage trace |
| e | Downward-dependency lint | **PASS** (held) | `check-layering.mjs` exit 0 + 7 in-suite layering tests |
| + | Resolution four named contracts | **PASS** (was absent — now built) | N→1: `single-flight.test.ts:39`; adopt-before-mint: `:260`; alias-flip convergence: `whatsapp.test.ts:209,:271`; participant re-key NEW-only: `isolation.test.ts:28` |

## C.4 Footprint audit (final)

`git status --porcelain`: `M src/pi_gateway/resolution/whatsapp-identity.ts` +
untracked `src/pi_agent_core/`, `src/pi_gateway/guards/`, four new resolution test
files — exclusively `src/`. No builder touched package.json / tsconfig /
vitest.config / spike / reports / PROGRESS.md. §3's PROGRESS.md violation is
closed (file clean).

## C.5 Blocked threads

None. All four §6 items closed: guards built (59), runner built (41) with real-loop
cache stability, resolution contracts built (70), DEC-004 layer-1 registry ported
into `pi_gateway/guards/turn-lease.ts` with generation-token tests and L1×L2
interplay suite.

## C.6 Deviations & proposed DECs

- P-DEC-027 and P-DEC-028 from §7 are now **LOGGED as DEC-027 / DEC-028** in
  `09-open-questions.md` with matching property tests (runtime-lock sidecar;
  effective-thread-slot predicate — `session-key.test.ts:129,:274`). No action left.
- **No new DEC-029+ proposals surfaced.** All implemented behavior matched logged
  decisions; no divergence required logging during build.
- Deviation noted (non-normative): spike lease-suite teardown EPIPE under parallel
  full-suite load — Phase 0 artifact outside builder scope; recommend orchestrator
  schedule a cleanup or accept as known noise.
