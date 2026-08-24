# PHASE 4 REPORT — Security Hardening + Multiplex

**Verifier:** independent re-run of every gate; nothing taken from builder reports on trust.
**Status:** Phase 4 close · **Repo:** `/root/pi-gateway/pi-gateway` · **VERDICT: PASS**

## 1. Gates (measured by verifier, final)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** |
| `node scripts/check-layering.mjs` | **exit 0** — `layering OK (downward-only holds across src/)` |
| `node scripts/check-secret-scope.mjs` (NEW) | **exit 0** — clean across **173 files** under `src/` (prod only; `*.test.ts` excluded by design) |
| Gate provably able to FAIL (seeded fixture) | seeded mkdtemp project with one file per banned-shape class → **exit 1, all three rules fire**: `UNSCOPED_CATCH_FALLBACK` (catch naming `UnscopedSecretError` touching `process.env` outside the wrapper), `COALESCED_SCOPED_MISS_FALLBACK` (`getSecret("K") ?? process.env.K`), `RAW_ENV_BESIDE_SCOPE` (scope-referencing file with a raw env read). The SAME catch shape planted AT the canonical wrapper path is exempt — verified in the fixture AND self-tested in-repo (`secretscope/gate.test.ts`, mutation-style: gate detects its own banned shapes and exempts the sanctioned copy). |
| `npm test` (full repo) | **1204/1204 PASSED, 103 files, ~19 s**, 0 failures |
| Baseline reconciliation | Phase-3 baseline 873/873 (74 files) → **+331 tests / +29 test files**, all inside the Phase-4 footprint |

## 2. Footprint audit

```
modified:  package.json            (+1 line: "check:secrets" npm script — the ONE authorized line)
new:       scripts/check-secret-scope.mjs
new:       src/pi_gateway/security/{secretscope,authz,tokenlock,trust,multiplex}/   (61 files)
new:       src/pi_gateway/delegation/                                               (12 files)
```

Zero modifications under `src/pi_state/**`, `src/pi_platforms/**`, `src/pi_agent_core/**`, PROGRESS.md, SPEC_DIR, tsconfig/vitest config. The builders' claim of "zero adapter edits" is confirmed by git status: the kit secret-reader and token-lock seams are consumed structurally (declared mirrors asserted at type level in `wrapper.test.ts`), so no adapter-side wiring was needed. Construction sites for `PluginContext` do not exist yet; `kitScopedSecretReader()` is documented as the pass-through for when the runner grows them (Phase 5).

## 3. Per-module summary

### Secret scope engine — `src/pi_gateway/security/secretscope/` (12 files, 51 tests)

`resolve.ts` implements the 06 §3 resolution table **verbatim** (verified line-by-line against spec): global-env carve-out → scope hit → scoped-miss + multiplex ACTIVE = declared default (**fail closed**) → scoped-miss + multiplex OFF = env overlay (cron parity) → no scope + ACTIVE = raise `UnscopedSecretError` → no scope + OFF = legacy env. Presence-parity: `""` is a hit. `global-env.ts` ports `_GLOBAL_ENV_EXACT/_PREFIXES` with documented HERMES_*→PI_* renaming; credentials deliberately excluded even when prefixed (`API_SERVER_KEY`, `GATEWAY_RELAY_SECRET/ID/DELIVERY_KEY` stay profile-scoped — checked against Hermes anchor `agent/secret_scope.py:@98/@135`). `scope.ts`: AsyncLocalStorage ≙ ctxvar, set/reset tokens with double-reset AND out-of-order (stale-token) detection matching Python's `ContextVar.reset` ValueError. `wrapper.ts`: THE single sanctioned `catch UnscopedSecretError → process.env` copy in the repository (DEC-003 as amended by DEC-009), grep-gate exempts exactly that path. `.env` loader never touches process env.

### Authz decision chain + DM pairing — `src/pi_gateway/security/authz/` (11 files, 88 tests)

`decision.ts` is THE §2.1 chain as a table-driven matrix proving each precedence pair: gate 0 system platforms → gate 1 upstream-auth delegation (exact-`true` marker discipline) → …→ gate 3 `{PLATFORM}_ALLOW_BOTS` bypass **ahead of** the gate-4 no-user-id deny (A6/06 §2.5, unknown values normalize to `none`) → allowlist union → default-deny gate 10. `#34515` regression rows present in both DM and group shapes (own-policy adapter with open dm_policy/group_policy DENIES fail-closed). Platform tables are DATA (18 platforms' env-var maps, pairing-mirror targets). Both accessors (`authEnv`/`platformGateEnv`) route through the canonical wrapper — zero raw env reads (gate-clean). Pairing lifecycle: 8-char unambiguous codes, salted-hash-only persistence (never plaintext), exact-TTL boundary, per-user rate limit, capacity cap, failed-attempt lockout (#10195) with success-streak reset, request-id admin approval, durability across full close/reopen.

### Token locks — `src/pi_gateway/security/tokenlock/` (11 files, 27 tests)

Staleness rungs: live-PID+start_time match → ours → dead (kernel-proven ESRCH or zombie ⇒ dead) / start_time mismatch (PID-reuse guard via `/proc/<pid>/stat` field 22) → conservative-inert cmdline oracles. **No TTL wait anywhere — liveness IS staleness.** Two-process contracts over real processes: refusal names the holder while A lives; **SIGKILL A ⇒ B acquires in < 2 s wall** (the only wall bound, measured kill-to-reclaim); racing starters on one stale record yield exactly one winner; inventory sees cross-process holders; foreign-PID release is a no-op. Records carry `identity_hash = sha256[:16]` — raw credential never on disk — plus OOF-3 profile label; refusals are fatal adapter errors NAMING the holder.

### HTTP-ingress trust boundaries — `src/pi_gateway/security/trust/` (17 files, 81 tests)

Scheme registry AS DATA (06 §8.1): six schemes with detection headers/skew/rotation/deprecation. Negative matrix per scheme (tampered / wrong-ts binding / expired >300 s / boundary-exactly-300s admits / malformed key fails closed). Anti-downgrade COMMIT: V2 missing/malformed/expired timestamp rejects, never falls through to deprecated V1; first-presented-wins dispatch; route pinning; secret-configured-without-headers fails closed; deprecation warn-once-per-route. Constant-time comparator: structural non-early-exit construction (both operands fully materialized before exactly one `timingSafeEqual`; length mismatch burns a comparison on a fixed scratch buffer), non-ASCII hostile headers fail closed instead of raising, byte-position mutation matrix rejects identically. Confinement escape matrix (`relative_to` port): `../`, absolute-outside, `~/.hermes` alias, `$HOME`/`${VAR}` expansion, lexically-inside-symlink all refuse. msgraph CIDR gating, bounded replay seen-set (fresh delivery admits, identical re-request inside window rejects as REPLAY), api_server opt-in session headers, artifacts TTL/size/MIME caps, body caps.

### Multiplex profile isolation — `src/pi_gateway/security/multiplex/` (10 files, 41 tests)

`profile-env.ts`: fail-closed profile-scoped reads in every mode; presence-parity; scoped miss → declared default or `ProfileEnvMissingError`; no-scope → `UnscopedSecretError`. **Poisoned-env contract runs the REAL §2.1 chain with process env poisoned in BOTH directions**: profile-B sender denied while profile-A allowlist would admit, and vice versa; poisoned `ALLOW_ALL`/`ALLOW_BOTS` flags never flip a scoped profile open; attacker-id-in-env-only admitted by NO profile. Over-correction guards prove the two LEGITIMATE paths stay reachable (multiplex-OFF overlay fallthrough; unscoped default-profile sanctioned read). `withProfileIsolation` installs home override + secret scope + profile stamp together; exception hygiene resets ALL THREE layers, including nested-B-throwing-inside-A and exceptions crossing both boundaries; concurrent turns never cross scopes. check_fn caches keyed `(fn, scope)` with distinct-fn-object discipline, TTL re-probe, FIFO 512 cap, multiplex-OFF process-wide bypass, ON-flip without restart, request-bound-session triple-field bypass, last-good-True grace window without caching failures. Per-profile pairing/authz stores are DISTINCT instances over DISTINCT connections; adapter-view refusal router resolves only the RIGHT profile's view.

### Async-delegation durable rail (store side) — `src/pi_gateway/delegation/` (12 files, 43 tests)

Producer: `recordDispatch`/`publishCompletion` (≡ `_persist_dispatch`/`_persist_completion`). Handshake: `claimCompletion`/`releaseClaim`/`dropClaim`/`completeClaim`/`markDelivered` — each one CAS'd guarded UPDATE inside `BEGIN IMMEDIATE`, so two engines racing get one winner. Boot restore `restoreUndelivered` (+ `recoverAbandoned` for dead-owner running rows, PID-recycling-aware via start ticks). **Exactly-once crash recovery proven over real processes**: SIGKILL between claim-write and ack ⇒ boot 2 replays EXACTLY once (`secondRestore=0`, `attempts=2`, stale takeover won), durable `event_json` bytes unchanged (restored flag is in-memory only, asserted byte-for-byte), boot 3 sees nothing. N=6 processes race one atomic claim ⇒ exactly one winner, attempts increment once. 48h prune keeps active states and delivered receipts per policy. Injected manual clock throughout; two-process tests are the only wall-bounded ones (generous timeouts).

## 4. Exit-criteria table (roadmap §Phase 4)

| # | Criterion | Verdict | Evidence |
| --- | --- | --- | --- |
| a | Grep gate clean AND provably able to fail on every banned-shape class | **PASS** | exit 0 on tree (173 files); seeded fixture → exit 1 with all three rules firing; sanctioned-wrapper exemption proven in fixture + self-tested in `gate.test.ts` |
| b | Poisoned-env: profile-B denied while profile-A allowlist would admit, both directions | **PASS** | `poisoned-env.test.ts` runs the real decision chain, env poisoned both directions incl. ALLOW_ALL/ALLOW_BOTS flags; attacker-id admitted nowhere; legitimate overlay + sanctioned paths guard against over-correction |
| c | Negative signature matrix per scheme + constant-time structural proof | **PASS** | 24 scheme tests (per-scheme tamper/ts-binding/expiry/boundary/rotation/pinning/fail-closed/replay) + comparator construction with burned-comparison length path, non-ASCII fail-closed, compute-before-inspect ordering |
| d | Kill-holder lock reacquisition green (SIGKILL, well under TTL) | **PASS** | two-process test: genuine first refusal proven (`refusedFirst=true`), SIGKILL → reclaim measured < 2 s wall; engine has NO TTL wait |
| e | Crash-recovery restores undelivered completions EXACTLY once | **PASS** | SIGKILL between claim and ack over real processes; 3-boot exactly-once with byte-for-byte durability of `event_json`; plus N-process atomic-claim race |
| f | Pairing lifecycle + multiplex isolation + decision-order matrix genuine | **PASS** | 27 pairing contracts (TTL boundary, lockout, capacity, hash-only persistence); table-driven §2.1 matrix with per-pair precedence rows incl. A6 bypass order and #34515 regressions; isolation proven across env/pairing/check_fn/home layers |

**Scope-item coverage vs roadmap §Phase 4:** items 1–6 all delivered. Item 6 explicitly storeside-only ("watcher wiring and idle-gated re-entry land in Phase 5") — consistent with 06 §7.2/DEC-018 phasing.

**Spec doc observation (flagged, not fixed — SPEC_DIR off-limits):** the roadmap's Phase-4 block carries an "Exit criteria"/"tests to port" list duplicated verbatim from Phase 5 (cron bounds, handoff E2E, update drill, receipts, shutdown signals). Those belong to Phase 5's scope (§Phase 5 items 1–7); Phase 4's normative Scope list (items 1–6 above) is complete and self-consistent. Recommend a SPEC_DIR erratum in the next phase transition.

## 5. Blocked threads

None. All six modules shipped compiling + green; no deferred rows inside the Phase-4 footprint.

## 6. Proposed DECs collected from builders (for ratification — DEC-026 discipline: logged BEFORE divergence, none implemented silently)

| ID (proposed) | Module | Divergence from Hermes | Rationale recorded by builder |
| --- | --- | --- | --- |
| DEC-035 | delegation/rail | `claimCompletion` on an UNKNOWN `delegation_id` returns `false`; Hermes returned `true` ("legacy" pre-durable-dispatch events) | Greenfield store has no pre-durable rows; silently succeeding would break the exactly-one-owner contract |
| DEC-035 (companion note) | delegation/rail | `pruneDurable` active-state set includes explicit pre-running `'dispatched'` (Hermes: `'running','finalizing'`) | 06 §7.1 adds the `'dispatched'` vocabulary; both are non-terminal for retention — same policy, richer state names |
| DEC-036 (proposed) | authz/pairing | Pairing state persists in pi_state `state.db` tables (`pairing_pending`/`pairing_approved`/`pairing_rate_limits`, additive idempotent DDL owned by the module) instead of Hermes' per-platform JSON files under `~/.hermes/pairing` | One durable, crash-safe, cross-process substrate already exists (02 §1); JSON files fork gateway state across two storage systems; BEGIN IMMEDIATE mutations reproduce `_secure_write`+RLock semantics; 0600 dir hygiene kept |
| DEC-037 (proposed) | authz/env-accessors | `_auth_env`'s post-scoped-miss `os.getenv` fallthrough collapsed into the canonical wrapper — BOTH authz accessors fail closed to declared defaults | That fallthrough is precisely the banned after-a-scoped-miss shape; 06 §2.2: "§3 has no exceptions for authz reads" (leaked default-profile allow-all class) |
| DEC-038 (proposed) | tokenlock | Rung-6 cmdline-oracle liveness rungs stay CONSERVATIVE/inert until the pi-gateway CLI entrypoint exists; `identityProbe` seam provided to sharpen later | Process identity must never be inferred from argv substrings (banned); unreadable cmdline yields no verdict unless evidence affirmative |

All five are smallest-Hermes-consistent behaviors with recorded rationale, none silent; each needs verifier-owner ratification into `09-open-questions.md`.

## 7. Verifier notes

- Layering holds downward-only across the new engines; `pi_gateway` never imports `pi_platforms` — kit seams are mirrored structurally and type-asserted.
- Test discipline: behavior contracts throughout (state machines, race outcomes, byte-exact round-trips); mkdtemp isolation everywhere; injected clocks in rail/prune/pairing-TTL; real processes for every liveness/exactly-once claim; the grep seal itself is mutation-checked. No change-detectors found in the new suites.
- The 11 lint warnings reported by builders are intentional house patterns (gate script console use mirroring `check-layering.mjs`; deliberate `delete process.env[k]` poison cleanup in tests; escape-aware parser index loops) — reviewed, accepted.
