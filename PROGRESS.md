# PROGRESS — Pi Gateway build

Tracking protocol per EXECUTION-PROMPT: before phase N starts, phase N−1 exit
criteria are re-verified and recorded here. Reports stay compact; artifacts carry
detail. Orchestrated via multi-agent workflows; commits are atomic per workstream.

## Status board

| Phase | Name                              | State        | Exit evidence |
| ----- | --------------------------------- | ------------ | ------------- |
| 0     | Runtime spike                     | DONE ✅      | 46/46 tests green (×3 + independent rerun); `spike/SPIKE-REPORT.md`; DEC-023 verification appended |
| 1     | Spine                             | DONE ✅      | 329/329 tests; all exit criteria measured PASS; `reports/PHASE-1-REPORT.md` |
| 2     | Streaming, obligations, registry  | not started  | — |
| 3     | Reference adapters + conformance  | not started  | — |
| 4     | Security + multiplex              | not started  | — |
| 5     | Embedded services + update        | not started  | — |
| 6     | Census ports                      | not started  | — |

## Phase 0 — Runtime spike

Entry check: docs 01–08 stable v1.0 candidate; DEC-001..022 ratified; DEC-023
already logged (2026-08-23). ✔ (spec dir verified stable-candidate)

Goal: prove the three riskiest mechanisms in TypeScript on this runtime —

1. Two-layer turn lease (02 §5 / DEC-004): in-process registry +
   cross-process DB lease keyed on compression-lineage root resolved
   in-transaction; dead-PID reclaim; TTL 300s; exactly-one winner under N racers.
2. Stream consumer parity (04 §5.2): prefix-stable drafts, `finish()` final,
   `_interim_send` popped at BOTH egress doors, seal-interception, non-prefix
   draft mutation detected.
3. WAL ladder (02 §1.1): WAL open, busy_timeout, step-down on SQLITE_BUSY,
   two-process writer/reader contention survives.

Spike code lives under `spike/` only (throwaway — never ships). Test shapes port
into Phases 1–2 suites. On exit: attach passing runs to DEC-023 verification entry
in `../09-open-questions.md` (append-only).

- Phase 1 completion (2026-08-23): workflow `pi_gateway_phase_1_completion` closed
  all four blocked threads — guards (59 tests: sync-install interleave⇒ONE turn,
  stale-lock no-heal-without-owner, drain boundary, cap32, forged-events both
  guards, L1×L2 lease interplay incl. two-process SIGKILL reclaim), resolution
  contracts (70 tests: single-flight N→1, adopt-before-mint, WhatsApp alias-flip
  convergence, participant re-key; fixed alias-walk seed bug), agent_core runner
  on real pi SDK loop (41 tests incl. cache-stability byte-identical system prompt
  - toolset hash across consecutive turns). FINAL: 329/329 green, tsc clean,
  layering exit 0. DEC-027/028 logged pre-merge. Known hygiene item: transient
  EPIPE in spike/tests/lease.spike.test.ts teardown under full parallel load
  (Phase 0 throwaway; disappears when spike/ is retired after Phase 2 ports its
  shapes).

## Log

- Session start: repo at LICENSE-only initial commit; toolchain Node v26.7.0 /
  npm 11.19.0 verified; npm registry + git remote reachability confirmed.
- Phase 0 (2026-08-23): workflow `pi_gateway_phase_0_spike` — scaffold + 3 parallel
  proof agents + verifier. PASS: 46/46 (12 lease / 8 stream / 22 WAL contracts +
  4 driver-evidence), tsc clean, footprint confined to spike/ + harness configs.
  Commits `8e85b9f` (scaffold) + `f76b515` (proofs) pushed to origin/main.
  DEC-023 verification entry appended in ../09-open-questions.md. Residuals for
  later phases: SIGHUP→SIG_IGN installability, ws transport, Windows-gated test
  variant.
- Phase 1 / lifecycle skeleton (2026-08-23): `src/pi_gateway/lifecycle/**` —
  binding ten-stage startup engine (01 §3.1; optional stages 7–9 degrade loudly
  per-service, required abort), duplicate-instance guard (PID file O_EXCL,
  SQLite-held runtime lock, getRunningPid evidence chain), takeover handshake
  (marker-before-SIGTERM, ≤10s @0.5s, force+confirm, give-up cleanup), boot
  fingerprint + code_skew detect, gateway_state.json stamps (08 §4 field set),
  shutdown classes takeover/planned/unexpected (#42675 stop-persist suppression,
  exit codes 0/0/1), ordered drain with flush-before-clear backstop +
  pending_messages recovery files + replay-on-boot, double-signal fast-exit
  releasing locks pre-exit. Layering lint `scripts/check-layering.mjs` wired as
  npm `check:layering`. Tests: 49/49 lifecycle (stages 12 · guard 16 ·
  two-process takeover 2 · shutdown 12 · layering gate 7); full repo 159/159;
  tsc clean. Proposed DEC-027: runtime lock held as open BEGIN IMMEDIATE on a
  dedicated `<home>/gateway.lock.db` sidecar (Node core has no flock; new lock
  deps suspect per 01 §5.2) — semantics identical to fcntl locks (live-process
  ownership, OS auto-release on death).
