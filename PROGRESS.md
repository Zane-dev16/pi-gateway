# PROGRESS — Pi Gateway build

Tracking protocol per EXECUTION-PROMPT: before phase N starts, phase N−1 exit
criteria are re-verified and recorded here. Reports stay compact; artifacts carry
detail. Orchestrated via multi-agent workflows; commits are atomic per workstream.

## Status board

| Phase | Name                              | State        | Exit evidence |
| ----- | --------------------------------- | ------------ | ------------- |
| 0     | Runtime spike                     | DONE ✅      | 46/46 tests green (×3 + independent rerun); DEC-023 verification appended; spike retired at Phase 2 exit (report: `reports/PHASE-0-SPIKE-REPORT.md`) |
| 1     | Spine                             | DONE ✅      | 329/329 tests; all exit criteria measured PASS; `reports/PHASE-1-REPORT.md` |
| 2     | Streaming, obligations, registry  | DONE ✅      | 678/678 incl. 349 new (632 after spike retirement); both fake adapter shapes; derivation property; injected-clock caps; `reports/PHASE-2-REPORT.md` |
| 3     | Reference adapters + conformance  | DONE ✅      | 873/873 (74 files), tsc clean, layering clean; all three adapters pass ALL applicable §8 rows; ws gate `allApplicablePassed === true`, zero deferred (DEC-032); DEC-033/034 satisfied by execution; `reports/PHASE-3-REPORT.md` |
| 4     | Security + multiplex              | DONE ✅      | 1204/1204 tests; grep gate clean + provably failing; poisoned-env/sig-matrix/kill-holder/exactly-once-restore all PASS; `reports/PHASE-4-REPORT.md` |
| 5     | Embedded services + update        | DONE ✅      | 1694/1694 ×2 runs; all (a)–(f) measured PASS incl. two-profile fleet drill w/ stale-gateway exit 1 + receipts-on-refusal; `reports/PHASE-5-REPORT.md` |
| 6     | Census ports                      | IN PROGRESS  | — |

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

- Phase 5 / update pipeline (2026-08-24): `src/pi_embedded/update/**` — the
  transactional update subsystem (08 §5–§10): plan stage (deployment-kind
  classification from code-scoped `.install_method` stamp → .git → package.json,
  config.py:detect_install_method parity; UpdatePlan/RuntimeRecord schema),
  snapshot stage (#66140 per-profile state-snapshots, identical critical set,
  1 GiB cap skip-with-reason, keep=1 prune with pruning suppression on protected
  skips, zeroed-db guard + post-copy quick_check), apply stage (argv/git-CLASSIFIED
  failure gates; ZIP fallback strictly git-classified AND win32; #87304 double
  dirty-tree refusal up-front + TOCTOU with staging-artifact filter and -uall;
  two-phase staging swap preserving .git/.env/node_modules; built-artifact graft),
  restart-per-kind (fleet-wide drain-first SIGUSR1, launchers resolved before any
  signal, per-unit isolation, survivors stopped after window, fail-closed verdict
  table `_restart_phase_failure_is_incomplete` ported verbatim), verify stage
  (settle ~2s ONLY after actual restarts on injected clock; fleet sha matrix;
  stale ⇒ partial + exit 1; unknown never fails), receipts (JSON receipt +
  atomic latest.json pointer on EVERY terminal path incl. refusals/exceptions;
  bounded prune keeps 20), canonical process matchers (08 §9: token-based
  gateway/holder subcommand extraction, parser-DERIVED value-flag set from a
  single option-spec table, /proc cmdline enumeration; adversarial argv matrix),
  SIGHUP hangup protection (DEC-042: window listener-absorption + trap-wrapped
  child exec delivering true inherited SIG_IGN to git/package-manager children).
  Tests: 84 new across 10 files — 64 pure contracts + 20 two-process contracts
  (real git pull/diverge/overlay drills; SIGUSR1 fleet drain on spawned units;
  SIGHUP survival across two exec hops + /proc SigIgn evidence; TWO-PROFILE
  stale-gateway drill ⇒ partial + exit 1 per roadmap exit criteria c/f).
  FULL: 1694/1694 green, tsc clean, layering + secretscope gates green.
  DEC-042 logged pre-implementation (Node cannot express SIG_IGN in-process;
  measured caveat recorded: Node resets its own inherited SIGHUP at bootstrap,
  so the binding property targets non-Node children). Layering note: update owns
  a reader for the DOCUMENTED gateway_state.json schema instead of importing
  pi_gateway/lifecycle (01 §5.3); shared contract is the spec field set.

## Log

- Phase 4 (2026-08-24): workflow `pi_gateway_phase_4_security` — scope engine first,
  then authz ∥ locks ∥ trust, then multiplex ∥ delegation rail; verifier incl.
  self-tested grep gate. PASS: 1204/1204 (331 new). DEC-035..038 logged. Roadmap
  v1.2 erratum recorded (Phase-4/5 text duplication owned by Phase 5). Flake fix:
  two-process rail test timeout raised to 300s against fork starvation on 4 CPUs
  (contract unchanged; passes in isolation in ~1.5s).

- Phase 3 close (2026-08-24): verifier sweep of the persistent-ws completion thread — real-engine `WsSubject` + five-row transport fixture + 25 engine tests + ws wiring suite; suite 844→873 (+29, +2 files); footprint confined to `src/pi_platforms/{persistent-ws,conformance,kit}/`; lying-fixture negative validation green; no new DECs required. FINAL verdict PASS (`reports/PHASE-3-REPORT.md`).

- Phase 2 (2026-08-23): workflow `pi_gateway_phase_2_egress` — 4 parallel builders
  - verifier. PASS: 678/678 (349 new: streaming both-fake-shape mutation suite,
  obligations caps under injected clock w/ real-process crash recovery, command
  registry derivation property across six consumers, media offset-mask mutation-
  checked). DEC-029/030/031 logged. spike/ retired (shapes ported; EPIPE artifact
  gone); suite now 632/632.

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
