# PROGRESS — Pi Gateway build

Tracking protocol per EXECUTION-PROMPT: before phase N starts, phase N−1 exit
criteria are re-verified and recorded here. Reports stay compact; artifacts carry
detail. Orchestrated via multi-agent workflows; commits are atomic per workstream.

## Status board

| Phase | Name                              | State        | Exit evidence |
| ----- | --------------------------------- | ------------ | ------------- |
| 0     | Runtime spike                     | IN PROGRESS  | — |
| 1     | Spine                             | not started  | — |
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

## Log

- Session start: repo at LICENSE-only initial commit; toolchain Node v26.7.0 /
  npm 11.19.0 verified; npm registry + git remote reachability confirmed.
