# Changelog

All notable changes to Pi Gateway are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Behavioral decisions are recorded in the project decision log
(DEC-001…069, [../09-open-questions.md](../09-open-questions.md)); the log is
append-only and every divergence from the Hermes Gateway reference requires a
logged entry before implementation (DEC-026).

## [Unreleased]

Nothing yet.

## [0.1.0] — 2026-08-27

Initial public release: the complete gateway spine, the three reference
adapters, security and multiplex, the embedded services and update pipeline,
and the 31-surface platform census.

### Added

- **Spine.** Shared SQLite substrate (`state.db`, WAL ladder with guarded
  fallback) with declarative schema reconcile, FTS5 search
  (`messages_fts` + trigram, optional CJK index), byte-exact `api_content`
  replay sidecar, and session resolution with compression-lineage tracking
  (spec 02, DEC-007). Two-level busy guard and the two-layer turn lease —
  in-process registry with generation-scoped release plus a cross-process DB
  lease keyed on the compression-lineage root, TTL 300s, bounded wait,
  dead-PID reclaim (spec 02 §5, DEC-004). Bounded worker pool and runner
  (spec 01 §2).
- **Streaming, obligations, registry.** Stream consumer with the four
  streaming invariants, prefix-stable drafts, and seal-interception at every
  audited egress door, mutation-tested (spec 04 §5, DEC-006); delivery
  obligations ledger with CAS-guarded state transitions and caps of 3 attempts
  / 24h staleness / 7d retention / 500 rows (DEC-053/054); the single frozen
  slash-command registry from which every surface — busy policies, help,
  bot menus, completions — derives (spec 07 §1, DEC-005).
- **Reference adapters.** Polling, persistent-WebSocket, and webhook adapters
  covering all three transport shapes, plus the executable conformance suite
  that every adapter — including all later census ports — must pass
  (spec 04 §8, DEC-002/032).
- **Security and multiplex.** Fail-closed secret scope with grep-gated
  fallback ban (DEC-003, as amended by DEC-009), centralized authorization
  with reason-coded denials, pairing handshake, per-credential token locks
  with stale detection, signed webhooks with explicit trust boundaries
  (spec 06 §2–§8, DEC-017), and per-profile multiplex scoping.
- **Embedded services and updates.** Cron ticker with inactivity-based bound
  (default 600s), kanban dispatcher and notifier, DB claim/re-bind/replay
  handoff watcher (DEC-008), observer vs decision-bearing hooks (DEC-014),
  durable idle-gated delegation rail (DEC-018), and the transactional update
  pipeline — plan, snapshot, apply, fleet-wide drain-first restart, verify,
  receipt — that fails closed on a stale fleet (spec 08 §5–§10).
- **Platform census.** 31 surfaces shipped across the three transport shapes
  (7 polling, 13 WebSocket, 11 webhook), each passing all applicable
  conformance rows with zero deferred; `dingtalk` is an explicit, documented
  exclusion (DEC-043). See [docs/platforms.md](docs/platforms.md).
- **Entrypoints.** `pi gateway run` composition root binding boot fingerprint,
  duplicate-instance takeover handshake, runtime lock, store recovery, cron,
  watchers, and manifest-driven adapter registration (DEC-058; spec 01 §3).
- **Documentation.** Public-facing docs set: hub [README](README.md) plus
  quickstart, installation, configuration, architecture, platforms,
  adding-a-platform, operations, and troubleshooting under [docs/](docs/).

### Conformity

- A cross-reference conformity campaign against upstream
  `NousResearch/hermes-agent` audited nine axes and closed 326 findings:
  306 fixed, 13 ratified as logged decisions, the rest adjudicated or
  documented. Verdict: STABLE — zero open unjustified divergences. A
  residual-settle pass closed all remaining adjudication items
  (DEC-059…069).

### Quality

- 3096 behavior-contract tests across 237 files, green twice consecutively on
  the tree of record; `tsc --noEmit` clean tree-wide; the layering gate and
  the secret-scope grep gate pass (see
  [CONTRIBUTING.md](CONTRIBUTING.md)). Specs that spawn real OS child
  processes run serialized under full-suite execution (DEC-041).

[Unreleased]: https://github.com/IrellZane/pi-gateway/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/IrellZane/pi-gateway/releases/tag/v0.1.0
