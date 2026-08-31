# Changelog

All notable changes to Pi Gateway are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Behavioral decisions are recorded in the project decision log
(DEC-001…069, [../09-open-questions.md](../09-open-questions.md)). The log is
append-only, and every divergence from the Hermes Gateway reference requires a
logged entry before implementation (DEC-026).

## [Unreleased]

### Removed

Scope reduction to the chat-application bridge (DEC-070 in
[../09-open-questions.md](../09-open-questions.md); owner-validated
OVERENGINEERED and POTENTIALLY tiers from the feature-tier analysis):

- Kanban task board + dispatcher, `/kanban` command, and completion
  notifications (the DEC-057 dispatcher-lock machinery included).
- Cron inactivity runaway bound (the HERMES_CRON_TIMEOUT hard interrupt);
  cron CORE — scheduling, tick lock, durability, delivery-to-chat — stays.
- Fleet self-update machinery (`pi_embedded/update`: plan/snapshot/apply/
  restart-per-kind/verify/receipts/latest.json/prune), the `/update`
  command, the SIGHUP hangup protection, and the pause-for-update handshake.
- Non-chat protocol connectors: `a2a`, `raft`, `homeassistant`, `ntfy`.
- Personal/local chat connectors: `whatsapp-personal`, `bluebubbles`
  (`whatsapp-cloud` stays).
- The async delegation durability rail + delegation-watcher (idle-gated
  re-entry).
- Recurring in-session `/loop` wakeups (the `/loop` command).
- File-drop hooks + plugin discovery runtime (the command-registry hook
  seam stays).
- FTS full-text message search (`messages_fts`, trigram/CJK index, triggers,
  rebuild machinery); the schema retreat is authorized and idempotent
  (DEC-070).

Platform census: 31 → 25 surfaces. The test suite re-baselined from 3096
tests across 237 files to 2536 tests across 187 files — the removed tests
belong to the removed features; green remains the invariant, not the count
(DEC-070).

## [0.1.0] - 2026-08-27

Initial public release: the complete gateway spine, the three reference
adapters, security and multiplex, the embedded services and update pipeline,
and the 31-surface platform census. (The DEC-070 scope amendment above
subsequently removed the update pipeline and trimmed the census to 25
surfaces — see [Unreleased].)

### Added

- Shared SQLite substrate (`state.db`, WAL ladder with guarded fallback) with
  declarative schema reconcile, FTS5 search (`messages_fts` + trigram,
  optional CJK index), byte-exact `api_content` replay sidecar, and session
  resolution with compression-lineage tracking (spec 02, DEC-007).
- Two-level busy guard and the two-layer turn lease: an in-process registry
  with generation-scoped release, plus a cross-process DB lease keyed on the
  compression-lineage root (TTL 300s, bounded wait, dead-PID reclaim) (spec
  02 §5, DEC-004).
- Bounded worker pool and runner (spec 01 §2).
- Stream consumer with the four streaming invariants, prefix-stable drafts,
  and seal-interception at every audited egress door, mutation-tested (spec
  04 §5, DEC-006).
- Delivery obligations ledger with CAS-guarded state transitions and caps of
  3 attempts, 24h staleness, 7d retention, and 500 rows (DEC-053/054).
- Single frozen slash-command registry from which every surface derives: busy
  policies, help, bot menus, completions (spec 07 §1, DEC-005).
- Reference adapters covering all three transport shapes (polling,
  persistent-WebSocket, webhook), plus the executable conformance suite that
  every adapter, including all later census ports, must pass (spec 04 §8,
  DEC-002/032).
- Security and multiplex: fail-closed secret scope with grep-gated fallback
  ban (DEC-003, as amended by DEC-009), centralized authorization with
  reason-coded denials, pairing handshake, per-credential token locks with
  stale detection, signed webhooks with explicit trust boundaries (spec 06
  §2–§8, DEC-017), and per-profile multiplex scoping.
- Embedded services: cron ticker and the DB claim/re-bind/replay handoff
  watcher (DEC-008).
- Platform census: 31 surfaces shipped across the three transport shapes
  (7 polling, 13 WebSocket, 11 webhook), each passing all applicable
  conformance rows with zero deferred; `dingtalk` is an explicit, documented
  exclusion (DEC-043). See [docs/platforms.md](docs/platforms.md).
- Entrypoints: `pi gateway run` composition root binding boot fingerprint,
  duplicate-instance takeover handshake, runtime lock, store recovery, cron,
  watchers, and manifest-driven adapter registration (DEC-058; spec 01 §3).
- Documentation: public-facing docs set, hub [README](README.md) plus
  quickstart, installation, configuration, architecture, platforms,
  adding-a-platform, operations, and troubleshooting under [docs/](docs/).

### Conformity

- A cross-reference conformity campaign against upstream
  `NousResearch/hermes-agent` audited nine axes and closed 326 findings:
  306 fixed, 13 ratified as logged decisions, the rest adjudicated or
  documented. Verdict: STABLE, with zero open unjustified divergences. A
  residual-settle pass closed all remaining adjudication items
  (DEC-059…069).

### Quality

- 3096 behavior-contract tests across 237 files, green twice consecutively on
  the tree of record; `tsc --noEmit` clean tree-wide; the layering gate and
  the secret-scope grep gate pass (see
  [CONTRIBUTING.md](CONTRIBUTING.md)). Specs that spawn real OS child
  processes run serialized under full-suite execution (DEC-041).

[Unreleased]: https://github.com/Zane-dev16/pi-gateway/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Zane-dev16/pi-gateway/releases/tag/v0.1.0
