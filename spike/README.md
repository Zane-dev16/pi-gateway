# spike/ — THROWAWAY Phase-0 code

Everything in this directory is **mechanism-spike code for roadmap Phase 0 only**
(see `/root/pi-gateway/10-implementation-roadmap.md` §Phase 0 and
`EXECUTION-PROMPT.md` working protocol rule 2).

**Spike code never ships.** It stays out of production modules; only the tests'
*shapes* port forward into the Phase 1–2 suites (lease race + generation-release →
02 §13 Turn-lease row; stream mutation → 04 §8 Streaming rows; WAL ladder fallback +
two-writer contention → 02 §13 Reconcile/race column).

SQLite driver ratified by `sanity.test.ts`: **better-sqlite3** (prebuilt native
binding verified on Node v26.7.0: WAL pragma, busy_timeout, BEGIN IMMEDIATE,
byte-exact multi-byte TEXT/BLOB round-trip, cross-connection visibility).
