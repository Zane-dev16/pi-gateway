# CONTRACTS — exported API surface of `src/pi_state`

> **Downstream binding:** pi_agent_core / pi_gateway / platforms build against
> EXACTLY these exports. Dependency layer (01 §5.3): `pi_home → pi_state →
> (pi_agent_core ∥ pi_gateway)`. Nothing in pi_state imports upward.
> Normative mechanism: `/root/pi-gateway/02-session-and-state.md` (§ refs below);
> Hermes anchors cited as `file:symbol`. This file is regenerated per phase —
> keep it truthful to what ships.

---

## `src/pi_home.ts` — the home accessor (01 §6)

| Export | Signature | Semantics |
| --- | --- | --- |
| `resolvePiHome` | `(): string` | Single source of truth: context-local override → `PI_HOME` env (**read once at first call**, cached for process life) → platform default (`~/.pi`; win32 `%LOCALAPPDATA%\pi`). Loud ONE-SHOT stderr warning when env unset while `<default>/active_profile` names a non-default profile. Parity `hermes_constants.py:get_hermes_home`. |
| `displayPiHome` | `(): string` | User-facing form; collapses user-home prefix to `~`. |
| `setPiHomeOverride` | `(home: string): void` | Installs a context-local override on the current async context (AsyncLocalStorage ≙ ctxvar). Entrypoints call this BEFORE any project import (01 §6 step 1). |
| `resetPiHomeOverride` | `(): void` | Clears the override on the current context. |
| `runWithPiHomeOverride` | `<T>(home: string, fn: () => T): T` | Scoped override for `fn` and everything it spawns (multiplex per-turn scope shape). |
| `processScopedPiHome` | `(): string` | Env/default resolution IGNORING context overrides — what spawners propagate to children (01 §6 step 4). |
| `resetPiHomeCacheForTests` | `(): void` | Test hook: forgets cached env read + warning latch. |

## `src/pi_state/schema.ts` — DDL single source of truth (02 §2.1 VERBATIM)

| Export | Semantics |
| --- | --- |
| `SCHEMA_VERSION = 26` | Relational version counter; advances freely on writable open (§2.2). |
| `FTS_STORAGE_VERSION = 1` | Independent FTS layout version (`state_meta['fts_storage_version']`). |
| `FTS_REBUILD_HIGH_WATER_KEY` / `FTS_REBUILD_PROGRESS_KEY` / `FTS_STORAGE_VERSION_KEY` | state_meta keys for crash-safe rebuild bookkeeping + layout stamp. |
| `SCHEMA_TABLES_SQL` | All tables of 02 §2.1 verbatim (`CREATE TABLE IF NOT EXISTS`, exact columns/types/constraints incl. `messages.api_content` sidecar column, partial-index WHERE clauses). |
| `SCHEMA_TIER1_INDEXES_SQL` | Tier-1 indexes verbatim; executed AFTER column reconcile so legacy stores can always reconcile first. |
| `SCHEMA_SQL` | Concatenation (fresh-store convenience). |
| `DEFERRED_INDEX_SQL` | Tier-2 indexes (`idx_messages_session_active`, `idx_sessions_session_key`, `idx_sessions_gateway_peer`). |
| `TITLE_UNIQUE_INDEX_SQL` | Partial unique index on `sessions(title) WHERE title IS NOT NULL` (§9). |
| `FTS_CJK_VIEW_SQL` / `FTS_CJK_TABLE_SQL` | OPTIONAL CJK pair (`cjk_unicode61`); best-effort only, never aborts an open. |
| `buildFtsDdl(): string` | External-content FTS5 pair + gated triggers (row indexed iff `id > COALESCE(high_water,-1) OR id <= COALESCE(progress,-1)`; absent keys ⇒ tautology; tool rows excluded via `role <> 'tool'`). |
| `ftsTriggerNames(table): string[]` | Trigger names owned by an FTS table. |
| `parseSchemaTables(sql): TableColumns[]` / `declaredSchemaTables()` | Declared `{table → {column → type}}` derivation powering reconcile diff + auto-derived probes (can't go stale). |

## `src/pi_state/wal.ts` — journal ladder + write ladder (02 §1.1, §12)

| Export | Signature | Semantics |
| --- | --- | --- |
| `openDatabase` | `(opts: { path, readOnly?, busyTimeoutMs? =5000, operatorJournalMode?, requireWal?, trace? }): Promise<{ db, journalMode }>` | Opens with busy_timeout set and runs the WAL ladder. **Read-only opens never flip journal modes nor DDL.** Manual txn control (callers issue BEGIN IMMEDIATE). |
| `applyWalWithFallback` | `(port, opts): Promise<JournalMode>` | The ladder: operator setting (invalid ⇒ fail-safe wal) → #70055 vulnerable-SQLite gate (fresh stays DELETE; existing WAL NEVER live-downgraded; indeterminate left alone) → read-only probe (already-WAL ⇒ no set-pragma) → configured delete verified via no-wait setter → WAL attempt trusting the RETURNED ROW (silent-refusal shape) → guarded DELETE fallback that never downgrades unowned DBs. Per-label ERROR dedup. Port of `hermes_state.py:apply_wal_with_fallback`. |
| `executeWrite` | `<T>(db, fn: (db) => T, opts?: ExecuteWriteOptions): Promise<T>` | BEGIN IMMEDIATE + COMMIT/ROLLBACK around `fn`; busy-class errors retried on a two-band jittered schedule (fast 20–150ms, slow 250–1000ms after 2s), TIME-based patience (default 20s), terminal rethrow. Port of `SessionDB._execute_write`. |
| `runWithJitteredPatience` | `<T>(body: () => T \| Promise<T>, opts?): Promise<T>` | The shared retry schedule (used by whole-init retry too). |
| `resolveJournalMode(raw)` | `→ "wal" \| "delete"` | Operator setting; anything invalid fails safe to wal. |
| `isLockedOrBusy(err)` / `isWalIncompatError(err)` | error classifiers | SQLITE_BUSY/_SNAPSHOT/LOCKED codes + message classes; WAL-incompat markers (`locking protocol`, `not authorized`, `disk i/o error`) + SQLITE_PROTOCOL. |
| `WalUnsupportedError` / `isWalUnsupportedError` | requireWal failure shape | Loud refusal instead of silent degradation. |
| `isSqliteWalResetVulnerable(v)` / `parseSqliteVersion(s)` | #70055 gate predicate | Documented boundaries (fixed 3.51.3+, backports 3.50.7/3.44.6). |
| `onDiskJournalMode(port, sleep?)` | `→ Promise<string \| null>` | Read-only header probe before any mode write; transient-EIO retries ×4; null = undeterminable. |
| `setJournalModeNoWait(port, "DELETE")` | `→ Promise<string>` | ONLY place a non-WAL switch may be issued; forces busy_timeout=0 around it (concurrent-opener detector). |
| `applyWalSizeLimit(port)` | journal_size_limit=64MiB after WAL success; never raises. | |
| `RealPragmaPort` / `PragmaPort` | pragma seam for tests/diagnostics. | |
| `resetWalWarningsForTests` / `walWarningCount` | test hooks for the per-label dedup. | |
| `JournalMode`, `LadderOptions`, `OpenDatabaseOptions`, `PatienceOptions`, `ExecuteWriteOptions` | option types. | |

## `src/pi_state/reconcile.ts` — declarative reconcile (02 §2.2, §3, §9)

| Export | Signature | Semantics |
| --- | --- | --- |
| `initStore(db, opts?: InitStoreOptions): InitReport` | Steps 1–7 exactly: tables DDL → **column reconcile** → gateway_routing PK heal → tier-1 indexes → tier-2 indexes → title index w/ dedup repair → FTS objects per storage-version gate → bump `schema_version` **only when FTS complete & available** (claiming otherwise would be a lie). `InitStoreOptions`: `{ ensureCjkFts?, ftsChunkRows? =2000, maxFtsChunksPerOpen? =∞ }` (bounded backfill work per open). |
| `reconcileColumns(db): ReconcileResult` | PRAGMA-diff vs SCHEMA; `ALTER TABLE ADD COLUMN`. Error taxonomy: `'duplicate column'` → sibling won race, continue; `'locked'/'busy'` → **RE-RAISE** (whole-init retries); else loud warn, store stays behind SCHEMA. |
| `connectAndInitWithPatience(attempt, opts?)` | Whole-open jittered-patience retry wrapper (parity `SessionDB._connect_and_init_with_lock_patience`). |
| `readProbeStatements(): SchemaProbe[]` / `assertStoreMatchesSchema(db)` | Auto-derived prepare-time probes (all declared cols per table); any failure ⇒ `StoreBehindSchemaError` ⇒ caller reopens writable to heal. RO opens never DDL. |
| `ensureTitleUniqueIndex(db): boolean` | Ensure partial unique index OUTSIDE SCHEMA; IntegrityError ⇒ `repairDuplicateTitles` (older dups `title=NULL`, NEWEST by rowid keeps alias) ⇒ retry; residual failure logs and continues WITHOUT the index (never aborts an open). |
| `ensureFtsObjects(db, opts?): FtsStatus` | Version-gated: stamped+matching ⇒ idempotent ensure; legacy/mismatch ⇒ seed high-water/progress keys and chew BOUNDED chunked backfill (crash-safe via the gated triggers); complete ⇒ delete keys + stamp version. FTS5-unavailable ⇒ available:false, blocks version stamp. |
| `ftsMigrationComplete(db): boolean` | No pending rebuild keys AND version matches. |
| `healGatewayRoutingPk(db): boolean` | One-time structural heal: legacy `session_key`-only PK rebuilt as `(scope, session_key)` composite, rows preserved, newest wins collisions. |
| `getMeta/setMeta/deleteMeta(db, key[, value])` | state_meta helpers. |
| `InitReport` | `{ reconciled: {added}, titleIndexEnsured, routingPkHealed, fts: {available, cjkAvailable, complete}, versionBumped }` |

## `src/pi_state/leases.ts` — cross-process turn lease, layer 2 (02 §5, DEC-004)

Defaults pinned: `DEFAULT_TTL_SECONDS=300`, `DEFAULT_WAIT_SECONDS=1800`, poll 1s, notice every 15s.
The key is the **compression-lineage ROOT**, resolved IN THE SAME write transaction as every lease mutation (`hermes_state.py:_session_turn_lease_key_on_conn`). Attach to an already-open connection: `new DbTurnLeaseStore(db, options?)`.

| Method | Signature | Semantics |
| --- | --- | --- |
| `tryAcquire` | `(sessionId, holder, ttlSeconds?=300): boolean` | Atomic CAS in ONE `BEGIN IMMEDIATE`: walk root → reclaim if expired OR holder PID provably dead → INSERT OR IGNORE → ownership = final SELECT matches holder. Exactly one winner ever. |
| `acquireWait` | `(sessionId, holder, { ttlSeconds?, waitSeconds?=1800, pollIntervalSeconds?=1, waitNoticeIntervalSeconds?=15, onWait?(elapsed), shouldAbort? }): Promise<boolean>` | Bounded polling WITHOUT holding a SQLite lock; abort bails mid-wait; SQLITE_BUSY keeps polling; other SQLite errors propagate. |
| `refresh` | `(sessionId, holder, ttlSeconds?=300): boolean` | Extend only while still owner; **false = lease lost mid-turn** (first-class `"turn_lease"` condition — handle, never crash). Walks parent after rotation. |
| `releaseHolder` | `(sessionId, holder): void` | `DELETE … WHERE holder = ours` — idempotent; stale release can never free a newer acquirer's row (generation-scoped release analogue). |
| `lineageRootOnConn(sessionId)` / `lineageRoot(sessionId)` | `→ string` | The walk: follow `parent_session_id` upward through compression-ended parents ONLY; explicit forks (markers bound to `parent_session_id`: `_branched_from`/`_delegate_from`, or `source='tool'`), missing parents, cycles terminate. |
| `holderProcessIsDead(holder)` | `→ boolean` | True only when a structured `pid=<n>` holder's local PID is PROVABLY gone (ESRCH). Unstructured/same-process/doubtful stay protected until TTL. |
| `probeOwner(sessionId)` | `→ { conversationId, holder, acquiredAt, expiresAt } \| null` | Diagnostic ownership probe. |

Module-level: `extractHolderPid(holder)` (regex parity `_COMPRESSION_LOCK_HOLDER_PID_RE`), `structuredHolder(prefix, pid)`, `isExplicitForkChildRow(row)`.
**Layer 1 (in-process registry, generation tokens) lives in pi_gateway per 01 §5.3 — NOT here.**

## `src/pi_state/usage.ts` — coalescing token writer (02 §7.2, DEC-011)

Token math NEVER blocks a turn. Usage lands ONLY in `sessions` + `session_model_usage`.

| Export | Signature | Semantics |
| --- | --- | --- |
| `TokenWriter` | `new TokenWriter(db, { monotonicSeconds?, idleRetireSeconds?=30, applyHook? })` | Coalescing background writer. Worker lazily spawned on first enqueue; retires after idle window; respawns on demand; drains at process `beforeExit`. |
| `.queueTokenCounts(sessionId, delta)` | cheap append+notify; safe on the turn thread. After permanent stop falls back to SYNCHRONOUS apply (may raise like the direct path). |
| `.flushTokenCounts(timeoutMs?=5000): Promise<boolean>` | **Flush barrier** — callers MUST run BEFORE switching a session's model/route so queued old-route deltas land first. True when drained, false on timeout; never raises. Busy-flag protocol prevents double-drain races. |
| `.stop(joinTimeoutMs?=10000): Promise<void>` | Drain + permanent retire (shutdown path). |
| `.pendingCount() / .isBusy() / .isWriterActive()` | diagnostics/tests. |
| `coalesceTokenDeltas(batch): QueuedDelta[]` | Pure fn: merges CONSECUTIVE same-route incremental deltas only (route = session + model/costStatus/costSource/pricingVersion/billingProvider/billingBaseUrl/billingMode); sums add; costs sum None-preserving (all-None stays None ⇒ COALESCE keeps stored); `absolute=true` never merges. Order across sessions/model switches preserved exactly. |
| `updateTokenCounts(db, sessionId, delta)` / `applyTokenCountsInTx(conn, …)` | Direct apply port of `update_token_counts`: incremental vs absolute SQL (absolute SETs, preserves NULL actual_cost); first ACCOUNTED usage stamps the summary route (`model`,`billing_*`) which later switches never clobber; per-call attribution into `session_model_usage` keyed (session × model × billing dims × task), omitted route falls back to session-row values; absolute path records NO per-model rows. Apply failures logged, never raised into a turn. |
| `TokenDelta` | `{ inputTokens?, outputTokens?, cacheReadTokens?, cacheWriteTokens?, reasoningTokens?, apiCallCount?, estimatedCostUsd?/actualCostUsd?: number\|null, model?, costStatus?, costSource?, pricingVersion?, billingProvider?, billingBaseUrl?, billingMode?, task?, absolute? }` |

## `src/pi_state/messages.ts` — byte-exact sidecar discipline (02 §7)

**Invariant:** persist-what-you-send. `api_content` binds as-is — NO trim/sanitize/normalize anywhere in the persist path. Strict alternation repair is agent-layer pre-request (DEC-015), NEVER here.

| Export | Signature | Semantics |
| --- | --- | --- |
| `insertMessageInTx(conn, m: NewMessage): number` | Insert inside caller's txn; rowid returned. `NewMessage`: sessionId, role, content?, apiContent?, plus optional tool/reasoning/codex/platform/display fields, active?=true, compacted?=false, timestamp?. |
| `readReplayMessages(db, sessionId, { includeAncestors?=true, dedupeReplayedUserRows? }): MessageRow[]` | §7.3 projection: fixed columns INCLUDING api_content over self + compression ancestors (bounded 100 hops), ORDER BY id, active-only head. `dedupeReplayedUserRows` = clone defense (content-keyed across segments, keeps LAST). Rows RAW — substitution/alternation are consumers' jobs. |
| `listMessages(db, sessionId, { includeInactive?, includeCompacted? })` | insertion-order listing; compacted rows are durable display history, rewind rows need includeInactive. |
| `getMessageRow(db, id, includeInactive?)` | one row via fixed projection. |
| `getApiContent(db, id): string \| null` | EXACT sidecar bytes. |
| `dropStaleApiContentInTx(conn, messageId)` | Content-rewrite companion: replaying the pre-rewrite sidecar would resend removed content; dropping costs one cache-boundary miss, never wrong content. |
| `setLatestUserApiContent(conn, sessionId, apiContent, expectedContent?): number` | Backfill stamp onto newest ACTIVE user row; `expectedContent` defensive guard writes nothing on mismatch; returns rows updated (0\|1). Crash-resilient ordering (user row written last, once). Scrubs lone surrogates explicitly. |
| `substituteApiContent(row): string \| null` | Sidecar-over-content rule as ONE shared implementation for every API-bound build site. |
| `scrubSurrogates(text): string` | Paired surrogates survive; singles → U+FFFD (documented driver-boundary mapping, parity `_scrub_surrogates`). |

## `src/pi_state/store.ts` — StateStore facade

| Member | Signature | Semantics |
| --- | --- | --- |
| `StateStore.open(path, opts?)` | `Promise<StateStore>` | Writable: WAL ladder + full reconcile under whole-init patience. Opts: `{ readOnly?, operatorJournalMode?, requireWal?, busyTimeoutMs?, patienceMs?, init?: InitStoreOptions, lease?, tokens? }`. |
| `StateStore.openReadOnly(path)` | `Promise<StateStore>` | Never DDL; behind-schema ⇒ `StoreBehindSchemaError`. |
| `path / db / journalMode / initReport` | raw access for composition (leases, rotation txns, etc.). |
| `withWrite(fn, opts?)` | `Promise<T>` | executeWrite ladder. |
| `leases` / `tokens` | attached `DbTurnLeaseStore` / `TokenWriter`. |
| `appendMessage(m)` / `getMessage(id)` / `listMessages(sessionId, opts?)` / `readReplayMessages(sessionId, opts?)` / `getApiContent(id)` / `dropStaleApiContent(id)` / `setLatestUserApiContent(...)` | message surface wrapped in withWrite. |
| `queueTokenCounts(sid, delta)` / `flushTokenCounts(ms?)` / `updateTokenCounts(sid, delta)` | usage surface passthroughs. |
| `close(drainTokens?=true)` | drain writer then close connection. |

## Not built here (owned by later phases — do not assume)

- Session resolution chain / routing keys / adopt-before-mint (02 §4, §9 runtime half)
- In-process lease layer-1 registry (gateway layer, DEC-004)
- Compression rotation txn (02 §8) · delivery-obligations ledger logic (caps live in DDL only)
- Repair cascade / malformed-DB classification · portability export/import · FTS search query helpers

## Verification status

64 behavior-contract tests under `src/pi_state/*.test.ts` (+5 pi_home): reconcile
additive/sibling-race/whole-init-retry, title uniqueness + dedup repair, storage-version
two-tier + bounded chunked backfill, gateway_routing PK heal, read-probe RO detection,
lease expiry/dead-PID(unstructured+same-PID protected)/fork-exclusion/root-sharing/
refresh-after-rotation/generation-safe release/exactly-one-winner/**two-OS-process
root contention**, token coalesce/adjacency/absolute/None-preserving/flush-barrier/
route-stamp/per-model-attribution/failure-tolerance/idle-retirement/sync-fallback,
byte-exact sidecar corpus (astral/combining/ZWJ/RTL/CJK/NUL/NFC-vs-NFD/200KB)/
rollback-residue/backfill-guard/drop-on-rewrite/replay-lineage, WAL ladder order +
silent-refusal + guarded-delete + #70055 gate + EIO retry + **two-OS-process zero-lost-
commits + committed-snapshot-only + busy_timeout ride-out** + deterministic jitter bands.
