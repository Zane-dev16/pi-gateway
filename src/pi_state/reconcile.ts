// pi_state/reconcile.ts — declarative schema reconcile, storage-version
// tracking, title-uniqueness dedup repair, the DEC-070 FTS retreat, the
// DEC-070 async-delegation rail retreat, and the whole-open jittered-patience
// wrapper.
//
// Spec: /root/pi-gateway/02-session-and-state.md
//   §2.2 storage-version tracking (schema_version advances freely on
//        writable open)
//   §3   "Migration Style: Declarative Reconcile" — startup steps 1–7, the
//        error taxonomy for ALTER races, and "read-only opens never DDL"
//   §9   title partial unique index ensured at every open with self-healing
//        dedup; index creation must never abort an open
//
// Hermes anchors (READ-ONLY reference):
//   hermes_state_schema.py:_reconcile_columns            → reconcileColumns
//   hermes_state_schema.py:schema_read_probe_statements  → readProbeStatements
//   hermes_state_schema.py:_init_schema (title dedup)    → ensureTitleUniqueIndex
//   hermes_state_schema.py:_ensure_fts_schema            → REMOVED (DEC-070):
//        retreatFtsObjects drops the FTS objects that parity created
//   hermes_state_schema.py:_heal_gateway_routing_pk      → healGatewayRoutingPk
//   SessionDB._connect_and_init_with_lock_patience       → connectAndInitWithPatience

import type Database from "better-sqlite3";

import {
	DEFERRED_INDEX_SQL,
	SCHEMA_TABLES_SQL,
	SCHEMA_TIER1_INDEXES_SQL,
	SCHEMA_VERSION,
	TITLE_UNIQUE_INDEX_SQL,
	declaredSchemaTables,
	type TableColumns,
} from "./schema.js";
import { isLockedOrBusy, runWithJitteredPatience } from "./wal.js";

function errMessage(err: unknown): string {
	if (err instanceof Error) return err.message;
	return String(err);
}

/** Strict identifier allowlist for anything interpolated into DDL. */
function isSafeIdentifier(name: string): boolean {
	return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

export interface ColumnAdd {
	table: string;
	column: string;
}

export interface ReconcileResult {
	added: ColumnAdd[];
}

/**
 * Diff live columns against SCHEMA and ADD what's missing — the Beets/
 * sqlite-utils pattern (hermes_state_schema.py:_reconcile_columns). Error
 * taxonomy is load-bearing (02 §3 step 2):
 *   'duplicate column' → a sibling won the race between our PRAGMA diff and
 *                        the ALTER; continue (store ends up correct either way)
 *   'locked'/'busy'    → RE-RAISE so the whole-init retry re-runs init with
 *                        jittered patience instead of serving a half-reconciled
 *                        store that 500s on 'no such column'
 *   anything else      → loud warning; store stays behind SCHEMA (bug)
 */
export function reconcileColumns(db: Database.Database): ReconcileResult {
	const expected = declaredSchemaTables();
	const added: ColumnAdd[] = [];
	for (const spec of expected) {
		let liveCols: Set<string>;
		try {
			const rows = db
				.prepare(`PRAGMA table_info("${spec.table}")`)
				.all() as Array<Record<string, unknown>>;
			liveCols = new Set(rows.map((r) => String(r["name"])));
		} catch {
			continue; // table doesn't exist yet (shouldn't happen after executescript)
		}
		for (const [colName, colType] of spec.columns) {
			if (liveCols.has(colName)) continue;
			// Identifiers come ONLY from our own parsed SCHEMA constant (allowlist
			// source), are pattern-checked, and quote-escaped — the interpolation is
			// structural (DDL identifiers cannot be bound parameters).
			if (!isSafeIdentifier(colName) || !isSafeIdentifier(spec.table)) {
				throw new Error(
					`reconcile: unsafe identifier ${spec.table}.${colName} in SCHEMA constant`,
				);
			}
			const safeName = colName.replace(/"/g, '""');
			const safeTable = spec.table.replace(/"/g, '""');
			try {
				db.exec(
					`ALTER TABLE "${safeTable}" ADD COLUMN "${safeName}" ${colType}`,
				);
				added.push({ table: spec.table, column: colName });
			} catch (err) {
				const msg = errMessage(err).toLowerCase();
				if (msg.includes("duplicate column")) {
					continue; // sibling won the race — expected under concurrent opens
				}
				if (msg.includes("locked") || msg.includes("busy")) {
					throw err; // whole-init retries with jittered patience (02 §3)
				}
				console.warn(
					`[pi_state] reconcile ${spec.table}.${colName} failed; store remains behind SCHEMA_SQL: ${errMessage(err)}`,
				);
			}
		}
	}
	return { added };
}

export interface SchemaProbe {
	table: string;
	sql: string;
}

/**
 * Read-probe statements derived from SCHEMA so they can't go stale (02 §3):
 * one prepare-time probe per declared table selecting ALL its declared columns.
 * Any prepare-time failure means "store behind schema" — the caller reopens
 * writable to heal. (A hand-maintained probe list went stale within days in
 * Hermes — hermes_state_schema.py:schema_read_probe_statements.)
 */
export function readProbeStatements(): SchemaProbe[] {
	return declaredSchemaTables().map((spec: TableColumns) => ({
		table: spec.table,
		sql: `SELECT ${[...spec.columns.keys()]
			.map((c) => `"${c}"`)
			.join(", ")} FROM "${spec.table}" LIMIT 0`,
	}));
}

export class StoreBehindSchemaError extends Error {
	readonly failedProbes: Array<{ table: string; reason: string }>;
	constructor(failed: Array<{ table: string; reason: string }>) {
		super(
			`state.db is behind SCHEMA (read-probe failures: ${failed
				.map((f) => `${f.table}: ${f.reason}`)
				.join("; ")}) — reopen writable to heal`,
		);
		this.name = "StoreBehindSchemaError";
		this.failedProbes = failed;
	}
}

/** Run every derived read-probe; throws StoreBehindSchemaError on any failure. */
export function assertStoreMatchesSchema(db: Database.Database): void {
	const failed: Array<{ table: string; reason: string }> = [];
	for (const probe of readProbeStatements()) {
		try {
			db.prepare(probe.sql).run();
		} catch (err) {
			failed.push({ table: probe.table, reason: errMessage(err) });
		}
	}
	if (failed.length > 0) throw new StoreBehindSchemaError(failed);
}

// ---------------------------------------------------------------------------
// state_meta helpers
// ---------------------------------------------------------------------------

export function getMeta(db: Database.Database, key: string): string | null {
	const row = db
		.prepare("SELECT value FROM state_meta WHERE key = ?")
		.get(key) as { value: string | null } | undefined;
	return row === undefined ? null : row.value;
}

export function setMeta(
	db: Database.Database,
	key: string,
	value: string,
): void {
	db.prepare(
		"INSERT INTO state_meta (key, value) VALUES (?, ?) " +
			"ON CONFLICT(key) DO UPDATE SET value = excluded.value",
	).run(key, value);
}

export function deleteMeta(db: Database.Database, key: string): void {
	db.prepare("DELETE FROM state_meta WHERE key = ?").run(key);
}

// ---------------------------------------------------------------------------
// Step 4 — title uniqueness with dedup repair (02 §9)
// ---------------------------------------------------------------------------

/**
 * Ensure idx_sessions_title_unique OUTSIDE SCHEMA with self-healing dedup:
 * older duplicate rows get title=NULL, the newest keeps the alias. Index
 * creation must NEVER abort an open — IntegrityError triggers repair then a
 * retry; residual failure logs and continues without the index.
 */
export function ensureTitleUniqueIndex(db: Database.Database): boolean {
	try {
		db.exec(TITLE_UNIQUE_INDEX_SQL);
		return true;
	} catch {
		// Expected path when pre-existing duplicates violate the unique index.
	}
	repairDuplicateTitles(db);
	try {
		db.exec(TITLE_UNIQUE_INDEX_SQL);
		return true;
	} catch (err) {
		console.warn(
			`[pi_state] could not ensure idx_sessions_title_unique; continuing WITHOUT it: ${errMessage(err)}`,
		);
		return false;
	}
}

/** NULL the title of every duplicate row except the NEWEST (highest rowid =
 * insertion order; id is TEXT in practice, so id-MAX would be lexicographic
 * and wrong) per title. */
export function repairDuplicateTitles(db: Database.Database): void {
	db.prepare(
		`UPDATE sessions SET title = NULL
		 WHERE title IS NOT NULL AND rowid NOT IN (
		   SELECT MAX(rowid) FROM sessions WHERE title IS NOT NULL GROUP BY title
		 )`,
	).run();
}

// ---------------------------------------------------------------------------
// Step 5 — FTS retreat (DEC-070 item 9): drop-if-present, no-op when absent
// ---------------------------------------------------------------------------

/** state_meta keys the legacy FTS rebuild/version machinery wrote. */
const LEGACY_FTS_META_KEYS = [
	"fts_rebuild_high_water",
	"fts_rebuild_progress",
	"fts_storage_version",
] as const;

/**
 * Legacy FTS objects follow the exact Hermes-parity family: the two
 * external-content tables, the optional CJK pair (VIEW + virtual table), and
 * the rebuild-gated triggers on `messages`. Names come from sqlite_master and
 * are interpolated into DDL (identifiers cannot be bound parameters), so
 * anything outside this exact set is refused loudly instead of dropped.
 */
const LEGACY_FTS_OBJECT_NAMES: ReadonlySet<string> = new Set([
	"messages_fts",
	"messages_fts_trigram",
	"messages_fts_cjk",
	"messages_fts_cjk_src",
	"messages_fts_ai",
	"messages_fts_ad",
	"messages_fts_au",
	"messages_fts_trigram_ai",
	"messages_fts_trigram_ad",
	"messages_fts_trigram_au",
]);

function isLegacyFtsObjectName(name: string): boolean {
	return LEGACY_FTS_OBJECT_NAMES.has(name);
}

/**
 * Retreat the FTS full-text search surface (DEC-070 item 9 — the owner
 * authorized this DDL retreat; the reconcile machinery itself stays CORE):
 * DROP the legacy external-content FTS tables (`messages_fts`,
 * `messages_fts_trigram`), the optional CJK pair (`messages_fts_cjk` + its
 * `messages_fts_cjk_src` VIEW), their rebuild-gated triggers, and any
 * residual `fts_*` state_meta bookkeeping keys.
 *
 * Retreat semantics: drop-if-present, no-op when absent — idempotent in both
 * directions. Every writable open of a pre-retreat store heals it; fresh and
 * already-retreated stores skip all work (probe matches nothing). Individual
 * drops are guarded so one bad object cannot strand the rest, and any failure
 * logs and never aborts an open (§9 rule). Returns the number of legacy
 * objects dropped (0 when the store is clean).
 */
export function retreatFtsObjects(db: Database.Database): number {
	try {
		const rows = db
			.prepare(
				"SELECT name, type FROM sqlite_master " +
					"WHERE name LIKE 'messages_fts%' ORDER BY rowid DESC",
			)
			.all() as Array<{ name: string; type: string }>;
		let dropped = 0;
		for (const row of rows) {
			if (!isLegacyFtsObjectName(row.name)) {
				console.warn(
					`[pi_state] fts retreat: refusing to drop unexpected object ${row.type} ${row.name}`,
				);
				continue;
			}
			try {
				db.exec(`DROP ${row.type.toUpperCase()} IF EXISTS "${row.name}"`);
				dropped++;
			} catch (err) {
				console.warn(
					`[pi_state] fts retreat: could not drop ${row.type} ${row.name}: ${errMessage(err)}`,
				);
			}
		}
		for (const key of LEGACY_FTS_META_KEYS) deleteMeta(db, key);
		return dropped;
	} catch (err) {
		console.warn(
			`[pi_state] fts retreat failed; continuing without it: ${errMessage(err)}`,
		);
		return 0;
	}
}

// ---------------------------------------------------------------------------
// Step 6.5 — DEC-070 async-delegation rail retreat (drop-if-present)
// ---------------------------------------------------------------------------

/**
 * Retreat the async-delegation durability rail (DEC-070 item 5 — the owner
 * authorized this DDL retreat; the reconcile machinery itself stays CORE):
 * DROP the `async_delegations` table and its `idx_async_delegations_delivery`
 * index — Hermes parity objects (`tools/async_delegation.py` persistence half,
 * 02 §2.1 DDL) whose only consumers (pi_gateway/delegation rail +
 * pi_embedded/delegation-watcher) were removed under the same amendment.
 *
 * Retreat semantics: drop-if-present, no-op when absent — idempotent in both
 * directions, same contract as the FTS retreat above. Every writable open of
 * a pre-retreat store heals it; fresh and already-retreated stores skip all
 * work. Rail rows are deliberately NOT migrated anywhere: the feature is
 * owner-excluded, its durability obligations end with it.
 *
 * Returns the number of legacy objects dropped (0 when the store is clean).
 */
export function retreatAsyncDelegationObjects(db: Database.Database): number {
	try {
		let dropped = 0;
		const hasTable =
			db.prepare(
				"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'async_delegations'",
			).get() !== undefined;
		if (hasTable) {
			db.exec("DROP TABLE IF EXISTS async_delegations");
			dropped++;
		}
		const hasIndex =
			db.prepare(
				"SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_async_delegations_delivery'",
			).get() !== undefined;
		if (hasIndex) {
			// DROP TABLE removes its own indexes; this only fires for a stray
			// index orphaned by a hand-edited store.
			db.exec("DROP INDEX IF EXISTS idx_async_delegations_delivery");
			dropped++;
		}
		return dropped;
	} catch (err) {
		console.warn(
			`[pi_state] async-delegation retreat failed; continuing without it: ${errMessage(err)}`,
		);
		return 0;
	}
}

// ---------------------------------------------------------------------------
// Step 7 — one-time structural heals
// ---------------------------------------------------------------------------

/**
 * Rebuild gateway_routing when its PRIMARY KEY predates scoping
 * (02 §3 step 7; hermes_state_schema.py:_heal_gateway_routing_pk): early
 * tables had `session_key TEXT PRIMARY KEY` with no composite scope key, so
 * upserts targeting (scope, session_key) fail forever. Recreate with the
 * correct DDL preserving rows; newest wins cross-scope collisions.
 */
export function healGatewayRoutingPk(db: Database.Database): boolean {
	const rows = db
		.prepare('PRAGMA table_info("gateway_routing")')
		.all() as Array<Record<string, unknown>>;
	if (rows.length === 0) return false; // not created yet — nothing to heal
	const pkCols = rows
		.filter((r) => Number(r["pk"]) > 0)
		.sort((a, b) => Number(a["pk"]) - Number(b["pk"]))
		.map((r) => String(r["name"]));
	const correct =
		pkCols.length === 2 && pkCols[0] === "scope" && pkCols[1] === "session_key";
	if (correct) return false;

	db.exec("BEGIN IMMEDIATE");
	try {
		db.exec(`
			CREATE TABLE gateway_routing_rebuilt (
			  scope TEXT NOT NULL DEFAULT '', session_key TEXT NOT NULL,
			  entry_json TEXT NOT NULL, updated_at REAL NOT NULL,
			  PRIMARY KEY (scope, session_key)
			);
		`);
		// Newest wins collisions (ORDER BY updated_at ASC so later rows overwrite).
		// Defensive against pre-reconcile shapes missing the scope column.
		const hasScope = db
			.prepare('PRAGMA table_info("gateway_routing")')
			.all()
			.some((c) => String((c as Record<string, unknown>)["name"]) === "scope");
		if (hasScope) {
			db.exec(`
				INSERT OR REPLACE INTO gateway_routing_rebuilt
				  (scope, session_key, entry_json, updated_at)
				SELECT COALESCE(scope, ''), session_key, entry_json, updated_at
				  FROM gateway_routing ORDER BY updated_at ASC;
			`);
		} else {
			db.exec(`
				INSERT OR REPLACE INTO gateway_routing_rebuilt
				  (scope, session_key, entry_json, updated_at)
				SELECT '', session_key, entry_json, updated_at
				  FROM gateway_routing ORDER BY updated_at ASC;
			`);
		}
		db.exec("DROP TABLE gateway_routing;");
		db.exec("ALTER TABLE gateway_routing_rebuilt RENAME TO gateway_routing;");
		db.exec("COMMIT");
		return true;
	} catch (err) {
		try {
			db.exec("ROLLBACK");
		} catch {
			/* best-effort */
		}
		throw err;
	}
}

// ---------------------------------------------------------------------------
// Whole-init sequence (02 §3 steps 1–7)
// ---------------------------------------------------------------------------

export interface InitReport {
	reconciled: ReconcileResult;
	titleIndexEnsured: boolean;
	routingPkHealed: boolean;
	/** Legacy FTS objects dropped by the DEC-070 retreat (0 when clean). */
	ftsRetreated: number;
	/** Legacy async-delegation rail objects dropped by the DEC-070 retreat (0
	 *  when clean). */
	delegationRetreated: number;
	versionBumped: boolean;
}

export interface InitStoreOptions {
	/**
	 * Retreat legacy FTS objects at open (DEC-070 item 9): drop-if-present,
	 * no-op when absent. Default true; false is a test hook only.
	 */
	dropLegacyFtsObjects?: boolean;
	/**
	 * Retreat the async-delegation rail objects at open (DEC-070 item 5):
	 * drop-if-present, no-op when absent. Default true; false is a test
	 * hook only.
	 */
	dropLegacyDelegationObjects?: boolean;
}

function bumpSchemaVersion(db: Database.Database): void {
	const res = db
		.prepare("UPDATE schema_version SET version = ?")
		.run(SCHEMA_VERSION);
	if (Number(res.changes) === 0) {
		db.prepare("INSERT INTO schema_version (version) VALUES (?)").run(
			SCHEMA_VERSION,
		);
	}
}

/**
 * Startup reconcile sequence, exactly 02 §3 steps 1–7:
 *   1. executescript(SCHEMA_SQL)          (CREATE … IF NOT EXISTS)
 *   2. column reconcile ('duplicate column' tolerated; locked/busy RE-RAISED)
 *   3. DEFERRED_INDEX_SQL                 (indexes on reconciled-in columns)
 *   4. unique title index w/ dedup repair
 *   5. FTS retreat — DEC-070 item 9: drop-if-present legacy FTS objects
 *   6. bump schema_version (unconditional — no FTS gate remains)
 *   7. one-time structural heals (gateway_routing PK predating scope)
 *
 * Additive change = add a line to SCHEMA; destructive change = explicit
 * versioned migration, never reconcile.
 */
export function initStore(
	db: Database.Database,
	opts: InitStoreOptions = {},
): InitReport {
	db.exec(SCHEMA_TABLES_SQL); // step 1 (tables; indexes follow reconcile)
	const reconciled = reconcileColumns(db); // step 2 (locked/busy propagate)
	const routingPkHealed = healGatewayRoutingPk(db); // step 7 before indexes (table shape final)
	db.exec(SCHEMA_TIER1_INDEXES_SQL); // step 1 (tier-1 indexes, post-reconcile)
	db.exec(DEFERRED_INDEX_SQL); // step 3
	const titleIndexEnsured = ensureTitleUniqueIndex(db); // step 4
	const ftsRetreated =
		opts.dropLegacyFtsObjects !== false
			? retreatFtsObjects(db) // step 5 — DEC-070 DDL retreat (drop-if-present)
			: 0;
	const delegationRetreated =
		opts.dropLegacyDelegationObjects !== false
			? retreatAsyncDelegationObjects(db) // step 6.5 — DEC-070 DDL retreat
			: 0;
	bumpSchemaVersion(db); // step 6 (unconditional — no FTS gate remains)
	return {
		reconciled,
		titleIndexEnsured,
		routingPkHealed,
		ftsRetreated,
		delegationRetreated,
		versionBumped: true,
	};
}

/**
 * Whole-open retry with jittered patience (parity of
 * SessionDB._connect_and_init_with_lock_patience): any busy/locked error from
 * the LADDER or from INIT retries the ENTIRE attempt — executescript is
 * idempotent CREATE IF NOT EXISTS, so re-running init is always safe, whereas
 * swallowing a locked ALTER would serve a half-reconciled store.
 */
export function connectAndInitWithPatience<T>(
	attempt: () => T | Promise<T>,
	opts?: { patienceMs?: number },
): Promise<T> {
	return runWithJitteredPatience(attempt, {
		patienceMs: opts?.patienceMs ?? 20_000,
	});
}

export { isLockedOrBusy };
