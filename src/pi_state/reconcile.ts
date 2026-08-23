// pi_state/reconcile.ts — declarative schema reconcile, storage-version
// tracking, title-uniqueness dedup repair, FTS ensure/backfill, and the
// whole-open jittered-patience wrapper.
//
// Spec: /root/pi-gateway/02-session-and-state.md
//   §2.2 two-tier storage-version tracking (schema_version advances freely;
//        FTS layout version opt-in via state_meta['fts_storage_version'])
//   §3   "Migration Style: Declarative Reconcile" — startup steps 1–7, the
//        error taxonomy for ALTER races, and "read-only opens never DDL"
//   §9   title partial unique index ensured at every open with self-healing
//        dedup; index creation must never abort an open
//
// Hermes anchors (READ-ONLY reference):
//   hermes_state_schema.py:_reconcile_columns            → reconcileColumns
//   hermes_state_schema.py:schema_read_probe_statements  → readProbeStatements
//   hermes_state_schema.py:_init_schema (title dedup)    → ensureTitleUniqueIndex
//   hermes_state_schema.py:_ensure_fts_schema            → ensureFtsObjects
//   hermes_state_schema.py:_heal_gateway_routing_pk      → healGatewayRoutingPk
//   SessionDB._connect_and_init_with_lock_patience       → connectAndInitWithPatience

import type Database from "better-sqlite3";

import {
	DEFERRED_INDEX_SQL,
	FTS_CJK_TABLE_SQL,
	FTS_CJK_VIEW_SQL,
	FTS_REBUILD_HIGH_WATER_KEY,
	FTS_REBUILD_PROGRESS_KEY,
	FTS_STORAGE_VERSION,
	FTS_STORAGE_VERSION_KEY,
	SCHEMA_TABLES_SQL,
	SCHEMA_TIER1_INDEXES_SQL,
	SCHEMA_VERSION,
	TITLE_UNIQUE_INDEX_SQL,
	buildFtsDdl,
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
// Steps 5/6 — FTS objects per storage-version gate + bounded backfill (02 §2.2)
// ---------------------------------------------------------------------------

export interface FtsStatus {
	/** FTS5 available in the linked SQLite build. */
	available: boolean;
	/** Optional CJK view+table created (tokenizer present). */
	cjkAvailable: boolean;
	/** Backfill finished this open (version may be stamped only when true). */
	complete: boolean;
}

const DEFAULT_FTS_CHUNK_ROWS = 2000;

interface RebuildBookkeeping {
	highWater: number;
	progress: number;
	freshStart: boolean;
}

function readRebuildBookkeeping(db: Database.Database): RebuildBookkeeping {
	const hwRaw = getMeta(db, FTS_REBUILD_HIGH_WATER_KEY);
	const progRaw = getMeta(db, FTS_REBUILD_PROGRESS_KEY);
	if (hwRaw === null && progRaw === null)
		return { highWater: -1, progress: -1, freshStart: true };
	return {
		highWater: hwRaw === null ? -1 : Number.parseInt(hwRaw, 10),
		progress: progRaw === null ? -1 : Number.parseInt(progRaw, 10),
		freshStart: false,
	};
}

/**
 * One bounded chunk of the crash-safe backfill. The gating triggers make any
 * interruption consistent: rows above high_water flow in live; rows at/below
 * progress are already owned by the index (their mutation triggers fire); rows
 * in between are skipped by triggers until their chunk lands (02 §2.1).
 */
function backfillChunk(
	db: Database.Database,
	fromExclusive: number,
	toInclusive: number,
): void {
	db.prepare(
		`INSERT INTO messages_fts(rowid, content, tool_name, tool_calls)
		 SELECT id, content, tool_name, tool_calls FROM messages
		 WHERE id > ? AND id <= ? AND role <> 'tool'`,
	).run(fromExclusive, toInclusive);
	db.prepare(
		`INSERT INTO messages_fts_trigram(rowid, content, tool_name, tool_calls)
		 SELECT id, content, tool_name, tool_calls FROM messages
		 WHERE id > ? AND id <= ? AND role <> 'tool'`,
	).run(fromExclusive, toInclusive);
}

/**
 * Ensure FTS objects exist and are consistent with fts_storage_version
 * (02 §2.2): matching version ⇒ idempotent ensure; legacy/mismatched version ⇒
 * seed bookkeeping keys and run a BOUNDED chunked backfill (crash-safe across
 * opens via high-water/progress keys; keys deleted on completion). FTS5 being
 * unavailable never aborts an open — it only blocks the version stamp (step 6:
 * "claiming current schema would be a lie").
 */
export function ensureFtsObjects(
	db: Database.Database,
	opts: { cjk?: boolean; chunkRows?: number; maxChunksPerOpen?: number } = {},
): FtsStatus {
	// Availability probe inside a savepoint so a failed CREATE leaves no residue.
	let available = false;
	try {
		db.exec("SAVEPOINT fts_probe");
		db.exec("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_probe USING fts5(x)");
		db.exec("DROP TABLE IF EXISTS _fts5_probe");
		db.exec("RELEASE fts_probe");
		available = true;
	} catch {
		try {
			db.exec("ROLLBACK TO fts_probe");
			db.exec("RELEASE fts_probe");
		} catch {
			/* probe savepoint cleanup best-effort */
		}
	}
	if (!available) {
		return { available: false, cjkAvailable: false, complete: false };
	}

	db.exec(buildFtsDdl());

	let cjkAvailable = false;
	if (opts.cjk === true) {
		try {
			db.exec("SAVEPOINT cjk_probe");
			db.exec(FTS_CJK_VIEW_SQL);
			db.exec(FTS_CJK_TABLE_SQL);
			db.exec("RELEASE cjk_probe");
			cjkAvailable = true;
		} catch (err) {
			try {
				db.exec("ROLLBACK TO cjk_probe");
				db.exec("RELEASE cjk_probe");
			} catch {
				/* best-effort */
			}
			// Optional feature (02 §2.1): stock SQLite lacks cjk_unicode61.
			console.warn(
				`[pi_state] optional CJK FTS unavailable; continuing without it: ${errMessage(err)}`,
			);
		}
	}

	const stampedVersion = getMeta(db, FTS_STORAGE_VERSION_KEY);
	if (stampedVersion === String(FTS_STORAGE_VERSION)) {
		return { available: true, cjkAvailable, complete: true };
	}

	// Legacy or mid-rebuild store: seed bookkeeping on first sight, then chew
	// bounded chunks per open until progress reaches high-water.
	const book = readRebuildBookkeeping(db);
	let highWater = book.highWater;
	let progress = book.progress;
	if (book.freshStart) {
		const row = db
			.prepare("SELECT COALESCE(MAX(id), -1) AS m FROM messages")
			.get() as { m: number };
		highWater = Number(row.m);
		progress = -1;
		setMeta(db, FTS_REBUILD_HIGH_WATER_KEY, String(highWater));
		setMeta(db, FTS_REBUILD_PROGRESS_KEY, String(progress));
	}

	const chunkRows = opts.chunkRows ?? DEFAULT_FTS_CHUNK_ROWS;
	const maxChunks = opts.maxChunksPerOpen ?? Number.POSITIVE_INFINITY;
	let chunksDone = 0;
	while (
		progress < highWater &&
		chunksDone < maxChunks &&
		highWater !== undefined &&
		Number.isFinite(highWater)
	) {
		const nextProgress = Math.min(progress + chunkRows, highWater);
		if (nextProgress <= progress) break; // defensive against non-finite keys
		backfillChunk(db, progress, nextProgress);
		progress = nextProgress;
		setMeta(db, FTS_REBUILD_PROGRESS_KEY, String(progress));
		chunksDone++;
	}

	if (progress >= highWater) {
		// Backfill complete: revert triggers to tautology (delete keys), stamp
		// the layout version (02 §11 GC-hooks row; §2.2).
		deleteMeta(db, FTS_REBUILD_HIGH_WATER_KEY);
		deleteMeta(db, FTS_REBUILD_PROGRESS_KEY);
		setMeta(db, FTS_STORAGE_VERSION_KEY, String(FTS_STORAGE_VERSION));
		return { available: true, cjkAvailable, complete: true };
	}
	return { available: true, cjkAvailable, complete: false };
}

/** True when no rebuild bookkeeping is pending and the layout version matches. */
export function ftsMigrationComplete(db: Database.Database): boolean {
	if (getMeta(db, FTS_STORAGE_VERSION_KEY) !== String(FTS_STORAGE_VERSION)) {
		return false;
	}
	return (
		getMeta(db, FTS_REBUILD_HIGH_WATER_KEY) === null &&
		getMeta(db, FTS_REBUILD_PROGRESS_KEY) === null
	);
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
	fts: FtsStatus;
	versionBumped: boolean;
}

export interface InitStoreOptions {
	/** Opt-in optional CJK FTS pair (02 §2.1 "Optional CJK"). Default off. */
	ensureCjkFts?: boolean;
	/** Backfill chunk size in rows. */
	ftsChunkRows?: number;
	/** Max backfill chunks to chew per open (bounded work per open). */
	maxFtsChunksPerOpen?: number;
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
 *   5. FTS objects per storage-version gate
 *   6. bump schema_version — SKIPPED while FTS migrations are incomplete or
 *      FTS5 is unavailable (claiming current schema would be a lie)
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
	const ftsOpts: {
		cjk?: boolean;
		chunkRows?: number;
		maxChunksPerOpen?: number;
	} = {};
	if (opts.ensureCjkFts !== undefined) ftsOpts.cjk = opts.ensureCjkFts;
	if (opts.ftsChunkRows !== undefined) ftsOpts.chunkRows = opts.ftsChunkRows;
	if (opts.maxFtsChunksPerOpen !== undefined) {
		ftsOpts.maxChunksPerOpen = opts.maxFtsChunksPerOpen;
	}
	const fts = ensureFtsObjects(db, ftsOpts); // step 5
	let versionBumped = false;
	if (fts.available && fts.complete) {
		bumpSchemaVersion(db); // step 6
		versionBumped = true;
	}
	return {
		reconciled,
		titleIndexEnsured,
		routingPkHealed,
		fts,
		versionBumped,
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
