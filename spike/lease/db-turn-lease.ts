// Layer 2 of the two-layer turn lease (02-session-and-state.md §5, DEC-004):
// the cross-process SQLite lease keyed on the compression-lineage ROOT,
// resolved in the SAME transaction as the lease write.
//
// Semantic port of hermes hermes_state.py (READ-ONLY reference; semantics
// only, no code vendored). Anchors:
//   - hermes_state.py:_session_turn_lease_key_on_conn  → lineageRootOnConn
//     (walk parent_session_id upward while parent.end_reason == 'compression';
//      stop at explicit forks, cycles, and missing parents; same connection /
//      transaction as the INSERT/UPDATE/DELETE — a failed walk must not let a
//      later write fail-open onto the wrong key)
//   - hermes_state.py:_is_explicit_fork_child_row      → isExplicitForkChildRow
//   - hermes_state.py:try_acquire_session_turn_lease   → tryAcquire (atomic
//     CAS inside BEGIN IMMEDIATE: expired OR dead-PID holders reclaimed
//     in-transaction, then INSERT OR IGNORE, ownership = final SELECT)
//   - hermes_state.py:acquire_session_turn_lease       → acquireWait (bounded
//     polling loop, on_wait notices, should_abort bail-out, SQLITE_BUSY kept
//     polling — never runs unserialized)
//   - hermes_state.py:refresh_session_turn_lease       → refresh (holder-scoped UPDATE)
//   - hermes_state.py:release_session_turn_lease       → releaseHolder
//     (DELETE ... WHERE holder = ours → idempotent + generation-safe)
//   - hermes_state.py:_compression_lock_holder_process_is_dead (+ its PID
//     regex) → holderProcessIsDead: reclaim ONLY when a structured `pid=<n>`
//     marker's process is provably gone; unstructured/same-process/doubtful
//     stays protected until TTL.
//
// Defaults per spec §5: TTL 300s, waiter bound 1800s @1s poll.

import Database from "better-sqlite3";
import { performance } from "node:perf_hooks";

export const DEFAULT_TTL_SECONDS = 300;
export const DEFAULT_WAIT_SECONDS = 1800;
export const DEFAULT_POLL_INTERVAL_SECONDS = 1.0;
export const DEFAULT_WAIT_NOTICE_INTERVAL_SECONDS = 15;

/** Spec table: session_turn_leases(conversation_id PK, holder, acquired_at, expires_at). */
export const SESSION_TURN_LEASES_DDL = `
CREATE TABLE IF NOT EXISTS session_turn_leases (
	conversation_id TEXT PRIMARY KEY,
	holder TEXT NOT NULL,
	acquired_at REAL NOT NULL,
	expires_at REAL NOT NULL
)`;

/** Minimal spike stand-in for the sessions domain rows the lineage walk reads. */
export const SESSIONS_DDL = `
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	parent_session_id TEXT,
	source TEXT,
	model_config TEXT,
	end_reason TEXT
)`;

export function ensureSchema(db: Database.Database): void {
	db.exec(SESSIONS_DDL);
	db.exec(SESSION_TURN_LEASES_DDL);
}

/**
 * Structured holder ids embed `pid=<n>` so a dead holder can be proven dead
 * without waiting out the TTL. Regex parity of
 * hermes_state.py:_COMPRESSION_LOCK_HOLDER_PID_RE: `(?:^|:)pid=(\d+)(?::|$)`.
 */
const HOLDER_PID_RE = /(?:^|:)pid=(\d+)(?::|$)/;

export function extractHolderPid(holder: string): number | null {
	const m = HOLDER_PID_RE.exec(holder ?? "");
	if (!m || m[1] === undefined) return null;
	const pid = Number.parseInt(m[1], 10);
	return Number.isFinite(pid) ? pid : null;
}

export function structuredHolder(prefix: string, pid: number): string {
	return `${prefix}:pid=${pid}`;
}

/** Conservative kernel liveness probe (POSIX): ESRCH proves death, everything else is doubt. */
function defaultProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ESRCH") return false;
		return true; // EPERM etc. → any doubt → alive (TTL remains the recovery path)
	}
}

interface SessionRow {
	id: string;
	parent_session_id: string | null;
	source: string | null;
	model_config: string | null;
	end_reason: string | null;
}

interface LeaseRow {
	conversation_id: string;
	holder: string;
	acquired_at: number;
	expires_at: number;
}

export interface DbTurnLeaseOptions {
	/** Injected wall clock in SECONDS (parity of time.time()); default Date.now()/1000. */
	nowSeconds?: () => number;
	/** Injected monotonic clock in SECONDS (parity of time.monotonic()). */
	monotonicSeconds?: () => number;
	/** Injected sleep for the wait loop. */
	sleep?: (ms: number) => Promise<void>;
	/** Override self-PID (tests). Default process.pid. */
	pid?: number;
	/** Liveness probe override (tests). */
	processAlive?: (pid: number) => boolean;
	busyTimeoutMs?: number;
}

export interface AcquireWaitOptions {
	ttlSeconds?: number;
	waitSeconds?: number;
	pollIntervalSeconds?: number;
	waitNoticeIntervalSeconds?: number;
	onWait?: (elapsedSeconds: number) => void;
	shouldAbort?: () => boolean;
}

function isSqliteBusy(err: unknown): boolean {
	const code = (err as { code?: string } | null)?.code ?? "";
	return (
		code === "SQLITE_BUSY" ||
		code === "SQLITE_BUSY_SNAPSHOT" ||
		code === "SQLITE_LOCKED"
	);
}

async function defaultSleep(ms: number): Promise<void> {
	await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

export class DbTurnLeaseStore {
	readonly db: Database.Database;
	private readonly ownsDb: boolean;
	private readonly nowSeconds: () => number;
	private readonly monotonicSeconds: () => number;
	private readonly sleep: (ms: number) => Promise<void>;
	private readonly selfPid: number;
	private readonly processAlive: (pid: number) => boolean;

	private constructor(
		db: Database.Database,
		ownsDb: boolean,
		options: DbTurnLeaseOptions = {},
	) {
		this.db = db;
		this.ownsDb = ownsDb;
		this.nowSeconds = options.nowSeconds ?? (() => Date.now() / 1000);
		this.monotonicSeconds =
			options.monotonicSeconds ?? (() => performance.now() / 1000);
		this.sleep = options.sleep ?? defaultSleep;
		this.selfPid = options.pid ?? process.pid;
		this.processAlive = options.processAlive ?? defaultProcessAlive;
	}

	static open(
		dbPath: string,
		options: DbTurnLeaseOptions = {},
	): DbTurnLeaseStore {
		const db = new Database(dbPath);
		db.pragma("journal_mode = WAL");
		db.pragma(`busy_timeout = ${options.busyTimeoutMs ?? 5000}`);
		ensureSchema(db);
		return new DbTurnLeaseStore(db, true, options);
	}

	/** Wrap an existing open connection (schema must already exist or will be ensured). */
	static attach(
		db: Database.Database,
		options: DbTurnLeaseOptions = {},
	): DbTurnLeaseStore {
		db.pragma(`busy_timeout = ${options.busyTimeoutMs ?? 5000}`);
		ensureSchema(db);
		return new DbTurnLeaseStore(db, false, options);
	}

	close(): void {
		if (this.ownsDb) this.db.close();
	}

	insertSession(row: {
		id: string;
		parentSessionId?: string | null;
		source?: string | null;
		modelConfig?: string | null;
		endReason?: string | null;
	}): void {
		this.db
			.prepare(
				`INSERT INTO sessions (id, parent_session_id, source, model_config, end_reason)
				 VALUES (?, ?, ?, ?, ?)
				 ON CONFLICT(id) DO UPDATE SET
				   parent_session_id = excluded.parent_session_id,
				   source = excluded.source,
				   model_config = excluded.model_config,
				   end_reason = excluded.end_reason`,
			)
			.run(
				row.id,
				row.parentSessionId ?? null,
				row.source ?? null,
				row.modelConfig ?? null,
				row.endReason ?? null,
			);
	}

	/**
	 * hermes_state.py:_session_turn_lease_key_on_conn — MUST run on the same
	 * connection (inside the same write transaction) as the lease mutation.
	 * Walk compression parents to the lineage root; explicit forks, missing
	 * parents, and cycles terminate the walk.
	 */
	lineageRootOnConn(sessionId: string): string {
		if (!sessionId) return sessionId;
		const stmt = this.db.prepare(
			`SELECT id, parent_session_id, source, model_config, end_reason
			 FROM sessions WHERE id = ?`,
		);
		const rowFor = (sid: string): SessionRow | undefined =>
			stmt.get(sid) as SessionRow | undefined;

		let current = rowFor(sessionId);
		const seen = new Set<string>([sessionId]);
		while (current) {
			const parentId = current.parent_session_id;
			if (!parentId || seen.has(parentId) || isExplicitForkChildRow(current))
				break;
			const parent = rowFor(parentId);
			if (!parent || parent.end_reason !== "compression") break;
			seen.add(parentId);
			current = parent;
		}
		return current ? String(current.id) : sessionId;
	}

	/** Diagnostic parity of hermes_state.py:_session_turn_lease_key (read-only walk + read). */
	lineageRoot(sessionId: string): string {
		return this.lineageRootOnConn(sessionId);
	}

	/**
	 * hermes_state.py:_compression_lock_holder_process_is_dead — True only when
	 * a structured holder's local PID is PROVABLY gone. Unstructured holders,
	 * same-process holders, and any probe doubt stay protected until TTL expiry
	 * (conservative: PID reuse must never steal a live lease).
	 */
	holderProcessIsDead(holder: string): boolean {
		const pid = extractHolderPid(holder);
		if (pid === null || pid <= 0) return false;
		if (pid === this.selfPid) return false; // another thread's live lease; refresher/release own it
		return !this.processAlive(pid);
	}

	/**
	 * hermes_state.py:try_acquire_session_turn_lease — atomic compare-and-set
	 * inside ONE BEGIN IMMEDIATE transaction:
	 *   walk lineage root → read current row → reclaim if expired OR holder
	 *   PID provably dead → INSERT OR IGNORE → ownership = final SELECT matches
	 *   our holder. Exactly one winner per conversation_id, ever.
	 */
	tryAcquire(
		sessionId: string,
		holder: string,
		ttlSeconds: number = DEFAULT_TTL_SECONDS,
	): boolean {
		if (!sessionId || !holder) return false;
		const now = this.nowSeconds();
		const expiresAt = now + Math.max(0.1, ttlSeconds);

		this.db.exec("BEGIN IMMEDIATE");
		try {
			const conversationId = this.lineageRootOnConn(sessionId); // same tx as the write
			const current = this.db
				.prepare(
					"SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
				)
				.get(conversationId) as
				| Pick<LeaseRow, "holder" | "expires_at">
				| undefined;
			if (current) {
				if (
					Number(current.expires_at) <= now ||
					this.holderProcessIsDead(String(current.holder))
				) {
					this.db
						.prepare(
							"DELETE FROM session_turn_leases WHERE conversation_id = ? AND holder = ?",
						)
						.run(conversationId, String(current.holder));
				}
			}
			this.db
				.prepare(
					`INSERT OR IGNORE INTO session_turn_leases
					 (conversation_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
				)
				.run(conversationId, holder, now, expiresAt);
			const owner = this.db
				.prepare(
					"SELECT holder FROM session_turn_leases WHERE conversation_id = ?",
				)
				.get(conversationId) as { holder: string } | undefined;
			this.db.exec("COMMIT");
			return owner !== undefined && owner.holder === holder;
		} catch (err) {
			try {
				this.db.exec("ROLLBACK");
			} catch {
				/* rollback of a failed tx is best-effort */
			}
			throw err;
		}
	}

	/**
	 * hermes_state.py:acquire_session_turn_lease — bounded polling WITHOUT
	 * holding a SQLite lock across polls. should_abort() bails mid-wait;
	 * SQLITE_BUSY keeps polling until the deadline (long holder transactions
	 * must not fail the waiter); on_wait(elapsed) fires on first failure and
	 * ~every waitNoticeIntervalSeconds after.
	 */
	async acquireWait(
		sessionId: string,
		holder: string,
		options: AcquireWaitOptions = {},
	): Promise<boolean> {
		const ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
		const waitSeconds = options.waitSeconds ?? DEFAULT_WAIT_SECONDS;
		const pollIntervalSeconds = Math.max(
			0.01,
			options.pollIntervalSeconds ?? DEFAULT_POLL_INTERVAL_SECONDS,
		);
		const noticeEvery = Math.max(
			0,
			options.waitNoticeIntervalSeconds ?? DEFAULT_WAIT_NOTICE_INTERVAL_SECONDS,
		);

		const startMonotonic = this.monotonicSeconds();
		const deadline = startMonotonic + Math.max(0, waitSeconds);
		let waitStarted: number | null = null;
		let lastNoticeAt: number | null = null;

		for (;;) {
			if (options.shouldAbort?.()) return false;
			try {
				if (this.tryAcquire(sessionId, holder, ttlSeconds)) return true;
			} catch (err) {
				if (!isSqliteBusy(err)) throw err;
				// 'locked' classification parity: keep polling until budget/abort.
			}
			const nowMono = this.monotonicSeconds();
			const remaining = deadline - nowMono;
			if (remaining <= 0) return false;
			if (waitStarted === null) waitStarted = nowMono;
			if (
				options.onWait &&
				(lastNoticeAt === null ||
					noticeEvery === 0 ||
					nowMono - lastNoticeAt >= noticeEvery)
			) {
				options.onWait(Math.max(0, nowMono - waitStarted));
				lastNoticeAt = nowMono;
			}
			await this.sleep(Math.min(pollIntervalSeconds * 1000, remaining * 1000));
		}
	}

	/** hermes_state.py:refresh_session_turn_lease — extend only while still owner. */
	refresh(
		sessionId: string,
		holder: string,
		ttlSeconds: number = DEFAULT_TTL_SECONDS,
	): boolean {
		if (!sessionId || !holder) return false;
		const expiresAt = this.nowSeconds() + Math.max(0.1, ttlSeconds);
		this.db.exec("BEGIN IMMEDIATE");
		try {
			const conversationId = this.lineageRootOnConn(sessionId);
			const info = this.db
				.prepare(
					"UPDATE session_turn_leases SET expires_at = ? WHERE conversation_id = ? AND holder = ?",
				)
				.run(expiresAt, conversationId, holder);
			this.db.exec("COMMIT");
			return info.changes > 0;
		} catch (err) {
			try {
				this.db.exec("ROLLBACK");
			} catch {
				/* best-effort */
			}
			throw err;
		}
	}

	/**
	 * hermes_state.py:release_session_turn_lease — delete only WHERE holder =
	 * ours; idempotent, and a stale holder's release can never free a newer
	 * acquirer's row (the DB analogue of generation-scoped release).
	 */
	releaseHolder(sessionId: string, holder: string): void {
		if (!sessionId || !holder) return;
		this.db.exec("BEGIN IMMEDIATE");
		try {
			const conversationId = this.lineageRootOnConn(sessionId);
			this.db
				.prepare(
					"DELETE FROM session_turn_leases WHERE conversation_id = ? AND holder = ?",
				)
				.run(conversationId, holder);
			this.db.exec("COMMIT");
		} catch (err) {
			try {
				this.db.exec("ROLLBACK");
			} catch {
				/* best-effort */
			}
			throw err;
		}
	}

	/** Read-only ownership probe for tests/diagnostics. */
	probeOwner(sessionId: string): {
		conversationId: string;
		holder: string;
		acquiredAt: number;
		expiresAt: number;
	} | null {
		const conversationId = this.lineageRootOnConn(sessionId);
		const row = this.db
			.prepare(
				"SELECT conversation_id, holder, acquired_at, expires_at FROM session_turn_leases WHERE conversation_id = ?",
			)
			.get(conversationId) as LeaseRow | undefined;
		return row
			? {
					conversationId: String(row.conversation_id),
					holder: String(row.holder),
					acquiredAt: Number(row.acquired_at),
					expiresAt: Number(row.expires_at),
				}
			: null;
	}
}

/**
 * hermes_state.py:_is_explicit_fork_child_row — branch/delegate/tool children
 * are NOT compression continuations. Markers count only when they point AT
 * parent_session_id (a delegate's continuation carries _delegate_from of ITS
 * OWN parent — presence-only matching would misclassify it).
 */
function isExplicitForkChildRow(session: SessionRow): boolean {
	if (session.source === "tool") return true;
	const raw = session.model_config;
	if (!raw) return false;
	let cfg: unknown;
	try {
		cfg = typeof raw === "string" ? JSON.parse(raw) : raw;
	} catch {
		return false;
	}
	if (typeof cfg !== "object" || cfg === null) return false;
	const record = cfg as Record<string, unknown>;
	const branched = record["_branched_from"];
	const delegated = record["_delegate_from"];
	const parentId = session.parent_session_id;
	if (parentId) return branched === parentId || delegated === parentId;
	return branched !== undefined && branched !== null
		? true
		: delegated !== undefined && delegated !== null;
}
