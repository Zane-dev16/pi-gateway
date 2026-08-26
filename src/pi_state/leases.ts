// pi_state/leases.ts — the cross-process turn-lease store (layer 2 of the two
// cooperating lease layers, 02-session-and-state.md §5 / DEC-004).
//
// The durable key is the compression-lineage ROOT of the resolved session id,
// walked in the SAME write transaction as the lease mutation
// (hermes_state.py:_session_turn_lease_key_on_conn) — a prior read failure must
// never let a later write fail-open onto the wrong key. This is what makes
// compression rotation safe: all segments of one conversation contend on ONE
// lease row.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored —
// production-quality port of the proven Phase-0 spike shape):
//   hermes_state.py:_session_turn_lease_key_on_conn      → lineageRootOnConn / lineageRootOnDb
//   hermes_state.py:_is_explicit_fork_child_row          → isExplicitForkChildRow
//   hermes_state.py:try_acquire_session_turn_lease       → tryAcquire (atomic CAS in BEGIN IMMEDIATE)
//   hermes_state.py:acquire_session_turn_lease           → acquireWait (bounded poll, on_wait, abort)
//   hermes_state.py:refresh_session_turn_lease           → refresh (holder-scoped extend)
//   hermes_state.py:release_session_turn_lease           → releaseHolder (holder-scoped delete = generation-safe release)
//   hermes_state.py:_compression_lock_holder_process_is_dead (+ _COMPRESSION_LOCK_HOLDER_PID_RE)
//                                                        → holderProcessIsDead / extractHolderPid
//   hermes_state.py:SessionTurnLeaseLostError            → SessionTurnLeaseLostError
//   hermes_state.py:_check_transcript_write_guards       → checkTurnLeaseWriteGuardOnConn
//                                                          (turn-lease admission leg, in-txn)
//
// Defaults per 02 §5: TTL 300s; waiting acquire polls ≤1800s @1s with
// on_wait(elapsed) notices and should_abort() bail-out.
//
// Losing the lease mid-turn is a detected, handled condition — callers treat
// refresh()===false / probeOwner() mismatch as the first-class "turn_lease"
// failure category, never a crash. (The in-process layer-1 registry lives in
// pi_gateway per 01 §5.3 — NOT here.)

import type Database from "better-sqlite3";
import { performance } from "node:perf_hooks";

export const DEFAULT_TTL_SECONDS = 300;
export const DEFAULT_WAIT_SECONDS = 1800;
export const DEFAULT_POLL_INTERVAL_SECONDS = 1.0;
export const DEFAULT_WAIT_NOTICE_INTERVAL_SECONDS = 15;

/**
 * Structured holder ids embed `pid=<n>` so a dead holder can be PROVEN dead
 * without waiting out the TTL. Regex parity of
 * hermes_state.py:_COMPRESSION_LOCK_HOLDER_PID_RE: `(?:^|:)pid=(\d+)(?::|$)`.
 */
const HOLDER_PID_RE = /(?:^|:)pid=(\d+)(?::|$)/;

/**
 * hermes_state.py:SessionTurnLeaseLostError — a transcript write presented a
 * turn-lease holder that no longer owns it (foreign holder, released, or
 * reclaimed). Fail-fast fencing: never retried by the write ladder; callers
 * surface the first-class "turn_lease" persistence-failure category (02 §5),
 * never silent interleaving with a newer turn.
 */
export class SessionTurnLeaseLostError extends Error {
	readonly sessionId: string;
	constructor(sessionId: string, detail?: string) {
		super(
			`Session turn lease lost; refusing transcript write for ${sessionId}${detail ? ` (${detail})` : ""}`,
		);
		this.name = "SessionTurnLeaseLostError";
		this.sessionId = sessionId;
	}
}

export function extractHolderPid(holder: string): number | null {
	const m = HOLDER_PID_RE.exec(holder ?? "");
	if (!m || m[1] === undefined) return null;
	const pid = Number.parseInt(m[1], 10);
	return Number.isFinite(pid) ? pid : null;
}

/** Build a structured holder id reclaimable via dead-PID detection. */
export function structuredHolder(prefix: string, pid: number): string {
	return `${prefix}:pid=${pid}`;
}

/**
 * Conservative kernel liveness probe (POSIX): ESRCH proves death, everything
 * else is doubt. Any doubt ⇒ alive — TTL remains the recovery path.
 */
function defaultProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ESRCH") return false;
		return true; // EPERM etc. → any doubt → alive
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

export interface TurnLeaseStoreOptions {
	/** Injected wall clock in SECONDS (parity of time.time()). */
	nowSeconds?: () => number;
	/** Injected monotonic clock in SECONDS (parity of time.monotonic()). */
	monotonicSeconds?: () => number;
	/** Injected sleep for the wait loop. */
	sleep?: (ms: number) => Promise<void>;
	/** Override self-PID (tests). Default process.pid. */
	pid?: number;
	/** Liveness probe override (tests). Default ESRCH-only kill(pid,0). */
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

/**
 * hermes_state.py:_is_explicit_fork_child_row — branch/delegate/tool children
 * are NOT compression continuations. Markers count only when they point AT
 * parent_session_id (a delegate's continuation carries _delegate_from of ITS
 * OWN parent — presence-only matching would misclassify it).
 */
export function isExplicitForkChildRow(session: {
	source: string | null;
	model_config: string | null;
	parent_session_id: string | null;
}): boolean {
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

/**
 * Standalone form of the lineage-root walk so IN-TRANSACTION guards on an
 * arbitrary connection share ONE implementation with DbTurnLeaseStore
 * (hermes_state.py:_session_turn_lease_key_on_conn is likewise connection-
 * scoped, not store-scoped).
 */
export function lineageRootOnDb(
	db: Database.Database,
	sessionId: string,
): string {
	if (!sessionId) return sessionId;
	const stmt = db.prepare(
		`SELECT id, parent_session_id, source, model_config, end_reason
		 FROM sessions WHERE id = ?`,
	);
	const rowFor = (sid: string): SessionRow | undefined =>
		stmt.get(sid) as SessionRow | undefined;

	let current = rowFor(sessionId);
	const seen = new Set<string>([sessionId]);
	while (current) {
		const parentId = current.parent_session_id;
		if (!parentId || seen.has(parentId) || isExplicitForkChildRow(current)) {
			break;
		}
		const parent = rowFor(parentId);
		if (!parent || parent.end_reason !== "compression") break;
		seen.add(parentId);
		current = parent;
	}
	return current ? String(current.id) : sessionId;
}

/**
 * hermes_state.py:_check_transcript_write_guards (turn-lease leg) — transcript-
 * write ADMISSION check run INSIDE the write transaction, on the SAME
 * connection as the pending insert. When `holder` is presented:
 *
 *  • a missing row OR a foreign holder raises SessionTurnLeaseLostError —
 *    landing this write would interleave a stale flush after another process
 *    reclaimed the lineage slot (the >TTL-stalled-writer window the periodic
 *    refresher cannot close);
 *  • an EXPIRED but still-matching lease is RENEWED in this same transaction:
 *    expiry makes the row reclaimable but does not prove a takeover occurred,
 *    and BEGIN IMMEDIATE serializes this renewal with acquisition, so a
 *    still-matching owner recovers from a starved refresher without weakening
 *    the foreign-holder fence.
 *
 * Absent/empty `holder` skips the guard entirely (parity: appends without a
 * turn_lease_holder run no ownership check — shutdown-recovery replays and
 * user-initiated mutations keep working).
 */
export function checkTurnLeaseWriteGuardOnConn(
	conn: Database.Database,
	sessionId: string,
	opts: {
		holder?: string | null;
		ttlSeconds?: number;
		/** Injected wall clock in SECONDS (tests). Default Date.now()/1000. */
		nowSeconds?: () => number;
	},
): void {
	const holder = opts.holder;
	if (!holder) return;
	const conversationId = lineageRootOnDb(conn, sessionId); // same tx as the write
	const now = (opts.nowSeconds ?? (() => Date.now() / 1000))();
	const lease = conn
		.prepare(
			"SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
		)
		.get(conversationId) as Pick<LeaseRow, "holder" | "expires_at"> | undefined;
	if (lease === undefined || String(lease.holder) !== holder) {
		throw new SessionTurnLeaseLostError(sessionId);
	}
	if (Number(lease.expires_at) <= now) {
		conn
			.prepare(
				"UPDATE session_turn_leases SET expires_at = ? WHERE conversation_id = ? AND holder = ?",
			)
			.run(
				now + Math.max(0.1, opts.ttlSeconds ?? DEFAULT_TTL_SECONDS),
				conversationId,
				holder,
			);
	}
}

/**
 * Cross-process turn leases over `session_turn_leases(conversation_id PK,
 * holder, acquired_at, expires_at)` (02 §2.1 DDL). Attach to an ALREADY-OPEN,
 * already-initialized connection (StateStore owns open/init/close).
 *
 * Every mutating method walks the lineage root and writes inside ONE
 * BEGIN IMMEDIATE transaction. SQLITE_BUSY propagates to acquireWait's
 * polling loop but fails fast elsewhere (callers retry via the write ladder).
 */
export class DbTurnLeaseStore {
	private readonly db: Database.Database;
	private readonly nowSecondsFn: () => number;
	private readonly monotonicSecondsFn: () => number;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly selfPid: number;
	private readonly processAliveFn: (pid: number) => boolean;

	constructor(db: Database.Database, options: TurnLeaseStoreOptions = {}) {
		this.db = db;
		this.nowSecondsFn = options.nowSeconds ?? (() => Date.now() / 1000);
		this.monotonicSecondsFn =
			options.monotonicSeconds ?? (() => performance.now() / 1000);
		this.sleepFn = options.sleep ?? defaultSleep;
		this.selfPid = options.pid ?? process.pid;
		this.processAliveFn = options.processAlive ?? defaultProcessAlive;
		db.pragma(`busy_timeout = ${options.busyTimeoutMs ?? 5000}`);
	}

	/**
	 * MUST run on the same connection (inside the same write transaction) as
	 * the lease mutation (hermes_state.py:_session_turn_lease_key_on_conn):
	 * walk `parent_session_id` upward while the parent's end_reason is
	 * 'compression'; explicit forks (branch/delegate/tool children), missing
	 * parents, and cycles terminate the walk. Lock errors propagate so callers
	 * can retry — never swallow-and-fail-open.
	 */
	lineageRootOnConn(sessionId: string): string {
		return lineageRootOnDb(this.db, sessionId);
	}

	/** Diagnostic/read-only walk (hermes_state.py:_session_turn_lease_key). */
	lineageRoot(sessionId: string): string {
		return this.lineageRootOnConn(sessionId);
	}

	/**
	 * True only when a structured holder's local PID is PROVABLY gone
	 * (hermes_state.py:_compression_lock_holder_process_is_dead). Unstructured
	 * holders, same-process holders, and any probe doubt stay protected until
	 * TTL expiry — PID reuse must never steal a live lease.
	 */
	holderProcessIsDead(holder: string): boolean {
		const pid = extractHolderPid(holder);
		if (pid === null || pid <= 0) return false;
		if (pid === this.selfPid) {
			// Same-process holder (another async task's live lease): never
			// self-reclaim — refresher/release own it.
			return false;
		}
		return !this.processAliveFn(pid);
	}

	/**
	 * Atomic compare-and-set inside ONE BEGIN IMMEDIATE transaction
	 * (try_acquire_session_turn_lease): walk lineage root → read current row →
	 * reclaim if expired OR holder PID provably dead → INSERT OR IGNORE →
	 * ownership = final SELECT matches our holder. Exactly one winner ever.
	 */
	tryAcquire(
		sessionId: string,
		holder: string,
		ttlSeconds: number = DEFAULT_TTL_SECONDS,
	): boolean {
		if (!sessionId || !holder) return false;
		const now = this.nowSecondsFn();
		const expiresAt = now + Math.max(0.1, ttlSeconds);

		this.db.exec("BEGIN IMMEDIATE");
		try {
			const conversationId = this.lineageRootOnConn(sessionId); // same tx as the write
			const selectCurrent = this.db.prepare(
				"SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
			);
			const insertIgnored = this.db.prepare(
				`INSERT OR IGNORE INTO session_turn_leases
				 (conversation_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
			);
			const selectOwner = this.db.prepare(
				"SELECT holder FROM session_turn_leases WHERE conversation_id = ?",
			);

			const current = selectCurrent.get(conversationId) as
				| Pick<LeaseRow, "holder" | "expires_at">
				| undefined;
			if (
				current &&
				(Number(current.expires_at) <= now ||
					this.holderProcessIsDead(String(current.holder)))
			) {
				this.db
					.prepare(
						"DELETE FROM session_turn_leases WHERE conversation_id = ? AND holder = ?",
					)
					.run(conversationId, String(current.holder));
			}
			insertIgnored.run(conversationId, holder, now, expiresAt);
			const owner = selectOwner.get(conversationId) as
				| { holder: string }
				| undefined;
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
	 * Bounded polling WITHOUT holding a SQLite lock across polls
	 * (acquire_session_turn_lease). should_abort() bails mid-wait; SQLITE_BUSY
	 * keeps polling until the deadline (long holder transactions must not fail
	 * the waiter); other SQLite errors propagate. on_wait(elapsed) fires on the
	 * first failure and ~every waitNoticeIntervalSeconds after.
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

		const startMonotonic = this.monotonicSecondsFn();
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
			const nowMono = this.monotonicSecondsFn();
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
			await this.sleepFn(
				Math.min(pollIntervalSeconds * 1000, remaining * 1000),
			);
		}
	}

	/** Extend only while still owner; false means we lost the lease mid-turn. */
	refresh(
		sessionId: string,
		holder: string,
		ttlSeconds: number = DEFAULT_TTL_SECONDS,
	): boolean {
		if (!sessionId || !holder) return false;
		const expiresAt = this.nowSecondsFn() + Math.max(0.1, ttlSeconds);
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
	 * Release iff still ours — DELETE … WHERE holder = ours; idempotent, and a
	 * stale holder's release can never free a newer acquirer's row (the DB
	 * analogue of generation-scoped release).
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

	describe(): string {
		return `DbTurnLeaseStore(pid=${this.selfPid})`;
	}
}
