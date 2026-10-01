// handoff/session-lock.ts — cross-process driver lock for foreign sessions
// (DEC-079).
//
// A foreign LIVE session drives only under this lock; without it the gateway
// offers read-only context plus takeover instead of interleaving writes into
// the owner's loop. Mechanism: the instance-guard SQLite-BEGIN-IMMEDIATE
// idiom on a per-session sidecar next to gateway.lock.db, reimplemented here
// — layering forbids pi_embedded from importing pi_gateway/lifecycle, so the
// ~30-line hold cannot be shared. Contention is SQLITE_BUSY on a
// zero-busy-timeout BEGIN IMMEDIATE; the OS file-handle lifetime owns the
// release, so a crashed holder frees automatically exactly like fcntl locks.

import Database from "better-sqlite3";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

/** The local fact of one drive claim: what WE wrote when WE hold it. The
 * claim itself is the open write txn, never a committed row — so no reader
 * can see a holder identity (SQLite hides uncommitted rows cross-connection
 * by design); contention answers only held-or-free. */
export interface LockRecord {
	sessionId: string;
	/** Sidecar file carrying the claim, beside gateway.lock.db. */
	path: string;
	holderPid: number;
	/** Local acquire time while held; zero when released. */
	acquiredAtMs: number;
}

export const SESSION_LOCK_FILENAME_PREFIX = "session-drive-";

function sanitizeSessionId(sessionId: string): string {
	const safe = sessionId.replace(/[^A-Za-z0-9_-]/g, "_");
	if (safe === "") throw new Error("session lock needs a non-empty session id");
	return safe;
}

/** Sidecar file for one session's drive claim, beside gateway.lock.db. */
export function sessionLockPath(lockDir: string, sessionId: string): string {
	return join(
		lockDir,
		`${SESSION_LOCK_FILENAME_PREFIX}${sanitizeSessionId(sessionId)}.lock.db`,
	);
}

const LOCK_TABLE_SQL =
	"CREATE TABLE IF NOT EXISTS session_drive_lock (" +
	"holder_pid INTEGER NOT NULL, acquired_at REAL NOT NULL)";

export class SessionDriverLock {
	readonly record: LockRecord;
	private db: Database.Database | null = null;
	private readonly selfPid: number;

	constructor(
		lockDir: string,
		sessionId: string,
		options: { selfPid?: number } = {},
	) {
		this.selfPid = options.selfPid ?? process.pid;
		this.record = {
			sessionId,
			path: sessionLockPath(lockDir, sessionId),
			holderPid: this.selfPid,
			acquiredAtMs: 0,
		};
	}

	/**
	 * Claim the drive lock. True when held (idempotent re-entry); false when
	 * another live process holds it. The open transaction commits ONLY at
	 * release — a dead holder's fd closes and SQLite rolls back, which IS the
	 * auto-release.
	 */
	acquire(): boolean {
		if (this.db !== null) return true;
		mkdirSync(join(this.record.path, ".."), { recursive: true });
		let db: Database.Database;
		try {
			db = new Database(this.record.path);
		} catch {
			return false;
		}
		try {
			db.pragma("journal_mode = WAL");
			db.pragma("busy_timeout = 0");
			db.exec("BEGIN IMMEDIATE");
			db.exec(LOCK_TABLE_SQL);
			db.prepare("DELETE FROM session_drive_lock").run();
			const acquiredAtMs = Date.now();
			db.prepare(
				"INSERT INTO session_drive_lock (holder_pid, acquired_at) VALUES (?, ?)",
			).run(this.selfPid, acquiredAtMs / 1000);
			this.db = db;
			this.record.acquiredAtMs = acquiredAtMs;
			return true;
		} catch (err) {
			db.close();
			if (isSqliteBusy(err)) return false;
			throw err;
		}
	}

	/** Release if held. Idempotent; never throws. */
	release(): void {
		const db = this.db;
		if (db === null) return;
		this.db = null;
		this.record.acquiredAtMs = 0;
		try {
			db.exec("ROLLBACK");
		} catch {
			try {
				db.exec("COMMIT");
			} catch {
				/* txn already resolved */
			}
		}
		try {
			db.close();
		} catch {
			/* best-effort */
		}
	}

	isHeld(): boolean {
		return this.db !== null;
	}
}

function isSqliteBusy(err: unknown): boolean {
	const code = (err as { code?: string } | null)?.code ?? "";
	return (
		code === "SQLITE_BUSY" ||
		code === "SQLITE_BUSY_SNAPSHOT" ||
		code === "SQLITE_LOCKED"
	);
}

/**
 * Non-acquiring liveness probe: true when another live process holds the
 * session's drive claim right now. A zero-busy-timeout BEGIN IMMEDIATE on
 * a THROWAWAY connection succeeds (rolled back at once) only when nobody
 * holds it. Never creates the parent dir — a missing file reports free.
 */
export function isSessionDriveLocked(
	lockDir: string,
	sessionId: string,
): boolean {
	const path = sessionLockPath(lockDir, sessionId);
	if (!existsSync(path)) return false;
	let db: Database.Database;
	try {
		db = new Database(path);
	} catch {
		return true;
	}
	try {
		db.pragma("busy_timeout = 0");
		db.exec("BEGIN IMMEDIATE");
		db.exec("ROLLBACK");
		return false;
	} catch (err) {
		return isSqliteBusy(err) ? true : true;
	} finally {
		try {
			db.close();
		} catch {
			/* best-effort */
		}
	}
}
