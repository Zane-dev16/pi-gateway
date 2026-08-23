// pi_gateway/lifecycle/instance-guard.ts — duplicate-instance guard
// primitives: PID file, cross-process runtime lock, live-instance probe.
//
// Spec: /root/pi-gateway/08-operations.md §1.1 stage 4 ("PID file scoped to
// PI_HOME"); 01-architecture.md §3.2. Hermes anchors (READ-ONLY reference;
// semantics ported, no code vendored — gateway/status.py):
//   _get_pid_path / write_pid_file / remove_pid_file → pidFilePath /
//     writePidFile (atomic O_CREAT|O_EXCL race) / removePidFile
//     (ownership-guarded unlink)
//   acquire_gateway_runtime_lock / release_gateway_runtime_lock /
//     is_gateway_runtime_lock_active                → RuntimeLock
//   get_running_pid                                 → getRunningPid
//   _build_pid_record                               → buildPidRecord shape
//
// Runtime-lock mechanism note (runtime-idiom adaptation under DEC-023, same
// semantics): Node core exposes no flock(2), and new lock dependencies are
// "suspect by default" (01 §5.2). The lock is therefore held as an OPEN
// BEGIN IMMEDIATE transaction on a dedicated SQLite sidecar
// (`<home>/gateway.lock.db`, sibling of the PID file so it stays scoped to the
// profile home). Contention = SQLITE_BUSY on a zero-busy-timeout BEGIN
// IMMEDIATE; ownership rides the OS file-handle lifetime, so a crashed holder
// releases automatically exactly like fcntl locks do. Proposed DEC entry in
// the phase report records this adaptation.

import Database from "better-sqlite3";
import {
	closeSync,
	existsSync,
	mkdirSync,
	openSync,
	readFileSync,
	rmSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import {
	processAlive,
	processStartTime,
	startTimeMatches,
} from "./process-info.js";

export const PID_FILENAME = "gateway.pid";
export const LOCK_DB_FILENAME = "gateway.lock.db";
const GATEWAY_KIND = "pi-gateway";

export interface PidRecord {
	pid: number;
	kind: string;
	argv: string[];
	start_time: number | null;
	pi_home: string;
}

export function pidFilePath(home: string): string {
	return join(home, PID_FILENAME);
}

export function lockDbPath(home: string): string {
	return join(home, LOCK_DB_FILENAME);
}

/** Parity of status.py:_build_pid_record (hermes_home → pi_home). */
export function buildPidRecord(home: string, selfPid?: number): PidRecord {
	return {
		pid: selfPid ?? process.pid,
		kind: GATEWAY_KIND,
		argv: [...process.argv],
		start_time: processStartTime(selfPid ?? process.pid),
		pi_home: home,
	};
}

function readPidRecordAt(path: string): PidRecord | null {
	try {
		const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
		if (typeof parsed !== "object" || parsed === null) return null;
		const record = parsed as Partial<PidRecord>;
		if (typeof record.pid !== "number" || !Number.isFinite(record.pid))
			return null;
		return {
			pid: record.pid,
			kind: typeof record.kind === "string" ? record.kind : GATEWAY_KIND,
			argv: Array.isArray(record.argv) ? record.argv.map(String) : [],
			start_time:
				typeof record.start_time === "number" ? record.start_time : null,
			pi_home: typeof record.pi_home === "string" ? record.pi_home : "",
		};
	} catch {
		return null;
	}
}

export function readPidFile(home: string): PidRecord | null {
	const path = pidFilePath(home);
	return existsSync(path) ? readPidRecordAt(path) : null;
}

/**
 * Atomic O_CREAT|O_EXCL claim of the PID file (status.py:write_pid_file).
 * Concurrent starters race; exactly one wins. Idempotent re-entry: when the
 * existing record already names THIS process (same start time where known),
 * the claim succeeds — a partially-completed boot may re-run its stages.
 * Returns false only when another live starter owns the race.
 */
export function writePidFile(
	home: string,
	options: { selfPid?: number } = {},
): boolean {
	const path = pidFilePath(home);
	mkdirSync(dirname(path), { recursive: true });
	const record = buildPidRecord(home, options.selfPid);
	let fd: number;
	try {
		fd = openSync(path, "wx"); // O_CREAT | O_EXCL | O_WRONLY
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
		// Lost the create race — but is the winner US (idempotent re-run)?
		const existing = readPidRecordAt(path);
		if (
			existing !== null &&
			startTimeMatches(
				existing.start_time,
				record.start_time,
				existing.pid === record.pid,
			)
		) {
			return true;
		}
		return false;
	}
	try {
		writeFileSync(fd, JSON.stringify(record));
		closeSync(fd);
	} catch (err) {
		try {
			closeSync(fd);
		} catch {
			/* already closed */
		}
		try {
			unlinkSync(path);
		} catch {
			/* best-effort cleanup parity */
		}
		throw err;
	}
	return true;
}

interface RemovePidOptions {
	selfPid?: number;
	selfStartTime?: number | null;
}

/**
 * Ownership-guarded removal (status.py:remove_pid_file — "only unlinks a PID
 * file that belongs to this process"). A stale record naming a DIFFERENT pid
 * is left alone; callers doing takeover force-unlink explicitly instead.
 */
export function removePidFile(
	home: string,
	options: RemovePidOptions = {},
): void {
	const path = pidFilePath(home);
	if (!existsSync(path)) return;
	const record = readPidRecordAt(path);
	if (record === null) {
		try {
			unlinkSync(path); // unparseable junk cannot belong to anyone
		} catch {
			/* best-effort */
		}
		return;
	}
	const selfPid = options.selfPid ?? process.pid;
	const selfStartTime =
		options.selfStartTime !== undefined
			? options.selfStartTime
			: processStartTime(selfPid);
	if (
		!startTimeMatches(record.start_time, selfStartTime, record.pid === selfPid)
	) {
		return; // names someone else — never grief their shutdown
	}
	try {
		unlinkSync(path);
	} catch {
		/* best-effort */
	}
}

/** Force-unlink regardless of ownership (takeover cleanup parity, run.py). */
export function forceRemovePidFile(home: string): void {
	try {
		rmSync(pidFilePath(home), { force: true });
	} catch {
		/* best-effort */
	}
}

// ---------------------------------------------------------------------------
// Cross-process runtime lock (SQLite-held BEGIN IMMEDIATE — see module header)
// ---------------------------------------------------------------------------

const LOCK_TABLE_SQL =
	"CREATE TABLE IF NOT EXISTS runtime_lock (" +
	"holder_pid INTEGER NOT NULL, acquired_at REAL NOT NULL)";

export class RuntimeLock {
	private db: Database.Database | null = null;
	readonly path: string;
	private readonly selfPid: number;

	constructor(homeOrPath: string, options: { selfPid?: number } = {}) {
		this.path = homeOrPath.endsWith(".db")
			? homeOrPath
			: lockDbPath(homeOrPath);
		this.selfPid = options.selfPid ?? process.pid;
	}

	/**
	 * Claim the lock. True when we hold it (idempotent re-entry); false when
	 * another live process holds it. The open transaction is committed ONLY at
	 * release — the OS closes the fd (and SQLite rolls the txn back) if this
	 * process dies, which IS the auto-release property of flock.
	 */
	acquire(): boolean {
		if (this.db !== null) return true;
		mkdirSync(dirname(this.path), { recursive: true });
		let db: Database.Database;
		try {
			db = new Database(this.path);
		} catch {
			return false;
		}
		try {
			db.pragma("journal_mode = WAL");
			db.pragma("busy_timeout = 0");
			db.exec("BEGIN IMMEDIATE");
			db.exec(LOCK_TABLE_SQL);
			db.prepare("DELETE FROM runtime_lock").run();
			db.prepare(
				"INSERT INTO runtime_lock (holder_pid, acquired_at) VALUES (?, ?)",
			).run(this.selfPid, Date.now() / 1000);
			this.db = db;
			return true;
		} catch (err) {
			db.close();
			if (isSqliteBusy(err)) return false; // contended — another live holder
			throw err;
		}
	}

	/** Release if we hold it. Idempotent; never throws. */
	release(): void {
		const db = this.db;
		if (db === null) return;
		this.db = null;
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
 * Non-acquiring liveness probe (parity of is_gateway_runtime_lock_active):
 * try BEGIN IMMEDIATE with busy_timeout=0 on a THROWAWAY connection; success
 * ⇒ nobody holds it (probe txn rolled back immediately). Never creates the
 * parent dir — probing a nonexistent lock file simply reports inactive.
 */
export function isRuntimeLockActive(homeOrPath: string): boolean {
	const path = homeOrPath.endsWith(".db") ? homeOrPath : lockDbPath(homeOrPath);
	if (!existsSync(path)) return false;
	let db: Database.Database;
	try {
		db = new Database(path);
	} catch {
		return true; // unreadable ⇒ assume held (fail closed against double-run)
	}
	try {
		db.pragma("busy_timeout = 0");
		db.exec("BEGIN IMMEDIATE");
		db.exec("ROLLBACK");
		return false;
	} catch (err) {
		return isSqliteBusy(err) ? true : true; // any error ⇒ conservative: active
	} finally {
		try {
			db.close();
		} catch {
			/* best-effort */
		}
	}
}

// ---------------------------------------------------------------------------
// Live-instance probe (status.py:get_running_pid)
// ---------------------------------------------------------------------------

export interface RunningInstance {
	pid: number;
	record: PidRecord;
}

export interface GetRunningPidOptions {
	/** Accepted for call-site compatibility; the reuse guard is pid-agnostic. */
	selfPid?: number;
	/** When false, stale pid files are reported rather than cleaned. */
	cleanupStale?: boolean;
}

/**
 * Return the live gateway instance for this home, or null. Evidence chain
 * (status.py:get_running_pid): PID file record must name a PROVABLY alive
 * process whose start time still matches (PID-reuse guard), AND the runtime
 * lock must be active — a lock-less PID file is a stale leftover from a dead
 * instance and gets cleaned up.
 */
export function getRunningPid(
	home: string,
	options: GetRunningPidOptions = {},
): RunningInstance | null {
	const cleanupStale = options.cleanupStale ?? true;
	const path = pidFilePath(home);
	const record = existsSync(path) ? readPidRecordAt(path) : null;
	if (record === null) return null;

	const lockActive = isRuntimeLockActive(lockDbPath(home));
	if (!lockActive) {
		if (cleanupStale) forceRemovePidFile(home);
		return null;
	}
	if (!processAlive(record.pid)) {
		if (cleanupStale) forceRemovePidFile(home);
		return null;
	}
	// PID-reuse guard applies to ANY recorded pid (status.py compares the
	// record against the live process unconditionally): a start_time mismatch
	// means the original holder exited and the OS recycled the PID.
	const liveStart = processStartTime(record.pid);
	if (!startTimeMatches(record.start_time, liveStart, true)) {
		if (cleanupStale) forceRemovePidFile(home);
		return null;
	}
	return { pid: record.pid, record };
}
