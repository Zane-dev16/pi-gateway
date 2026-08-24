// pi_embedded/cron/tick-lock.ts — cross-process tick lock with the
// contention-vs-fd-exhaustion fork (#87644).
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py:tick                → acquireTickLock / TickLock
//   cron/scheduler.py:_is_lock_contention_errno → isLockContentionErrno
//   cron/scheduler.py:_is_fd_exhaustion   → isFdExhaustion
//   cron/scheduler_provider.py:_backoff_wait_seconds → backoffWaitSeconds
//   cron/scheduler_provider.py:_note_tick_failure    → noteTickFailure
//
// Binding invariant (07 §5.2 tick lock row + 08 §11): only GENUINE lock
// contention (another ticker holds the lock) skips a tick silently (returns
// 0). A REAL acquisition failure — most importantly EMFILE/ENFILE fd
// exhaustion — must NOT be swallowed as "another instance holds the lock":
// that previously made the scheduler appear healthy (tick returned 0,
// heartbeat recorded success) while no job ever ran again. Pi reactions:
//   contention     ⇒ silent skip (debug log, executed=0)
//   fd exhaustion  ⇒ LOUD error + raise; the ticker loop reclaims fds and
//                    retries with exponential backoff (capped at 15 min)
//   other failure  ⇒ LOUD error + raise; loop resets backoff (transient
//                    errors are not the self-inflicted EMFILE storm)
//
// Runtime-idiom adaptation (instance-guard precedent): the lock is an OPEN
// BEGIN IMMEDIATE transaction on a dedicated SQLite sidecar
// (`<cronDir>/.tick.lock.db`, busy_timeout=0). Contention surfaces as
// SQLITE_BUSY; ownership rides the OS file-handle lifetime so a crashed
// holder auto-releases exactly like flock(LOCK_EX) death cleanup. The
// errno-based classifiers keep full flock-parity coverage for raw fs errors.

import { mkdirSync } from "node:fs";
import { join } from "node:path";
import Database from "better-sqlite3";

export const TICK_LOCK_FILENAME = ".tick.lock.db";

/** Backoff cap while consecutive ticks fail with fd exhaustion. */
export const EMFILE_BACKOFF_MAX_SECONDS = 15 * 60;
export const DEFAULT_TICK_INTERVAL_SECONDS = 60;

export type TickFailureKind = "contention" | "fd_exhaustion" | "other";

/** Parity of _is_fd_exhaustion_text: wrapped errors carry the wording. */
function isFdExhaustionText(text: string): boolean {
	const lowered = text.toLowerCase();
	return lowered.includes("too many open files") || lowered.includes("emfile");
}

/** True when exc indicates file-descriptor exhaustion (EMFILE/ENFILE). */
export function isFdExhaustion(err: unknown): boolean {
	const code = (err as { code?: string } | null)?.code ?? "";
	if (code === "EMFILE" || code === "ENFILE") return true;
	return isFdExhaustionText(String(err));
}

/**
 * True when an error from the lock syscall means "the lock is held"
 * (parity of _is_lock_contention_errno):
 * - SQLite sidecar path: SQLITE_BUSY/_SNAPSHOT/_LOCKED on the
 *   zero-busy-timeout BEGIN IMMEDIATE.
 * - Raw fs path (flock parity): EWOULDBLOCK/EAGAIN/EACCES.
 */
export function isLockContention(err: unknown): boolean {
	const code = (err as { code?: string } | null)?.code ?? "";
	if (
		code === "SQLITE_BUSY" ||
		code === "SQLITE_BUSY_SNAPSHOT" ||
		code === "SQLITE_LOCKED"
	) {
		return true;
	}
	return code === "EWOULDBLOCK" || code === "EAGAIN" || code === "EACCES";
}

/** Classify one failed acquisition into the fork the spec demands. */
export function classifyTickAcquireFailure(err: unknown): TickFailureKind {
	if (isLockContention(err)) return "contention";
	if (isFdExhaustion(err)) return "fd_exhaustion";
	return "other";
}

export class TickLockAcquisitionError extends Error {
	readonly kind: Exclude<TickFailureKind, "contention">;

	constructor(kind: "fd_exhaustion" | "other", cause: unknown) {
		super(
			kind === "fd_exhaustion"
				? `Cron tick could not acquire tick lock: ${String(cause)} — ` +
						`fd exhaustion must NOT be swallowed as contention (#87644); ` +
						`scheduler will attempt fd reclamation and retry with backoff`
				: `Cron tick could not acquire tick lock: ${String(cause)}`,
			cause === undefined ? undefined : { cause },
		);
		this.name = "TickLockAcquisitionError";
		this.kind = kind;
	}
}

export interface TickLease {
	release(): void;
}

/** Low-level open+lock seam (fault injection documents the fork honestly). */
export interface TickLockIo {
	openAndLock(path: string): { db: Database.Database };
}

const productionIo: TickLockIo = {
	openAndLock(path: string) {
		// The Database open itself can raise genuine EMFILE under fd
		// exhaustion; BEGIN IMMEDIATE with busy_timeout=0 raises SQLITE_BUSY
		// when another ticker holds the lock.
		mkdirSync(join(path, ".."), { recursive: true });
		const db = new Database(path);
		try {
			db.pragma("journal_mode = WAL");
			db.pragma("busy_timeout = 0");
			db.exec("BEGIN IMMEDIATE");
			return { db };
		} catch (err) {
			try {
				db.close();
			} catch {
				/* best-effort */
			}
			throw err;
		}
	},
};

export interface TickLockOptions {
	selfPid?: number;
	io?: TickLockIo;
}

export type AcquireResult =
	| { acquired: true; lease: TickLease }
	| { acquired: false; kind: "contention" };

export class TickLock {
	readonly path: string;
	private readonly io: TickLockIo;

	constructor(cronDir: string, options: TickLockOptions = {}) {
		void options.selfPid;
		this.io = options.io ?? productionIo;
		this.path = join(cronDir, TICK_LOCK_FILENAME);
	}

	/**
	 * One non-blocking acquisition attempt. Contention ⇒ {acquired:false};
	 * fd exhaustion or any other real failure ⇒ THROW (never masquerade as
	 * a healthy empty tick).
	 */
	acquire(): AcquireResult {
		let db: Database.Database;
		try {
			db = this.io.openAndLock(this.path).db;
		} catch (err) {
			const kind = classifyTickAcquireFailure(err);
			if (kind === "contention") return { acquired: false, kind };
			throw new TickLockAcquisitionError(kind, err);
		}
		return {
			acquired: true,
			lease: {
				release: () => {
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
				},
			},
		};
	}
}

/**
 * Exponential tick backoff shared by the ticker loops (#87644 parity of
 * _backoff_wait_seconds): plain interval while healthy; doubles per
 * CONSECUTIVE fd-exhaustion failure, capped at 15 min. Any other failure
 * resets the counter — backoff is reserved for the self-inflicted EMFILE
 * storm, not transient errors.
 */
export function backoffWaitSeconds(
	intervalSeconds: number,
	consecutiveFailures: number,
): number {
	if (consecutiveFailures <= 0) return intervalSeconds;
	return Math.min(
		intervalSeconds * 2 ** (consecutiveFailures - 1),
		EMFILE_BACKOFF_MAX_SECONDS,
	);
}

/**
 * Classify one failed tick and return the updated failure counter (parity of
 * _note_tick_failure). On fd exhaustion runs the best-effort reclamation so
 * the NEXT tick can succeed; any other failure resets the counter.
 *
 * Reclamation port note: `_reclaim_fds_best_effort` runs gc.collect() +
 * raises RLIMIT_NOFILE's soft limit. Node exposes gc only under
 * --expose-gc (invoked defensively when present); there is no sanctioned
 * runtime API to raise RLIMIT_NOFILE, so the injected `reclaim` hook is the
 * extension point (default: gc-if-available). Never throws.
 */
export function reclaimFdsBestEffort(reclaim?: () => void): void {
	try {
		const maybeGc = (globalThis as { gc?: () => void }).gc;
		maybeGc?.();
	} catch {
		/* never make the ticker worse */
	}
	try {
		reclaim?.();
	} catch {
		/* never make the ticker worse */
	}
}

export function noteTickFailure(
	err: unknown,
	consecutiveFailures: number,
	reclaim?: () => void,
): number {
	if (
		classifyTickAcquireFailure(err) === "fd_exhaustion" ||
		isFdExhaustion(err)
	) {
		reclaimFdsBestEffort(reclaim);
		return consecutiveFailures + 1;
	}
	return 0;
}
