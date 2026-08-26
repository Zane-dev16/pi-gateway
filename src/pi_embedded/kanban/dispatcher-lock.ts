// dispatcher-lock.ts — the machine-global KANBAN DISPATCHER SINGLETON lock.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/kanban_watchers.py:_acquire_singleton_lock → KanbanDispatcherLock.acquire
//     Only ONE gateway process machine-wide may run the embedded kanban
//     dispatcher: concurrent dispatchers double the reclaim frequency (each
//     runs its own release_stale_claims → promote → dispatch loop), double
//     claim-attempt events in the event log, and — with wal_autocheckpoint=0
//     — concurrent manual WAL checkpoints can corrupt index pages. The
//     config flag (kanban.dispatch_in_gateway) is the PRIMARY control; this
//     lock is the BACKSTOP that survives config drift and same-profile
//     restart races. Acquired BEFORE the dispatch loop starts and HELD FOR
//     PROCESS LIFETIME; "contended" ⇒ the caller must NOT dispatch;
//     "unavailable" ⇒ the caller proceeds on config control alone (loud).
//   gateway/kanban_watchers.py:_release_singleton_lock → release
//   gateway/kanban_watchers.py:_owns_kanban_dispatcher_lock → owns
//     Ownership gates the notifier's legacy profile-less subscription rows
//     (include_unowned): they are visible ONLY while THIS process holds the
//     actual dispatcher lock.
//
// Mechanism (DEC-057, DEC-027 mechanism-parity precedent): Node core exposes
// no flock(2) and new native lock dependencies are suspect by default (01
// §5.2), so the advisory lock is held as an OPEN BEGIN IMMEDIATE transaction
// on a dedicated SQLite sidecar `<kanbanHome>/kanban/.dispatcher.lock.db` —
// byte-for-byte the DEC-027 instance-guard idiom applied at the kanban root.
// Contention = SQLITE_BUSY on a zero-busy-timeout BEGIN IMMEDIATE (a
// NON-blocking probe exactly like the reference fcntl LOCK_EX|LOCK_NB);
// ownership rides the OS file-handle lifetime, so a crashed holder
// auto-releases exactly like fcntl locks. The sidecar lives at the
// MACHINE-GLOBAL kanban root (shared across profiles by design, kanban_home()
// anchor) so it serialises ALL gateways on the host, not just one profile's.

import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import Database from "better-sqlite3";

/** Sidecar filename (`.dispatcher.lock` flock parity, `.db` suffix marks the
 * SQLite-sidecar realization per the DEC-027 house idiom). */
export const DISPATCHER_LOCK_FILENAME = ".dispatcher.lock.db";

/** Outcome vocabulary of the reference `_acquire_singleton_lock`. */
export type SingletonLockState = "held" | "contended" | "unavailable";

function isSqliteBusy(err: unknown): boolean {
	const code = (err as { code?: string } | null)?.code ?? "";
	return (
		code === "SQLITE_BUSY" ||
		code === "SQLITE_BUSY_SNAPSHOT" ||
		code === "SQLITE_LOCKED"
	);
}

export class KanbanDispatcherLock {
	readonly path: string;
	private db: Database.Database | null = null;

	constructor(lockPath: string) {
		this.path = lockPath;
	}

	/**
	 * Non-blocking exclusive acquisition. Idempotent for the holder: an
	 * already-owned lock reports "held" again without touching the txn.
	 */
	acquire(): SingletonLockState {
		if (this.db !== null) return "held";
		let db: Database.Database;
		try {
			mkdirSync(dirname(this.path), { recursive: true });
			db = new Database(this.path);
		} catch {
			return "unavailable"; // OSError parity: locking cannot be performed
		}
		try {
			db.pragma("journal_mode = WAL");
			db.pragma("busy_timeout = 0"); // NON-blocking probe (LOCK_NB parity)
			db.exec("BEGIN IMMEDIATE");
		} catch (err) {
			try {
				db.close();
			} catch {
				/* best-effort */
			}
			if (isSqliteBusy(err)) return "contended";
			return "unavailable";
		}
		this.db = db;
		return "held";
	}

	/** True while THIS object holds the machine-global dispatcher role
	 * (parity _owns_kanban_dispatcher_lock). */
	owns(): boolean {
		return this.db !== null;
	}

	/** Release at process/service shutdown (parity _release_singleton_lock).
	 * Best-effort like the reference: never throws. */
	release(): void {
		const db = this.db;
		this.db = null;
		if (db === null) return;
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
}

/**
 * Process-shared lock registry: every consumer in ONE gateway process that
 * resolves the same machine-global path gets THE SAME handle — the port of
 * Hermes storing `_kanban_dispatcher_lock_handle` on the runner so the
 * dispatcher holds it while the notifier merely consults ownership. One
 * gateway process runs one dispatcher; the registry makes that assumption
 * structural instead of hopeful.
 */
const sharedLocks = new Map<string, KanbanDispatcherLock>();

export function sharedKanbanDispatcherLock(
	lockPath: string,
): KanbanDispatcherLock {
	let lock = sharedLocks.get(lockPath);
	if (lock === undefined) {
		lock = new KanbanDispatcherLock(lockPath);
		sharedLocks.set(lockPath, lock);
	}
	return lock;
}
