// pi_embedded/cron/jobs-lock.ts — critical-section lock around
// load_jobs → modify → save_jobs cycles.
//
// Hermes anchors (READ-ONLY reference):
//   cron/jobs.py:_jobs_file_lock (threading.RLock — the in-process half)
//   cron/jobs.py:_JOBS_LOCK_TIMEOUT_SECONDS = 30 (#60703: an UNBOUNDED wait
//     on a lock held by a wedged sibling process freezes the ticker forever;
//     every waiter fails LOUDLY past the bound instead)
//
// Runtime-idiom adaptation (same precedent as pi_gateway/lifecycle/
// instance-guard.ts RuntimeLock, proposed-DEC discipline): Node core exposes
// no flock(2) and new lock dependencies are suspect by default (01 §5.2), so
// the cross-process half holds an OPEN BEGIN IMMEDIATE transaction on a
// dedicated SQLite sidecar (`<cronDir>/.jobs.lock.db`). Ownership rides the
// OS file-handle lifetime — a crashed holder releases automatically exactly
// like fcntl locks. The in-process half is a promise-chain mutex (≙ RLock);
// all store mutations funnel through withJobsLock so concurrent callers can
// never clobber each other's read-modify-write cycles.

import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import Database from "better-sqlite3";

/** Upper bound waiting for the cross-process lock (#60703 parity). */
export const JOBS_LOCK_TIMEOUT_MS = 30_000;

export class JobsLockTimeoutError extends Error {
	constructor(path: string, waitedMs: number) {
		super(
			`cron jobs lock at ${path} still held after ${waitedMs}ms — refusing to ` +
				`freeze the ticker behind a wedged holder (#60703)`,
		);
		this.name = "JobsLockTimeoutError";
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

export class JobsFileLock {
	readonly path: string;
	private db: Database.Database | null = null;
	private chain: Promise<unknown> = Promise.resolve();
	private closed = false;

	constructor(cronDir: string, filename = ".jobs.lock.db") {
		this.path = join(cronDir, filename);
	}

	private connection(): Database.Database {
		if (this.db !== null) return this.db;
		mkdirSync(dirname(this.path), { recursive: true });
		const db = new Database(this.path);
		db.pragma("journal_mode = WAL");
		this.db = db;
		return db;
	}

	/**
	 * Serialize `fn` against every other withJobsLock caller IN THIS PROCESS,
	 * then take the cross-process sidecar transaction around it. A sibling
	 * process holding the lock causes retries-with-polling up to the 30s
	 * bound, then JobsLockTimeoutError (loud, never a silent infinite stall).
	 */
	async withJobsLock<T>(fn: () => Promise<T> | T): Promise<T> {
		if (this.closed) throw new Error("jobs lock is closed");
		// FIFO in-process mutex (≙ _jobs_file_lock RLock): chain onto the tail
		// of every previous holder; each caller resolves its own slot when done,
		// so the next enqueued caller observes full mutual exclusion.
		const prev = this.chain;
		let release!: () => void;
		const slot = new Promise<void>((resolvePromise) => {
			release = resolvePromise;
		});
		this.chain = prev.then(() => slot);
		await prev.catch(() => undefined); // wait for our turn
		try {
			await this.acquireCrossProcess();
			try {
				return await fn();
			} finally {
				this.releaseCrossProcess();
			}
		} finally {
			release();
		}
	}

	private async acquireCrossProcess(): Promise<void> {
		const db = this.connection();
		const deadline = Date.now() + JOBS_LOCK_TIMEOUT_MS;
		for (;;) {
			db.pragma(`busy_timeout = 250`);
			try {
				db.exec("BEGIN IMMEDIATE");
				return;
			} catch (err) {
				if (!isSqliteBusy(err)) throw err;
				if (Date.now() >= deadline) {
					throw new JobsLockTimeoutError(this.path, JOBS_LOCK_TIMEOUT_MS);
				}
				await new Promise<void>((r) => setTimeout(r, 25));
			}
		}
	}

	private releaseCrossProcess(): void {
		const db = this.db;
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
	}

	close(): void {
		this.closed = true;
		const db = this.db;
		this.db = null;
		try {
			db?.close();
		} catch {
			/* best-effort */
		}
	}
}
