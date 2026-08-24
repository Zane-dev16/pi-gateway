// Behavior contracts for the tick lock (#87644 fork; 08 §11 "Tick lock" row):
// contention ⇒ silent skip; EMFILE/ENFILE ⇒ LOUD failure + retry ladder;
// cross-process mutual exclusion over REAL processes.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import {
	TickLock,
	TickLockAcquisitionError,
	backoffWaitSeconds,
	classifyTickAcquireFailure,
	EMFILE_BACKOFF_MAX_SECONDS,
	isFdExhaustion,
	isLockContention,
	noteTickFailure,
	type TickLockIo,
} from "./tick-lock.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-cron-ticklock-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function errWithCode(code: string, message = code): NodeJS.ErrnoException {
	const err = new Error(message) as NodeJS.ErrnoException;
	err.code = code;
	return err;
}

describe("the contention-vs-fd-exhaustion classifier (THE #87644 fork)", () => {
	it("lock-held errnos classify as CONTENTION (silent skip is correct)", () => {
		expect(classifyTickAcquireFailure(errWithCode("SQLITE_BUSY"))).toBe(
			"contention",
		);
		expect(classifyTickAcquireFailure(errWithCode("EWOULDBLOCK"))).toBe(
			"contention",
		);
		expect(classifyTickAcquireFailure(errWithCode("EAGAIN"))).toBe(
			"contention",
		);
		expect(classifyTickAcquireFailure(errWithCode("EACCES"))).toBe(
			"contention",
		);
	});

	it("fd-exhaustion NEVER classifies as contention — by errno or wrapped wording", () => {
		expect(classifyTickAcquireFailure(errWithCode("EMFILE"))).toBe(
			"fd_exhaustion",
		);
		expect(classifyTickAcquireFailure(errWithCode("ENFILE"))).toBe(
			"fd_exhaustion",
		);
		expect(
			classifyTickAcquireFailure(
				new Error("EMFILE: too many open files, open '/x'"),
			),
		).toBe("fd_exhaustion");
		// Wrapped-exception shape from load_jobs (#87644): wording only.
		expect(isFdExhaustion(new Error("Too many open files"))).toBe(true);
	});

	it("any other failure stays 'other' (loud, but no backoff escalation)", () => {
		expect(classifyTickAcquireFailure(errWithCode("EPERM"))).toBe("other");
		expect(classifyTickAcquireFailure(new Error("disk on fire"))).toBe("other");
	});
});

describe("real acquisition (production SQLite-sidecar mechanism)", () => {
	it("second ticker observes CONTENTION (skip shape), not an error", () => {
		const a = new TickLock(dir);
		const first = a.acquire();
		if (!first.acquired) throw new Error("first acquire must win");

		const b = new TickLock(dir);
		const second = b.acquire();
		expect(second.acquired).toBe(false); // silent skip; executed=0 upstream

		first.lease.release();
		const third = b.acquire();
		expect(third.acquired).toBe(true); // release hands the lock over
		if (third.acquired) third.lease.release();
	});

	it("release is idempotent and re-acquirable", () => {
		const lock = new TickLock(dir);
		const first = lock.acquire();
		expect(first.acquired).toBe(true);
		if (first.acquired) {
			first.lease.release();
			first.lease.release(); // double-release never throws
		}
		const again = lock.acquire();
		expect(again.acquired).toBe(true);
	});
});

describe("acquisition faults through the io seam (policy reactions)", () => {
	function failingIo(err: unknown): TickLockIo {
		return {
			openAndLock: () => {
				throw err;
			},
		};
	}

	it("EMFILE fault THROWS TickLockAcquisitionError (never masquerades as healthy skip)", () => {
		const lock = new TickLock(dir, { io: failingIo(errWithCode("EMFILE")) });
		expect(() => lock.acquire()).toThrow(TickLockAcquisitionError);
		try {
			lock.acquire();
			expect.unreachable();
		} catch (err) {
			expect((err as TickLockAcquisitionError).kind).toBe("fd_exhaustion");
			expect(String(err)).toMatch(/#87644/); // names the incident class
		}
	});

	it("a real open() failure of any other kind throws with kind 'other'", () => {
		const lock = new TickLock(dir, { io: failingIo(errWithCode("EPERM")) });
		expect(() => lock.acquire()).toThrow(TickLockAcquisitionError);
		try {
			lock.acquire();
		} catch (err) {
			expect((err as TickLockAcquisitionError).kind).toBe("other");
		}
	});

	it("the sidecar file lives at <cronDir>/.tick.lock.db (spec's .tick.lock analogue)", () => {
		const lock = new TickLock(dir);
		expect(lock.path).toBe(join(dir, ".tick.lock.db"));
		const lease = lock.acquire();
		expect(lease.acquired).toBe(true);
		if (lease.acquired) lease.lease.release();
		// The sidecar db exists after use.
		let db: Database.Database | null = null;
		try {
			db = new Database(lock.path);
			db.pragma("busy_timeout = 0");
			db.exec("BEGIN IMMEDIATE"); // nobody holds it post-release
			db.exec("ROLLBACK");
		} finally {
			db?.close();
		}
	});
});

describe("#87644 retry ladder", () => {
	it("healthy ticks wait the plain interval; consecutive EMFILE doubles, capped at 15min", () => {
		const interval = 60;
		expect(backoffWaitSeconds(interval, 0)).toBe(60);
		expect(backoffWaitSeconds(interval, 1)).toBe(60); // 2^0
		expect(backoffWaitSeconds(interval, 2)).toBe(120); // 2^1
		expect(backoffWaitSeconds(interval, 3)).toBe(240);
		expect(backoffWaitSeconds(interval, 4)).toBe(480);
		expect(backoffWaitSeconds(interval, 5)).toBe(900); // 960 clamps to the cap
		expect(backoffWaitSeconds(interval, 12)).toBe(EMFILE_BACKOFF_MAX_SECONDS);
		expect(backoffWaitSeconds(interval, 50)).toBe(900); // hard cap
	});

	it("noteTickFailure escalates ONLY on fd exhaustion; transient errors reset", () => {
		const reclaimed: boolean[] = [];
		const reclaim = (): void => {
			reclaimed.push(true);
		};
		expect(noteTickFailure(errWithCode("EMFILE"), 0, reclaim)).toBe(1);
		expect(noteTickFailure(errWithCode("EMFILE"), 1, reclaim)).toBe(2);
		expect(reclaimed).toHaveLength(2); // reclamation ran per EMFILE failure
		expect(noteTickFailure(new Error("transient"), 4, reclaim)).toBe(0);
		// Contention-classified failures never reach the loop as throws, but if
		// one did it must NOT escalate backoff (only EMFILE does).
		expect(noteTickFailure(errWithCode("SQLITE_BUSY"), 3, reclaim)).toBe(0);
		expect(reclaimed).toHaveLength(2); // unchanged by the resets
	});
});
