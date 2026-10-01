// session-lock.test.ts — contention contracts for the foreign-session
// driver lock (DEC-079): two drivers, one winner, no interleave, against the
// real lock file. Cross-process proof runs a real second OS process holding
// the SQLite write txn via an inline driver (no new files) — the parent
// loses while it holds and wins after it dies.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	isSessionDriveLocked,
	SessionDriverLock,
	sessionLockPath,
} from "./session-lock.js";

let lockDir: string;

beforeEach(() => {
	lockDir = mkdtempSync(join(tmpdir(), "pi-gw-session-lock-"));
});

afterEach(() => {
	rmSync(lockDir, { recursive: true, force: true });
});

describe("SessionDriverLock — one winner, no interleave", () => {
	it("two acquirers on one session: first wins, second loses, release frees", () => {
		const winner = new SessionDriverLock(lockDir, "sess-1");
		const loser = new SessionDriverLock(lockDir, "sess-1");
		expect(winner.acquire()).toBe(true);
		expect(winner.isHeld()).toBe(true);
		expect(loser.acquire()).toBe(false);
		expect(loser.isHeld()).toBe(false);
		// Re-entry by the holder is not a second claim.
		expect(winner.acquire()).toBe(true);
		winner.release();
		expect(winner.isHeld()).toBe(false);
		expect(loser.acquire()).toBe(true);
		loser.release();
	});

	it("different sessions do not contend", () => {
		const a = new SessionDriverLock(lockDir, "sess-a");
		const b = new SessionDriverLock(lockDir, "sess-b");
		expect(a.acquire()).toBe(true);
		expect(b.acquire()).toBe(true);
		a.release();
		b.release();
	});

	it("the lock file lands next to gateway.lock.db with a safe name", () => {
		const lock = new SessionDriverLock(lockDir, "sess:1/x");
		expect(lock.record.path).toBe(
			join(lockDir, "session-drive-sess_1_x.lock.db"),
		);
		expect(sessionLockPath(lockDir, "sess:1/x")).toBe(lock.record.path);
	});

	it("the local record names the claim while held and resets on release", () => {
		const lock = new SessionDriverLock(lockDir, "sess-1");
		expect(lock.record).toMatchObject({
			sessionId: "sess-1",
			holderPid: process.pid,
			acquiredAtMs: 0,
		});
		expect(lock.acquire()).toBe(true);
		expect(lock.record.acquiredAtMs).toBeGreaterThan(0);
		expect(isSessionDriveLocked(lockDir, "sess-1")).toBe(true);
		lock.release();
		expect(lock.record.acquiredAtMs).toBe(0);
		expect(isSessionDriveLocked(lockDir, "sess-1")).toBe(false);
	});

	it("a live second process wins; its death frees the claim", async () => {
		const sessionId = "foreign-live";
		const path = sessionLockPath(lockDir, sessionId);
		const child = holdInChild(path);
		try {
			await waitForMarker(child, "HOLDING");
			// Contended while the child holds the write txn: exactly one winner.
			const contender = new SessionDriverLock(lockDir, sessionId);
			expect(contender.acquire()).toBe(false);
			expect(isSessionDriveLocked(lockDir, sessionId)).toBe(true);
		} finally {
			child.kill("SIGKILL");
			await waitExit(child);
		}
		// Death closes the holder fd and rolls the txn back: the claim is free.
		expect(new SessionDriverLock(lockDir, sessionId).acquire()).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// Inline cross-process holder (better-sqlite3 write txn, no gateway imports)
// ---------------------------------------------------------------------------

let childOutput = "";

const BETTER_SQLITE3_PATH = createRequire(import.meta.url).resolve(
	"better-sqlite3",
);

function holdInChild(lockPath: string): ChildProcess {
	childOutput = "";
	const driver = [
		`const Database = require(${JSON.stringify(BETTER_SQLITE3_PATH)});`,
		`const db = new Database(${JSON.stringify(lockPath)});`,
		"db.pragma('journal_mode = WAL');",
		"db.pragma('busy_timeout = 0');",
		"db.exec('BEGIN IMMEDIATE');",
		"db.exec('CREATE TABLE IF NOT EXISTS session_drive_lock (holder_pid INTEGER NOT NULL, acquired_at REAL NOT NULL)');",
		`db.prepare('INSERT INTO session_drive_lock (holder_pid, acquired_at) VALUES (?, ?)').run(process.pid, Date.now() / 1000);`,
		"console.log('HOLDING');",
		"setInterval(() => {}, 1000);",
	].join("\n");
	const child = spawn(process.execPath, ["-e", driver], {
		stdio: ["ignore", "pipe", "pipe"],
	});
	child.stdout?.on("data", (chunk: Buffer) => {
		childOutput += String(chunk);
	});
	return child;
}

function waitForMarker(child: ChildProcess, marker: string): Promise<void> {
	const deadline = Date.now() + 15_000;
	return new Promise((resolve, reject) => {
		const poll = (): void => {
			if (childOutput.includes(marker)) {
				resolve();
				return;
			}
			if (child.exitCode !== null || child.signalCode !== null) {
				reject(new Error("holder child exited before holding"));
				return;
			}
			if (Date.now() > deadline) {
				reject(new Error("holder child never held the lock"));
				return;
			}
			setTimeout(poll, 20);
		};
		poll();
	});
}

function waitExit(child: ChildProcess): Promise<void> {
	return new Promise((resolve) => {
		if (child.exitCode !== null || child.signalCode !== null) resolve();
		else child.once("exit", () => resolve());
	});
}
