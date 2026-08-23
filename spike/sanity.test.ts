// Phase-0 sanity: proves the ratified SQLite driver (better-sqlite3) works on this
// host/runtime. Doubles as driver evidence for DEC-023's verification entry.
// Shape ports forward into the Phase 1–2 suites (02 §1.1 WAL ladder row).
import { mkdtempSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

const EMOJI = "hello 🚀🎉 — 👨‍👩‍👧‍👦 🇯🇵 café 中文 \u{10FFFF}";

describe("sqlite driver sanity (better-sqlite3)", () => {
	let dir: string;
	let dbPath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "pi-gw-sanity-"));
		dbPath = join(dir, "state.db");
	});

	afterEach(() => {
		// best-effort cleanup; temp dirs under os.tmpdir() are disposable either way
		try {
			rmRf(dir);
		} catch {
			/* ignore */
		}
	});

	it("opens with WAL mode active and busy_timeout honored", () => {
		const db = new Database(dbPath);
		try {
			expect(db.open).toBe(true);
			const [wal] = db.pragma("journal_mode = WAL") as Array<{
				journal_mode: string;
			}>;
			expect(wal?.journal_mode.toLowerCase()).toBe("wal");
			expect(db.pragma("busy_timeout = 5000", { simple: true })).toBe(5000);
			// -wal sidecar materializes on first write; its existence proves real WAL mode,
			// not just a reported pragma value
			db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)");
			db.prepare("INSERT INTO t (id) VALUES (?)").run(1);
			expect(existsSync(`${dbPath}-wal`)).toBe(true);
		} finally {
			db.close();
		}
	});

	it("supports BEGIN IMMEDIATE/COMMIT transaction control", () => {
		const db = new Database(dbPath);
		try {
			db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT NOT NULL)");
			db.exec("BEGIN IMMEDIATE");
			db.prepare("INSERT INTO t (txt) VALUES (?)").run(EMOJI);
			db.exec("COMMIT");
			expect(db.prepare("SELECT COUNT(*) AS n FROM t").get()).toEqual({ n: 1 });

			// rollback path is available too (immediate-mode write lock taken at BEGIN)
			db.exec("BEGIN IMMEDIATE");
			db.prepare("INSERT INTO t (txt) VALUES (?)").run("rolled back");
			db.exec("ROLLBACK");
			expect(db.prepare("SELECT COUNT(*) AS n FROM t").get()).toEqual({ n: 1 });
		} finally {
			db.close();
		}
	});

	it("round-trips multi-byte emoji byte-exactly through TEXT and BLOB columns", () => {
		const db = new Database(dbPath);
		try {
			db.exec(
				"CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT NOT NULL, blob BLOB NOT NULL)",
			);
			const bytes = Buffer.from(EMOJI, "utf8");
			db.exec("BEGIN IMMEDIATE");
			const info = db
				.prepare("INSERT INTO t (txt, blob) VALUES (?, ?)")
				.run(EMOJI, bytes);
			db.exec("COMMIT");

			const row = db
				.prepare("SELECT txt, blob FROM t WHERE id = ?")
				.get(info.lastInsertRowid) as { txt: string; blob: Buffer };
			expect(row.txt).toBe(EMOJI); // text identity
			expect(Buffer.from(row.txt, "utf8").equals(bytes)).toBe(true); // TEXT column byte-exact
			expect(Buffer.from(row.blob).equals(bytes)).toBe(true); // BLOB column byte-exact
		} finally {
			db.close();
		}
	});

	it("survives two concurrent connections writing to one DB (busy_timeout honored)", () => {
		const dbA = new Database(dbPath);
		const dbB = new Database(dbPath);
		try {
			for (const d of [dbA, dbB]) d.pragma("busy_timeout = 5000");
			dbA.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT NOT NULL)");
			dbB.prepare("INSERT INTO t (txt) VALUES (?)").run("from B ✅");
			dbA.prepare("INSERT INTO t (txt) VALUES (?)").run("from A 🚀");
			expect(dbA.prepare("SELECT COUNT(*) AS n FROM t").get()).toEqual({
				n: 2,
			});
		} finally {
			dbB.close();
			dbA.close();
		}
	});
});

function rmRf(path: string): void {
	rmSync(path, { recursive: true, force: true });
}
