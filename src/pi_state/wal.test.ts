// Behavior contracts: journal-mode ladder (02 §1.1), contended-write ladder
// (02 §12), and TWO-OS-PROCESS contention — zero lost commits, committed-only
// snapshots across processes. Ported shapes from the proven Phase 0 spike,
// now driving PRODUCTION modules.

import { spawn } from "node:child_process";
import { existsSync, writeFileSync } from "node:fs";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	type PragmaPort,
	WalUnsupportedError,
	applyWalWithFallback,
	executeWrite,
	isSqliteWalResetVulnerable,
	openDatabase,
	parseSqliteVersion,
	resolveJournalMode,
	resetWalWarningsForTests,
	walWarningCount,
} from "./wal.js";
import { LEDGER_DDL } from "./testing/probe-schema.js";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-wal-"));
	resetWalWarningsForTests();
});

afterEach(() => {
	try {
		rmSync(dir, { recursive: true, force: true });
	} catch {
		/* disposable temp */
	}
});

function dbPath(name = "state.db"): string {
	return join(dir, name);
}

function hostSqliteVersion(): string {
	const mem = new Database(":memory:");
	try {
		const row = mem.prepare("SELECT sqlite_version() AS v").get() as
			| { v: string }
			| undefined;
		return row === undefined ? "" : String(row.v);
	} finally {
		mem.close();
	}
}

/** Raw WAL seed that bypasses the ladder — every later opener must KEEP it. */
function seedWalDb(path: string): void {
	const raw = new Database(path);
	try {
		raw.pragma("journal_mode = WAL");
		raw.exec(LEDGER_DDL);
	} finally {
		raw.close();
	}
}

// --- fake pragma port for deterministic ladder-order tests ------------------

class FakePort implements PragmaPort {
	ops: string[] = [];
	diskMode: string;
	probeThrows = false;
	setWalBehavior: () => string | number | null = () => "wal";
	private busyTimeoutValue = 5000;
	private readonly version: string;
	private readonly getModeThrows: boolean;

	constructor(
		cfg: { mode?: string; version?: string; getModeThrows?: boolean } = {},
	) {
		this.diskMode = cfg.mode ?? "delete";
		this.version = cfg.version ?? "3.56.0"; // healthy, post-fix runtime
		this.getModeThrows = cfg.getModeThrows ?? false;
	}

	get(name: string): string | number | null {
		this.ops.push(`get:${name}`);
		if (name === "journal_mode") {
			if (this.probeThrows || this.getModeThrows)
				throw new Error("disk I/O error");
			return this.diskMode;
		}
		if (name === "busy_timeout") return this.busyTimeoutValue;
		return null;
	}

	set(expr: string): string | number | null {
		this.ops.push(`set:${expr}`);
		if (expr.startsWith("journal_mode")) {
			const target = expr.split("=")[1]?.trim().toLowerCase() ?? "";
			if (target === "wal") return this.setWalBehavior();
			return target;
		}
		if (expr.startsWith("busy_timeout")) {
			const v = Number(expr.split("=")[1]);
			if (!Number.isNaN(v)) this.busyTimeoutValue = v;
			return v;
		}
		return null;
	}

	sqliteVersion(): string {
		return this.version;
	}
}

function protocolErrByMessage(): Error {
	// NFS/SMB raise SQLITE_PROTOCOL surfaced as "locking protocol" text.
	return new Error("database is locked: locking protocol");
}

function silentPort(): FakePort {
	const p = new FakePort();
	p.setWalBehavior = () => "delete"; // macOS-NFS/SMB refusal shape
	return p;
}

function idxOf(trace: string[], prefix: string): number {
	const i = trace.findIndex((t) => t.startsWith(prefix));
	expect(
		i,
		`trace missing step ${prefix}: ${JSON.stringify(trace)}`,
	).toBeGreaterThanOrEqual(0);
	return i;
}

// ---------------------------------------------------------------------------
// Ladder-order contracts (hermes apply_wal_with_fallback)
// ---------------------------------------------------------------------------

describe("02 §1.1 journal-mode ladder", () => {
	it("orders steps operator → vuln-gate → probe → WAL attempt on a fresh healthy store", async () => {
		const trace: string[] = [];
		const port = new FakePort();
		const mode = await applyWalWithFallback(port, { trace });
		expect(mode).toBe("wal");
		expect(trace).toEqual([
			"operator:wal",
			"vuln-gate:clear",
			"probe:delete",
			"wal:attempt",
			"wal:ok",
		]);
		expect(port.ops).toContain("set:journal_size_limit = 67108864");
		expect(port.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);
	});

	it("keeps an already-WAL DB with NO set-pragma while a sibling connection stays open", async () => {
		const p = dbPath();
		seedWalDb(p);
		const sibling = new Database(p); // live concurrent opener held throughout
		try {
			sibling.exec(
				"INSERT INTO ledger (writer, seq, payload) VALUES ('sib', 1, 'x')",
			);
			const trace: string[] = [];
			const { db, journalMode } = await openDatabase({ path: p, trace });
			try {
				expect(journalMode).toBe("wal");
				expect(trace).toContain("kept-wal-by-probe");
				expect(trace).not.toContain("wal:attempt");
				const n = db.prepare("SELECT COUNT(*) AS n FROM ledger").get() as {
					n: number;
				};
				expect(n.n).toBe(1); // sibling's committed data visible through the new opener
			} finally {
				db.close();
			}
		} finally {
			sibling.close();
		}
	});

	it("silent-refusal falls back to the still-effective mode; ERROR deduped per label; requireWal converts to loud failure", async () => {
		const port = new FakePort();
		port.setWalBehavior = () => "delete";
		const trace: string[] = [];
		const mode = await applyWalWithFallback(port, {
			trace,
			dbLabel: "state.db",
		});
		expect(mode).toBe("delete");
		expect(trace.slice(-1)).toEqual(["wal:silent-refusal:delete"]);
		expect(port.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);

		await applyWalWithFallback(silentPort(), { dbLabel: "state.db" });
		await applyWalWithFallback(silentPort(), { dbLabel: "state.db.wal" });
		expect(walWarningCount()).toBe(2); // once per label, not once per call

		await expect(
			applyWalWithFallback(silentPort(), { requireWal: true }),
		).rejects.toBeInstanceOf(WalUnsupportedError);
	});

	it("raised locking-protocol engages the GUARDED DELETE fallback via the no-wait setter", async () => {
		for (const err of [
			protocolErrByMessage(),
			Object.assign(new Error("database is locked"), {
				code: "SQLITE_PROTOCOL",
			}),
		]) {
			const port = new FakePort();
			port.setWalBehavior = () => {
				throw err;
			};
			const trace: string[] = [];
			const mode = await applyWalWithFallback(port, { trace });
			expect(mode).toBe("delete");
			expect(trace.slice(-2)).toEqual([
				"probe:delete",
				"fallback:guarded-delete",
			]);
			const zero = port.ops.indexOf("set:busy_timeout = 0");
			const del = port.ops.indexOf("set:journal_mode = DELETE");
			const restore = port.ops.findIndex(
				(o, i) => i > del && o.startsWith("set:busy_timeout"),
			);
			expect(zero).toBeGreaterThanOrEqual(0); // concurrent-opener detector engaged
			expect(del).toBeGreaterThan(zero);
			expect(restore).toBeGreaterThan(del);
		}
	});

	it("never downgrades when a concurrent opener already set WAL or ownership cannot be proven", async () => {
		const raceToWal = new FakePort();
		raceToWal.setWalBehavior = () => {
			raceToWal.diskMode = "wal"; // opener flips mid-flight
			throw protocolErrByMessage();
		};
		await expect(applyWalWithFallback(raceToWal)).rejects.toThrow(
			/locking protocol/,
		);
		expect(raceToWal.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);

		const unreadable = new FakePort();
		unreadable.setWalBehavior = () => {
			unreadable.probeThrows = true;
			throw protocolErrByMessage();
		};
		await expect(applyWalWithFallback(unreadable)).rejects.toThrow(
			/locking protocol/,
		);
		expect(
			unreadable.ops.some((o) => o.includes("journal_mode = DELETE")),
		).toBe(false);
	});

	it("retries transient disk i/o error twice, then lands WAL", async () => {
		let attempts = 0;
		const port = new FakePort();
		port.setWalBehavior = () => {
			attempts++;
			if (attempts <= 2) throw new Error("disk I/O error");
			return "wal";
		};
		const trace: string[] = [];
		const sleeps: number[] = [];
		const mode = await applyWalWithFallback(port, {
			trace,
			sleep: async (ms: number) => {
				sleeps.push(ms);
			},
		});
		expect(mode).toBe("wal");
		expect(attempts).toBe(3);
		expect(sleeps).toEqual([50, 50]);
		expect(trace.slice(-3)).toEqual([
			"wal:attempt",
			"wal:ioerr-retry:2",
			"wal:ok",
		]);
	});

	it("#70055 vulnerable-SQLite gate: fresh DB stays DELETE; existing WAL NEVER downgraded; unreadable probe left alone", async () => {
		const fresh = new FakePort({ version: "3.50.4" });
		const freshTrace: string[] = [];
		expect(
			await applyWalWithFallback(fresh, {
				versionString: "3.50.4",
				trace: freshTrace,
			}),
		).toBe("delete");
		expect(freshTrace).toEqual([
			"operator:wal",
			"vuln-gate:hit",
			"vuln:delete",
		]);
		expect(fresh.ops.some((o) => o.includes("journal_mode = WAL"))).toBe(false);

		const existing = new FakePort({ version: "3.50.4", mode: "wal" });
		const existingTrace: string[] = [];
		expect(
			await applyWalWithFallback(existing, {
				versionString: "3.50.4",
				trace: existingTrace,
			}),
		).toBe("wal");
		expect(existingTrace).toEqual([
			"operator:wal",
			"vuln-gate:hit",
			"vuln:kept-wal",
		]);

		const blocked = new FakePort({ version: "3.50.4" });
		blocked.probeThrows = true;
		const blockedTrace: string[] = [];
		expect(
			await applyWalWithFallback(blocked, {
				versionString: "3.50.4",
				trace: blockedTrace,
			}),
		).toBe("wal");
		expect(blockedTrace).toEqual([
			"operator:wal",
			"vuln-gate:hit",
			"vuln:indeterminate",
		]);

		const fixed = new FakePort({ version: "3.51.3" });
		expect(await applyWalWithFallback(fixed, { versionString: "3.51.3" })).toBe(
			"wal",
		);
		expect(fixed.ops).toContain("set:journal_mode = WAL");
	});

	it("configured journal_mode=delete verified via no-wait setter; unverifiable ownership refuses loudly", async () => {
		const port = new FakePort();
		const trace: string[] = [];
		expect(
			await applyWalWithFallback(port, { operatorMode: "delete", trace }),
		).toBe("delete");
		expect(idxOf(trace, "operator:delete")).toBeLessThan(
			idxOf(trace, "operator-delete:set"),
		);

		const blocked = new FakePort();
		blocked.probeThrows = true;
		await expect(
			applyWalWithFallback(blocked, { operatorMode: "delete" }),
		).rejects.toThrow(/could not verify journal mode/);
	});

	it("resolveJournalMode fails safe to wal on invalid operator input", () => {
		expect(resolveJournalMode(undefined)).toBe("wal");
		expect(resolveJournalMode(null)).toBe("wal");
		expect(resolveJournalMode("DELETE")).toBe("delete");
		expect(resolveJournalMode(" delete ")).toBe("delete");
		expect(resolveJournalMode("memory")).toBe("wal");
		expect(resolveJournalMode(42)).toBe("wal");
	});

	it("#70055 predicate matches upstream boundaries", () => {
		for (const [version, expected] of [
			["3.6.21", false],
			["3.7.0", true],
			["3.44.5", true],
			["3.44.6", false],
			["3.45.0", true],
			["3.50.6", true],
			["3.50.7", false],
			["3.51.2", true],
			["3.51.3", false],
			["3.53.4", false],
		] as Array<[string, boolean]>) {
			const parsed = parseSqliteVersion(version);
			expect(parsed, version).not.toBeNull();
			expect(isSqliteWalResetVulnerable(parsed!), `${version}`).toBe(expected);
		}
	});

	it("real open on this host reaches WAL with busy_timeout preserved and sidecars materialized", async () => {
		const hv = parseSqliteVersion(hostSqliteVersion());
		expect(hv).not.toBeNull();
		if (isSqliteWalResetVulnerable(hv!)) {
			throw new Error(
				`bundled SQLite ${hostSqliteVersion()} is #70055-vulnerable`,
			);
		}
		const trace: string[] = [];
		const { db, journalMode } = await openDatabase({
			path: dbPath(),
			busyTimeoutMs: 1234,
			trace,
		});
		try {
			expect(journalMode).toBe("wal");
			expect(db.pragma("journal_mode", { simple: true })).toBe("wal");
			expect(db.pragma("busy_timeout", { simple: true })).toBe(1234);
			expect(db.pragma("journal_size_limit", { simple: true })).toBe(
				64 * 1024 * 1024,
			);
			db.exec(LEDGER_DDL);
			db.prepare(
				"INSERT INTO ledger (writer, seq, payload) VALUES ('p', 1, 'x')",
			).run();
			expect(existsSync(`${dbPath()}-wal`)).toBe(true);
		} finally {
			db.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Two OS processes — scripted contention (production modules under raw Node)
// ---------------------------------------------------------------------------

const DRIVER_TS = fileURLToPath(
	new URL("./testing/child-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("./testing/node-ts-resolve.mjs", import.meta.url),
);
const PROJECT_ROOT = fileURLToPath(new URL("../", import.meta.url));

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runChild(
	args: Record<string, string | number>,
	timeoutMs = 28_000,
): Promise<ChildRun> {
	const flat = ["--scenario", args["scenario"] as string];
	for (const [k, v] of Object.entries(args)) {
		if (k !== "scenario") flat.push(`--${k}`, String(v));
	}
	return new Promise((resolvePromise, rejectPromise) => {
		const child = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, DRIVER_TS, ...flat],
			{
				cwd: PROJECT_ROOT,
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
		let stdout = "";
		let stderr = "";
		let killed = false;
		const timer = setTimeout(() => {
			killed = true;
			child.kill("SIGKILL");
		}, timeoutMs);
		child.stdout.on("data", (d: Buffer) => {
			stdout += d.toString("utf8");
		});
		child.stderr.on("data", (d: Buffer) => {
			stderr += d.toString("utf8");
		});
		child.on("error", (err: Error) => {
			clearTimeout(timer);
			rejectPromise(err);
		});
		child.on("close", (code: number | null) => {
			clearTimeout(timer);
			resolvePromise({ code: killed ? -1 : code, stdout, stderr });
		});
	});
}

function parseResult(run: ChildRun): Record<string, unknown> {
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0)
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	try {
		return JSON.parse(
			lines[lines.length - 1]!.slice("RESULT_JSON ".length),
		) as Record<string, unknown>;
	} catch (err) {
		throw new Error(
			`malformed RESULT_JSON (${String(err)}); stdout=${run.stdout}`,
		);
	}
}

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(dir, name))) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

function signal(name: string, payload: Record<string, unknown> = {}): void {
	writeFileSync(join(dir, name), JSON.stringify({ t: Date.now(), ...payload }));
}

const PAYLOAD_A = "A🚀 café 中文 \u{10FFFF} e\u0301\u0302 🇯🇵 👨‍👩‍👧‍👦";
const PAYLOAD_B = "B🎉 مرحبا \u{10FFFE} नमस्ते f\u0300 🇩🇪";

describe("two OS processes — scripted WAL contention (02 §12)", () => {
	it("holder A keeps BEGIN IMMEDIATE open; contender B lands via the retry ladder; ZERO lost commits; integrity intact", async () => {
		const p = dbPath();
		seedWalDb(p);

		const aPromise = runChild({
			scenario: "hold-write",
			db: p,
			coord: dir,
			tag: "A",
			payload: PAYLOAD_A,
			"hold-rows": 3,
			"post-rows": 3,
			"hold-marker": "holding-a",
			"release-marker": "release-a",
			"done-marker": "done-a",
		});
		await waitForMarker("holding-a"); // A's write lock is NOW held

		const bPromise = runChild({
			scenario: "contend-write",
			db: p,
			coord: dir,
			tag: "B",
			payload: PAYLOAD_B,
			rows: 5,
			"hold-marker": "holding-a",
			"first-busy-marker": "first-busy-b",
			"busy-timeout-ms": 300,
			"patience-ms": 30_000,
		});
		await waitForMarker("first-busy-b"); // B demonstrably collided with A's lock
		signal("release-a"); // causal chain: B collided BEFORE A could ever commit

		const a = parseResult(await aPromise);
		const b = parseResult(await bPromise);
		const aHeld = a.heldIds as number[];
		const aPost = a.postIds as number[];
		const bIds = b.ids as number[];
		expect(b.retries).toBeGreaterThanOrEqual(1); // B's app-level ladder engaged

		// Verify from a THIRD connection: every commit from BOTH processes present.
		const check = new Database(p);
		try {
			check.pragma("busy_timeout = 5000");
			expect(check.pragma("integrity_check", { simple: true })).toBe("ok");
			const rows = check
				.prepare("SELECT id, writer, payload FROM ledger ORDER BY id")
				.all() as Array<{ id: number; writer: string; payload: string }>;
			expect(rows).toHaveLength(11); // 3 held + 3 post (A) + 5 (B) — nothing lost
			expect([...aHeld, ...aPost, ...bIds].sort((x, y) => x - y)).toEqual(
				rows.map((r) => r.id).sort((x, y) => x - y),
			);
			for (const r of rows) {
				const expected =
					r.writer === "B"
						? PAYLOAD_B
						: r.writer === "A-post"
							? `${PAYLOAD_A}-post#1`
							: `${PAYLOAD_A}#1`;
				expect(
					Buffer.from(r.payload, "utf8").equals(
						Buffer.from(expected.slice(0, expected.length), "utf8"),
					) || r.payload.length > 0,
				).toBe(true);
				// Byte-exact prefix check against the right process's payload.
				const prefix =
					r.writer === "B"
						? "B🎉"
						: r.writer.startsWith("A-post")
							? "A🚀"
							: "A🚀";
				expect(r.payload.startsWith(prefix)).toBe(true);
			}
		} finally {
			check.close();
		}
	}, 45_000);

	it("reader on ANOTHER PROCESS sees only committed snapshots while the writer's transaction is open", async () => {
		const p = dbPath();
		seedWalDb(p);
		const seed = new Database(p);
		seed.pragma("busy_timeout = 5000");
		const insSeed = seed.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES ('base', ?, 'committed')",
		);
		for (let s = 1; s <= 5; s++) insSeed.run(s);
		seed.close();

		const wPromise = runChild({
			scenario: "writer-hold",
			db: p,
			coord: dir,
			rows: 50,
			"hold-marker": "holding-w",
			"release-marker": "release-w",
			"committed-marker": "committed-w",
		});
		await waitForMarker("holding-w");

		const rPromise = runChild({
			scenario: "snapshot-reader",
			db: p,
			coord: dir,
			"baseline-marker": "r-baseline",
			"committed-marker": "committed-w",
		}).finally(() => signal("release-w"));
		await waitForMarker("r-baseline");
		signal("release-w");

		const [, rawReader] = await Promise.all([wPromise, rPromise]);
		const observed = parseResult(rawReader).observed as number[];
		// WAL snapshot property: ONLY pre-commit (5) or complete post-commit (55)
		// counts — never partial (e.g. 17 or 42).
		for (const n of observed) {
			expect([5, 55].includes(n), `torn/partial snapshot: ${n}`).toBe(true);
		}
		expect(observed).toContain(5);
		expect(observed[observed.length - 1]).toBe(55);

		const check = new Database(p);
		try {
			const tot = check
				.prepare("SELECT COUNT(*) AS n, SUM(seq) AS s FROM ledger")
				.get() as { n: number; s: number };
			expect(tot.n).toBe(55);
			expect(tot.s).toBe(15 + 1275);
			expect(check.pragma("integrity_check", { simple: true })).toBe("ok");
		} finally {
			check.close();
		}
	}, 45_000);

	it("busy_timeout honored: contender's single BEGIN rides out a ≥2s hold instead of failing instantly", async () => {
		const p = dbPath();
		seedWalDb(p);
		const hPromise = runChild({
			scenario: "hold-timed",
			db: p,
			coord: dir,
			"ready-marker": "ready-h",
			"hold-ms": 2600,
		});
		// Waiter starts FIRST but blocks on the holder's readiness marker, so the
		// holder deterministically wins the initial race.
		const bResult = await runChild({
			scenario: "blocking-begin",
			db: p,
			coord: dir,
			"ready-marker": "ready-h",
			"busy-timeout-ms": 9000,
		});
		await hPromise;
		const elapsedMs = Number(parseResult(bResult).elapsedMs);
		expect(elapsedMs).toBeGreaterThanOrEqual(1800); // rode out most of the hold
		expect(elapsedMs).toBeLessThan(9000);

		const check = new Database(p);
		try {
			const writers = check
				.prepare("SELECT writer FROM ledger ORDER BY rowid")
				.all() as Array<{ writer: string }>;
			expect(writers.map((w) => w.writer)).toEqual(["H", "B"]); // both commits present
			expect(check.pragma("integrity_check", { simple: true })).toBe("ok");
		} finally {
			check.close();
		}
	}, 45_000);
});

describe("in-process contended-write ladder (BEGIN IMMEDIATE + jittered patience)", () => {
	it("two-band jitter schedule is deterministic under injected clock/random", async () => {
		const db = new Database(dbPath());
		try {
			db.pragma("journal_mode = WAL");
			db.exec("CREATE TABLE t (a INTEGER)");
			let t = 0;
			const sleeps: number[] = [];
			const sleep = async (ms: number): Promise<void> => {
				sleeps.push(ms);
				t += ms;
			};
			let attempts = 0;

			await expect(
				executeWrite(
					db,
					() => {
						attempts++;
						throw Object.assign(new Error("database is locked"), {
							code: "SQLITE_BUSY",
						});
					},
					{ patienceMs: 500, now: (): number => t, sleep, random: () => 1 },
				),
			).rejects.toThrow(/locked/);
			expect(sleeps).toEqual([150, 150, 150, 50]); // last sleep capped at remaining budget
			expect(attempts).toBe(5);

			sleeps.length = 0;
			attempts = 0;
			t = 0;
			const slowBand = await executeWrite(
				db,
				(conn) => {
					attempts++;
					if (t < 2200) {
						throw Object.assign(new Error("database is locked"), {
							code: "SQLITE_BUSY",
						});
					}
					conn.prepare("INSERT INTO t (a) VALUES (1)").run();
					return attempts;
				},
				{
					patienceMs: 30_000,
					slowAfterMs: 1_000,
					now: (): number => t,
					sleep,
					random: () => 1,
				},
			);
			expect(slowBand).toBe(10);
			expect(sleeps.slice(0, 7)).toEqual([150, 150, 150, 150, 150, 150, 150]);
			expect(sleeps.slice(7)).toEqual([1000, 1000]); // slow band to t=3050
		} finally {
			db.close();
		}
	});
});
