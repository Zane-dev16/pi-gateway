// Spike proof contracts: SQLite WAL ladder (02 §1.1). THROWAWAY spike.
// Behavior contracts only — races, cross-process contention, byte-exact
// round-trips. Every test isolates its DB/home/state under mkdtemp temp paths.
//
// Spec: /root/pi-gateway/02-session-and-state.md §1.1 (+§7.1 replay fidelity,
// §12 error-handling row "Write-lock contention").
// Hermes anchors cited inline per block (reference repo READ-ONLY).

import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	LEDGER_DDL,
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
} from "../wal/core.js";

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

/** Host bundled-SQLite version (better-sqlite3 13.0.3 → 3.53.4, non-vulnerable). */
function hostSqliteVersion(): string {
	const mem = new Database(":memory:");
	try {
		const row = mem.prepare("SELECT sqlite_version() AS v").get() as {
			v: string;
		};
		return row.v;
	} finally {
		mem.close();
	}
}

/** Raw WAL seed that bypasses the ladder — simulates an on-disk WAL DB created
 * by any runtime; the ladder must then KEEP it regardless of host gate state. */
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

interface FakePortConfig {
	mode?: string;
	version?: string;
	getModeThrows?: boolean;
}

class FakePort implements PragmaPort {
	ops: string[] = [];
	diskMode: string;
	probeThrows = false;
	/** Behavior override for `PRAGMA journal_mode = WAL` (default: accept, return "wal"). */
	setWalBehavior: () => string | number | null = () => "wal";
	private busyTimeoutValue = 5000;
	private readonly version: string;
	private readonly getModeThrows: boolean;

	constructor(cfg: FakePortConfig = {}) {
		this.diskMode = cfg.mode ?? "delete";
		this.version = cfg.version ?? "3.56.0"; // healthy, post-fix runtime
		this.getModeThrows = cfg.getModeThrows ?? false;
	}

	get(name: string): string | number | null {
		this.ops.push(`get:${name}`);
		if (name === "journal_mode") {
			if (this.probeThrows || this.getModeThrows) {
				throw new Error("disk I/O error");
			}
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
	// NFS/SMB raise SQLITE_PROTOCOL surfaced as "locking protocol" text
	// (hermes_state.py:_WAL_INCOMPAT_MARKERS[0]).
	return new Error("database is locked: locking protocol");
}

function protocolErrByCode(): Error {
	return Object.assign(new Error("database is locked"), {
		code: "SQLITE_PROTOCOL",
	});
}

/** Port whose journal_mode=WAL pragma silently returns the still-effective
 * mode — the macOS-NFS/SMB refusal shape. */
function silentPort(): FakePort {
	const p = new FakePort();
	p.setWalBehavior = () => "delete";
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
// Ladder-order contracts (hermes_state.py:apply_wal_with_fallback)
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
		// WAL success applies the 64MiB size limit (_apply_wal_size_limit)…
		expect(port.ops).toContain("set:journal_size_limit = 67108864");
		// …and never issues a journal-mode switch to a non-WAL target.
		expect(port.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);
	});

	it("keeps an already-WAL DB with NO set-pragma while a sibling connection stays open", async () => {
		// Step 3 of the ladder: read-only header probe BEFORE any journal_mode write —
		// issuing the pragma would unlink -wal/-shm sidecars other connections hold.
		const p = dbPath();
		seedWalDb(p);
		const sibling = new Database(p); // live concurrent opener, held open throughout
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
				// committed data of the sibling opener is visible through the new opener
				const n = db.prepare("SELECT COUNT(*) AS n FROM ledger").get() as {
					n: number;
				};
				expect(n.n).toBe(1);
			} finally {
				db.close();
			}
		} finally {
			sibling.close();
		}
	});

	it("silent-refusal shape falls back to the still-effective mode; ERROR deduped per label", async () => {
		// macOS NFS/SMB/FUSE refuses WAL without raising: the pragma RETURNS the
		// still-effective mode. Trust the returned row, not the absence of a throw.
		const port = new FakePort();
		port.setWalBehavior = () => "delete";
		const trace: string[] = [];
		const mode = await applyWalWithFallback(port, {
			trace,
			dbLabel: "state.db",
		});
		expect(mode).toBe("delete");
		expect(trace.slice(-1)).toEqual(["wal:silent-refusal:delete"]);
		// No active DELETE switch is issued — the mode is ALREADY effective.
		expect(port.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);

		await applyWalWithFallback(silentPort(), { dbLabel: "state.db" });
		await applyWalWithFallback(silentPort(), {
			dbLabel: "kanban.db", // different label logs independently
		});
		expect(walWarningCount()).toBe(2); // once per label, not once per call

		await expect(
			applyWalWithFallback(silentPort(), { requireWal: true }),
		).rejects.toBeInstanceOf(WalUnsupportedError);
	});

	it("raised locking-protocol engages the GUARDED DELETE fallback via the no-wait setter", async () => {
		for (const err of [protocolErrByMessage(), protocolErrByCode()]) {
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
			// _set_journal_mode_no_wait forces busy_timeout=0 (concurrent-opener detector)
			// around the DELETE switch, then restores the previous timeout.
			const zero = port.ops.indexOf("set:busy_timeout = 0");
			const del = port.ops.indexOf("set:journal_mode = DELETE");
			const restore = port.ops.findIndex(
				(o, i) => i > del && o.startsWith("set:busy_timeout"),
			);
			expect(zero).toBeGreaterThanOrEqual(0);
			expect(del).toBeGreaterThan(zero);
			expect(restore).toBeGreaterThan(del);
		}
	});

	it("requireWal converts every fallback shape into WalUnsupportedError instead of degrading", async () => {
		const silent = silentPort();
		await expect(
			applyWalWithFallback(silent, { requireWal: true }),
		).rejects.toBeInstanceOf(WalUnsupportedError);

		const raised = new FakePort();
		raised.setWalBehavior = (): never => {
			throw protocolErrByMessage();
		};
		await expect(
			applyWalWithFallback(raised, { requireWal: true }),
		).rejects.toBeInstanceOf(WalUnsupportedError);
		// Loud failure means loud failure: no silent DELETE downgrade either.
		expect(raised.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);
	});

	it("never downgrades when a concurrent opener already set WAL or ownership cannot be proven", async () => {
		// Case A: between our failed WAL attempt and the fallback, another opener
		// flipped the DB to WAL — downgrading would destroy its uncheckpointed frames.
		const raceToWal = new FakePort();
		raceToWal.setWalBehavior = () => {
			raceToWal.diskMode = "wal"; // concurrent opener wins the flip mid-flight
			throw protocolErrByMessage();
		};
		await expect(applyWalWithFallback(raceToWal)).rejects.toThrow(
			/locking protocol/,
		);
		expect(raceToWal.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);

		// Case B: the probe is unreadable (blocked by a concurrent opener's locks) —
		// "cannot verify" must refuse the downgrade too.
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
		expect(attempts).toBe(3); // initial + 2 retries (hermes's documented cap)
		expect(sleeps).toEqual([50, 50]); // one 50ms pause per retry
		expect(trace.slice(-3)).toEqual([
			"wal:attempt",
			"wal:ioerr-retry:2",
			"wal:ok",
		]);
	});

	it("#70055 vulnerable-SQLite gate: fresh DB stays DELETE; existing WAL is NEVER downgraded; unreadable probe left alone", async () => {
		// Vulnerable runtime + fresh/non-WAL file → DELETE (no WAL enable attempt).
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
		expect(fresh.ops).toContain("set:journal_mode = DELETE");

		// Already-WAL on disk → kept, never live-downgraded under possible openers.
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
		expect(existing.ops.some((o) => o.includes("journal_mode = DELETE"))).toBe(
			false,
		);

		// Probe blocked by concurrent-opener locks → mode unknown ⇒ leave alone.
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

		// Fixed runtime (3.51.3+) proceeds onto the normal WAL path.
		const fixed = new FakePort({ version: "3.51.3" });
		expect(await applyWalWithFallback(fixed, { versionString: "3.51.3" })).toBe(
			"wal",
		);
		expect(fixed.ops).toContain("set:journal_mode = WAL");
	});

	it("configured journal_mode=delete is applied and verified; unreadable probe refuses the flip", async () => {
		const port = new FakePort();
		const trace: string[] = [];
		expect(
			await applyWalWithFallback(port, { operatorMode: "delete", trace }),
		).toBe("delete");
		expect(idxOf(trace, "operator:delete")).toBeLessThan(
			idxOf(trace, "operator-delete:set"),
		);
		expect(port.ops).toContain("set:journal_mode = DELETE");

		// Operator-requested DELETE with unverifiable ownership → refuse loudly.
		const blocked = new FakePort();
		blocked.probeThrows = true;
		await expect(
			applyWalWithFallback(blocked, { operatorMode: "delete" }),
		).rejects.toThrow(/could not verify journal mode/);
	});

	it("resolveJournalMode fails safe to wal on invalid operator input", () => {
		expect(resolveJournalMode(undefined)).toBe("wal");
		expect(resolveJournalMode(null)).toBe("wal");
		expect(resolveJournalMode("wal")).toBe("wal");
		expect(resolveJournalMode("DELETE")).toBe("delete"); // strip+lower accepted
		expect(resolveJournalMode(" delete ")).toBe("delete");
		expect(resolveJournalMode("memory")).toBe("wal"); // invalid → fail safe
		expect(resolveJournalMode(42)).toBe("wal");
		expect(resolveJournalMode({ mode: "wal" })).toBe("wal");
	});

	it("is_sqlite_wal_reset_vulnerable predicate matches upstream boundaries", () => {
		const cases: Array<[string, boolean]> = [
			["3.6.21", false], // pre-WAL: cannot hit the race
			["3.7.0", true], // first vulnerable
			["3.44.5", true],
			["3.44.6", false], // backport
			["3.45.0", true], // unrelated line carries no fix
			["3.50.6", true],
			["3.50.7", false], // backport
			["3.51.2", true], // last documented-vulnerable
			["3.51.3", false], // fix line
			["3.53.4", false], // this host
		];
		for (const [version, expected] of cases) {
			const parsed = parseSqliteVersion(version);
			expect(parsed, version).not.toBeNull();
			expect(isSqliteWalResetVulnerable(parsed!), `${version}`).toBe(expected);
		}
	});

	it("real open on this host: fresh DB reaches WAL with busy_timeout set and preserved", async () => {
		// Precondition: this host's bundled SQLite must be past the #70055 fix line,
		// otherwise the CORRECT ladder outcome below would be delete (gate hit).
		const hv = parseSqliteVersion(hostSqliteVersion());
		expect(hv).not.toBeNull();
		if (isSqliteWalResetVulnerable(hv!)) {
			throw new Error(
				`bundled SQLite ${hostSqliteVersion()} is #70055-vulnerable; update better-sqlite3 — this spike pins post-fix behavior`,
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
			expect(trace).toEqual([
				"operator:wal",
				"vuln-gate:clear",
				"probe:delete",
				"wal:attempt",
				"wal:ok",
			]);
			expect(db.pragma("journal_mode", { simple: true })).toBe("wal");
			expect(db.pragma("busy_timeout", { simple: true })).toBe(1234);
			expect(db.pragma("journal_size_limit", { simple: true })).toBe(
				64 * 1024 * 1024,
			);
			db.exec(LEDGER_DDL);
			db.prepare(
				"INSERT INTO ledger (writer, seq, payload) VALUES ('p', 1, 'x')",
			).run();
			expect(existsSync(`${dbPath()}-wal`)).toBe(true); // sidecar materialized
		} finally {
			db.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Write-ladder contracts (hermes_state.py:SessionDB._execute_write)
// ---------------------------------------------------------------------------

describe("contended-write ladder (BEGIN IMMEDIATE + jittered patience)", () => {
	it("fails TERMINALLY with the busy error once patience is exhausted", async () => {
		const a = new Database(dbPath());
		const b = new Database(dbPath());
		try {
			a.pragma("journal_mode = WAL");
			a.exec(LEDGER_DDL);
			a.pragma("busy_timeout = 100");
			b.pragma("busy_timeout = 100");
			a.exec("BEGIN IMMEDIATE"); // foreign writer holds the WAL write lock
			let retries = 0;
			const caught: unknown = await executeWrite(
				b,
				(conn) => {
					conn
						.prepare(
							"INSERT INTO ledger (writer, seq, payload) VALUES ('B', 1, 'x')",
						)
						.run();
					return true;
				},
				{ patienceMs: 400, onRetry: () => retries++ },
			).then(
				() => null,
				(e: unknown) => e,
			);
			// Terminal error IS the spec's busy class (SQLITE_BUSY / "database is locked")
			expect(caught).toBeInstanceOf(Error);
			expect((caught as { code?: string }).code).toBe("SQLITE_BUSY");
			expect((caught as Error).message).toMatch(/locked/i);
			expect(retries).toBeGreaterThanOrEqual(1); // the ladder engaged before giving up
		} finally {
			a.close();
			b.close();
		}
	});

	it("lands the write after a scripted lock release, having retried meanwhile", async () => {
		const a = new Database(dbPath());
		const b = new Database(dbPath());
		try {
			a.pragma("journal_mode = WAL");
			a.exec(LEDGER_DDL);
			a.pragma("busy_timeout = 100");
			b.pragma("busy_timeout = 100");
			a.exec("BEGIN IMMEDIATE");
			a.prepare(
				"INSERT INTO ledger (writer, seq, payload) VALUES ('A', 1, 'held')",
			).run();

			// Release A's lock ~2.6s in (event-loop timer, bounded hold ≥2s).
			const releaseTimer = setTimeout(() => a.exec("COMMIT"), 2600);
			let retries = 0;
			try {
				const landed = await executeWrite(
					b,
					(conn) => {
						conn
							.prepare(
								"INSERT INTO ledger (writer, seq, payload) VALUES ('B', 1, 'landed 🚀')",
							)
							.run();
						return true;
					},
					{ patienceMs: 15_000, onRetry: () => retries++ },
				);
				expect(landed).toBe(true);
			} finally {
				clearTimeout(releaseTimer);
			}
			expect(retries).toBeGreaterThanOrEqual(1);
			// Both writers' commits coexist — no lost commit.
			const rows = a
				.prepare("SELECT writer, payload FROM ledger ORDER BY rowid")
				.all() as Array<{ writer: string; payload: string }>;
			expect(rows.map((r) => r.writer)).toEqual(["A", "B"]);
			expect(rows[1]!.payload).toBe("landed 🚀");
		} finally {
			a.close();
			b.close();
		}
	});

	it("two-band jitter schedule is deterministic under injected clock/random", async () => {
		const db = new Database(dbPath());
		try {
			db.pragma("journal_mode = WAL");
			db.exec("CREATE TABLE t (a INTEGER)");
			let t = 0;
			const now = (): number => t;
			const sleeps: number[] = [];
			const sleep = async (ms: number): Promise<void> => {
				sleeps.push(ms);
				t += ms;
			};
			let attempts = 0;

			// Fast band (20–150ms) while young; rand()=1 pins the band maximum.
			await expect(
				executeWrite(
					db,
					() => {
						attempts++;
						throw Object.assign(new Error("database is locked"), {
							code: "SQLITE_BUSY",
						});
					},
					{
						patienceMs: 500,
						now,
						sleep,
						random: () => 1,
					},
				),
			).rejects.toThrow(/locked/);
			expect(sleeps).toEqual([150, 150, 150, 50]); // last sleep capped at remaining budget
			expect(attempts).toBe(5);

			// Past _WRITE_RETRY_SLOW_AFTER_S the schedule backs off into the slow band.
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
					now,
					sleep,
					random: () => 1,
				},
			);
			expect(slowBand).toBe(10); // attempts 1–9 fail; attempt 10 lands at t=3050
			expect(sleeps.slice(0, 7)).toEqual([150, 150, 150, 150, 150, 150, 150]); // fast band while elapsed < slowAfterMs (7×150ms ⇒ t=1050)
			expect(sleeps.slice(7)).toEqual([1000, 1000]); // slow band (250–1000ms, max pinned) to t=3050
		} finally {
			db.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Two OS-process scripted contention
// ---------------------------------------------------------------------------

// --- child-process harness ---------------------------------------------------

const CHILD_TS = fileURLToPath(
	new URL("../wal/child_runner.ts", import.meta.url),
);
const CORE_TS = fileURLToPath(new URL("../wal/core.ts", import.meta.url));
const PROJECT_ROOT = fileURLToPath(new URL("../../", import.meta.url));

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runChild(args: string[], timeoutMs = 28_000): Promise<ChildRun> {
	return new Promise((resolvePromise, rejectPromise) => {
		const child = spawn(process.execPath, [CHILD_TS, ...args], {
			cwd: PROJECT_ROOT,
			env: { ...process.env, PIG_WAL_CORE: CORE_TS },
			stdio: ["ignore", "pipe", "pipe"],
		});
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
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON from child; stderr=${run.stderr}`);
	}
	return JSON.parse(
		lines[lines.length - 1]!.slice("RESULT_JSON ".length),
	) as Record<string, unknown>;
}

async function runScenario(
	scenario: string,
	args: Record<string, string | number>,
	timeoutMs = 28_000,
): Promise<Record<string, unknown>> {
	const flat: string[] = ["--scenario", scenario];
	for (const [k, v] of Object.entries(args)) {
		flat.push(`--${k}`, String(v));
	}
	const run = await runChild(flat, timeoutMs);
	if (run.code !== 0) {
		throw new Error(
			`child ${scenario} exited ${run.code}; stderr=${run.stderr}`,
		);
	}
	return parseResult(run);
}

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	const path = join(dir, name);
	while (!existsSync(path)) {
		if (Date.now() > deadline)
			throw new Error(`timeout waiting for marker ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

function signal(name: string, payload: Record<string, unknown> = {}): void {
	writeFileSync(join(dir, name), JSON.stringify({ t: Date.now(), ...payload }));
}

const PAYLOAD_A = "A🚀 café 中文 \u{10FFFF} e\u0301\u0302 🇯🇵 👨‍👩‍👧‍👦";
const PAYLOAD_B = "B🎉 مرحبا \u{10FFFE} नमस्ते f\u0300 \u{1F1E9}\u{1F1EA}";

describe("two OS processes — scripted WAL contention", () => {
	it("holder A keeps BEGIN IMMEDIATE open; contender B lands via the retry ladder; zero lost commits; integrity intact", async () => {
		const p = dbPath();
		seedWalDb(p); // on-disk WAL: every opener takes the probe-keep ladder path

		// Writer A: opens, inserts 3 rows INSIDE an uncommitted IMMEDIATE tx, signals,
		// blocks until released, commits, then runs a post batch through executeWrite.
		const aPromise = runScenario("hold-write", {
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

		// Writer B (separate OS process): starts ONLY under A's live lock; short
		// busy_timeout makes SQLite give up fast so the APP-level ladder engages.
		const bPromise = runScenario("contend-write", {
			db: p,
			coord: dir,
			tag: "B",
			payload: PAYLOAD_B,
			rows: 5,
			"hold-marker": "holding-a",
			"first-busy-marker": "first-busy-b",
			"busy-timeout-ms": 300,
		}).finally(() => signal("release-a")); // deadlock-proof release ordering

		await waitForMarker("first-busy-b"); // B demonstrably collided with A's lock
		signal("release-a"); // causal chain: B collided BEFORE A could ever commit

		const a = await aPromise;
		const b = await bPromise;
		const aHeld = a.heldIds as number[];
		const aPost = a.postIds as number[];
		const bIds = b.ids as number[];
		expect(b.retries).toBeGreaterThanOrEqual(1); // B's ladder engaged

		// Verify from a THIRD connection: every commit from BOTH processes present.
		const check = new Database(p);
		try {
			check.pragma("busy_timeout = 5000");
			expect(check.pragma("integrity_check", { simple: true })).toBe("ok");
			const rows = check
				.prepare("SELECT id, writer, seq, payload FROM ledger ORDER BY id")
				.all() as Array<{
				id: number;
				writer: string;
				seq: number;
				payload: string;
			}>;
			expect(rows).toHaveLength(11); // 3 held + 3 post (A) + 5 (B) — nothing lost
			expect(rows.map((r) => r.id).sort((x, y) => x - y)).toEqual(
				[...aHeld, ...aPost, ...bIds].sort((x, y) => x - y),
			);
			// Byte-exact payloads survived the contended cross-process writes.
			for (const r of rows) {
				const expected =
					r.writer === "B"
						? PAYLOAD_B
						: r.writer === "A-post"
							? `${PAYLOAD_A}-post`
							: PAYLOAD_A;
				expect(
					Buffer.from(r.payload, "utf8").equals(Buffer.from(expected, "utf8")),
				).toBe(true);
			}
		} finally {
			check.close();
		}
	}, 45_000);

	it("reader on ANOTHER PROCESS sees only committed snapshots while the writer's transaction is open", async () => {
		const p = dbPath();
		seedWalDb(p);
		// Committed baseline: 5 rows.
		const seed = new Database(p);
		seed.pragma("busy_timeout = 5000");
		const insSeed = seed.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES ('base', ?, 'committed')",
		);
		for (let s = 1; s <= 5; s++) insSeed.run(s);
		seed.close();

		// Writer: 50 UNCOMMITTED inserts inside an open IMMEDIATE tx.
		const wPromise = runScenario("writer-hold", {
			db: p,
			coord: dir,
			rows: 50,
			"hold-marker": "holding-w",
			"release-marker": "release-w",
		});
		await waitForMarker("holding-w");

		// Reader: own process/connection; polls COUNT(*) across the whole hold.
		const rPromise = runScenario("snapshot-reader", {
			db: p,
			coord: dir,
			"baseline-marker": "r-baseline",
			"committed-marker": "committed-w",
		}).finally(() => signal("release-w"));

		await waitForMarker("r-baseline"); // reader completed its first (mid-hold) read
		signal("release-w"); // only now may the writer commit

		const [, r] = await Promise.all([wPromise, rPromise]);
		const observed = r.observed as number[];

		// WAL snapshot property: observations are ONLY the pre-commit snapshot (5)
		// or the complete post-commit snapshot (55) — never partial (e.g. 17, 42).
		const allowed = new Set([5, 55]);
		for (const n of observed) {
			expect(allowed.has(n), `reader saw torn/partial snapshot: ${n}`).toBe(
				true,
			);
		}
		expect(observed).toContain(5); // consistent snapshot DURING the writer's tx
		expect(observed[observed.length - 1]).toBe(55); // converged after commit

		// Post-commit totals from a fresh connection: 55 rows, sums intact.
		const check = new Database(p);
		try {
			const tot = check
				.prepare("SELECT COUNT(*) AS n, SUM(seq) AS s FROM ledger")
				.get() as { n: number; s: number };
			expect(tot.n).toBe(55);
			expect(tot.s).toBe(15 + 1275); // baseline 1..5 + writer 1..50
			expect(check.pragma("integrity_check", { simple: true })).toBe("ok");
		} finally {
			check.close();
		}
	}, 45_000);

	it("busy_timeout honored: contender's single BEGIN blocks out a ≥2s hold instead of failing instantly", async () => {
		const p = dbPath();
		seedWalDb(p);

		// Holder acquires FIRST (signals readiness from inside its tx), then holds
		// 2600ms — so the waiter deterministically loses the initial race.
		const hPromise = runScenario("hold-timed", {
			db: p,
			coord: dir,
			"ready-marker": "ready-h",
			"hold-ms": 2600,
		});

		// Waiter: ONE bare BEGIN IMMEDIATE; SQLite's busy handler (9000ms) must ride
		// out the remaining hold — no app-level retry involved.
		const bResult = await runScenario("blocking-begin", {
			db: p,
			coord: dir,
			"ready-marker": "ready-h",
			"busy-timeout-ms": 9000,
		});
		await hPromise;

		const elapsedMs = Number(bResult.elapsedMs);
		// Loose bounds: waited most of the 2.6s hold, well under its 9s budget.
		expect(elapsedMs).toBeGreaterThanOrEqual(1800);
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

// ---------------------------------------------------------------------------
// Byte-exact api_content replay fidelity (02 §7.1 sidecar invariant)
// ---------------------------------------------------------------------------

describe("byte-exact api_content round-trip (replay fidelity)", () => {
	async function seededProbeDb(): Promise<{
		path: string;
		write: (apiContent: string) => Promise<number>;
	}> {
		const p = dbPath("replay.db");
		const { db } = await openDatabase({ path: p });
		db.exec(LEDGER_DDL);
		db.close();
		const write = async (apiContent: string): Promise<number> => {
			const opened = await openDatabase({ path: p });
			try {
				return await executeWrite(opened.db, (conn) =>
					Number(
						conn
							.prepare(
								"INSERT INTO replay_probe (session_id, role, content, api_content) VALUES ('s1', 'user', NULL, ?)",
							)
							.run(apiContent).lastInsertRowid,
					),
				);
			} finally {
				opened.db.close();
			}
		};
		return { path: p, write };
	}

	function readAll(path: string): Array<{ id: number; api_content: string }> {
		const db = new Database(path);
		try {
			return db
				.prepare("SELECT id, api_content FROM replay_probe ORDER BY id")
				.all() as Array<{ id: number; api_content: string }>;
		} finally {
			db.close();
		}
	}

	function expectByteExact(written: string, stored: string): void {
		const want = Buffer.from(written, "utf8");
		const got = Buffer.from(stored, "utf8");
		expect(got.equals(want)).toBe(true);
		expect(got.byteLength).toBe(want.byteLength);
		expect(stored.length).toBe(written.length); // UTF-16 code-unit identity too
	}

	it("multi-byte emoji, combining marks, astral chars, CJK/RTL mix survive write→read BYTE-EXACTLY", async () => {
		const { path, write } = await seededProbeDb();
		const values = [
			"hello 🚀🎉 — 👨‍👩‍👧‍👦 👨‍👩‍👦 🇯🇵🇩🇪 café", // ZWJ families, flags, latin+diacritic-free
			"combining: e\u0301\u0302 a\u0328 o\u0308 f\u0300 q̇", // combining marks (NFD forms stay UNnormalized)
			"astral edge: \u{10FFFF}\u{10FFFE}\u{10000}", // max Unicode scalar values
			"scripts: 中文漢字 日本語 العربية الْعَرَبِيَّة עברית हिन्दี ไทย", // CJK + RTL + Indic + Thai
			"kitchen sink: 🚀café 中文 \u{10FFFF} e\u0301 مرحبا 👨‍👩‍👧‍👦 🇯🇵 — ✅",
		];
		for (const v of values) await write(v);

		const rows = readAll(path);
		expect(rows).toHaveLength(values.length);
		for (let i = 0; i < values.length; i++) {
			expectByteExact(values[i]!, rows[i]!.api_content); // persist-what-you-send
		}
	});

	it("lone surrogates pin the U+FFFD replacement-char normalization (never truncation)", async () => {
		// JS strings may carry unpaired surrogates; the driver's UTF-8 encoder maps
		// each to U+FFFD (WTF-8 is rejected). The pinned contract: round-trip equals
		// the replacement form EXACTLY — same code units, same byte length — so
		// replayed api_content diverges by nothing beyond the documented mapping.
		const { path, write } = await seededProbeDb();
		const lone = "\uD800abc\uDFFF";
		await write(lone);
		const rows = readAll(path);
		const expected = "\uFFFDabc\uFFFD";
		expect(rows[0]!.api_content).toBe(expected);
		expectByteExact(expected, rows[0]!.api_content);
		expect(Buffer.byteLength(expected, "utf8")).toBe(9); // EF BF BD ×2 + 'abc'
	});

	it("~200KB long mixed-script value round-trips byte-exactly across connections", async () => {
		const { path, write } = await seededProbeDb();
		const unit = "ab🚀中\u0301العربية👨‍👩‍👧‍👦\u{10FFFF}x";
		const big = unit.repeat(6000);
		const writtenBytes = Buffer.byteLength(big, "utf8");
		expect(writtenBytes).toBeGreaterThan(100_000);

		await write(big);

		// Read back through an INDEPENDENT connection (WAL cross-connection visibility).
		const reader = new Database(path);
		try {
			reader.pragma("busy_timeout = 5000");
			const row = reader
				.prepare("SELECT api_content FROM replay_probe WHERE id = 1")
				.get() as {
				api_content: string;
			};
			expectByteExact(big, row.api_content);
		} finally {
			reader.close();
		}
	});

	it("sidecar survives an uncommitted→committed lifecycle with rollback neighbor intact", async () => {
		// §7.1: crash-resilient ordering — user row written once with its final
		// sidecar; a rolled-back neighbor must leave ZERO residue behind.
		const p = dbPath("replay.db");
		const { db } = await openDatabase({ path: p });
		try {
			db.exec(LEDGER_DDL);
			db.exec("BEGIN IMMEDIATE");
			db.prepare(
				"INSERT INTO replay_probe (session_id, role, content, api_content) VALUES ('s1','user',NULL,?)",
			).run("kept 🚀 \u{10FFFF}");
			db.exec("COMMIT");
			db.exec("BEGIN IMMEDIATE");
			db.prepare(
				"INSERT INTO replay_probe (session_id, role, content, api_content) VALUES ('s1','assistant',NULL,?)",
			).run("rolled-back \uD83D\uDE00-with-lone-\uD800");
			db.exec("ROLLBACK");

			const rows = db
				.prepare("SELECT api_content FROM replay_probe ORDER BY id")
				.all() as Array<{ api_content: string }>;
			expect(rows).toHaveLength(1);
			expectByteExact("kept 🚀 \u{10FFFF}", rows[0]!.api_content);
			expect(db.pragma("integrity_check", { simple: true })).toBe("ok");
		} finally {
			db.close();
		}
	});
});
