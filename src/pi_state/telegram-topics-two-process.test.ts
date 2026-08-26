// Two OS processes — the Telegram topic-mode schema migration under writer
// contention (fix cluster "telegram-topic-bindings"). The EXPLICIT migration
// is DDL, so a naive port could half-create tables or fail forever on a held
// write lock. Contracts:
//
//   A. migration waits out a HELD BEGIN IMMEDIATE via the contended-write
//      ladder and lands the full converged schema (no partial DDL);
//   B. two racing migrators on a feature-absent store both succeed; exactly
//      one copy of each table/index exists and the version stamp converges.
//
// Shape parity of wal.test.ts / lease.test.ts (spawned children + marker
// files + RESULT_JSON). Runs in the heavy-process vitest project (serialized).

import { existsSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import Database from "better-sqlite3";

import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-tgtopics-2p-");
});

afterEach(() => {
	removeTempDir(dir);
});

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

function signal(name: string): Promise<void> {
	return writeFile(join(dir, name), JSON.stringify({ t: Date.now() }));
}

const EXPECTED_TABLES = [
	"telegram_dm_topic_bindings",
	"telegram_dm_topic_mode",
];
const EXPECTED_INDEXES = [
	"idx_telegram_dm_topic_bindings_session",
	"idx_telegram_dm_topic_bindings_user",
];

describe("two OS processes — telegram topic schema migration contention", () => {
	it("A: migration waits out a HELD writer via the ladder, then lands the converged schema", async () => {
		const p = join(dir, "state.db");
		const holder = runChild({
			scenario: "topic-mig-hold",
			db: p,
			coord: dir,
			"hold-marker": "mig-held",
			"release-marker": "mig-release",
		});

		await waitForMarker("mig-held"); // writer owns BEGIN IMMEDIATE now

		// Contender opens with a SHORT busy_timeout so SQLITE_BUSY surfaces to
		// the app-level ladder — proving retry, not silent success.
		const migrator = runChild({
			scenario: "topic-mig-run",
			db: p,
			coord: dir,
			"busy-timeout-ms": 250,
			"patience-ms": 30_000,
		});

		// Give the contender time to hit the wall while the lock is still held…
		await new Promise<void>((r) => setTimeout(r, 600));
		await signal("mig-release"); // …then let the holder commit

		const holdRes = await holder;
		expect(holdRes.code).toBe(0);
		const migRes = await migrator;
		expect(migRes.code).toBe(0);
		const result = parseResult(migRes);
		expect(result["version"]).toBe("2");
		expect(result["tables"]).toEqual(EXPECTED_TABLES);
		expect(result["indexes"]).toEqual(EXPECTED_INDEXES);
		expect(result["sessionsFkOnDelete"]).toBe("CASCADE");
		expect(result["integrity"]).toBe("ok");

		// Parent-side convergence probe on the same file.
		const raw = new Database(p, { readonly: true });
		try {
			const stamps = raw
				.prepare(
					"SELECT COUNT(*) AS n FROM state_meta WHERE key = 'telegram_dm_topic_schema_version' AND value = '2'",
				)
				.get() as { n: number };
			expect(stamps.n).toBe(1);
		} finally {
			raw.close();
		}
	}, 40_000);

	it("B: two racing migrators converge — both succeed, exactly one schema, version stamped once", async () => {
		const p = join(dir, "state.db");

		// Production shape: the migration only ever runs on an OPENED state.db
		// (base schema present, topic side tables absent). Seed that baseline
		// exactly as a writable open would.
		const { openDatabase } = await import("./wal.js");
		const { initStore } = await import("./reconcile.js");
		const seeded = await openDatabase({ path: p });
		initStore(seeded.db);
		seeded.db.close();

		// Neither child coordinates with the other: pure open→migrate race.
		const [a, b] = await Promise.all([
			runChild({
				scenario: "topic-mig-run",
				db: p,
				coord: dir,
				"busy-timeout-ms": 250,
				"patience-ms": 30_000,
			}),
			runChild({
				scenario: "topic-mig-run",
				db: p,
				coord: dir,
				"busy-timeout-ms": 250,
				"patience-ms": 30_000,
			}),
		]);

		expect(a.code).toBe(0);
		expect(b.code).toBe(0);
		for (const run of [a, b]) {
			const result = parseResult(run);
			expect(result["version"]).toBe("2");
			expect(result["tables"]).toEqual(EXPECTED_TABLES);
			expect(result["indexes"]).toEqual(EXPECTED_INDEXES);
			expect(result["integrity"]).toBe("ok");
		}

		// Exactly ONE copy of every object survived the race.
		const raw = new Database(p, { readonly: true });
		try {
			const tables = raw
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'telegram_dm%' ORDER BY name",
				)
				.all();
			expect(tables).toHaveLength(EXPECTED_TABLES.length);
			const indexes = raw
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_telegram_dm%' ORDER BY name",
				)
				.all();
			expect(indexes).toHaveLength(EXPECTED_INDEXES.length);
			const stamps = raw
				.prepare(
					"SELECT COUNT(*) AS n FROM state_meta WHERE key = 'telegram_dm_topic_schema_version'",
				)
				.get() as { n: number };
			expect(stamps.n).toBe(1);
			const fk = raw
				.prepare("PRAGMA foreign_key_list('telegram_dm_topic_bindings')")
				.all() as Array<Record<string, unknown>>;
			const sessionsFk = fk.find((r) => String(r["table"]) === "sessions");
			expect(String(sessionsFk!["on_delete"])).toBe("CASCADE");
		} finally {
			raw.close();
		}
	}, 40_000);
});
