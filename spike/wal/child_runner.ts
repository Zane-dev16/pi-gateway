// Spike proof: scripted two-process contention driver (02 §1.1 / §12 row
// "Write-lock contention"). THROWAWAY spike — never ships.
//
// Runs under plain `node child_runner.ts` (Node >=23 type stripping; verified on
// v26.7.0). The shared ladder/write semantics are imported AT RUNTIME from the
// single source of truth via $PIG_WAL_CORE (a dynamic specifier keeps this file
// free of .ts-extension static imports, which tsc would reject under NodeNext).
//
// Coordination protocol: marker files in a temp dir written atomically enough for
// tests via writeFileSync rename-free create; waiters poll existsSync on a 10ms
// cadence with hard deadlines (event-based sync — never sleeps-as-sync).
// Final state goes to stdout as exactly one line: "RESULT_JSON {...}".

import { existsSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

type Core = typeof import("./core.js");

interface Args {
	get(key: string): string | undefined;
}

function parseArgs(argv: string[]): Args {
	const map = new Map<string, string>();
	for (let i = 0; i < argv.length; i += 2) {
		const key = argv[i];
		const val = argv[i + 1];
		if (key?.startsWith("--") && val !== undefined) {
			map.set(key.slice(2), val);
		}
	}
	return {
		get: (key: string) => map.get(key),
	};
}

function requireArg(args: Args, key: string): string {
	const v = args.get(key);
	if (v === undefined) throw new Error(`missing --${key}`);
	return v;
}

function numArg(args: Args, key: string, fallback: number): number {
	const raw = args.get(key);
	if (raw === undefined) return fallback;
	return Number(raw);
}

function writeMarker(
	dir: string,
	name: string,
	payload: Record<string, unknown> = {},
): void {
	writeFileSync(
		join(dir, name),
		JSON.stringify({ t: Date.now(), ...payload }),
		"utf8",
	);
}

async function waitMarker(
	dir: string,
	name: string,
	timeoutMs: number,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	const path = join(dir, name);
	while (!existsSync(path)) {
		if (Date.now() > deadline) {
			throw new Error(`timeout waiting for marker ${name}`);
		}
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

function emit(result: Record<string, unknown>): void {
	console.log(`RESULT_JSON ${JSON.stringify(result)}`);
}

/**
 * Scenario: hold-write — open through the ladder, take BEGIN IMMEDIATE, insert
 * rows, signal holding, block until released, COMMIT, then land a second batch
 * through the contended-write ladder (executeWrite).
 */
async function scenarioHoldWrite(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const tag = requireArg(args, "tag");
	const payload = requireArg(args, "payload");
	const holdRows = numArg(args, "hold-rows", 3);
	const postRows = numArg(args, "post-rows", 3);
	const holdMarker = requireArg(args, "hold-marker");
	const releaseMarker = requireArg(args, "release-marker");
	const doneMarker = requireArg(args, "done-marker");

	const { db } = await core.openDatabase({
		path: dbPath,
		busyTimeoutMs: numArg(args, "busy-timeout-ms", 5000),
	});
	try {
		db.exec(core.LEDGER_DDL);
		db.exec("BEGIN IMMEDIATE");
		const insert = db.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)",
		);
		const heldIds: number[] = [];
		for (let seq = 1; seq <= holdRows; seq++) {
			heldIds.push(Number(insert.run(tag, seq, payload).lastInsertRowid));
		}
		writeMarker(coord, holdMarker);
		await waitMarker(coord, releaseMarker, 25_000);
		db.exec("COMMIT");
		writeMarker(coord, doneMarker);

		let retries = 0;
		const postIds = await core.executeWrite(
			db,
			(conn) => {
				const ins = conn.prepare(
					"INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)",
				);
				const postTag = `${tag}-post`;
				const postPayload = `${payload}-post`;
				const ids: number[] = [];
				for (let seq = 101; seq < 101 + postRows; seq++) {
					ids.push(Number(ins.run(postTag, seq, postPayload).lastInsertRowid));
				}
				return ids;
			},
			{
				patienceMs: 15_000,
				onRetry: () => {
					retries++;
				},
			},
		);
		emit({ role: tag, heldIds, postIds, retries });
	} finally {
		db.close();
	}
}

/**
 * Scenario: contend-write — start ONLY once the holder signals an open write
 * transaction; every BEGIN IMMEDIATE collides until the holder commits, so the
 * app-level retry ladder engages (short SQLite busy_timeout + jittered patience,
 * mirroring hermes_state.py:SessionDB._execute_write).
 */
async function scenarioContendWrite(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const tag = requireArg(args, "tag");
	const payload = requireArg(args, "payload");
	const rows = numArg(args, "rows", 5);
	const holdMarker = requireArg(args, "hold-marker");
	const firstBusyMarker = requireArg(args, "first-busy-marker");

	await waitMarker(coord, holdMarker, 25_000);
	const { db } = await core.openDatabase({
		path: dbPath,
		busyTimeoutMs: numArg(args, "busy-timeout-ms", 300),
	});
	try {
		let retries = 0;
		let firstBusySignaled = false;
		const ids = await core.executeWrite(
			db,
			(conn) => {
				const ins = conn.prepare(
					"INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)",
				);
				const out: number[] = [];
				for (let seq = 1; seq <= rows; seq++) {
					out.push(Number(ins.run(tag, seq, payload).lastInsertRowid));
				}
				return out;
			},
			{
				patienceMs: 15_000,
				onRetry: () => {
					retries++;
					if (!firstBusySignaled) {
						firstBusySignaled = true;
						writeMarker(coord, firstBusyMarker);
					}
				},
			},
		);
		emit({ role: tag, ids, retries });
	} finally {
		db.close();
	}
}

/**
 * Scenario: writer-hold — insert UNCOMMITTED rows inside an open write
 * transaction; commit only on external release (for the reader-under-writer proof).
 */
async function scenarioWriterHold(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const rows = numArg(args, "rows", 50);
	const holdMarker = requireArg(args, "hold-marker");
	const releaseMarker = requireArg(args, "release-marker");

	const { db } = await core.openDatabase({ path: dbPath });
	try {
		db.exec("BEGIN IMMEDIATE");
		const insert = db.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES ('w', ?, 'uncommitted')",
		);
		for (let seq = 1; seq <= rows; seq++) insert.run(seq);
		writeMarker(coord, holdMarker);
		await waitMarker(coord, releaseMarker, 25_000);
		db.exec("COMMIT");
		writeMarker(coord, "committed-w");
		emit({ role: "w", inserted: rows });
	} finally {
		db.close();
	}
}

/**
 * Scenario: snapshot-reader — poll COUNT(*) on its own connection across the
 * writer's open transaction. WAL snapshot isolation: observations may only ever
 * be the pre-commit baseline or the full post-commit total, never partial.
 */
async function scenarioSnapshotReader(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const baselineMarker = requireArg(args, "baseline-marker");
	const committedMarker = requireArg(args, "committed-marker");

	const { db } = await core.openDatabase({ path: dbPath });
	try {
		const countStmt = db.prepare("SELECT COUNT(*) AS n FROM ledger");
		const observed = new Set<number>();
		let firstRead = true;
		const deadline = Date.now() + 25_000;
		while (true) {
			const n = Number((countStmt.get() as { n: number }).n);
			observed.add(n);
			if (firstRead) {
				firstRead = false;
				writeMarker(coord, baselineMarker);
			}
			if (existsSync(join(coord, committedMarker))) break;
			if (Date.now() > deadline)
				throw new Error("reader timeout before commit");
			await new Promise<void>((r) => setTimeout(r, 12));
		}
		for (let i = 0; i < 3; i++) {
			observed.add(Number((countStmt.get() as { n: number }).n));
			await new Promise<void>((r) => setTimeout(r, 12));
		}
		emit({ role: "reader", observed: [...observed].sort((a, b) => a - b) });
	} finally {
		db.close();
	}
}

/**
 * Scenario: hold-timed — acquire the write lock FIRST, signal readiness, then
 * hold it for a fixed wall-clock span (busy_timeout-honored proof counterpart).
 */
async function scenarioHoldTimed(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const holdMs = numArg(args, "hold-ms", 2600);
	const readyMarker = requireArg(args, "ready-marker");

	const { db } = await core.openDatabase({ path: dbPath });
	try {
		db.exec("BEGIN IMMEDIATE"); // acquire BEFORE signaling — waiter must lose the race
		db.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES ('H', 1, 'holder')",
		).run();
		writeMarker(coord, readyMarker);
		await new Promise<void>((r) => setTimeout(r, holdMs));
		db.exec("COMMIT");
		writeMarker(coord, "done-h");
		emit({ role: "H" });
	} finally {
		db.close();
	}
}

/**
 * Scenario: blocking-begin — a SINGLE bare BEGIN IMMEDIATE that must block inside
 * SQLite's busy handler (busy_timeout) until the holder commits — never fail-fast.
 */
async function scenarioBlockingBegin(core: Core, args: Args): Promise<void> {
	const dbPath = requireArg(args, "db");
	const coord = requireArg(args, "coord");
	const readyMarker = requireArg(args, "ready-marker");
	const busyTimeoutMs = numArg(args, "busy-timeout-ms", 9000);

	await waitMarker(coord, readyMarker, 25_000);
	const { db } = await core.openDatabase({
		path: dbPath,
		busyTimeoutMs,
	});
	try {
		const t0 = Date.now();
		db.exec("BEGIN IMMEDIATE"); // one attempt; SQLite busy handler does the waiting
		const elapsedMs = Date.now() - t0;
		db.prepare(
			"INSERT INTO ledger (writer, seq, payload) VALUES ('B', 1, 'waiter')",
		).run();
		db.exec("COMMIT");
		emit({ role: "B", elapsedMs });
	} finally {
		db.close();
	}
}

async function main(): Promise<void> {
	const corePath = process.env.PIG_WAL_CORE;
	if (!corePath) throw new Error("PIG_WAL_CORE env var required");
	const core = (await import(pathToFileURL(resolve(corePath)).href)) as Core;
	const args = parseArgs(process.argv.slice(2));
	const scenario = requireArg(args, "scenario");
	switch (scenario) {
		case "hold-write":
			return scenarioHoldWrite(core, args);
		case "contend-write":
			return scenarioContendWrite(core, args);
		case "writer-hold":
			return scenarioWriterHold(core, args);
		case "snapshot-reader":
			return scenarioSnapshotReader(core, args);
		case "hold-timed":
			return scenarioHoldTimed(core, args);
		case "blocking-begin":
			return scenarioBlockingBegin(core, args);
		default:
			throw new Error(`unknown scenario ${scenario}`);
	}
}

const invokedDirectly =
	process.argv[1] !== undefined &&
	import.meta.url === pathToFileURL(resolve(process.argv[1])).href;

if (invokedDirectly) {
	main().catch((err: unknown) => {
		console.error(err instanceof Error ? err.stack : String(err));
		process.exitCode = 3;
	});
}
