// TEST INFRASTRUCTURE — child-process driver for two-OS-process contention
// contracts (WAL ladder, turn leases). Run under:
//   node --import <node-ts-resolve.mjs> child-driver.ts --scenario <name> ...
//
// Ported shape from the proven Phase-0 spike (spike/wal/child_runner.ts +
// spike/lease/child-driver.mjs), now driving the PRODUCTION modules.
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files in a shared dir (write = signal; poll = wait).

import { writeFileSync } from "node:fs";

interface Args {
	[k: string]: string | number;
}

function parseArgs(argv: string[]): Args {
	const out: Args = {};
	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === "--scenario") {
			out["scenario"] = argv[i + 1] ?? "";
			i++;
			continue;
		}
		if (argv[i]?.startsWith("--") === true) {
			const key = argv[i]!.slice(2);
			out[key] = argv[i + 1] ?? "";
			i++;
		}
	}
	return out;
}

const args = parseArgs(process.argv.slice(2));
const coordDir = String(args["coord"] ?? ".");

function signal(name: string, payload: Record<string, unknown> = {}): void {
	writeFileSync(
		`${coordDir}/${name}`,
		JSON.stringify({ t: Date.now(), ...payload }),
	);
}

async function waitForMarker(name: string, timeoutMs = 20_000): Promise<void> {
	const { existsSync } = await import("node:fs");
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(`${coordDir}/${name}`)) {
		if (Date.now() > deadline) {
			throw new Error(`child timeout waiting for marker ${name}`);
		}
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

async function openProbeDb(
	path: string,
	busyTimeoutMs?: number,
): Promise<{
	db: import("better-sqlite3").Database;
}> {
	const { openDatabase } = await import("../wal.js");
	const { LEDGER_DDL } = await import("./probe-schema.js");
	const openOpts: { path: string; busyTimeoutMs?: number } = { path };
	if (busyTimeoutMs !== undefined) openOpts.busyTimeoutMs = busyTimeoutMs;
	const opened = await openDatabase(openOpts);
	opened.db.exec(LEDGER_DDL);
	return { db: opened.db };
}

async function openLeaseDb(
	path: string,
	pidOverride?: number,
): Promise<{
	store: import("../leases.js").DbTurnLeaseStore;
	close: () => void;
}> {
	const { DbTurnLeaseStore } = await import("../leases.js");
	const { LEASE_PROBE_DDL } = await import("./probe-schema.js");
	const DatabaseCtor = (await import("better-sqlite3")).default;
	const db = new DatabaseCtor(path);
	db.exec(LEASE_PROBE_DDL);
	db.pragma("journal_mode = WAL");
	const store = new DbTurnLeaseStore(
		db,
		pidOverride === undefined ? {} : { pid: pidOverride },
	);
	return { store, close: () => db.close() };
}

async function holdWrite(): Promise<Record<string, unknown>> {
	const { executeWrite } = await import("../wal.js");
	const path = String(args["db"]);
	const tag = String(args["tag"]);
	const payload = String(args["payload"]);
	const holdRows = Number(args["hold-rows"] ?? 1);
	const postRows = Number(args["post-rows"] ?? 0);
	const { db } = await openProbeDb(path);
	db.exec("BEGIN IMMEDIATE");
	const heldIds: number[] = [];
	const ins = db.prepare(
		"INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)",
	);
	for (let s = 1; s <= holdRows; s++) {
		const rowPayload = payload + "#" + String(s); // bound parameter (plain string)
		heldIds.push(Number(ins.run(tag, s, rowPayload).lastInsertRowid));
	}
	signal(String(args["hold-marker"]));
	await waitForMarker(String(args["release-marker"]));
	db.exec("COMMIT");
	// Post-batch through the contended-write ladder.
	const postIds: number[] = [];
	for (let s = 1; s <= postRows; s++) {
		const postWriter = tag + "-post";
		const postPayload = payload + "-post#" + String(s);
		const id = await executeWrite(db, (conn) =>
			Number(
				conn
					.prepare("INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)")
					.run(postWriter, s, postPayload).lastInsertRowid,
			),
		);
		postIds.push(id);
	}
	signal(String(args["done-marker"]));
	db.close();
	return { heldIds, postIds };
}

async function contendWrite(): Promise<Record<string, unknown>> {
	const { executeWrite } = await import("../wal.js");
	const path = String(args["db"]);
	const tag = String(args["tag"]);
	const payload = String(args["payload"]);
	const rows = Number(args["rows"] ?? 1);
	await waitForMarker(String(args["hold-marker"]));
	const { db } = await openProbeDb(
		path,
		Number(args["busy-timeout-ms"] ?? 300),
	);
	let retries = 0;
	let firstBusySignaled = false;
	const ids: number[] = [];
	try {
		const contendPayloadPrefix = payload; // bound-parameter values below are plain strings
		for (let s = 1; s <= rows; s++) {
			const rowValue = contendPayloadPrefix + "#" + String(s);
			const id = await executeWrite(
				db,
				(conn) =>
					Number(
						conn
							.prepare(
								"INSERT INTO ledger (writer, seq, payload) VALUES (?, ?, ?)",
							)
							.run(tag, s, rowValue).lastInsertRowid,
					),
				{
					patienceMs: Number(args["patience-ms"] ?? 30_000),
					onRetry: () => {
						retries++;
						if (!firstBusySignaled) {
							firstBusySignaled = true;
							signal(String(args["first-busy-marker"] ?? "first-busy"));
						}
					},
				},
			);
			ids.push(id);
		}
	} finally {
		db.close();
	}
	if (!firstBusySignaled && args["first-busy-marker"] !== undefined) {
		signal(String(args["first-busy-marker"]));
	}
	return { ids, retries };
}

async function writerHold(): Promise<Record<string, unknown>> {
	const { db } = await openProbeDb(String(args["db"]));
	db.pragma("busy_timeout = 5000");
	db.exec("BEGIN IMMEDIATE");
	const ins = db.prepare(
		"INSERT INTO ledger (writer, seq, payload) VALUES ('W', ?, 'uncommitted')",
	);
	const rows = Number(args["rows"] ?? 10);
	for (let s = 1; s <= rows; s++) ins.run(s);
	signal(String(args["hold-marker"]));
	await waitForMarker(String(args["release-marker"]));
	db.exec("COMMIT");
	if (args["committed-marker"] !== undefined) {
		signal(String(args["committed-marker"])); // reader's stop signal
	}
	db.close();
	return { ok: true };
}

async function snapshotReader(): Promise<Record<string, unknown>> {
	const { existsSync } = await import("node:fs");
	const { db } = await openProbeDb(String(args["db"]));
	const countStmt = db.prepare("SELECT COUNT(*) AS n FROM ledger");
	const observed: number[] = [];
	let firstRead = true;
	const deadline = Date.now() + 20_000;
	try {
		for (;;) {
			observed.push(Number((countStmt.get() as { n: number }).n)); // record FIRST
			if (firstRead) {
				firstRead = false;
				signal(String(args["baseline-marker"]));
			}
			if (existsSync(`${coordDir}/${String(args["committed-marker"])}`)) break;
			if (Date.now() > deadline)
				throw new Error("reader timeout before commit");
			await new Promise<void>((r) => setTimeout(r, 12));
		}
	} finally {
		db.close();
	}
	return { observed };
}

async function holdTimed(): Promise<Record<string, unknown>> {
	const { db } = await openProbeDb(String(args["db"]), 5000);
	db.exec("BEGIN IMMEDIATE");
	signal(String(args["ready-marker"] ?? "ready"));
	const holdMs = Number(args["hold-ms"] ?? 2000);
	await new Promise<void>((r) => setTimeout(r, holdMs));
	db.prepare(
		"INSERT INTO ledger (writer, seq, payload) VALUES ('H', 1, 'held-timed')",
	).run();
	db.exec("COMMIT");
	const elapsedMs = 0; // holder measures nothing; waiter does
	db.close();
	return { elapsedMs };
}

async function blockingBegin(): Promise<Record<string, unknown>> {
	const { db } = await openProbeDb(
		String(args["db"]),
		Number(args["busy-timeout-ms"] ?? 5000),
	);
	// Wait for the holder's readiness BEFORE beginning so the waiter
	// deterministically loses the initial race.
	await waitForMarker(String(args["ready-marker"]));
	const startMs = Date.now();
	db.exec("BEGIN IMMEDIATE");
	db.prepare(
		"INSERT INTO ledger (writer, seq, payload) VALUES ('B', 1, 'blocked-begin')",
	).run();
	db.exec("COMMIT");
	const elapsedMs = Date.now() - startMs;
	db.close();
	return { elapsedMs };
}

async function leaseHolder(): Promise<Record<string, unknown>> {
	const sessionId = String(args["session-id"]);
	// Holder embeds the child's REAL pid — a genuinely live process, so peers
	// cannot dead-reclaim it and must wait out the TTL or the release.
	const holder = String(args["holder-prefix"]) + ":pid=" + String(process.pid);
	const ttl = Number(args["ttl"] ?? 300);
	const { store, close } = await openLeaseDb(String(args["db"]));
	const acquired = store.tryAcquire(sessionId, holder, ttl);
	signal(String(args["acquired-marker"]), { acquired });
	await waitForMarker(String(args["release-marker"]));
	store.releaseHolder(sessionId, holder);
	signal(String(args["released-marker"]));
	const root = store.lineageRoot(sessionId);
	close();
	return { acquired, root, holder };
}

async function leaseContender(): Promise<Record<string, unknown>> {
	const sessionId = String(args["session-id"]);
	const holder = String(args["holder-prefix"]) + ":pid=" + String(process.pid);
	const { store, close } = await openLeaseDb(String(args["db"]));
	await waitForMarker(String(args["acquired-marker"]));
	let notices = 0;
	const won = await store.acquireWait(sessionId, holder, {
		ttlSeconds: Number(args["ttl"] ?? 60),
		waitSeconds: Number(args["wait"] ?? 15),
		pollIntervalSeconds: Math.max(0.02, Number(args["poll"] ?? 0.05)),
		waitNoticeIntervalSeconds: 0,
		onWait: () => {
			notices++;
			if (notices === 1) {
				signal(String(args["blocked-marker"] ?? "blocked"));
			}
		},
	});
	const owner = store.probeOwner(sessionId);
	close();
	return {
		won,
		notices,
		conversationId: owner?.conversationId,
		holder: owner?.holder,
	};
}

const SCENARIOS: Record<string, () => Promise<Record<string, unknown>>> = {
	"hold-write": holdWrite,
	"contend-write": contendWrite,
	"writer-hold": writerHold,
	"snapshot-reader": snapshotReader,
	"hold-timed": holdTimed,
	"blocking-begin": blockingBegin,
	"lease-holder": leaseHolder,
	"lease-contender": leaseContender,
};

async function main(): Promise<number> {
	const scenario = String(args["scenario"] ?? "");
	const run = SCENARIOS[scenario];
	if (run === undefined) throw new Error(`unknown scenario: ${scenario}`);
	const result = await run();
	console.log(`RESULT_JSON ${JSON.stringify(result)}`);
	return 0;
}

main()
	.then((code) => process.exit(code))
	.catch((err) => {
		console.error(`CHILD_ERROR ${String(err)}`);
		process.exit(1);
	});
