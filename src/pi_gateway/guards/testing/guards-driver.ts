// TEST INFRASTRUCTURE — child-process driver for two-layer turn-lease
// coexistence contracts (DEC-004: in-process registry × cross-process DB
// store). Modeled on the proven Phase-0 spike driver pattern
// (spike/tests/lease.spike.test.ts + pi_state/testing/child-driver.ts).
//
// Run under: node --import <node-ts-resolve.mjs> guards-driver.ts --scenario <name> ...
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files in a shared dir (write = signal; poll = wait).

import { writeFileSync } from "node:fs";

interface Args {
	[k: string]: string;
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

interface Opened {
	store: import("../../../pi_state/leases.js").DbTurnLeaseStore;
	close: () => void;
}

async function openLeaseDb(path: string): Promise<Opened> {
	const { DbTurnLeaseStore } = await import("../../../pi_state/leases.js");
	const { LEASE_PROBE_DDL } = await import(
		"../../../pi_state/testing/probe-schema.js"
	);
	const DatabaseCtor = (await import("better-sqlite3")).default;
	const db = new DatabaseCtor(path);
	db.exec(LEASE_PROBE_DDL);
	db.pragma("journal_mode = wal");
	const store = new DbTurnLeaseStore(db);
	return { store, close: () => db.close() };
}

/**
 * Child holds BOTH layers for one session: its OWN in-process L1 token AND
 * the cross-process DB lease keyed on the compression-lineage root. Signals
 * `acquired` once both are held; releases BOTH on the marker; reports the
 * walked root so the parent can assert keying agreement.
 */
async function bothHold(): Promise<Record<string, unknown>> {
	const { SessionTurnLeaseRegistry } = await import("../turn-lease.js");
	const sessionId = String(args["session-id"]);
	const { store, close } = await openLeaseDb(String(args["db"]));
	try {
		// Layer 2 first (cross-process visibility)…
		const holder = `${String(args["holder-prefix"])}:pid=${String(process.pid)}`;
		const acquiredDb = store.tryAcquire(sessionId, holder, 300);
		// …then layer 1 (in-process serialization).
		const registry = new SessionTurnLeaseRegistry();
		let l1Token: unknown = null;
		if (acquiredDb) {
			l1Token = await registry.acquire(sessionId, {
				ownerKey: String(args["holder-prefix"]),
				generation: 1,
			});
		}
		signal(String(args["acquired-marker"]), {
			acquired: acquiredDb && l1Token !== null,
			holder,
		});
		await waitForMarker(String(args["release-marker"]));
		if (l1Token !== null) registry.release(l1Token as never);
		store.releaseHolder(sessionId, holder);
		signal(String(args["released-marker"]));
		return { root: store.lineageRoot(sessionId), holder };
	} finally {
		close();
	}
}

/**
 * Child that grabs the DB lease and then HANGS FOREVER holding it — the
 * SIGKILL victim. Only dead-PID reclaim can free this lease before TTL.
 */
async function holdForever(): Promise<Record<string, unknown>> {
	const sessionId = String(args["session-id"]);
	// The victim never closes its connection — the SIGKILL is the cleanup.
	const { store } = await openLeaseDb(String(args["db"]));
	const holder = `${String(args["holder-prefix"])}:pid=${String(process.pid)}`;
	const acquired = store.tryAcquire(
		sessionId,
		holder,
		Number(args["ttl"] ?? 300),
	);
	signal(String(args["acquired-marker"]), { acquired, holder });
	// Never returns: the parent SIGKILLs us mid-hold. A live interval handle
	// pins the event loop — a bare pending promise would NOT keep the process
	// alive and the victim would exit 0 before the kill lands.
	setInterval(() => {}, 10_000);
	return new Promise<Record<string, unknown>>(() => {});
}

const SCENARIOS: Record<string, () => Promise<Record<string, unknown>>> = {
	"both-hold": bothHold,
	"hold-forever": holdForever,
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
	.catch((err: unknown) => {
		console.error(`CHILD_ERROR ${String(err)}`);
		process.exit(1);
	});
