// TEST INFRASTRUCTURE — standalone driver for cross-PROCESS handoff
// contracts. Run under RAW Node with pi_state/testing/node-ts-resolve.mjs
// (maps ".js" specifiers to ".ts"). Emits a single `RESULT_JSON {...}` line;
// multi-child scenarios coordinate via marker files (write = signal), the
// proven obligations two-process driver protocol.
//
//   claim-and-hold <db> <sessionId> <platform> <claimedMarker>
//       Seed a pending row, CLAIM it with this process's real clock, signal
//       <claimedMarker>, then hang — the parent SIGKILLs us between the claim
//       write and any terminal write, modeling a gateway crash mid-handoff.
//
//   claim-and-hold-setup <db> <sessionId> <platform>
//       Seed ONLY the pending row (no hold) — setup for the N-racer scene.
//
//   racer <db> <goMarker> <resultFile> <index> <sessionId>
//       Wait for the go marker, race ONE atomic pending→running claim, write
//       {won,index} to its own result file (no stdout races).
//
//   probe <db> <sessionId>
//       Fresh-boot read of the handoff state + pending list (durable truth).

import { writeFileSync } from "node:fs";

import { StateStore } from "../../../pi_state/index.js";
import { HandoffQueue } from "../queue.js";

function emit(result: unknown): void {
	process.stdout.write(`RESULT_JSON ${JSON.stringify(result)}\n`, () => {
		process.exit(0);
	});
}

async function waitForMarker(path: string, timeoutMs = 20_000): Promise<void> {
	const { existsSync } = await import("node:fs");
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(path)) {
		if (Date.now() > deadline) throw new Error(`timeout waiting ${path}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

async function open(
	dbPath: string,
): Promise<{ store: StateStore; queue: HandoffQueue }> {
	const store = await StateStore.open(dbPath);
	return { store, queue: new HandoffQueue(store.db) };
}

async function claimAndHold(
	dbPath: string,
	sessionId: string,
	platform: string,
	claimedMarker: string,
): Promise<Record<string, unknown>> {
	const { store, queue } = await open(dbPath);
	await queue.ensureSessionRow(sessionId);
	await queue.requestHandoff(sessionId, platform);
	const claimed = await queue.claimHandoff(sessionId);
	writeFileSync(claimedMarker, JSON.stringify({ claimed }));
	if (!claimed) {
		await store.close();
		return { claimed };
	}
	// Never returns: the parent SIGKILLs us while the row sits at 'running'.
	// A live interval pins the event loop — a bare pending promise would NOT
	// keep the process alive and we would exit 0 before the kill lands.
	setInterval(() => {}, 10_000);
	return new Promise<Record<string, unknown>>(() => {});
}

async function racer(
	dbPath: string,
	goMarker: string,
	resultFile: string,
	index: number,
	sessionId: string,
): Promise<Record<string, unknown>> {
	const { store, queue } = await open(dbPath);
	await waitForMarker(goMarker);
	const won = await queue.claimHandoff(sessionId);
	writeFileSync(resultFile, JSON.stringify({ won, index, pid: process.pid }));
	await store.close();
	return { won, index };
}

async function probe(
	dbPath: string,
	sessionId: string,
): Promise<Record<string, unknown>> {
	const { store, queue } = await open(dbPath);
	const state = queue.getHandoffState(sessionId);
	const pendingIds = queue.listPendingHandoffs().map((r) => r.id);
	await store.close();
	return { state, pendingIds };
}

const [, , scenario, ...rest] = process.argv;
async function main(): Promise<Record<string, unknown>> {
	switch (scenario) {
		case "claim-and-hold":
			return claimAndHold(rest[0]!, rest[1]!, rest[2]!, rest[3]!);
		case "claim-and-hold-setup": {
			const { store, queue } = await open(rest[0]!);
			await queue.ensureSessionRow(rest[1]!);
			const requested = await queue.requestHandoff(rest[1]!, rest[2]!);
			await store.close();
			return { requested };
		}
		case "racer":
			return racer(rest[0]!, rest[1]!, rest[2]!, Number(rest[3]), rest[4]!);
		case "probe":
			return probe(rest[0]!, rest[1]!);
		default:
			throw new Error(`unknown scenario: ${String(scenario)}`);
	}
}

main()
	.then((result) => emit(result))
	.catch((err: unknown) => {
		console.error(`CHILD_ERROR ${String(err)}`);
		process.exit(1);
	});
