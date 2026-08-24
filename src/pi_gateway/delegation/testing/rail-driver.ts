// TEST INFRASTRUCTURE — standalone driver for cross-PROCESS rail contracts.
// Run under RAW Node with the pi_state/testing/node-ts-resolve.mjs hook
// (maps ".js" specifiers to ".ts"). Emits a single `RESULT_JSON {...}` line
// on stdout; multi-child scenarios coordinate via marker files in a shared
// dir (write = signal; poll = wait), mirroring the Phase-0/pi_state
// child-driver protocol.
//
//   setup-pending <db> <delegationId>
//       Seed one durable pending completion (no claim).
//   publish-claim-hold <db> <coordDir> <delegationId> <claimedMarker>
//       Publish durably, CLAIM with this process's REAL wall clock, signal
//       <claimedMarker>, then hang — the parent SIGKILLs us mid-claim to
//       model a gateway crash between the claim write and any ack.
//   restore-complete <db> <delegationId> <graceSeconds>
//       One boot: restoreUndelivered (injected now = real + grace), then a
//       fresh consumer claim + complete ack, then restore AGAIN. Reports
//       both counts so the caller can pin EXACTLY-once replay.
//   restore-only <db>
//       A later boot's restore pass alone (must see zero rows).
//   racer <db> <goMarker> <resultFile> <index> [delegationId]
//       Wait for the go marker, race ONE atomic claim, the winner completes
//       the ack. Writes {won,index} to its own result file (no stdout races).

import { writeFileSync } from "node:fs";

import { StateStore } from "../../../pi_state/index.js";
import { DelegationRail } from "../rail.js";
import { ManualClock } from "./manual-clock.js";

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
		await new Promise<void>((r) => setTimeout(r, 5));
	}
}

async function seedPending(
	rail: DelegationRail,
	delegationId: string,
): Promise<void> {
	await rail.recordDispatch({
		delegationId,
		originSession: "telegram|chat|crash",
		parentSessionId: "sess-crash",
		task: { goal: "survive the crash" },
	});
	await rail.publishCompletion({
		delegationId,
		event: { type: "async_delegation", delegation_id: delegationId },
		result: { summary: "crash-surviving answer" },
	});
}

async function main(): Promise<void> {
	const [, , command, dbPath, ...rest] = process.argv;
	if (!command || !dbPath) {
		emit({ error: "usage: rail-driver.ts <command> <db> [...]" });
		return;
	}

	if (command === "setup-pending") {
		const store = await StateStore.open(dbPath);
		const rail = new DelegationRail(store.db);
		await seedPending(rail, String(rest[0]));
		const state = rail.deliveryStateOf(String(rest[0]));
		await store.close(false);
		emit({ seeded: true, deliveryState: state });
		return;
	}

	if (command === "publish-claim-hold") {
		// rest[0] is coordDir (unused inside; kept for CLI symmetry with siblings)
		const delegationId = String(rest[1]);
		const claimedMarker = String(rest[2]);
		const store = await StateStore.open(dbPath);
		const rail = new DelegationRail(store.db); // REAL system clock
		await seedPending(rail, delegationId);
		const claim = rail.makeClaimId("doomed-consumer");
		const claimed = await rail.claimCompletion(delegationId, claim);
		writeFileSync(claimedMarker, JSON.stringify({ claimed }));
		// Hold until killed — no graceful close, no ack, no release.
		await new Promise<void>(() => {
			// never resolves; the parent's SIGKILL is the point
		});
		return;
	}

	if (command === "restore-complete") {
		const delegationId = String(rest[0]);
		const graceSeconds = Number(rest[1] ?? 301);
		const store = await StateStore.open(dbPath);
		// Injected clock consistent with the crashed child's REAL stamps:
		// advance past the 300 s stale-claim window without wall waits.
		const clock = new ManualClock(Date.now() / 1000 + graceSeconds);
		const rail = new DelegationRail(store.db, { clock });
		const events: Array<Record<string, unknown>> = [];
		const firstRestore = await rail.restoreUndelivered((e) => events.push(e));
		// The next consumer proves ownership and acks AFTER acceptance.
		const claim = rail.makeClaimId("boot-two");
		const claimed = await rail.claimCompletion(delegationId, claim);
		let completed = false;
		if (claimed) completed = await rail.completeClaim(delegationId, claim);
		const secondRestore = await rail.restoreUndelivered(() => {});
		const row = rail.row(delegationId);
		await store.close(false);
		emit({
			firstRestore,
			events,
			claimed,
			completed,
			secondRestore,
			deliveryState: row?.delivery_state ?? null,
			attempts: row?.delivery_attempts ?? null,
			deliveredAt: row?.delivered_at ?? null,
			eventJsonStillDurable: row?.event_json ?? null,
		});
		return;
	}

	if (command === "restore-only") {
		const store = await StateStore.open(dbPath);
		const clock = new ManualClock(Date.now() / 1000 + 400);
		const rail = new DelegationRail(store.db, { clock });
		const seen: string[] = [];
		const restored = await rail.restoreUndelivered((e) => {
			seen.push(String(e["delegation_id"] ?? "?"));
		});
		await store.close(false);
		emit({ restored, seen });
		return;
	}

	if (command === "probe") {
		const store = await StateStore.open(dbPath);
		const rail = new DelegationRail(store.db);
		const row = rail.row(String(rest[0]));
		await store.close(false);
		emit({
			exists: row !== null,
			deliveryState: row?.delivery_state ?? null,
			attempts: row?.delivery_attempts ?? null,
			claim: row?.delivery_claim ?? null,
		});
		return;
	}

	if (command === "racer") {
		const goMarker = String(rest[0]);
		const resultFile = String(rest[1]);
		const index = Number(rest[2] ?? -1);
		const delegationId = String(rest[3] ?? "dlg-race");
		const store = await StateStore.open(dbPath);
		const rail = new DelegationRail(store.db); // real clock; race is NOW
		await waitForMarker(goMarker);
		const claim = rail.makeClaimId(`racer-${process.pid}-${index}`);
		const won = await rail.claimCompletion(delegationId, claim);
		let completed = false;
		if (won) completed = await rail.completeClaim(delegationId, claim);
		const attempts = rail.row(delegationId)?.delivery_attempts ?? null;
		await store.close(false);
		writeFileSync(
			resultFile,
			JSON.stringify({ won, completed, index, attempts }),
		);
		emit({ done: true });
		return;
	}

	emit({ error: `unknown command ${command}` });
}

main().catch((err) => {
	emit({ error: String(err) });
});
