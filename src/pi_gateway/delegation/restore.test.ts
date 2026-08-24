// Behavior contracts for §7.1 boot restore: undelivered completions come
// back EXACTLY ONCE (the claim handshake makes redelivery at-most-once even
// when several boots race), the restored=True stamp is IN-MEMORY ONLY
// (#64484), and dead-owner running rows synthesize status:'unknown'
// completions instead of sticking forever. Cross-process crash simulation
// lives in two-process.test.ts.

import { rmSync } from "node:fs";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { DelegationRail } from "./rail.js";
import { openRailHarness, type RailHarness } from "./testing/harness.js";
import { readProcessStartTime } from "./rail.js";

let h: RailHarness;

beforeEach(async () => {
	h = await openRailHarness();
});

afterEach(async () => {
	await h.close();
	rmSync(h.dir, { recursive: true, force: true });
});

async function seedCompleted(
	delegationId: string,
	completedAtOffset = 0,
): Promise<void> {
	const t = h.clock.nowSeconds() + completedAtOffset;
	await h.rail.recordDispatch({
		delegationId,
		originSession: "telegram|chat|7",
		parentSessionId: "p-1",
		task: { goal: "g" },
		dispatchedAt: t - 100,
	});
	await h.rail.publishCompletion({
		delegationId,
		event: { type: "async_delegation", delegation_id: delegationId },
		result: { summary: delegationId },
		completedAt: t,
	});
}

describe("restore_undelivered parity", () => {
	it("crash between claim-write and ack ⇒ next boot restores EXACTLY once; later boots see nothing", async () => {
		const id = "dlg-crash";
		await seedCompleted(id);
		// Consumer A claimed, injected into the adapter... then died before any
		// durable ack. The claim columns remain, delivery_state stays pending.
		const deadClaim = h.rail.makeClaimId("dead-consumer");
		expect(await h.rail.claimCompletion(id, deadClaim)).toBe(true);

		// BOOT 2: a fresh engine (new instance = fresh process stand-in) with a
		// clock past the 300 s stale-claim window.
		h.clock.advance(301);
		const restoredEvents: Array<Record<string, unknown>> = [];
		const rail2 = new DelegationRail(h.store.db, { clock: h.clock });
		expect(await rail2.restoreUndelivered((e) => restoredEvents.push(e))).toBe(
			1,
		);
		const evt = restoredEvents[0];
		expect(evt?.["delegation_id"]).toBe(id);
		expect(evt?.["restored"]).toBe(true); // ownership-proving stamp present

		// Exactly ONE consumer can act on the replay: stale claim is taken over
		// atomically, delivery acks once.
		const b = rail2.makeClaimId("consumer-b");
		expect(await rail2.claimCompletion(id, b)).toBe(true);
		expect(await rail2.completeClaim(id, b)).toBe(true);

		// BOOT 3 (+ every later boot): nothing left to restore.
		const again: Array<Record<string, unknown>> = [];
		expect(await rail2.restoreUndelivered((e) => again.push(e))).toBe(0);
		expect(again).toHaveLength(0);
		expect(h.rail.deliveryStateOf(id)).toBe("delivered");
	});

	it("restored stamp NEVER persists: durable event_json bytes unchanged", async () => {
		await seedCompleted("dlg-stamp");
		const before = h.rail.row("dlg-stamp")?.event_json;
		const seen: Array<Record<string, unknown>> = [];
		await h.rail.restoreUndelivered((e) => seen.push(e));
		expect(seen[0]?.["restored"]).toBe(true);
		expect(h.rail.row("dlg-stamp")?.event_json).toBe(before);
		expect(before).not.toContain("restored");
	});

	it("repeated boots re-enqueue the same pending row WITHOUT duplicating durable effects; delivery still lands once", async () => {
		await seedCompleted("dlg-twice");
		const boot1: Array<Record<string, unknown>> = [];
		const boot2: Array<Record<string, unknown>> = [];
		expect(await h.rail.restoreUndelivered((e) => boot1.push(e))).toBe(1);
		h.clock.advance(30); // a second boot some seconds later
		expect(await h.rail.restoreUndelivered((e) => boot2.push(e))).toBe(1);
		expect(boot1).toHaveLength(1);
		expect(boot2).toHaveLength(1);
		// Two consumers race for the replayed item...
		const a = h.rail.makeClaimId("a");
		const b = h.rail.makeClaimId("b");
		const [wa, wb] = await Promise.all([
			h.rail.claimCompletion("dlg-twice", a),
			h.rail.claimCompletion("dlg-twice", b),
		]);
		expect([wa, wb].filter(Boolean)).toHaveLength(1); // exactly one owner
		if (wa !== true && wb !== true) throw new Error("unreachable: no winner");
		const winner = wa === true ? a : b;
		expect(await h.rail.completeClaim("dlg-twice", winner)).toBe(true);
		// ...and the loser's ack can never double-mark anything.
		const loser = wa ? b : a;
		expect(await h.rail.completeClaim("dlg-twice", loser)).toBe(false);
		expect(h.rail.row("dlg-twice")?.delivery_attempts).toBe(1);
	});

	it("restore order is completed_at then delegation_id; running (unfinalized) rows are skipped", async () => {
		await seedCompleted("dlg-b", 10); // older completion
		await seedCompleted("dlg-a", 20);
		await seedCompleted("dlg-c", 20); // tie broken by id: a before c
		await h.rail.recordDispatch({
			delegationId: "dlg-running",
			originSession: "telegram|chat|7",
		}); // still running, no event
		const order: string[] = [];
		await h.rail.restoreUndelivered((e) =>
			order.push(String(e["delegation_id"])),
		);
		expect(order).toEqual(["dlg-b", "dlg-a", "dlg-c"]);
	});
});

describe("recover_abandoned parity (dead-owner running rows)", () => {
	it("dead owner ⇒ status:'unknown' completion synthesized, delivery reset to pending, routable origin kept", async () => {
		// A genuinely DEAD pid: this child exited already, so ESRCH proves it.
		const { spawnSync } = await import("node:child_process");
		const dead = spawnSync(process.execPath, ["-e", "process.exit(0)"]);
		expect(dead.status).toBe(0);
		await h.rail.recordDispatch({
			delegationId: "dlg-orphan",
			originSession: "slack|chan|9",
			parentSessionId: "sess-9",
			task: {
				goal: "nightly sweep",
				scope_id: "tenant-77",
				user_id: "u-42",
			},
			state: "running",
		});
		// Overwrite the owner stamp with the dead child's REAL pid/start-time.
		const deadStart = readProcessStartTime(dead.pid ?? 1) ?? 12345;
		await h.store.withWrite((conn) => {
			conn
				.prepare(
					"UPDATE async_delegations SET owner_pid = ?, owner_started_at = ? WHERE delegation_id = 'dlg-orphan'",
				)
				.run(dead.pid ?? 999_999_999, deadStart);
		});

		h.clock.advance(60);
		const restored: Array<Record<string, unknown>> = [];
		expect(await h.rail.restoreUndelivered((e) => restored.push(e))).toBe(1);
		const evt = restored[0];
		expect(evt?.["status"]).toBe("unknown");
		expect(evt?.["session_key"]).toBe("slack|chan|9");
		expect(evt?.["scope_id"]).toBe("tenant-77"); // routing origin rides along
		expect(evt?.["error"]).toContain("outcome unknown");
		const row = h.rail.row("dlg-orphan");
		expect(row?.state).toBe("unknown");
		expect(row?.delivery_state).toBe("pending");
		let storedResult: unknown = null;
		try {
			storedResult = JSON.parse(row?.result_json ?? "null") as unknown;
		} catch {
			storedResult = "unparsable";
		}
		expect(storedResult).toMatchObject({ status: "unknown" });
	});

	it("LIVE owners are never touched; a recycled PID (same pid, different start ticks) counts as dead", async () => {
		const liveStamp = h.rail.selfOwner();
		const probePid = liveStamp.pid;
		const probeStart = readProcessStartTime(probePid);
		await h.rail.recordDispatch({
			delegationId: "dlg-live",
			originSession: "s",
			state: "running",
		});
		await h.rail.recordDispatch({
			delegationId: "dlg-reused",
			originSession: "s",
			state: "running",
		});
		await h.store.withWrite((conn) => {
			conn
				.prepare(
					"UPDATE async_delegations SET owner_pid = ?, owner_started_at = ? WHERE delegation_id = 'dlg-live'",
				)
				.run(probePid, probeStart);
			conn
				.prepare(
					"UPDATE async_delegations SET owner_pid = ?, owner_started_at = ? WHERE delegation_id = 'dlg-reused'",
				)
				// Same LIVE pid, WRONG start ticks ⇒ recycled-PID fingerprint.
				.run(probePid, (probeStart ?? 0) + 1);
		});
		expect(await h.rail.recoverAbandoned()).toBe(1);
		expect(h.rail.row("dlg-live")?.state).toBe("running"); // untouched
		expect(h.rail.row("dlg-reused")?.state).toBe("unknown"); // reuse detected
	});
});
