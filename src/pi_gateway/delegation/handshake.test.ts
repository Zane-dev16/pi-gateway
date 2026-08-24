// Behavior contracts for the §7.1 claim/release/complete handshake.
// Every mutation is a CAS'd single-row UPDATE under BEGIN IMMEDIATE, so
// racing consumers get exactly one winner per transition. Cross-PROCESS
// atomicity lives in two-process.test.ts; this file pins the durable state
// machine itself with injected time (no wall-clock waits).

import { rmSync } from "node:fs";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CLAIM_STALE_SECONDS, MAX_DELIVERY_ATTEMPTS } from "./rail.js";
import { openRailHarness, type RailHarness } from "./testing/harness.js";

let h: RailHarness;

beforeEach(async () => {
	h = await openRailHarness();
});

afterEach(async () => {
	await h.close();
	rmSync(h.dir, { recursive: true, force: true });
});

async function publishPendingItem(delegationId = "dlg-1"): Promise<string> {
	await h.rail.recordDispatch({
		delegationId,
		originSession: "telegram|chat|42",
		parentSessionId: "sess-parent",
		task: { goal: "summarize the fleet log" },
	});
	const now = h.clock.nowSeconds();
	await h.rail.publishCompletion({
		delegationId,
		event: {
			type: "async_delegation",
			delegation_id: delegationId,
			status: "completed",
			completed_at: now,
		},
		result: { summary: "all green" },
	});
	return delegationId;
}

describe("durable publish", () => {
	it("producer write lands event+result with delivery_state=pending BEFORE any queue entry", async () => {
		const id = await publishPendingItem();
		const row = h.rail.row(id);
		expect(row?.state).toBe("completed");
		expect(row?.delivery_state).toBe("pending");
		expect(row?.delivery_attempts).toBe(0);
		expect(row?.event_json).toContain(`"delegation_id":"${id}"`);
		expect(row?.result_json).toContain("all green");
		expect(row?.completed_at).toBe(h.clock.nowSeconds());
	});

	it("publishing an unknown delegation throws typed NotFound (no phantom rows)", async () => {
		await expect(
			h.rail.publishCompletion({ delegationId: "nope", event: {} }),
		).rejects.toMatchObject({ name: "DelegationNotFoundError" });
	});

	it("re-publish resets a delivered row's delivery machine to pending (producer finalize parity)", async () => {
		const id = await publishPendingItem();
		const claim = h.rail.makeClaimId("consumer-a");
		expect(await h.rail.claimCompletion(id, claim)).toBe(true);
		expect(await h.rail.completeClaim(id, claim)).toBe(true);
		expect(h.rail.deliveryStateOf(id)).toBe("delivered");
		await h.rail.publishCompletion({
			delegationId: id,
			event: { again: true },
		});
		expect(h.rail.deliveryStateOf(id)).toBe("pending");
	});
});

describe("claim handshake atomicity (in-process)", () => {
	it("N racers claim one pending item => exactly ONE owner", async () => {
		const id = await publishPendingItem();
		const RACERS = 25;
		const results: boolean[] = [];
		for (let i = 0; i < RACERS; i++) {
			results.push(
				await h.rail.claimCompletion(id, h.rail.makeClaimId(`racer-${i}`)),
			);
		}
		expect(results.filter((w) => w)).toHaveLength(1);
		const row = h.rail.row(id);
		expect(row?.delivery_attempts).toBe(1); // incremented once, for the winner
		expect(row?.delivery_claim).toMatch(/^racer-0:/);
	});

	it("a live claim blocks reclaim until CLAIM_STALE_SECONDS passes; stale claims are taken over", async () => {
		const id = await publishPendingItem();
		const first = h.rail.makeClaimId("first");
		expect(await h.rail.claimCompletion(id, first)).toBe(true);
		// Younger than the 300 s window: rejected even for a fresh consumer.
		h.clock.advance(CLAIM_STALE_SECONDS - 1);
		expect(await h.rail.claimCompletion(id, h.rail.makeClaimId("eager"))).toBe(
			false,
		);
		// EXACTLY at 300 s the guard (strict <) still holds the line...
		h.clock.advance(1);
		expect(
			await h.rail.claimCompletion(id, h.rail.makeClaimId("boundary")),
		).toBe(false);
		// ...one second past it, the stale claim is taken over atomically.
		h.clock.advance(1);
		const second = h.rail.makeClaimId("second");
		expect(await h.rail.claimCompletion(id, second)).toBe(true);
		const row = h.rail.row(id);
		expect(row?.delivery_claim).toBe(second);
		expect(row?.delivery_attempts).toBe(2);
	});

	it("claims against non-pending rows are rejected; unknown ids NEVER claim (divergence 1)", async () => {
		await publishPendingItem("live-1");
		const c1 = h.rail.makeClaimId("c1");
		expect(await h.rail.claimCompletion("unknown-id", c1)).toBe(false);
		expect(await h.rail.claimCompletion("live-1", c1)).toBe(true);
		expect(await h.rail.completeClaim("live-1", c1)).toBe(true);
		// delivered rows are not claimable
		expect(
			await h.rail.claimCompletion("live-1", h.rail.makeClaimId("c2")),
		).toBe(false);
	});
});

describe("release / drop / complete transitions", () => {
	it("release returns the item to pending cleanly; another consumer claims it", async () => {
		const id = await publishPendingItem();
		const a = h.rail.makeClaimId("a");
		expect(await h.rail.claimCompletion(id, a)).toBe(true);
		h.clock.advance(5);
		expect(await h.rail.releaseClaim(id, a)).toBe(true);
		let row = h.rail.row(id);
		expect(row?.delivery_state).toBe("pending");
		expect(row?.delivery_claim).toBeNull();
		expect(row?.delivery_claimed_at).toBeNull();

		h.clock.advance(5);
		const b = h.rail.makeClaimId("b");
		expect(await h.rail.claimCompletion(id, b)).toBe(true); // no 300s wait: claim was CLEARED
		row = h.rail.row(id);
		expect(row?.delivery_claim).toBe(b);
		expect(row?.delivery_attempts).toBe(2); // budget spent per claim, not per release
	});

	it("double-release is rejected: only the current claim holder may release", async () => {
		const id = await publishPendingItem();
		const a = h.rail.makeClaimId("a");
		expect(await h.rail.claimCompletion(id, a)).toBe(true);
		expect(await h.rail.releaseClaim(id, a)).toBe(true);
		// Second release with the SAME (now cleared) token must fail...
		expect(await h.rail.releaseClaim(id, a)).toBe(false);
		// ...and so must a release from someone who never held the claim.
		expect(await h.rail.releaseClaim(id, h.rail.makeClaimId("b"))).toBe(false);
		expect(h.rail.deliveryStateOf(id)).toBe("pending");
	});

	it("complete marks delivered exactly once and ONLY for the claim holder", async () => {
		const id = await publishPendingItem();
		const a = h.rail.makeClaimId("a");
		expect(await h.rail.claimCompletion(id, a)).toBe(true);
		// A non-holder cannot ack.
		expect(await h.rail.completeClaim(id, h.rail.makeClaimId("impostor"))).toBe(
			false,
		);
		expect(h.rail.deliveryStateOf(id)).toBe("pending");
		const tComplete = h.clock.nowSeconds() + 10;
		h.clock.advance(10);
		expect(await h.rail.completeClaim(id, a)).toBe(true);
		const row = h.rail.row(id);
		expect(row?.delivery_state).toBe("delivered");
		expect(row?.delivered_at).toBe(tComplete);
		expect(row?.delivery_claim).toBeNull();
		// Repeat acks are no-ops (at-most-once).
		expect(await h.rail.completeClaim(id, a)).toBe(false);
	});

	it("markDelivered acks any non-delivered row once, then reports false forever", async () => {
		const id = await publishPendingItem();
		expect(await h.rail.markDelivered(id)).toBe(true);
		expect(await h.rail.markDelivered(id)).toBe(false);
		expect(h.rail.row(id)?.delivered_at).not.toBeNull();
	});

	it("dropClaim terminally drops only for the holder; payload stays queryable", async () => {
		const id = await publishPendingItem();
		const a = h.rail.makeClaimId("a");
		await h.rail.claimCompletion(id, a);
		expect(await h.rail.dropClaim(id, h.rail.makeClaimId("other"))).toBe(false);
		expect(await h.rail.dropClaim(id, a)).toBe(true);
		const row = h.rail.row(id);
		expect(row?.delivery_state).toBe("dropped");
		expect(row?.result_json).toContain("all green"); // queryable, not deleted
		expect(row?.delivery_claim).toBeNull();
		expect(await h.rail.claimCompletion(id, h.rail.makeClaimId("zombie"))).toBe(
			false,
		);
	});
});

describe("attempt cap convergence", () => {
	it(`${MAX_DELIVERY_ATTEMPTS} claimed-and-released attempts converge the row to terminal 'dropped'`, async () => {
		const id = await publishPendingItem();
		for (let i = 1; i <= MAX_DELIVERY_ATTEMPTS; i++) {
			const claim = h.rail.makeClaimId(`try-${i}`);
			// Each cycle releases explicitly so no clock advance is needed.
			expect(await h.rail.claimCompletion(id, claim)).toBe(true);
			expect(await h.rail.releaseClaim(id, claim)).toBe(true);
		}
		// The 8th release saw attempts>=8 and converged instead of re-queuing.
		expect(h.rail.deliveryStateOf(id)).toBe("dropped");
		expect(await h.rail.claimCompletion(id, h.rail.makeClaimId("late"))).toBe(
			false,
		);
		const row = h.rail.row(id);
		expect(row?.delivery_attempts).toBe(MAX_DELIVERY_ATTEMPTS);
		expect(row?.result_json).toContain("all green"); // still queryable
	});

	it("attempt-cap boundary sits at >=8 on RELEASE, not claim-time state change", async () => {
		const id = await publishPendingItem();
		for (let i = 1; i < MAX_DELIVERY_ATTEMPTS; i++) {
			const claim = h.rail.makeClaimId(`t-${i}`);
			await h.rail.claimCompletion(id, claim);
			await h.rail.releaseClaim(id, claim);
		}
		// 7 attempts burned: still recoverable.
		expect(h.rail.deliveryStateOf(id)).toBe("pending");
		const last = h.rail.makeClaimId("t-final");
		expect(await h.rail.claimCompletion(id, last)).toBe(true);
		// An ACK still wins at the boundary — dropping only happens on release.
		expect(await h.rail.completeClaim(id, last)).toBe(true);
		expect(h.rail.deliveryStateOf(id)).toBe("delivered");
	});
});
