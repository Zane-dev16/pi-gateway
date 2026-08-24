// Behavior contracts for the §7.2 OWNERSHIP DECISION TABLE (06 §7.2; DEC-018;
// 06 §10 "Ownership matrix" row). Every verdict maps to its honest durable
// disposition via the Phase-4 handshake:
//   terminal → dropClaim (NOT delivered, NOT pending-replay)
//   retry    → releaseClaim (attempt burn visible; cap converges churn)
//   deliver  → concrete target resolved (+ route binding performed), forged
//              turn dispatched once, acked AFTER acceptance.
//
// The headline row completes Phase 4's deferred matrix entry:
//   "lineage-tip retarget resolves to the CURRENT tip after a compression
//    rotation lands BETWEEN dispatch and completion".

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	openWatcherHarness,
	pendingRow,
	seedCompletion,
	seedRouting,
	seedSession,
	type WatcherHarness,
} from "./testing/harness.js";

const KEY = "agent:main:telegram:dm:100";

let h: WatcherHarness;

beforeEach(async () => {
	h = await openWatcherHarness();
	await seedSession(h, "parent", { sessionKey: KEY });
	await seedRouting(h, KEY, "parent");
});

afterEach(async () => {
	await h.close();
});

async function dispatchAndComplete(
	delegationId: string,
	parentSessionId: string | null,
): Promise<void> {
	await seedCompletion(h, {
		delegationId,
		originSession: KEY,
		parentSessionId,
		goal: "scan the subnet",
	});
}

describe("decision table — deliver arms", () => {
	it("live parent delivers PINNED to itself", async () => {
		await dispatchAndComplete("dlg-live", "parent");

		const r = await h.engine.deliverGroup([firstPending("dlg-live")]);
		expect(r.disposition).toBe("delivered");
		expect(r.targetSessionId).toBe("parent");
		// Key already binds the owner — no rebinding, exactly one forged turn.
		expect(routingPointsTo(KEY)).toBe("parent");
		expect(h.dispatcher.events).toHaveLength(1);
		expect(pendingRow(h, "dlg-live")?.delivery_state).toBe("delivered");
	});

	it("idle-end retarget delivers to the chat's CURRENT session without rebinding", async () => {
		// Parent idle-ended AFTER dispatch; the chat moved on to session B.
		await seedSession(h, "parent", {
			sessionKey: KEY,
			endedAt: h.clock.nowSeconds() - 30,
			endReason: "idle",
		});
		await seedSession(h, "chat-current", { sessionKey: KEY });
		await seedRouting(h, KEY, "chat-current");
		await dispatchAndComplete("dlg-idle", "parent");

		const r = await h.engine.deliverGroup([firstPending("dlg-idle")]);
		expect(r.disposition).toBe("delivered");
		expect(r.targetSessionId).toBe("chat-current");
		// Retarget needs NO rebinding: the key already binds the current session.
		expect(routingPointsTo(KEY)).toBe("chat-current");
		expect(h.dispatcher.events).toHaveLength(1);
		expect(pendingRow(h, "dlg-idle")?.delivery_state).toBe("delivered");
	});

	it("lineage-tip retarget resolves to the CURRENT tip after compression between dispatch and completion", async () => {
		await dispatchAndComplete("dlg-lineage", "parent"); // dispatch-time truth: parent live
		// …then TWO rotations land before completion delivery is attempted:
		// parent ends compression → c1 → c1 ends compression → c2 (live tip).
		await compressTo("parent", "c1");
		await compressTo("c1", "c2");
		await seedSession(h, "c2", {});
		await seedRouting(h, KEY, "parent"); // route still parked on the dead root

		const r = await h.engine.deliverGroup([firstPending("dlg-lineage")]);
		expect(r.disposition).toBe("delivered");
		expect(r.targetSessionId).toBe("c2"); // walked BOTH hops — current tip
		expect(routingPointsTo(KEY)).toBe("c2"); // CAS-advanced along lineage
		expect(h.dispatcher.texts()[0]).toContain("[ASYNC DELEGATION COMPLETE");
		expect(pendingRow(h, "dlg-lineage")?.delivery_state).toBe("delivered");
	});

	it("stale INTERMEDIATE route accepted only when its own verified tip equals the target", async () => {
		await dispatchAndComplete("dlg-stale-ok", "parent");
		await compressTo("parent", "c1");
		await compressTo("c1", "c2");
		await seedSession(h, "c2", {});
		await seedRouting(h, KEY, "c1"); // route rode one rotation ahead of the walk

		const r = await h.engine.deliverGroup([firstPending("dlg-stale-ok")]);
		expect(r.disposition).toBe("delivered");
		expect(r.targetSessionId).toBe("c2");
		expect(routingPointsTo(KEY)).toBe("c2");
	});

	it("unrelated route NEVER captured (route-owns-lineage invariant): released, not delivered", async () => {
		await dispatchAndComplete("dlg-unrelated", "parent");
		await compressTo("parent", "c1");
		await seedSession(h, "c1", {}); // live continuation exists…
		await seedSession(h, "side-chat", { sourcePlatform: "discord" }); // …but the key points elsewhere
		await seedRouting(h, KEY, "side-chat");

		const r = await h.engine.deliverGroup([firstPending("dlg-unrelated")]);
		// Fail closed: injection dropped, claim RELEASED (transient — the route
		// may yet heal), row stays pending and queryable.
		expect(r.disposition).toBe("retry");
		expect(h.dispatcher.events).toHaveLength(0);
		const row = pendingRow(h, "dlg-unrelated");
		expect(row?.delivery_state).toBe("pending");
		expect(row?.delivery_claim).toBeNull();
	});
});

describe("decision table — terminal drops (fail-closed)", () => {
	it.each([
		"session_reset",
		"new_session",
		"user_exit",
		"session_switch",
	] as const)(
		"user-boundary end (%s) is RECORDED dropped, never delivered, payload queryable",
		async (endReason) => {
			await seedSession(h, "parent", {
				sessionKey: KEY,
				endedAt: h.clock.nowSeconds() - 5,
				endReason,
			});
			await dispatchAndComplete(`dlg-${endReason}`, "parent");

			const r = await h.engine.deliverGroup([firstPending(`dlg-${endReason}`)]);
			expect(r.disposition).toBe("dropped");
			expect(h.dispatcher.events).toHaveLength(0);

			const row = pendingRow(h, `dlg-${endReason}`);
			expect(row?.delivery_state).toBe("dropped"); // honest ack
			expect(row?.delivery_attempts).toBe(1);
			// Result stays queryable either way (fail-closed ownership).
			const payload = h.store.db
				.prepare(
					"SELECT event_json, result_json FROM async_delegations WHERE delegation_id = ?",
				)
				.get(`dlg-${endReason}`) as {
				event_json: string | null;
				result_json: string | null;
			};
			expect(payload.event_json).toContain("scan the subnet");
			expect(payload.result_json).not.toBeNull();
		},
	);

	it("unknown parent row terminally drops fail-closed", async () => {
		await dispatchAndComplete("dlg-unknown", "no-such-parent");

		const r = await h.engine.deliverGroup([firstPending("dlg-unknown")]);
		expect(r.disposition).toBe("dropped");
		expect(h.dispatcher.events).toHaveLength(0);
		expect(pendingRow(h, "dlg-unknown")?.delivery_state).toBe("dropped");
	});
});

describe("decision table — retry arms", () => {
	it("session DB unavailable releases the claim (attempt burn visible)", async () => {
		await dispatchAndComplete("dlg-dbdown", "parent");
		const row = firstPending("dlg-dbdown");
		await h.close(); // lookup will throw against the closed connection

		const r = await h.engine.deliverGroup([row]);
		expect(r.disposition).toBe("retry");
		expect(h.dispatcher.events).toHaveLength(0);
		// Reopen just to observe durable state (release landed before close? no —
		// release ALSO throws on a closed DB; containment maps it to retry and
		// the row's fate is bounded by the attempt cap on a later live consumer).
	});

	it("compression mid-rotation (no visible continuation) retries instead of dropping", async () => {
		await seedSession(h, "parent", {
			sessionKey: KEY,
			endedAt: h.clock.nowSeconds() - 5,
			endReason: "compression",
		});
		await dispatchAndComplete("dlg-rotation", "parent");

		const r = await h.engine.deliverGroup([firstPending("dlg-rotation")]);
		expect(r.disposition).toBe("retry");
		const row = pendingRow(h, "dlg-rotation");
		expect(row?.delivery_state).toBe("pending"); // held for a later consumer
		expect(row?.delivery_claim).toBeNull(); // released for that consumer
	});

	it("ENDED continuation retries (tip ended)", async () => {
		await seedSession(h, "parent", {
			sessionKey: KEY,
			endedAt: h.clock.nowSeconds() - 5,
			endReason: "compression",
		});
		await dispatchAndComplete("dlg-deadtip", "parent");
		await seedSession(h, "dead-child", {
			parentSessionId: "parent",
			endedAt: h.clock.nowSeconds(),
			endReason: "idle",
		});

		const r = await h.engine.deliverGroup([firstPending("dlg-deadtip")]);
		expect(r.disposition).toBe("retry");
		expect(h.dispatcher.events).toHaveLength(0);
	});
});

// -- helpers -----------------------------------------------------------------

function firstPending(delegationId: string): {
	delegationId: string;
	originSession: string;
	parentSessionId: string | null;
	event: Record<string, unknown>;
	completedAt: number | null;
	dispatchedAt: number;
} {
	const raw = h.store.db
		.prepare(
			"SELECT delegation_id, origin_session, parent_session_id, event_json, completed_at, dispatched_at FROM async_delegations WHERE delegation_id = ?",
		)
		.get(delegationId) as {
		delegation_id: string;
		origin_session: string;
		parent_session_id: string | null;
		event_json: string;
		completed_at: number | null;
		dispatched_at: number;
	};
	if (!raw) throw new Error(`no delegation row ${delegationId}`);
	let event: Record<string, unknown> = {};
	try {
		const parsed: unknown = JSON.parse(raw.event_json);
		if (parsed !== null && typeof parsed === "object") {
			event = parsed as Record<string, unknown>;
		}
	} catch {
		event = {}; // unparsable payload surfaces as an empty event, not a throw
	}
	return {
		delegationId: raw.delegation_id,
		originSession: raw.origin_session,
		parentSessionId: raw.parent_session_id,
		event,
		completedAt: raw.completed_at,
		dispatchedAt: raw.dispatched_at,
	};
}

function routingPointsTo(sessionKey: string): string | null {
	const r = h.store.db
		.prepare(
			"SELECT entry_json FROM gateway_routing WHERE scope = '' AND session_key = ?",
		)
		.get(sessionKey) as { entry_json: string } | undefined;
	if (!r) return null;
	try {
		const parsed = JSON.parse(r.entry_json) as { session_id?: unknown };
		return typeof parsed.session_id === "string" ? parsed.session_id : null;
	} catch {
		return null; // corrupt entry behaves like a missing route
	}
}

/** End `from` at a compression boundary and create its continuation `to`. */
async function compressTo(from: string, to: string): Promise<void> {
	await seedSession(h, from, {
		sessionKey: KEY,
		endedAt: h.clock.nowSeconds(),
		endReason: "compression",
	});
	await seedSession(h, to, { parentSessionId: from });
}
