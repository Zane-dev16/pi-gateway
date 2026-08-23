// Behavior contracts for the in-process turn lease (03 §7, DEC-004 layer 1).
// Generation-scoped release is THE race contract: a stale unwind carrying an
// old token must never free a newer turn's lease.

import { describe, expect, it } from "vitest";
import {
	SessionTurnLeaseRegistry,
	TurnLeaseTimeoutError,
	type AcquireOptions,
	type TurnLeaseToken,
} from "./turn-lease.js";

async function mustAcquire(
	registry: SessionTurnLeaseRegistry,
	sessionId: string,
	options: AcquireOptions,
): Promise<TurnLeaseToken> {
	const token = await registry.acquire(sessionId, options);
	if (token === null) {
		throw new Error(`expected lease grant for session ${sessionId}`);
	}
	return token;
}

function holderOf(registry: SessionTurnLeaseRegistry, sid: string) {
	return registry.holderOf(sid);
}

describe("in-process turn lease registry (layer 1, DEC-004)", () => {
	it("generation check: stale unwind carrying an old token cannot release a newer lease", async () => {
		const registry = new SessionTurnLeaseRegistry();
		const stale = await mustAcquire(registry, "sess", {
			ownerKey: "k-old",
			generation: 7,
		});
		expect(stale.generation).toBe(7);
		expect(registry.release(stale)).toBe(true);

		// A newer turn takes over the same session…
		const fresh = await mustAcquire(registry, "sess", {
			ownerKey: "k-new",
			generation: 8,
		});
		// …and the OLD holder's late unwind replays release with its stale token:
		expect(registry.release(stale)).toBe(false);
		expect(holderOf(registry, "sess")).toBe(fresh);

		// The newer lease is still exclusive — a third acquirer fails closed.
		await expect(
			registry.acquire("sess", {
				ownerKey: "k-third",
				generation: 9,
				timeoutMs: 80,
			}),
		).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		expect(holderOf(registry, "sess")).toBe(fresh);
		expect(holderOf(registry, "sess")?.generation).toBe(8);

		// Release is idempotent and ownership-checked.
		expect(registry.release(fresh)).toBe(true);
		expect(registry.release(fresh)).toBe(false);
		expect(holderOf(registry, "sess")).toBeNull();
	});

	it("timed-out waiter fails closed while FIFO waiter acquires after release; contention reported", async () => {
		const contended: string[] = [];
		const registry = new SessionTurnLeaseRegistry({
			onContended: (info) =>
				contended.push(`${info.waitingOwnerKey}<-${info.holderOwnerKey}`),
		});
		const holder = await mustAcquire(registry, "s", {
			ownerKey: "h0",
			generation: 1,
		});

		let grantedOrder: string[] = [];
		const slowWaiterP = registry
			.acquire("s", { ownerKey: "w-slow", generation: 2 })
			.then((t) => {
				grantedOrder = ["slow"];
				if (!t) throw new Error("expected slow waiter grant");
				return t;
			});
		const impatientP = registry.acquire("s", {
			ownerKey: "w-fast",
			generation: 3,
			timeoutMs: 100,
		});

		await expect(impatientP).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		// Both waiters saw the contention warning; neither produced a token.
		expect(contended).toEqual(["w-slow<-h0", "w-fast<-h0"]);

		registry.release(holder);
		const slowToken = await slowWaiterP;
		expect(grantedOrder).toEqual(["slow"]); // FIFO: first waiter wins
		expect(slowToken.ownerKey).toBe("w-slow");

		// Old holder's second release is a no-op once ownership moved on.
		expect(registry.release(holder)).toBe(false);
		expect(registry.release(slowToken)).toBe(true);
	});

	it("bounded registry: eviction drops only idle entries, never a live or contended lease", async () => {
		const registry = new SessionTurnLeaseRegistry({ maxEntries: 3 });
		const live = await mustAcquire(registry, "live", {
			ownerKey: "keep",
			generation: 1,
		});
		for (const sid of ["a", "b", "c", "d", "e"]) {
			const t = await mustAcquire(registry, sid, {
				ownerKey: sid,
				generation: 1,
			});
			registry.release(t);
		}
		// Cap holds (one transient over-cap allowed during insert), and
		// correctness beats the cap: the LIVE lease survives eviction.
		expect(registry.size).toBeLessThanOrEqual(4);
		expect(holderOf(registry, "live")).toBe(live);
	});

	it("rebind aliases a HELD lease onto the rotated session id; blocked when target is live", async () => {
		const registry = new SessionTurnLeaseRegistry();

		const t = await mustAcquire(registry, "old-id", {
			ownerKey: "turn",
			generation: 4,
		});
		expect(registry.rebind(t, "new-id")).toBe(true);
		expect(holderOf(registry, "new-id")).toBe(t); // same slot under both ids…
		expect(holderOf(registry, "old-id")).toBe(t); // …rotation never orphans serialization
		await expect(
			registry.acquire("new-id", {
				ownerKey: "alias-key",
				generation: 5,
				timeoutMs: 60,
			}),
		).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		expect(registry.release(t)).toBe(true);
		expect(holderOf(registry, "new-id")).toBeNull();

		// Rebind onto a LIVE target id is blocked.
		const ta = await mustAcquire(registry, "a", {
			ownerKey: "a",
			generation: 1,
		});
		const tb = await mustAcquire(registry, "b", {
			ownerKey: "b",
			generation: 1,
		});
		expect(registry.rebind(ta, "b")).toBe(false);
		registry.release(ta);
		registry.release(tb);
	});

	it("falsy session id never grants a token (parity of acquire(None) → None)", async () => {
		const registry = new SessionTurnLeaseRegistry();
		expect(
			await registry.acquire("", { ownerKey: "x", generation: 1 }),
		).toBeNull();
	});
});
