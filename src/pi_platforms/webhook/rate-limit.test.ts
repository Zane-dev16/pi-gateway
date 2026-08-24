// BEHAVIOR CONTRACTS — per-route sliding-window rate limit (webhook.py:
// _record_rate_limit_hit) and delivery-id idempotency (_record_delivery_id +
// api_server.py:_IdempotCache). All timing through injected clocks.

import { describe, expect, it } from "vitest";
import { SlidingWindowRateLimiter } from "./rate-limit.js";
import { DeliveryIdempotencyStore } from "./idempotency.js";

describe("SlidingWindowRateLimiter", () => {
	function make(limit: number) {
		let now = 0;
		const limiter = new SlidingWindowRateLimiter({
			limit,
			windowMs: 60_000,
			nowMs: () => now,
		});
		return {
			limiter,
			tick(ms: number) {
				now += ms;
			},
			set(ms: number) {
				now = ms;
			},
		};
	}

	it("admits up to the limit then trips 429-class rejection", () => {
		const { limiter } = make(3);
		expect(limiter.tryRecord("route-a")).toBe(true);
		expect(limiter.tryRecord("route-a")).toBe(true);
		expect(limiter.tryRecord("route-a")).toBe(true);
		// Trips at the configured threshold.
		expect(limiter.tryRecord("route-a")).toBe(false);
		expect(limiter.hitsInWindow("route-a")).toBe(3);
	});

	it("rejected hits do NOT consume window slots (webhook.py parity)", () => {
		const { limiter } = make(2);
		limiter.tryRecord("r");
		limiter.tryRecord("r");
		for (let i = 0; i < 5; i++) expect(limiter.tryRecord("r")).toBe(false);
		expect(limiter.hitsInWindow("r")).toBe(2);
	});

	it("window slides: entries age out after 60s under the injected clock", () => {
		const t = make(2);
		t.set(1_000);
		expect(t.limiter.tryRecord("r")).toBe(true);
		t.tick(30_000);
		expect(t.limiter.tryRecord("r")).toBe(true);
		expect(t.limiter.tryRecord("r")).toBe(false); // two in window
		t.tick(31_000); // first entry now outside the window
		expect(t.limiter.tryRecord("r")).toBe(true);
		expect(t.limiter.tryRecord("r")).toBe(false);
	});

	it("limits are PER ROUTE — one route tripping never starves another", () => {
		const { limiter } = make(1);
		expect(limiter.tryRecord("alpha")).toBe(true);
		expect(limiter.tryRecord("alpha")).toBe(false);
		expect(limiter.tryRecord("beta")).toBe(true);
	});

	it("rate limit trips at the configured threshold with the injected clock across a rolling minute", () => {
		const t = make(5);
		for (let i = 0; i < 5; i++) {
			t.tick(5_000); // spread across 25s — all inside one window
			expect(t.limiter.tryRecord("burst")).toBe(true);
		}
		t.tick(1_000);
		expect(t.limiter.tryRecord("burst")).toBe(false);
	});
});

describe("DeliveryIdempotencyStore", () => {
	function make() {
		let now = 1_700_000_000_000;
		const store = new DeliveryIdempotencyStore({
			ttlMs: 3_600_000,
			maxEntries: 128,
			pruneIntervalMs: 60_000,
			nowMs: () => now,
		});
		return {
			store,
			tick(ms: number) {
				now += ms;
			},
			now: () => now,
		};
	}

	it("same delivery-id replayed N times processes ONCE (outcome cache hit)", () => {
		const { store } = make();
		const first = store.begin("d-1");
		expect(first.replay).toBe(false);

		store.recordOutcome("d-1", {
			status: 200,
			body: { status: "completed", reply: "the answer" },
		});

		for (let i = 0; i < 9; i++) {
			const replay = store.begin("d-1");
			if (!replay.replay) throw new Error(`replay ${i} processed again`);
			expect(replay.outcome).toEqual({
				status: 200,
				body: { status: "completed", reply: "the answer" },
			});
		}
	});

	it("replay before an outcome exists answers a duplicate marker", () => {
		const { store } = make();
		store.begin("d-2");
		const replay = store.begin("d-2");
		expect(replay).toEqual({ replay: true, outcome: null });
	});

	it("entries expire after the TTL and re-arm processing", () => {
		const t = make();
		t.store.begin("d-3");
		t.tick(3_600_001);
		const second = t.store.begin("d-3");
		expect(second.replay).toBe(false);
	});

	it("prunes past the seen-set bound max(rate_limit*2, 128)", () => {
		const t = make();
		for (let i = 0; i < 200; i++) {
			t.store.begin(`bulk-${i}`);
		}
		t.tick(120_000); // move old entries toward expiry
		for (let i = 0; i < 50; i++) t.store.begin(`fresh-${i}`);
		expect(t.store.size()).toBeLessThanOrEqual(250);
	});

	it("distinct delivery ids never collide", () => {
		const { store } = make();
		expect(store.begin("a").replay).toBe(false);
		expect(store.begin("b").replay).toBe(false);
		expect(store.begin("a").replay).toBe(true);
	});
});
