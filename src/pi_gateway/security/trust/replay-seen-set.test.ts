// Behavior contracts for the bounded seen-set (msgraph receipt dedupe +
// webhook idempotency shape): replay rejection, FIFO bound under adversarial
// churn (msgraph_webhook.py:_remember_receipt parity), TTL expiry re-arm.

import { describe, expect, it } from "vitest";
import { BoundedSeenSet } from "./replay-seen-set.js";

describe("BoundedSeenSet — bounded memory + replay rejection", () => {
	it("first add admits; immediate re-add rejects as REPLAY", () => {
		const nowMs = 1_000;
		const set = new BoundedSeenSet({ maxEntries: 10, nowMs: () => nowMs });
		expect(set.add("receipt-1")).toBe(true);
		expect(set.add("receipt-1")).toBe(false); // live duplicate
		expect(set.add("receipt-2")).toBe(true);
	});

	it("FIFO eviction keeps size <= maxEntries under unlimited key churn", () => {
		let nowMs = 0;
		const set = new BoundedSeenSet({ maxEntries: 100, nowMs: () => nowMs });
		for (let i = 0; i < 5_000; i++) {
			set.add(`key-${i}`);
			nowMs += 1;
		}
		expect(set.size()).toBe(100);
		// The OLDEST keys were evicted: key-0 is unseen again, key-4999 live.
		expect(set.has("key-0")).toBe(false);
		expect(set.has("key-4999")).toBe(true);
	});

	it("evicted entries RE-ARM: a replay after wrap-around is admitted again", () => {
		const nowMs = 0;
		const set = new BoundedSeenSet({
			maxEntries: 3,
			ttlMs: null,
			nowMs: () => nowMs,
		});
		set.add("r");
		set.add("a");
		set.add("b");
		set.add("c"); // evicts "r"
		expect(set.has("r")).toBe(false);
		expect(set.add("r")).toBe(true); // re-arm admitted
	});

	it("TTL expiry: expired entries stop matching and are pruned lazily", () => {
		let nowMs = 1_000_000;
		const set = new BoundedSeenSet({
			maxEntries: 100,
			ttlMs: 3_600_000,
			nowMs: () => nowMs,
		});
		set.add("delivery-x");
		nowMs += 3_599_999; // just inside
		expect(set.has("delivery-x")).toBe(true);
		expect(set.add("delivery-x")).toBe(false);
		nowMs += 1; // past TTL
		expect(set.has("delivery-x")).toBe(false);
		expect(set.add("delivery-x")).toBe(true); // expired entry re-arms
		expect(set.pruneExpired()).toBe(0); // already replaced on re-arm
	});

	it("pruneExpired removes only expired entries and reports the count", () => {
		let nowMs = 0;
		const set = new BoundedSeenSet({
			maxEntries: 50,
			ttlMs: 1_000,
			nowMs: () => nowMs,
		});
		set.add("old-1");
		set.add("old-2");
		nowMs += 2_000;
		set.add("fresh");
		expect(set.pruneExpired()).toBe(2);
		expect(set.size()).toBe(1);
		expect(set.has("fresh")).toBe(true);
	});

	it("default bound matches DEFAULT_MAX_SEEN_ENTRIES=5000 (msgraph cap)", () => {
		let nowMs = 0;
		const set = new BoundedSeenSet({ nowMs: () => nowMs });
		for (let i = 0; i < 6_000; i++) {
			set.add(`n${i}`);
			nowMs += 1;
		}
		expect(set.size()).toBe(5_000);
	});
});
