// Behavior contracts: DEC-020 ConversationState boundary registry +
// context-local identity; DEC-021 agent-cache LRU + memory-pressure bound;
// per-turn checkpoint dedup ledger.

import { describe, expect, it } from "vitest";

import { AgentInstanceCache } from "./agent-cache.js";
import { TurnCheckpointLedger } from "./checkpoints.js";
import {
	CONVERSATION_STATE_FIELDS,
	ConversationState,
	conversationStateShapeViolations,
	currentConversation,
	requireConversation,
	runWithConversation,
} from "./conversation-state.js";

describe("ConversationState — DEC-020", () => {
	it("every instance field is registered in the boundary registry and vice versa", () => {
		const violations = conversationStateShapeViolations();
		expect(violations).toEqual([]);
		expect(CONVERSATION_STATE_FIELDS.length).toBeGreaterThanOrEqual(9);
	});

	it("context identity: state visible inside runWithConversation, absent outside", () => {
		const outside = currentConversation();
		expect(outside).toBeUndefined();
		const state = new ConversationState("s1", { routingKey: "rk" });
		runWithConversation(state, () => {
			expect(currentConversation()?.sessionId).toBe("s1");
			expect(requireConversation().routingKey).toBe("rk");
		});
		expect(currentConversation()).toBeUndefined();
		expect(() => requireConversation()).toThrow(/no ConversationState/);
	});

	it("interleaved async turns have ZERO scoped-field cross-talk", async () => {
		const a = new ConversationState("chat-A", { routingKey: "rk-a" });
		const b = new ConversationState("chat-B", { routingKey: "rk-b" });
		const observed: string[] = [];

		const turn = async (
			state: ConversationState,
			marker: string,
			yieldFirst: boolean,
		) => {
			return runWithConversation(state, async () => {
				if (yieldFirst) await new Promise((r) => setTimeout(r, 5));
				const seen = requireConversation();
				observed.push(`${marker}:${seen.sessionId}`);
				// Mutations stay scoped to THIS context even after awaits.
				seen.iterations += 2;
				await new Promise((r) => setTimeout(r, 1));
				requireConversation().extPrefetchCache = `prefetch-${marker}`;
				await new Promise((r) => setTimeout(r, 1));
				return {
					sessionId: requireConversation().sessionId,
					iterations: requireConversation().iterations,
					prefetch: requireConversation().extPrefetchCache,
				};
			});
		};

		const [ra, rb] = await Promise.all([
			turn(a, "A", false),
			turn(b, "B", true),
		]);
		expect(ra.sessionId).toBe("chat-A");
		expect(ra.iterations).toBe(2);
		expect(ra.prefetch).toBe("prefetch-A");
		expect(rb.sessionId).toBe("chat-B");
		expect(rb.prefetch).toBe("prefetch-B");
		for (const line of observed) {
			if (line.startsWith("A:")) expect(line).toBe("A:chat-A");
			if (line.startsWith("B:")) expect(line).toBe("B:chat-B");
		}
	});
});

describe("AgentInstanceCache — DEC-021", () => {
	it("LRU entry cap evicts least-recently-used first", () => {
		let t = 1000;
		const clock = () => ++t * 1000;
		const cache = new AgentInstanceCache<string>({ maxEntries: 2, now: clock });
		cache.set("a", "va", 10);
		cache.set("b", "vb", 10);
		cache.set("c", "vc", 10);
		expect(cache.keys().sort()).toEqual(["b", "c"]);
		expect(cache.get("a")).toBeUndefined();
		// Touch b, insert d → b survives, c goes.
		cache.get("b");
		cache.set("d", "vd", 10);
		expect(cache.keys().sort()).toEqual(["b", "d"]);
	});

	it("byte-pressure bound sheds LRU entries until under budget (synthetic RSS accounting)", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxEntries: 64,
			maxTotalBytes: 100,
			now: () => ++t,
		});
		cache.set("warm-1", "v1", 60);
		cache.set("warm-2", "v2", 30); // total 90 ≤ 100
		expect(cache.size).toBe(2);
		cache.set("big-3", "v3", 80); // pushes over → shed oldest
		expect(cache.keys()).toEqual(["big-3"]);
		expect(cache.totalBytes).toBe(80);
		// An entry larger than the whole budget is kept (no thrash on every op).
		cache.set("huge", "vh", 500);
		expect(cache.has("huge")).toBe(true);
		expect(cache.has("big-3")).toBe(false);
	});

	it("idle sweep drops entries past TTL using the injected clock", () => {
		let now = 10_000;
		const cache = new AgentInstanceCache<string>({
			idleTtlMs: 1000,
			now: () => now,
		});
		cache.set("old", "v", 5);
		now = 10_500;
		cache.set("new", "w", 5);
		now = 11_200;
		const evicted = cache.sweepIdle();
		expect(evicted).toEqual(["old"]);
		expect(cache.keys()).toEqual(["new"]);
	});

	it("set() over an existing key refreshes bytes without double counting", () => {
		const cache = new AgentInstanceCache<string>({ maxTotalBytes: 1000 });
		cache.set("k", "v1", 50);
		expect(cache.totalBytes).toBe(50);
		cache.set("k", "v2", 90);
		expect(cache.totalBytes).toBe(90);
		expect(cache.size).toBe(1);
	});
});

describe("TurnCheckpointLedger — checkpoint dedup (05 §4)", () => {
	it("duplicate payloads within one turn are recorded exactly once", () => {
		const ledger = new TurnCheckpointLedger();
		ledger.newTurn();
		expect(ledger.record("iter:1:start")).toBe(true);
		expect(ledger.record("iter:1:end")).toBe(true);
		expect(ledger.record("iter:1:start")).toBe(false); // duplicate
		const counts = ledger.counts();
		expect(counts).toMatchObject({ recorded: 2, duplicates: 1, distinct: 2 });
	});

	it("newTurn resets dedup so the next turn records the same payloads again", () => {
		const ledger = new TurnCheckpointLedger();
		ledger.newTurn();
		expect(ledger.record("snapshot")).toBe(true);
		ledger.newTurn();
		expect(ledger.record("snapshot")).toBe(true);
		expect(ledger.counts().turn).toBe(2);
		expect(ledger.counts().distinct).toBe(1);
	});
});
