// Behavior contracts: DEC-020 ConversationState boundary registry +
// context-local identity; per-turn checkpoint dedup ledger.
// DEC-021 agent-cache dissolved per DEC-084 (RPC children own sessions).

import { describe, expect, it } from "vitest";
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
