// Behavior contracts: pressure shedding NEVER evicts the wrong cached agent
// (stability round secops-13). Hermes gates every soft eviction — gateway/
// run.py:_is_evictable (mid-turn exclusion + transcript-persistence-caught-up)
// for the byte-pressure path, _enforce_agent_cache_cap's running-agents skip
// for the LRU-entry-cap path — because "a skipped eviction costs memory, a
// wrong one costs the user their conversation." These specs drive the REAL
// host loop through GatewayAgentRunner and observe survival/eviction via the
// public diagnostics surface (isCached/cacheStats), never internals.
//
// Byte accounting note: estimateSessionBytes floors every cached host session
// at a 4KiB scaffolding baseline, so maxTotalBytes: 1 arms DEC-021 pressure
// shedding against EVERY cached entry deterministically.

import { describe, expect, it, vi } from "vitest";

import type { AssistantMessage } from "./host.js";
import { createRunnerHarness } from "./testing/runner-harness.js";
import { fauxAssistantMessage } from "./testing/faux-model.js";

describe("pressure shedding evictability — run.py:_is_evictable parity", () => {
	it("an idled session whose turn fully persisted IS shed once over budget", async () => {
		const h = await createRunnerHarness({
			cacheOptions: { maxTotalBytes: 1 },
		});
		try {
			h.ensureSession("settled");
			h.faux.setResponses([fauxAssistantMessage("done")]);
			const out = await h.runner.handleTurn({
				sessionId: "settled",
				routingKey: "rk",
				text: "hi",
			});
			expect(out.exitReason).toBe("finalized");
			expect(h.runner.isCached("settled")).toBe(true);

			// A second session's insert trips the byte bound; the protected-tail
			// clamp shields only the MRU entrant, so "settled" sheds.
			h.ensureSession("entrant");
			h.faux.appendResponses([fauxAssistantMessage("b")]);
			await h.runner.handleTurn({
				sessionId: "entrant",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("settled")).toBe(false);
			expect(h.runner.isCached("entrant")).toBe(true);
			expect(h.runner.cacheStats.entries).toBe(1);
		} finally {
			await h.close();
		}
	});

	it("a FAILED turn leaves persistence uncaught-up ⇒ the pass refuses to shed it", async () => {
		const h = await createRunnerHarness({
			cacheOptions: { maxTotalBytes: 1 },
		});
		try {
			h.ensureSession("wounded");
			// Errored cycle: no final payload ⇒ NO assistant row ever lands ⇒
			// the flush-through marker stays unknown (turn-start reset parity).
			h.faux.setResponses([
				fauxAssistantMessage("partial", {
					stopReason: "error",
					errorMessage: "provider exploded",
				}),
			]);
			const out = await h.runner.handleTurn({
				sessionId: "wounded",
				routingKey: "rk",
				text: "hi",
			});
			expect(out.exitReason).toBe("error");
			// Only the user row persisted — the live transcript is ahead of disk.
			expect(h.store.listMessages("wounded").map((r) => r.role)).toEqual([
				"user",
			]);
			expect(h.runner.isCached("wounded")).toBe(true);

			// Over-budget pressure arrives (second insert) — the wounded entry
			// SURVIVES: rebuilding it from durable rows would drop the undelivered
			// tail. The cache tolerates staying over bound instead.
			h.ensureSession("entrant");
			h.faux.appendResponses([fauxAssistantMessage("b")]);
			await h.runner.handleTurn({
				sessionId: "entrant",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("wounded")).toBe(true);
			expect(h.runner.isCached("entrant")).toBe(true);
			expect(h.runner.cacheStats.entries).toBe(2); // over budget, nothing shed
		} finally {
			await h.close();
		}
	});

	it("a MID-TURN session is never shed — protection lifts exactly when its turn persists", async () => {
		const h = await createRunnerHarness({
			cacheOptions: { maxTotalBytes: 1 },
		});
		try {
			h.ensureSession("turn-a");
			let releaseA!: (m: AssistantMessage) => void;
			h.faux.setResponses([
				() =>
					new Promise<AssistantMessage>((resolve) => {
						releaseA = resolve;
					}),
			]);
			const pendingA = h.runner.handleTurn({
				sessionId: "turn-a",
				routingKey: "rk",
				text: "hold",
			});
			await vi.waitFor(() =>
				expect(h.runner.isTurnActive("turn-a")).toBe(true),
			);

			// While A holds a worker slot, an entrant trips the byte bound. A is
			// the coldest candidate and would be shed positionally — instead it
			// survives untouched.
			h.ensureSession("turn-b");
			h.faux.appendResponses([fauxAssistantMessage("b")]);
			await h.runner.handleTurn({
				sessionId: "turn-b",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("turn-a")).toBe(true);
			expect(h.runner.isCached("turn-b")).toBe(true);

			// Release A: the turn finalizes, its assistant row lands durably, and
			// the NEXT pressure pass may take it (and b) — never before.
			releaseA(fauxAssistantMessage("late"));
			const outA = await pendingA;
			expect(outA.exitReason).toBe("finalized");
			expect(outA.finalText).toBe("late");

			h.ensureSession("turn-c");
			h.faux.appendResponses([fauxAssistantMessage("c")]);
			await h.runner.handleTurn({
				sessionId: "turn-c",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("turn-a")).toBe(false); // safe now
			expect(h.runner.isCached("turn-b")).toBe(false);
			expect(h.runner.isCached("turn-c")).toBe(true);
		} finally {
			await h.close();
		}
	});
});

describe("entry-cap eviction skips mid-turn holders — _enforce_agent_cache_cap parity", () => {
	it("a held slot keeps its cache entry without substituting newer ones", async () => {
		const h = await createRunnerHarness({
			cacheOptions: {
				maxEntries: 1,
				maxTotalBytes: Number.MAX_SAFE_INTEGER,
			},
		});
		try {
			h.ensureSession("held");
			let releaseHeld!: (m: AssistantMessage) => void;
			h.faux.setResponses([
				() =>
					new Promise<AssistantMessage>((resolve) => {
						releaseHeld = resolve;
					}),
			]);
			const pendingHeld = h.runner.handleTurn({
				sessionId: "held",
				routingKey: "rk",
				text: "hold",
			});
			await vi.waitFor(() => expect(h.runner.isTurnActive("held")).toBe(true));

			// The second insert exceeds the 1-slot cap; the only eviction-window
			// candidate is the mid-turn holder ⇒ cache stays over cap instead.
			h.ensureSession("entrant");
			h.faux.appendResponses([fauxAssistantMessage("b")]);
			await h.runner.handleTurn({
				sessionId: "entrant",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("held")).toBe(true);
			expect(h.runner.isCached("entrant")).toBe(true);
			expect(h.runner.cacheStats.entries).toBe(2);

			// Once held finishes, the next insert re-checks the cap and sheds the
			// now-idle holders (mid-turn was the ONLY thing shielding them).
			releaseHeld(fauxAssistantMessage("late"));
			const out = await pendingHeld;
			expect(out.exitReason).toBe("finalized");

			h.ensureSession("third");
			h.faux.appendResponses([fauxAssistantMessage("c")]);
			await h.runner.handleTurn({
				sessionId: "third",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.isCached("held")).toBe(false);
			expect(h.runner.isCached("entrant")).toBe(false);
			expect(h.runner.isCached("third")).toBe(true);
			expect(h.runner.cacheStats.entries).toBe(1);
		} finally {
			await h.close();
		}
	});
});
