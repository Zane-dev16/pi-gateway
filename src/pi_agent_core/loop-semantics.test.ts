// Behavior contracts over the REAL host loop: grace-call semantics, interrupt
// latency, steer drain placement, prefetch-once-per-turn, interrupted-turn
// memory silence (#15218), and the trivial-prompt gates.

import { describe, expect, it } from "vitest";

import { Type, defineTool } from "./host.js";
import type { Context } from "./host.js";
import { createRunnerHarness } from "./testing/runner-harness.js";
import { fauxAssistantMessage, fauxToolCall } from "./testing/faux-model.js";

/** A tool whose execution is released by an external gate. */
function gatedTool(name = "gate_tool") {
	let release: () => void = () => {};
	const gate = new Promise<void>((r) => {
		release = r;
	});
	let markEntered: () => void = () => {};
	const entered = new Promise<void>((r) => {
		markEntered = r;
	});
	const tool = defineTool({
		name,
		label: name,
		description: "blocks until released",
		parameters: Type.Object({}),
		execute: async (_id, _params, signal) => {
			markEntered();
			await Promise.race([
				gate,
				new Promise<void>((resolve) => {
					signal?.addEventListener("abort", () => resolve(), {
						once: true,
					});
				}),
			]);
			return {
				content: [{ type: "text", text: "gate released" }],
				details: {},
			};
		},
	});
	return { tool, release: () => release(), entered };
}

describe("budget + grace semantics (05 §4.1)", () => {
	it("maxIterations=0 still executes exactly ONE model call, then exits budget_exhausted", async () => {
		const h = await createRunnerHarness({ maxIterations: 0 });
		try {
			h.ensureSession("grace-0");
			// Model always wants another iteration (tool calls).
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t1" })]),
				fauxAssistantMessage("never reached"),
				fauxAssistantMessage("never reached 2"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "grace-0",
				routingKey: "rk",
				text: "go",
			});
			expect(outcome.exitReason).toBe("budget_exhausted");
			// Exactly ONE completed model call (the grace call); the cut-off
			// request never completes, so it does not count as an iteration.
			expect(outcome.iterations).toBe(1);
		} finally {
			await h.close();
		}
	});

	it("exhaustion grants exactly ONE grace call (max=2 ⇒ at most 3 calls), then stops", async () => {
		const h = await createRunnerHarness({ maxIterations: 2 });
		try {
			h.ensureSession("grace-2");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t1" })]),
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t2" })]),
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t3" })]),
				fauxAssistantMessage("never reached"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "grace-2",
				routingKey: "rk",
				text: "go",
			});
			expect(outcome.exitReason).toBe("budget_exhausted");
			expect(outcome.iterations).toBe(3); // max + the single grace call
			// The 4th (not-allowed) call was killed before completing.
			expect(h.faux.getPendingResponseCount()).toBe(0);
		} finally {
			await h.close();
		}
	});

	it("a turn finishing naturally AT the budget finalizes instead of exhausting", async () => {
		const h = await createRunnerHarness({ maxIterations: 2 });
		try {
			h.ensureSession("grace-ok");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t1" })]),
				fauxAssistantMessage("done on time"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "grace-ok",
				routingKey: "rk",
				text: "go",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("done on time");
			expect(outcome.iterations).toBe(2);
		} finally {
			await h.close();
		}
	});

	it("the grace call DELIVERING final text records finalized, not exhausted", async () => {
		const h = await createRunnerHarness({ maxIterations: 1 });
		try {
			h.ensureSession("grace-final");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "t1" })]),
				fauxAssistantMessage("grace delivered the answer"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "grace-final",
				routingKey: "rk",
				text: "go",
			});
			expect(h.faux.state.callCount).toBe(2); // normal + grace
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("grace delivered the answer");
		} finally {
			await h.close();
		}
	});
});

describe("interrupt latency + memory silence", () => {
	it("/stop during a slow tool breaks the turn within a wall-clock bound", async () => {
		let syncCalls = 0;
		let warmCalls = 0;
		const gated = gatedTool();
		const h = await createRunnerHarness({
			customTools: [gated.tool],
			memoryHooks: {
				prefetchAll: async () => "ctx",
				syncAll: () => {
					syncCalls += 1;
				},
				queuePrefetchAll: () => {
					warmCalls += 1;
				},
			},
		});
		try {
			h.ensureSession("stop-sess");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("gate_tool", {}, { id: "g1" })]),
				fauxAssistantMessage("unreachable"),
			]);

			const turnPromise = h.runner.handleTurn({
				sessionId: "stop-sess",
				routingKey: "rk",
				text: "long task",
			});
			await gated.entered; // tool executing now

			const t0 = Date.now();
			const stopped = await h.runner.interrupt("stop-sess");
			expect(stopped).toBe(true);
			await turnPromise;
			const latencyMs = Date.now() - t0;

			expect(latencyMs).toBeLessThan(2000); // wall-clock bound ≥2s headroom
			// Interrupted turns are memory-SILENT (#15218): neither sync nor warm.
			expect(syncCalls).toBe(0);
			expect(warmCalls).toBe(0);
		} finally {
			await h.close();
		}
	});
});

describe("steer drain placement (05 §8)", () => {
	it("redirect queued mid-tool lands after the tool result, before the next model call", async () => {
		const gated = gatedTool();
		const h = await createRunnerHarness({
			maxIterations: 5,
			customTools: [gated.tool],
		});
		try {
			h.ensureSession("steer-sess");
			const secondCallMessages: Array<{ role: string; text: string }> = [];
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("gate_tool", {}, { id: "g1" })]),
				(context: Context) => {
					const textOf = (content: unknown): string => {
						if (typeof content === "string") return content;
						return (content as Array<{ type: string; text?: string }>)
							.filter((b) => b.type === "text")
							.map((b) => b.text ?? "")
							.join("");
					};
					for (const m of context.messages) {
						if (m.role === "user") {
							secondCallMessages.push({
								role: m.role,
								text: textOf(m.content),
							});
						} else if (m.role === "toolResult") {
							secondCallMessages.push({ role: m.role, text: "" });
						}
					}
					return fauxAssistantMessage("redirected answer");
				},
			]);

			const turnPromise = h.runner.handleTurn({
				sessionId: "steer-sess",
				routingKey: "rk",
				text: "original instruction",
			});
			await gated.entered;
			await h.runner.steer("steer-sess", "mid-flight redirect");
			gated.release();
			const outcome = await turnPromise;

			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("redirected answer");
			// History order preserved: original user … toolResult … THEN the
			// steered user message — applied at the loop top, never spliced.
			const roles = secondCallMessages.map((m) => m.role);
			expect(roles).toEqual(["user", "toolResult", "user"]);
			expect(secondCallMessages[0]!.text).toBe("original instruction");
			expect(secondCallMessages[2]!.text).toContain("mid-flight redirect");
		} finally {
			await h.close();
		}
	});
});

describe("memory turn-boundary contract (05 §4.2)", () => {
	function spyHooks(trivial = false) {
		const calls = { prefetch: 0, sync: 0, warm: 0, queries: [] as string[] };
		return {
			calls,
			hooks: {
				prefetchAll: async (q: string) => {
					calls.prefetch += 1;
					calls.queries.push(q);
					return `ctx:${q}`;
				},
				syncAll: () => {
					calls.sync += 1;
				},
				queuePrefetchAll: (q: string) => {
					calls.warm += 1;
					void q;
				},
				isTrivialPrompt: () => trivial,
			},
		};
	}

	it("N loop iterations in one turn ⇒ exactly ONE prefetchAll", async () => {
		const spy = spyHooks();
		const h = await createRunnerHarness({ memoryHooks: spy.hooks });
		try {
			h.ensureSession("mem-once");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "m1" })]),
				fauxAssistantMessage([fauxToolCall("x", {}, { id: "m2" })]),
				fauxAssistantMessage("finished"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "mem-once",
				routingKey: "rk",
				text: "research thing",
			});
			expect(outcome.iterations).toBe(3);
			expect(spy.calls.prefetch).toBe(1);
			expect(spy.calls.sync).toBe(1);
			expect(spy.calls.warm).toBe(1);
			expect(spy.calls.queries).toEqual(["research thing"]);
		} finally {
			await h.close();
		}
	});

	it("trivial prompt skips prefetch AND warming but still syncs", async () => {
		const spy = spyHooks(true);
		const h = await createRunnerHarness({ memoryHooks: spy.hooks });
		try {
			h.ensureSession("mem-trivial");
			h.faux.setResponses([fauxAssistantMessage("hi!")]);
			await h.runner.handleTurn({
				sessionId: "mem-trivial",
				routingKey: "rk",
				text: "ok thanks",
			});
			expect(spy.calls.prefetch).toBe(0);
			expect(spy.calls.warm).toBe(0);
			expect(spy.calls.sync).toBe(1);
		} finally {
			await h.close();
		}
	});

	it("empty user text or empty response skips BOTH legs", async () => {
		const spy = spyHooks();
		const h = await createRunnerHarness({ memoryHooks: spy.hooks });
		try {
			h.ensureSession("mem-empty");
			h.faux.setResponses([fauxAssistantMessage([])]); // no content blocks
			await h.runner.handleTurn({
				sessionId: "mem-empty",
				routingKey: "rk",
				text: "silent please",
			});
			expect(spy.calls.prefetch).toBe(1); // prefetch leg ran pre-loop
			expect(spy.calls.sync).toBe(0); // empty response → skip
			expect(spy.calls.warm).toBe(0);
		} finally {
			await h.close();
		}
	});
});
