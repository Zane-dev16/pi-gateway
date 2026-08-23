// Runner-level worker-pool integration: concurrent turns across sessions
// respect the bounded pool; >N demands never exceed N executing.

import { describe, expect, it } from "vitest";

import { Type, defineTool } from "./host.js";
import { createRunnerHarness } from "./testing/runner-harness.js";
import { fauxAssistantMessage, fauxToolCall } from "./testing/faux-model.js";

describe("runner × worker pool", () => {
	it("concurrent handleTurn demands never exceed the configured workers", async () => {
		let active = 0;
		let maxActive = 0;
		let released = false;
		const waiters: Array<() => void> = [];
		const releaseAll = () => {
			released = true;
			for (const w of waiters.splice(0)) w();
		};
		const slowTool = defineTool({
			name: "slow",
			label: "slow",
			description: "tracks concurrency",
			parameters: Type.Object({}),
			execute: async () => {
				active += 1;
				maxActive = Math.max(maxActive, active);
				await new Promise<void>((resolve) => {
					if (released) resolve();
					else waiters.push(resolve);
				});
				active -= 1;
				return { content: [{ type: "text", text: "done" }], details: {} };
			},
		});
		const h = await createRunnerHarness({
			poolMaxWorkers: 2,
			customTools: [slowTool],
		});
		try {
			for (const sid of ["p1", "p2", "p3"]) h.ensureSession(sid);
			// Shared faux queue, ordered: every session's FIRST call draws a
			// toolCall (blocking it inside the tool), the follow-ups draw finals.
			// Whichever session runs last still gets a valid sequence.
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("slow", {}, { id: "t-a" })]),
				fauxAssistantMessage([fauxToolCall("slow", {}, { id: "t-b" })]),
				fauxAssistantMessage([fauxToolCall("slow", {}, { id: "t-c" })]),
				fauxAssistantMessage("done one"),
				fauxAssistantMessage("done two"),
				fauxAssistantMessage("done three"),
			]);
			const turns = Promise.all(
				["p1", "p2", "p3"].map((sid) =>
					h.runner.handleTurn({
						sessionId: sid,
						routingKey: "rk",
						text: "go",
					}),
				),
			);

			// Wait until the pool reaches steady state: two executing, one queued.
			for (let i = 0; i < 200; i++) {
				if (
					maxActive >= 2 &&
					h.runner.poolStats.pending === 1 &&
					h.runner.poolStats.active === 2
				) {
					break;
				}
				await new Promise((r) => setTimeout(r, 10));
			}
			expect(maxActive).toBe(2); // bound held while p3 waited
			expect(h.runner.poolStats.pending).toBe(1);

			releaseAll();
			const outcomes = await turns;
			expect(outcomes.map((o) => o.exitReason)).toEqual([
				"finalized",
				"finalized",
				"finalized",
			]);
			expect(maxActive).toBeLessThanOrEqual(2);
			expect(h.runner.poolStats.active).toBe(0);
		} finally {
			releaseAll();
			await h.close();
		}
	});
});
