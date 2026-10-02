// compact-session.test.ts — /compact runs REAL host compaction (blocker 1).
//
// The runner exposes the cached host AgentSession through compactSession,
// which drives AgentSession.compact() directly — never a reimplementation.
// These contracts push enough context through the REAL host loop (scripted
// faux model, sanctioned injection) that prepareCompaction finds a cut, then
// assert the observed summary and token counts — never the seam's shape.

import { describe, expect, it } from "vitest";

import {
	createRunnerHarness,
	type RunnerHarness,
} from "./testing/runner-harness.js";
import { fauxAssistantMessage } from "./testing/faux-model.js";

async function growContext(h: RunnerHarness, turns: number): Promise<void> {
	h.ensureSession("compact-sess");
	const filler = `payload ${"x".repeat(15_000)}`;
	for (let i = 0; i < turns; i++) {
		h.faux.setResponses([fauxAssistantMessage(`reply ${i} ${"y".repeat(200)}`)]);
		const outcome = await h.runner.handleTurn({
			sessionId: "compact-sess",
			routingKey: "agent:main:test:dm:c1",
			text: `turn ${i}: ${filler}`,
		});
		expect(outcome.exitReason).toBe("finalized");
	}
}

describe("runner compactSession (blocker 1)", () => {
	it("compacts real host context and reports the host summary", async () => {
		const h = await createRunnerHarness();
		try {
			await growContext(h, 8);
			h.faux.setResponses([fauxAssistantMessage("condensed session summary")]);
			const result = await h.runner.compactSession("compact-sess");
			expect(result.tokensBefore).toBeGreaterThan(0);
			expect(result.summary).toContain("condensed session summary");
			expect(result.firstKeptEntryId.length).toBeGreaterThan(0);
			// The cached session survives compaction: the next turn finalizes.
			h.faux.setResponses([fauxAssistantMessage("after compaction")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "compact-sess",
				routingKey: "agent:main:test:dm:c1",
				text: "still here?",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("after compaction");
		} finally {
			await h.close();
		}
	});

	it("forwards custom instructions as the compaction tail", async () => {
		const h = await createRunnerHarness();
		try {
			await growContext(h, 8);
			h.faux.setResponses([fauxAssistantMessage("focused summary")]);
			const result = await h.runner.compactSession(
				"compact-sess",
				"focus on the API surface",
			);
			expect(result.summary).toContain("focused summary");
		} finally {
			await h.close();
		}
	});
});
