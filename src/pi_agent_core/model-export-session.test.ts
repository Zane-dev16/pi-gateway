// model-export-session.test.ts — /model + /export run REAL host execution.
//
// The runner exposes the cached host AgentSession through setSessionModel
// (resolveCliModel + AgentSession.setModel, persist:false) and
// exportSessionJsonl (serializeSessionBranch, inline bytes — never a file
// write). These contracts push turns through the REAL host loop (scripted
// faux models, sanctioned injection) and assert the observed model identity
// and export bytes — never the seam's shape.

import { describe, expect, it } from "vitest";

import {
	createRunnerHarness,
	type RunnerHarness,
} from "./testing/runner-harness.js";
import {
	fauxAssistantMessage,
	fauxProvider,
} from "./testing/faux-model.js";

const ROUTING = "agent:main:test:dm:c1";

async function oneTurn(h: RunnerHarness, sessionId: string, text: string) {
	h.faux.setResponses([fauxAssistantMessage(`reply to ${text}`)]);
	const outcome = await h.runner.handleTurn({
		sessionId,
		routingKey: ROUTING,
		text,
	});
	expect(outcome.exitReason).toBe("finalized");
}

/** A second registered provider so /model has something to switch to. */
function registerSecondProvider(h: RunnerHarness) {
	const second = fauxProvider({
		provider: "fauxb",
		models: [{ id: "faux-2", name: "faux-2" } as never],
	});
	h.env.modelRuntime.registerNativeProvider(
		(second as unknown as { provider: never }).provider,
	);
	return second as unknown as {
		setResponses(responses: never[]): void;
	};
}

describe("runner setSessionModel", () => {
	it("switches the cached host session to the resolved model", async () => {
		const h = await createRunnerHarness();
		try {
			registerSecondProvider(h);
			h.ensureSession("model-sess");
			await oneTurn(h, "model-sess", "hello");
			const before = await h.runner.getSessionModel("model-sess");
			expect(`${before.provider}/${before.id}`).toBe("faux/faux-1");
			const switched = await h.runner.setSessionModel(
				"model-sess",
				"fauxb/faux-2",
			);
			expect(`${switched.provider}/${switched.id}`).toBe("fauxb/faux-2");
			const current = await h.runner.getSessionModel("model-sess");
			expect(`${current.provider}/${current.id}`).toBe("fauxb/faux-2");
			expect(h.runner.listAvailableModels().map((m) => `${m.provider}/${m.id}`))
				.toContain("fauxb/faux-2");
		} finally {
			await h.close();
		}
	});

	it("the next turn finalizes after the switch", async () => {
		const h = await createRunnerHarness();
		try {
			const second = registerSecondProvider(h);
			h.ensureSession("model-turn");
			await oneTurn(h, "model-turn", "hello");
			await h.runner.setSessionModel("model-turn", "fauxb/faux-2");
			second.setResponses([fauxAssistantMessage("on the new model")] as never[]);
			const outcome = await h.runner.handleTurn({
				sessionId: "model-turn",
				routingKey: ROUTING,
				text: "still here?",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("on the new model");
		} finally {
			await h.close();
		}
	});

	it("the switch survives a cache rebuild", async () => {
		const h = await createRunnerHarness();
		try {
			registerSecondProvider(h);
			h.ensureSession("model-evict");
			await oneTurn(h, "model-evict", "hello");
			await h.runner.setSessionModel("model-evict", "fauxb/faux-2");
			h.runner.dropCachedSession("model-evict");
			const rebuilt = await h.runner.getSessionModel("model-evict");
			expect(`${rebuilt.provider}/${rebuilt.id}`).toBe("fauxb/faux-2");
		} finally {
			await h.close();
		}
	});

	it("an unknown ref throws the host resolution error", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("model-unknown");
			await expect(
				h.runner.setSessionModel("model-unknown", "nope/nothing-here"),
			).rejects.toThrow();
		} finally {
			await h.close();
		}
	});
});

describe("runner exportSessionJsonl", () => {
	it("renders the branch as JSONL with the session header", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("export-sess");
			await oneTurn(h, "export-sess", "export-marker-hello");
			const jsonl = await h.runner.exportSessionJsonl("export-sess");
			const lines = jsonl.trim().split("\n");
			expect(lines.length).toBeGreaterThan(1);
			expect(JSON.parse(lines[0] as string).type).toBe("session");
			expect(jsonl).toContain("export-marker-hello");
		} finally {
			await h.close();
		}
	});
});
