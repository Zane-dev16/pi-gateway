// Behavior contracts: happy-path final delivery, cache stability (byte-
// identical system prompt + toolset across consecutive turns), and DEC-015
// alternation repair on the WIRE with persisted bytes untouched.

import { describe, expect, it } from "vitest";

import type { Context } from "./host.js";
import { createRunnerHarness } from "./testing/runner-harness.js";
import { fauxAssistantMessage, fauxToolCall } from "./testing/faux-model.js";

describe("runner loop — happy path", () => {
	it("drives the REAL host loop to final text and persists both rows", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("sess-1");
			h.faux.setResponses([fauxAssistantMessage("Hello from the host loop")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "sess-1",
				routingKey: "agent:main:test:dm:c1",
				text: "hi there",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("Hello from the host loop");
			expect(outcome.iterations).toBe(1);
			expect(outcome.repairs).toBe(0);

			// Persisted rows: user row (clean display content) + assistant row.
			const rows = h.store.listMessages("sess-1");
			expect(rows.map((r) => r.role)).toEqual(["user", "assistant"]);
			expect(rows[0]?.content).toBe("hi there");
			expect(rows[0]?.api_content).toBe("hi there"); // composed == clean here
			expect(rows[1]?.content).toBe("Hello from the host loop");

			// api_content sidecar binds the EXACT wire bytes of the assistant turn.
			const expectedWire = JSON.stringify({
				role: "assistant",
				content: [{ type: "text", text: "Hello from the host loop" }],
			});
			expect(h.store.getApiContent(rows[1]!.id)).toBe(expectedWire);
			expect(outcome.assistantRowId).toBe(rows[1]!.id);
			expect(outcome.userRowId).toBe(rows[0]!.id);
		} finally {
			await h.close();
		}
	});

	it("a tool-calling turn iterates inside ONE prompt and delivers the final text", async () => {
		const h = await createRunnerHarness({ maxIterations: 5 });
		try {
			h.ensureSession("sess-tool");
			h.faux.setResponses([
				fauxAssistantMessage([
					fauxToolCall("echo", { say: "step" }, { id: "tc-1" }),
				]),
				fauxAssistantMessage("all done"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "sess-tool",
				routingKey: "rk",
				text: "do the thing",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("all done");
			expect(outcome.iterations).toBe(2); // two model calls, one prompt()
			expect(h.faux.state.callCount).toBe(2);
			// Rows: user, assistant(toolCall), tool(result), assistant(final).
			// The host loop's internal assistant+toolResult pair is session-local
			// (in-memory SessionManager); the gateway persists the authoritative
			// final payload per turn plus the user row.
			const roles = h.store.listMessages("sess-tool").map((r) => r.role);
			expect(roles).toEqual(["user", "assistant"]);
		} finally {
			await h.close();
		}
	});
});

describe("cache stability (05 §8)", () => {
	it("two consecutive turns observe byte-identical system prompt + toolset hash via the REAL request context", async () => {
		const seen: Array<{ sys: string; tools: string; n: number }> = [];
		const h = await createRunnerHarness({
			systemPrompt: "STABLE SYSTEM PROMPT BYTES v1",
		});
		try {
			h.ensureSession("cache-sess");
			h.faux.setResponses([]);
			h.faux.appendResponses([
				(context: Context) => {
					seen.push({
						sys: context.systemPrompt ?? "",
						tools: JSON.stringify((context.tools ?? []).map((t) => t.name)),
						n: seen.length + 1,
					});
					return fauxAssistantMessage(`reply ${seen.length}`);
				},
				(context: Context) => {
					seen.push({
						sys: context.systemPrompt ?? "",
						tools: JSON.stringify((context.tools ?? []).map((t) => t.name)),
						n: seen.length + 1,
					});
					return fauxAssistantMessage(`reply ${seen.length}`);
				},
			]);
			await h.runner.handleTurn({
				sessionId: "cache-sess",
				routingKey: "rk",
				text: "turn one",
			});
			await h.runner.handleTurn({
				sessionId: "cache-sess",
				routingKey: "rk",
				text: "turn two",
			});
			expect(seen).toHaveLength(2);
			// The configured override anchors the prompt (the host composes
			// further deterministic sections around it).
			expect(seen[0]!.sys.startsWith("STABLE SYSTEM PROMPT BYTES v1")).toBe(
				true,
			);
			// Byte equality across turns: same cached session → identical prefix.
			expect(seen[1]!.sys).toBe(seen[0]!.sys);
			expect(seen[1]!.tools).toBe(seen[0]!.tools);
			// The cache actually held ONE entry for both turns.
			expect(h.runner.cacheStats.entries).toBe(1);
		} finally {
			await h.close();
		}
	});
});

describe("alternation repair PRE-REQUEST (DEC-015)", () => {
	it("compacts a replayed user→user tail onto the wire; persisted rows untouched", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("repair-sess");
			// Seed malformed history DIRECTLY as rows (crash-between-appends shape):
			// user, user adjacent.
			await h.store.appendMessage({
				sessionId: "repair-sess",
				role: "user",
				content: "first queued message",
				apiContent: "first queued message",
			});
			await h.store.appendMessage({
				sessionId: "repair-sess",
				role: "user",
				content: "second queued message",
				apiContent: "second queued message",
			});
			const beforeRows = h.store.listMessages("repair-sess");

			const wireUserContents: string[] = [];
			const userText = (content: unknown): string => {
				if (typeof content === "string") return content;
				return (content as Array<{ type: string; text?: string }>)
					.filter((b) => b.type === "text")
					.map((b) => b.text ?? "")
					.join("");
			};
			h.faux.setResponses([
				(context: Context) => {
					for (const m of context.messages) {
						if (m.role === "user") {
							wireUserContents.push(userText(m.content));
						}
					}
					return fauxAssistantMessage("repaired");
				},
			]);

			const outcome = await h.runner.handleTurn({
				sessionId: "repair-sess",
				routingKey: "rk",
				text: "live third message",
			});
			expect(outcome.repairs).toBe(1); // one merge event
			expect(outcome.exitReason).toBe("finalized");

			// Wire copy repaired: exactly TWO user messages reach the model —
			// the merged tail (both inputs preserved, blank-line joined) and the
			// live message; NOT three adjacent users.
			expect(wireUserContents).toEqual([
				"first queued message\n\nsecond queued message",
				"live third message",
			]);

			// Persisted bytes UNTOUCHED: the two original rows keep their own
			// content AND sidecar bytes (no rewrite outside compression).
			const afterRows = h.store
				.listMessages("repair-sess")
				.filter((r) => r.role === "user");
			expect(afterRows).toHaveLength(3);
			expect(afterRows[0]!.id).toBe(beforeRows[0]!.id);
			expect(afterRows[0]!.content).toBe("first queued message");
			expect(afterRows[0]!.api_content).toBe("first queued message");
			expect(afterRows[1]!.id).toBe(beforeRows[1]!.id);
			expect(afterRows[1]!.content).toBe("second queued message");
			expect(afterRows[1]!.api_content).toBe("second queued message");
		} finally {
			await h.close();
		}
	});
});
