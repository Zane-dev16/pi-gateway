// Behavior contracts for the DEC-015 pre-request alternation repair.
// Shapes ported from 05-toolsets-and-agent-loop.md §5.3 / §8 ("Alternation
// repair: inject user→user tail → repair compacts … request succeeds") and
// the Hermes docstring cases of repair_message_sequence.

import { describe, expect, it } from "vitest";

import type { AssistantMessage, Message, ToolResultMessage } from "./host.js";
import {
	repairMessageSequence,
	repairMessageSequenceWithCursor,
	repairToolCallArgumentsJson,
	sanitizeToolCallArguments,
} from "./alternation-repair.js";
import {
	fauxAssistantMessage,
	fauxText,
	fauxToolCall,
} from "./testing/faux-model.js";

let ts = 1_700_000_000_000;
const nextTs = () => ++ts;

function assistant(content: AssistantMessage["content"]): AssistantMessage {
	return fauxAssistantMessage(content, { timestamp: nextTs() });
}

function user(text: string): Message {
	return { role: "user", content: text, timestamp: nextTs() };
}

function toolResult(
	toolCallId: string,
	text = "ok",
	isError = false,
): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId,
		toolName: "echo",
		content: [{ type: "text", text }],
		isError,
		timestamp: nextTs(),
	};
}

describe("alternation repair (DEC-015)", () => {
	it("merges consecutive assistant messages and unions their tool calls", () => {
		const callA = fauxToolCall("echo", { x: 1 }, { id: "call_a" });
		const callB = fauxToolCall("echo", { y: 2 }, { id: "call_b" });
		const messages: Message[] = [
			user("hi"),
			assistant([fauxText("part one"), callA]),
			assistant([fauxText("part two"), callB]),
		];
		const repairs = repairMessageSequence(messages);
		expect(repairs).toBe(1);
		expect(messages).toHaveLength(2);
		const merged = messages[1] as AssistantMessage;
		const calls = merged.content.filter((b) => b.type === "toolCall");
		expect(calls.map((c) => c.id)).toEqual(["call_a", "call_b"]);
		// Plain-text content of both turns concatenates into ONE joined block.
		const texts = merged.content
			.filter((b) => b.type === "text")
			.map((b) => (b as { text: string }).text);
		expect(texts).toEqual(["part one\npart two"]);
	});

	it("does NOT merge assistants separated by a tool result (two valid rounds)", () => {
		const messages: Message[] = [
			user("go"),
			assistant([fauxToolCall("echo", {}, { id: "c1" })]),
			toolResult("c1"),
			assistant([fauxToolCall("echo", {}, { id: "c2" })]),
			toolResult("c2"),
			assistant([fauxText("done")]),
		];
		expect(repairMessageSequence(messages)).toBe(0);
		expect(messages).toHaveLength(6);
	});

	it("drops stray tool results with no matching preceding tool call", () => {
		const messages: Message[] = [
			user("hi"),
			toolResult("ghost-id"),
			assistant([fauxText("hello")]),
		];
		const repairs = repairMessageSequence(messages);
		expect(repairs).toBe(1);
		expect(messages.map((m) => m.role)).toEqual(["user", "assistant"]);
	});

	it("drops DUPLICATE tool results for the same id but keeps the first", () => {
		const messages: Message[] = [
			user("hi"),
			assistant([fauxToolCall("echo", {}, { id: "c1" })]),
			toolResult("c1", "first"),
			toolResult("c1", "duplicate"),
		];
		const repairs = repairMessageSequence(messages);
		expect(repairs).toBe(1);
		const results = messages.filter((m) => m.role === "toolResult");
		expect(results).toHaveLength(1);
		expect((results[0] as ToolResultMessage).content[0]).toEqual({
			type: "text",
			text: "first",
		});
	});

	it("a user turn closes a tool-result run: later results after it are orphans", () => {
		const messages: Message[] = [
			user("q"),
			assistant([fauxToolCall("echo", {}, { id: "c1" })]),
			toolResult("c1"),
			user("redirect"),
			toolResult("stale-after-user"),
		];
		const repairs = repairMessageSequence(messages);
		expect(repairs).toBe(1);
		expect(messages.map((m) => m.role)).toEqual([
			"user",
			"assistant",
			"toolResult",
			"user",
		]);
	});

	it("keeps valid assistant(toolCall)+toolResult before a user redirect", () => {
		const messages: Message[] = [
			user("q"),
			assistant([fauxToolCall("echo", {}, { id: "keep" })]),
			toolResult("keep"),
			user("actually stop"),
		];
		expect(repairMessageSequence(messages)).toBe(0);
		expect(messages).toHaveLength(4);
	});

	it("compacts a user→user tail into ONE merged message preserving both inputs", () => {
		const messages: Message[] = [
			user("first half"),
			assistant([fauxText("answer")]),
			user("second half"),
			user("third part"),
		];
		// Each pairwise merge event counts as one repair (parity with Hermes).
		const repairs = repairMessageSequence(messages);
		expect(repairs).toBe(1);
		expect(messages.map((m) => m.role)).toEqual(["user", "assistant", "user"]);
		const mergedUser = messages[2] as { content: string };
		expect(mergedUser.content).toBe("second half\n\nthird part");
	});

	it("user merge keeps unmergeable structured content separate instead of mangling", () => {
		const structured: Message = {
			role: "user",
			content: [
				{ type: "image", data: "Zm9v", mimeType: "image/png" },
				{ type: "text", text: "with attachment" },
			],
			timestamp: nextTs(),
		};
		const messages: Message[] = [user("plain"), structured];
		const repairs = repairMessageSequence(messages);
		// string + structured array → plainText(structured)=null → no merge
		expect(repairs).toBe(0);
		expect(messages).toHaveLength(2);
	});

	it("repairs nothing on well-formed history", () => {
		const messages: Message[] = [
			user("q"),
			assistant([fauxText("partial")]),
			user("more"),
			assistant([fauxText("final")]),
		];
		expect(repairMessageSequence(messages)).toBe(0);
	});

	it("empty history is a no-op returning zero", () => {
		const messages: Message[] = [];
		expect(repairMessageSequence(messages)).toBe(0);
		expect(messages).toHaveLength(0);
	});

	it("cursor recomputation counts survivors of the flushed prefix (#44837 shape)", () => {
		// Flushed prefix = [u0, a0]. Repair merges u1+u2 and would shift indexes;
		// cursor must count surviving prefix members (2), not clamp blindly to
		// something that skips unflushed rows.
		const u0 = user("u0");
		const a0 = assistant([fauxText("a0")]);
		const messages: Message[] = [u0, a0, user("u1"), user("u2")];
		const { repairs, cursor } = repairMessageSequenceWithCursor(messages, 2);
		expect(repairs).toBe(1);
		expect(cursor).toBe(2);
		expect(messages).toHaveLength(3);
	});

	it("cursor falls back to min() clamp when nothing was flushed yet", () => {
		const messages: Message[] = [user("a"), user("b")];
		const { repairs, cursor } = repairMessageSequenceWithCursor(messages, 0);
		expect(repairs).toBe(1);
		expect(cursor).toBe(0); // flushedCount<=0 → clamp path, stays <= len
	});

	it("repair mutates the passed array in place so callers republish it", () => {
		const messages: Message[] = [user("x"), user("y")];
		const sameRef = messages;
		repairMessageSequence(messages);
		expect(sameRef).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// DEC-015 companion sanitation family (agent_runtime_helpers.py:
// sanitize_tool_call_arguments ← message_sanitization.py:
// _repair_tool_call_arguments / _escape_invalid_chars_in_json_strings).

describe("tool-call argument repair ladder (_repair_tool_call_arguments)", () => {
	it("empty / whitespace / Python-None / JSON-null arguments normalize to {}", () => {
		expect(repairToolCallArgumentsJson("")).toBe("{}");
		expect(repairToolCallArgumentsJson("   ")).toBe("{}");
		expect(repairToolCallArgumentsJson("None")).toBe("{}");
		expect(repairToolCallArgumentsJson("null")).toBe("{}");
	});

	it("well-formed JSON passes through compactly reserialized", () => {
		expect(repairToolCallArgumentsJson('{"a": 1}')).toBe('{"a":1}');
		expect(repairToolCallArgumentsJson("[1,2]")).toBe("[1,2]");
	});

	it("trailing commas before closing braces/brackets are stripped", () => {
		expect(repairToolCallArgumentsJson('{"a": [1, 2,]}')).toBe('{"a":[1,2]}');
		expect(repairToolCallArgumentsJson('{"a": 1,}')).toBe('{"a":1}');
	});

	it("truncated structures are closed; hopeless truncation degrades to {}", () => {
		expect(repairToolCallArgumentsJson("[1, 2")).toBe("[1,2]");
		expect(repairToolCallArgumentsJson('{"a": {"b": 1')).toBe('{"a":{"b":1}}');
		// Unterminated STRING can't be repaired by brace-closing → {} parity.
		expect(repairToolCallArgumentsJson('{"truncated": "val')).toBe("{}");
	});

	it("excess closers are removed (bounded loop)", () => {
		expect(repairToolCallArgumentsJson('{"a": 1}}}')).toBe('{"a":1}');
	});

	it("literal control characters inside strings are escaped and retried", () => {
		// llama.cpp-style raw tab inside a string value (#12068 family): JS
		// JSON.parse rejects it outright.
		const laced = '{"cmd": "a\tb"}'; // literal TAB byte inside the value
		expect(() => JSON.parse(laced)).toThrow();
		expect(repairToolCallArgumentsJson(laced)).toBe('{"cmd":"a\\tb"}');
	});
});

describe("sanitizeToolCallArguments (pre-request companion pass)", () => {
	function assistantWithArgs(args: unknown): AssistantMessage {
		return assistant([
			fauxToolCall("echo", args as Record<string, unknown>, { id: "c1" }),
		]);
	}

	it("string arguments parse into objects; corrupt ones go through the ladder", () => {
		const messages: Message[] = [assistantWithArgs('{"x": 1,}')];
		expect(sanitizeToolCallArguments(messages)).toBe(1);
		const call = (
			(messages[0] as AssistantMessage).content as Array<{
				type: string;
				arguments?: unknown;
			}>
		).find((b) => b.type === "toolCall");
		expect(call?.arguments).toEqual({ x: 1 });
	});

	it("null/undefined arguments become {}; object arguments are untouched", () => {
		const untouched = fauxAssistantMessage([
			fauxToolCall("ok", { keep: true }, { id: "c0" }),
		]);
		const nulled = fauxAssistantMessage([
			fauxToolCall("n", undefined as unknown as Record<string, unknown>, {
				id: "c1",
			}),
		]);
		const messages: Message[] = [
			user("q"),
			untouched,
			nulled,
			assistant([fauxText("done")]),
		];
		expect(sanitizeToolCallArguments(messages)).toBe(1); // only the null one
		const calls = (
			nulled.content as Array<{ type: string; arguments?: unknown }>
		).filter((b) => b.type === "toolCall");
		expect(calls[0]?.arguments).toEqual({});
		const kept = (
			untouched.content as Array<{ type: string; arguments?: unknown }>
		).find((b) => b.type === "toolCall");
		expect(kept?.arguments).toEqual({ keep: true });
	});

	it("counts zero repairs on already-clean history", () => {
		const messages: Message[] = [
			user("q"),
			assistant([fauxText("hi")]),
			assistantWithArgs({ fine: 1 }),
		];
		expect(sanitizeToolCallArguments(messages)).toBe(0);
	});
});
