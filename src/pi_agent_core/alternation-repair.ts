// pi_agent_core/alternation-repair.ts — strict role-alternation repair run
// PRE-REQUEST in the agent layer (DEC-015), on the LIVE history the host loop
// will send. The persisted rows are NEVER touched here: persistence stores
// bytes; flush methods are token-count/persistence, not repair.
//
// Semantic port of (READ-ONLY reference; no code vendored):
//   agent/agent_runtime_helpers.py:repair_message_sequence
//   agent/agent_runtime_helpers.py:repair_message_sequence_with_cursor
//
// Repairs applied (same pass order as Hermes — order is load-bearing):
//   Pass 0: merge consecutive assistant messages. Runs BEFORE pass 1 so the
//     merged turn's union of tool-call ids is known when pass 1 validates
//     which tool results are orphans.
//   Pass 1: drop stray tool results whose tool_call_id doesn't match any
//     preceding assistant tool call; each id is consumed once so a DUPLICATE
//     tool result (retry/crash/resume glitch) drops instead of replaying;
//     a user turn closes a tool-result run.
//   Pass 2: merge consecutive user messages with a blank-line separator so no
//     user input is lost. Plain string content merges (pi UserMessage allows
//     `string`); structured/multimodal content blocks are deliberately left
//     unmerged (parity: collapsing attachment structure risks mangling).
//
// Deliberately does NOT rewind orphan assistant(toolCalls)+toolResult pairs
// that precede a user message — that pattern IS valid when the previous turn
// completed normally and the user redirected before the continuation turn.
//
// Companion sanitation step (same pre-request family, DEC-015):
//   sanitizeToolCallArguments      ← agent/agent_runtime_helpers.py:
//     sanitize_tool_call_arguments (repair corrupted tool_call argument JSON
//     in-place on live history) + agent/message_sanitization.py:
//     _repair_tool_call_arguments / _escape_invalid_chars_in_json_strings
//     (the repair pipeline itself: trailing commas, unclosed structures,
//     excess closers bounded 50, control-char lacing, "{}" last resort).
//
// TS mapping notes vs the Python source:
//   - tool calls live INSIDE assistant.content as ToolCall blocks (pi shape),
//     not a separate `tool_calls` key → union = appending later blocks.
//   - reasoning parity: an earlier merged turn lacking a thinking block picks
//     up the later turn's thinking block (strict thinking providers require
//     reasoning on the merged tool-call turn; first non-empty suffices).

import type {
	AssistantMessage,
	Message,
	TextContent,
	ToolCall,
} from "./host.js";

/** Joined plain text when every content block is text; null otherwise. */
function plainText(content: unknown): string | null {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return null;
	let out = "";
	for (const block of content) {
		const b = block as { type?: string; text?: unknown };
		if (b?.type !== "text" || typeof b.text !== "string") return null;
		out += b.text;
	}
	return out;
}

function textBlocks(text: string): TextContent[] {
	return [{ type: "text", text }];
}

function isAssistant(m: Message | undefined): m is AssistantMessage {
	return m?.role === "assistant";
}

function toolCallBlocks(m: AssistantMessage): ToolCall[] {
	return m.content.filter((b): b is ToolCall => b.type === "toolCall");
}

/**
 * Compact malformed role alternation left in the live history, in place.
 * Returns the number of repairs made (for logging/telemetry).
 */
export function repairMessageSequence(messages: Message[]): number {
	if (messages.length === 0) return 0;
	let repairs = 0;

	// ---- Pass 0: merge consecutive assistant messages -----------------------
	const collapsed: Message[] = [];
	for (const msg of messages) {
		const prev = collapsed[collapsed.length - 1];
		if (isAssistant(msg) && isAssistant(prev)) {
			// Union tool calls (preserve order; both turns may carry them) by
			// appending the later turn's ToolCall blocks to the survivor.
			for (const call of toolCallBlocks(msg)) prev.content.push(call);
			// Concatenate plain-text content of both turns.
			const prevText = plainText(
				prev.content.filter((b) => b.type !== "toolCall"),
			);
			const nextText = plainText(
				msg.content.filter((b) => b.type !== "toolCall"),
			);
			if (prevText !== null && nextText !== null) {
				const joined = [prevText.trim(), nextText.trim()]
					.filter((p) => p.length > 0)
					.join("\n");
				// Rebuild content: surviving thinking/toolCall blocks + joined text.
				const kept = prev.content.filter(
					(b) => b.type === "thinking" || b.type === "toolCall",
				);
				prev.content = [
					...kept.filter((b) => b.type === "thinking"),
					...(joined ? textBlocks(joined) : []),
					...kept.filter((b) => b.type === "toolCall"),
				] as AssistantMessage["content"];
			} else if (
				prev.content.length === 0 ||
				prev.content.every((b) => b.type === "toolCall")
			) {
				// Survivor carries no prose; adopt the later turn's non-tool blocks.
				for (const block of msg.content) {
					if (block.type === "text" || block.type === "thinking") {
						prev.content.push(block);
					}
				}
			}
			repairs += 1;
			continue;
		}
		collapsed.push(msg);
	}

	// ---- Pass 1: drop stray/duplicate tool results ---------------------------
	const knownToolIds = new Set<string>();
	const filtered: Message[] = [];
	for (const msg of collapsed) {
		if (msg.role === "assistant") {
			knownToolIds.clear();
			for (const tc of toolCallBlocks(msg)) {
				if (tc.id) knownToolIds.add(tc.id);
			}
			filtered.push(msg);
		} else if (msg.role === "toolResult") {
			const tcId = msg.toolCallId;
			if (tcId && knownToolIds.has(tcId)) {
				filtered.push(msg);
				// Consume: a second result with the same id is an orphan now.
				knownToolIds.delete(tcId);
			} else {
				repairs += 1; // stray or duplicate → dropped
			}
		} else {
			if (msg.role === "user") knownToolIds.clear();
			filtered.push(msg);
		}
	}

	// ---- Pass 2: merge consecutive user messages ----------------------------
	const merged: Message[] = [];
	for (const msg of filtered) {
		const prev = merged[merged.length - 1];
		if (prev?.role === "user" && msg.role === "user") {
			const prevText =
				typeof prev.content === "string"
					? prev.content
					: plainText(prev.content);
			const nextText =
				typeof msg.content === "string" ? msg.content : plainText(msg.content);
			// Merge only plain-text pairs (both string form or all-text arrays);
			// anything structured stays separate — no input lost, nothing mangled.
			if (prevText !== null && nextText !== null) {
				prev.content =
					prevText && nextText
						? `${prevText}\n\n${nextText}`
						: prevText || nextText;
				repairs += 1;
				continue;
			}
		}
		merged.push(msg);
	}

	if (repairs > 0) {
		// Rewrite in place so callers assigning this array back into agent state
		// publish exactly the repaired sequence (Hermes `messages[:] = merged`).
		messages.length = 0;
		for (const m of merged) messages.push(m);
	}
	return repairs;
}

export interface RepairWithCursorResult {
	repairs: number;
	/** Recomputed flush cursor (see below). */
	cursor: number;
}

/**
 * Escape unescaped control characters inside JSON string values
 * (message_sanitization.py:_escape_invalid_chars_in_json_strings): walk the
 * raw JSON tracking double-quoted strings; inside strings, literal control
 * chars (< 0x20, not already part of an escape pair) become \uXXXX. Passes
 * everything else through untouched.
 */
function escapeInvalidCharsInJsonStrings(raw: string): string {
	const out: string[] = [];
	let inString = false;
	let i = 0;
	const n = raw.length;
	while (i < n) {
		const ch = raw[i]!;
		if (inString) {
			if (ch === "\\" && i + 1 < n) {
				// Already-escaped char — pass through as-is.
				out.push(ch, raw[i + 1]!);
				i += 2;
				continue;
			}
			if (ch === '"') {
				inString = false;
				out.push(ch);
			} else if (ch.charCodeAt(0) < 0x20) {
				out.push(`\\u${ch.charCodeAt(0).toString(16).padStart(4, "0")}`);
			} else {
				out.push(ch);
			}
		} else {
			if (ch === '"') inString = true;
			out.push(ch);
		}
		i += 1;
	}
	return out.join("");
}

/**
 * message_sanitization.py:_repair_tool_call_arguments — attempt to repair
 * malformed tool-call argument JSON; when every repair fails return "{}" so
 * the request succeeds instead of crashing the session on an HTTP 400.
 * Repair ladder (order is load-bearing):
 *   fast-path empty/whitespace → "{}"; Python-literal None → "{}"; direct
 *   parse+reserialize; strip trailing commas before }/] ; close unclosed {/[ ;
 *   remove excess closers (bounded 50); control-char escape retry; "{}".
 */
export function repairToolCallArgumentsJson(
	rawArgs: string,
	toolName = "?",
): string {
	const rawStripped = typeof rawArgs === "string" ? rawArgs.trim() : "";
	void toolName; // parity parameter — Hermes logs per-tool WARNINGs here

	// Fast-path: empty / whitespace-only → empty object.
	if (!rawStripped) return "{}";
	// Python-literal None (and its JSON spelling) → normalize to {}.
	if (rawStripped === "None" || rawStripped === "null") return "{}";

	const tryParse = (s: string): unknown => {
		try {
			return JSON.parse(s);
		} catch {
			return undefined;
		}
	};

	// Pass 0: direct parse + compact reserialize (normalizes formatting).
	let parsed = tryParse(rawStripped);
	if (parsed !== undefined) return JSON.stringify(parsed);

	// Common repairs.
	let fixed = rawStripped.replace(/,\s*([}\]])/g, "$1"); // 1. trailing commas
	const openCurly =
		(fixed.match(/\{/g)?.length ?? 0) - (fixed.match(/\}/g)?.length ?? 0);
	const openBracket =
		(fixed.match(/\[/g)?.length ?? 0) - (fixed.match(/\]/g)?.length ?? 0);
	if (openCurly > 0) fixed += "}".repeat(openCurly); // 2. close unclosed
	if (openBracket > 0) fixed += "]".repeat(openBracket);
	// 3. Remove excess closers (bounded to 50 iterations).
	for (let i = 0; i < 50; i++) {
		if (tryParse(fixed) !== undefined) break;
		if (
			fixed.endsWith("}") &&
			(fixed.match(/\}/g)?.length ?? 0) > (fixed.match(/\{/g)?.length ?? 0)
		) {
			fixed = fixed.slice(0, -1);
		} else if (
			fixed.endsWith("]") &&
			(fixed.match(/\]/g)?.length ?? 0) > (fixed.match(/\[/g)?.length ?? 0)
		) {
			fixed = fixed.slice(0, -1);
		} else {
			break;
		}
	}
	parsed = tryParse(fixed);
	if (parsed !== undefined) return JSON.stringify(parsed);

	// Pass 4: escape control chars inside strings, then retry.
	const escaped = escapeInvalidCharsInJsonStrings(fixed);
	if (escaped !== fixed) {
		parsed = tryParse(escaped);
		if (parsed !== undefined) return JSON.stringify(parsed);
	}

	// Last resort: empty object so the request still succeeds.
	return "{}";
}

/** Parse repaired argument JSON; always yields a record ({} fallback). */
function argumentsRecord(repairedJson: string): Record<string, unknown> {
	try {
		const parsed: unknown = JSON.parse(repairedJson);
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed)
		) {
			return parsed as Record<string, unknown>;
		}
	} catch {
		/* fall through */
	}
	return {};
}

/**
 * agent_runtime_helpers.py:sanitize_tool_call_arguments companion pass over
 * LIVE history (DEC-015 family): every assistant ToolCall block must carry a
 * well-formed arguments OBJECT before the request goes out. String arguments
 * are parsed (through the _repair_tool_call_arguments ladder when malformed);
 * null/empty arguments become {}; unshapeable values degrade to {}. Returns
 * the number of repairs made. Runs AFTER the sequence passes so merged
 * survivors are sanitized exactly once.
 */
export function sanitizeToolCallArguments(messages: Message[]): number {
	let repairs = 0;
	for (const msg of messages) {
		if (!isAssistant(msg)) continue;
		for (let i = 0; i < msg.content.length; i++) {
			const block = msg.content[i]!;
			if (block.type !== "toolCall") continue;
			const call = block as ToolCall;
			const args: unknown = call.arguments;
			if (typeof args === "string") {
				call.arguments = argumentsRecord(
					repairToolCallArgumentsJson(args, call.name),
				);
				repairs += 1;
			} else if (args === undefined || args === null) {
				call.arguments = {};
				repairs += 1;
			}
		}
	}
	return repairs;
}

/**
 * repair_message_sequence + flush-cursor consistency (#44837): the cursor
 * indexes into the compacted list, so counting SURVIVORS of the
 * previously-flushed prefix by object identity gives the exact new cursor even
 * when messages are dropped/merged at indexes before it — a plain min() clamp
 * would silently skip that many unflushed rows. Falls back to the clamp when
 * no prefix snapshot is available (flushedCount <= 0).
 */
export function repairMessageSequenceWithCursor(
	messages: Message[],
	flushedCount: number,
): RepairWithCursorResult {
	const validCursor = Number.isInteger(flushedCount) && flushedCount > 0;
	const preRepairFlushedIds = validCursor
		? new Set(messages.slice(0, flushedCount))
		: null;

	const repairs = repairMessageSequence(messages);

	let cursor = flushedCount;
	if (repairs > 0) {
		cursor =
			preRepairFlushedIds !== null
				? messages.filter((m) => preRepairFlushedIds.has(m)).length
				: Math.min(flushedCount, messages.length);
	}
	return { repairs, cursor };
}
