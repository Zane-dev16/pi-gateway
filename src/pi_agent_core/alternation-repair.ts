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
