// pi_gateway/streaming/stream-events — the TYPED agent→gateway stream-event
// vocabulary (04-platform-adapters.md §5.3).
//
// Port of the READ-ONLY Hermes reference, semantics only, cited as file:symbol
// anchors — no code vendored:
//   - gateway/stream_events.py:MessageChunk / MessageStop / Commentary
//   - gateway/stream_events.py:ToolCallChunk / ToolCallFinished
//   - gateway/stream_events.py:LongToolHint / GatewayNotice / StreamEvent
//
// Design constraints copied verbatim from 04 §5.3:
//   * Events name WHAT HAPPENED, never HOW TO SEND. They are plain frozen
//     records — no behavior, no platform knowledge, no I/O — cheap to build on
//     the agent's worker thread and safe to hand across the thread boundary.
//   * Events describe TRANSPORT, never CONTEXT. Nothing here is persisted to
//     history; whatever the gateway "eats" must not diverge from the bytes the
//     agent stored. History is agent-owned; this stream is presentation-only.
//   * The union is explicit (no marker base class) so a missing case in an
//     exhaustive switch is a visible TYPE error rather than silent fall-through.

/** A delta of streamed assistant text (think-blocks filtered UPSTREAM). */
export interface MessageChunk {
	readonly type: "message_chunk";
	readonly text: string;
}

/**
 * The current assistant segment is complete. `final` is true ONLY for the
 * terminal stop of the whole turn; an intermediate stop (text → tool → text)
 * carries final:false so the consumer finalizes the current bubble and opens a
 * fresh segment below any tool chrome. The TERMINAL stop itself is signalled
 * by the gateway via `GatewayStreamConsumer.finish(final_text)` — never here.
 */
export interface MessageStop {
	readonly type: "message_stop";
	readonly final: boolean;
}

/** A complete interim assistant message between tool iterations (own beat). */
export interface Commentary {
	readonly type: "commentary";
	readonly text: string;
}

/**
 * A tool invocation started (or its in-progress state changed). Carries raw
 * facts; the ADAPTER decides presentation (emoji, truncation, eat entirely).
 * `index` is the monotonic per-turn call index so a finish can be correlated
 * with its start and "new"-mode dedup works without consumer bookkeeping.
 */
export interface ToolCallChunk {
	readonly type: "tool_call_chunk";
	readonly toolName: string;
	readonly preview?: string | undefined;
	readonly args?: Record<string, unknown> | undefined;
	readonly index: number;
}

/** A tool invocation completed. No tool OUTPUT travels here (agent-owned). */
export interface ToolCallFinished {
	readonly type: "tool_call_finished";
	readonly toolName: string;
	/** Wall-clock duration in seconds (float), parity stream_events.py. */
	readonly durationSeconds: number;
	readonly ok: boolean;
	readonly index: number;
}

/** One-shot onboarding nudge when a tool runs longer than the threshold. */
export interface LongToolHint {
	readonly type: "long_tool_hint";
	readonly toolName: string;
	readonly durationSeconds: number;
}

/**
 * A gateway-originated control message (restart, online, long_run …). `notice`
 * is a STABLE string the adapter can switch on; `text` is the human-readable
 * default rendered when the adapter has no platform-specific treatment.
 */
export interface GatewayNotice {
	readonly type: "gateway_notice";
	readonly notice: string;
	readonly text: string;
	readonly extra: Readonly<Record<string, unknown>>;
}

/** Union of EVERY event the dispatcher accepts — kept exhaustive on purpose. */
export type StreamEvent =
	| MessageChunk
	| MessageStop
	| Commentary
	| ToolCallChunk
	| ToolCallFinished
	| LongToolHint
	| GatewayNotice;

// ── frozen factories (frozen-dataclass parity; structural objects accepted) ──

export function messageChunk(text: string): MessageChunk {
	return Object.freeze({ type: "message_chunk", text });
}

export function messageStop(final: boolean): MessageStop {
	return Object.freeze({ type: "message_stop", final });
}

export function commentary(text: string): Commentary {
	return Object.freeze({ type: "commentary", text });
}

export function toolCallChunk(
	toolName: string,
	opts?: {
		preview?: string | undefined;
		args?: Record<string, unknown> | undefined;
		index?: number | undefined;
	},
): ToolCallChunk {
	return Object.freeze({
		type: "tool_call_chunk",
		toolName,
		preview: opts?.preview,
		args: opts?.args ? Object.freeze({ ...opts.args }) : undefined,
		index: opts?.index ?? 0,
	});
}

export function toolCallFinished(
	toolName: string,
	opts?: {
		durationSeconds?: number | undefined;
		ok?: boolean | undefined;
		index?: number | undefined;
	},
): ToolCallFinished {
	return Object.freeze({
		type: "tool_call_finished",
		toolName,
		durationSeconds: opts?.durationSeconds ?? 0,
		ok: opts?.ok ?? true,
		index: opts?.index ?? 0,
	});
}

export function longToolHint(
	toolName: string,
	durationSeconds: number,
): LongToolHint {
	return Object.freeze({ type: "long_tool_hint", toolName, durationSeconds });
}

export function gatewayNotice(
	notice: string,
	text?: string,
	extra?: Record<string, unknown>,
): GatewayNotice {
	return Object.freeze({
		type: "gateway_notice",
		notice,
		text: text ?? "",
		extra: Object.freeze({ ...(extra ?? {}) }),
	});
}
