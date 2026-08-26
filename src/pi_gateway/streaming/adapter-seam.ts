import type {
	Commentary,
	MessageChunk,
	MessageStop,
	ToolCallChunk,
} from "./stream-events.js";

// pi_gateway/streaming/adapter-seam — THE adapter stream seam (04 §1.1, §5.2).
//
// These are the TypeScript interfaces Phase-3 platform/relay adapters implement
// against. NOTHING here imports a concrete platform. Semantics ported from the
// READ-ONLY Hermes reference, cited as file:symbol anchors — no code vendored:
//   - gateway/platforms/base.py:send / edit_message / send_draft   (egress doors)
//   - gateway/platforms/base.py:supports_draft_streaming           (METHOD probe)
//   - gateway/platforms/base.py:stream_is_message_for_chat /
//       relay/adapter.py:draft_stream_is_message                   (DEC-006)
//   - gateway/platforms/base.py:SendResult(success, message_id, error,
//       retryable, retry_after)                                    (04 §1)
//   - gateway/platforms/base.py:render_message_event / format_tool_event
//   - gateway/stream_consumer.py:_metadata_for_send (`_interim_send` producer;
//       `expect_edits=not final` stamps preview sends so formatting-ladder
//       adapters skip the rich path)
//   - gateway/stream_consumer.py:_edit_message (routing metadata forwarded on
//       stream edits when the adapter's edit accepts it)
//   - gateway/stream_consumer.py:_suppress_silence_marker (best-effort
//       delete_message retract of a streamed silence marker)
//   - gateway/stream_consumer.py:_abandon_native_stream (optional best-effort
//       abandon_open_draft seal-in-place for stale-generation exits)
//   - gateway/stream_consumer.py:on_commentary (commentary enqueues FIFO —
//       ConsumerSink.onCommentary; direct sends reorder vs queued deltas)

/** Gateway→platform wire metadata (gateway-internal keys popped at the door). */
export type Metadata = Record<string, unknown>;

/**
 * Gateway-internal marker for any consumer-side send that is NOT the
 * turn-final (04 §5 invariant 3). Producers: commentary sends, tail flushes,
 * heartbeats/advisories. It MUST be popped/stripped by the egress-door
 * chokepoint before anything reaches the wire — an interim send never triggers
 * seal-interception.
 */
export const INTERIM_SEND_MARKER = "_interim_send";

/** Per-turn identity key stamped by _metadata_for_send parity paths. */
export const REPLY_TO_METADATA_KEY = "reply_to_message_id";

/**
 * SendResult — 04 §1 verbatim shape. `retryAfter` carries the
 * server-authoritative flood delay in seconds (Telegram FloodWait).
 */
export interface SendResult {
	success: boolean;
	messageId?: string | null | undefined;
	error?: string | null | undefined;
	retryable?: boolean | undefined;
	retryAfter?: number | null | undefined;
}

export interface DraftFrameArgs {
	chatId: string;
	draftId: number;
	content: string;
	metadata?: Metadata | undefined;
}

export interface EditOptions {
	finalize?: boolean | undefined;
	/**
	 * Routing metadata forwarded when the adapter's edit accepts it
	 * (stream_consumer.py:_edit_message — passes self.metadata so threaded
	 * platforms keep thread routing on stream edits).
	 */
	metadata?: Metadata | undefined;
}

/**
 * The slice of a platform/relay adapter the stream consumer and dispatcher
 * drive. Capability discovery is PER-CHAT METHOD PROBES, never class data
 * (DEC-006): `supportsDraftStreaming` and `streamIsMessageForChat` are probed,
 * `draftStreamIsMessage` exists only as the class-level fallback for adapters
 * predating the probe. Boolean capabilities resolve with `is True` discipline
 * (`=== true`) to stay MagicMock-safe (base.py::_stream_is_message).
 */
export interface StreamEgressAdapter {
	/** base.py:supports_draft_streaming — per-chat METHOD probe, default false. */
	supportsDraftStreaming?(
		chatType?: string | undefined,
		metadata?: Metadata | undefined,
		chatId?: string | number | undefined,
	): boolean;

	/**
	 * Per-chat probe PREFERRED by the consumer over the class attribute
	 * (relay/adapter.py:stream_is_message_for_chat; DEC-006).
	 */
	streamIsMessageForChat?(chatId: string): boolean;

	/** Class-level fallback only (relay/adapter.py construction). */
	draftStreamIsMessage?: boolean | undefined;

	/** base.py:REQUIRES_EDIT_FINALIZE — checked `is True`; forces finalize edits. */
	requiresEditFinalize?: boolean | undefined;

	/**
	 * DOOR 1 (04 §1.1). Every user-visible text send routes through here, which
	 * must carry the seal check via ONE audited chokepoint (§5.1).
	 */
	send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;

	/** Default edit support is "not supported" (base.py:edit_message default). */
	editMessage(
		chatId: string,
		messageId: string,
		content: string,
		opts?: EditOptions | undefined,
	): Promise<SendResult>;

	/** One cumulative native draft frame (base.py:send_draft). */
	sendDraft(args: DraftFrameArgs): Promise<SendResult>;

	/**
	 * Optional best-effort deletion (base.py:delete_message analogue). Present
	 * on adapters that CAN retract a message; the consumer's silence-marker
	 * suppression and stale-session preview retraction use it defensively and
	 * tolerate absence.
	 */
	deleteMessage?(chatId: string, messageId: string): Promise<unknown>;

	/**
	 * Optional best-effort native-stream abandonment
	 * (stream_consumer.py:_abandon_native_stream): seal the open draft IN
	 * PLACE with `content` — what is already on screen — so a stale-generation
	 * exit (/new, /stop) never leaves a live streaming indicator or armed
	 * interception state behind. Best-effort; absence is tolerated.
	 */
	abandonOpenDraft?(
		chatId: string,
		content: string,
		metadata?: Metadata | undefined,
	): Promise<unknown>;

	/**
	 * DOOR 2 (04 §1.1, finding #7): the delivery-resolver lane (queued
	 * follow-ups, media finals, scheduled sends) calls THIS directly, bypassing
	 * `send()`. The seal check must hold here too — both-door coverage.
	 */
	sendForPlatform?(
		logicalPlatform: string,
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
}

/** Resolved tool-progress mode for a channel ("new"-dedup is dispatcher-state). */
export type ToolProgressMode = "all" | "new" | "verbose" | "off";

/**
 * What render hooks drive message events onto (base.py:render_message_event
 * receives the sink = the GatewayStreamConsumer primitives). Commentary MUST
 * route through the QUEUING ingress (`onCommentary`) — a direct send would
 * reorder interim beats against deltas already queued in the DeltaQueue.
 */
export interface ConsumerSink {
	onDelta(text: string): void;
	onSegmentBreak(): void;
	onCommentary(text: string): void;
}

/**
 * Adapter render hooks (base.py defaults reproduce legacy behavior; adapters
 * override; returning null from formatToolEvent EATS the event on platforms
 * that cannot render tool chrome).
 */
export interface StreamRenderAdapter {
	renderMessageEvent?(
		event: MessageChunk | MessageStop | Commentary,
		sink: ConsumerSink,
	): void;
	formatToolEvent?(
		event: ToolCallChunk,
		opts: { mode: ToolProgressMode; previewMaxLen: number },
	): string | null | undefined;
}

// ── base-class DEFAULT render hooks (legacy-behavior parity) ─────────────────

/**
 * base.py:render_message_event default: map typed events 1:1 onto the
 * consumer's existing primitives. An intermediate stop is a segment break; the
 * terminal stop is signalled via finish(), NOT here.
 */
export function defaultRenderMessageEvent(
	event: MessageChunk | MessageStop | Commentary,
	sink: ConsumerSink,
): void {
	switch (event.type) {
		case "message_chunk":
			if (event.text) sink.onDelta(event.text);
			return;
		case "message_stop":
			if (!event.final) sink.onSegmentBreak();
			return;
		case "commentary":
			if (event.text) sink.onCommentary(event.text);
			return;
	}
}

const DEFAULT_PREVIEW_CAP = 40;

function capPreview(preview: string, previewMaxLen: number): string {
	const cap = previewMaxLen > 0 ? previewMaxLen : DEFAULT_PREVIEW_CAP;
	return preview.length > cap ? `${preview.slice(0, cap - 3)}...` : preview;
}

/**
 * base.py:format_tool_event default: historical tool-progress formatting —
 * gear emoji + tool name + short capped preview (verbose mode renders the full
 * args dict JSON, truncated with "..." when a positive cap is set). The emoji
 * table (agent/display.py:get_tool_emoji) is intentionally not ported into the
 * seam; every tool gets the ⚙️ default until an adapter overrides.
 */
export function defaultFormatToolEvent(
	event: ToolCallChunk,
	opts: { mode: ToolProgressMode; previewMaxLen: number },
): string | null {
	const emoji = "⚙️";
	if (opts.mode === "verbose") {
		if (event.args && Object.keys(event.args).length > 0) {
			let argsStr: string;
			try {
				argsStr = JSON.stringify(event.args) ?? "";
			} catch {
				argsStr = "";
			}
			if (opts.previewMaxLen > 0 && argsStr.length > opts.previewMaxLen) {
				argsStr = `${argsStr.slice(0, opts.previewMaxLen - 3)}...`;
			}
			return `${emoji} ${event.toolName}(${Object.keys(event.args)})\n${argsStr}`;
		}
		if (event.preview) return `${emoji} ${event.toolName}: "${event.preview}"`;
		return `${emoji} ${event.toolName}...`;
	}
	const preview = event.preview
		? capPreview(event.preview, opts.previewMaxLen)
		: "";
	return preview
		? `${emoji} ${event.toolName}: ${preview}`
		: `${emoji} ${event.toolName}...`;
}

/** Minimal structural logger (lifecycle's Logger is structurally compatible). */
export interface StreamLogger {
	debug(message: string, meta?: Record<string, unknown>): void;
	warn(message: string, meta?: Record<string, unknown>): void;
	error(message: string, meta?: Record<string, unknown>): void;
	info?(message: string, meta?: Record<string, unknown>): void;
}
