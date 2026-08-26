// pi_gateway/streaming/gateway-stream-consumer — the production stream
// consumer (04-platform-adapters.md §5.2; DEC-006).
//
// Ported from the READ-ONLY Hermes reference, semantics only, cited as
// file:symbol anchors — no code vendored:
//   - gateway/stream_consumer.py:GatewayStreamConsumer  (queue drain loop)
//   - gateway/stream_consumer.py:StreamConsumer.finish  (authoritative final, EXACTLY ONCE)
//   - gateway/stream_consumer.py:_stream_is_message / _resolve_draft_streaming
//   - gateway/stream_consumer.py:_send_draft_frame    (+ prefix-stability guard, invariant 1)
//   - gateway/stream_consumer.py:_send_or_edit        (edit-based preview path)
//   - gateway/stream_consumer.py:_reset_segment_state (tool-boundary segments)
//   - gateway/stream_consumer.py:_metadata_for_send   (`_interim_send` producer)
//   - gateway/stream_consumer.py:_suppress_silence_marker
//       (+ gateway/run.py is_intentional_silence_agent_result per-lane gate)
//       → turn-final delivery-disposition gate via outbound/response-filters
//       resolveDeliveryDisposition; exact-marker finals RETRACT the preview
//   - gateway/stream_consumer.py:run (_is_partial_silence_marker hold-back)
//       → mid-stream flushes defer while the buffer could still resolve to a
//       silence marker; got_done resolves it
//   - gateway/stream_consumer.py:_metadata_for_send(expect_edits=not final)
//       → preview sends stamp `expect_edits` for formatting-ladder adapters
//   - gateway/stream_consumer.py:_edit_message
//       → routing metadata forwarded on stream edits
//   - gateway/stream_consumer.py:_truncate_for_stream/_split_text_chunks/
//       _send_fallback_final → length-aware overflow sealing + fresh-send
//       continuation when progressive edits stop working (segmentation
//       primitive imported from outbound/segmentation, never duplicated)
//   - gateway/stream_consumer.py:_filter_and_accumulate/
//       _strip_orphan_close_tags/_flush_think_buffer
//       → consumer-side think-block scrubber behind onDelta
//   - gateway/stream_consumer.py:delivered_final_matches + has_delivered_text
//       (tri-state reconciliation; payload-less split delivery refuses legacy
//       trust #78541; fallback matching over segment/commentary records)
//   - gateway/stream_consumer.py:_clean_for_display
//       → outbound/media-grammar.ts:stripMediaDirectivesForDisplay applied at
//       EVERY emission boundary (frames, finals, commentary) and before every
//       silence/delivered-payload comparison — raw MEDIA:/[[audio_as_voice]]
//       directives never reach chats
//   - gateway/stream_consumer.py:ensure_closed_code_fences
//       → fence-balancing on persistent-surface composition (edit-path frames,
//       finalize, fallback continuation); draft frames stay UNfenced to keep
//       hard prefix stability (pi invariant 1 is stricter than Hermes, which
//       tolerates connector-side whole-snapshot re-append)
//   - gateway/stream_consumer.py:on_commentary + run() commentary branch
//       (_reset_segment_state around _send_commentary)
//       → commentary enqueues FIFO through DeltaQueue behind deltas; the drain
//       path delivers buffered prose, resets segment state around the send so
//       post-commentary prose opens a NEW bubble below
//   - gateway/stream_consumer.py:__init__ run_still_current + run()
//       early-abandon/_abandon_native_stream → injected staleness probe checked
//       each drain iteration; stale runs stop editing and best-effort seal or
//       retract the preview so post-/new or post-/stop deltas never edit into a
//       fresh session
//   - gateway/stream_consumer.py:flush_pending_sync/_FLUSH sentinel + run()
//       barrier handling → flush-barrier queue item + flushPendingSync(timeout)
//       blocking the worker until buffered prose delivers before interactive
//       prompts
//   - gateway/run.py:_interim_metadata                (commentary = interim lane)
//
// The four invariants (04 §5) this class enforces:
//   1. Draft frames are PREFIX-STABLE; a violating frame is DETECTED and the
//      draft lane is permanently disabled for the run (graceful degradation).
//   2. finish(final_text) declares the AUTHORITATIVE final; adoption REPLACES
//      the accumulator exactly once — never concatenates.
//   3. Interim sends carry `_interim_send`; the egress-door chokepoint pops it.
//   4. Beside an already-sealed stream, delivery reconciles BY EDIT via the
//      door's lane registry — never a second plain send.

import {
	INTERIM_SEND_MARKER,
	REPLY_TO_METADATA_KEY,
	type Metadata,
	type SendResult,
	type StreamEgressAdapter,
	type StreamLogger,
} from "./adapter-seam.js";
import { StreamingCapabilities } from "./capability.js";
import {
	isIntentionalSilenceResponse,
	isPartialSilenceMarker,
	resolveDeliveryDisposition,
	type DeliveryDisposition,
	type ResponseLane,
	type SilenceAgentResult,
} from "../outbound/response-filters.js";
import { stripMediaDirectivesForDisplay } from "../outbound/media-grammar.js";
import { segmentByOffsets } from "../outbound/segmentation.js";

/** Observable evidence that a draft frame mutated previously-emitted content. */
export interface PrefixViolation {
	kind: "non_prefix_frame";
	prevFrame: string;
	nextFrame: string;
}

export interface StreamConsumerConfig {
	/** Minimum ms between mid-stream flushes (stream_consumer.py:edit_interval). */
	editIntervalMs?: number | undefined;
	/** Buffered-growth chars required before a mid-stream flush (buffer_threshold). */
	bufferThreshold?: number | undefined;
	/** Streaming cursor suffix; stripped from every frame. Default none. */
	cursor?: string | undefined;
	/** Transport selection (StreamConsumerConfig.transport): auto prefers drafts. */
	transport?: "auto" | "draft" | "edit" | undefined;
	chatType?: string | undefined;
	/**
	 * Composition-time transform applied to the ACCUMULATED buffer when building
	 * each MID-STREAM DRAFT FRAME ONLY (never the finalize payload — only the
	 * finalize path may transform the real final, invariant 1/2). This seam
	 * models the historically banned transforms (fence-closing, cursor suffix,
	 * segment-state resets): a composeFrame that breaks prefix stability is the
	 * exact bug class the MUTATION contract test injects.
	 */
	composeFrame?: ((accumulated: string) => string) | undefined;
	/**
	 * REQUIRES_EDIT_FINALIZE override (base.py, checked `is True`); defaults to
	 * the adapter's flag. True forces the redundant final edit even when the
	 * preview already matches.
	 */
	requiresEditFinalize?: boolean | undefined;
	/**
	 * Shared per-chat capability latch (04 §5). Pass one resolver per adapter
	 * so probes latch across turns; defaults to a private resolver.
	 */
	capabilities?: StreamingCapabilities | undefined;
	/**
	 * Delivery lane for the turn-final disposition gate (se-1; default
	 * "interactive"). Interactive uses the EXACT-marker rule, cron/webhook the
	 * loose autonomous rule — gateway/run.py is_intentional_silence_agent_result
	 * parity.
	 */
	lane?: ResponseLane | undefined;
	/**
	 * Per-chat message length limit in UTF-16 code units (se-9;
	 * stream_consumer.py:_raw_message_limit analogue). 0/undefined disables
	 * overflow handling: buffers split into sealed head chunks with an active
	 * tail once they exceed it, and failed progressive edits fall back to a
	 * fresh continuation send instead of failing every flush forever.
	 */
	messageLimit?: number | undefined;
	/**
	 * Session-staleness probe (stream_consumer.py:__init__ run_still_current).
	 * Checked at the top of EVERY drain iteration; when it returns false (the
	 * session was reset by /new or /stop) the drain loop stops editing, seals
	 * or retracts the preview best-effort, and returns — stale deltas never
	 * edit into a fresh session. Defaults to "always current".
	 */
	runStillCurrent?: (() => boolean) | undefined;
	log?: StreamLogger | undefined;
	/** Injected clock for editIntervalMs decisions (flake discipline). */
	now?: (() => number) | undefined;
}

interface ResolvedConfig {
	editIntervalMs: number;
	bufferThreshold: number;
	cursor: string;
	transport: "auto" | "draft" | "edit";
	chatType: string | undefined;
	composeFrame: ((accumulated: string) => string) | undefined;
	requiresEditFinalize: boolean;
	lane: ResponseLane;
	messageLimit: number;
	runStillCurrent: () => boolean;
	now: () => number;
}

type QueueItem =
	| { kind: "delta"; text: string }
	| { kind: "segment-break" }
	| { kind: "commentary"; text: string }
	| { kind: "flush"; settle: () => void }
	| { kind: "final-text"; text: string }
	| { kind: "done" };

/** Worker-thread → drain-loop queue (queue.Queue parity). Single reader. */
class DeltaQueue {
	private buf: QueueItem[] = [];
	private closed = false;
	private wake: (() => void) | null = null;

	push(item: QueueItem): void {
		if (this.closed) return; // post-DONE stragglers are dropped
		this.buf.push(item);
		const w = this.wake;
		this.wake = null;
		w?.();
	}

	close(): void {
		this.closed = true;
		const w = this.wake;
		this.wake = null;
		w?.();
	}

	/** Resolve with every currently-buffered item (drain parity); null once closed+empty. */
	async nextBatch(): Promise<QueueItem[] | null> {
		for (;;) {
			if (this.buf.length > 0) {
				const batch = this.buf;
				this.buf = [];
				return batch;
			}
			if (this.closed) return null;
			await new Promise<void>((resolve) => {
				this.wake = resolve;
			});
		}
	}
}

// Class-wide monotonic counter for native-streaming draft ids, seeded from a
// RANDOM process nonce — parity of
// gateway/stream_consumer.py:GatewayStreamConsumer._draft_id_counter (the
// connector keys sealed-stream tombstones on this wire identity).
let draftIdCounter = Math.floor(Math.random() * 2 ** 49);

function nextDraftId(): number {
	draftIdCounter += 1;
	return draftIdCounter;
}

// ── think-block scrubber constants (se-12) ────────────────────────────
// Reasoning/thinking tags models emit inline in content. Port of
// stream_consumer.py:_OPEN_THINK_TAGS/_CLOSE_THINK_TAGS (must stay in sync
// with the CLI's _OPEN_TAGS/_CLOSE_TAGS and run_agent.py:_strip_think_blocks
// tag variants).
const OPEN_THINK_TAGS: readonly string[] = [
	"<REASONING_SCRATCHPAD>",
	"<think>",
	"<reasoning>",
	"<THINKING>",
	"<thinking>",
	"<thought>",
];
const CLOSE_THINK_TAGS: readonly string[] = [
	"</REASONING_SCRATCHPAD>",
	"</think>",
	"</reasoning>",
	"</THINKING>",
	"</thinking>",
	"</thought>",
];

/** Consecutive progressive-edit failures before edits are abandoned for the
 * turn and the fresh-send fallback takes over (_MAX_FLOOD_STRIKES parity). */
const MAX_EDIT_FAILURE_STRIKES = 3;

function maxThinkTagLength(): number {
	return Math.max(
		...[...OPEN_THINK_TAGS, ...CLOSE_THINK_TAGS].map((t) => t.length),
	);
}

/**
 * Remove close tags with no matching open (stream_consumer.py:
 * _strip_orphan_close_tags — mirrors agent/think_scrubber.py). An orphan
 * close tag is always noise — stripped along with any trailing whitespace so
 * surrounding prose flows naturally.
 */
function stripOrphanCloseTags(text: string): string {
	if (!text.includes("</")) return text;
	const lower = text.toLowerCase();
	let out = "";
	let i = 0;
	while (i < text.length) {
		let matched = false;
		if (lower.slice(i, i + 2) === "</") {
			for (const tag of CLOSE_THINK_TAGS) {
				const tagLower = tag.toLowerCase();
				if (lower.startsWith(tagLower, i)) {
					let j = i + tagLower.length;
					while (j < text.length && " \t\n\r".includes(text[j] as string)) {
						j += 1;
					}
					i = j;
					matched = true;
					break;
				}
			}
		}
		if (!matched) {
			out += text[i];
			i += 1;
		}
	}
	return out;
}

/**
 * Append a closing fence/backtick when markers are orphaned. Port of
 * stream_consumer.py:ensure_closed_code_fences: output truncated mid-code-
 * block (finish_reason="length") leaves an unclosed ``` which renders the
 * ENTIRE remainder as one code block on Discord/Slack/Matrix; an orphaned
 * single backtick does the same for inline code.
 *
 * Triple-backtick: odd ``` count appends a closing fence on its own line (a
 * stray extra fence costs a brief empty block — far cheaper than the whole
 * message becoming one). Inline: after stripping complete ```…``` regions
 * (and any trailing unclosed ```), an odd standalone ` count appends a
 * closing backtick.
 */
export function ensureClosedCodeFences(text: string): string {
	if (!text) return text;
	// Step 1: balance triple-backtick code-block fences.
	if ((text.split("```").length - 1) % 2 === 1) {
		text = `${text.replace(/\n+$/, "")}\n\`\`\``;
	}
	// Step 2: balance standalone inline-code backticks OUTSIDE complete fence
	// regions (their internal backticks must not pollute the count).
	const withoutFences = text
		.replace(/```[\s\S]*?```/g, "")
		.replace(/```[^`]*$/, "");
	if ((withoutFences.split("`").length - 1) % 2 === 1) {
		text += "`";
	}
	return text;
}

/**
 * Length-aware splitter for overflow chunks (se-9): delegates to the ONE
 * offset-safe segmentation primitive (outbound/segmentation.ts) so protected
 * spans (fenced code, inline code, quotes) are never cut. Never returns an
 * empty list.
 */
function splitOverflowChunks(text: string, limit: number): string[] {
	const plan = segmentByOffsets(text, limit);
	const chunks = plan.segments.map((s) => s.text);
	return chunks.length > 0 ? chunks : [text];
}

export class GatewayStreamConsumer {
	private readonly adapter: StreamEgressAdapter;
	private readonly chatId: string;
	private readonly cfg: ResolvedConfig;
	private readonly caps: StreamingCapabilities;
	private readonly log: StreamLogger | undefined;
	private readonly metadata: Metadata | undefined;
	private readonly initialReplyToId: string | undefined;
	private readonly queue = new DeltaQueue();

	// Drain-loop state (field names mirror stream_consumer.py).
	private accumulated = "";
	private streamLedger = "";
	private lastSentText = ""; // last emitted frame/message THIS segment (detection baseline)
	private messageId: string | null = null;
	// Drafts do NOT set this — it gates the gateway's fallback final-send path
	// (stream_consumer.py:_send_or_edit comment).
	private alreadySentInternal = false;
	private useDraftStreaming = false;
	private draftId: number | null = null;
	private gotDone = false;
	private finished = false; // finish() latch — absorbed EXACTLY once
	private finalAdopted = false; // authoritative-final adoption latch
	private lastFlushAt = Number.NEGATIVE_INFINITY;
	private deliveredSegmentTexts: string[] = [];
	private deliveredCommentaryTexts: string[] = [];
	private deliveredFinalText: string | null = null;

	/** Live flushPendingSync waiters; settled by the drain loop or the run()
	 * exit sweep (stream_consumer.py:_signal_flush/finally parity). */
	private readonly pendingFlushSettles = new Set<() => void>();

	// Think-block scrubber state (_in_think_block/_think_buffer parity).
	private inThinkBlock = false;
	private thinkBuffer = "";

	// Fallback-final machinery (stream_consumer.py:_fallback_final_send parity):
	// progressive edits gave up for this turn; the final goes out as a fresh
	// continuation send carrying only the UNSEEN tail.
	private fallbackFinalSend = false;
	private editFailureStrikes = 0;

	/** Turn result carried into the disposition gate (finish() meta). */
	private turnAgentResult: SilenceAgentResult | null = null;
	/** Disposition computed for the most recent turn-final (null before). */
	private dispositionInternal: DeliveryDisposition | null = null;
	/** True when this turn delivered sealed overflow head chunks (split delivery). */
	private splitDeliveryInternal = false;
	/**
	 * Wire identity the ACTIVE tail threads onto once overflow heads are
	 * sealed (_send_new_chunk reply-chain parity): each sealed piece gets a
	 * distinct turn identity so the door's lane registry never collapses them.
	 */
	private overflowChainTail: string | undefined;

	// Runner-read properties after drain (04 §5.2 sketch).
	private finalResponseSentInternal = false;
	private finalContentDeliveredInternal = false;

	/** Observable non-prefix-stability detections (invariant 1 enforcement). */
	readonly prefixViolations: PrefixViolation[] = [];

	constructor(
		adapter: StreamEgressAdapter,
		chatId: string,
		config?: StreamConsumerConfig | undefined,
		metadata?: Metadata | undefined,
		initialReplyToId?: string | undefined,
	) {
		this.adapter = adapter;
		this.chatId = chatId;
		this.cfg = {
			editIntervalMs: config?.editIntervalMs ?? 0,
			bufferThreshold: config?.bufferThreshold ?? 1,
			cursor: config?.cursor ?? "",
			transport: config?.transport ?? "auto",
			chatType: config?.chatType,
			composeFrame: config?.composeFrame,
			requiresEditFinalize:
				(config?.requiresEditFinalize ?? adapter.requiresEditFinalize) === true,
			lane: config?.lane ?? "interactive",
			messageLimit: config?.messageLimit ?? 0,
			runStillCurrent: config?.runStillCurrent ?? (() => true),
			now: config?.now ?? (() => Date.now()),
		};
		this.log = config?.log;
		this.caps =
			config?.capabilities ?? new StreamingCapabilities(adapter, this.log);
		this.metadata = metadata;
		this.initialReplyToId = initialReplyToId;
	}

	// ── probes (latched per chat; DEC-006 method probes) ──────────────────

	/** Whether THIS chat's transport treats the stream as the message. */
	private streamIsMessage(): boolean {
		return this.caps.streamIsMessage(String(this.chatId));
	}

	/**
	 * Transport gate. Port of _resolve_draft_streaming: capability via per-chat
	 * METHOD probe (base.py:supports_draft_streaming), "edit"/"off" never drafts.
	 */
	private resolveDraftStreaming(): void {
		if (this.cfg.transport === "edit") return;
		const supported = this.caps.supportsDraftStreaming(
			this.cfg.chatType,
			this.metadata,
			this.chatId,
		);
		if (supported) {
			this.useDraftStreaming = true;
			this.draftId = nextDraftId();
		} else if (this.cfg.transport === "draft") {
			this.log?.debug(
				"draft streaming requested but unsupported — falling back to edit",
				{ chatId: String(this.chatId) },
			);
		}
	}

	// ── ingress (worker-thread side) ──────────────────────────────────────

	/**
	 * Thread-safe delta callback. `null` signals a tool boundary / segment break
	 * (stream_consumer.py:on_delta). Post-finish stragglers are dropped: the
	 * turn is over and their bytes must never reach the wire.
	 */
	onDelta(text: string | null): void {
		if (this.finished) return;
		if (text === null) {
			this.queue.push({ kind: "segment-break" });
			return;
		}
		if (text === "") return; // falsy-but-not-None ignored (on_delta parity)
		this.queue.push({ kind: "delta", text });
	}

	/** Explicit segment-break primitive for render hooks (sink contract). */
	onSegmentBreak(): void {
		this.onDelta(null);
	}

	/**
	 * Thread-safe commentary ingress (stream_consumer.py:on_commentary). The
	 * text ENQUEUES FIFO behind any pending deltas — it must never jump the
	 * queue with a direct send, which would reorder interim beats against
	 * buffered prose. Post-finish stragglers are dropped like deltas.
	 */
	onCommentary(text: string): void {
		if (!text) return;
		this.queue.push({ kind: "commentary", text });
	}

	/**
	 * Block the calling (agent-worker) context until everything queued BEFORE
	 * this point has been finalized and delivered to the platform. Port of
	 * stream_consumer.py:flush_pending_sync: enqueues a flush-barrier item
	 * (_FLUSH sentinel parity) behind pending deltas/commentary/segment
	 * breaks; the drain loop delivers the buffered segment, then settles the
	 * barrier. Needed before sending a blocking interactive prompt so the
	 * prompt lands BELOW its buffered explanation instead of racing ahead of
	 * it. Resolves true when the barrier was consumed within `timeoutMs`
	 * (default 5000), false on timeout or run exit before consumption — the
	 * caller continues either way, never hangs.
	 */
	flushPendingSync(timeoutMs = 5000): Promise<boolean> {
		const budget = Math.max(0, timeoutMs);
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const settle = (): void => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				this.pendingFlushSettles.delete(settle);
				resolve(true);
			};
			const timer = setTimeout(() => {
				if (settled) return;
				settled = true;
				this.pendingFlushSettles.delete(settle);
				resolve(false);
			}, budget);
			(timer as unknown as { unref?: () => void }).unref?.();
			this.pendingFlushSettles.add(settle);
			// A closed queue drops the item; the run-exit sweep below settles
			// registered waiters so the caller wakes immediately.
			this.queue.push({ kind: "flush", settle });
		});
	}

	/** Settle every live flush waiter (run-exit safety net; finally parity). */
	private settleAllFlushes(): void {
		const waiters = [...this.pendingFlushSettles];
		this.pendingFlushSettles.clear();
		for (const settle of waiters) settle();
	}

	/**
	 * Signal completion. `finalText`, when provided, is the AUTHORITATIVE
	 * completed final_response — including post-stream augmentation the
	 * accumulator never saw — and is absorbed EXACTLY ONCE (latch below).
	 * `meta.agentResult` feeds the turn-final delivery-disposition gate (se-1:
	 * failed turns always deliver their errors); the FIRST finish wins.
	 * Port of stream_consumer.py:StreamConsumer.finish; bare finish() keeps
	 * legacy behavior. Racing/double calls are inert.
	 */
	finish(
		finalText?: string | undefined,
		meta?: { agentResult?: SilenceAgentResult | null | undefined },
	): void {
		if (this.finished) return; // second/racing finish is inert
		this.finished = true;
		if (meta?.agentResult != null) this.turnAgentResult = meta.agentResult;
		if (finalText !== undefined) {
			this.queue.push({ kind: "final-text", text: finalText });
		}
		this.queue.push({ kind: "done" });
		this.queue.close();
	}

	// ── runner-read properties (04 §5.2) ──────────────────────────────────

	get alreadySent(): boolean {
		return this.alreadySentInternal;
	}

	get finalResponseSent(): boolean {
		return this.finalResponseSentInternal;
	}

	get finalContentDelivered(): boolean {
		return this.finalContentDeliveredInternal;
	}

	get message_id(): string | null {
		return this.messageId;
	}

	get draftIdUsed(): number | null {
		return this.draftId;
	}

	get deliveredSegments(): readonly string[] {
		return this.deliveredSegmentTexts;
	}

	get deliveredCommentary(): readonly string[] {
		return this.deliveredCommentaryTexts;
	}

	/**
	 * Whether the recorded turn-final payload reconciles with `finalText`.
	 * Tri-state port of stream_consumer.py:delivered_final_matches:
	 *   - true  — the recorded payload, a delivered segment/commentary, or the
	 *     visible prefix matches; suppressing the normal final send is safe.
	 *   - false — a turn-final delivery was recorded but demonstrably differs,
	 *     OR this was a payload-less split delivery (#78541: the flag alone
	 *     must never suppress the normal final send).
	 *   - null  — nothing comparable was recorded on a non-split path; legacy
	 *     flag-trusting rules apply upstream.
	 * Both sides normalize identically (display-clean + fence-close + trim) so
	 * raw payloads reconcile against what actually reached the screen.
	 */
	deliveredFinalMatches(finalText: string): boolean | null {
		const target = this.normalizeDelivered(finalText ?? "");
		if (target === "") return null;
		if (this.deliveredFinalText === null) {
			// Payload-less split delivery must NOT inherit legacy trust (#78541
			// — that combination swallowed complete replies after an early
			// multi-message delivery).
			if (this.splitDeliveryInternal) return false;
			return null;
		}
		if (this.deliveredFinalText === target) return true;
		// A segment break / commentary may have delivered the text earlier in
		// the turn under a different record (has_delivered_text parity).
		if (this.hasDeliveredText(finalText)) return true;
		return false;
	}

	/**
	 * Whether `text` already reached the user as visible chat content. Port of
	 * stream_consumer.py:has_delivered_text: checks the visible prefix and
	 * every recorded segment/commentary text (records survive segment resets
	 * precisely so this reconciliation keeps working — #65919 review).
	 */
	hasDeliveredText(text: string): boolean {
		const target = this.cleanForDisplay(text ?? "").trim();
		if (target === "") return false;
		if (this.visiblePrefix().trim() === target) return true;
		return [
			...this.deliveredCommentaryTexts,
			...this.deliveredSegmentTexts,
		].some((sent) => sent.trim() === target);
	}

	/** Visible text already shown in the streamed message (_visible_prefix). */
	private visiblePrefix(): string {
		let prefix = this.lastSentText;
		const { cursor } = this.cfg;
		if (cursor.length > 0 && prefix.endsWith(cursor)) {
			prefix = prefix.slice(0, -cursor.length);
		}
		return this.cleanForDisplay(prefix);
	}

	/** Display-side directive stripper (_clean_for_display parity). MEDIA:
	 * tags / [[audio_as_voice]] / [[as_document]] directives are meant for the
	 * adapter's post-processing — the files deliver separately, and the raw
	 * directives must never reach chats. */
	private cleanForDisplay(text: string): string {
		return stripMediaDirectivesForDisplay(text);
	}

	/** Outgoing persistent-surface composition: display-clean THEN fence-
	 * close (stream_consumer.py:_send_or_edit composition order). Draft frames
	 * deliberately use cleanForDisplay ONLY — see composeDraftFrame. */
	private displayReady(text: string): string {
		return ensureClosedCodeFences(this.cleanForDisplay(text));
	}

	/** Recorded-payload normalization (_record_turn_final_payload parity). */
	private normalizeDelivered(text: string): string {
		return this.displayReady(text).trim();
	}

	/** Disposition computed at finalizeTurn for observability/tests (se-1). */
	get turnDisposition(): DeliveryDisposition | null {
		return this.dispositionInternal;
	}

	/** True when this turn sealed overflow head chunks (multi-message delivery). */
	get turnSplitDelivery(): boolean {
		return this.splitDeliveryInternal;
	}

	// ── drain loop (stream_consumer.py:GatewayStreamConsumer.run) ─────────

	async run(): Promise<void> {
		this.resolveDraftStreaming();
		try {
			for (;;) {
				// Session-staleness guard (stream_consumer.py:run — abandon the
				// stream early when the session was reset, e.g. /new or /stop,
				// so stale deltas are never edited/delivered after the user has
				// already moved on).
				if (!this.cfg.runStillCurrent()) {
					await this.abandonStream();
					return;
				}
				const batch = await this.queue.nextBatch();
				if (batch === null) break; // closed without DONE (abandoned run)
				// Re-check staleness WITH the wake-up: a session reset that landed
				// while the drain was parked must win over the freshly dequeued
				// batch — stale deltas are dropped, never delivered.
				if (!this.cfg.runStillCurrent()) {
					await this.abandonStream();
					return;
				}
				let dirty = false;
				for (const item of batch) {
					switch (item.kind) {
						case "delta": {
							// _filter_and_accumulate parity (se-12): buffer through the
							// think-block state machine so reasoning tags never display.
							this.filterAndAccumulate(item.text);
							dirty = true;
							break;
						}
						case "segment-break": {
							await this.finalizeSegment();
							break;
						}
						case "commentary": {
							await this.deliverCommentary(item.text);
							break;
						}
						case "flush": {
							// _FLUSH barrier: deliver everything buffered BEFORE the
							// marker (forced — interval/threshold gates do not apply),
							// close the segment like a tool boundary, then wake the
							// blocked worker so its interactive prompt lands BELOW the
							// buffered prose (flush_pending_sync ordering contract).
							await this.flushCurrent();
							await this.finalizeSegment();
							item.settle();
							break;
						}
						case "final-text": {
							this.adoptAuthoritativeFinal(item.text);
							break;
						}
						case "done": {
							this.gotDone = true;
							break;
						}
					}
					if (this.gotDone) break;
				}
				if (this.gotDone) {
					// Flush any held-back partial-tag buffer on stream end so trailing
					// text waiting on a possible open tag is not lost (_flush_think_buffer).
					this.flushThinkBuffer();
					await this.finalizeTurn();
					return;
				}
				if (dirty && this.shouldFlushNow()) {
					this.lastFlushAt = this.nowMs();
					await this.flushCurrent();
				}
			}
		} finally {
			// Safety net: if run() exits while a flush barrier is still queued or
			// was pushed but can never be consumed (abandoned/stale run), wake the
			// waiters now instead of letting them stall the full timeout
			// (stream_consumer.py:run finally-block parity).
			this.settleAllFlushes();
		}
	}

	private nowMs(): number {
		return this.cfg.now();
	}

	private shouldFlushNow(): boolean {
		const growth = this.accumulated.length - this.lastSentText.length;
		if (
			!(
				growth >= this.cfg.bufferThreshold &&
				this.nowMs() - this.lastFlushAt >= this.cfg.editIntervalMs
			)
		) {
			return false;
		}
		// Hold-back mid-stream flushes while the buffer could STILL resolve to
		// an intentional-silence marker (stream_consumer.py:run — without this a
		// partial marker like "NO"→"NO_REPLY" would flash onto the screen on an
		// interval tick before got_done suppresses it). Compared against the
		// DISPLAY-CLEANED buffer: a MEDIA: directive must not mask a marker that
		// would be visible after cleaning. Only defers display:
		// got_done always resolves the buffer, so genuine prose that merely
		// starts marker-like is never lost.
		return !isPartialSilenceMarker(
			this.cleanForDisplay(this.stripCursor(this.accumulated)),
		);
	}

	// ── frame emission (_send_or_edit + _send_draft_frame) ────────────────

	private async flushCurrent(): Promise<void> {
		// (se-9) Length-aware overflow: seal head chunks as fixed messages and
		// keep only the bounded tail active before the normal frame paths run.
		if (!(await this.sealOverflowHeads())) return;
		// Mid-turn boundaries on stream-is-message adapters emit another
		// cumulative frame — only the turn-final seals (finding #5). The
		// turn-final itself is delivered by finalizeTurn(), never here.
		if (this.useDraftStreaming && this.messageId === null) {
			const ok = await this.sendDraftFrame(this.composeDraftFrame());
			if (ok) return; // drafts don't set already_sent (fallback-send gating)
			// Failure (incl. detected prefix violation) permanently disabled the
			// draft lane; traffic reroutes through the edit-based path below
			// (graceful degradation, 04 §5 verified behaviors).
		}
		const frame = this.displayReady(this.stripCursor(this.accumulated));
		if (!frame.trim() || frame === this.lastSentText) return;
		if (this.messageId === null) {
			// First send of the edit-based preview. NOTE: this send goes through
			// the adapter door UNMARKED — on a stream-is-message chat with an
			// armed open draft the DOOR seal-intercepts it, converting the live
			// stream into a normal editable message (04 §5.1). expect_edits is
			// stamped so ladder adapters skip the rich path (_metadata_for_send:
			// expect_edits=not final parity). After a split delivery the fresh
			// tail threads onto the sealed chain so it gets its OWN turn lane.
			const res = await this.adapter.send(
				this.chatId,
				frame,
				this.overflowChainTail,
				this.metadataForSend({
					expectEdits: true,
					...(this.overflowChainTail !== undefined
						? { replyToOverride: this.overflowChainTail }
						: {}),
				}),
			);
			if (res.success) {
				this.messageId = res.messageId ?? null;
				if (res.messageId !== undefined && res.messageId !== null) {
					this.overflowChainTail = res.messageId;
				}
				this.lastSentText = frame;
				this.alreadySentInternal = true;
				this.editFailureStrikes = 0;
			}
		} else if (frame !== this.lastSentText) {
			// Routing metadata rides on stream edits when the adapter accepts it
			// (stream_consumer.py:_edit_message — threaded platforms keep their
			// thread root).
			const res = await this.adapter.editMessage(
				this.chatId,
				this.messageId,
				frame,
				{ metadata: this.metadata },
			);
			if (res.success) {
				this.lastSentText = frame;
				this.alreadySentInternal = true;
				this.editFailureStrikes = 0;
			} else {
				// Consecutive progressive-edit failures: after MAX_EDIT_FAILURE_
				// STRIKES enter fallback mode — stop editing and deliver only the
				// missing tail once the final is available (stream_consumer.py:
				// _send_or_edit fallback-mode entry, _MAX_FLOOD_STRIKES parity).
				this.editFailureStrikes += 1;
				if (this.editFailureStrikes >= MAX_EDIT_FAILURE_STRIKES) {
					this.fallbackFinalSend = true;
				}
			}
		}
	}

	/**
	 * Send ONE sealed split-delivery piece threaded onto `parent`
	 * (_send_new_chunk parity: chunks thread reply-to each other so every
	 * piece carries a distinct turn identity). Returns the new message id, or
	 * null when the piece failed to land.
	 */
	private async sendSealedPiece(
		piece: string,
		parent: string | undefined,
		opts: { final: boolean },
	): Promise<string | null> {
		// _send_new_chunk parity: sealed chunks are display-cleaned at the
		// emission boundary (fence-balancing stays the splitter's job).
		const text = this.cleanForDisplay(piece);
		const res = await this.adapter.send(
			this.chatId,
			text,
			parent,
			this.metadataForSend({
				final: opts.final,
				expectEdits: !opts.final,
				...(parent !== undefined ? { replyToOverride: parent } : {}),
			}),
		);
		return res.success ? (res.messageId ?? null) : null;
	}

	/** Chain parent for the next sealed piece (reply-chain root). */
	private overflowChainParent(): string | undefined {
		return (
			this.overflowChainTail ??
			this.initialReplyToId ??
			this.messageId ??
			undefined
		);
	}

	/**
	 * (se-9) Overflow branch of stream_consumer.py:run — when the buffer
	 * exceeds the per-chat limit, seal the overflowing head chunks as fixed
	 * messages (threaded into a reply chain) and keep ONLY the trailing chunk
	 * in `accumulated` so the normal send/edit path keeps updating the active
	 * preview in place. Returns false when this tick must stop (a sealed head
	 * failed to land — the full accumulated text stays intact so the fallback
	 * path can still deliver completely, #78541 parity). Without ANY turn
	 * identity to thread onto, sealing is skipped and the legacy single-send
	 * path runs (never fabricate wire identities).
	 */
	private async sealOverflowHeads(): Promise<boolean> {
		const limit = this.cfg.messageLimit;
		if (limit <= 0) return true;
		while (this.accumulated.length > limit) {
			const parent = this.overflowChainParent();
			if (parent === undefined) return true;
			const chunks = splitOverflowChunks(this.accumulated, limit);
			if (chunks.length <= 1) return true; // unsplittable — normal path tries
			let cursor = parent;
			for (const head of chunks.slice(0, -1)) {
				const id = await this.sendSealedPiece(head, cursor, {
					final: false,
				});
				if (id === null) {
					this.fallbackFinalSend = true;
					return false;
				}
				this.splitDeliveryInternal = true;
				cursor = id;
			}
			this.overflowChainTail = cursor;
			this.accumulated = chunks[chunks.length - 1] as string;
			// The heads are sealed. Clear the edit target so the remaining tail
			// becomes a fresh active chunk edited by subsequent deltas.
			this.lastSentText = "";
			this.messageId = null;
		}
		return true;
	}

	/**
	 * Emit one cumulative draft frame. Port of
	 * stream_consumer.py:_send_draft_frame plus the PREFIX-STABILITY GUARD:
	 * frame N must be a string prefix of frame N+1 (invariant 1). A violating
	 * frame is DETECTED here — recorded in `prefixViolations` — and the draft
	 * lane is PERMANENTLY disabled for the remainder of the run (graceful
	 * degradation); the caller falls through to the edit-based path. Removing
	 * the guard makes the violation invisible and leaves the draft lane armed,
	 * which is exactly what the MUTATION contract test asserts against.
	 */
	private async sendDraftFrame(text: string): Promise<boolean> {
		if (!this.useDraftStreaming || this.draftId === null) return false;
		// No-op skip: identical to the last frame we sent (_send_or_edit draft
		// branch parity) — forced flushes from commentary/barrier delivery must
		// not re-emit an unchanged cumulative snapshot.
		if (text === this.lastSentText) return true;
		if (this.lastSentText !== "" && !text.startsWith(this.lastSentText)) {
			this.prefixViolations.push({
				kind: "non_prefix_frame",
				prevFrame: this.lastSentText,
				nextFrame: text,
			});
			this.useDraftStreaming = false; // permanent disable for this run
			return false;
		}
		// Carry the per-turn identity on EVERY frame (_send_draft_frame review
		// B2 parity) so the final can find the open stream.
		const res = await this.adapter.sendDraft({
			chatId: this.chatId,
			draftId: this.draftId,
			content: text,
			metadata: this.metadataOrUndefined(),
		});
		if (!res.success) {
			this.useDraftStreaming = false; // failure disables drafts mid-response
			return false;
		}
		this.lastSentText = text; // detection baseline advances on real frames only
		return true;
	}

	/**
	 * Composition-time draft-frame build (composeFrame seam + display-clean +
	 * cursor strip). Media directives are stripped from EVERY frame, but draft
	 * frames stay UNFENCED: appending a closing ``` to a mid-code-block frame
	 * would make frame N not a prefix of frame N+1 and trip the hard prefix-
	 * stability guard (invariant 1) — Hermes tolerates that via connector-side
	 * whole-snapshot re-append; pi disables the lane instead. Native streams
	 * render unclosed fences progressively; the finalize path fence-closes the
	 * real final message (_pre_fence_text parity, generalized to both draft
	 * shapes for pi's stricter invariant).
	 */
	private composeDraftFrame(): string {
		const raw = this.cfg.composeFrame?.(this.accumulated) ?? this.accumulated;
		return this.stripCursor(this.cleanForDisplay(raw));
	}

	private stripCursor(text: string): string {
		const { cursor } = this.cfg;
		if (cursor.length > 0 && text.endsWith(cursor)) {
			return text.slice(0, -cursor.length);
		}
		return text;
	}

	// ── segments (stream_consumer.py:_reset_segment_state) ────────────────

	private async finalizeSegment(): Promise<void> {
		if (this.lastSentText !== "") {
			const finalized = this.lastSentText.trim();
			if (finalized !== "") this.deliveredSegmentTexts.push(finalized);
		}
		// Stream-is-the-message adapters keep ONE stream per turn and the
		// boundary emits NOTHING (findings #4/#5): the connector appends the
		// fresh segment whole. Edit-path adapters finalize the segment as a
		// real message first.
		if (!this.streamIsMessage() && this.messageId !== null) {
			const frame = this.displayReady(this.stripCursor(this.accumulated));
			if (frame.trim() !== "" && frame !== this.lastSentText) {
				await this.adapter.editMessage(this.chatId, this.messageId, frame, {
					finalize: true,
					metadata: this.metadata,
				});
			}
		}
		this.accumulated = "";
		this.streamLedger = "";
		this.lastSentText = ""; // detection baseline resets: a fresh segment is
		// legitimately NOT a prefix-extension of the previous wire frame
		this.messageId = null;
		// A segment boundary re-opens progressive editing: the failed message is
		// behind us (stream_consumer.py resets fallback state on segment break).
		this.fallbackFinalSend = false;
		this.editFailureStrikes = 0;
		// draft_id bump ONLY for Telegram-shaped adapters (finding #4):
		if (this.useDraftStreaming && !this.streamIsMessage()) {
			this.draftId = nextDraftId();
		}
	}

	// ── finalization ──────────────────────────────────────────────────────

	/**
	 * Adopt the authoritative final EXACTLY ONCE. Port of the _FINAL_TEXT
	 * handler in stream_consumer.py:GatewayStreamConsumer.run: adoption
	 * REPLACES the accumulator (it must never concatenate — the payload is the
	 * complete response) and fires only when this consumer actually streamed
	 * something (a no-stream turn keeps gateway-owned delivery).
	 */
	private adoptAuthoritativeFinal(text: string): void {
		if (this.finalAdopted) return; // EXACTLY ONCE
		this.finalAdopted = true;
		const streamedSomething =
			this.accumulated !== "" ||
			this.messageId !== null ||
			this.lastSentText !== "";
		if (!streamedSomething) return;
		// Split-stable reconciliation (_FINAL_TEXT handler parity): when sealed
		// overflow heads are already on the wire, the streamLedger holds every
		// delivered byte while `accumulated` holds only the active tail. An
		// authoritative payload that EXTENDS the ledger absorbs just the suffix
		// (sealed chunks are never duplicated); one EQUAL to it leaves the tail
		// untouched so finalize only reconciles the last chunk.
		if (
			this.splitDeliveryInternal &&
			this.streamLedger !== "" &&
			text.startsWith(this.streamLedger)
		) {
			const delivered = this.streamLedger;
			this.streamLedger = text;
			if (text.length > delivered.length) {
				this.accumulated += text.slice(delivered.length);
			}
			return;
		}
		this.accumulated = text;
		this.streamLedger = text;
	}

	private async finalizeTurn(): Promise<void> {
		// Only the finalize path may transform the real final — and the
		// authoritative payload rides INSIDE the seal/final edit verbatim
		// (invariants 1+2; live finding #11). The DISPLAY form is display-
		// cleaned first (media directives never reach chats, including in the
		// silence gate below — _is_intentional_silence_response(
		// _clean_for_display(...)) parity); fence-closing happens per delivered
		// piece further down (_send_or_edit composition order).
		const finalText = this.cleanForDisplay(this.stripCursor(this.accumulated));
		if (finalText.trim() === "") return;

		// (se-1) Turn-final delivery-disposition gate (gateway/run.py per-lane
		// silence filtering): an intentional-silence marker RETRACTS any streamed
		// preview instead of finalizing it (_suppress_silence_marker) so raw
		// markers never reach chats. Failed turns always deliver their errors.
		const disposition = resolveDeliveryDisposition({
			lane: this.cfg.lane,
			response: finalText,
			agentResult: this.turnAgentResult,
		});
		this.dispositionInternal = disposition;
		if (!disposition.deliver) {
			this.suppressSilenceMarker();
			return;
		}

		// (se-9) Oversized finals: split into sealed head chunks with an active
		// tail (_truncate_for_stream/_split_text_chunks overflow branches), then
		// finalize the bounded tail through the normal paths below.
		const tail = await this.sealFinalOverflow(finalText);

		if (this.messageId !== null) {
			// Already-on-screen preview: skip a verbatim finalize edit unless the
			// content changed — UNLESS the adapter REQUIRES_EDIT_FINALIZE (checked
			// `is True`), which forces the redundant edit through. Comparison uses
			// the fence-closed form: lastSentText always stores exactly what was
			// emitted (already fenced on the edit path).
			const closedTail = this.displayReady(tail);
			if (closedTail === this.lastSentText && !this.cfg.requiresEditFinalize) {
				this.markTurnFinalDelivered(tail);
				return;
			}
			if (this.fallbackFinalSend) {
				// Progressive edits stopped working earlier in the stream: send
				// only the unseen continuation as a fresh message instead of
				// letting a doomed edit drop the tail (_send_fallback_final).
				await this.sendFallbackContinuation(finalText);
				return;
			}
			const res = await this.adapter.editMessage(
				this.chatId,
				this.messageId,
				closedTail,
				{ finalize: true, metadata: this.metadata },
			);
			if (res.success) {
				this.markTurnFinalDelivered(tail);
			} else {
				// The final edit attempt itself may be the one that exhausts the
				// strike budget — retry once via the fresh-send continuation so a
				// full-response fallback is never left pending (#78541 class).
				await this.sendFallbackContinuation(finalText);
			}
			return;
		}
		if (
			this.accumulated !== "" ||
			this.lastSentText !== "" ||
			this.alreadySentInternal ||
			this.splitDeliveryInternal
		) {
			// Regular sendMessage — the ADAPTER DOOR seal-intercepts it for
			// stream-is-message chats (04 §5.1; relay/adapter.py:send). Split
			// deliveries thread the bounded tail onto the sealed chain so it gets
			// its own turn lane instead of reconciling into a sealed head.
			const res = await this.adapter.send(
				this.chatId,
				this.displayReady(tail),
				this.overflowChainTail,
				this.metadataForSend({
					final: true,
					...(this.overflowChainTail !== undefined
						? { replyToOverride: this.overflowChainTail }
						: {}),
				}),
			);
			if (res.success) {
				// The door's seal result carries the stream's wire identity
				// (_seal_open_draft parity: the connector returns the stream ts).
				if (res.messageId !== undefined && res.messageId !== null) {
					this.messageId = res.messageId;
				}
				this.markTurnFinalDelivered(tail);
			}
		}
		// else: no-stream turn — delivery ownership stays with the gateway
		// (test_stream_final_contract.py:TestFinalAdoptionGuards parity).
	}

	private markTurnFinalDelivered(text: string): void {
		// Record the NORMALIZED display form (_record_turn_final_payload parity:
		// media-strip + fence-close + trim) so raw final_response payloads
		// reconcile against what actually reached the screen (#71643).
		this.deliveredFinalText = this.normalizeDelivered(text);
		this.finalResponseSentInternal = true;
		this.finalContentDeliveredInternal = true;
		this.alreadySentInternal = true;
	}

	/**
	 * (se-1) Retract any streamed preview when the turn-final is an
	 * intentional-silence marker (stream_consumer.py:_suppress_silence_marker).
	 * The agent chose not to respond; anything already on screen must go so the
	 * raw marker is never left visible. Delivery flags stay FALSE — nothing was
	 * delivered — and `already_sent` clears so gateway short-circuits don't
	 * fire; the boundary's own disposition gate then suppresses any fallback
	 * send. Deletion reuses a best-effort adapter delete when available.
	 */
	private suppressSilenceMarker(): void {
		const deleteFn = this.adapter.deleteMessage?.bind(this.adapter);
		if (deleteFn !== undefined && this.messageId !== null) {
			try {
				void deleteFn(String(this.chatId), this.messageId);
			} catch {
				// best-effort cleanup — logger.debug parity
			}
		}
		this.messageId = null;
		this.accumulated = "";
		this.streamLedger = "";
		this.lastSentText = "";
		this.alreadySentInternal = false;
		this.finalResponseSentInternal = false;
		this.finalContentDeliveredInternal = false;
		this.deliveredFinalText = null;
	}

	/**
	 * (se-9) got_done overflow for an oversized final: seal head chunks so the
	 * tail that continues through the normal finalize paths always fits the
	 * platform limit. The first head seals INTO the live preview (finalize
	 * edit); further heads go out as sealed threaded sends. Returns the bounded
	 * tail; on a failed seal the FULL text comes back so the fallback path can
	 * still deliver it completely (#78541 parity).
	 */
	private async sealFinalOverflow(finalText: string): Promise<string> {
		const limit = this.cfg.messageLimit;
		if (limit <= 0 || finalText.length <= limit) return finalText;
		const parent = this.overflowChainParent();
		if (parent === undefined) return finalText; // no lane identity — legacy path
		let text = finalText;
		let cursor = parent;
		if (this.messageId !== null) {
			const [head] = splitOverflowChunks(text, limit);
			if (head !== undefined && head !== text) {
				const res = await this.adapter.editMessage(
					this.chatId,
					this.messageId,
					this.displayReady(head),
					{ finalize: true, metadata: this.metadata },
				);
				if (!res.success) return text;
				this.splitDeliveryInternal = true;
				text = text.slice(head.length).replace(/^\n+/, "");
				// Extra heads thread off the just-sealed preview's identity.
				cursor = this.messageId;
				this.messageId = null;
				this.lastSentText = "";
			}
		}
		while (text.length > limit) {
			const [head] = splitOverflowChunks(text, limit);
			if (head === undefined || head === text) break;
			const id = await this.sendSealedPiece(head, cursor, { final: true });
			if (id === null) {
				this.fallbackFinalSend = true;
				return text;
			}
			this.splitDeliveryInternal = true;
			cursor = id;
			text = text.slice(head.length).replace(/^\n+/, "");
		}
		this.overflowChainTail = cursor;
		return text;
	}

	/**
	 * (se-9) stream_consumer.py:_send_fallback_final — after progressive edits
	 * stop working, deliver only the UNSEEN continuation as a fresh message so
	 * the visible prefix is never duplicated. Oversized continuations ride the
	 * same splitter; every chunk must land for delivery to count.
	 */
	private async sendFallbackContinuation(finalText: string): Promise<void> {
		this.fallbackFinalSend = false;
		// Fence-close BEFORE computing the continuation so the closing fence
		// reaches the user even when the fallback delivers only the tail after
		// mid-stream edits failed (_send_fallback_final composition parity).
		const closed = this.displayReady(finalText);
		const prefix = this.lastSentText;
		let continuation =
			prefix !== "" && closed.startsWith(prefix)
				? closed.slice(prefix.length)
				: closed;
		continuation = continuation.replace(/^\s+/, "");
		if (continuation.trim() === "") {
			// Nothing new to send — the visible partial already matches the final.
			this.markTurnFinalDelivered(finalText);
			return;
		}
		const limit = this.cfg.messageLimit;
		const chunks =
			limit > 0 && continuation.length > limit
				? splitOverflowChunks(continuation, limit)
				: [continuation];
		for (const chunk of chunks) {
			const parent = this.overflowChainTail;
			const res = await this.adapter.send(
				this.chatId,
				chunk,
				parent,
				this.metadataForSend({
					final: true,
					...(parent !== undefined ? { replyToOverride: parent } : {}),
				}),
			);
			if (!res.success) return; // delivery stays unclaimed — gateway retries
		}
		this.markTurnFinalDelivered(finalText);
	}

	// ── metadata helpers (stream_consumer.py:_metadata_for_send) ──────────

	private metadataForSend(opts?: {
		final?: boolean | undefined;
		/** Stamp `expect_edits` (_metadata_for_send(expect_edits=…) parity):
		 * ladder adapters skip the rich path for previews that get edited later. */
		expectEdits?: boolean | undefined;
		/** Reply-chain override for split-delivery pieces (each sealed piece
		 * needs its OWN turn identity — see overflowChainTail). */
		replyToOverride?: string | undefined;
	}): Metadata {
		const meta: Metadata = { ...(this.metadata ?? {}) };
		const replyTo = opts?.replyToOverride ?? this.initialReplyToId;
		if (replyTo !== undefined) {
			meta[REPLY_TO_METADATA_KEY] = replyTo;
		}
		if (opts?.expectEdits === true) meta["expect_edits"] = true;
		if (opts?.final === true) meta["notify"] = true;
		return meta;
	}

	private metadataOrUndefined(): Metadata | undefined {
		const meta = this.metadataForSend();
		return Object.keys(meta).length > 0 ? meta : undefined;
	}

	// ── commentary (stream_consumer.py:_send_commentary / run() branch) ──

	/**
	 * Drain-path commentary delivery. FIFO order is guaranteed by the queue —
	 * by the time this runs, every delta queued BEFORE the commentary has been
	 * accumulated and is force-flushed first (Hermes' should_edit fires on
	 * commentary too), then the send is wrapped in segment-state resets so
	 * post-commentary prose opens a NEW bubble below instead of editing the
	 * stale preview above (_reset_segment_state around _send_commentary).
	 *
	 * Stream-is-the-message native-draft runs are the exception: resetting
	 * would break the connector's append-only invariant (whole-snapshot
	 * re-append class), so they just post the beat onto the live stream.
	 */
	private async deliverCommentary(raw: string): Promise<void> {
		const text = this.cleanForDisplay(raw);
		if (text.trim() === "") return;
		// Deliver buffered prose FIRST so the commentary lands below it.
		await this.flushCurrent();
		const resetAround = !(this.useDraftStreaming && this.streamIsMessage());
		if (resetAround) await this.finalizeSegment();
		await this.sendCommentary(text);
		if (resetAround) await this.finalizeSegment();
	}

	/**
	 * Send a completed interim assistant commentary message. Declares interim
	 * intent via `_interim_send` (invariant 3): the door chokepoint pops the
	 * marker and must NOT seal-intercept this send. Does NOT set already_sent
	 * (#10454 parity — the final must never be suppressed by commentary).
	 * Direct primitive: production ingress routes through onCommentary so the
	 * beat queues FIFO behind deltas.
	 */
	async sendCommentary(text: string): Promise<boolean> {
		const cleaned = this.cleanForDisplay(text);
		if (cleaned.trim() === "") return false;
		const meta = this.metadataForSend();
		meta[INTERIM_SEND_MARKER] = true;
		try {
			const res: SendResult = await this.adapter.send(
				this.chatId,
				cleaned,
				undefined,
				meta,
			);
			if (res.success) {
				// Record the exact DELIVERED text so has_delivered_text/delivered_
				// final_matches can reconcile interim beats (#14238 parity).
				this.deliveredCommentaryTexts.push(cleaned);
				return true;
			}
			return false;
		} catch {
			return false;
		}
	}

	/**
	 * Stale-session abandonment (stream_consumer.py:_abandon_native_stream +
	 * run()'s early return when run_still_current() goes false). The session
	 * was reset (/new, /stop): stop editing, best-effort SEAL the open native
	 * stream in place with what is already on screen (or retract an edit-path
	 * preview), and return WITHOUT setting any delivery flags — an abandoned
	 * turn's text was partial and the gateway's normal paths own whatever
	 * happens next.
	 */
	private async abandonStream(): Promise<void> {
		if (this.useDraftStreaming && this.draftId !== null) {
			const abandon = this.adapter.abandonOpenDraft?.bind(this.adapter);
			if (abandon !== undefined) {
				try {
					await abandon(
						String(this.chatId),
						this.lastSentText !== ""
							? this.lastSentText
							: this.cleanForDisplay(this.accumulated),
						this.metadataOrUndefined(),
					);
				} catch {
					// best-effort seal — logger.debug parity
				}
			}
		} else if (this.messageId !== null) {
			// Edit-path preview: best-effort retract so a stale partial never
			// masquerades as the answer inside a fresh session.
			const deleteFn = this.adapter.deleteMessage?.bind(this.adapter);
			if (deleteFn !== undefined) {
				try {
					await deleteFn(String(this.chatId), this.messageId);
				} catch {
					// best-effort cleanup
				}
			}
		}
		this.messageId = null;
		this.lastSentText = "";
	}

	// ── think-block scrubber (stream_consumer.py:_filter_and_accumulate) ──

	/** Append to the live buffer and the split-stable stream ledger. */
	private appendAccumulated(text: string): void {
		if (!text) return;
		this.accumulated += text;
		this.streamLedger += text;
	}

	/** Flush any held-back partial-tag buffer (called on got_done). */
	private flushThinkBuffer(): void {
		if (this.thinkBuffer && !this.inThinkBlock) {
			// Strip any orphan close tags that may have been held back.
			this.appendAccumulated(stripOrphanCloseTags(this.thinkBuffer));
			this.thinkBuffer = "";
		}
	}

	/**
	 * Add a text delta to the accumulated buffer, suppressing think blocks.
	 * State machine tracking whether we are inside a reasoning/thinking block;
	 * text inside such blocks is silently discarded. Partial tags at buffer
	 * boundaries are held back in `thinkBuffer` until enough characters arrive
	 * to decide; orphan close tags with no matching open are stripped.
	 */
	private filterAndAccumulate(text: string): void {
		let buf = this.thinkBuffer + text;
		this.thinkBuffer = "";

		for (;;) {
			if (buf.length === 0) return;
			const lowerBuf = buf.toLowerCase();
			if (this.inThinkBlock) {
				// Look for the earliest closing tag.
				let bestIdx = -1;
				let bestLen = 0;
				for (const tag of CLOSE_THINK_TAGS) {
					const idx = lowerBuf.indexOf(tag.toLowerCase());
					if (idx !== -1 && (bestIdx === -1 || idx < bestIdx)) {
						bestIdx = idx;
						bestLen = tag.length;
					}
				}
				if (bestLen > 0) {
					// Found closing tag — discard block, process remainder.
					this.inThinkBlock = false;
					buf = buf.slice(bestIdx + bestLen);
					continue;
				}
				// No closing tag yet — hold tail that could be a partial closing
				// tag prefix, discard the rest.
				const maxTag = maxThinkTagLength();
				this.thinkBuffer = buf.length > maxTag ? buf.slice(-maxTag) : buf;
				return;
			}

			// Look for earliest opening tag at a block boundary (start of text /
			// preceded by newline + optional whitespace). This prevents false
			// positives when models *mention* tags in prose.
			let bestIdx = -1;
			let bestLen = 0;
			for (const tag of OPEN_THINK_TAGS) {
				const tagLower = tag.toLowerCase();
				let searchStart = 0;
				for (;;) {
					const idx = lowerBuf.indexOf(tagLower, searchStart);
					if (idx === -1) break;
					let isBoundary: boolean;
					if (idx === 0) {
						isBoundary =
							this.accumulated === "" || this.accumulated.endsWith("\n");
					} else {
						const preceding = buf.slice(0, idx);
						const lastNl = preceding.lastIndexOf("\n");
						isBoundary =
							lastNl === -1
								? (this.accumulated === "" ||
										this.accumulated.endsWith("\n")) &&
									preceding.trim() === ""
								: preceding.slice(lastNl + 1).trim() === "";
					}
					if (isBoundary && (bestIdx === -1 || idx < bestIdx)) {
						bestIdx = idx;
						bestLen = tag.length;
						break; // first boundary hit for this tag is enough
					}
					searchStart = idx + 1;
				}
			}

			if (bestLen > 0) {
				// Emit text before the tag, enter think block.
				this.appendAccumulated(buf.slice(0, bestIdx));
				this.inThinkBlock = true;
				buf = buf.slice(bestIdx + bestLen);
				continue;
			}
			// No opening tag — check for a partial tag at the tail.
			let heldBack = 0;
			for (const tag of OPEN_THINK_TAGS) {
				const tagLower = tag.toLowerCase();
				for (let i = 1; i < tag.length; i++) {
					if (lowerBuf.endsWith(tagLower.slice(0, i)) && i > heldBack) {
						heldBack = i;
					}
				}
			}
			if (heldBack > 0) {
				this.appendAccumulated(buf.slice(0, buf.length - heldBack));
				this.thinkBuffer = buf.slice(buf.length - heldBack);
			} else {
				// No (partial) open tag — but the model may have emitted an orphan
				// close tag like </think> on its own; strip those before
				// accumulating so they never reach the user.
				this.appendAccumulated(stripOrphanCloseTags(buf));
			}
			return;
		}
	}
}
