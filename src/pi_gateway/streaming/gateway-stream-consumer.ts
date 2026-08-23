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
//   - gateway/stream_consumer.py:delivered_final_matches
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
	now: () => number;
}

type QueueItem =
	| { kind: "delta"; text: string }
	| { kind: "segment-break" }
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
	 * Signal completion. `finalText`, when provided, is the AUTHORITATIVE
	 * completed final_response — including post-stream augmentation the
	 * accumulator never saw — and is absorbed EXACTLY ONCE (latch below).
	 * Port of stream_consumer.py:StreamConsumer.finish; bare finish() keeps
	 * legacy behavior. Racing/double calls are inert.
	 */
	finish(finalText?: string | undefined): void {
		if (this.finished) return; // second/racing finish is inert
		this.finished = true;
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
	 * Port of stream_consumer.py:delivered_final_matches (null = nothing was
	 * recorded — legacy-trust rules apply upstream).
	 */
	deliveredFinalMatches(finalText: string): boolean | null {
		const delivered = this.deliveredFinalText;
		if (delivered === null) return null;
		return delivered.trim() === finalText.trim();
	}

	// ── drain loop (stream_consumer.py:GatewayStreamConsumer.run) ─────────

	async run(): Promise<void> {
		this.resolveDraftStreaming();
		for (;;) {
			const batch = await this.queue.nextBatch();
			if (batch === null) break; // closed without DONE (abandoned run)
			let dirty = false;
			for (const item of batch) {
				switch (item.kind) {
					case "delta": {
						// _append_accumulated parity: buffer + split-stable ledger.
						this.accumulated += item.text;
						this.streamLedger += item.text;
						dirty = true;
						break;
					}
					case "segment-break": {
						await this.finalizeSegment();
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
				await this.finalizeTurn();
				return;
			}
			if (dirty && this.shouldFlushNow()) {
				this.lastFlushAt = this.nowMs();
				await this.flushCurrent();
			}
		}
	}

	private nowMs(): number {
		return this.cfg.now();
	}

	private shouldFlushNow(): boolean {
		const growth = this.accumulated.length - this.lastSentText.length;
		return (
			growth >= this.cfg.bufferThreshold &&
			this.nowMs() - this.lastFlushAt >= this.cfg.editIntervalMs
		);
	}

	// ── frame emission (_send_or_edit + _send_draft_frame) ────────────────

	private async flushCurrent(): Promise<void> {
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
		const frame = this.stripCursor(this.accumulated);
		if (!frame.trim() || frame === this.lastSentText) return;
		if (this.messageId === null) {
			// First send of the edit-based preview. NOTE: this send goes through
			// the adapter door UNMARKED — on a stream-is-message chat with an
			// armed open draft the DOOR seal-intercepts it, converting the live
			// stream into a normal editable message (04 §5.1).
			const res = await this.adapter.send(
				this.chatId,
				frame,
				undefined,
				this.metadataForSend(),
			);
			if (res.success) {
				this.messageId = res.messageId ?? null;
				this.lastSentText = frame;
				this.alreadySentInternal = true;
			}
		} else if (frame !== this.lastSentText) {
			const res = await this.adapter.editMessage(
				this.chatId,
				this.messageId,
				frame,
			);
			if (res.success) {
				this.lastSentText = frame;
				this.alreadySentInternal = true;
			}
		}
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

	/** Composition-time draft-frame build (composeFrame seam + cursor strip). */
	private composeDraftFrame(): string {
		const raw = this.cfg.composeFrame?.(this.accumulated) ?? this.accumulated;
		return this.stripCursor(raw);
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
			const frame = this.stripCursor(this.accumulated);
			if (frame.trim() !== "" && frame !== this.lastSentText) {
				await this.adapter.editMessage(this.chatId, this.messageId, frame, {
					finalize: true,
				});
			}
		}
		this.accumulated = "";
		this.streamLedger = "";
		this.lastSentText = ""; // detection baseline resets: a fresh segment is
		// legitimately NOT a prefix-extension of the previous wire frame
		this.messageId = null;
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
		this.accumulated = text;
		this.streamLedger = text;
	}

	private async finalizeTurn(): Promise<void> {
		// Only the finalize path may transform the real final — and the
		// authoritative payload rides INSIDE the seal/final edit verbatim
		// (invariants 1+2; live finding #11).
		const finalText = this.stripCursor(this.accumulated);
		if (finalText.trim() === "") return;
		if (this.messageId !== null) {
			// Already-on-screen preview: skip a verbatim finalize edit unless the
			// content changed — UNLESS the adapter REQUIRES_EDIT_FINALIZE (checked
			// `is True`), which forces the redundant edit through.
			if (finalText === this.lastSentText && !this.cfg.requiresEditFinalize) {
				this.markTurnFinalDelivered(finalText);
				return;
			}
			const res = await this.adapter.editMessage(
				this.chatId,
				this.messageId,
				finalText,
				{ finalize: true },
			);
			if (res.success) this.markTurnFinalDelivered(finalText);
			return;
		}
		if (
			this.accumulated !== "" ||
			this.lastSentText !== "" ||
			this.alreadySentInternal
		) {
			// Regular sendMessage — the ADAPTER DOOR seal-intercepts it for
			// stream-is-message chats (04 §5.1; relay/adapter.py:send).
			const res = await this.adapter.send(
				this.chatId,
				finalText,
				undefined,
				this.metadataForSend({ final: true }),
			);
			if (res.success) {
				// The door's seal result carries the stream's wire identity
				// (_seal_open_draft parity: the connector returns the stream ts).
				if (res.messageId !== undefined && res.messageId !== null) {
					this.messageId = res.messageId;
				}
				this.markTurnFinalDelivered(finalText);
			}
		}
		// else: no-stream turn — delivery ownership stays with the gateway
		// (test_stream_final_contract.py:TestFinalAdoptionGuards parity).
	}

	private markTurnFinalDelivered(text: string): void {
		this.deliveredFinalText = text;
		this.finalResponseSentInternal = true;
		this.finalContentDeliveredInternal = true;
		this.alreadySentInternal = true;
	}

	// ── metadata helpers (stream_consumer.py:_metadata_for_send) ──────────

	private metadataForSend(opts?: { final?: boolean | undefined }): Metadata {
		const meta: Metadata = { ...(this.metadata ?? {}) };
		if (this.initialReplyToId !== undefined) {
			meta[REPLY_TO_METADATA_KEY] = this.initialReplyToId;
		}
		if (opts?.final === true) meta["notify"] = true;
		return meta;
	}

	private metadataOrUndefined(): Metadata | undefined {
		const meta = this.metadataForSend();
		return Object.keys(meta).length > 0 ? meta : undefined;
	}

	// ── interim sends (stream_consumer.py:_send_commentary) ───────────────

	/**
	 * Send a completed interim assistant commentary message. Declares interim
	 * intent via `_interim_send` (invariant 3): the door chokepoint pops the
	 * marker and must NOT seal-intercept this send. Does NOT set already_sent
	 * (#10454 parity — the final must never be suppressed by commentary).
	 */
	async sendCommentary(text: string): Promise<boolean> {
		if (text.trim() === "") return false;
		const meta = this.metadataForSend();
		meta[INTERIM_SEND_MARKER] = true;
		try {
			const res: SendResult = await this.adapter.send(
				this.chatId,
				text,
				undefined,
				meta,
			);
			if (res.success) {
				this.deliveredCommentaryTexts.push(text);
				return true;
			}
			return false;
		} catch {
			return false;
		}
	}
}
