// Spike proof: GatewayStreamConsumer parity (04-platform-adapters.md §5, DEC-006).
//
// Ported from the READ-ONLY Hermes reference (/usr/local/lib/hermes-agent),
// semantics only, cited as file:symbol anchors — no code vendored:
//   - gateway/stream_consumer.py:GatewayStreamConsumer (queue drain loop, finish())
//   - gateway/stream_consumer.py:StreamConsumer.finish        (authoritative final)
//   - gateway/stream_consumer.py:GatewayStreamConsumer._stream_is_message
//   - gateway/stream_consumer.py:GatewayStreamConsumer._send_draft_frame
//   - gateway/stream_consumer.py:GatewayStreamConsumer._send_commentary
//   - gateway/stream_consumer.py:GatewayStreamConsumer._reset_segment_state
//   - gateway/stream_consumer.py:GatewayStreamConsumer._metadata_for_send
//   - gateway/stream_consumer.py:GatewayStreamConsumer.delivered_final_matches
//   - gateway/platforms/base.py:supports_draft_streaming       (per-chat METHOD probe)

/**
 * Gateway-internal metadata marker for any consumer-side send that is NOT the
 * turn-final (04 §5 invariant 3). Producers:
 *   - gateway/stream_consumer.py:_send_commentary            (`_md["_interim_send"] = True`)
 *   - gateway/stream_consumer.py:_flush_segment_tail_on_edit_failure
 *   - gateway/run.py:_interim_metadata                       (heartbeats, advisories)
 * It is popped/stripped at the adapter egress doors — see fake-relay-adapter.ts
 * (DEC-006 single audited chokepoint).
 */
export const INTERIM_SEND_MARKER = "_interim_send";

export type Metadata = Record<string, unknown>;

export interface SendResult {
	success: boolean;
	messageId?: string | null | undefined;
	error?: string | null | undefined;
}

/** The slice of a platform adapter the consumer drives. Implemented by FakeRelayAdapter. */
export interface StreamEgressAdapter {
	/** Per-chat METHOD probe, not class data (DEC-006; base.py:supports_draft_streaming). */
	supportsDraftStreaming?(chatType?: string | undefined): boolean;
	sendDraft(args: {
		chatId: string;
		draftId: number;
		content: string;
		metadata?: Metadata | undefined;
	}): Promise<SendResult>;
	send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
	editMessage(
		chatId: string,
		messageId: string,
		content: string,
		opts?: { finalize?: boolean | undefined } | undefined,
	): Promise<SendResult>;
	/** Per-chat probe preferred by the consumer (relay/adapter.py:stream_is_message_for_chat). */
	streamIsMessageForChat?(chatId: string): boolean;
	/** Class-level fallback (relay/adapter.py construction: `draft_stream_is_message`). */
	draftStreamIsMessage?: boolean | undefined;
}

/** Observable evidence that a draft frame mutated previously-emitted prefix content. */
export interface PrefixViolation {
	kind: "non_prefix_frame";
	prevFrame: string;
	nextFrame: string;
}

export interface StreamConsumerConfig {
	/** Minimum ms between mid-stream flushes (stream_consumer.py:StreamConsumerConfig.edit_interval). */
	editIntervalMs?: number | undefined;
	/** Buffered-growth chars required before a mid-stream flush (buffer_threshold). */
	bufferThreshold?: number | undefined;
	/** Streaming cursor suffix; stripped from every frame (finding #6). Default none. */
	cursor?: string | undefined;
	/** Transport selection (StreamConsumerConfig.transport): auto prefers drafts. */
	transport?: "auto" | "draft" | "edit" | undefined;
	chatType?: string | undefined;
	/**
	 * Composition-time transform applied to the ACCUMULATED buffer when building
	 * each MID-STREAM DRAFT FRAME ONLY (never the finalize payload — "only the
	 * finalize path may transform the real final", 04 §5 invariant 1). This seam
	 * models the historically banned transforms (fence-closing, cursor suffix,
	 * segment-state resets): a composeFrame that breaks prefix stability is the
	 * exact bug class the MUTATION contract test injects.
	 */
	composeFrame?: ((accumulated: string) => string) | undefined;
	/** Injected clock for editIntervalMs decisions (flake discipline). */
	now?: (() => number) | undefined;
}

/** Resolved config — every knob has a concrete value after construction. */
interface ResolvedConfig {
	editIntervalMs: number;
	bufferThreshold: number;
	cursor: string;
	transport: "auto" | "draft" | "edit";
	chatType: string | undefined;
	composeFrame: ((accumulated: string) => string) | undefined;
	now: (() => number) | undefined;
}

type QueueItem =
	| { kind: "delta"; text: string }
	| { kind: "segment-break" }
	| { kind: "final-text"; text: string }
	| { kind: "done" };

/** Worker-thread → drain-loop queue (Python queue.Queue parity). Single reader. */
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
	private _alreadySent = false;
	private useDraftStreaming = false;
	private draftId: number | null = null;
	private gotDone = false;
	private finished = false; // finish() latch — absorbed EXACTLY once
	private finalAdopted = false; // _FINAL_TEXT adoption latch
	private lastFlushAt = Number.NEGATIVE_INFINITY;
	private deliveredSegmentTexts: string[] = [];
	private deliveredCommentaryTexts: string[] = [];
	private deliveredFinalText: string | null = null;

	// Runner-read properties after drain (spec §5.2 sketch).
	private _finalResponseSent = false;
	private _finalContentDelivered = false;

	/** Observable non-prefix-stability detections (04 §5 invariant 1 enforcement). */
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
			now: config?.now,
		};
		this.metadata = metadata;
		this.initialReplyToId = initialReplyToId;
	}

	// ── probes ────────────────────────────────────────────────────────────

	/**
	 * Whether THIS chat's transport treats the stream as the message. Port of
	 * gateway/stream_consumer.py:GatewayStreamConsumer._stream_is_message:
	 * per-chat METHOD probe preferred (one relay adapter fronts N platforms),
	 * class attribute as fallback.
	 */
	private streamIsMessage(): boolean {
		const probe = this.adapter.streamIsMessageForChat;
		if (typeof probe === "function") {
			try {
				return probe.call(this.adapter, String(this.chatId)) === true;
			} catch {
				return false;
			}
		}
		return this.adapter.draftStreamIsMessage === true;
	}

	/**
	 * Transport gate. Port of
	 * gateway/stream_consumer.py:GatewayStreamConsumer._resolve_draft_streaming
	 * (capability via per-chat method probe, base.py:supports_draft_streaming).
	 */
	private resolveDraftStreaming(): void {
		const transport = this.cfg.transport ?? "auto";
		if (transport === "edit") return;
		const supports =
			this.adapter.supportsDraftStreaming?.(this.cfg.chatType) ?? false;
		if (supports) {
			this.useDraftStreaming = true;
			this.draftId = nextDraftId();
		}
	}

	// ── ingress (worker-thread side) ──────────────────────────────────────

	/**
	 * Thread-safe delta callback. `null` signals a tool boundary / segment break
	 * (gateway/stream_consumer.py:GatewayStreamConsumer.on_delta).
	 */
	onDelta(text: string | null): void {
		if (this.finished) return; // post-finish straggler: the turn is over
		if (text === null) {
			this.queue.push({ kind: "segment-break" });
			return;
		}
		if (text === "") return; // falsy-but-not-None is ignored (on_delta parity)
		this.queue.push({ kind: "delta", text });
	}

	/**
	 * Signal completion. `finalText`, when provided, is the AUTHORITATIVE
	 * completed final_response — including post-stream augmentation the
	 * accumulator never saw — and is absorbed EXACTLY ONCE (latch below).
	 * Port of gateway/stream_consumer.py:StreamConsumer.finish; bare finish()
	 * keeps legacy behavior.
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

	// ── runner-read properties (spec §5.2) ────────────────────────────────

	get alreadySent(): boolean {
		return this._alreadySent;
	}

	get finalResponseSent(): boolean {
		return this._finalResponseSent;
	}

	get finalContentDelivered(): boolean {
		return this._finalContentDelivered;
	}

	get message_id(): string | null {
		return this.messageId;
	}

	/**
	 * Whether the recorded turn-final payload reconciles with `finalText`.
	 * Port of gateway/stream_consumer.py:delivered_final_matches (null = nothing
	 * was recorded — legacy-trust rules apply upstream).
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
			const flushable = dirty;
			if (flushable && this.shouldFlushNow()) {
				this.lastFlushAt = this.nowMs();
				await this.flushCurrent();
			}
		}
	}

	private nowMs(): number {
		return this.cfg.now !== undefined ? this.cfg.now() : Date.now();
	}

	private shouldFlushNow(): boolean {
		const growth = this.accumulated.length - this.lastSentText.length;
		return (
			growth >= this.cfg.bufferThreshold &&
			this.nowMs() - this.lastFlushAt >= this.cfg.editIntervalMs
		);
	}

	// ── frame emission (stream_consumer.py:_send_or_edit + _send_draft_frame)

	private async flushCurrent(): Promise<void> {
		// Mid-turn boundaries on stream-is-message adapters emit another
		// cumulative frame — only the turn-final seals (finding #5). The
		// turn-final itself is delivered by finalizeTurn(), never here.
		if (this.useDraftStreaming && this.messageId === null) {
			const ok = await this.sendDraftFrame(this.composeDraftFrame());
			if (ok) return; // drafts don't set already_sent (fallback-send gating)
			// Failure (incl. detected prefix violation) permanently disabled the
			// draft lane; traffic reroutes through the edit-based path below
			// (04 §5 verified behaviors: graceful degradation).
		}
		const frame = this.stripCursor(this.accumulated);
		if (!frame.trim() || frame === this.lastSentText) return;
		if (this.messageId === null) {
			// First send of the edit-based preview. NOTE: this send goes through
			// the adapter door UNMARKED — on a stream-is-message adapter with an
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
				this._alreadySent = true;
			}
		} else if (frame !== this.lastSentText) {
			const res = await this.adapter.editMessage(
				this.chatId,
				this.messageId,
				frame,
			);
			if (res.success) {
				this.lastSentText = frame;
				this._alreadySent = true;
			}
		}
	}

	/**
	 * Emit one cumulative draft frame. Port of
	 * gateway/stream_consumer.py:GatewayStreamConsumer._send_draft_frame plus
	 * the PREFIX-STABILITY GUARD: frame N must be a string prefix of frame N+1
	 * (04 §5 invariant 1). A violating frame is DETECTED here — recorded in
	 * `prefixViolations` — and the draft lane is PERMANENTLY disabled for the
	 * remainder of the run (graceful degradation); the caller falls through to
	 * the edit-based path. Removing the guard makes the violation invisible and
	 * leaves the draft lane armed, which is exactly what the MUTATION contract
	 * test asserts against.
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
		// Carry the per-turn identity on EVERY frame (review B2 parity in
		// _send_draft_frame) so the final can find the open stream.
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
		// boundary emits NOTHING (finding #4 "Alice" / finding #5): the
		// connector appends the fresh segment whole. Edit-path adapters
		// finalize the segment as a real message first.
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
	 * handler in gateway/stream_consumer.py:GatewayStreamConsumer.run: adoption
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
		// (04 §5 invariant 2; live finding #11).
		const finalText = this.stripCursor(this.accumulated);
		if (finalText.trim() === "") return;
		if (this.messageId !== null) {
			// Already-on-screen preview: skip a verbatim finalize edit unless the
			// content changed (no-op short-circuit parity in _send_or_edit).
			if (finalText === this.lastSentText) {
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
			this._alreadySent
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
		this._finalResponseSent = true;
		this._finalContentDelivered = true;
		this._alreadySent = true;
	}

	// ── metadata helpers (stream_consumer.py:_metadata_for_send) ──────────

	private metadataForSend(opts?: { final?: boolean | undefined }): Metadata {
		const meta: Metadata = { ...(this.metadata ?? {}) };
		if (this.initialReplyToId !== undefined) {
			meta["reply_to_message_id"] = this.initialReplyToId;
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
	 * intent via `_interim_send` (04 §5 invariant 3): a stream-is-the-message
	 * adapter's seal-interception must not convert this into a seal. Does NOT
	 * set already_sent (#10454 parity — the final must never be suppressed).
	 */
	async sendCommentary(text: string): Promise<boolean> {
		if (text.trim() === "") return false;
		const meta = this.metadataForSend();
		meta[INTERIM_SEND_MARKER] = true;
		try {
			const res = await this.adapter.send(this.chatId, text, undefined, meta);
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
