// Spike proof: FAKE relay adapter exercising the egress doors (04 §5.1).
//
// The spec defines this fake seam — it exists so the seal-interception and
// interim-marker contracts can be proven without a real connector. Semantics
// ported from the READ-ONLY Hermes reference, cited file:symbol:
//   - gateway/relay/adapter.py:send                    (door 1: pop marker → match open draft → seal | fall through)
//   - gateway/relay/adapter.py:send_for_platform       (door 2: same contract; delivery-resolver lane, finding #7)
//   - gateway/relay/adapter.py:_match_open_draft       (exact turn key; single-open-stream fallback)
//   - gateway/relay/adapter.py:_seal_open_draft        (pop + tombstone BEFORE attempt)
//   - gateway/relay/adapter.py:send_draft              (arm interception ONLY for stream-is-message chats, review B4;
//                                                       post-seal straggler swallowed by tombstone)
//   - gateway/run.py:_deliver_queued_first_response    (reconcile-by-edit beside a sealed stream, invariant 4)
//
// DEC-006: "seal-interception is stated as a single-audited-chokepoint PROPERTY
// with both-door coverage rather than two magic method names." Hermes pops the
// `_interim_send` marker inline at TWO sites (send ~L1859 / send_for_platform
// ~L1699); this port UNIFIES them into ONE audited chokepoint property,
// `egressDoor`, that both doors route through.

import type { Metadata, SendResult } from "./gateway-stream-consumer.js";
import { INTERIM_SEND_MARKER } from "./gateway-stream-consumer.js";

export type DoorName = "send" | "send_for_platform";

export type EgressAction =
	| "seal" // turn-final absorbed into the open stream (draft final:true)
	| "seal-failed-plain-send" // seal failed → plain send, NEVER swallow (PR 85796 pt 1)
	| "reconcile-edit" // delivered by EDITING an existing message (invariant 4)
	| "plain-send"; // no editable message existed

/** One audit entry per door admission — proves both-door chokepoint coverage. */
export interface ChokepointAuditEntry {
	door: DoorName;
	interim: boolean; // was the popped marker set?
	action: EgressAction;
	chatId: string;
	contentPreview: string;
}

export type WireOp =
	| {
			op: "send";
			chatId: string;
			content: string;
			metadata?: Metadata | undefined;
			platform?: string | undefined;
	  }
	| {
			op: "edit";
			chatId: string;
			messageId: string;
			content: string;
			finalize?: boolean | undefined;
			metadata?: Metadata | undefined;
			platform?: string | undefined;
	  }
	| {
			op: "draft";
			chatId: string;
			draftId: number;
			content: string;
			final: boolean;
			/** Sealing frames carry the stream's message identity (_seal_open_draft parity). */
			messageId?: string | undefined;
			metadata?: Metadata | undefined;
	  };

export type SendOp = Extract<WireOp, { op: "send" }>;
export type EditOp = Extract<WireOp, { op: "edit" }>;
export type DraftOp = Extract<WireOp, { op: "draft" }>;

interface LaneIds {
	finalId?: string | undefined; // sealed stream's message identity (the stream IS the message)
	interimId?: string | undefined; // the interim lane's OWN earlier message id
}

/**
 * Turn-scoped draft key — parity of relay/adapter.py:_draft_key: per-(chat,
 * turn) identity from metadata message ids; bare chat fallback for
 * identity-less callers.
 */
function turnKey(chatId: string, md: Metadata | undefined): string {
	const m = md ?? {};
	const turn = m["reply_to_message_id"] ?? m["message_id"];
	return typeof turn === "string" && turn.length > 0
		? `${chatId}|${turn}`
		: `${chatId}|_`;
}

function contentPreview(text: string): string {
	return text.length > 40 ? `${text.slice(0, 37)}…` : text;
}

/**
 * DEC-006 single-audited-chokepoint property. EVERY egress admission goes
 * through `EgressDoor.admit` exactly once — both doors, one code path.
 */
class EgressDoor {
	constructor(private readonly a: FakeRelayAdapter) {}

	async admit(input: {
		door: DoorName;
		chatId: string;
		content: string;
		replyTo?: string | undefined;
		metadata?: Metadata | undefined;
		platform?: string | undefined;
	}): Promise<SendResult> {
		const wireMetadata: Metadata = { ...(input.metadata ?? {}) };
		if (
			input.replyTo !== undefined &&
			wireMetadata["reply_to_message_id"] === undefined
		) {
			wireMetadata["reply_to_message_id"] = input.replyTo;
		}
		// THE POP — the only place the gateway-internal interim marker is
		// stripped before the wire (unifies relay/adapter.py send L1859 +
		// send_for_platform L1699). An interim send NEVER triggers
		// seal-interception (04 §5 invariant 3): sealing the live stream with
		// interim text orphans the true final into a plain-send duplicate.
		const interim = wireMetadata[INTERIM_SEND_MARKER] === true;
		delete wireMetadata[INTERIM_SEND_MARKER];

		let action: EgressAction = "plain-send";

		try {
			// Seal-interception runs BEFORE any explicit-platform branch
			// (finding #7 ordering parity).
			let sealKey: string | null = null;
			if (!interim) {
				sealKey = this.a.matchOpenDraft(input.chatId, wireMetadata);
			}
			if (sealKey === null) {
				// Reconcile-by-edit FIRST, plain send only when no editable
				// message exists (04 §5 invariant 4; port of
				// gateway/run.py:_deliver_queued_first_response). Interim sends
				// reconcile against their OWN lane id — never against the final.
				const lane = this.a.laneById.get(turnKey(input.chatId, wireMetadata));
				const editTarget = interim ? lane?.interimId : lane?.finalId;
				if (editTarget !== undefined) {
					const edited = await this.a.recordEdit(
						input.chatId,
						editTarget,
						input.content,
						{ finalize: !interim },
						input.platform,
					);
					if (edited.success) {
						action = "reconcile-edit";
						return edited;
					}
					// Edit failed → fall through to plain send (run.py parity).
				}
			} else {
				const seal = await this.a.sealOpenDraft(
					input.chatId,
					input.content,
					wireMetadata,
					sealKey,
				);
				action = seal.success ? "seal" : "seal-failed-plain-send";
				if (seal.success) return seal;
				// Failed seal falls through to plain delivery — never swallow the
				// turn-final (review finding, PR 85796 point 1). The orphaned
				// stream is sealed connector-side (recycling/MAX_OPEN_STREAMS).
			}

			const sent = await this.a.recordSend(
				input.chatId,
				input.content,
				wireMetadata,
				input.platform,
			);
			// Record the fresh message identity on its lane so any later
			// beside-sealed delivery reconciles BY EDIT instead of duplicating.
			const laneKey = turnKey(input.chatId, wireMetadata);
			const lane = this.a.laneById.get(laneKey) ?? {};
			if (interim) lane.interimId = sent.messageId ?? undefined;
			else lane.finalId = sent.messageId ?? undefined;
			this.a.laneById.set(laneKey, lane);
			return sent;
		} finally {
			this.a.chokepointAudit.push({
				door: input.door,
				interim,
				action,
				chatId: input.chatId,
				contentPreview: contentPreview(input.content),
			});
		}
	}
}

export class FakeRelayAdapter {
	/**
	 * DEC-006 single-audited-chokepoint property with both-door coverage.
	 * Both `send()` and `sendForPlatform()` admit through THIS property and
	 * nowhere else — there are no two magic method names to keep in sync.
	 */
	readonly egressDoor = new EgressDoor(this);

	/** relay/adapter.py:_open_draft_by_chat — armed (key → draft_id) streams. @internal door seam */
	readonly openDraftByKey = new Map<string, number>();
	/** relay/adapter.py:_sealed_draft_by_chat — sealed-stream tombstones. @internal door seam */
	readonly sealedDraftByKey = new Map<string, number>();
	private readonly tombstoneSwallowLogged = new Set<string>();
	/** Reconcile registry: per-turn message identities for edit-first redelivery. @internal door seam */
	readonly laneById = new Map<string, LaneIds>();

	chokepointAudit: ChokepointAuditEntry[] = [];
	ops: WireOp[] = [];

	// Test knobs (model connector-side conditions).
	failSeals = false; // _seal_open_draft returns failure (ambiguous ack shape)
	failDraftFrames = false;

	private messageIdCounter = 0;
	private waiters: Array<(op: WireOp) => boolean> = [];

	// ── capability probes (DEC-006: method probes, not class data) ────────

	/** base.py:supports_draft_streaming — per-chat METHOD probe. */
	supportsDraftStreaming(_chatType?: string | undefined): boolean {
		return true;
	}

	/** Class-level fallback flag (relay/adapter.py construction). */
	draftStreamIsMessage = true;

	/**
	 * Per-chat probe PREFERRED over the class attribute
	 * (relay/adapter.py:stream_is_message_for_chat, review r2 finding 2).
	 */
	streamIsMessageForChat(_chatId: string): boolean {
		return true;
	}

	// ── door 1: send() — port of relay/adapter.py:send ────────────────────

	async send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		return this.egressDoor.admit({
			door: "send",
			chatId,
			content,
			replyTo,
			metadata,
		});
	}

	// ── door 2: send_for_platform() — port of relay/adapter.py:send_for_platform

	/**
	 * The delivery-resolver lane (follow-up queue, media finals, scheduled
	 * sends) calls THIS door directly, bypassing send() — finding #7. The
	 * interim contract must hold here too (both-door coverage).
	 */
	async sendForPlatform(
		logicalPlatform: string,
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		return this.egressDoor.admit({
			door: "send_for_platform",
			chatId,
			content,
			replyTo,
			metadata,
			platform: logicalPlatform,
		});
	}

	// ── native draft streaming — port of relay/adapter.py:send_draft ─────

	async sendDraft(args: {
		chatId: string;
		draftId: number;
		content: string;
		metadata?: Metadata | undefined;
	}): Promise<SendResult> {
		const key = turnKey(args.chatId, args.metadata);
		// Post-seal straggler: its content is already in the sealed message;
		// report success, send nothing, arm nothing (tombstone parity).
		if (this.sealedDraftByKey.get(key) === args.draftId) {
			if (!this.tombstoneSwallowLogged.has(key)) {
				this.tombstoneSwallowLogged.add(key);
			}
			return { success: true };
		}
		await this.pushOp({
			op: "draft",
			chatId: args.chatId,
			draftId: args.draftId,
			content: args.content,
			final: false,
			metadata: args.metadata,
		});
		// Arm seal-interception ONLY for stream-is-message chats (review B4):
		// Telegram-shaped drafts clear client-side; arming there would eat the
		// real final into draft(final=true) with no history message.
		if (this.streamIsMessageForChat(String(args.chatId))) {
			this.openDraftByKey.set(key, args.draftId);
		}
		if (this.failDraftFrames) {
			return { success: false, error: "forced draft-frame failure" };
		}
		return { success: true };
	}

	// ── internals shared with EgressDoor ──────────────────────────────────

	/** Port of relay/adapter.py:_match_open_draft. @internal door seam */
	matchOpenDraft(
		chatId: string,
		metadata: Metadata | undefined,
	): string | null {
		const key = turnKey(chatId, metadata);
		if (this.openDraftByKey.has(key)) return key;
		const md = metadata ?? {};
		// Only a per-turn MESSAGE id is turn identity; callers carrying one
		// never fall back (their mismatch means the stream is someone else's).
		if (
			typeof md["message_id"] === "string" ||
			typeof md["reply_to_message_id"] === "string"
		) {
			return null;
		}
		// Single-open-stream fallback for identity-less callers only.
		const prefix = `${chatId}|`;
		const candidates = [...this.openDraftByKey.keys()].filter((k) =>
			k.startsWith(prefix),
		);
		return candidates.length === 1 ? (candidates[0] ?? null) : null;
	}

	/** Port of relay/adapter.py:_seal_open_draft. @internal door seam */
	async sealOpenDraft(
		chatId: string,
		content: string,
		metadata: Metadata | undefined,
		draftKey: string,
	): Promise<SendResult> {
		const draftId = this.openDraftByKey.get(draftKey);
		if (draftId === undefined) {
			return { success: false, error: "no open draft under key" };
		}
		// Tombstone BEFORE the transport call: whatever the ack says, this
		// draft_id's stream is never re-armed by a straggler frame.
		this.openDraftByKey.delete(draftKey);
		this.sealedDraftByKey.set(draftKey, draftId);
		if (this.failSeals) {
			return { success: false, error: "forced seal failure" };
		}
		const messageId = this.nextMessageId("sealed");
		await this.pushOp({
			op: "draft",
			chatId,
			draftId,
			content,
			final: true,
			messageId,
			metadata,
		});
		// The connector returns the stream's ts as the message identity —
		// record it on the lane so later beside-sealed lanes reconcile by edit.
		const lane = this.laneById.get(draftKey) ?? {};
		lane.finalId = messageId;
		this.laneById.set(draftKey, lane);
		return { success: true, messageId };
	}

	/** @internal door seam */
	async recordSend(
		chatId: string,
		content: string,
		metadata: Metadata | undefined,
		platform?: string | undefined,
	): Promise<SendResult> {
		const messageId = this.nextMessageId("msg");
		await this.pushOp({
			op: "send",
			chatId,
			content,
			metadata,
			platform,
		});
		return { success: true, messageId };
	}

	/** @internal door seam */
	async recordEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts?: { finalize?: boolean | undefined } | undefined,
		platform?: string | undefined,
	): Promise<SendResult> {
		await this.pushOp({
			op: "edit",
			chatId,
			messageId,
			content,
			finalize: opts?.finalize,
			metadata: undefined,
			platform,
		});
		return { success: true, messageId };
	}

	private nextMessageId(prefix: string): string {
		this.messageIdCounter += 1;
		return `${prefix}_${this.messageIdCounter}`;
	}

	// StreamEgressAdapter.editMessage — direct edits bypass the doors (they
	// carry no interception semantics of their own in this spike).
	async editMessage(
		chatId: string,
		messageId: string,
		content: string,
		opts?: { finalize?: boolean | undefined } | undefined,
	): Promise<SendResult> {
		return this.recordEdit(chatId, messageId, content, opts);
	}

	// ── test observability (event-based sync, no sleeps) ─────────────────

	async pushOp(op: WireOp): Promise<void> {
		this.ops.push(op);
		const current = this.waiters;
		this.waiters = [];
		for (const w of current) w(op);
	}

	waitFor<T extends WireOp>(
		pred: (op: WireOp) => boolean,
		timeoutMs = 10_000,
	): Promise<T> {
		return this.waitForCount(1, pred, timeoutMs).then((ops) => ops[0] as T);
	}

	/** Event-based wait for the Nth matching op — no sleeps; loose default bound. */
	async waitForCount(
		count: number,
		pred: (op: WireOp) => boolean,
		timeoutMs = 10_000,
	): Promise<WireOp[]> {
		const matched = (): WireOp[] => this.ops.filter(pred);
		if (matched().length >= count) return matched().slice(0, count);
		return new Promise<WireOp[]>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.waiters = this.waiters.filter((w) => w !== waiter);
				reject(new Error(`FakeRelayAdapter.waitForCount(${count}) timed out`));
			}, timeoutMs);
			const waiter = (op: WireOp): boolean => {
				if (!pred(op)) return false;
				const found = matched();
				if (found.length < count) return false;
				clearTimeout(timer);
				resolve(found.slice(0, count));
				return true;
			};
			this.waiters.push(waiter);
		});
	}

	openDraftCount(): number {
		return this.openDraftByKey.size;
	}

	isOpenDraft(chatId: string, metadata: Metadata | undefined): boolean {
		return this.openDraftByKey.has(turnKey(chatId, metadata));
	}

	isSealedDraft(chatId: string, metadata: Metadata | undefined): boolean {
		return this.sealedDraftByKey.has(turnKey(chatId, metadata));
	}
}
