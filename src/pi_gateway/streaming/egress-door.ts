// pi_gateway/streaming/egress-door — THE single-audited seal chokepoint (DEC-006).
//
// 04 §1.1: "any public method that transmits user-visible text must route
// through ONE audited chokepoint, and the chokepoint carries the seal check."
// Hermes pops `_interim_send` inline at TWO sites (relay/adapter.py:send ~L1859
// and :send_for_platform ~L1699); this port UNIFIES them into ONE audited
// chokepoint property — `EgressChokepoint.admit` — that BOTH doors route
// through, so there are no two magic method names to keep in sync.
//
// Semantics ported from the READ-ONLY Hermes reference, cited as file:symbol:
//   - gateway/relay/adapter.py:send               (door 1)
//   - gateway/relay/adapter.py:send_for_platform  (door 2; finding #7)
//   - gateway/relay/adapter.py:_match_open_draft  (exact turn key; single-open fallback)
//   - gateway/relay/adapter.py:_seal_open_draft   (tombstone BEFORE attempt)
//   - gateway/relay/adapter.py:send_draft         (arm ONLY stream-is-message chats,
//                                                  review B4; tombstone swallows stragglers)
//   - gateway/run.py:_deliver_queued_first_response (reconcile-by-edit, invariant 4)
//
// §5.1 mechanics, in order, at EVERY admission:
//   pop _interim_send        → interim? skip interception entirely (invariant 3)
//   match open draft         → none? reconcile-by-edit lane, then plain send
//   seal_open_draft(final)   → success? return seal result
//                            → failure? fall through to plain send — NEVER
//                              swallow the turn-final
//   ordering: interception BEFORE any explicit-platform branch (finding #7)

import {
	INTERIM_SEND_MARKER,
	REPLY_TO_METADATA_KEY,
	type Metadata,
	type SendResult,
} from "./adapter-seam.js";

export type DoorName = "send" | "send_for_platform";

/** Observable door decision for one admission (audit trail of the property). */
export type EgressAction =
	| "seal" // turn-final absorbed into the open native stream
	| "seal-failed-plain-send" // seal failed → plain send; final never swallowed
	| "reconcile-edit" // delivered by EDITING an existing message (invariant 4)
	| "plain-send"; // no open draft, no editable message

export interface ChokepointAuditEntry {
	door: DoorName;
	/** Was the popped `_interim_send` marker set on admission? */
	interim: boolean;
	action: EgressAction;
	chatId: string;
	contentPreview: string;
}

export interface DoorAdmission {
	door: DoorName;
	chatId: string;
	content: string;
	replyTo?: string | undefined;
	metadata?: Metadata | undefined;
	platform?: string | undefined;
}

/**
 * The wire transport the OWNING relay adapter supplies. The chokepoint owns
 * interception policy; the transport owns bytes-on-the-wire.
 */
export interface DoorTransport {
	/** Arming gate: interception armed ONLY for stream-is-message chats (B4). */
	streamIsMessageForChat(chatId: string): boolean;
	/** Ordinary wire send after interception decided not to seal/reconcile. */
	transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
		platform?: string | undefined,
	): Promise<SendResult>;
	/** Wire edit (reconcile-by-edit lane). */
	transmitEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: { finalize: boolean },
		platform?: string | undefined,
	): Promise<SendResult>;
	/**
	 * Connector-side draft(final:true) seal (_seal_open_draft parity). On
	 * success it MUST return the stream's wire message identity — the sealed
	 * stream IS the message. Failure shapes (ambiguous acks, connector errors)
	 * are the transport's own report; the chokepoint falls through on any.
	 */
	transmitSeal(
		draftKey: string,
		chatId: string,
		draftId: number,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
}

interface LaneIds {
	/** Sealed/final message identity on this turn lane. */
	finalId?: string | undefined;
	/** The interim lane's OWN earlier message id (never reconciles the final). */
	interimId?: string | undefined;
}

/**
 * Turn-scoped draft key — parity of relay/adapter.py:_draft_key: per-(chat,
 * turn) identity from metadata message ids; bare-chat fallback for
 * identity-less callers.
 */
export function turnKey(chatId: string, md: Metadata | undefined): string {
	const m = md ?? {};
	const turn = m[REPLY_TO_METADATA_KEY] ?? m["message_id"];
	return typeof turn === "string" && turn.length > 0
		? `${chatId}|${turn}`
		: `${chatId}|_`;
}

function contentPreview(text: string): string {
	return text.length > 40 ? `${text.slice(0, 37)}…` : text;
}

export interface DraftAdmissionVerdict {
	key: string;
	/**
	 * Post-seal straggler frame for an already-tombstoned draft_id: its content
	 * is already in the sealed message → send nothing, arm nothing, report
	 * success (tombstone parity).
	 */
	swallow: boolean;
	/** Arm seal-interception for this draft key (stream-is-message chats only). */
	arm: boolean;
}

export class EgressChokepoint {
	/** One entry per door admission, in admission order — both-door coverage. */
	readonly audit: ChokepointAuditEntry[] = [];
	/** relay/adapter.py:_open_draft_by_chat — armed (key → draft_id) streams. */
	readonly openDraftByKey = new Map<string, number>();
	/** relay/adapter.py:_sealed_draft_by_chat — sealed-stream tombstones. */
	readonly sealedDraftByKey = new Map<string, number>();
	/** Per-turn message identities for edit-first redelivery (invariant 4). */
	readonly laneById = new Map<string, LaneIds>();

	constructor(private readonly transport: DoorTransport) {}

	// ── THE audited chokepoint ────────────────────────────────────────────

	/**
	 * EVERY user-visible text transmission admits here exactly once — both
	 * doors, one code path. Pops the gateway-internal interim marker, runs
	 * seal-interception (skipped for interim sends), falls through to
	 * reconcile-by-edit / plain send, and appends exactly one audit entry per
	 * admission even when the transport throws.
	 */
	async admit(input: DoorAdmission): Promise<SendResult> {
		const wireMetadata: Metadata = { ...(input.metadata ?? {}) };
		if (
			input.replyTo !== undefined &&
			wireMetadata[REPLY_TO_METADATA_KEY] === undefined
		) {
			wireMetadata[REPLY_TO_METADATA_KEY] = input.replyTo;
		}
		// THE POP — the only place the interim marker is stripped before the
		// wire. An interim send NEVER triggers seal-interception (invariant 3):
		// sealing the live stream with interim text would orphan the true final
		// into a plain-send duplicate.
		const interim = wireMetadata[INTERIM_SEND_MARKER] === true;
		delete wireMetadata[INTERIM_SEND_MARKER];

		let action: EgressAction = "plain-send";
		try {
			// Interception BEFORE any explicit-platform branch (finding #7) —
			// but interim sends skip interception entirely (invariant 3).
			const sealKey = interim
				? null
				: this.matchOpenDraft(input.chatId, wireMetadata);
			if (sealKey !== null) {
				const seal = await this.sealOpenDraft(
					sealKey,
					input.chatId,
					input.content,
					wireMetadata,
				);
				if (seal.success) {
					action = "seal";
					return seal;
				}
				// Failed seal falls through to plain delivery — never swallow
				// the turn-final. The orphaned stream is sealed connector-side
				// (recycling / MAX_OPEN_STREAMS eviction); the tombstone above
				// already prevents straggler frames from re-arming it.
				action = "seal-failed-plain-send";
			} else {
				// Reconcile by edit FIRST; plain send only when no editable
				// message exists (invariant 4). Interim sends reconcile against
				// their OWN lane id — never against the final's message.
				const lane = this.laneById.get(turnKey(input.chatId, wireMetadata));
				const editTarget = interim ? lane?.interimId : lane?.finalId;
				if (editTarget !== undefined) {
					const edited = await this.transport.transmitEdit(
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
			}

			const sent = await this.transport.transmitSend(
				input.chatId,
				input.content,
				wireMetadata,
				input.platform,
			);
			// Record the fresh message identity on its lane so any later
			// beside-sealed delivery reconciles BY EDIT instead of duplicating.
			const laneKey = turnKey(input.chatId, wireMetadata);
			const lane = this.laneById.get(laneKey) ?? {};
			if (interim) lane.interimId = sent.messageId ?? undefined;
			else lane.finalId = sent.messageId ?? undefined;
			this.laneById.set(laneKey, lane);
			return sent;
		} finally {
			this.audit.push({
				door: input.door,
				interim,
				action,
				chatId: input.chatId,
				contentPreview: contentPreview(input.content),
			});
		}
	}

	// ── draft-frame admission (adapter's send_draft delegates here first) ──

	/**
	 * Verdict for one outgoing draft frame: whether it is a post-seal straggler
	 * to swallow, and whether emitting it should arm seal-interception. Arming
	 * happens ONLY for stream-is-message chats (review B4): Telegram-shaped
	 * drafts clear client-side; arming there would eat the real final into
	 * draft(final=true) with no history message.
	 */
	draftAdmission(args: {
		chatId: string;
		draftId: number;
		metadata?: Metadata | undefined;
	}): DraftAdmissionVerdict {
		const key = turnKey(args.chatId, args.metadata);
		const swallow = this.sealedDraftByKey.get(key) === args.draftId;
		return {
			key,
			swallow,
			arm:
				!swallow && this.transport.streamIsMessageForChat(String(args.chatId)),
		};
	}

	/** Arm interception for an emitted draft frame (post-admission). */
	armOpenDraft(key: string, draftId: number): void {
		this.openDraftByKey.set(key, draftId);
	}

	// ── internals shared with the doors ────────────────────────────────────

	/**
	 * Port of relay/adapter.py:_match_open_draft. Exact per-turn key first.
	 * Only a per-turn MESSAGE id is turn identity: callers carrying one never
	 * fall back (their mismatch means the stream is someone else's).
	 */
	matchOpenDraft(
		chatId: string,
		metadata: Metadata | undefined,
	): string | null {
		const key = turnKey(chatId, metadata);
		if (this.openDraftByKey.has(key)) return key;
		const md = metadata ?? {};
		if (
			typeof md["message_id"] === "string" ||
			typeof md[REPLY_TO_METADATA_KEY] === "string"
		) {
			return null;
		}
		// Single-open-stream fallback for identity-less callers only.
		const prefix = `${chatId}|`;
		let only: string | null = null;
		for (const k of this.openDraftByKey.keys()) {
			if (!k.startsWith(prefix)) continue;
			if (only !== null) return null; // second candidate → ambiguous
			only = k;
		}
		return only;
	}

	/**
	 * Port of relay/adapter.py:_seal_open_draft. Tombstone BEFORE the transport
	 * call: whatever the ack says, this draft_id's stream is never re-armed by
	 * a straggler frame.
	 */
	private async sealOpenDraft(
		draftKey: string,
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const draftId = this.openDraftByKey.get(draftKey);
		if (draftId === undefined) {
			return { success: false, error: "no open draft under key" };
		}
		this.openDraftByKey.delete(draftKey);
		this.sealedDraftByKey.set(draftKey, draftId);
		const seal = await this.transport.transmitSeal(
			draftKey,
			chatId,
			draftId,
			content,
			metadata,
		);
		if (
			seal.success &&
			seal.messageId !== undefined &&
			seal.messageId !== null
		) {
			const lane = this.laneById.get(draftKey) ?? {};
			lane.finalId = seal.messageId;
			this.laneById.set(draftKey, lane);
		}
		return seal;
	}

	// ── observability ─────────────────────────────────────────────────────

	isOpenDraft(chatId: string, metadata?: Metadata | undefined): boolean {
		return this.openDraftByKey.has(turnKey(chatId, metadata));
	}

	isSealedDraft(chatId: string, metadata?: Metadata | undefined): boolean {
		return this.sealedDraftByKey.has(turnKey(chatId, metadata));
	}

	openDraftCount(): number {
		return this.openDraftByKey.size;
	}
}
