// Contract-test fixtures: TWO fake adapter shapes, BOTH built on the production
// EgressChokepoint so the single-chokepoint property is exercised by shared
// code, not duplicated magic (DEC-006; 04 §8 streaming rows).
//
//   FakeDraftStreamAdapter    — Telegram-shaped: the stream is NOT the message;
//                               drafts clear client-side; segment boundaries
//                               bump draft ids; the final goes out as an
//                               ordinary send.
//   FakeStreamIsMessageAdapter — relay-shaped: ONE native stream per turn IS
//                               the message; draft frames arm seal-
//                               interception; the turn-final send is absorbed
//                               into the stream as draft(final:true).
//
// Semantics ported from the READ-ONLY Hermes reference, cited as file:symbol:
//   - gateway/relay/adapter.py:send / send_for_platform / send_draft
//   - gateway/platforms/base.py:supports_draft_streaming (per-chat METHOD probe)

import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	Metadata as MetadataType,
	SendResult,
} from "../adapter-seam.js";
import { EgressChokepoint, type DoorTransport } from "../egress-door.js";

export type WireOp =
	| {
			op: "send";
			chatId: string;
			content: string;
			metadata?: MetadataType | undefined;
			platform?: string | undefined;
	  }
	| {
			op: "edit";
			chatId: string;
			messageId: string;
			content: string;
			finalize?: boolean | undefined;
			metadata?: MetadataType | undefined;
			/** True when the transport FORCED this attempt to fail (observability). */
			failed?: boolean | undefined;
			platform?: string | undefined;
	  }
	| {
			op: "draft";
			chatId: string;
			draftId: number;
			content: string;
			final: boolean;
			/** Sealing frames carry the stream's message identity. */
			messageId?: string | undefined;
			metadata?: MetadataType | undefined;
	  }
	| { op: "delete"; chatId: string; messageId: string };

export type SendOp = Extract<WireOp, { op: "send" }>;
export type EditOp = Extract<WireOp, { op: "edit" }>;
export type DraftOp = Extract<WireOp, { op: "draft" }>;

abstract class FakeAdapterBase {
	/** The ONE audited chokepoint both doors route through (DEC-006). */
	readonly chokepoint: EgressChokepoint;

	ops: WireOp[] = [];

	// Connector-side knobs (model transport conditions).
	failSeals = false;
	failDraftFrames = false;
	/** Force progressive editMessage failures (fallback-final contracts). */
	failEdits = false;
	/** Force door-send failures (payload-less-split #78541 contracts). */
	failSends = false;

	// Probe observability for latch assertions.
	supportsProbeCalls = 0;
	isMessageProbeCalls = new Map<string, number>();

	private messageIdCounter = 0;
	private waiters: Array<(op: WireOp) => boolean> = [];

	constructor() {
		this.chokepoint = new EgressChokepoint(this.transport());
	}

	get audit() {
		return this.chokepoint.audit;
	}

	protected abstract isMessageAnswer(chatId: string): boolean;

	private transport(): DoorTransport {
		return {
			streamIsMessageForChat: (chatId) => this.isMessageAnswer(chatId),
			transmitSend: async (chatId, content, metadata, platform) => {
				if (this.failSends) {
					return { success: false, error: "forced send failure" };
				}
				const messageId = this.nextMessageId("msg");
				await this.pushOp({
					op: "send",
					chatId,
					content,
					metadata,
					platform,
				});
				return { success: true, messageId };
			},
			transmitEdit: async (
				chatId,
				messageId,
				content,
				opts,
				platform,
			): Promise<SendResult> => {
				if (this.failEdits) {
					// Record the ATTEMPT so strike-count contracts are observable.
					await this.pushOp({
						op: "edit",
						chatId,
						messageId,
						content,
						finalize: opts?.finalize,
						metadata: opts?.metadata,
						failed: true,
						platform,
					});
					return { success: false, error: "forced edit failure" };
				}
				await this.pushOp({
					op: "edit",
					chatId,
					messageId,
					content,
					finalize: opts?.finalize,
					metadata: opts?.metadata,
					platform,
				});
				return { success: true, messageId };
			},
			transmitSeal: async (_draftKey, chatId, draftId, content, metadata) => {
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
				// The connector returns the stream's ts as the message identity.
				return { success: true, messageId };
			},
		};
	}

	// ── capability probes (counted for latch tests) ───────────────────────

	supportsDraftStreaming(
		_chatType?: string | undefined,
		_metadata?: Metadata | undefined,
		_chatId?: string | number | undefined,
	): boolean {
		this.supportsProbeCalls += 1;
		return true;
	}

	streamIsMessageForChat(chatId: string): boolean {
		const key = String(chatId);
		this.isMessageProbeCalls.set(
			key,
			(this.isMessageProbeCalls.get(key) ?? 0) + 1,
		);
		return this.isMessageAnswer(key);
	}

	// ── door 1: send() ────────────────────────────────────────────────────

	async send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		return this.chokepoint.admit({
			door: "send",
			chatId,
			content,
			replyTo,
			metadata,
		});
	}

	// ── door 2: send_for_platform() — delivery-resolver lane ──────────────

	async sendForPlatform(
		logicalPlatform: string,
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		return this.chokepoint.admit({
			door: "send_for_platform",
			chatId,
			content,
			replyTo,
			metadata,
			platform: logicalPlatform,
		});
	}

	// ── native draft streaming ────────────────────────────────────────────

	async sendDraft(args: DraftFrameArgs): Promise<SendResult> {
		const verdict = this.chokepoint.draftAdmission(args);
		// Post-seal straggler: content already in the sealed message → report
		// success, send nothing, arm nothing (tombstone parity).
		if (verdict.swallow) return { success: true };
		await this.pushOp({
			op: "draft",
			chatId: args.chatId,
			draftId: args.draftId,
			content: args.content,
			final: false,
			metadata: args.metadata,
		});
		if (verdict.arm) this.chokepoint.armOpenDraft(verdict.key, args.draftId);
		if (this.failDraftFrames) {
			return { success: false, error: "forced draft-frame failure" };
		}
		return { success: true };
	}

	/** Direct edits bypass the doors (no interception semantics of their own). */
	async editMessage(
		chatId: string,
		messageId: string,
		content: string,
		opts?: EditOptions | undefined,
	): Promise<SendResult> {
		return this.transport().transmitEdit(
			chatId,
			messageId,
			content,
			{ finalize: opts?.finalize === true, metadata: opts?.metadata },
			undefined,
		);
	}

	/** Best-effort retraction used by silence-marker suppression. */
	async deleteMessage(chatId: string, messageId: string): Promise<boolean> {
		await this.pushOp({ op: "delete", chatId, messageId });
		return true;
	}

	/** Recorded stale-stream abandonments (_abandon_native_stream observability). */
	readonly abandons: Array<{ chatId: string; content: string }> = [];

	async abandonOpenDraft(chatId: string, content: string): Promise<void> {
		this.abandons.push({ chatId, content });
	}

	// ── test observability (event-based sync; no sleeps) ──────────────────

	async pushOp(op: WireOp): Promise<void> {
		this.ops.push(op);
		const current = this.waiters;
		this.waiters = [];
		for (const w of current) {
			// Non-matching waiters stay registered for FUTURE ops — a waiter
			// must survive intermediate ops its predicate rejects.
			if (!w(op)) this.waiters.push(w);
		}
	}

	waitForCount<T extends WireOp>(
		count: number,
		pred: (op: WireOp) => op is T,
		timeoutMs?: number,
	): Promise<T[]>;
	waitForCount(
		count: number,
		pred: (op: WireOp) => boolean,
		timeoutMs?: number,
	): Promise<WireOp[]>;
	async waitForCount(
		count: number,
		pred: (op: WireOp) => boolean,
		timeoutMs = 5_000,
	): Promise<WireOp[]> {
		const matched = (): WireOp[] => this.ops.filter(pred);
		if (matched().length >= count)
			return Promise.resolve(matched().slice(0, count));
		return new Promise<WireOp[]>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.waiters = this.waiters.filter((w) => w !== waiter);
				reject(new Error(`FakeAdapter.waitForCount(${count}) timed out`));
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

	waitFor<T extends WireOp>(pred: (op: WireOp) => op is T): Promise<T> {
		return this.waitForCount(1, pred).then((ops) => ops[0] as T);
	}

	draftFrames(): DraftOp[] {
		return this.ops.filter((o): o is DraftOp => o.op === "draft" && !o.final);
	}

	private nextMessageId(prefix: string): string {
		this.messageIdCounter += 1;
		return `${prefix}_${this.messageIdCounter}`;
	}
}

/** Telegram-shaped: stream is NOT the message; drafts clear client-side. */
export class FakeDraftStreamAdapter extends FakeAdapterBase {
	draftStreamIsMessage = false; // class fallback flag, off
	isMessage = false;

	protected isMessageAnswer(_chatId: string): boolean {
		return this.isMessage;
	}
}

/** Relay-shaped: ONE native stream per turn IS the message. */
export class FakeStreamIsMessageAdapter extends FakeAdapterBase {
	draftStreamIsMessage = true;
	isMessage = true;

	protected isMessageAnswer(_chatId: string): boolean {
		return this.isMessage;
	}
}
