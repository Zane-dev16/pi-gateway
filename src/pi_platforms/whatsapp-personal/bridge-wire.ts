// pi_platforms/whatsapp-personal/bridge-wire — the LOCAL NODE BRIDGE HTTP
// plane as an INJECTED transport SEAM plus the in-process fake server
// (04 §8: rows run headless against fake platform servers; NO sockets, NO OS
// children, NO real fetch — mirror of whatsapp-cloud/graph-wire.ts).
//
// The Hermes personal adapter talks to a Node.js bridge daemon over loopback
// HTTP (adapter.py): GET /messages poll, POST /send, POST /edit, POST /read,
// GET /health. The port NEVER spawns that daemon — production binds this seam
// to loopback fetch calls; tests bind FakeBridgeServer with scriptable
// failures/timeouts, full call capture, a seeded LID-mapping table, and a
// bridge-side inbound queue.
//
// CURSOR SEMANTICS (modeled from what the source implies): the poll loop
// treats GET /messages as returning "the messages queued since last time" —
// there is NO ack parameter and NO client-side cursor in adapter.py, so the
// BRIDGE owns both queue and cursor and hands each message ONCE per successful
// 200 handoff (drain-on-read). A failed/timed-out GET therefore PRESERVES the
// queue server-side (exactly the outage-reconnect contract), while messages
// already handed off are never redelivered (at-most-once downstream). When
// the bridge PROCESS dies, its buffer dies with it — crash-respawn drops
// pending updates by construction (pinned in the conflict fixture).

import { normalizeWhatsappIdentifier } from "../../pi_gateway/resolution/whatsapp-identity.js";

/** Envelope every bridge edge resolves to (HTTP status + parsed body/text). */
export interface BridgeCallEnvelope {
	status: number;
	json?: unknown;
	text?: string;
}

export interface BridgeSendPayload {
	chatId: string;
	message: string;
	/** Reply-context message id — FIRST text chunk ONLY (adapter.py:send). */
	replyTo?: string | undefined;
}

export interface BridgeEditPayload {
	chatId: string;
	messageId: string;
	message: string;
}

/**
 * THE transport seam. The adapter NEVER imports http/undici/node:net —
 * production and tests supply different implementations of these five calls.
 */
export interface BridgeTransport {
	/** GET /messages — drain-and-return one batch of queued inbound messages. */
	getMessages(): Promise<BridgeCallEnvelope>;
	/**
	 * POST /send {chatId, message, replyTo?} → 200 {messageId}. The optional
	 * `metadata` bag is DOOR metadata passed through verbatim; production
	 * transports ignore it, the conformance harness reads its script markers
	 * (e.g. forceFormattingError) exactly like the reference subjects do.
	 */
	sendText(
		payload: BridgeSendPayload,
		metadata?: Record<string, unknown>,
	): Promise<BridgeCallEnvelope>;
	/** POST /edit {chatId, messageId, message} (15s window at the call site). */
	editMessage(payload: BridgeEditPayload): Promise<BridgeCallEnvelope>;
	/** POST /read {key} — fire-and-forget read receipt. */
	markRead(key: Record<string, unknown>): Promise<{ status: number }>;
	/** GET /health {status} — heartbeat-escalation verdict source. */
	getHealthStatus(): Promise<BridgeCallEnvelope>;
}

/** Scriptable failure consumed FIFO per endpoint. */
export type BridgeScriptedFailure =
	| { kind: "http"; status: number; body?: string }
	| { kind: "timeout" }
	| { kind: "down" };

export type BridgeEndpoint = "messages" | "send" | "edit" | "read" | "health";

export interface RecordedBridgeSend {
	payload: BridgeSendPayload;
	seq: number;
}

/**
 * In-memory Baileys-bridge double. Behaviors are consumed FIFO per endpoint;
 * an exhausted script defaults to the vendor-shaped success body.
 */
export class FakeBridgeServer implements BridgeTransport {
	private scripts = new Map<BridgeEndpoint, BridgeScriptedFailure[]>();
	private seqCounter = 0;

	readonly polls: Array<{ seq: number }> = [];
	readonly sentMessages: RecordedBridgeSend[] = [];
	readonly edits: Array<{ payload: BridgeEditPayload; seq: number }> = [];
	readonly readReceipts: Array<{
		key: Record<string, unknown>;
		seq: number;
	}> = [];
	readonly healthProbes: Array<{ seq: number }> = [];

	/** Bridge-side inbound queue (drained on successful GET handoff). */
	private readonly inboundQueue: Array<Record<string, unknown>> = [];
	/** WhatsApp connection verdict served on /health. */
	private healthStatus: "connected" | "disconnected" = "connected";
	/** Process-liveness model: a "down" bridge rejects every edge. */
	private down = false;

	/** Undrained inbound count (outage-preservation probe). */
	get pendingCount(): number {
		return this.inboundQueue.length;
	}

	queueInbound(message: Record<string, unknown>): void {
		this.inboundQueue.push(message);
	}

	setHealthStatus(status: "connected" | "disconnected"): void {
		this.healthStatus = status;
	}

	setDown(down: boolean): void {
		this.down = down;
	}

	isDown(): boolean {
		return this.down;
	}

	/** Crash simulation: the process AND its undrained queue die together. */
	crash(): void {
		this.down = true;
		this.inboundQueue.length = 0;
	}

	/** Program the next N failures for an endpoint kind. */
	script(endpoint: BridgeEndpoint, ...failures: BridgeScriptedFailure[]): void {
		const queue = this.scripts.get(endpoint) ?? [];
		queue.push(...failures);
		this.scripts.set(endpoint, queue);
	}

	hasScript(endpoint: BridgeEndpoint): boolean {
		return (this.scripts.get(endpoint)?.length ?? 0) > 0;
	}

	private next(endpoint: BridgeEndpoint): BridgeScriptedFailure | undefined {
		const queue = this.scripts.get(endpoint);
		if (queue === undefined || queue.length === 0) return undefined;
		return queue.shift();
	}

	reset(): void {
		this.scripts.clear();
		this.polls.length = 0;
		this.sentMessages.length = 0;
		this.edits.length = 0;
		this.readReceipts.length = 0;
		this.healthProbes.length = 0;
		this.inboundQueue.length = 0;
	}

	// ── seeded LID mappings (gateway/whatsapp_identity parity surface) ──────

	/** phone/lid alias pairs, stored BOTH directions like lid-mapping files. */
	private readonly aliasPairs = new Map<string, Set<string>>();

	seedAlias(a: string, b: string): void {
		const na = normalizeWhatsappIdentifier(a);
		const nb = normalizeWhatsappIdentifier(b);
		if (!na || !nb) return;
		for (const [from, to] of [
			[na, nb],
			[nb, na],
		] as const) {
			const set = this.aliasPairs.get(from) ?? new Set<string>();
			set.add(to);
			this.aliasPairs.set(from, set);
		}
	}

	/**
	 * Transitive alias closure ALWAYS containing the normalized input itself
	 * (expand_whatsapp_aliases contract); unmapped ids degrade to singleton.
	 */
	expandAliases(id: string): Set<string> {
		const start = normalizeWhatsappIdentifier(id);
		if (!start) return new Set();
		const resolved = new Set<string>([start]);
		const queue = [start];
		while (queue.length > 0) {
			const current = queue.shift() as string;
			for (const mapped of this.aliasPairs.get(current) ?? []) {
				if (!resolved.has(mapped)) {
					resolved.add(mapped);
					queue.push(mapped);
				}
			}
		}
		return resolved;
	}

	nextFailure(endpoint: BridgeEndpoint): BridgeScriptedFailure | undefined {
		return this.next(endpoint);
	}

	/** Shape a scripted failure into the call envelope/exception vocabulary. */
	static shapeFailure(failure: BridgeScriptedFailure): BridgeCallEnvelope {
		if (failure.kind === "http") {
			return {
				status: failure.status,
				...(failure.body !== undefined ? { text: failure.body } : {}),
			};
		}
		if (failure.kind === "timeout") {
			throw new Error("bridge request timed out");
		}
		throw new Error("bridge unreachable");
	}

	private guardDown(): void {
		if (this.down) throw new Error("bridge unreachable (ECONNREFUSED)");
	}

	private nextSeq(): number {
		this.seqCounter += 1;
		return this.seqCounter;
	}

	// ── BridgeTransport ──

	async getMessages(): Promise<BridgeCallEnvelope> {
		this.guardDown();
		this.polls.push({ seq: this.nextSeq() });
		const failure = this.next("messages");
		if (failure !== undefined) {
			if (failure.kind === "timeout") {
				throw new Error("bridge request timed out");
			}
			if (failure.kind === "down") throw new Error("bridge unreachable");
			return {
				status: failure.status,
				...(failure.body !== undefined ? { text: failure.body } : {}),
			};
		}
		// Drain-on-read: the batch leaves the bridge-side queue on the 200
		// handoff (cursor owned SERVER-SIDE — see module header).
		const batch = this.inboundQueue.splice(0, this.inboundQueue.length);
		return { status: 200, json: batch };
	}

	async sendText(payload: BridgeSendPayload): Promise<BridgeCallEnvelope> {
		this.guardDown();
		this.sentMessages.push({ payload, seq: this.nextSeq() });
		const failure = this.next("send");
		if (failure !== undefined) {
			if (failure.kind === "timeout") {
				throw new Error("bridge request timed out");
			}
			if (failure.kind === "down") throw new Error("bridge unreachable");
			return {
				status: failure.status,
				...(failure.body !== undefined ? { text: failure.body } : {}),
			};
		}
		return {
			status: 200,
			json: { messageId: `wamid.out.${this.seqCounter}` },
		};
	}

	async editMessage(payload: BridgeEditPayload): Promise<BridgeCallEnvelope> {
		this.guardDown();
		this.edits.push({ payload, seq: this.nextSeq() });
		const failure = this.next("edit");
		if (failure !== undefined) {
			if (failure.kind === "timeout") {
				throw new Error("bridge request timed out");
			}
			if (failure.kind === "down") throw new Error("bridge unreachable");
			return {
				status: failure.status,
				...(failure.body !== undefined ? { text: failure.body } : {}),
			};
		}
		return { status: 200, json: { messageId: payload.messageId } };
	}

	async markRead(key: Record<string, unknown>): Promise<{ status: number }> {
		this.guardDown();
		this.readReceipts.push({ key, seq: this.nextSeq() });
		const failure = this.next("read");
		if (failure !== undefined) {
			if (failure.kind === "down") throw new Error("bridge unreachable");
			if (failure.kind === "timeout") {
				throw new Error("bridge request timed out");
			}
			return { status: failure.status };
		}
		return { status: 200 };
	}

	async getHealthStatus(): Promise<BridgeCallEnvelope> {
		this.guardDown();
		this.healthProbes.push({ seq: this.nextSeq() });
		const failure = this.next("health");
		if (failure !== undefined) {
			if (failure.kind === "timeout") {
				throw new Error("bridge request timed out");
			}
			if (failure.kind === "down") throw new Error("bridge unreachable");
			return {
				status: failure.status,
				...(failure.body !== undefined ? { text: failure.body } : {}),
			};
		}
		return { status: 200, json: { status: this.healthStatus } };
	}
}
