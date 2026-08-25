// pi_platforms/signal/signal-wire — the signal-cli daemon transport SEAM plus
// the in-process fake server (04 §8: rows run headless against fake platform
// servers; NO external network, NO vendor SDK).
//
// Production binds this seam to HTTP calls against a LOCAL signal-cli daemon
// (`signal-cli daemon --http 127.0.0.1:8080`; signal.py module docstring):
//
//   POST /api/v1/rpc    {"jsonrpc":"2.0","method":M,"params":P,"id":ID}
//                       → {"result":R} | {"error":{code,message,data}}
//                       (send / sendTyping / sendReaction / getAttachment /
//                        listContacts / getContact — ONE endpoint)
//   GET  /api/v1/check  → 200 | non-200 (connect + health monitor)
//   GET  /api/v1/events?account=… → text/event-stream of signal-cli
//                       envelopes; ":" lines are keepalive comments, "data:"
//                       lines carry JSON envelopes (_sse_listener line loop)
//
// The FAKE models those wire shapes in-process: scriptable RPC error bodies
// (typed −5, "[429]", "Retry after N seconds", NETWORK_FAILURE…), captured
// calls, an attachment store, and a controllable SSE pump whose backlog
// semantics are documented on the class (fixture semantics, not a claim
// about any particular signal-cli build).

import type { Metadata } from "../../pi_gateway/streaming/adapter-seam.js";

export type RpcErrorBody = {
	code?: number;
	message: string;
	data?: Record<string, unknown>;
};

export type RpcOutcome =
	| { ok: true; result: unknown }
	| { ok: false; error: RpcErrorBody };

export interface SignalRpcCall {
	method: string;
	params: Record<string, unknown>;
	id: string;
	/** The outcome result the server returned for this call (capture aid). */
	result?: unknown;
}

/** A live SSE connection: raw text chunks in order until close(). */
export interface SignalEventStream {
	/** Raw SSE text chunks, in order, until close(). */
	readonly chunks: AsyncIterable<string>;
	close(): void;
	readonly closed: boolean;
}

/**
 * THE transport seam. The adapter NEVER imports http/undici — production and
 * tests supply different implementations.
 */
export interface SignalCliTransport {
	rpc(
		method: string,
		params: Record<string, unknown>,
		opts?: {
			id?: string;
			/**
			 * DOOR metadata passed through verbatim (wa-cloud seam parity):
			 * production transports ignore it; the conformance harness reads its
			 * script markers (e.g. forceFormattingError).
			 */
			metadata?: Metadata;
		},
	): Promise<RpcOutcome>;
	/** GET /api/v1/check — true ⇔ 200. */
	checkHealth(): Promise<boolean>;
	/** Open the SSE event stream for the configured account. */
	openEventStream(): Promise<SignalEventStream>;
	/**
	 * Harness seam ONLY: whether a "rich" behavior is scripted on the fake
	 * wire (models an optional rich endpoint for the §10.1 latch row — the
	 * real signal-cli RPC surface has none, so the default false keeps the
	 * ladder latched off without burning a wire roundtrip).
	 */
	hasRichScript?(): boolean;
	/** Harness seam ONLY: record/consume one rich-probe attempt. */
	transmitRichProbe?(chatId: string, content: string): Promise<boolean>;
}

/** Build one JSON-RPC 2.0 request body (_rpc payload shape). */
export function jsonRpcBody(
	method: string,
	params: Record<string, unknown>,
	id: string,
): Record<string, unknown> {
	return { jsonrpc: "2.0", method, params, id };
}

/** Encode one SSE data frame ("data: <json>\n\n"). */
export function sseData(payload: unknown): string {
	return `data: ${JSON.stringify(payload)}\n\n`;
}

/** Encode one SSE keepalive comment (":" proves liveness upstream). */
export function sseKeepalive(): string {
	return ": keepalive\n\n";
}

type ScriptedFailure = RpcErrorBody;

interface QueuedEvent {
	text: string;
}

/**
 * In-memory signal-cli daemon double. Behaviors are consumed FIFO per method;
 * an exhausted script defaults to the vendor-shaped success body:
 *   send/sendTyping/sendReaction → {"timestamp":N,"results":[{"type":"SUCCESS"}]}
 *   getAttachment → {"data":"<base64>"}
 *   listContacts → seeded contacts · getContact → seeded name
 *
 * SSE backlog fixture semantics: events pushed while NO consumer holds the
 * stream (or after the stream was dropped mid-life) queue in `backlog` and
 * flush BEFORE new events on the next openEventStream(). Delivered events do
 * NOT redeliver. This models "the daemon held what we could not consume" so
 * reconnect rows can assert zero-loss/exactly-once downstream without
 * inventing a cursor protocol (signal-cli has none).
 */
export class FakeSignalCliServer implements SignalCliTransport {
	readonly rpcCalls: SignalRpcCall[] = [];
	private readonly failureScripts = new Map<string, ScriptedFailure[]>();
	private readonly resultScripts = new Map<string, unknown[]>();
	private healthOk = true;
	private refuseEvents = false;

	private contacts: Array<Record<string, unknown>> = [];
	private contactNames = new Map<string, string>();
	private readonly attachments = new Map<string, Buffer>();
	private nextAttachmentNum = 0;
	private nextTimestamp = 1_700_000_000_000;

	readonly connectionLog: Array<{ atMs: number }> = [];
	readonly drops: Array<{ atMs: number; reason: string }> = [];
	private clockNow: () => number = () => Date.now();

	private activeStream: FakeEventStream | null = null;
	private readonly backlog: QueuedEvent[] = [];

	/** Wire the server's clock (fixtures inject the manual clock). */
	setClock(nowMs: () => number): void {
		this.clockNow = nowMs;
	}

	// ── scripting ──

	/** Program the next N RPC failures for a method (error bodies). */
	scriptRpcFailure(method: string, ...errors: ScriptedFailure[]): void {
		const q = this.failureScripts.get(method) ?? [];
		q.push(...errors);
		this.failureScripts.set(method, q);
	}

	/** Override the next N success results for a method verbatim. */
	scriptRpcResult(method: string, ...results: unknown[]): void {
		const q = this.resultScripts.get(method) ?? [];
		q.push(...results);
		this.resultScripts.set(method, q);
	}

	setHealth(ok: boolean): void {
		this.healthOk = ok;
	}

	/** Make openEventStream() reject (daemon unreachable). */
	refuseEventConnections(refuse: boolean): void {
		this.refuseEvents = refuse;
	}

	setContacts(contacts: Array<Record<string, unknown>>): void {
		this.contacts = contacts;
	}

	setContactName(recipient: string, name: string): void {
		this.contactNames.set(recipient, name);
	}

	seedAttachment(bytes: Buffer): string {
		this.nextAttachmentNum += 1;
		const id = `att-${this.nextAttachmentNum}`;
		this.attachments.set(id, bytes);
		return id;
	}

	// ── SSE pump ──

	pushEvent(envelope: unknown): void {
		if (this.activeStream !== null) {
			this.activeStream.push(sseData(envelope));
		} else {
			this.backlog.push({ text: sseData(envelope) });
		}
	}

	/** Keepalive comment through the LIVE stream only (proves activity). */
	pushKeepalive(): void {
		this.activeStream?.push(sseKeepalive());
	}

	/** Tear the live stream (outage parity: client sees EOF mid-stream). */
	dropStream(reason: string): void {
		if (this.activeStream !== null) {
			this.drops.push({ atMs: this.clockNow(), reason });
			this.activeStream.close();
		}
	}

	get hasLiveStream(): boolean {
		return this.activeStream !== null && !this.activeStream.closed;
	}

	get backlogDepth(): number {
		return this.backlog.length;
	}

	callsOf(method: string): SignalRpcCall[] {
		return this.rpcCalls.filter((c) => c.method === method);
	}

	// ── SignalCliTransport ──

	async rpc(
		method: string,
		params: Record<string, unknown>,
		opts?: { id?: string; metadata?: Metadata },
	): Promise<RpcOutcome> {
		void opts?.metadata; // production fake ignores door metadata
		const rpcId = opts?.id ?? `${method}_${this.clockNow()}`;
		this.rpcCalls.push({
			method,
			params,
			id: rpcId,
		});
		const failures = this.failureScripts.get(method) ?? [];
		const failure = failures.shift();
		if (failure !== undefined) return { ok: false, error: failure };

		const scripted = (this.resultScripts.get(method) ?? []).shift();
		if (scripted !== undefined) {
			this.recordCallResult(rpcId, scripted);
			return { ok: true, result: scripted };
		}

		switch (method) {
			case "send": {
				const ts = this.nextTimestamp++;
				const body = {
					timestamp: ts,
					results: [{ type: "SUCCESS", timestamp: ts }],
				};
				this.recordCallResult(rpcId, body);
				return { ok: true, result: body };
			}
			case "sendTyping":
			case "sendReaction":
				return { ok: true, result: {} };
			case "getAttachment": {
				const id = String(params["id"] ?? "");
				const bytes = this.attachments.get(id);
				if (bytes === undefined) {
					return {
						ok: false,
						error: { code: -32602, message: "Unknown attachment" },
					};
				}
				return { ok: true, result: { data: bytes.toString("base64") } };
			}
			case "listContacts":
				return { ok: true, result: this.contacts };
			case "getContact": {
				const addr = String(params["contactAddress"] ?? "");
				const name = this.contactNames.get(addr);
				return { ok: true, result: name ? { name } : {} };
			}
			default:
				return { ok: true, result: {} };
		}
	}

	private recordCallResult(rpcId: string, result: unknown): void {
		for (let i = this.rpcCalls.length - 1; i >= 0; i--) {
			const call = this.rpcCalls[i];
			if (call !== undefined && call.id === rpcId) {
				call.result = result;
				return;
			}
		}
	}

	async checkHealth(): Promise<boolean> {
		return this.healthOk;
	}

	async openEventStream(): Promise<SignalEventStream> {
		if (this.refuseEvents) throw new Error("cannot reach signal-cli daemon");
		this.connectionLog.push({ atMs: this.clockNow() });
		const prior = this.activeStream;
		if (prior !== null && !prior.closed) prior.close();
		const stream = new FakeEventStream(() => {
			if (this.activeStream === stream) this.activeStream = null;
		});
		this.activeStream = stream;
		// Backlog first — the daemon delivers what we could not consume.
		while (this.backlog.length > 0) {
			const item = this.backlog.shift();
			if (item !== undefined) stream.push(item.text);
		}
		return stream;
	}
}

/**
 * One live SSE byte pipe. Chunks are delivered through an async iterator that
 * pends until data or close — mirroring a hung-open HTTP response body.
 */
class FakeEventStream implements SignalEventStream {
	private readonly queue: string[] = [];
	private wake: (() => void) | null = null;
	private done = false;

	readonly chunks: AsyncIterable<string>;

	constructor(private onClose: () => void) {
		this.chunks = {
			[Symbol.asyncIterator]: (): AsyncIterator<string> => ({
				next: async (): Promise<IteratorResult<string>> => {
					for (;;) {
						const item = this.queue.shift();
						if (item !== undefined) return { value: item, done: false };
						if (this.done) return { value: undefined, done: true };
						await new Promise<void>((r) => {
							this.wake = r;
						});
					}
				},
			}),
		};
	}

	get closed(): boolean {
		return this.done;
	}

	push(text: string): void {
		if (this.done) return;
		this.queue.push(text);
		this.wake?.();
		this.wake = null;
	}

	close(): void {
		if (this.done) return;
		this.done = true;
		this.wake?.();
		this.wake = null;
		this.onClose();
	}
}
