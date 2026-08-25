// pi_platforms/qqbot/fake-qq-gateway — the IN-PROCESS fake QQ open-platform
// (04 §8: rows run headless against fake platform servers; NO real network).
//
// Two faces:
//   • the GATEWAY WebSocket face speaking the QQ op-code protocol
//     (op 10 Hello → Identify(2)/Resume(6); op 0 dispatch; op 11 heartbeat
//     ACK; op 7 server-reconnect; op 9 invalid-session) with scripted closes;
//   • the REST face (token, /gateway, v2 messages, interactions ACK, media +
//     chunked-upload endpoints) with scriptable failures carrying vendor
//     biz_codes in error MESSAGES (never snapshotted vendor strings — the
//     uploader matches codes numerically, mirroring Hermes).
//
// Resume semantics mirror the QQ protocol as Hermes uses it
// (adapter.py:_send_resume): on RESUME the server replays every dispatch with
// seq > the client's last_seq, then RESUMED.

export type QQGatewayPayload = {
	op: number;
	d?: unknown;
	s?: number;
	t?: string;
};

export type QQCloseInfo = {
	code: number;
	reason?: string | undefined;
};

export interface QQSocketListener {
	onOpen(): void;
	onPayload(payload: QQGatewayPayload): void;
	onClose(info: QQCloseInfo): void;
	/** Hard transport death WITHOUT a close frame (dead-TCP shape). */
	onError(err: Error): void;
}

export interface QQClientSocket {
	readonly readyState: "connecting" | "open" | "closed";
	sendPayload(payload: QQGatewayPayload): void;
	close(code?: number): void;
	listener: QQSocketListener;
}

export type RestBehavior =
	| { kind: "ok"; body?: Record<string, unknown> }
	| { kind: "fail"; message: string };

export interface RestCallRecord {
	key: string;
	method: "POST" | "GET" | "PUT";
	path: string;
	body: Record<string, unknown>;
	headers?: Record<string, string> | undefined;
	seq: number;
}

const OPEN = "open" as const;

interface ServerConn {
	socket: QQClientSocketImpl;
	sessionId: string | null;
	lastSeqAcked: number;
}

/**
 * The fake platform. Scenario controls: helloIntervalMs, scripted closes,
 * connection refusal, hard drops, REST failure scripts, and the replay queue
 * that holds dispatches while no live session exists.
 */
export class FakeQQGateway {
	private readonly connections: ServerConn[] = [];
	private readonly replayQueue: Array<{ payload: QQGatewayPayload }> = [];
	private seqCounter = 0;
	private sessionCounter = 0;

	acceptNext = true;

	/** Scripted failures per REST endpoint key ("token", "messages:c2c", …). */
	private readonly scripts = new Map<string, RestBehavior[]>();
	/** Every REST transmission, in order (row observability). */
	readonly restCalls: RestCallRecord[] = [];

	helloIntervalMs = 30_000;

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === OPEN).length;
	}

	// ── scenario controls ───────────────────────────────────────────────

	script(key: string, ...behaviors: RestBehavior[]): void {
		const q = this.scripts.get(key) ?? [];
		q.push(...behaviors);
		this.scripts.set(key, q);
	}

	refuseConnections(): void {
		this.acceptNext = false;
	}

	acceptConnections(): void {
		this.acceptNext = true;
	}

	/** Close every live socket with a QQ close code (4008 carries Retry-After). */
	dropActive(code: number, reason = ""): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({ code, reason });
		}
	}

	/** Kill the socket WITHOUT a close frame — the dead-TCP shape. */
	hardDrop(): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverHardDrop();
		}
	}

	/** Push a dispatch event to live sockets; otherwise hold for resume replay. */
	pushDispatch(t: string, d: Record<string, unknown>): void {
		this.seqCounter += 1;
		const payload: QQGatewayPayload = { op: 0, t, d, s: this.seqCounter };
		let delivered = false;
		for (const conn of this.connections) {
			if (conn.socket.readyState === OPEN && conn.sessionId !== null) {
				conn.socket.serverDeliver(payload);
				delivered = true;
			}
		}
		if (!delivered) this.replayQueue.push({ payload });
	}

	/** Dispatches currently held for post-resume replay (observability). */
	get heldDispatchCount(): number {
		return this.replayQueue.length;
	}

	clearReplay(): void {
		this.replayQueue.length = 0;
	}

	/** The REST calls recorded against one endpoint key. */
	callsOf(key: string): RestCallRecord[] {
		return this.restCalls.filter((c) => c.key === key);
	}

	// ── REST face ───────────────────────────────────────────────────────────

	private tokenCounter = 0;
	private msgCounter = 0;
	accessToken = "fake-access-token";
	expiresInS = 7200;
	gatewayUrl = "wss://fake-qq-gateway.invalid/gateway";

	/**
	 * Route one REST request. Failures resolve as {status≥400, body} — the
	 * ADAPTER turns them into errors embedding the numeric biz_code/status
	 * (Hermes `_api_request` message shape), so uploader code matches codes
	 * numerically instead of snapshotting vendor strings.
	 */
	handleRest(
		method: "POST" | "GET" | "PUT",
		path: string,
		body: Record<string, unknown>,
		headers?: Record<string, string> | undefined,
	): { status: number; body: Record<string, unknown> } {
		if (path.includes("getAppAccessToken")) {
			const b = this.nextRest("token", {
				key: "token",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 500, body: { message: b.message } };
			this.tokenCounter += 1;
			return {
				status: 200,
				body:
					b.body && Object.keys(b.body).length > 0
						? b.body
						: {
								access_token: `${this.accessToken}-${this.tokenCounter}`,
								expires_in: this.expiresInS,
							},
			};
		}
		if (path.endsWith("/gateway")) {
			return { status: 200, body: { url: this.gatewayUrl } };
		}
		if (path.startsWith("/v2/users/") && path.endsWith("/messages")) {
			const b = this.nextRest("messages:c2c", {
				key: "messages:c2c",
				method,
				path,
				body,
				headers,
			});
			return this.finishMessageSend(b);
		}
		if (path.startsWith("/v2/groups/") && path.endsWith("/messages")) {
			const b = this.nextRest("messages:group", {
				key: "messages:group",
				method,
				path,
				body,
				headers,
			});
			return this.finishMessageSend(b);
		}
		if (path.startsWith("/channels/") && path.endsWith("/messages")) {
			const b = this.nextRest("messages:guild", {
				key: "messages:guild",
				method,
				path,
				body,
				headers,
			});
			return this.finishMessageSend(b);
		}
		if (path.startsWith("/interactions/")) {
			const b = this.nextRest("interactions", {
				key: "interactions",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 500, body: { message: b.message } };
			return { status: 204, body: {} };
		}
		if (path.endsWith("/upload_prepare")) {
			const b = this.nextRest("upload_prepare", {
				key: "upload_prepare",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 400, body: { message: b.message } };
			return { status: 200, body: b.body ?? {} };
		}
		if (path.endsWith("/upload_part_finish")) {
			const b = this.nextRest("upload_part_finish", {
				key: "upload_part_finish",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 400, body: { message: b.message } };
			return { status: 200, body: {} };
		}
		if (path.startsWith("/cos-part/")) {
			const b = this.nextRest("cos-part", {
				key: "cos-part",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 503, body: { message: b.message } };
			return { status: 200, body: {} };
		}
		if (path.endsWith("/files")) {
			const b = this.nextRest("files", {
				key: "files",
				method,
				path,
				body,
				headers,
			});
			if (b.kind === "fail")
				return { status: 400, body: { message: b.message } };
			return {
				status: 200,
				body:
					b.body && Object.keys(b.body).length > 0
						? b.body
						: { file_info: "fi-fake" },
			};
		}
		return { status: 404, body: { message: `no route: ${path}` } };
	}

	private finishMessageSend(b: RestBehavior): {
		status: number;
		body: Record<string, unknown>;
	} {
		if (b.kind === "fail") return { status: 400, body: { message: b.message } };
		this.msgCounter += 1;
		return {
			status: 200,
			body:
				b.body && Object.keys(b.body).length > 0
					? b.body
					: { id: `wmsg-${this.msgCounter}` },
		};
	}

	// ── internal wiring ────────────────────────────────────────────────

	connect(listener: QQSocketListener): QQClientSocket {
		const socket = new QQClientSocketImpl(listener, this);
		if (!this.acceptNext) {
			queueMicrotask(() => socket.serverRefuse());
			return socket;
		}
		queueMicrotask(() => socket.serverAccept());
		return socket;
	}

	internalRegister(conn: ServerConn): void {
		this.connections.push(conn);
	}

	internalRemove(conn: ServerConn): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}

	internalHandleClientPayload(
		conn: ServerConn,
		payload: QQGatewayPayload,
	): void {
		if (payload.op === 2) {
			// Identify → READY with a fresh session id.
			this.sessionCounter += 1;
			conn.sessionId = `sess-${this.sessionCounter}`;
			this.seqCounter += 1;
			conn.socket.serverDeliver({
				op: 0,
				t: "READY",
				s: this.seqCounter,
				d: { session_id: conn.sessionId, user: { id: "bot-self" } },
			});
			// Replay anything queued while no session existed.
			const held = this.replayQueue.splice(0);
			for (const { payload: p } of held) conn.socket.serverDeliver(p);
			return;
		}
		if (payload.op === 6) {
			// Resume → replay every held dispatch with s > the client's last
			// seq, THEN RESUMED stamped with the replayed high-water mark (the
			// resumed session keeps its id; new pushes flow live again).
			const d = (payload.d ?? {}) as Record<string, unknown>;
			const lastSeq = Number(d["seq"] ?? 0);
			conn.sessionId = String(d["session_id"] ?? conn.sessionId ?? "");
			let highWater = Math.max(lastSeq, 0);
			const held = this.replayQueue.splice(0);
			for (const { payload: p } of held) {
				if ((p.s ?? 0) > lastSeq) {
					conn.socket.serverDeliver(p);
					highWater = Math.max(highWater, p.s ?? 0);
				} else {
					this.replayQueue.push({ payload: p }); // already-seen: keep
				}
			}
			this.seqCounter = Math.max(this.seqCounter, highWater);
			conn.socket.serverDeliver({
				op: 0,
				t: "RESUMED",
				s: highWater,
				d: null,
			});
			return;
		}
		if (payload.op === 1) {
			// Heartbeat → op 11 ACK.
			conn.socket.serverDeliver({ op: 11, d: null });
		}
	}

	nextRest(key: string, record: Omit<RestCallRecord, "seq">): RestBehavior {
		this.restCalls.push({ ...record, seq: this.restCalls.length + 1 });
		const q = this.scripts.get(key);
		if (q === undefined || q.length === 0) return { kind: "ok", body: {} };
		return q.shift() as RestBehavior;
	}

	newSessionId(): string {
		this.sessionCounter += 1;
		return `sess-${this.sessionCounter}`;
	}
}

class QQClientSocketImpl implements QQClientSocket {
	private state: "connecting" | "open" | "closed" = "connecting";
	private conn: ServerConn | null = null;
	listener: QQSocketListener;
	private readonly serverRef: FakeQQGateway;

	constructor(listener: QQSocketListener, server: FakeQQGateway) {
		this.listener = listener;
		this.serverRef = server;
	}

	get readyState(): "connecting" | "open" | "closed" {
		return this.state;
	}

	sendPayload(payload: QQGatewayPayload): void {
		if (this.state !== OPEN) {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.serverRef.internalHandleClientPayload(
			this.conn as ServerConn,
			payload,
		);
	}

	close(code = 1000): void {
		if (this.state !== OPEN && this.state !== "connecting") return;
		this.detach();
		this.state = "closed";
		this.listener.onClose({ code });
	}

	serverAccept(): void {
		if (this.state !== "connecting") return;
		this.conn = { socket: this, sessionId: null, lastSeqAcked: 0 };
		this.serverRef.internalRegister(this.conn);
		this.state = OPEN;
		this.listener.onOpen();
		// QQ gateway: op 10 Hello FIRST with the heartbeat interval.
		this.listener.onPayload({
			op: 10,
			d: { heartbeat_interval: this.serverRef.helloIntervalMs },
		});
	}

	serverRefuse(): void {
		if (this.state !== "connecting") return;
		this.state = "closed";
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006 });
	}

	serverDeliver(payload: QQGatewayPayload): void {
		if (this.state !== OPEN) return;
		if (typeof payload.s === "number") {
			(this.conn as ServerConn).lastSeqAcked = Math.max(
				(this.conn as ServerConn).lastSeqAcked,
				payload.s,
			);
		}
		this.listener.onPayload(payload);
	}

	serverClose(info: QQCloseInfo): void {
		if (this.state !== OPEN && this.state !== "connecting") return;
		this.detach();
		this.state = "closed";
		this.listener.onClose(info);
	}

	/** Dead-TCP: error surfaces, NO close frame follows. */
	serverHardDrop(): void {
		if (this.state !== OPEN) return;
		this.detach();
		this.state = "closed";
		this.listener.onError(new Error("socket hang up (ECONNRESET)"));
	}

	private detach(): void {
		if (this.conn) {
			this.serverRef.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
