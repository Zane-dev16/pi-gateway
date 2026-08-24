// pi_platforms/discord/gateway-fake — the IN-PROCESS Gateway v10-shaped fake
// websocket plane (04 §8 headless rule: no external network, no vendor SDK).
//
// THE SHAPE DELTA vs the persistent-ws reference fixture (roadmap §Phase 6
// heuristic 2): Discord resumes via session_id + SEQUENCE NUMBER — the server
// replays every dispatch with s > resume-seq from its buffer — NOT via an
// event-id cursor. The envelope is the vendor dispatch model:
//   { op, t (event type|null), s (monotonic sequence|null), d (payload) }
// with application-level heartbeats (op 1 → op 11 ack), IDENTIFY (op 2),
// RESUME (op 6), RECONNECT (op 7), INVALID_SESSION (op 9), HELLO (op 10).
// Opcode/close-code values are transcribed vendor ground truth (manifest.ts).

import type { NowFn } from "./clock.js";
import {
	GATEWAY_OPCODES as OP,
	HEARTBEAT_INTERVAL_MS_DEFAULT,
} from "./manifest.js";

/** One JSON gateway frame. */
export interface GatewayFrame {
	op: number;
	/** Dispatch event type (op 0 only). */
	t?: string | null;
	/** Monotonic sequence (dispatches + READY/RESUMED only). */
	s?: number | null;
	d?: unknown;
}

/** MESSAGE_CREATE payload body the fake accepts (vendor field subset). */
export interface MessageCreateBody {
	id: string;
	channelId: string;
	guildId?: string | undefined;
	authorId: string;
	/** Vendor author.bot discriminator (allow_bots admission policy input). */
	authorBot?: boolean | undefined;
	content: string;
	/** Vendor message type: 0 default, 19 reply. Default 0. */
	messageType?: number | undefined;
	referencedMessageId?: string | undefined;
	/** True when the message lives inside a thread channel. */
	isThread?: boolean | undefined;
	threadId?: string | undefined;
	mentionIds?: readonly string[] | undefined;
}

export interface GatewayCloseInfo {
	code: number;
	reason: string;
	/**
	 * Server-authoritative Retry-After carried on rate-limited closes
	 * (`_extract_discord_retry_after` capture parity, adapter.py:2312-2335).
	 */
	retryAfterSeconds?: number | undefined;
}

export interface GatewaySocketListener {
	onOpen(): void;
	onFrame(frame: GatewayFrame): void;
	onClose(info: GatewayCloseInfo): void;
	onError(err: Error): void;
}

export interface GatewayClientSocket {
	readonly readyState: 0 | 1 | 3;
	send(frame: GatewayFrame): void;
	close(code?: number, reason?: string): void;
}

/** Connection factory seam the adapter consumes (substitutable). */
export interface GatewayConnectionFactory {
	connect(listener: GatewaySocketListener): GatewayClientSocket;
}

interface ServerSession {
	sessionId: string;
	alive: boolean;
}

interface ServerSideConn {
	socket: GatewayClientSocketImpl;
	helloSent: boolean;
	identified: boolean;
	sessionId: string | null;
	stalledAcks: boolean;
}

let sessionCounter = 0;

/**
 * The fake platform's Gateway v10 endpoint. Scenario controls cover the whole
 * reconnect surface: drop active sockets (optionally with Retry-After), stall
 * heartbeat ACKs (wedged-zombie shape), force RECONNECT ops, expire sessions
 * (RESUME → INVALID_SESSION ⇒ fresh IDENTIFY ladder), and script per-session
 * resume refusal.
 */
export class DiscordGatewayFake implements GatewayConnectionFactory {
	private readonly connections: ServerSideConn[] = [];
	/** Ring of dispatched frames (bounded) — the SEQ-keyed replay buffer. */
	private readonly replayBuffer: GatewayFrame[] = [];
	private readonly sessions = new Map<string, ServerSession>();
	private seqCounter = 0;
	private msgCounter = 0;
	private nowFn: NowFn = () => Date.now();

	acceptNext = true;
	heartbeatIntervalMs = HEARTBEAT_INTERVAL_MS_DEFAULT;
	/** Bot identity echoed in READY. */
	readonly botUserId: string;

	/** Frames RECEIVED from clients (identify/resume/heartbeat audit). */
	readonly receivedFrames: Array<{ frame: GatewayFrame; at: number }> = [];

	constructor(
		opts: { botUserId?: string; nowMs?: NowFn; replayWindowSize?: number } = {},
	) {
		this.botUserId = opts.botUserId ?? "bot-self";
		if (opts.nowMs) this.nowFn = opts.nowMs;
		this.replayWindowSize = opts.replayWindowSize ?? 512;
	}

	readonly replayWindowSize: number;

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === 1).length;
	}
	get identifyCount(): number {
		return this.identifyTotal;
	}
	private identifyTotal = 0;
	get resumeCount(): number {
		return this.resumeTotal;
	}
	private resumeTotal = 0;
	get invalidSessionCount(): number {
		return this.invalidTotal;
	}
	private invalidTotal = 0;

	// ── scenario controls ─────────────────────────────────────────────────

	/**
	 * Push a MESSAGE_CREATE dispatch. Delivered immediately to identified
	 * live connections; ALWAYS appended to the seq replay buffer.
	 */
	pushMessage(
		body: Omit<MessageCreateBody, never> & { id?: string },
	): GatewayFrame {
		this.msgCounter += 1;
		const id = body.id ?? `m${this.msgCounter}`;
		return this.dispatch("MESSAGE_CREATE", {
			id,
			channel_id: body.channelId,
			...(body.guildId !== undefined ? { guild_id: body.guildId } : {}),
			author: {
				id: body.authorId,
				...(body.authorBot === true ? { bot: true } : {}),
			},
			content: body.content,
			type: body.messageType ?? 0,
			...(body.referencedMessageId !== undefined
				? { referenced_message: { id: body.referencedMessageId } }
				: {}),
			...(body.isThread === true
				? { thread_id: body.threadId ?? body.channelId }
				: {}),
			mentions: (body.mentionIds ?? []).map((id2) => ({ id: id2 })),
		});
	}

	dispatch(t: string, d: unknown): GatewayFrame {
		this.seqCounter += 1;
		const frame: GatewayFrame = { op: OP.DISPATCH, t, s: this.seqCounter, d };
		this.replayBuffer.push(frame);
		while (this.replayBuffer.length > this.replayWindowSize)
			this.replayBuffer.shift();
		for (const conn of this.connections) {
			if (conn.socket.readyState === 1 && conn.identified)
				conn.socket.serverDeliver(frame);
		}
		return frame;
	}

	/** Drop every open socket (transport outage / server restart). */
	dropActive(
		opts: { retryAfterSeconds?: number; reason?: string; code?: number } = {},
	): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({
				code: opts.code ?? 1006,
				reason: opts.reason ?? "connection lost",
				...(opts.retryAfterSeconds !== undefined
					? { retryAfterSeconds: opts.retryAfterSeconds }
					: {}),
			});
		}
	}

	/** Wedged-zombie shape: connection OPEN but heartbeat ACKs stop. */
	stallHeartbeatAcks(): void {
		for (const conn of this.connections) conn.stalledAcks = true;
	}

	resumeHeartbeatAcks(): void {
		for (const conn of this.connections) conn.stalledAcks = false;
	}

	/** Push an unsolicited RECONNECT op (server-requested reconnection). */
	forceReconnect(): void {
		for (const conn of [...this.connections]) {
			if (conn.socket.readyState === 1)
				conn.socket.serverDeliver({
					op: OP.RECONNECT,
					t: null,
					s: null,
					d: null,
				});
		}
	}

	/** Kill ALL sessions — next RESUME draws INVALID_SESSION(d:false). */
	expireSessions(): void {
		for (const s of this.sessions.values()) s.alive = false;
	}

	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}

	// ── GatewayConnectionFactory ──────────────────────────────────────────

	connect(listener: GatewaySocketListener): GatewayClientSocket {
		const socket = new GatewayClientSocketImpl(listener, this);
		queueMicrotask(() => {
			if (!this.acceptNext) {
				socket.serverRefuse();
				return;
			}
			socket.serverAccept();
		});
		return socket;
	}

	internalHandleClientFrame(conn: ServerSideConn, frame: GatewayFrame): void {
		this.receivedFrames.push({ frame, at: this.nowFn() });
		switch (frame.op) {
			case OP.IDENTIFY: {
				this.identifyTotal += 1;
				conn.identified = true;
				sessionCounter += 1;
				const sessionId = `sess-${sessionCounter}`;
				this.sessions.set(sessionId, { sessionId, alive: true });
				conn.sessionId = sessionId;
				// READY carries a sequence and opens the session's stream.
				const ready = this.dispatch("READY", {
					session_id: sessionId,
					user: { id: this.botUserId },
					_session_start: true,
				});
				markFreshSession(ready);
				conn.socket.serverDeliver(ready);
				break;
			}
			case OP.RESUME: {
				this.resumeTotal += 1;
				const d = (frame.d ?? {}) as {
					session_id?: string;
					seq?: number;
				};
				const session = this.sessions.get(d.session_id ?? "");
				if (session === undefined || !session.alive) {
					this.invalidTotal += 1;
					conn.socket.serverDeliver({
						op: OP.INVALID_SESSION,
						t: null,
						s: null,
						d: false,
					});
					break;
				}
				// THE DELTA: replay every buffered dispatch with s > resume seq.
				const resumeSeq = typeof d.seq === "number" ? d.seq : -1;
				for (const buffered of this.replayBuffer) {
					if ((buffered.s ?? -1) <= resumeSeq) continue;
					if (isFreshSessionStart(buffered)) continue;
					conn.socket.serverDeliver(buffered);
				}
				conn.identified = true;
				conn.sessionId = session.sessionId;
				const resumed = this.dispatch("RESUMED", { _replay_done: true });
				markReplayBoundary(resumed);
				conn.socket.serverDeliver(resumed);
				break;
			}
			case OP.HEARTBEAT: {
				if (!conn.stalledAcks) {
					conn.socket.serverDeliver({
						op: OP.HEARTBEAT_ACK,
						t: null,
						s: null,
						d: null,
					});
				}
				break;
			}
			default:
				break;
		}
	}

	internalRegister(conn: ServerSideConn): void {
		this.connections.push(conn);
	}
	internalRemove(conn: ServerSideConn): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}
}

/** Fresh-session marker: READY frames reset the consumer's seq baseline. */
const FRESH_SESSION = Symbol("gateway.freshSession");
/** Replay-boundary marker: RESUMED ends a replay window. */
const REPLAY_BOUNDARY = Symbol("gateway.replayBoundary");

function markFreshSession(frame: GatewayFrame): void {
	(frame as GatewayFrame & { [FRESH_SESSION]?: boolean })[FRESH_SESSION] = true;
}
function markReplayBoundary(frame: GatewayFrame): void {
	(frame as GatewayFrame & { [REPLAY_BOUNDARY]?: boolean })[REPLAY_BOUNDARY] =
		true;
}
function isFreshSessionStart(frame: GatewayFrame): boolean {
	return (
		(frame as GatewayFrame & { [FRESH_SESSION]?: boolean })[FRESH_SESSION] ===
		true
	);
}

/** True when a dispatch frame marks the end of a RESUME replay window. */
export function isReplayBoundary(frame: GatewayFrame): boolean {
	return (
		(frame as GatewayFrame & { [REPLAY_BOUNDARY]?: boolean })[
			REPLAY_BOUNDARY
		] === true
	);
}

/** True when a MESSAGE_CREATE body decodes as a fresh-session READY marker. */
export function isReadyDispatch(frame: GatewayFrame): boolean {
	return frame.t === "READY";
}

class GatewayClientSocketImpl implements GatewayClientSocket {
	private state: 0 | 1 | 3 = 0;
	private conn: ServerSideConn | null = null;
	listener: GatewaySocketListener;
	private readonly serverRef: DiscordGatewayFake;

	constructor(listener: GatewaySocketListener, server: DiscordGatewayFake) {
		this.listener = listener;
		this.serverRef = server;
	}

	get readyState(): 0 | 1 | 3 {
		return this.state;
	}

	get serverConnection(): ServerSideConn | null {
		return this.conn;
	}

	send(frame: GatewayFrame): void {
		if (this.state !== 1)
			throw new Error(`send on non-open gateway socket (state=${this.state})`);
		this.serverRef.internalHandleClientFrame(
			this.conn as ServerSideConn,
			frame,
		);
	}

	close(code = 1000, reason = "client closing"): void {
		if (this.state !== 1 && this.state !== 0) return;
		this.detach();
		this.state = 3;
		this.listener.onClose({ code, reason });
	}

	serverAccept(): void {
		if (this.state !== 0) return;
		this.conn = {
			socket: this,
			helloSent: false,
			identified: false,
			sessionId: null,
			stalledAcks: false,
		};
		this.serverRef.internalRegister(this.conn);
		this.state = 1;
		this.listener.onOpen();
		// HELLO opens every session: heartbeat_interval + trace.
		this.listener.onFrame({
			op: OP.HELLO,
			t: null,
			s: null,
			d: {
				heartbeat_interval: this.serverRef.heartbeatIntervalMs,
				_trace: ["pi-gateway-fake"],
			},
		});
		// A listener callback above may have torn the connection down
		// synchronously (disconnect during connect — e.g. test cleanup racing
		// the queued accept microtask); detach() nulled this.conn. Only mark
		// helloSent when the session actually survived the handshake.
		if (this.conn !== null) this.conn.helloSent = true;
	}

	serverRefuse(): void {
		if (this.state !== 0) return;
		this.state = 3;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "connection refused" });
	}

	serverDeliver(frame: GatewayFrame): void {
		if (this.state !== 1) return;
		this.listener.onFrame(frame);
	}

	serverClose(info: GatewayCloseInfo): void {
		if (this.state !== 1 && this.state !== 0) return;
		this.detach();
		this.state = 3;
		this.listener.onClose(info);
	}

	private detach(): void {
		if (this.conn) {
			this.serverRef.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
