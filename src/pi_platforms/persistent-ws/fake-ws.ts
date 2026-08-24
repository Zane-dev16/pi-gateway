// pi_platforms/persistent-ws/fake-ws — the IN-PROCESS fake WebSocket plane
// (04 §8: rows run headless against fake platform servers; NO external
// network; no `ws` package needed). Node ships a global WebSocket CLIENT but
// no server, and real sockets would reintroduce wall-clock flakiness — so the
// event plane is a pure in-memory socket pair with the observable WebSocket
// surface: readyState, send/close/ping, open/message/close events.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_socket_ping_pong_stale  (staleness =
//     no recent ping/pong; is_connected() can LIE while the session retries)
//   slack adapter.py:Socket Mode redelivery after reconnects (#4777) — the
//     server replays buffered events on resubscribe (at-least-once)

import type { NowFn } from "./manual-clock.js";

export const WS_CONNECTING = 0 as const;
export const WS_OPEN = 1 as const;
export const WS_CLOSING = 2 as const;
export const WS_CLOSED = 3 as const;
export type WsReadyState = 0 | 1 | 2 | 3;

/** One JSON frame on the wire. */
export type WsFrame = Record<string, unknown>;

/** Inbound event envelope the fake platform pushes to subscribers. */
export interface WsPlatformEvent {
	/** Monotonic server-assigned id ("e1", "e2", …) — the replay cursor key. */
	id: string;
	type: "message";
	chatId: string;
	userId: string;
	text: string;
}

export interface WsCloseInfo {
	code: number;
	reason: string;
	/**
	 * Server-authoritative Retry-After carried in the CLOSE PAYLOAD (Slack
	 * refresh-guidance parity / §3 ws row: "Retry-After capture"). Seconds.
	 */
	retryAfterSeconds?: number | undefined;
}

export interface WsSocketListener {
	onOpen(): void;
	onFrame(frame: WsFrame): void;
	onClose(info: WsCloseInfo): void;
	onError(err: Error): void;
}

/** The client end of one connection. */
export interface WsClientSocket {
	readonly readyState: WsReadyState;
	send(frame: WsFrame): void;
	/** Heartbeat probe (ws-level ping; answered by a pong unless stalled). */
	ping(): void;
	close(code?: number, reason?: string): void;
}

/** Connection factory the adapter consumes (structural seam — substitutable). */
export interface WsConnectionFactory {
	connect(listener: WsSocketListener): WsClientSocket;
}

interface ServerSideConnection {
	socket: WsClientSocketImpl;
	subscriptionCursor: string | null;
	/** True once the client sent ANY subscribe frame (live stream active). */
	subscribed: boolean;
	stalled: boolean;
}

/**
 * The fake platform's WebSocket endpoint. Behaviors are scriptable per test:
 * accept/refuse, drop active sockets (optionally carrying Retry-After in the
 * close payload), stall pings (wedged-zombie shape), buffer events during
 * disconnect windows and REPLAY them past a resubscribe cursor.
 */
export class FakeWsServer implements WsConnectionFactory {
	private readonly connections: ServerSideConnection[] = [];
	/** Ring of undelivered/redeliverable events (bounded replay window). */
	private readonly replayBuffer: WsPlatformEvent[] = [];
	private eventCounter = 0;
	private nowMs: NowFn = () => Date.now();

	/** When false, connect attempts fail (connection refused → ladder). */
	acceptNext = true;

	constructor(opts: { nowMs?: NowFn; replayWindowSize?: number } = {}) {
		if (opts.nowMs) this.nowMs = opts.nowMs;
		this.replayWindowSize = opts.replayWindowSize ?? 256;
	}

	readonly replayWindowSize: number;

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === WS_OPEN)
			.length;
	}

	// ── server-side scenario controls ─────────────────────────────────────

	/**
	 * Enqueue a platform event. Delivered immediately to every live
	 * subscription; otherwise held in the replay window for the post-
	 * reconnect resubscribe to fetch (the disconnect-window obligation).
	 */
	pushEvent(evt: Omit<WsPlatformEvent, "id">): WsPlatformEvent {
		this.eventCounter += 1;
		const full: WsPlatformEvent = { ...evt, id: `e${this.eventCounter}` };
		this.replayBuffer.push(full);
		while (this.replayBuffer.length > this.replayWindowSize)
			this.replayBuffer.shift();
		for (const conn of this.connections) {
			if (conn.socket.readyState === WS_OPEN && conn.subscribed)
				conn.socket.serverDeliver(full);
		}
		return full;
	}

	/**
	 * Drop every open socket (transport outage). `retryAfterSeconds` rides
	 * the close payload — the adapter must CAPTURE it into its ladder.
	 */
	dropActive(opts: { retryAfterSeconds?: number; reason?: string } = {}): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({
				code: 1013,
				reason: opts.reason ?? "try-again-later",
				retryAfterSeconds: opts.retryAfterSeconds,
			});
		}
	}

	/** Wedged-zombie shape: socket stays OPEN but pings stop being answered. */
	stallPongs(): void {
		for (const conn of this.connections) conn.stalled = true;
	}

	/** Simulate a SECOND consumer stealing the session (refuse re-connects). */
	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}

	/**
	 * Events the server WILL redeliver on the next subscribe with cursor ≤
	 * their id even though they were already delivered once (#4777
	 * at-least-once simulation).
	 */
	markLastEventForRedelivery(): WsPlatformEvent | null {
		return this.replayBuffer[this.replayBuffer.length - 1] ?? null;
	}

	/** Frames the server RECEIVED from clients (subscribe/ping audit). */
	readonly receivedFrames: Array<{ frame: WsFrame; at: number }> = [];

	// ── WsConnectionFactory ───────────────────────────────────────────────

	connect(listener: WsSocketListener): WsClientSocket {
		const socket = new WsClientSocketImpl(listener, {
			server: this,
			nowMs: this.nowMs,
		});
		if (!this.acceptNext) {
			// Refusal surfaces asynchronously like a real failed handshake — and
			// MUST move the client socket out of CONNECTING (readyState parity:
			// callers gate re-connect attempts on CLOSED).
			queueMicrotask(() => {
				socket.serverRefuse();
			});
			return socket;
		}
		queueMicrotask(() => {
			if (!this.acceptNext) return;
			socket.serverAccept();
		});
		return socket;
	}
	internalHandleClientFrame(conn: ServerSideConnection, frame: WsFrame): void {
		this.receivedFrames.push({ frame, at: this.nowMs() });
		const type = frame["type"];
		if (type === "subscribe") {
			const cursorRaw = frame["cursor"];
			conn.subscribed = true; // live stream active REGARDLESS of cursor
			conn.subscriptionCursor =
				typeof cursorRaw === "string" ? cursorRaw : null;
			// Resubscribe replay: everything AFTER the cursor still in the
			// window is pushed again (at-least-once); null cursor = cold boot
			// (fresh subscription, stale backlog intentionally not dropped for
			// ws — the window replays; polling shapes differ, 04 §3).
			const cursorIdx =
				conn.subscriptionCursor === null
					? -1
					: this.replayBuffer.findIndex(
							(e) => e.id === conn.subscriptionCursor,
						);
			for (let i = cursorIdx + 1; i < this.replayBuffer.length; i++) {
				const evt = this.replayBuffer[i];
				if (evt) conn.socket.serverDeliver(evt);
			}
			conn.socket.serverFrame({
				type: "subscribed",
				resumeFrom: conn.subscriptionCursor,
			});
			return;
		}
		if (type === "ping") {
			if (!conn.stalled) conn.socket.serverFrame({ type: "pong" });
			return;
		}
	}

	/** Server internals: registry entry created ONCE by serverAccept. */
	internalRegister(conn: ServerSideConnection): void {
		this.connections.push(conn);
	}

	internalRemove(conn: ServerSideConnection): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}
}

/** Client-end implementation handed to the adapter. */
class WsClientSocketImpl implements WsClientSocket {
	private state: WsReadyState = WS_CONNECTING;
	/** Server-side handle for bidirectional delivery. */
	private conn: ServerSideConnection | null = null;
	listener: WsSocketListener;

	private readonly serverRef: FakeWsServer;
	private readonly nowFn: NowFn;
	/** Last observed pong receipt (client clock) — staleness input. */
	lastPongAt: number | null = null;
	lastPingSentAt: number | null = null;

	constructor(
		listener: WsSocketListener,
		opts: { server: FakeWsServer; nowMs: NowFn },
	) {
		this.listener = listener;
		this.serverRef = opts.server;
		this.nowFn = opts.nowMs;
	}

	get readyState(): WsReadyState {
		return this.state;
	}

	/** Test/server hook: the server-side twin for this socket. */
	get serverConnection(): ServerSideConnection | null {
		return this.conn;
	}

	send(frame: WsFrame): void {
		if (this.state !== WS_OPEN) {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.serverRef.internalHandleClientFrame(
			this.conn as ServerSideConnection,
			frame,
		);
	}

	ping(): void {
		if (this.state !== WS_OPEN) return;
		this.lastPingSentAt = this.nowFn();
		this.send({ type: "ping", ts: this.lastPingSentAt });
	}

	close(code = 1000, reason = "client closing"): void {
		if (this.state !== WS_OPEN && this.state !== WS_CONNECTING) return;
		this.detach();
		this.state = WS_CLOSED;
		this.listener.onClose({ code, reason });
	}

	// ── server→client plumbing (called by the factory/server only) ────────

	serverAccept(): void {
		if (this.state !== WS_CONNECTING) return;
		this.conn = {
			socket: this,
			subscriptionCursor: null,
			subscribed: false,
			stalled: false,
		};
		// ONE server-side state object: the registry entry IS this.conn, so
		// subscribe/cursor/stall mutations are visible to every scenario
		// control (pushEvent/dropActive/stallPongs read the same object).
		this.serverRef.internalRegister(this.conn);
		this.state = WS_OPEN;
		this.listener.onOpen();
	}

	/** Failed-handshake transition: ERROR then CLOSE(1006), terminal state. */
	serverRefuse(): void {
		if (this.state !== WS_CONNECTING) return;
		this.state = WS_CLOSED;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "connection refused" });
	}

	serverDeliver(evt: WsPlatformEvent): void {
		if (this.state !== WS_OPEN) return;
		this.listener.onFrame({ type: "event", event: evt });
	}

	serverFrame(frame: WsFrame): void {
		if (this.state !== WS_OPEN) return;
		if (frame["type"] === "pong") this.lastPongAt = this.nowFn();
		this.listener.onFrame(frame);
	}

	serverClose(info: WsCloseInfo): void {
		if (this.state !== WS_OPEN && this.state !== WS_CONNECTING) return;
		this.detach();
		this.state = WS_CLOSED;
		this.listener.onClose(info);
	}

	private detach(): void {
		if (this.conn) {
			this.serverRef.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
