// pi_platforms/homeassistant/ha-fake-server — the IN-PROCESS fake Home
// Assistant server (04 §8: rows run headless against fake platform servers;
// NO external network, no sockets). Models the observable HA WebSocket API
// surface plus a REST notification recorder:
//
//   - AUTH HANDSHAKE state machine: {"type":"auth_required"} first → client
//     {"type":"auth","access_token":…} → configurable verdict ("auth_ok" vs
//     "auth_invalid") → client subscribe_events → configurable result ack.
//     adapter.py:_ws_connect walks this ladder and refuses on ANY deviation.
//   - EVENT PUBLISHER: pushEvent builds the state_changed envelope
//     {type:"event",event:{data:{entity_id,old_state,new_state}}} exactly as
//     HA's event bus delivers it.
//   - PING/PONG: the real HA API answers {id,type:"ping"} with
//     {id,type:"pong"}; stall control models a wedged link for the watchdog
//     row (aiohttp heartbeat=30 staleness parity).
//   - BACKLOG QUEUE (signal-precedent mapping, proposed DEC): HA redelivers
//     NOTHING across reconnects (a plain subscribe_events has no resume
//     cursor) — loss-free ws-row coverage is modeled SERVER-SIDE: events
//     pushed while no subscription is live are held and flushed BEFORE new
//     ones when the client resubscribes. Exactly-once downstream holds.
//   - REST RECORDER: notification POSTs land in restRequests verbatim
//     (path/headers/payload) with scriptable outcomes for failure shapes.

import type { NowFn } from "../persistent-ws/manual-clock.js";

export const HA_WS_CONNECTING = 0 as const;
export const HA_WS_OPEN = 1 as const;
export const HA_WS_CLOSING = 2 as const;
export const HA_WS_CLOSED = 3 as const;
export type HaReadyState = 0 | 1 | 2 | 3;

/** One raw TEXT message on the wire (JSON or garbage; the CLIENT parses). */
export type HaWireText = string;

export interface HaCloseInfo {
	code: number;
	reason: string;
}

export interface HaSocketListener {
	onOpen(): void;
	onText(text: HaWireText): void;
	onClose(info: HaCloseInfo): void;
	onError(err: Error): void;
}

/** The client end of one connection (structural seam — substitutable). */
export interface HaClientSocket {
	readonly readyState: HaReadyState;
	sendText(text: HaWireText): void;
	close(code?: number, reason?: string): void;
}

/** Connection factory the adapter consumes. */
export interface HaConnectionFactory {
	connect(listener: HaSocketListener): HaClientSocket;
}

/** Scriptable REST outcome for notification POSTs. */
export type HaRestScript =
	| { kind: "ok" }
	| { kind: "http"; status: number; body: string }
	| { kind: "timeout" };

export interface HaRestRequestRecord {
	path: string;
	headers: Record<string, string>;
	payload: { title: string; message: string };
}

interface ServerSideConnection {
	socket: HaClientSocketImpl;
	authed: boolean;
	subscribed: boolean;
	stalled: boolean;
}

/**
 * The fake Home Assistant endpoint. Behaviors are scriptable per test:
 * accept/refuse connections, auth verdict, first-frame breakage, subscribe
 * nack, ping stalls, active drops, event backlog across disconnect windows,
 * and REST outcome scripting.
 */
export class FakeHaServer implements HaConnectionFactory {
	private readonly connections: ServerSideConnection[] = [];
	private nowMs: NowFn = () => Date.now();

	/** When false, connect attempts fail (connection refused → ladder). */
	acceptNext = true;
	/** Handshake step 3 verdict: auth_ok vs anything else (auth failed). */
	authVerdict: "ok" | "invalid" = "ok";
	/** Handshake step 1 override: send THIS type instead of auth_required. */
	firstFrameType: string | null = null;
	/** Handshake step 5 verdict: result success true vs false. */
	subscribeAck: "ok" | "fail" = "ok";

	/** Events held while NO live subscription existed (backlog obligation). */
	private readonly backlog: HaWireText[] = [];
	private eventCounter = 0;

	/** Frames the server RECEIVED from clients (auth/subscribe/ping audit). */
	readonly receivedFrames: Array<{
		frame: Record<string, unknown>;
		at: number;
	}> = [];
	/** REST notification POSTs recorded VERBATIM (path/headers/payload). */
	readonly restRequests: HaRestRequestRecord[] = [];
	private restScripts: HaRestScript[] = [];

	constructor(opts: { nowMs?: NowFn } = {}) {
		if (opts.nowMs) this.nowMs = opts.nowMs;
	}

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === HA_WS_OPEN)
			.length;
	}

	get hasLiveSubscription(): boolean {
		return this.connections.some(
			(c) => c.socket.readyState === HA_WS_OPEN && c.subscribed && c.authed,
		);
	}

	get backlogDepth(): number {
		return this.backlog.length;
	}

	// ── server-side scenario controls ─────────────────────────────────────

	/**
	 * Publish a state_changed event. Delivered immediately to the live
	 * subscription; otherwise HELD in the backlog for the post-reconnect
	 * resubscribe to flush BEFORE newer events (the disconnect-window
	 * obligation of the ws replay row).
	 */
	pushEvent(input: {
		entity_id: string;
		old_state?: Record<string, unknown> | null;
		new_state?: Record<string, unknown> | null;
	}): void {
		this.eventCounter += 1;
		const envelope = {
			type: "event",
			id: this.eventCounter,
			event: {
				event_type: "state_changed",
				data: {
					entity_id: input.entity_id,
					old_state: input.old_state ?? null,
					new_state: input.new_state ?? null,
				},
			},
		};
		const text = JSON.stringify(envelope);
		const live = this.connections.find(
			(c) => c.socket.readyState === HA_WS_OPEN && c.subscribed && c.authed,
		);
		if (live !== undefined) {
			live.socket.serverDeliver(text);
			return;
		}
		this.backlog.push(text);
	}

	/** Drop every open socket (transport outage). */
	dropActive(reason = "connection lost"): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({ code: 1006, reason });
		}
	}

	/** Wedged-zombie shape: socket stays OPEN but pongs stop coming. */
	stallPongs(): void {
		for (const conn of this.connections) conn.stalled = true;
	}

	unstallPongs(): void {
		for (const conn of this.connections) conn.stalled = false;
	}

	/** Simulate a SECOND consumer stealing the session (refuse re-connects). */
	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}

	// ── REST plane ────────────────────────────────────────────────────────

	/** Script the next N REST outcomes (FIFO; default ok when drained). */
	scriptRest(...outcomes: HaRestScript[]): void {
		this.restScripts.push(...outcomes);
	}

	pullRestScript(): HaRestScript | undefined {
		return this.restScripts.shift();
	}

	// ── WsConnectionFactory ───────────────────────────────────────────────

	connect(listener: HaSocketListener): HaClientSocket {
		const socket = new HaClientSocketImpl(listener, { server: this });
		if (!this.acceptNext) {
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

	internalHandleClientText(conn: ServerSideConnection, text: HaWireText): void {
		let frame: Record<string, unknown>;
		try {
			const parsed: unknown = JSON.parse(text);
			frame =
				parsed !== null && typeof parsed === "object"
					? (parsed as Record<string, unknown>)
					: {};
		} catch {
			return; // garbage from the client side is ignored by the fake
		}
		this.receivedFrames.push({ frame, at: this.nowMs() });
		const type = frame["type"];
		if (conn.authed) {
			if (type === "subscribe_events") {
				conn.subscribed = true;
				// Ack FIRST (real HA answers the command immediately; events flow
				// once the subscription is active), THEN the backlog — flush-
				// before-new is the documented replay mapping (proposed DEC text
				// in the wiring suite).
				conn.socket.serverFrame(
					JSON.stringify({
						id: frame["id"] ?? null,
						type: "result",
						success: this.subscribeAck === "ok",
						result: null,
					}),
				);
				while (this.backlog.length > 0) {
					const held = this.backlog.shift();
					if (held !== undefined) conn.socket.serverDeliver(held);
				}
				return;
			}
			if (type === "ping") {
				if (!conn.stalled) {
					conn.socket.serverFrame(
						JSON.stringify({ id: frame["id"] ?? null, type: "pong" }),
					);
				}
				return;
			}
			return;
		}
		if (type === "auth") {
			conn.authed = this.authVerdict === "ok";
			conn.socket.serverFrame(
				JSON.stringify(
					conn.authed
						? { type: "auth_ok" }
						: { type: "auth_invalid", message: "Invalid access token" },
				),
			);
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

	/** Handshake opener: auth_required (or the scripted breaker frame). */
	internalOpenGreeting(conn: ServerSideConnection): void {
		conn.socket.serverFrame(
			JSON.stringify({
				type: this.firstFrameType ?? "auth_required",
				ha_version: "2026.1.0",
			}),
		);
	}
}

/** Client-end implementation handed to the adapter. */
class HaClientSocketImpl implements HaClientSocket {
	private state: HaReadyState = HA_WS_CONNECTING;
	private conn: ServerSideConnection | null = null;
	listener: HaSocketListener;

	private readonly serverRef: FakeHaServer;

	constructor(listener: HaSocketListener, opts: { server: FakeHaServer }) {
		this.listener = listener;
		this.serverRef = opts.server;
	}

	get readyState(): HaReadyState {
		return this.state;
	}

	sendText(text: HaWireText): void {
		if (this.state !== HA_WS_OPEN) {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.serverRef.internalHandleClientText(
			this.conn as ServerSideConnection,
			text,
		);
	}

	close(code = 1000, reason = "client closing"): void {
		if (this.state !== HA_WS_OPEN && this.state !== HA_WS_CONNECTING) return;
		this.detach();
		this.state = HA_WS_CLOSED;
		this.listener.onClose({ code, reason });
	}

	// ── server→client plumbing (called by the factory/server only) ────────

	serverAccept(): void {
		if (this.state !== HA_WS_CONNECTING) return;
		this.conn = {
			socket: this,
			authed: false,
			subscribed: false,
			stalled: false,
		};
		this.serverRef.internalRegister(this.conn);
		this.state = HA_WS_OPEN;
		this.listener.onOpen();
		this.serverRef.internalOpenGreeting(this.conn);
	}

	/** Failed-handshake transition: ERROR then CLOSE(1006), terminal state. */
	serverRefuse(): void {
		if (this.state !== HA_WS_CONNECTING) return;
		this.state = HA_WS_CLOSED;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "connection refused" });
	}

	serverDeliver(text: HaWireText): void {
		if (this.state !== HA_WS_OPEN) return;
		this.listener.onText(text);
	}

	serverFrame(text: HaWireText): void {
		if (this.state !== HA_WS_OPEN) return;
		this.listener.onText(text);
	}

	serverClose(info: HaCloseInfo): void {
		if (this.state !== HA_WS_OPEN && this.state !== HA_WS_CONNECTING) return;
		this.detach();
		this.state = HA_WS_CLOSED;
		this.listener.onClose(info);
	}

	private detach(): void {
		if (this.conn) {
			this.serverRef.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
