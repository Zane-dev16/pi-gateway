// pi_platforms/simplex/simplex-wire — the simplex-chat daemon transport SEAM
// plus the in-process fake daemon (04 §8: rows run headless against fake
// platform servers; NO external network, NO sockets).
//
// Production binds this seam to ONE persistent WebSocket against the local
// simplex-chat daemon (`simplex-chat -p 5225`; adapter.py module docstring):
//
//   → {"corrId": "...", "cmd": "/_send @<id> json [...]"|"/accept <id>"|
//      "/freceive <id>"}            (outbound JSON text frames)
//   ← {"corrId": "...", "resp": {"type": "newChatItems"|"contactRequest"|
//      "rcvFileDescrReady"|"rcvFileComplete"|..., ...}}
//     (inbound events pushed over the SAME socket; older daemon builds may
//      put fields top-level — the dispatcher normalizes both forms)
//
// The FAKE models those wire shapes in-process: scriptable accept/refuse,
// drop controls, recorded commands, scripted correlated responders, and a
// BACKLOG QUEUE flushed BEFORE new events after reconnect. Backlog semantics
// are FIXTURE SEMANTICS modeled on the Signal precedent (FakeSignalCliServer
// backlog): "the daemon held what we could not consume" — simplex has NO
// cursor/replay protocol on this wire, so reconnect loss-free coverage is
// modeled exactly like signal-cli coverage was (documented leg mapping).

/** WebSocket readyState parity subset used by the engine. */
export type SimplexReadyState = 0 | 1 | 2 | 3;

export const SX_CONNECTING = 0 as const;
export const SX_OPEN = 1 as const;
export const SX_CLOSING = 2 as const;
export const SX_CLOSED = 3 as const;

export interface SimplexCloseInfo {
	code: number;
	reason: string;
}

/** The client end of one daemon connection. */
export interface SimplexSocketListener {
	onOpen(): void;
	/** One inbound JSON text frame from the daemon. */
	onText(text: string): void;
	onClose(info: SimplexCloseInfo): void;
	onError(err: Error): void;
	/**
	 * Protocol PONG received — the answer to a client keepalive ping
	 * (adapter.py:_ws_listener ping_interval/ping_timeout parity; the port
	 * expresses the keepalive itself since pi embeds no websockets library).
	 */
	onPong?(): void;
}

export interface SimplexConnection {
	readonly readyState: SimplexReadyState;
	/** Raw JSON text frame write. Throws when the socket is not open. */
	send(text: string): void;
	close(code?: number, reason?: string): void;
	/**
	 * Protocol-level WS ping frame (keepalive carrier). OPTIONAL: factories
	 * without transport-level ping support simply omit it, and the adapter's
	 * keepalive loop skips them. Throws when the socket is not open.
	 */
	ping?(): void;
}

/**
 * THE transport seam. The adapter NEVER imports net/ws client libraries —
 * production and tests supply different implementations.
 */
export interface SimplexConnectionFactory {
	connect(listener: SimplexSocketListener): SimplexConnection;
}

/** One command frame RECEIVED from the adapter (capture aid). */
export interface DaemonCommandRecord {
	corrId: string | null;
	cmd: string;
	atMs: number;
}

interface OutboundFrame {
	text: string;
}

/**
 * In-memory simplex-chat daemon double speaking the protocol. Behaviors are
 * scriptable per test: accept/refuse connections, drop the live socket,
 * record every client command, answer scripted command prefixes (plus the
 * BUILT-IN auto-responder for `/accept`, whose correlated reply the adapter
 * awaits), and buffer events emitted while disconnected — flushing them
 * BEFORE new events once the next connection opens (backlog-first, Signal
 * precedent mapping; see class docstring above).
 */
export class FakeSimplexDaemon implements SimplexConnectionFactory {
	private readonly connections: DaemonSideConnection[] = [];
	/** Events held while NO consumer holds the connection (backlog-first). */
	private readonly backlog: unknown[] = [];
	/** Ordered server→client delivery queue for the LIVE connection. */
	private readonly outbound: OutboundFrame[] = [];
	private drainScheduled = false;

	private readonly commandScripts = new Map<string, unknown[]>();
	private readonly responses: Array<{ corrId: string | null; resp: unknown }> =
		[];

	readonly commands: DaemonCommandRecord[] = [];
	readonly connectionLog: Array<{ atMs: number }> = [];
	readonly drops: Array<{ atMs: number; reason: string }> = [];
	/** Protocol ping frames RECEIVED from the adapter (keepalive audit). */
	readonly pingFrames: Array<{ atMs: number }> = [];

	/** When false, pings go UNANSWERED (wedged-link shape → ping timeout). */
	answerPongs = true;

	/** When false, connect attempts fail (daemon unreachable → ladder). */
	acceptNext = true;

	// Fixture-shared clock read (client sockets stamp ping/pong receipts).
	clockNow: () => number = () => Date.now();

	/** Wire the daemon's clock (fixtures inject the manual clock). */
	setClock(nowMs: () => number): void {
		this.clockNow = nowMs;
	}

	get hasLiveConnection(): boolean {
		return this.connections.some((c) => c.socket.readyState === SX_OPEN);
	}

	get liveConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === SX_OPEN)
			.length;
	}

	get backlogDepth(): number {
		return this.backlog.length;
	}

	commandsStartingWith(prefix: string): DaemonCommandRecord[] {
		return this.commands.filter((c) => c.cmd.startsWith(prefix));
	}

	hasCommand(cmd: string): boolean {
		return this.commands.some((c) => c.cmd === cmd);
	}

	// ── scripting ────────────────────────────────────────────────────────────

	/**
	 * Program a correlated response body for the NEXT command whose text
	 * starts with `prefix` (FIFO per prefix). The response is delivered as
	 * {"corrId": <same>, "resp": body}. Fixture semantics: real daemon reply
	 * bodies vary by version — rows only need SOME correlated reply.
	 */
	scriptCommandResponse(prefix: string, resp: unknown): void {
		const q = this.commandScripts.get(prefix) ?? [];
		q.push(resp);
		this.commandScripts.set(prefix, q);
	}

	// ── scenario controls ───────────────────────────────────────────────────

	/**
	 * Emit one daemon event. Delivered to the live connection in order;
	 * otherwise held in the backlog for post-reconnect flush (BEFORE any
	 * newer event — the disconnect-window obligation).
	 */
	pushEvent(evt: unknown): void {
		if (this.hasLiveConnection) {
			this.enqueueOutbound(evt);
		} else {
			this.backlog.push(evt);
		}
	}

	/** Tear every open socket (transport outage → ladder). */
	dropActive(reason = "daemon restarted"): void {
		for (const conn of [...this.connections]) {
			this.drops.push({ atMs: this.clockNow(), reason });
			conn.socket.serverClose({ code: 1006, reason });
		}
	}

	/** Make the next connect attempt(s) fail (refused handshake). */
	refuseConnections(): void {
		this.acceptNext = false;
	}

	acceptConnections(): void {
		this.acceptNext = true;
	}

	/** Wedged-zombie shape: socket stays OPEN but pings stop being answered. */
	stallPongs(): void {
		this.answerPongs = false;
	}

	resumePongs(): void {
		this.answerPongs = true;
	}

	/** Test seam: flush nothing — backlog visibility only. */
	clearBacklogForTest(): void {
		this.backlog.length = 0;
	}

	// ── SimplexConnectionFactory ─────────────────────────────────────────────

	connect(listener: SimplexSocketListener): SimplexConnection {
		const socket = new DaemonClientSocket(listener, this);
		if (!this.acceptNext) {
			// Refusal surfaces asynchronously like a real failed handshake — and
			// MUST move the client socket out of CONNECTING (readyState parity).
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

	// ── server internals ─────────────────────────────────────────────────────

	internalRegister(conn: DaemonSideConnection): void {
		this.connections.push(conn);
		this.connectionLog.push({ atMs: this.clockNow() });
	}

	internalRemove(conn: DaemonSideConnection): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}

	internalHandleClientText(conn: DaemonSideConnection, text: string): void {
		let parsed: Record<string, unknown>;
		try {
			parsed = JSON.parse(text) as Record<string, unknown>;
		} catch {
			return; // malformed client frame ignored (daemon parity)
		}
		const corrId =
			typeof parsed["corrId"] === "string"
				? (parsed["corrId"] as string)
				: null;
		const cmd = typeof parsed["cmd"] === "string" ? parsed["cmd"] : "";
		this.commands.push({ corrId, cmd, atMs: this.clockNow() });

		// Correlated reply resolution: scripted prefix first (FIFO), then the
		// BUILT-IN auto-responder for /accept (the adapter AWAITS that reply —
		// contact requests would stall 30s otherwise).
		if (cmd.startsWith("/accept")) {
			this.respond(conn, corrId, {
				type: "contactAccepted",
				command: cmd,
			});
			return;
		}
		for (const [prefix, queue] of this.commandScripts) {
			if (cmd.startsWith(prefix)) {
				const body = queue.shift();
				if (body !== undefined && corrId !== null) {
					this.respond(conn, corrId, body);
				}
				return;
			}
		}
	}

	private respond(
		conn: DaemonSideConnection,
		corrId: string | null,
		resp: unknown,
	): void {
		if (corrId === null) return;
		this.enqueueOutbound({ corrId, resp });
		void conn;
	}

	/**
	 * Protocol keepalive: record the ping and answer with a pong unless
	 * stalled — evaluated AT DELIVERY time so a stall armed between the ping
	 * and its pong deterministically suppresses the answer.
	 */
	internalHandleClientPing(conn: DaemonSideConnection): void {
		this.pingFrames.push({ atMs: this.clockNow() });
		queueMicrotask(() => {
			if (!this.answerPongs) return;
			conn.socket.serverDeliverPong();
		});
	}

	private enqueueOutbound(evt: unknown): void {
		this.outbound.push({ text: JSON.stringify(evt) });
		this.scheduleDrain();
	}

	private scheduleDrain(): void {
		if (this.drainScheduled) return;
		this.drainScheduled = true;
		queueMicrotask(() => {
			this.drainScheduled = false;
			const live = this.connections.find(
				(c) => c.socket.readyState === SX_OPEN,
			);
			if (live === undefined) return;
			while (this.outbound.length > 0) {
				const frame = this.outbound.shift();
				if (frame === undefined) break;
				live.socket.serverDeliverText(frame.text);
			}
		});
	}

	/** Post-connect flush: backlog FIRST, strictly before any newer events. */
	internalFlushBacklogOnAccept(): void {
		while (this.backlog.length > 0) {
			const evt = this.backlog.shift();
			if (evt !== undefined) this.enqueueOutbound(evt);
		}
	}
}

/** Server-side twin for one accepted connection. */
interface DaemonSideConnection {
	socket: DaemonClientSocket;
}

/** Client-end implementation handed to the adapter. */
class DaemonClientSocket implements SimplexConnection {
	private state: SimplexReadyState = SX_CONNECTING;
	private conn: DaemonSideConnection | null = null;

	constructor(
		readonly listener: SimplexSocketListener,
		private readonly server: FakeSimplexDaemon,
	) {}

	get readyState(): SimplexReadyState {
		return this.state;
	}

	send(text: string): void {
		if (this.state !== SX_OPEN) {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.server.internalHandleClientText(
			this.conn as DaemonSideConnection,
			text,
		);
	}

	/** Last keepalive ping sent on THIS socket (client clock; observability). */
	lastPingSentAt: number | null = null;
	/** Last pong received on THIS socket (client clock; observability). */
	lastPongAt: number | null = null;

	ping(): void {
		if (this.state !== SX_OPEN) {
			throw new Error(`ping on non-open socket (state=${this.state})`);
		}
		this.lastPingSentAt = this.server.clockNow();
		this.server.internalHandleClientPing(this.conn as DaemonSideConnection);
	}

	serverDeliverPong(): void {
		if (this.state !== SX_OPEN) return;
		this.lastPongAt = this.server.clockNow();
		this.listener.onPong?.();
	}

	close(code = 1000, reason = "client closing"): void {
		if (this.state !== SX_OPEN && this.state !== SX_CONNECTING) return;
		this.detach();
		this.state = SX_CLOSED;
		this.listener.onClose({ code, reason });
	}

	serverAccept(): void {
		if (this.state !== SX_CONNECTING) return;
		this.conn = { socket: this };
		this.server.internalRegister(this.conn);
		this.state = SX_OPEN;
		this.listener.onOpen();
		// Backlog first — the daemon delivers what we could not consume during
		// the disconnect window, BEFORE anything pushed after reconnection.
		// (internalFlushBacklogOnAccept → enqueueOutbound schedules the drain;
		// a live pushEvent after this enqueues behind the flushed backlog.)
		this.server.internalFlushBacklogOnAccept();
	}

	serverRefuse(): void {
		if (this.state !== SX_CONNECTING) return;
		this.state = SX_CLOSED;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "connection refused" });
	}

	serverDeliverText(text: string): void {
		if (this.state !== SX_OPEN) return;
		this.listener.onText(text);
	}

	serverClose(info: SimplexCloseInfo): void {
		if (this.state !== SX_OPEN && this.state !== SX_CONNECTING) return;
		this.detach();
		this.state = SX_CLOSED;
		this.listener.onClose(info);
	}

	private detach(): void {
		if (this.conn) {
			this.server.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
