// pi_platforms/mattermost/mm-fake-server — the IN-PHASE fake Mattermost
// server (04 §8 headless rule). Two planes:
//
//   - WebSocket event stream: client sends {seq, action:"authentication_challenge",
//     data:{token}}; the server answers {"status":"OK","seq_reply":N} and then
//     pushes {event:"posted", data:{channel_type, post}} envelopes. Protocol
//     level ping/pong with pong staleness (aiohttp heartbeat=30 parity),
//     scriptable drops carrying Retry-After, refusal, wedged pongs.
//   - REST v4: users/me, channels/{id}, posts (create), posts/{id}/patch,
//     posts/{id} lookup, channels/{id}/posts?since= (the reconnect BACKFILL
//     window — Mattermost's documented replay mechanism), typing capture,
//     429 Retry-After scripting.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/mattermost/adapter.py:_ws_loop / _ws_connect_and_listen
//     (reconnect ladder constants, auth-fatal escalation OOF-156)
//   plugins/platforms/mattermost/adapter.py:_handle_ws_event (posted pipeline)

import type { NowFn } from "../persistent-ws/manual-clock.js";

export const WS_CONNECTING = 0 as const;
export const WS_OPEN = 1 as const;
export const WS_CLOSED = 3 as const;

export interface MmPost {
	id: string;
	channel_id: string;
	user_id: string;
	message: string;
	/** "" for user posts; truthy for system posts (filtered upstream). */
	type: string;
	root_id: string;
	create_at: number;
}

/** One JSON frame on the MM websocket. */
export type MmFrame = Record<string, unknown>;

export interface MmCloseInfo {
	code: number;
	reason: string;
	/** Server-authoritative Retry-After carried in the close payload. */
	retryAfterSeconds?: number | undefined;
}

export interface MmSocketListener {
	onOpen(): void;
	onFrame(frame: MmFrame): void;
	onClose(info: MmCloseInfo): void;
	onError(err: Error): void;
}

export interface MmClientSocket {
	readonly readyState: number;
	send(frame: MmFrame): void;
	ping(): void;
	close(code?: number, reason?: string): void;
	/** Heartbeat staleness inputs (watchdog reads these). */
	lastPingSentAt: number | null;
	lastPongAt: number | null;
	markAlive(): void;
}

export class MmRestError extends Error {
	constructor(
		readonly status: number,
		message: string,
		readonly retryAfterSeconds?: number | undefined,
	) {
		super(message);
		this.name = "MmRestError";
	}
}

interface ServerSideConnection {
	socket: MmClientSocketImpl;
	authed: boolean;
	stalled: boolean;
	channelTypeByChannel: Map<string, string>;
}

let serverSeq = 0;

export function makeMmPostFrame(post: MmPost, channelType: string): MmFrame {
	return {
		event: "posted",
		data: { channel_type: channelType, post: JSON.stringify(post) },
	};
}

export class FakeMattermost {
	readonly id = ++serverSeq;

	acceptNext = true;
	private readonly connections: ServerSideConnection[] = [];
	nowMs: NowFn = () => Date.now();

	// ── identity ────────────────────────────────────────────────────────────
	botUserId = "bot-user-1";
	botUsername = "pi_gateway_bot";
	private authToken = "mm-fake-token";
	private authRequired = true;

	// ── scenario knobs ──────────────────────────────────────────────────────
	private wedged = false;

	// ── stores ──────────────────────────────────────────────────────────────
	private readonly posts = new Map<string, MmPost>();
	private lastCreateAt = 0;
	private postSeq = 0;
	private readonly channels = new Map<
		string,
		{ type: string; display_name: string; name: string }
	>();
	private readonly knownChannels = new Set<string>();
	private readonly frameLog: Array<{ frame: MmFrame; at: number }> = [];
	private readonly createdPosts: MmPost[] = [];
	private readonly patchedPosts: Array<{ id: string; message: string }> = [];
	private readonly typingCalls: Array<{ channelId: string; userId: string }> =
		[];
	private readonly restRateLimits: number[] = [];
	handshakeRateLimitSeconds: number | null = null;
	authChallengeFailures = 0;

	constructor(opts: { nowMs?: NowFn } = {}) {
		this.nowMs = opts.nowMs ?? (() => Date.now());
	}

	// ── fixture setup ───────────────────────────────────────────────────────

	addChannel(channelId: string, type = "O", displayName = channelId): void {
		this.channels.set(channelId, {
			type,
			display_name: displayName,
			name: channelId,
		});
		this.knownChannels.add(channelId);
	}
	setAuthToken(token: string): void {
		this.authToken = token;
	}
	setAuthRequired(required: boolean): void {
		this.authRequired = required;
	}
	setWedged(wedged: boolean): void {
		this.wedged = wedged;
	}
	scriptRestRateLimitOnce(retryAfterSeconds: number): void {
		this.restRateLimits.push(retryAfterSeconds);
	}

	// ── observability ───────────────────────────────────────────────────────

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === WS_OPEN)
			.length;
	}
	get receivedFrames(): Array<{ frame: MmFrame; at: number }> {
		return this.frameLog;
	}
	get createdPostCount(): number {
		return this.createdPosts.length;
	}
	get patchedCount(): number {
		return this.patchedPosts.length;
	}
	get typingEventCount(): number {
		return this.typingCalls.length;
	}
	postById(id: string): MmPost | undefined {
		return this.posts.get(id);
	}
	createdPostsFor(channelId: string): MmPost[] {
		return this.createdPosts.filter((p) => p.channel_id === channelId);
	}
	patchMessagesFor(postId: string): string[] {
		return this.patchedPosts
			.filter((p) => p.id === postId)
			.map((p) => p.message);
	}

	// ── scenario controls ───────────────────────────────────────────────────

	dropActive(opts: { retryAfterSeconds?: number; reason?: string } = {}): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({
				code: 1013,
				reason: opts.reason ?? "try-again-later",
				retryAfterSeconds: opts.retryAfterSeconds,
			});
		}
	}
	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}
	stallPongs(): void {
		for (const conn of this.connections) conn.stalled = true;
	}
	/** Fail the NEXT authentication challenges (auth-fatal scenario). */
	failNextChallenges(count: number): void {
		this.authChallengeFailures = count;
	}

	// ── event injection ─────────────────────────────────────────────────────

	pushPost(
		channelId: string,
		userId: string,
		message: string,
		opts: {
			type?: string | undefined;
			rootId?: string | undefined;
			createAt?: number | undefined;
			postId?: string | undefined;
		} = {},
	): MmPost {
		const channelType = this.channels.get(channelId)?.type ?? "O";
		this.postSeq += 1;
		const post: MmPost = {
			id: opts.postId ?? `post${this.postSeq}`,
			channel_id: channelId,
			user_id: userId,
			message,
			type: opts.type ?? "",
			root_id: opts.rootId ?? "",
			// Strictly increasing create_at even under a frozen injected clock —
			// the REST backfill window (since <) depends on it.
			create_at:
				opts.createAt ??
				(() => {
					const next = Math.max(this.nowMs(), this.lastCreateAt + 1);
					this.lastCreateAt = next;
					return next;
				})(),
		};
		this.posts.set(post.id, post);
		this.knownChannels.add(channelId);
		const frame = makeMmPostFrame(post, channelType);
		if (!this.wedged) {
			for (const conn of this.connections) {
				if (conn.socket.readyState === WS_OPEN && conn.authed)
					conn.socket.serverDeliver(frame);
			}
		}
		return post;
	}

	// ── WsConnectionFactory ─────────────────────────────────────────────────

	connect(listener: MmSocketListener): MmClientSocket {
		const socket = new MmClientSocketImpl(listener, { server: this });
		if (!this.acceptNext || this.handshakeRateLimitSeconds !== null) {
			const ra = this.handshakeRateLimitSeconds;
			queueMicrotask(() => {
				socket.serverRefuse(ra);
			});
			this.handshakeRateLimitSeconds = null;
			return socket;
		}
		queueMicrotask(() => socket.serverAccept());
		return socket;
	}

	internalHandleClientFrame(conn: ServerSideConnection, frame: MmFrame): void {
		this.frameLog.push({ frame, at: this.nowMs() });
		const action = frame["action"];
		if (action === "authentication_challenge") {
			const token = (frame["data"] as Record<string, unknown> | undefined)?.[
				"token"
			];
			if (
				!this.authRequired ||
				(token === this.authToken && this.authChallengeFailures === 0)
			) {
				conn.socket.serverFrame({ status: "OK", seq_reply: frame["seq"] });
				conn.authed = true;
				for (const [id, ch] of this.channels) {
					conn.channelTypeByChannel.set(id, ch.type);
				}
			} else if (this.authChallengeFailures > 0) {
				this.authChallengeFailures -= 1;
				conn.socket.serverClose({
					code: 4001,
					reason: "authentication failed",
				});
			} else {
				conn.socket.serverClose({
					code: 4001,
					reason: "authentication failed",
				});
			}
			return;
		}
		if (action === "ping") {
			if (!conn.stalled)
				conn.socket.serverFrame({ action: "pong", seq_reply: frame["seq"] });
		}
	}

	internalRegister(conn: ServerSideConnection): void {
		this.connections.push(conn);
	}
	internalRemove(conn: ServerSideConnection): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}
	internalDeliver(conn: ServerSideConnection, frame: MmFrame): void {
		conn.socket.serverDeliver(frame);
	}

	// ── REST v4 surface (engine calls; errors throw MmRestError) ───────────

	async restGetMe(token: string): Promise<{ id: string; username: string }> {
		if (token !== this.authToken) throw new MmRestError(401, "Unauthorized");
		return { id: this.botUserId, username: this.botUsername };
	}

	async restGetChannel(channelId: string): Promise<{
		type: string;
		display_name: string;
		name: string;
	}> {
		const ch = this.channels.get(channelId);
		if (!ch) throw new MmRestError(404, "Channel not found");
		return { ...ch };
	}

	async restCreatePost(payload: {
		channel_id: string;
		message: string;
		root_id?: string | undefined;
		props?: Record<string, unknown> | undefined;
	}): Promise<MmPost> {
		const limited = this.restRateLimits.shift();
		if (limited !== undefined) {
			throw new MmRestError(429, "rate limit exceeded", limited);
		}
		this.postSeq += 1;
		const post: MmPost = {
			id: `made${this.postSeq}`,
			channel_id: payload.channel_id,
			user_id: this.botUserId,
			message: payload.message,
			type: "",
			root_id: payload.root_id ?? "",
			create_at: this.nowMs(),
		};
		this.posts.set(post.id, post);
		this.createdPosts.push(post);
		this.knownChannels.add(payload.channel_id);
		return post;
	}

	async restPatchPost(postId: string, message: string): Promise<MmPost> {
		const limited = this.restRateLimits.shift();
		if (limited !== undefined) {
			throw new MmRestError(429, "rate limit exceeded", limited);
		}
		const existing = this.posts.get(postId);
		this.patchedPosts.push({ id: postId, message });
		if (existing) existing.message = message;
		return (
			existing ?? {
				id: postId,
				channel_id: "unknown",
				user_id: this.botUserId,
				message,
				type: "",
				root_id: "",
				create_at: this.nowMs(),
			}
		);
	}

	async restGetPost(postId: string): Promise<MmPost> {
		const post = this.posts.get(postId);
		if (!post) throw new MmRestError(404, "Post not found");
		return post;
	}

	/**
	 * GET /channels/{id}/posts?since= — posts created strictly after the
	 * timestamp (the reconnect BACKFILL window; Mattermost's documented
	 * disconnect-recovery mechanism).
	 */
	async restGetPostsSince(
		channelId: string,
		sinceMs: number,
	): Promise<MmPost[]> {
		const out: MmPost[] = [];
		for (const post of this.posts.values()) {
			if (post.channel_id !== channelId) continue;
			if (post.create_at > sinceMs) out.push(post);
		}
		out.sort((a, b) => a.create_at - b.create_at);
		return out;
	}

	async restTyping(channelId: string, userId: string): Promise<void> {
		const limited = this.restRateLimits.shift();
		if (limited !== undefined) {
			throw new MmRestError(429, "rate limit exceeded", limited);
		}
		this.typingCalls.push({ channelId, userId });
	}
}

/** Client-end socket handed to the adapter. */
class MmClientSocketImpl implements MmClientSocket {
	private state: number = WS_CONNECTING;
	private conn: ServerSideConnection | null = null;
	listener: MmSocketListener;

	lastPingSentAt: number | null = null;
	lastPongAt: number | null = null;

	private readonly serverRef: FakeMattermost;

	constructor(listener: MmSocketListener, opts: { server: FakeMattermost }) {
		this.listener = listener;
		this.serverRef = opts.server;
	}

	get readyState(): number {
		return this.state;
	}

	get serverConnection(): ServerSideConnection | null {
		return this.conn;
	}

	send(frame: MmFrame): void {
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
		this.lastPingSentAt = this.serverRef.nowMs();
		this.send({ action: "ping", seq: this.nextSeq() });
	}

	close(code = 1000, reason = "client closing"): void {
		if (this.state !== WS_OPEN && this.state !== WS_CONNECTING) return;
		this.detach();
		this.state = WS_CLOSED;
		this.listener.onClose({ code, reason });
	}

	private seqCounter = 0;
	private nextSeq(): number {
		this.seqCounter += 1;
		return this.seqCounter;
	}

	serverAccept(): void {
		if (this.state !== WS_CONNECTING) return;
		this.conn = {
			socket: this,
			authed: false,
			stalled: false,
			channelTypeByChannel: new Map(),
		};
		this.serverRef.internalRegister(this.conn);
		this.state = WS_OPEN;
		this.listener.onOpen();
	}

	serverRefuse(retryAfterSeconds?: number | null): void {
		if (this.state !== WS_CONNECTING) return;
		this.state = WS_CLOSED;
		this.listener.onError(
			new MmRestError(
				429,
				"handshake rate limited",
				retryAfterSeconds ?? undefined,
			),
		);
		this.listener.onClose({
			code: 1006,
			reason: "connection refused",
			retryAfterSeconds: retryAfterSeconds ?? undefined,
		});
	}

	serverDeliver(frame: MmFrame): void {
		if (this.state !== WS_OPEN) return;
		this.listener.onFrame(frame);
	}

	serverFrame(frame: MmFrame): void {
		if (this.state !== WS_OPEN) return;
		if (frame["action"] === "pong") this.lastPongAt = this.serverRef.nowMs();
		this.listener.onFrame(frame);
	}

	markAlive(): void {
		this.lastPongAt = this.serverRef.nowMs();
	}

	serverClose(info: MmCloseInfo): void {
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
