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
	/** Attached file ids (api/v4/files upload → posts payload). */
	file_ids?: string[];
	/** Post props (disable_mentions etc. persist through PATCH). */
	props?: Record<string, unknown> | undefined;
}

/** files/{id}/info shape (_upload_file/download parity). */
export interface MmFileInfo {
	id: string;
	name: string;
	mime_type: string;
	size: number;
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
	/** FULL PATCH payloads received on posts/{id}/patch (props audit). */
	readonly patchPayloads: Array<{
		postId: string;
		message: string;
		props?: Record<string, unknown> | undefined;
	}> = [];
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
	typingCallsFor(
		channelId: string,
	): Array<{ channelId: string; userId: string }> {
		return this.typingCalls.filter((c) => c.channelId === channelId);
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
			fileIds?: string[] | undefined;
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
			...(opts.fileIds !== undefined ? { file_ids: [...opts.fileIds] } : {}),
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
			const ok =
				!this.authRequired ||
				(token === this.authToken && this.authChallengeFailures === 0);
			if (ok) {
				conn.socket.serverFrame({ status: "OK", seq_reply: frame["seq"] });
				conn.authed = true;
				for (const [id, ch] of this.channels) {
					conn.channelTypeByChannel.set(id, ch.type);
				}
			} else if (this.challengeReplyFailMode) {
				// REAL server behavior: a rejected challenge is answered in-band
				// with {"status":"fail","error":{...}} — no close at all.
				this.authChallengeFailures -= Math.min(1, this.authChallengeFailures);
				conn.socket.serverFrame({
					status: "FAIL",
					seq_reply: frame["seq"],
					error: { id: "api.context.invalid_token" },
				});
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

	/**
	 * Script IN-BAND challenge rejections: the next N challenges get the real
	 * server's {"status":"fail"} reply instead of a 4001 close.
	 */
	challengeReplyFail(n: number): void {
		this.challengeReplyFailMode = true;
		this.authChallengeFailures = n;
	}
	private challengeReplyFailMode = false;

	/** SERVER-initiated keepalive ping (real MM servers ping every ~30s). */
	serverPing(): void {
		serverSeq += 1;
		for (const conn of this.connections) {
			if (conn.socket.readyState === WS_OPEN && conn.stalled !== true) {
				conn.socket.serverDeliver({ action: "ping", seq: serverSeq });
			}
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
		file_ids?: string[] | undefined;
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
			...(payload.file_ids !== undefined
				? { file_ids: [...payload.file_ids] }
				: {}),
		};
		this.createdPostsPayloads.push({
			channel_id: payload.channel_id,
			message: payload.message,
			...(payload.root_id !== undefined ? { root_id: payload.root_id } : {}),
			...(payload.props !== undefined ? { props: { ...payload.props } } : {}),
			...(payload.file_ids !== undefined
				? { file_ids: [...payload.file_ids] }
				: {}),
		});
		this.posts.set(post.id, post);
		this.createdPosts.push(post);
		this.knownChannels.add(payload.channel_id);
		return post;
	}

	/** Full payloads received on POST posts (props/file_ids audit). */
	readonly createdPostsPayloads: Array<{
		channel_id: string;
		message: string;
		root_id?: string | undefined;
		props?: Record<string, unknown> | undefined;
		file_ids?: string[] | undefined;
	}> = [];

	/**
	 * POST api/v4/files — multipart channel_id+'files' → file_infos[0].id
	 * (adapter.py:_upload_file).
	 */
	readonly uploadedFiles: Array<{
		channelId: string;
		filename: string;
		contentType: string;
		bytes: Uint8Array;
	}> = [];
	private fileSeq = 0;

	async restUploadFile(op: {
		channelId: string;
		filename: string;
		contentType: string;
		bytes: Uint8Array;
	}): Promise<string> {
		const limited = this.restRateLimits.shift();
		if (limited !== undefined) {
			throw new MmRestError(429, "rate limit exceeded", limited);
		}
		this.uploadedFiles.push({
			channelId: op.channelId,
			filename: op.filename,
			contentType: op.contentType,
			bytes: op.bytes,
		});
		this.fileSeq += 1;
		const fid = `fid${this.fileSeq}`;
		this.fileInfos.set(fid, {
			id: fid,
			name: op.filename,
			mime_type: op.contentType,
			size: op.bytes.byteLength,
		});
		this.fileContents.set(fid, op.bytes.slice());
		return fid;
	}

	private readonly fileInfos = new Map<string, MmFileInfo>();
	private readonly fileContents = new Map<string, Uint8Array>();

	/** GET files/{fid}/info. */
	async restGetFileInfo(fileId: string): Promise<MmFileInfo> {
		const info = this.fileInfos.get(fileId);
		if (info === undefined) throw new MmRestError(404, "File not found");
		return { ...info };
	}

	/** GET files/{fid} — the AUTHED download lane. */
	async restDownloadFile(fileId: string): Promise<Uint8Array> {
		const bytes = this.fileContents.get(fileId);
		if (bytes === undefined) throw new MmRestError(404, "File not found");
		return bytes.slice();
	}

	async restPatchPost(
		postId: string,
		payload: { message: string; props?: Record<string, unknown> | undefined },
	): Promise<MmPost> {
		const limited = this.restRateLimits.shift();
		if (limited !== undefined) {
			throw new MmRestError(429, "rate limit exceeded", limited);
		}
		const message = payload.message;
		const existing = this.posts.get(postId);
		this.patchedPosts.push({ id: postId, message });
		this.patchPayloads.push({
			postId,
			message,
			...(payload.props !== undefined ? { props: { ...payload.props } } : {}),
		});
		if (existing) {
			existing.message = message;
			if (payload.props !== undefined) {
				existing.props = { ...(existing.props ?? {}), ...payload.props };
			}
		}
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
