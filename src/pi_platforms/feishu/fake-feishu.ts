// pi_platforms/feishu/fake-feishu — the IN-PROCESS family FAKE server with
// vendor wire shapes (04 §8 headless rule; roadmap §Phase 6 heuristic 2 —
// the persistent-ws FakeWsServer machinery, re-shaped for Feishu's
// long-connection protocol).
//
// Vendor shapes modeled (READ-ONLY Hermes ground truth):
//   - Long-conn event frames: `{type: "event", header: {event_id, event_type},
//     event: {...}}` carrying p2_im_message_receive_v1 / p2_card_action_trigger /
//     customized drive.notice.comment_add_v1 / vc.bot.meeting_invited_v1.
//   - At-least-once redelivery: events pushed while NO live subscription sit in
//     a replay window and are REDELIVERED on the next accept (the SDK client's
//     unacked-redelivery shape) — exactly-once downstream is the ADAPTER's
//     persisted dedup store's job (adapter.py:_is_duplicate).
//   - REST plane: im.v1 message create/reply/update + drive comment endpoints,
//     scriptable FIFO behaviors with full capture (FakePlatformWire parity).

import type { NowFn } from "../persistent-ws/manual-clock.js";

export const WS_CONNECTING = 0 as const;
export const WS_OPEN = 1 as const;
export const WS_CLOSED = 3 as const;
export type WsReadyState = 0 | 1 | 3;

/** Any JSON payload on the socket (feishu p2 envelope or control frame). */
export type FeishuFrame = Record<string, unknown>;

export interface FeishuCloseInfo {
	code: number;
	retryAfterSeconds?: number | undefined;
}

export interface FeishuSocketListener {
	onOpen(): void;
	onFrame(frame: FeishuFrame): void;
	onClose(info: FeishuCloseInfo): void;
	onError(err: Error): void;
}

export interface FeishuClientSocket {
	readonly readyState: WsReadyState;
	send(frame: FeishuFrame): void;
	ping(): void;
	close(code?: number): void;
}

export interface FeishuConnectionFactory {
	connect(listener: FeishuSocketListener): FeishuClientSocket;
}

// ── vendor event payloads ────────────────────────────────────────────────

export interface FeishuMention {
	key: string;
	openId: string;
	userId?: string | undefined;
	name: string;
}

/** im.message.receive_v1 `event` body (fields the adapter actually parses). */
export interface FeishuImMessageBody {
	message_id: string;
	message_type: "text" | "post" | "image" | "file" | "audio" | "interactive";
	content: string; // JSON STRING per vendor wire
	chat_type: "p2p" | "group";
	chat_id: string;
	mentions?: FeishuMention[] | undefined;
	parent_id?: string | undefined;
	thread_id?: string | undefined;
}

export interface FeishuSender {
	sender_type: "user" | "bot" | "app";
	sender_id: { open_id?: string; user_id?: string; union_id?: string };
}

export interface FeishuEventEnvelope {
	header: { event_id: string; event_type: string };
	event: Record<string, unknown>;
}

export function imMessageEnvelope(
	body: FeishuImMessageBody,
	sender: FeishuSender,
	eventId?: string,
): FeishuEventEnvelope {
	return {
		header: {
			event_id: eventId ?? body.message_id,
			event_type: "im.message.receive_v1",
		},
		event: { message: body, sender },
	};
}

/** p2_card_action_trigger envelope (A12 card ingress). */
export function cardActionEnvelope(opts: {
	token: string;
	actionValue: Record<string, unknown>;
	actionTag?: string | undefined;
	openChatId: string;
	operatorOpenId: string;
	context?: Record<string, unknown> | undefined;
}): FeishuEventEnvelope {
	return {
		header: { event_id: opts.token, event_type: "card.action.trigger" },
		event: {
			token: opts.token,
			action: { tag: opts.actionTag ?? "button", value: opts.actionValue },
			context: { open_chat_id: opts.openChatId, ...(opts.context ?? {}) },
			operator: {
				operator_type: "user",
				operator_id: { open_id: opts.operatorOpenId },
			},
		},
	};
}

/** customized drive.notice.comment_add_v1 envelope (A12 Drive comments). */
export function driveCommentEnvelope(body: {
	event_id: string;
	comment_id: string;
	reply_id?: string | undefined;
	is_mentioned?: boolean | undefined;
	file_token: string;
	file_type: string;
	notice_type?: string | undefined;
	from_open_id: string;
	to_open_id: string;
}): FeishuEventEnvelope {
	return {
		header: {
			event_id: body.event_id,
			event_type: "drive.notice.comment_add_v1",
		},
		event: {
			event_id: body.event_id,
			comment_id: body.comment_id,
			reply_id: body.reply_id ?? "",
			is_mentioned: body.is_mentioned ?? true,
			timestamp: String(Date.now()),
			notice_meta: {
				file_token: body.file_token,
				file_type: body.file_type,
				notice_type: body.notice_type ?? "add_comment",
				from_user_id: { open_id: body.from_open_id },
				to_user_id: { open_id: body.to_open_id },
			},
		},
	};
}

/** im.message.reaction.{created,deleted}_v1 envelope (reaction ingress). */
export function reactionEnvelope(opts: {
	eventId: string;
	eventType:
		| "im.message.reaction.created_v1"
		| "im.message.reaction.deleted_v1";
	messageId: string;
	operatorType?: string | undefined;
	operatorOpenId?: string | undefined;
	userOpenId?: string | undefined;
	emojiType: string;
}): FeishuEventEnvelope {
	return {
		header: {
			event_id: opts.eventId,
			event_type: opts.eventType,
		},
		event: {
			message_id: opts.messageId,
			operator_type: opts.operatorType ?? "user",
			operator_id: { open_id: opts.operatorOpenId ?? "ou_reactor" },
			user_id: { open_id: opts.userOpenId ?? "ou_reactor" },
			reaction_type: { emoji_type: opts.emojiType },
		},
	};
}

/** customized vc.bot.meeting_invited_v1 envelope (A12 VC invites). */
export function meetingInvitedEnvelope(body: {
	eventId?: string | undefined;
	meeting: {
		id: string;
		topic?: string | undefined;
		meeting_no: string;
		start_time?: number | undefined;
		end_time?: number | undefined;
		host_open_id?: string | undefined;
		host_name?: string | undefined;
	};
	inviter: { open_id: string; user_name?: string | undefined };
	inviteTimeS?: number | undefined;
}): FeishuEventEnvelope {
	return {
		header: {
			event_id: body.eventId ?? "",
			event_type: "vc.bot.meeting_invited_v1",
		},
		event: {
			meeting: {
				id: body.meeting.id,
				topic: body.meeting.topic ?? "",
				meeting_no: body.meeting.meeting_no,
				start_time: body.meeting.start_time ?? 0,
				end_time: body.meeting.end_time ?? 0,
				host_user: {
					open_id: body.meeting.host_open_id ?? "",
					user_name: body.meeting.host_name ?? "",
				},
			},
			inviter: {
				open_id: body.inviter.open_id,
				user_name: body.inviter.user_name ?? "",
			},
			invite_time: body.inviteTimeS ?? 0,
		},
	};
}

// ── scripted REST plane ──────────────────────────────────────────────────

export type RestBehavior =
	| { kind: "ok"; body?: Record<string, unknown> }
	| { kind: "fail"; code: number; msg: string }
	| { kind: "timeout" };

export interface RestCallRecord {
	endpoint: string;
	method: "create" | "reply" | "update" | string;
	payload: Record<string, unknown>;
	seq: number;
	/** Extracted convenience fields (msg_type/post rows/text). */
	msgType?: string | undefined;
	textContent?: string | undefined;
}

interface ScriptedEndpoint {
	queue: RestBehavior[];
	calls: RestCallRecord[];
}

/**
 * The fake platform: WebSocket long-conn endpoint + REST face. Scenario
 * controls mirror FakeWsServer (dropActive w/ Retry-After close payloads,
 * stallPongs, refuseConnections) plus feishu-specific event pushes and the
 * scriptable drive-comment API.
 */
export class FakeFeishuServer implements FeishuConnectionFactory {
	private readonly connections: ServerConn[] = [];
	private readonly replayWindow: FeishuEventEnvelope[] = [];
	private nowMs: NowFn = () => Date.now();

	acceptNext = true;
	stalled = false;

	/** Scripted behaviors per REST endpoint key ("messages", "replies", …). */
	private readonly endpoints = new Map<string, ScriptedEndpoint>();
	/** Scripted GET-message bodies for reaction routing
	 * (adapter.py:_build_get_message_request plane). */
	readonly scriptedMessages = new Map<
		string,
		{ senderId: string; chatId: string; chatType: string }
	>();
	/** Scripted message-resource bodies keyed by file_key
	 * (GET im/v1/messages/:id/resources?type=image|file :4001). The filename
	 * rides the vendor response header/file_name field. */
	readonly scriptedResources = new Map<
		string,
		{ bytes: Uint8Array; contentType: string; filename: string }
	>();
	/** Scripted contact/v3/users/:id display names (:4205). */
	readonly scriptedUserNames = new Map<string, string>();
	/** Scripted bot/v3/bots/basic_batch names (:4257). */
	readonly scriptedBotNames = new Map<string, string>();
	/** Scripted im/v1/chats/:chat_id metadata (:2424). */
	readonly scriptedChats = new Map<
		string,
		{ name: string; chatType: string }
	>();
	/** Every REST transmission, in order. */
	readonly restCalls: RestCallRecord[] = [];
	private seqCounter = 0;

	constructor(opts: { nowMs?: NowFn; replayWindowSize?: number } = {}) {
		if (opts.nowMs) this.nowMs = opts.nowMs;
		this.replayWindowSize = opts.replayWindowSize ?? 256;
	}

	readonly replayWindowSize: number;

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === WS_OPEN)
			.length;
	}

	/** Frames the server RECEIVED from clients (ping audit). */
	readonly receivedFrames: Array<{ frame: FeishuFrame; at: number }> = [];

	// ── scenario controls ───────────────────────────────────────────────

	script(endpoint: string, ...behaviors: RestBehavior[]): void {
		const q = this.endpoints.get(endpoint)?.queue ?? [];
		q.push(...behaviors);
		this.endpoints.set(endpoint, { queue: q, calls: [] });
	}

	private nextBehavior(endpoint: string): RestBehavior {
		const ep = this.endpoints.get(endpoint);
		if (ep === undefined || ep.queue.length === 0) return { kind: "ok" };
		return ep.queue.shift() as RestBehavior;
	}

	recordRest(
		endpoint: string,
		method: RestCallRecord["method"],
		payload: Record<string, unknown>,
	): RestBehavior {
		this.seqCounter += 1;
		const record: RestCallRecord = {
			endpoint,
			method,
			payload,
			seq: this.seqCounter,
		};
		const msgType = payload["msg_type"];
		if (typeof msgType === "string") record.msgType = msgType;
		// Vendor create-message wire: content is ALWAYS a JSON STRING
		// (json.dumps payload, :4655/:4657). text lane carries {"text": …}.
		if (msgType === "text" && typeof payload["content"] === "string") {
			try {
				const parsed = JSON.parse(payload["content"]) as Record<
					string,
					unknown
				>;
				record.textContent = String(parsed["text"] ?? "");
			} catch {
				record.textContent = String(payload["content"]);
			}
		}
		this.restCalls.push(record);
		return this.nextBehavior(endpoint);
	}

	/**
	 * Push a p2 event envelope. Live+subscribed sockets get it NOW; otherwise
	 * it waits in the replay window for post-reconnect redelivery.
	 */
	pushEvent(envelope: FeishuEventEnvelope): FeishuEventEnvelope {
		this.replayWindow.push(envelope);
		while (this.replayWindow.length > this.replayWindowSize)
			this.replayWindow.shift();
		for (const conn of this.connections) {
			if (conn.socket.readyState === WS_OPEN && conn.subscribed)
				conn.socket.serverDeliver(envelope);
		}
		return envelope;
	}

	/** Redeliver EVERYTHING still in the window on the next accept (SDK shape). */
	markAllForRedelivery(): void {}

	dropActive(opts: { retryAfterSeconds?: number } = {}): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({
				code: 1013,
				retryAfterSeconds: opts.retryAfterSeconds,
			});
		}
	}

	stallPongs(): void {
		this.stalled = true;
	}

	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}

	clearReplayWindow(): void {
		this.replayWindow.length = 0;
	}

	// ── connection factory ───────────────────────────────────────────────

	connect(listener: FeishuSocketListener): FeishuClientSocket {
		const socket = new FeishuClientSocketImpl(listener, this);
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

	internalHandleClientFrame(conn: ServerConn, frame: FeishuFrame): void {
		this.receivedFrames.push({ frame, at: this.nowMs() });
		const type = frame["type"];
		if (
			type === "ping" &&
			!this.stalled &&
			conn.socket.readyState === WS_OPEN
		) {
			(conn.socket as FeishuClientSocketImpl).serverFrame({ type: "pong" });
		}
	}

	/** Events currently held for redelivery (test observability). */
	get replayWindowSnapshot(): readonly FeishuEventEnvelope[] {
		return this.replayWindow;
	}
}

interface ServerConn {
	socket: FeishuClientSocketImpl;
	subscribed: boolean;
}

class FeishuClientSocketImpl implements FeishuClientSocket {
	private state: WsReadyState = WS_CONNECTING;
	private conn: ServerConn | null = null;
	listener: FeishuSocketListener;
	lastPingSentAt: number | null = null;
	lastPongAt: number | null = null;

	private readonly serverRef: FakeFeishuServer;

	constructor(listener: FeishuSocketListener, server: FakeFeishuServer) {
		this.listener = listener;
		this.serverRef = server;
	}

	get readyState(): WsReadyState {
		return this.state;
	}

	get serverConnection(): ServerConn | null {
		return this.conn;
	}

	send(frame: FeishuFrame): void {
		if (this.state !== WS_OPEN)
			throw new Error(`send on non-open socket (state=${this.state})`);
		if ((frame["type"] as string) === "subscribe") {
			if (this.conn) this.conn.subscribed = true;
		}
		this.serverRef.internalHandleClientFrame(this.conn as ServerConn, frame);
	}

	ping(): void {
		if (this.state !== WS_OPEN) return;
		this.lastPingSentAt = Date.now();
		this.send({ type: "ping", ts: this.lastPingSentAt });
	}

	close(code = 1000): void {
		if (this.state !== WS_OPEN && this.state !== WS_CONNECTING) return;
		this.detach();
		this.state = WS_CLOSED;
		this.listener.onClose({ code });
	}

	serverAccept(): void {
		if (this.state !== WS_CONNECTING) return;
		this.conn = { socket: this, subscribed: false };
		this.serverRef.internalRegister(this.conn);
		this.state = WS_OPEN;
		this.listener.onOpen();
		// SDK redelivery shape: everything still unacked in the window arrives
		// immediately after the (re)connect, BEFORE new pushes.
		for (const evt of [...this.serverRef.replayWindowSnapshot]) {
			if (this.state !== WS_OPEN) return;
			this.listener.onFrame({
				type: "event",
				header: evt.header,
				event: evt.event,
			});
		}
	}

	serverRefuse(): void {
		if (this.state !== WS_CONNECTING) return;
		this.state = WS_CLOSED;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006 });
	}

	serverDeliver(evt: FeishuEventEnvelope): void {
		if (this.state !== WS_OPEN) return;
		this.listener.onFrame({
			type: "event",
			header: evt.header,
			event: evt.event,
		});
	}

	serverFrame(frame: FeishuFrame): void {
		if (this.state !== WS_OPEN) return;
		if (frame["type"] === "pong") this.lastPongAt = Date.now();
		this.listener.onFrame(frame);
	}

	serverClose(info: FeishuCloseInfo): void {
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
