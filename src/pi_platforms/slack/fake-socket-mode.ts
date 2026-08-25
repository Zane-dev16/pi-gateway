// pi_platforms/slack/fake-socket-mode — the Socket-Mode-SHAPED wire over the
// in-process fake ws plane (04 §8 headless rule; roadmap Phase-6 heuristic 2:
// ports INHERIT the reference transport family — only SHAPE DELTAS are
// written fresh). This is THE shape delta for Slack, written fresh against
// the same observable surface as the reference FakeWsServer (drop/stall/
// refuse controls, replay ring, ping/pong tracking) so the inherited engine,
// fixtures, and rows read it without adaptation.
//
// Socket-Mode framing → reference frame vocabulary (the engine's phase
// machine, watchdog, cursor and dedup are UNCHANGED):
//
//   Socket Mode                       this fake server
//   ───────────────────────────────   ─────────────────────────────────────
//   wss handshake completes           client socket OPEN
//   `hello` event on connect          frame {type:"hello"}
//   app-level subscription            client sends {type:"subscribe",cursor}
//                                     (engine parity; the cursor IS the durable
//                                     ack/replay point — see below)
//   session live                      reply {type:"subscribed",resumeFrom}
//   events_api envelope               frame {type:"event", event:{
//     envelope_id, retry_attempt,       …platform fields…, envelopeId,
//     retry_reason, payload.event}      retryAttempt, retryReason?, ts,
//                                       threadTs?, subtype?, botId?, teamId?}
//   envelope ack (EVERY envelope,     client sends {type:"ack",envelope_id}
//     within 3s, on receipt)            recorded in getFramesReceived() —
//                                       adapter.py parity: slack_bolt acks each
//                                       envelope independent of processing
//   reconnect redelivery              resubscribe replays everything AFTER
//                                     the cursor PLUS flagged un-acked ids;
//                                     replayed envelopes carry
//                                     retry_attempt ≥ 1 / retry_reason
//   interactive payload (block_actions etc.)
//                                     frame {type:"interactive", action}
//                                     (routed by the adapter's ONE
//                                     interactivity handler — slack_bolt
//                                     in-process dispatch parity)
//   disconnect w/ Retry-After         close payload retryAfterSeconds
//
// Hermes anchors (READ-ONLY reference):
//   plugins/platforms/slack/adapter.py:connect — AsyncSocketModeHandler over
//     SLACK_APP_TOKEN (xapp-, connections:write); duplicate-handler teardown
//     so reconnect never double-delivers (#18187 class)
//   plugins/platforms/slack/adapter.py:_slack_dedup_ttl_seconds — "Slack
//     buffers un-acked Socket Mode events and replays them when the websocket
//     reconnects… TTL must outlast Slack's worst-case reconnect-redelivery
//     gap" (#4777)
//   plugins/platforms/slack/adapter.py:_socket_watchdog_loop /
//     _socket_ping_pong_stale — wedged-zombie detection (is_connected lies)

import type { NowFn } from "../persistent-ws/manual-clock.js";
import type {
	WsClientSocket,
	WsCloseInfo,
	WsConnectionFactory,
	WsFrame,
	WsPlatformEvent,
	WsSocketListener,
} from "../persistent-ws/fake-ws.js";

export const WS_CONNECTING = 0 as const;
export const WS_OPEN = 1 as const;
export const WS_CLOSED = 3 as const;

/** Nested edited-message ref inside a message_changed envelope (:5773). */
export interface SlackChangedMessageRef {
	/** The ORIGINAL message's ts (the edit target). */
	ts: string;
	user?: string | undefined;
	/** The NEW text after the edit. */
	text?: string | undefined;
	threadTs?: string | undefined;
	botId?: string | undefined;
	/** Slack stamps edits with {edited:{user,ts}} — ts feeds dedup ladder. */
	edited?: { user?: string | undefined; ts?: string | undefined } | undefined;
}

/** Slack message-event fields riding ON the platform event (shape delta). */
export interface SlackEventFields {
	envelopeId: string;
	/** Prior delivery attempts of THIS envelope (0 = first delivery). */
	retryAttempt: number;
	retryReason?: string | undefined;
	ts?: string | undefined;
	threadTs?: string | undefined;
	subtype?: string | undefined;
	botId?: string | undefined;
	teamId?: string | undefined;
	/** Outer events_api `event_ts` (dedup-ladder head for edits). */
	eventTs?: string | undefined;
	/** message_changed payload — the nested EDITED message (:5773). */
	message?: SlackChangedMessageRef | undefined;
}

/** A delivered/replayed event = platform event + socket-mode envelope. */
export type SlackEnvelopeEvent = WsPlatformEvent & SlackEventFields;

/** Server-side test control for pushing a Slack message event. */
export interface SlackPushOptions {
	subtype?: string | undefined;
	botId?: string | undefined;
	teamId?: string | undefined;
}

/** Interactive payload shape (block_actions family + generic containers). */
export interface SlackInteractivePayload {
	type: "block_actions" | "slash_command" | "view_submission";
	team?: { id?: string } | undefined;
	user?: { id?: string; name?: string } | undefined;
	channel?: { id?: string } | undefined;
	message?: { ts?: string; blocks?: unknown[] } | undefined;
	actions?:
		| Array<{
				action_id: string;
				value?: string;
				block_id?: string;
		  }>
		| undefined;
}

interface BufferedEnvelope {
	seq: number;
	evt: SlackEnvelopeEvent;
	deliveredCount: number;
	/** Flagged for redelivery even if ≤ cursor (un-acked overlap, #4777). */
	flaggedForRedelivery: boolean;
}

interface ServerConnection {
	socket: SlackClientSocket;
	cursor: string | null;
	subscribed: boolean;
	stalled: boolean;
}

/**
 * THE Socket-Mode-shaped fake platform endpoint. Scenario controls mirror the
 * reference fixture 1:1 (acceptNext / dropActive{retryAfterSeconds} /
 * stallPongs / refuseConnections / receivedFrames / openConnectionCount).
 */
export class SlackSocketModeServer implements WsConnectionFactory {
	private readonly connections: ServerConnection[] = [];
	private readonly buffer: BufferedEnvelope[] = [];
	private seqCounter = 0;
	private envelopeCounter = 0;

	acceptNext = true;

	readonly replayWindowSize: number;
	private readonly nowMs: NowFn;

	constructor(opts: { nowMs?: NowFn; replayWindowSize?: number } = {}) {
		this.nowMs = opts.nowMs ?? (() => Date.now());
		this.replayWindowSize = opts.replayWindowSize ?? 256;
	}

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === WS_OPEN)
			.length;
	}

	/** Frames RECEIVED from clients (subscribe/ping/interactivity audit). */
	getFramesReceived(): Array<{ frame: WsFrame; at: number }> {
		return this._received;
	}
	private readonly _received: Array<{ frame: WsFrame; at: number }> = [];

	// ── scenario controls ─────────────────────────────────────────────────

	/**
	 * Push a Slack message event. Delivered to every live subscription
	 * immediately; otherwise held in the ring for post-reconnect replay.
	 */
	pushMessage(
		evt: {
			channel: string;
			user: string;
			text: string;
			ts?: string | undefined;
			thread_ts?: string | undefined;
		},
		opts: SlackPushOptions = {},
	): SlackEnvelopeEvent {
		return this.enqueue({
			type: "message",
			chatId: evt.channel,
			userId: evt.user,
			text: evt.text,
			ts:
				evt.ts ??
				`${1_700_000_000}.${String(this.seqCounter).padStart(6, "0")}`,
			threadTs: evt.thread_ts,
			subtype: opts.subtype,
			botId: opts.botId,
			teamId: opts.teamId ?? "W0",
		});
	}

	/**
	 * Push a message_changed envelope (adapter.py:_handle_slack_message :5773
	 * shape): subtype=message_changed, outer ts/event_ts, nested `message`
	 * carrying the ORIGINAL ts plus the NEW text under `edited`. Rides the
	 * standard buffer/replay machinery (ids eN; acks/replay identical).
	 */
	pushMessageChanged(
		evt: {
			channel: string;
			/** Edited message author (nested message.user). */
			user?: string | undefined;
			/** The NEW text after the edit. */
			text: string;
			/** The ORIGINAL message's ts — the edit target. */
			originalTs: string;
			/** edited.ts stamp on the nested message. */
			editedTs?: string | undefined;
			/** Outer event_ts — dedup-ladder head when present. */
			eventTs?: string | undefined;
			thread_ts?: string | undefined;
		},
		opts: SlackPushOptions = {},
	): SlackEnvelopeEvent {
		return this.enqueue({
			type: "message",
			chatId: evt.channel,
			userId: evt.user ?? "",
			// The OUTER envelope carries no text of its own — the payload lives
			// in the nested message ref (adapter normalizes :5803-5812).
			text: "",
			ts:
				evt.eventTs ??
				`${1_700_000_000}.${String(this.seqCounter).padStart(6, "0")}`,
			eventTs: evt.eventTs,
			threadTs: evt.thread_ts,
			subtype: "message_changed",
			botId: opts.botId,
			teamId: opts.teamId ?? "W0",
			message: {
				ts: evt.originalTs,
				...(evt.user !== undefined ? { user: evt.user } : {}),
				text: evt.text,
				...(evt.thread_ts !== undefined ? { threadTs: evt.thread_ts } : {}),
				...(opts.botId !== undefined ? { botId: opts.botId } : {}),
				edited: {
					...(evt.user !== undefined ? { user: evt.user } : {}),
					...(evt.editedTs !== undefined ? { ts: evt.editedTs } : {}),
				},
			},
		});
	}

	/**
	 * Flag an already-buffered id for REDelivery on the next subscribe EVEN IF
	 * at/below the resume cursor (#4777: Slack replays un-acked events whose
	 * original delivery raced a disconnect). Redelivered envelopes carry an
	 * incremented retry_attempt.
	 */
	markUnackedForRedelivery(id: string): boolean {
		const found = [...this.buffer].reverse().find((b) => b.evt.id === id);
		if (found === undefined) return false;
		found.flaggedForRedelivery = true;
		return true;
	}

	/** Push an interactivity payload to every live connection. */
	pushInteractive(payload: SlackInteractivePayload): void {
		for (const conn of this.connections) {
			if (conn.socket.readyState === WS_OPEN && conn.subscribed) {
				conn.socket.serverFrame({ type: "interactive", action: payload });
			}
		}
	}

	/** Drop every open socket; Retry-After rides the close payload. */
	dropActive(opts: { retryAfterSeconds?: number; reason?: string } = {}): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({
				code: 1013,
				reason: opts.reason ?? "try-again-later",
				retryAfterSeconds: opts.retryAfterSeconds,
			});
		}
	}

	/** Wedged-zombie shape: OPEN socket whose pings stop being answered. */
	stallPongs(): void {
		for (const conn of this.connections) conn.stalled = true;
	}

	refuseConnections(): void {
		this.acceptNext = false;
	}
	acceptConnections(): void {
		this.acceptNext = true;
	}

	// ── internals ──────────────────────────────────────────────────────────

	private enqueue(evtFields: {
		type: "message";
		chatId: string;
		userId: string;
		text: string;
		ts: string;
		threadTs?: string | undefined;
		subtype?: string | undefined;
		botId?: string | undefined;
		teamId?: string | undefined;
		eventTs?: string | undefined;
		message?: SlackChangedMessageRef | undefined;
	}): SlackEnvelopeEvent {
		this.seqCounter += 1;
		const evt: SlackEnvelopeEvent = {
			id: `e${this.seqCounter}`,
			...evtFields,
			envelopeId: `emv${++this.envelopeCounter}`,
			retryAttempt: 0,
		};
		const entry: BufferedEnvelope = {
			seq: this.seqCounter,
			evt,
			deliveredCount: 0,
			flaggedForRedelivery: false,
		};
		this.buffer.push(entry);
		while (this.buffer.length > this.replayWindowSize) this.buffer.shift();
		for (const conn of this.connections) {
			if (conn.socket.readyState === WS_OPEN && conn.subscribed)
				this.deliverFresh(conn, entry);
		}
		return evt;
	}

	private deliverFresh(conn: ServerConnection, entry: BufferedEnvelope): void {
		const prior = entry.deliveredCount;
		entry.deliveredCount += 1;
		if (prior === 0) {
			conn.socket.serverDeliver(entry.evt);
			return;
		}
		conn.socket.serverDeliver({
			...entry.evt,
			retryAttempt: prior,
			retryReason: "retry_timeout",
		});
	}

	/**
	 * Resubscribe replay: everything strictly AFTER the resume cursor plus any
	# flagged un-acked overlap ids; replayed envelopes carry retry_attempt ≥ 1.
	 */
	private replayAfterSubscribe(conn: ServerConnection): void {
		const cursorIdx =
			conn.cursor === null
				? -1
				: this.buffer.findIndex((b) => b.evt.id === conn.cursor);
		const toReplay: BufferedEnvelope[] = [];
		for (let i = cursorIdx + 1; i < this.buffer.length; i++) {
			const entry = this.buffer[i];
			if (entry) toReplay.push(entry);
		}
		for (const entry of this.buffer) {
			if (entry.flaggedForRedelivery && !toReplay.includes(entry)) {
				toReplay.unshift(entry);
				entry.flaggedForRedelivery = false; // one-shot redelivery
			}
		}
		toReplay.sort((a, b) => a.seq - b.seq);
		for (const entry of toReplay) {
			const prior = entry.deliveredCount;
			entry.deliveredCount += 1;
			if (prior === 0) {
				// Buffered during the outage, never delivered before — a FRESH
				// first delivery (no retry flags).
				conn.socket.serverDeliver(entry.evt);
				continue;
			}
			// True redelivery (#4777): incremented attempt + reason.
			conn.socket.serverDeliver({
				...entry.evt,
				retryAttempt: prior,
				retryReason: "retry_timeout",
			});
		}
	}

	internalHandleClientFrame(conn: ServerConnection, frame: WsFrame): void {
		this._received.push({ frame, at: this.nowMs() });
		const type = frame["type"];
		if (type === "subscribe") {
			const cursorRaw = frame["cursor"];
			conn.subscribed = true;
			conn.cursor = typeof cursorRaw === "string" ? cursorRaw : null;
			// Engine parity reply FIRST (session goes live), then the replay
			// window drains — exactly the reference fixture's ordering.
			conn.socket.serverFrame({
				type: "subscribed",
				resumeFrom: conn.cursor,
			});
			this.replayAfterSubscribe(conn);
			return;
		}
		if (type === "ping") {
			if (!conn.stalled) conn.socket.serverFrame({ type: "pong" });
			return;
		}
		// Envelope ACKs ({type:"ack",envelope_id}) are recorded for audit; the
		// durable ack point remains the engine's resume cursor.
	}

	internalRegister(conn: ServerConnection): void {
		this.connections.push(conn);
	}
	internalRemove(conn: ServerConnection): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}

	// ── WsConnectionFactory ───────────────────────────────────────────────

	connect(listener: WsSocketListener): WsClientSocket {
		const socket = new SlackClientSocket(listener, this, this.nowMs);
		if (!this.acceptNext) {
			queueMicrotask(() => socket.serverRefuse());
			return socket;
		}
		queueMicrotask(() => {
			if (!this.acceptNext) return;
			socket.serverAccept();
		});
		return socket;
	}
}

/** Client end handed to the adapter. */
class SlackClientSocket implements WsClientSocket {
	private state: 0 | 1 | 3 = WS_CONNECTING;
	private conn: ServerConnection | null = null;

	listener: WsSocketListener;
	lastPongAt: number | null = null;
	lastPingSentAt: number | null = null;

	constructor(
		listener: WsSocketListener,
		private readonly server: SlackSocketModeServer,
		private readonly nowFn: NowFn,
	) {
		this.listener = listener;
	}

	get readyState(): 0 | 1 | 3 {
		return this.state;
	}

	send(frame: WsFrame): void {
		if (this.state !== WS_OPEN) {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.server.internalHandleClientFrame(this.conn as ServerConnection, frame);
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

	serverAccept(): void {
		if (this.state !== WS_CONNECTING) return;
		this.conn = {
			socket: this,
			cursor: null,
			subscribed: false,
			stalled: false,
		};
		this.server.internalRegister(this.conn);
		this.state = WS_OPEN;
		this.listener.onOpen();
		// SOCKET-MODE SHAPE DELTA: the `hello` event arrives immediately after
		// the wss handshake, before ANY client frame.
		this.serverFrame({ type: "hello" });
	}

	serverRefuse(): void {
		if (this.state !== WS_CONNECTING) return;
		this.state = WS_CLOSED;
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "connection refused" });
	}

	serverDeliver(evt: SlackEnvelopeEvent): void {
		if (this.state !== WS_OPEN) return;
		// events_api envelope mapped onto the reference event frame; envelope
		// fields ride the event object (see module header mapping table).
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
			this.server.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
