// pi_platforms/telegram/telegram-fake-server — the IN-PHASE fake TELEGRAM
// Bot API endpoint for the census port (04 §8 headless rule: NO network, NO
// vendor SDK). Transport-family machinery (long-poll offset confirmation,
// ONE live consumer per token / 409 Conflict, drop_pending_updates zombie
// eviction, webhook-info pending counts, reachability pools) mirrors the
// Phase-3 polling reference fake semantics — re-expressed here over REAL
// TELEGRAM WIRE SHAPES (snake_case update objects):
//
//   getUpdates(allowed_updates) → { update_id, message? | edited_message?
//                  | callback_query? | message_reaction? }
//     (real-API allowed_updates FILTERING: kinds outside the requested set
//      are never delivered; an OMITTED argument applies Telegram's DEFAULT
//      filter which EXCLUDES message_reaction/message_reaction_count)
//   deleteWebhook(drop_pending_updates=false)  (cold-boot/best-effort clear)
//   sendMessage / editMessageText / editMessageReplyMarkup   (shaped results)
//   answerCallbackQuery                                       (spinner clear)
//   setMessageReaction                                        (A1 ack surface)
//   sendChatAction(action, message_thread_id)                 (A11 variants)
//   sendPhoto/sendDocument/sendVoice/sendAudio/sendVideo/sendAnimation
//     (the outgoing media family w/ notification+thread kwargs capture)
//
// The ENGINE plane (openSession/getUpdates/commitOffset/getMe/
// getWebhookInfo/sendChatAction) is structurally compatible with the
// polling reference engine's FakeTelegramServer dep, so the inherited
// offset-commit/conflict/heartbeat machinery drives THIS server unmodified.
// Every wire update keeps its RAW payload in a registry keyed by update_id —
// the adapter's kind-routing seam (update-object parsing deltas).

import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import { TELEGRAM_DEFAULT_ALLOWED_UPDATES } from "./manifest.js";

// ── Telegram wire shapes (Bot API ground truth, snake_case) ──────────────────

export interface TgWireUser {
	id: number;
	is_bot: boolean;
	username?: string | undefined;
}

export interface TgWireChat {
	id: number;
	type: "private" | "group" | "supergroup" | "channel";
	title?: string | undefined;
	username?: string | undefined;
	/** Forum-flagged supergroup (adapter.py _ensure_forum_commands gate). */
	is_forum?: boolean | undefined;
}

export interface TgWireSticker {
	file_unique_id: string;
	emoji?: string | undefined;
	set_name?: string | undefined;
	is_animated?: boolean | undefined;
	is_video?: boolean | undefined;
}

export interface TgWireMessage {
	message_id: number | string;
	from?: TgWireUser | undefined;
	chat: TgWireChat;
	date: number;
	text?: string | undefined;
	sticker?: TgWireSticker | undefined;
	/** Media caption (edited_message normalization reads text ?? caption). */
	caption?: string | undefined;
	/** Forum-topic placement markers (_normalize_message_edited_event). */
	message_thread_id?: number | undefined;
	is_topic_message?: boolean | undefined;
	/** Unix-seconds edit timestamp on edited_message updates. */
	edit_date?: number | undefined;
}

export interface TgWireCallbackQuery {
	id: string;
	from: TgWireUser;
	message?: TgWireMessage | undefined;
	data: string;
}

/** Re-exported normalized-reaction input shape (A2). */
export type TgWireMessageReaction =
	import("./reactions.js").WireMessageReaction;

export interface TgWireUpdate {
	update_id: number;
	message?: TgWireMessage | undefined;
	edited_message?: TgWireMessage | undefined;
	/** Real Telegram kind — KNOWN to the wire, UNWIRED by our platform-event
	 * contract (adapter.py:_normalize_platform_event returns None): the
	 * adapter must tolerate it as a no-op, never a turn or an error. */
	channel_post?: TgWireMessage | undefined;
	callback_query?: TgWireCallbackQuery | undefined;
	message_reaction?: TgWireMessageReaction | undefined;
}

/** Engine-plane view handed to the inherited polling loop. */
export interface EngineUpdateView {
	updateId: number;
	chatId: string;
	senderId: string;
	/** Message TEXT for message updates; "" for non-text kinds. */
	text: string;
}

export interface TgSentMessage {
	message_id: number;
	chat: { id: number };
	date: number;
	text: string;
}

// ── behaviors ────────────────────────────────────────────────────────────────

export type TgBehavior =
	| { kind: "ok" }
	| { kind: "flood"; retryAfter: number }
	| { kind: "fail"; error: string };

export class TelegramConflictError extends Error {
	constructor() {
		super(
			"409 Conflict: terminated by other getUpdates request; make sure that only one bot instance is running",
		);
		this.name = "TelegramConflictError";
	}
}

export class TelegramTransportError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramTransportError";
	}
}

/** The Bot API media-send methods the fake models (tg-9 family). */
export type TgMediaMethod =
	| "sendPhoto"
	| "sendDocument"
	| "sendVoice"
	| "sendAudio"
	| "sendVideo"
	| "sendAnimation";

export interface TgMediaOp {
	method: TgMediaMethod;
	/** FULL wire arg set as transmitted (snake_case kwargs). */
	args: Record<string, unknown>;
	seq: number;
}

/** Raw Bot API 10.1 rich endpoints the fake models (tg2-6). */
export type TgRichMethod = "sendRichMessage" | "sendRichMessageDraft";

export interface TgRichOp {
	method: TgRichMethod;
	args: Record<string, unknown>;
	seq: number;
}

/** set_my_commands scope shapes (Bot API ground truth, tg2-3). */
export type TgCommandScope =
	| { type: "default" }
	| { type: "all_private_chats" }
	| { type: "all_group_chats" }
	| { type: "chat"; chat_id: number | string };

export interface TgMyCommandsOp {
	commands: Array<{ command: string; description: string }>;
	scope: TgCommandScope;
	seq: number;
}

interface PollWaiter {
	token: number;
	offset: number;
	allowedUpdates?: readonly string[] | undefined;
	settle: (result: { updates: EngineUpdateView[] }) => void;
	reject: (err: Error) => void;
	cancelTimer: () => void;
	flushTimer?: ReturnType<typeof setTimeout> | undefined;
}

let serverSeq = 0;

/**
 * The fake Telegram Bot API server. Egress TURN-TEXT capture lives on the
 * shared harness wire (conformance/wire.ts); this server owns the POLLING
 * surface plus every CONTROL-PLANE capture (callbacks, reactions,
 * chat-action variants, reply-markup strips).
 */
export class TelegramBotApiFake {
	readonly id = ++serverSeq;

	// ── raw update queue + single offset space ───────────────────────────
	private rawUpdates: TgWireUpdate[] = [];
	private nextUpdateId = 5000;
	private nextMessageId = 10000;
	private nextCallbackQueryId = 9000;
	private nextThreadSeq = 3000;
	private confirmedThrough = 0;

	/**
	 * Registry: update_id → RAW wire update. The adapter's handleIngress
	 * looks non-message kinds up HERE (keyed by IncomingEvent.messageId =
	 * String(update_id)) — the kind-routing seam forced by the inherited
	 * engine's text-shaped dispatch pipeline.
	 */
	private readonly rawRegistry = new Map<number, TgWireUpdate>();

	// ── getUpdates session model (reference-fake parity) ─────────────────
	private sessions = new Map<number, { terminated: boolean }>();
	private sessionSeq = 0;
	private holderToken: number | null = null;
	private pollWaiters: PollWaiter[] = [];

	readonly dropPendingFlags: boolean[] = [];
	readonly commitLog: Array<{ token: number; offset: number }> = [];

	// ── scripted behaviors ───────────────────────────────────────────────
	private typingScripts: TgBehavior[] = [];
	private reactionScripts: TgBehavior[] = [];
	private webhookScripts: TgBehavior[] = [];
	private readonly mediaScripts = new Map<TgMediaMethod, TgBehavior[]>();
	private readonly richScripts = new Map<TgRichMethod, TgBehavior[]>();
	private readonly topicScripts: TgBehavior[] = [];
	private readonly commandScripts: TgBehavior[] = [];

	// ── control-plane capture ────────────────────────────────────────────
	readonly chatActions: Array<{
		chatId: string;
		action: string;
		threadId: number | null;
		seq: number;
	}> = [];
	readonly reactionOps: Array<{
		chatId: string;
		messageId: string;
		/** Emoji set; null ⇒ documented clear-all call. */
		reaction: string | null;
		seq: number;
	}> = [];
	readonly callbackAnswers: Array<{
		callbackQueryId: string;
		text: string;
		seq: number;
	}> = [];
	readonly replyMarkupEdits: Array<{
		chatId: string;
		messageId: string;
		markup: unknown | null;
		seq: number;
	}> = [];
	readonly sentMessages: TgSentMessage[] = [];
	/** deleteWebhook capture (tg-7): drop_pending_updates is ALWAYS false. */
	readonly webhookDeletes: Array<{ dropPendingUpdates: boolean; seq: number }> =
		[];
	/** Every getUpdates allowed_updates argument, in call order (tg-1). */
	readonly allowedUpdatesLog: Array<readonly string[] | undefined> = [];
	/** Every getUpdates long-poll timeoutMs, in call order (tg2-8). */
	readonly pollTimeoutLog: number[] = [];
	/** Outgoing media family capture (tg-9), in call order. */
	readonly mediaOps: TgMediaOp[] = [];
	/** Rich endpoint capture (tg2-6), in call order (successful calls only). */
	readonly richOps: TgRichOp[] = [];
	/** EVERY rich attempt incl. scripted failures, in call order (tg2-6). */
	readonly richAttemptsLog: TgRichMethod[] = [];
	/** FULL sendMessage kwargs, in call order (Bot API arg-set truth). */
	readonly sendKwargs: Array<Record<string, unknown>> = [];
	/** FULL editMessageText kwargs, in call order (parse_mode truth). */
	readonly editKwargs: Array<Record<string, unknown>> = [];
	/** set_my_commands capture (tg2-3 housekeeping + lazy forum scopes). */
	readonly myCommandsOps: TgMyCommandsOp[] = [];
	/** set_my_short_description capture (status indicator, tg2-3). */
	readonly shortDescriptions: Array<{ text: string; seq: number }> = [];
	/** create_forum_topic capture (DM topics, tg2-3). */
	readonly forumTopicCreates: Array<{
		chatId: string;
		name: string;
		threadId: number;
		seq: number;
	}> = [];
	/** edit_forum_topic capture (DM-topic rename, tg2-3). */
	readonly forumTopicEdits: Array<{
		chatId: string;
		threadId: number;
		name: string;
		seq: number;
	}> = [];
	private actionSeq = 0;
	private opSeq = 0;

	// ── scenario knobs ───────────────────────────────────────────────────
	private reachable = { all: true, general: true, poll: true };
	private unkillableZombie = false;
	private readonly wedgedTokens = new Set<number>();

	scriptTyping(...behaviors: TgBehavior[]): void {
		this.typingScripts.push(...behaviors);
	}

	scriptReactions(...behaviors: TgBehavior[]): void {
		this.reactionScripts.push(...behaviors);
	}

	scriptWebhook(...behaviors: TgBehavior[]): void {
		this.webhookScripts.push(...behaviors);
	}

	scriptMedia(method: TgMediaMethod, ...behaviors: TgBehavior[]): void {
		const queue = this.mediaScripts.get(method) ?? [];
		queue.push(...behaviors);
		this.mediaScripts.set(method, queue);
	}

	scriptRich(method: TgRichMethod, ...behaviors: TgBehavior[]): void {
		const queue = this.richScripts.get(method) ?? [];
		queue.push(...behaviors);
		this.richScripts.set(method, queue);
	}

	scriptForumTopics(...behaviors: TgBehavior[]): void {
		this.topicScripts.push(...behaviors);
	}

	scriptCommands(...behaviors: TgBehavior[]): void {
		this.commandScripts.push(...behaviors);
	}

	setReachable(
		reachable: boolean,
		pool: "all" | "general" | "poll" = "all",
	): void {
		if (pool === "all") {
			this.reachable = { all: reachable, general: reachable, poll: reachable };
		} else {
			this.reachable = { ...this.reachable, [pool]: reachable };
		}
		if (!this.reachable.poll) {
			this.rejectAllWaiters(new TelegramTransportError("fetch failed"));
		}
	}

	setUnkillableZombie(on: boolean): void {
		this.unkillableZombie = on;
	}

	setConsumerWedged(sessionToken: number, wedged: boolean): void {
		if (wedged) this.wedgedTokens.add(sessionToken);
		else this.wedgedTokens.delete(sessionToken);
	}

	// ── raw update ingest (the wire-shape entry point) ───────────────────

	/**
	 * Push a FULLY-SHAPED Telegram update onto the wire queue. The engine
	 * plane sees a flattened view; the raw object stays registry-addressable
	 * for kind routing.
	 */
	pushRawUpdate(update: Omit<TgWireUpdate, "update_id">): TgWireUpdate {
		const full: TgWireUpdate = { update_id: this.nextUpdateId++, ...update };
		this.rawUpdates.push(full);
		this.rawRegistry.set(full.update_id, full);
		this.wakeWaiters();
		return full;
	}

	/** Convenience: a plain text message update. */
	pushTextUpdate(chatId: number, text: string, senderId = 7001): TgWireUpdate {
		return this.pushRawUpdate({
			message: {
				message_id: this.nextMessageId++,
				from: { id: senderId, is_bot: false, username: `user${senderId}` },
				chat: { id: chatId, type: "private" },
				date: 1760000000 + this.rawUpdates.length,
				text,
			},
		});
	}

	/** Convenience: a text message from a FORUM supergroup chat (tg2-3). */
	pushForumTextUpdate(chatId: number, text: string, senderId = 7001): TgWireUpdate {
		return this.pushRawUpdate({
			message: {
				message_id: this.nextMessageId++,
				from: { id: senderId, is_bot: false, username: `user${senderId}` },
				chat: { id: chatId, type: "supergroup", is_forum: true },
				date: 1760000000 + this.rawUpdates.length,
				text,
			},
		});
	}

	/** Convenience: a callback_query update attached to a host message. */
	pushCallbackUpdate(opts: {
		hostChatId: number;
		hostMessageId: string | number;
		data: string;
		clickerId?: number | undefined;
	}): TgWireUpdate {
		const clickerId = opts.clickerId ?? 7001;
		const hostIdNum = Number(opts.hostMessageId);
		return this.pushRawUpdate({
			callback_query: {
				id: `cbq${this.nextCallbackQueryId++}`,
				from: { id: clickerId, is_bot: false, username: `user${clickerId}` },
				message: {
					message_id: Number.isFinite(hostIdNum)
						? hostIdNum
						: opts.hostMessageId,
					chat: { id: opts.hostChatId, type: "private" },
					date: 1760000000,
				},
				data: opts.data,
			},
		});
	}

	rawUpdateFor(updateId: number | string): TgWireUpdate | undefined {
		return this.rawRegistry.get(Number(updateId));
	}

	get pendingUpdateCount(): number {
		return this.rawUpdates.filter((u) => u.update_id > this.confirmedThrough)
			.length;
	}

	/** Server-side ACK: offsets ≤ `offset - 1` confirmed and pruned. */
	commitOffset(token: number, offset: number): void {
		void token;
		this.commitLog.push({ token, offset });
		if (offset - 1 > this.confirmedThrough) this.confirmedThrough = offset - 1;
		this.rawUpdates = this.rawUpdates.filter((u) => u.update_id >= offset);
	}

	// ── session surface (reference-fake parity) ──────────────────────────

	openSession(): number {
		const token = ++this.sessionSeq;
		this.sessions.set(token, { terminated: false });
		return token;
	}

	stealHolderAsZombie(): number {
		const token = this.openSession();
		this.holderToken = token;
		return token;
	}

	get currentHolder(): number | null {
		return this.holderToken;
	}

	liveSessionCount(): number {
		let n = 0;
		for (const s of this.sessions.values()) if (!s.terminated) n += 1;
		return n;
	}

	isSessionTerminated(token: number): boolean {
		return this.sessions.get(token)?.terminated ?? true;
	}

	// ── Bot API surface — engine plane ───────────────────────────────────

	async getMe(): Promise<{ username: string }> {
		this.assertReachable("general", "getMe");
		return { username: "pi_gateway_bot" };
	}

	async getWebhookInfo(): Promise<{ pending_update_count: number }> {
		this.assertReachable("general", "getWebhookInfo");
		return { pending_update_count: this.pendingUpdateCount };
	}

	/**
	 * Long-poll fetch over the SINGLE offset space. Returns the flattened
	 * engine view (text for messages, "" for other kinds — kind routing
	 * happens adapter-side via rawUpdateFor).
	 *
	 * REAL allowed_updates FILTERING (tg-1): updates whose kind is outside
	 * the requested set are never delivered. An OMITTED argument applies the
	 * Bot API DEFAULT filter, which excludes message_reaction /
	 * message_reaction_count — so a poller that forgets allowed_updates
	 * silently loses reaction ingress, exactly like production.
	 */
	async getUpdates(opts: {
		sessionToken: number;
		offset: number;
		timeoutMs: number;
		dropPendingUpdates?: boolean | undefined;
		allowedUpdates?: readonly string[] | undefined;
	}): Promise<{ updates: EngineUpdateView[] }> {
		this.assertReachable("poll", "getUpdates");
		this.allowedUpdatesLog.push(opts.allowedUpdates);
		this.pollTimeoutLog.push(opts.timeoutMs);
		const effectiveAllowed =
			opts.allowedUpdates ?? TELEGRAM_DEFAULT_ALLOWED_UPDATES;
		const session = this.sessions.get(opts.sessionToken);
		if (session === undefined || session.terminated) {
			throw new TelegramConflictError();
		}

		if (opts.dropPendingUpdates === true) {
			this.dropPendingFlags.push(true);
			this.evictRivalSessions(opts.sessionToken);
			this.rawUpdates = [];
		}

		if (
			this.holderToken !== null &&
			this.holderToken !== opts.sessionToken &&
			!(this.sessions.get(this.holderToken)?.terminated ?? true)
		) {
			throw new TelegramConflictError();
		}
		this.holderToken = opts.sessionToken;

		const available = (): EngineUpdateView[] =>
			this.rawUpdates
				.filter((u) => u.update_id >= opts.offset)
				.filter((u) => this.kindAllowed(u, effectiveAllowed))
				.map((u) => this.engineViewOf(u));
		const ready = available();
		if (ready.length > 0) return { updates: ready };
		if (opts.timeoutMs <= 0) return { updates: [] };

		return new Promise<{ updates: EngineUpdateView[] }>((resolve, reject) => {
			const waiter: PollWaiter = {
				token: opts.sessionToken,
				offset: opts.offset,
				...(opts.allowedUpdates !== undefined
					? { allowedUpdates: opts.allowedUpdates }
					: {}),
				settle: resolve,
				reject,
				cancelTimer: () => clearTimeout(t),
			};
			const t = setTimeout(
				() => {
					const idx = this.pollWaiters.indexOf(waiter);
					if (idx >= 0) this.pollWaiters.splice(idx, 1);
					resolve({ updates: available() });
				},
				Math.max(1, opts.timeoutMs),
			);
			t.unref?.();
			waiter.cancelTimer = () => clearTimeout(t);
			this.pollWaiters.push(waiter);
		});
	}

	/** The wire update-kind name for a raw update (Bot API update object). */
	private kindOf(u: TgWireUpdate): string {
		if (u.message !== undefined) return "message";
		if (u.edited_message !== undefined) return "edited_message";
		if (u.channel_post !== undefined) return "channel_post";
		if (u.callback_query !== undefined) return "callback_query";
		if (u.message_reaction !== undefined) return "message_reaction";
		return "unknown";
	}

	private kindAllowed(u: TgWireUpdate, allowed: readonly string[]): boolean {
		return allowed.includes(this.kindOf(u));
	}

	/** Flatten a raw update for the engine plane (message text only). */
	private engineViewOf(u: TgWireUpdate): EngineUpdateView {
		if (u.message !== undefined) {
			return {
				updateId: u.update_id,
				chatId: String(u.message.chat.id),
				senderId: String(u.message.from?.id ?? 0),
				text: u.message.text ?? "",
			};
		}
		const chatId =
			u.callback_query?.message?.chat.id ??
			u.message_reaction?.chat?.id ??
			u.edited_message?.chat.id ??
			0;
		const senderId =
			u.callback_query?.from.id ??
			u.message_reaction?.new_reaction?.length ??
			u.edited_message?.from?.id ??
			0;
		return {
			updateId: u.update_id,
			chatId: String(chatId),
			senderId: String(senderId),
			text: "", // non-message kinds route by registry lookup, never text
		};
	}

	// ── Bot API surface — control plane ──────────────────────────────────

	/**
	 * deleteWebhook (tg-7): clears any stale webhook so getUpdates never
	 * fails with "can\u0027t use getUpdates while webhook is active". The real call
	 * always carries drop_pending_updates=false (Hermes
	 * adapter.py:_delete_webhook_best_effort); scripted failures surface as
	 * transport errors for the cold-boot require_success / reconnect
	 * best-effort split.
	 */
	async deleteWebhook(
		opts: { drop_pending_updates?: boolean | undefined } = {},
	): Promise<{ ok: true }> {
		this.assertReachable("general", "deleteWebhook");
		const behavior =
			this.webhookScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		if (behavior.kind === "fail") {
			throw new TelegramTransportError(behavior.error);
		}
		this.opSeq += 1;
		this.webhookDeletes.push({
			dropPendingUpdates: opts.drop_pending_updates === true,
			seq: this.opSeq,
		});
		return { ok: true };
	}

	/** sendMessage — shaped result; full kwargs captured for assertions. */
	async sendMessage(opts: {
		chat_id: number | string;
		text: string;
		parse_mode?: string | undefined;
		reply_markup?: unknown;
		reply_to_message_id?: number | null | undefined;
		message_thread_id?: number | null | undefined;
		disable_notification?: boolean | undefined;
	}): Promise<TgSentMessage> {
		this.assertReachable("general", "sendMessage");
		this.opSeq += 1;
		this.sendKwargs.push({ ...opts });
		const msg: TgSentMessage = {
			message_id: this.nextMessageId++,
			chat: { id: Number(opts.chat_id) },
			date: 1760000000 + this.opSeq,
			text: opts.text,
		};
		this.sentMessages.push(msg);
		return msg;
	}

	/** editMessageText — capture-only (assertion surface for shape rows). */
	async editMessageText(opts: {
		chat_id: number | string;
		message_id: number | string;
		text: string;
		parse_mode?: string | undefined;
		reply_markup?: unknown | null;
	}): Promise<TgSentMessage> {
		this.assertReachable("general", "editMessageText");
		this.opSeq += 1;
		this.editKwargs.push({ ...opts });
		return {
			message_id: Number(opts.message_id),
			chat: { id: Number(opts.chat_id) },
			date: 1760000000 + this.opSeq,
			text: opts.text,
		};
	}

	/**
	 * Consumed-button strip (§9.1): resolved callbacks edit the host message
	 * with reply_markup REMOVED — consumed state visible client-side.
	 */
	async editMessageReplyMarkup(opts: {
		chat_id: number | string;
		message_id: number | string;
		reply_markup: unknown | null;
	}): Promise<void> {
		this.opSeq += 1;
		this.replyMarkupEdits.push({
			chatId: String(opts.chat_id),
			messageId: String(opts.message_id),
			markup: opts.reply_markup,
			seq: this.opSeq,
		});
	}

	/** answerCallbackQuery — the spinner-clear surface; EVERY tap answers. */
	async answerCallbackQuery(opts: {
		callback_query_id: string;
		text?: string | undefined;
	}): Promise<SendResult> {
		this.assertReachable("general", "answerCallbackQuery");
		this.opSeq += 1;
		this.callbackAnswers.push({
			callbackQueryId: opts.callback_query_id,
			text: opts.text ?? "",
			seq: this.opSeq,
		});
		return { success: true };
	}

	/** set_message_reaction — A1 ack surface (reaction=null clears all). */
	async setMessageReaction(opts: {
		chat_id: number | string;
		message_id: number | string;
		reaction: string | null;
	}): Promise<SendResult> {
		this.assertReachable("general", "setMessageReaction");
		const behavior =
			this.reactionScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		switch (behavior.kind) {
			case "ok":
				break;
			case "flood":
				return {
					success: false,
					error: `Too Many Requests: retry after ${behavior.retryAfter}`,
					retryAfter: behavior.retryAfter,
				};
			case "fail":
				return { success: false, error: behavior.error };
		}
		this.opSeq += 1;
		this.reactionOps.push({
			chatId: String(opts.chat_id),
			messageId: String(opts.message_id),
			reaction: opts.reaction,
			seq: this.opSeq,
		});
		return { success: true };
	}

	/** sendChatAction with the FULL variant surface (A11). */
	async sendChatActionEx(opts: {
		chat_id: number | string;
		action: string;
		message_thread_id?: number | null | undefined;
	}): Promise<SendResult> {
		this.assertReachable("general", "sendChatAction");
		const behavior =
			this.typingScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		switch (behavior.kind) {
			case "ok":
				break;
			case "flood":
				return {
					success: false,
					error: `Too Many Requests: retry after ${behavior.retryAfter}`,
					retryAfter: behavior.retryAfter,
				};
			case "fail":
				return { success: false, error: behavior.error };
		}
		this.actionSeq += 1;
		this.chatActions.push({
			chatId: String(opts.chat_id),
			action: opts.action,
			threadId: opts.message_thread_id ?? null,
			seq: this.actionSeq,
		});
		return { success: true };
	}

	/**
	 * Reference-engine compatibility shim: the inherited core's sendTyping
	 * calls sendChatAction(chatId, action) without a thread id. Subclass
	 * overrides route through sendChatActionEx; this keeps the base path
	 * functional (default action, no thread).
	 */
	async sendChatAction(chatId: string, action = "typing"): Promise<SendResult> {
		return this.sendChatActionEx({ chat_id: chatId, action });
	}

	// ── Bot API surface — outgoing media family (tg-9) ──────────────────

	/**
	 * The outgoing media methods Hermes exercises on the Bot API
	 * (adapter.py:send_image_file/send_document/send_video/send_voice/
	 * send_animation): each captures its FULL snake_case kwarg set —
	 * notification kwargs, thread kwargs, reply anchors, caption — and can
	 * be scripted per-method for fallback-contract tests.
	 */
	private async transmitMedia(
		method: TgMediaMethod,
		opts: Record<string, unknown>,
	): Promise<SendResult> {
		this.assertReachable("general", method);
		const queue = this.mediaScripts.get(method) ?? [];
		const behavior = queue.shift() ?? ({ kind: "ok" } as TgBehavior);
		if (behavior.kind === "fail") {
			return { success: false, error: behavior.error };
		}
		if (behavior.kind === "flood") {
			return {
				success: false,
				error: `Too Many Requests: retry after ${behavior.retryAfter}`,
				retryAfter: behavior.retryAfter,
			};
		}
		this.opSeq += 1;
		this.mediaOps.push({ method, args: { ...opts }, seq: this.opSeq });
		return { success: true, messageId: `media-${this.opSeq}` };
	}

	async sendPhoto(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendPhoto", opts);
	}

	async sendDocument(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendDocument", opts);
	}

	async sendVoice(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendVoice", opts);
	}

	/** Bot API sendAudio — MP3/M4A audio files only (Hermes voice lane). */
	async sendAudio(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendAudio", opts);
	}

	async sendVideo(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendVideo", opts);
	}

	async sendAnimation(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitMedia("sendAnimation", opts);
	}

	// ── Bot API surface — post-connect housekeeping (tg2-3) ─────────

	/**
	 * set_my_commands — command-menu registration per scope (Default /
	 * AllPrivateChats / AllGroupChats + lazy BotCommandScopeChat for forum
	 * supergroups, tg2-3). Captured for the housekeeping row; per-scope
	 * failure tolerance is adapter-side (each scope registers independently).
	 */
	async setMyCommands(opts: {
		commands: Array<{ command: string; description: string }>;
		scope?: TgCommandScope | undefined;
	}): Promise<{ ok: true }> {
		this.assertReachable("general", "setMyCommands");
		const behavior =
			this.commandScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		if (behavior.kind === "fail") {
			throw new TelegramTransportError(behavior.error);
		}
		this.opSeq += 1;
		this.myCommandsOps.push({
			commands: opts.commands.map((c) => ({ ...c })),
			scope: opts.scope ?? { type: "default" },
			seq: this.opSeq,
		});
		return { ok: true };
	}

	/** set_my_short_description — the opt-in status indicator surface. */
	async setMyShortDescription(opts: {
		short_description: string;
	}): Promise<{ ok: true }> {
		this.assertReachable("general", "setMyShortDescription");
		this.opSeq += 1;
		this.shortDescriptions.push({
			text: opts.short_description,
			seq: this.opSeq,
		});
		return { ok: true };
	}

	/**
	 * create_forum_topic — Bot API 9.4 private DM topics + forum topics.
	 * Auto-assigns monotonic thread ids; scripted behaviors support the
	 * duplicate-topic / forums-disabled error classes the adapter classifies.
	 */
	async createForumTopic(opts: {
		chat_id: number | string;
		name: string;
		icon_color?: number | undefined;
		icon_custom_emoji_id?: string | undefined;
	}): Promise<{ message_thread_id: number }> {
		this.assertReachable("general", "createForumTopic");
		const behavior =
			this.topicScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		if (behavior.kind === "fail") {
			throw new TelegramTransportError(`Bad Request: ${behavior.error}`);
		}
		this.nextThreadSeq += 1;
		const threadId = this.nextThreadSeq;
		this.opSeq += 1;
		this.forumTopicCreates.push({
			chatId: String(opts.chat_id),
			name: opts.name,
			threadId,
			seq: this.opSeq,
		});
		return { message_thread_id: threadId };
	}

	/** edit_forum_topic — DM-topic rename (adapter.py rename_dm_topic). */
	async editForumTopic(opts: {
		chat_id: number | string;
		message_thread_id: number;
		name: string;
	}): Promise<{ ok: true }> {
		this.assertReachable("general", "editForumTopic");
		this.opSeq += 1;
		this.forumTopicEdits.push({
			chatId: String(opts.chat_id),
			threadId: Number(opts.message_thread_id),
			name: opts.name,
			seq: this.opSeq,
		});
		return { ok: true };
	}

	// ── Bot API 10.1 rich endpoints (tg2-6) ─────────────────────────

	/** Raw do_api_request-style rich transmission with per-method scripts. */
	private async transmitRich(
		method: TgRichMethod,
		opts: Record<string, unknown>,
	): Promise<SendResult> {
		this.assertReachable("general", method);
		this.richAttemptsLog.push(method);
		const queue = this.richScripts.get(method) ?? [];
		const behavior = queue.shift() ?? ({ kind: "ok" } as TgBehavior);
		if (behavior.kind === "fail") {
			return { success: false, error: behavior.error };
		}
		if (behavior.kind === "flood") {
			return {
				success: false,
				error: `Too Many Requests: retry after ${behavior.retryAfter}`,
				retryAfter: behavior.retryAfter,
			};
		}
		this.opSeq += 1;
		this.richOps.push({ method, args: { ...opts }, seq: this.opSeq });
		return { success: true, messageId: `rich-${this.opSeq}` };
	}

	async sendRichMessage(opts: Record<string, unknown>): Promise<SendResult> {
		return this.transmitRich("sendRichMessage", opts);
	}

	async sendRichMessageDraft(
		opts: Record<string, unknown>,
	): Promise<SendResult> {
		return this.transmitRich("sendRichMessageDraft", opts);
	}

	// ── internals (reference-fake parity) ────────────────────────────────

	private assertReachable(pool: "general" | "poll", method: string): void {
		if (!this.reachable.all || !this.reachable[pool]) {
			throw new TelegramTransportError(
				`${method}: getaddrinfo ENOTFOUND api.telegram.org`,
			);
		}
	}

	private evictRivalSessions(takerToken: number): void {
		for (const [token, session] of this.sessions) {
			if (token === takerToken || session.terminated) continue;
			session.terminated = true;
		}
		for (const waiter of [...this.pollWaiters]) {
			if (waiter.token === takerToken) continue;
			this.removeWaiter(waiter);
			this.clearFlush(waiter);
			waiter.cancelTimer();
			waiter.reject(
				new TelegramTransportError(
					"long-poll terminated by a drop_pending_updates takeover",
				),
			);
		}
		this.holderToken = takerToken;
		if (this.unkillableZombie) {
			this.stealHolderAsZombie();
		}
	}

	private wakeWaiters(): void {
		for (const waiter of [...this.pollWaiters]) {
			const session = this.sessions.get(waiter.token);
			if (session === undefined || session.terminated) {
				this.removeWaiter(waiter);
				this.clearFlush(waiter);
				waiter.cancelTimer();
				waiter.reject(new TelegramConflictError());
				continue;
			}
			if (this.wedgedTokens.has(waiter.token)) continue;
			if (waiter.flushTimer === undefined) {
				const t = setTimeout(() => {
					waiter.flushTimer = undefined;
					this.flushWaiter(waiter);
				}, 2);
				t.unref?.();
				waiter.flushTimer = t;
			}
		}
	}

	private clearFlush(waiter: PollWaiter): void {
		if (waiter.flushTimer !== undefined) {
			clearTimeout(waiter.flushTimer);
			waiter.flushTimer = undefined;
		}
	}

	private flushWaiter(waiter: PollWaiter): void {
		const session = this.sessions.get(waiter.token);
		if (session === undefined || session.terminated) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.reject(new TelegramConflictError());
			return;
		}
		if (this.wedgedTokens.has(waiter.token)) return;
		const allowed = waiter.allowedUpdates ?? TELEGRAM_DEFAULT_ALLOWED_UPDATES;
		const ready = this.rawUpdates
			.filter((u) => u.update_id >= waiter.offset)
			.filter((u) => this.kindAllowed(u, allowed))
			.map((u) => this.engineViewOf(u));
		if (ready.length > 0 && this.holderToken === waiter.token) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.settle({ updates: ready });
		}
	}

	private rejectAllWaiters(err: Error): void {
		for (const waiter of [...this.pollWaiters]) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.reject(err);
		}
	}

	private removeWaiter(waiter: PollWaiter): void {
		const idx = this.pollWaiters.indexOf(waiter);
		if (idx >= 0) this.pollWaiters.splice(idx, 1);
	}
}
