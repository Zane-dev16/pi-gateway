// pi_platforms/telegram/bot-api-client — REAL Telegram Bot API HTTP
// transport (DEC-077). Dependency-free (`global fetch`); implements the SAME
// seam the adapter consumes (`TelegramBotApiSeam`) so the production factory
// binds it where tests bind `TelegramBotApiFake` — the fake stays the TEST
// seam untouched.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/telegram/adapter.py:send                 (attempt %d/3)
//   plugins/platforms/telegram/adapter.py:_delete_webhook_best_effort
//   plugins/platforms/telegram/adapter.py:sendRichMessage / sendRichMessageDraft
//     via do_api_request
//   plugins/platforms/telegram/adapter.py:send_draft           (legacy lane)
//   plugins/platforms/telegram/adapter.py:_start_polling_once  (ALL_TYPES)
//   plugins/platforms/telegram/telegram_ids.py:normalize_telegram_chat_id

import type {
	DraftFrameArgs,
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { TELEGRAM_ALLOWED_UPDATES } from "./manifest.js";
import { normalizeTelegramChatId } from "./telegram-ids.js";
import type {
	EngineUpdateView,
	TgCommandScope,
	TgSentMessage,
	TgWireMessage,
	TgWireUpdate,
} from "./telegram-fake-server.js";
import {
	TelegramConflictError,
	TelegramTransportError,
} from "./telegram-fake-server.js";

/**
 * THE seam the adapter consumes. `TelegramBotApiFake` satisfies it
 * structurally (no fake edits); this module's HTTP client implements it for
 * production. `sendMessage` is intentionally NOT on the seam — the adapter
 * never calls it directly (plain sends travel `wireTransmitSend`, bound to
 * the client by `bindTelegramProductionTransport` below).
 */
export interface TelegramBotApiSeam {
	openSession(): number;
	commitOffset(token: number, offset: number): void;
	getUpdates(opts: {
		sessionToken: number;
		offset: number;
		timeoutMs: number;
		dropPendingUpdates?: boolean | undefined;
		allowedUpdates?: readonly string[] | undefined;
	}): Promise<{ updates: EngineUpdateView[] }>;
	rawUpdateFor(updateId: number | string): TgWireUpdate | undefined;
	getMe(): Promise<{ username: string }>;
	getWebhookInfo(): Promise<{ pending_update_count: number }>;
	deleteWebhook(opts?: {
		drop_pending_updates?: boolean | undefined;
	}): Promise<{ ok: true }>;
	sendChatAction(chatId: string, action?: string): Promise<SendResult>;
	sendChatActionEx(opts: {
		chat_id: number | string;
		action: string;
		message_thread_id?: number | null | undefined;
	}): Promise<SendResult>;
	answerCallbackQuery(opts: {
		callback_query_id: string;
		text?: string | undefined;
	}): Promise<SendResult>;
	editMessageText(opts: {
		chat_id: number | string;
		message_id: number | string;
		text: string;
		parse_mode?: string | undefined;
		reply_markup?: unknown | null;
	}): Promise<TgSentMessage>;
	editMessageReplyMarkup(opts: {
		chat_id: number | string;
		message_id: number | string;
		reply_markup: unknown | null;
	}): Promise<void>;
	setMessageReaction(opts: {
		chat_id: number | string;
		message_id: number | string;
		reaction: string | null;
	}): Promise<SendResult>;
	sendPhoto(opts: Record<string, unknown>): Promise<SendResult>;
	sendDocument(opts: Record<string, unknown>): Promise<SendResult>;
	sendVoice(opts: Record<string, unknown>): Promise<SendResult>;
	sendAudio(opts: Record<string, unknown>): Promise<SendResult>;
	sendVideo(opts: Record<string, unknown>): Promise<SendResult>;
	sendAnimation(opts: Record<string, unknown>): Promise<SendResult>;
	setMyCommands(opts: {
		commands: Array<{ command: string; description: string }>;
		scope?: TgCommandScope | undefined;
	}): Promise<{ ok: true }>;
	setMyShortDescription(opts: {
		short_description: string;
	}): Promise<{ ok: true }>;
	createForumTopic(opts: {
		chat_id: number | string;
		name: string;
		icon_color?: number | undefined;
		icon_custom_emoji_id?: string | undefined;
	}): Promise<{ message_thread_id: number }>;
	editForumTopic(opts: {
		chat_id: number | string;
		message_thread_id: number;
		name: string;
	}): Promise<{ ok: true }>;
	sendRichMessage(opts: Record<string, unknown>): Promise<SendResult>;
	sendRichMessageDraft(opts: Record<string, unknown>): Promise<SendResult>;
	sendMessageDraft(opts: {
		chat_id: number | string;
		draft_id: number;
		text: string;
		parse_mode?: string | undefined;
		message_thread_id?: number | null | undefined;
		direct_messages_topic_id?: number | undefined;
	}): Promise<SendResult>;
	deleteMessage(opts: {
		chat_id: number | string;
		message_id: number | string;
	}): Promise<SendResult>;
}

export interface HttpTelegramBotApiOptions {
	/** Bot token (TELEGRAM_BOT_TOKEN via the scoped secret reader). */
	token: string;
	/** Origin override for loopback stub servers; default is production. */
	baseUrl?: string | undefined;
}

/** Non-sync endpoints get a 30 s abort (matrix `_standalone_send` parity). */
const NON_SYNC_TIMEOUT_MS = 30_000;

const TELEGRAM_API_ORIGIN = "https://api.telegram.org";

interface BotEnvelope {
	ok: boolean;
	result: unknown;
	errorCode: number | null;
	description: string | null;
	retryAfter: number | null;
}

export class HttpTelegramBotApi implements TelegramBotApiSeam {
	private readonly token: string;
	private readonly baseUrl: string;
	private sessionSeq = 0;
	/**
	 * Client-side offset floor: the next update id the server may deliver.
	 * The engine owns offset bookkeeping; the floor only ever moves FORWARD
	 * (drop-discards + commits), so a stale engine offset can never
	 * redeliver updates the client already dropped or confirmed.
	 */
	private offsetFloor = 0;
	/**
	 * Registry: update_id → validated RAW wire update. The adapter's
	 * handleIngress looks non-message kinds up HERE — the kind-routing seam.
	 * Pruned below the offset floor on every commit/drop.
	 */
	private readonly rawRegistry = new Map<number, TgWireUpdate>();
	private readonly inflight = new Set<AbortController>();

	constructor(opts: HttpTelegramBotApiOptions) {
		this.token = opts.token;
		this.baseUrl = (opts.baseUrl ?? TELEGRAM_API_ORIGIN).replace(/\/+$/, "");
	}

	/** Parked long-polls DIE (matrix transport-class parity). */
	closeSessions(): void {
		for (const c of this.inflight) {
			try {
				c.abort();
			} catch {
				/* already settled */
			}
		}
		this.inflight.clear();
	}

	openSession(): number {
		this.sessionSeq += 1;
		return this.sessionSeq;
	}

	/** Server-side ACK: the floor advances; confirmed raws prune. */
	commitOffset(token: number, offset: number): void {
		void token;
		if (offset > this.offsetFloor) this.offsetFloor = offset;
		this.pruneRegistry();
	}

	async getUpdates(opts: {
		sessionToken: number;
		offset: number;
		timeoutMs: number;
		dropPendingUpdates?: boolean | undefined;
		allowedUpdates?: readonly string[] | undefined;
	}): Promise<{ updates: EngineUpdateView[] }> {
		void opts.sessionToken;
		const offset = Math.max(opts.offset, this.offsetFloor);
		if (opts.dropPendingUpdates === true) {
			// Cold-boot / conflict-recovery parity: one fetch-and-discard
			// quick poll. The higher-offset next poll already confirms
			// server-side, so no second round trip is needed.
			const stale = await this.poll(offset, 0, opts.allowedUpdates);
			if (stale.length > 0) {
				const maxId = Math.max(...stale.map((u) => u.updateId));
				this.offsetFloor = Math.max(this.offsetFloor, maxId + 1);
				this.pruneRegistry();
			} else if (offset > this.offsetFloor) {
				this.offsetFloor = offset;
				this.pruneRegistry();
			}
			return { updates: [] };
		}
		return {
			updates: await this.poll(offset, opts.timeoutMs, opts.allowedUpdates),
		};
	}

	rawUpdateFor(updateId: number | string): TgWireUpdate | undefined {
		return this.rawRegistry.get(Number(updateId));
	}

	async getMe(): Promise<{ username: string }> {
		const result = await this.callThrowing("getMe");
		if (typeof result !== "object" || result === null) {
			throw new TelegramTransportError("getMe: malformed response");
		}
		const username = (result as Record<string, unknown>)["username"];
		if (typeof username !== "string" || username === "") {
			throw new TelegramTransportError("getMe: missing username");
		}
		return { username };
	}

	async getWebhookInfo(): Promise<{ pending_update_count: number }> {
		const result = await this.callThrowing("getWebhookInfo");
		if (typeof result !== "object" || result === null) {
			throw new TelegramTransportError("getWebhookInfo: malformed response");
		}
		const count = (result as Record<string, unknown>)["pending_update_count"];
		if (typeof count !== "number" || !Number.isFinite(count)) {
			throw new TelegramTransportError("getWebhookInfo: malformed response");
		}
		return { pending_update_count: count };
	}

	async deleteWebhook(
		opts: { drop_pending_updates?: boolean | undefined } = {},
	): Promise<{ ok: true }> {
		await this.callThrowing("deleteWebhook", {
			drop_pending_updates: opts.drop_pending_updates === true,
		});
		return { ok: true };
	}

	async sendChatAction(chatId: string, action = "typing"): Promise<SendResult> {
		return this.sendChatActionEx({ chat_id: chatId, action });
	}

	async sendChatActionEx(opts: {
		chat_id: number | string;
		action: string;
		message_thread_id?: number | null | undefined;
	}): Promise<SendResult> {
		return this.postSend("sendChatAction", {
			chat_id: opts.chat_id,
			action: opts.action,
			...(opts.message_thread_id !== undefined &&
			opts.message_thread_id !== null
				? { message_thread_id: opts.message_thread_id }
				: {}),
		});
	}

	async answerCallbackQuery(opts: {
		callback_query_id: string;
		text?: string | undefined;
	}): Promise<SendResult> {
		return this.postSend("answerCallbackQuery", {
			callback_query_id: opts.callback_query_id,
			...(opts.text !== undefined ? { text: opts.text } : {}),
		});
	}

	async editMessageText(opts: {
		chat_id: number | string;
		message_id: number | string;
		text: string;
		parse_mode?: string | undefined;
		reply_markup?: unknown | null;
	}): Promise<TgSentMessage> {
		// Throwing parity with the fake: both direct sites try/catch and
		// classify the text (not-modified no-op, rich capability ladder).
		const env = await this.postRaw("editMessageText", { ...opts });
		if (!env.ok)
			throw new TelegramTransportError(`editMessageText: ${envText(env)}`);
		const result =
			typeof env.result === "object" && env.result !== null
				? (env.result as Record<string, unknown>)
				: {};
		const messageId = numericIdValue(result["message_id"]);
		if (messageId === null) {
			throw new TelegramTransportError("editMessageText: malformed response");
		}
		return {
			message_id: messageId,
			chat: { id: Number(opts.chat_id) },
			date: Math.floor(Date.now() / 1000),
			text: opts.text,
		};
	}

	async editMessageReplyMarkup(opts: {
		chat_id: number | string;
		message_id: number | string;
		reply_markup: unknown | null;
	}): Promise<void> {
		// Fake parity: the strip never throws (best-effort UI state).
		try {
			await this.postRaw("editMessageReplyMarkup", { ...opts });
		} catch {
			/* consumed-button strip is best-effort */
		}
	}

	async setMessageReaction(opts: {
		chat_id: number | string;
		message_id: number | string;
		reaction: string | null;
	}): Promise<SendResult> {
		return this.postSend("setMessageReaction", {
			chat_id: opts.chat_id,
			message_id: opts.message_id,
			reaction:
				opts.reaction === null ? [] : [{ type: "emoji", emoji: opts.reaction }],
		});
	}

	async sendPhoto(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendPhoto", opts);
	}

	async sendDocument(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendDocument", opts);
	}

	async sendVoice(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendVoice", opts);
	}

	async sendAudio(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendAudio", opts);
	}

	async sendVideo(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendVideo", opts);
	}

	async sendAnimation(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendAnimation", opts);
	}

	async setMyCommands(opts: {
		commands: Array<{ command: string; description: string }>;
		scope?: TgCommandScope | undefined;
	}): Promise<{ ok: true }> {
		await this.callThrowing("setMyCommands", {
			commands: opts.commands.map((c) => ({ ...c })),
			...(opts.scope !== undefined ? { scope: { ...opts.scope } } : {}),
		});
		return { ok: true };
	}

	async setMyShortDescription(opts: {
		short_description: string;
	}): Promise<{ ok: true }> {
		await this.callThrowing("setMyShortDescription", { ...opts });
		return { ok: true };
	}

	async createForumTopic(opts: {
		chat_id: number | string;
		name: string;
		icon_color?: number | undefined;
		icon_custom_emoji_id?: string | undefined;
	}): Promise<{ message_thread_id: number }> {
		const result = await this.callThrowing("createForumTopic", { ...opts });
		if (typeof result !== "object" || result === null) {
			throw new TelegramTransportError("createForumTopic: malformed response");
		}
		const threadId = (result as Record<string, unknown>)["message_thread_id"];
		if (typeof threadId !== "number" || !Number.isFinite(threadId)) {
			throw new TelegramTransportError("createForumTopic: malformed response");
		}
		return { message_thread_id: threadId };
	}

	async editForumTopic(opts: {
		chat_id: number | string;
		message_thread_id: number;
		name: string;
	}): Promise<{ ok: true }> {
		await this.callThrowing("editForumTopic", { ...opts });
		return { ok: true };
	}

	async sendRichMessage(opts: Record<string, unknown>): Promise<SendResult> {
		return this.postSend("sendRichMessage", opts);
	}

	async sendRichMessageDraft(
		opts: Record<string, unknown>,
	): Promise<SendResult> {
		return this.postSend("sendRichMessageDraft", opts);
	}

	async sendMessageDraft(opts: {
		chat_id: number | string;
		draft_id: number;
		text: string;
		parse_mode?: string | undefined;
		message_thread_id?: number | null | undefined;
		direct_messages_topic_id?: number | undefined;
	}): Promise<SendResult> {
		return this.postSend("sendMessageDraft", { ...opts });
	}

	async deleteMessage(opts: {
		chat_id: number | string;
		message_id: number | string;
	}): Promise<SendResult> {
		return this.postSend("deleteMessage", { ...opts });
	}

	/**
	 * Binding-only plain send (the seam omits it — the adapter never calls
	 * it directly). HTTP errors ride SendResult; only network death throws
	 * (the binding converts that too).
	 */
	async sendMessage(params: Record<string, unknown>): Promise<SendResult> {
		const env = await this.postRaw("sendMessage", params);
		if (!env.ok) return sendFailure(env);
		const result =
			typeof env.result === "object" && env.result !== null
				? (env.result as Record<string, unknown>)
				: {};
		const messageId = result["message_id"];
		return {
			success: true,
			...(typeof messageId === "number" || typeof messageId === "string"
				? { messageId: String(messageId) }
				: {}),
		};
	}

	// ── internals ─────────────────────────────────────────────────────────

	/** Long-poll fetch + validate + register (no client abort — the adapter
	 * watchdog owns stuck detection; closeSessions owns teardown). */
	private async poll(
		offset: number,
		timeoutMs: number,
		allowedUpdates: readonly string[] | undefined,
	): Promise<EngineUpdateView[]> {
		const env = await this.postRaw(
			"getUpdates",
			{
				offset,
				timeout: Math.max(0, Math.ceil(timeoutMs / 1000)),
				allowed_updates: [...(allowedUpdates ?? TELEGRAM_ALLOWED_UPDATES)],
			},
			{ sync: true },
		);
		if (!env.ok) {
			if (env.errorCode === 409) throw new TelegramConflictError();
			throw new TelegramTransportError(`getUpdates: ${envText(env)}`);
		}
		if (!Array.isArray(env.result)) {
			throw new TelegramTransportError("getUpdates: malformed response");
		}
		const views: EngineUpdateView[] = [];
		for (const raw of env.result) {
			const parsed = parseUpdate(raw);
			if (parsed === null) continue;
			this.rawRegistry.set(parsed.raw.update_id, parsed.raw);
			views.push(parsed.view);
		}
		return views;
	}

	private pruneRegistry(): void {
		for (const key of [...this.rawRegistry.keys()]) {
			if (key < this.offsetFloor) this.rawRegistry.delete(key);
		}
	}

	/** Throwing call (housekeeping + edit sites the adapter try/catches). */
	private async callThrowing(
		method: string,
		body: Record<string, unknown> = {},
	): Promise<unknown> {
		const env = await this.postRaw(method, body);
		if (!env.ok) throw new TelegramTransportError(`${method}: ${envText(env)}`);
		return env.result;
	}

	/**
	 * Send-path call: HTTP errors ride SendResult, never throw. Success
	 * maps the server message_id when present (media/rich lanes) and stays
	 * a bare success otherwise (drafts carry no id — fake parity).
	 */
	private async postSend(
		method: string,
		body: Record<string, unknown>,
	): Promise<SendResult> {
		const env = await this.postRaw(method, body);
		if (!env.ok) return sendFailure(env);
		const result = isRecord(env.result) ? env.result : null;
		const messageId = result !== null ? result["message_id"] : undefined;
		return {
			success: true,
			...(typeof messageId === "number" || typeof messageId === "string"
				? { messageId: String(messageId) }
				: {}),
		};
	}

	private async postRaw(
		method: string,
		body: Record<string, unknown>,
		opts: { sync?: boolean | undefined } = {},
	): Promise<BotEnvelope> {
		const url = `${this.baseUrl}/bot${this.token}/${method}`;
		const controller = new AbortController();
		this.inflight.add(controller);
		// Non-sync endpoints self-bound at 30 s; sync rides until the server
		// releases the long-poll or closeSessions() aborts it (DEC-077).
		const timer =
			opts.sync === true
				? null
				: setTimeout(() => {
						try {
							controller.abort();
						} catch {
							/* settled */
						}
					}, NON_SYNC_TIMEOUT_MS);
		timer?.unref?.();
		try {
			let res: Response;
			try {
				res = await fetch(url, {
					method: "POST",
					headers: {
						Accept: "application/json",
						"Content-Type": "application/json",
					},
					body: JSON.stringify(body),
					signal: controller.signal,
				});
			} catch (err) {
				throw new TelegramTransportError(`${method}: ${brief(err)}`);
			}
			const json = await safeJson(res);
			if (res.status === 409) {
				return {
					ok: false,
					result: null,
					errorCode: 409,
					description: descriptionOf(json) ?? "Conflict",
					retryAfter: retryAfterOf(json),
				};
			}
			return envelopeOf(json, res.status);
		} finally {
			if (timer !== null) clearTimeout(timer);
			this.inflight.delete(controller);
		}
	}
}

/**
 * Production egress binding: the adapter builds full snake_case arg sets
 * and this POSTs them as Bot API methods. Mirrors
 * `bindMatrixProductionTransport` + the telegram subject's harness binding
 * (same five lanes). Every lane converts network death into SendResult
 * failure — the wire contract never throws.
 */
export interface TelegramProductionEgressTarget {
	wireTransmitSend: (
		chatId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult>;
	wireTransmitDraft: (args: DraftFrameArgs) => Promise<SendResult>;
	wireTransmitDraftFinal: (args: DraftFrameArgs) => Promise<SendResult>;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult>;
	editTransmit: (
		chatId: string,
		messageId: string,
		content: string,
		metadata?: Metadata | undefined,
	) => Promise<SendResult>;
}

export function bindTelegramProductionTransport(
	target: TelegramProductionEgressTarget,
	client: HttpTelegramBotApi,
): void {
	// The send-lane metadata IS the built arg set (chat_id, text,
	// parse_mode, thread/topic kwargs, notification kwargs — tg2-5); the
	// binding forwards it verbatim with chat/text pinned.
	target.wireTransmitSend = async (chatId, content, metadata) => {
		try {
			const record = metadata as unknown as Record<string, unknown>;
			return await client.sendMessage({
				...record,
				chat_id: record["chat_id"] ?? normalizeTelegramChatId(chatId),
				text: content,
			});
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	};
	const transmitDraft = async (args: DraftFrameArgs): Promise<SendResult> => {
		try {
			const meta = (args.metadata ?? {}) as unknown as Record<string, unknown>;
			return await client.sendMessageDraft({
				chat_id: normalizeTelegramChatId(args.chatId),
				draft_id: args.draftId,
				text: args.content,
				...threadPassthrough(meta),
			});
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	};
	// Seal/final previews stay ephemeral draft frames (DEC-034: Hermes has
	// no Bot API to promote a draft — the persisted final travels sendMessage).
	target.wireTransmitDraft = (args) => transmitDraft(args);
	target.wireTransmitDraftFinal = (args) => transmitDraft(args);
	// Unreachable while the rich probe stays unarmed (extras-off default);
	// bound for seam completeness — the live rich lane calls bot.* directly.
	target.wireTransmitRich = async (content, metadata) => {
		try {
			const record = metadata as unknown as Record<string, unknown>;
			const chatRaw = record["chat_id"] ?? record["__tg_rich_chat"];
			if (
				(typeof chatRaw !== "string" && typeof chatRaw !== "number") ||
				String(chatRaw) === ""
			) {
				return { success: false, error: "rich delivery skipped" };
			}
			return await client.sendRichMessage({
				...publicKwargs(record),
				chat_id: chatRaw,
				rich_message: content,
			});
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	};
	// Edit metadata carries consumer keys — only Bot API edit keys cross.
	target.editTransmit = async (chatId, messageId, content, metadata) => {
		try {
			const record = (metadata ?? {}) as unknown as Record<string, unknown>;
			const params: Record<string, unknown> = {
				chat_id: normalizeTelegramChatId(chatId),
				message_id: numericOrRawId(messageId),
				text: content,
			};
			if (typeof record["parse_mode"] === "string") {
				params["parse_mode"] = record["parse_mode"];
			}
			if (record["reply_markup"] !== undefined) {
				params["reply_markup"] = record["reply_markup"];
			}
			const edited = await client.editMessageText({
				chat_id: params["chat_id"] as number | string,
				message_id: params["message_id"] as number | string,
				text: content,
				...(typeof params["parse_mode"] === "string"
					? { parse_mode: params["parse_mode"] as string }
					: {}),
				...(params["reply_markup"] !== undefined
					? { reply_markup: params["reply_markup"] }
					: {}),
			});
			return { success: true, messageId: String(edited.message_id) };
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	};
}

// ── wire mapping ────────────────────────────────────────────────────────────

/** 429 keeps the fake-identical flood shape the adapter parses; else text. */
function sendFailure(env: BotEnvelope): SendResult {
	const description = env.description ?? "request failed";
	if (env.errorCode === 429) {
		const retryAfter = env.retryAfter ?? parseRetryAfter(description) ?? null;
		if (retryAfter !== null) {
			const text = /retry\s+after\s+\d+/i.test(description)
				? description
				: `Too Many Requests: retry after ${retryAfter}`;
			return { success: false, error: text, retryAfter };
		}
	}
	return { success: false, error: description };
}

function parseRetryAfter(description: string): number | null {
	const m = /retry\s+(?:after|in)\s+(\d+)/i.exec(description);
	return m ? Number(m[1]) : null;
}

function envelopeOf(
	json: Record<string, unknown>,
	status: number,
): BotEnvelope {
	if (json["ok"] === true) {
		return {
			ok: true,
			result: json["result"] ?? null,
			errorCode: null,
			description: null,
			retryAfter: null,
		};
	}
	return {
		ok: false,
		result: null,
		errorCode:
			typeof json["error_code"] === "number" ? json["error_code"] : status,
		description: descriptionOf(json) ?? `HTTP ${status}`,
		retryAfter: retryAfterOf(json),
	};
}

function envText(env: BotEnvelope): string {
	return env.description ?? `HTTP ${env.errorCode ?? "error"}`;
}

function descriptionOf(json: Record<string, unknown>): string | null {
	return typeof json["description"] === "string" ? json["description"] : null;
}

function retryAfterOf(json: Record<string, unknown>): number | null {
	const params = json["parameters"];
	if (typeof params === "object" && params !== null) {
		const retry = (params as Record<string, unknown>)["retry_after"];
		if (typeof retry === "number" && Number.isFinite(retry)) return retry;
	}
	return null;
}

async function safeJson(res: Response): Promise<Record<string, unknown>> {
	try {
		const json = (await res.json()) as unknown;
		if (typeof json === "object" && json !== null) {
			return json as Record<string, unknown>;
		}
		return {};
	} catch {
		return {};
	}
}

/** Thread-routing passthrough for draft frames (numbers only, never null). */
function threadPassthrough(
	meta: Record<string, unknown>,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	if (typeof meta["message_thread_id"] === "number") {
		out["message_thread_id"] = meta["message_thread_id"];
	}
	if (typeof meta["direct_messages_topic_id"] === "number") {
		out["direct_messages_topic_id"] = meta["direct_messages_topic_id"];
	}
	return out;
}

/** Metadata minus __-internal keys (rich-lane chat stamp never ships). */
function publicKwargs(
	record: Record<string, unknown>,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(record)) {
		if (!key.startsWith("__")) out[key] = value;
	}
	return out;
}

/** Bot API message ids are ints on the wire; non-numeric ids pass through. */
function numericOrRawId(value: string): number | string {
	const n = Number.parseInt(value, 10);
	return Number.isFinite(n) ? n : value.trim();
}

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 120);
}

// ── update validation (HTTP edge → domain types) ────────────────────────────

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

function idValue(value: unknown): number | string | null {
	if (typeof value === "number" && Number.isFinite(value)) return value;
	if (typeof value === "string" && value.trim() !== "") return value;
	return null;
}

/** Incoming user/chat ids are Bot API ints (numeric strings coerce). */
function numericIdValue(value: unknown): number | null {
	if (typeof value === "number" && Number.isFinite(value)) return value;
	if (typeof value === "string" && /^[+-]?\d+$/.test(value.trim())) {
		return Number.parseInt(value.trim(), 10);
	}
	return null;
}

function optString(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

function optNumber(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value)
		? value
		: undefined;
}

/** Validated message-like member (message / edited_message / channel_post). */
function parseMessage(value: unknown): TgWireMessage | null {
	if (!isRecord(value)) return null;
	const chatRaw = isRecord(value["chat"]) ? value["chat"] : null;
	const chatId = chatRaw !== null ? numericIdValue(chatRaw["id"]) : null;
	const messageId = idValue(value["message_id"]);
	if (chatId === null || messageId === null) return null;
	const chatType = optString(chatRaw?.["type"]);
	if (
		chatType !== "private" &&
		chatType !== "group" &&
		chatType !== "supergroup" &&
		chatType !== "channel"
	) {
		return null;
	}
	const chat: TgWireMessage["chat"] = { id: chatId, type: chatType };
	if (chatRaw?.["is_forum"] === true) chat.is_forum = true;
	const title = optString(chatRaw?.["title"]);
	if (title !== undefined) chat.title = title;
	const username = optString(chatRaw?.["username"]);
	if (username !== undefined) chat.username = username;

	const msg: TgWireMessage = { message_id: messageId, chat, date: 0 };
	const date = optNumber(value["date"]);
	if (date !== undefined) msg.date = date;
	const fromRaw = isRecord(value["from"]) ? value["from"] : null;
	if (fromRaw !== null) {
		const fromId = numericIdValue(fromRaw["id"]);
		if (fromId !== null) {
			const from: TgWireMessage["from"] = {
				id: fromId,
				is_bot: fromRaw["is_bot"] === true,
			};
			const firstName = optString(fromRaw["first_name"]);
			if (firstName !== undefined && from !== undefined) {
				from.first_name = firstName;
			}
			const fromUsername = optString(fromRaw["username"]);
			if (fromUsername !== undefined && from !== undefined) {
				from.username = fromUsername;
			}
			msg.from = from;
		}
	}
	const text = optString(value["text"]);
	if (text !== undefined) msg.text = text;
	const caption = optString(value["caption"]);
	if (caption !== undefined) msg.caption = caption;
	const threadId = optNumber(value["message_thread_id"]);
	if (threadId !== undefined) msg.message_thread_id = threadId;
	if (value["is_topic_message"] === true) msg.is_topic_message = true;
	const editDate = optNumber(value["edit_date"]);
	if (editDate !== undefined) msg.edit_date = editDate;
	return msg;
}

function parseCallbackQuery(
	value: unknown,
): TgWireUpdate["callback_query"] | null {
	if (!isRecord(value)) return null;
	const id = optString(value["id"]);
	const data = optString(value["data"]);
	const fromRaw = isRecord(value["from"]) ? value["from"] : null;
	const fromId = fromRaw !== null ? numericIdValue(fromRaw["id"]) : null;
	if (id === undefined || data === undefined || fromId === null) return null;
	const cbq: NonNullable<TgWireUpdate["callback_query"]> = {
		id,
		from: { id: fromId, is_bot: fromRaw?.["is_bot"] === true },
		data,
	};
	const firstName = optString(fromRaw?.["first_name"]);
	if (firstName !== undefined) cbq.from.first_name = firstName;
	const message = parseMessage(value["message"]);
	if (message !== null) cbq.message = message;
	return cbq;
}

function parseMessageReaction(
	value: unknown,
): TgWireUpdate["message_reaction"] | null {
	if (!isRecord(value)) return null;
	const messageId = idValue(value["message_id"]);
	if (messageId === null) return null;
	const mr: NonNullable<TgWireUpdate["message_reaction"]> = {
		message_id: messageId,
	};
	const chatRaw = isRecord(value["chat"]) ? value["chat"] : null;
	const chatId = chatRaw !== null ? idValue(chatRaw["id"]) : null;
	if (chatId !== null) mr.chat = { id: chatId };
	if (Array.isArray(value["new_reaction"])) {
		const items: Array<{ emoji?: string; custom_emoji_id?: number | string }> =
			[];
		for (const item of value["new_reaction"]) {
			if (!isRecord(item)) continue;
			const emoji = optString(item["emoji"]);
			const customId = idValue(item["custom_emoji_id"]);
			if (emoji === undefined && customId === null) continue;
			items.push({
				...(emoji !== undefined ? { emoji } : {}),
				...(customId !== null ? { custom_emoji_id: customId } : {}),
			});
		}
		mr.new_reaction = items;
	}
	return mr;
}

/**
 * Validate one raw Bot API update into the registry shape + engine view.
 * Unparseable updates are SKIPPED (the normalize-* family tolerates them
 * the same way) — but a valid update_id always yields a view so the engine
 * offset still advances past it (no poison redelivery).
 */
function parseUpdate(
	raw: unknown,
): { raw: TgWireUpdate; view: EngineUpdateView } | null {
	if (!isRecord(raw)) return null;
	const updateId = optNumber(raw["update_id"]);
	if (updateId === undefined) return null;
	const update: TgWireUpdate = { update_id: updateId };
	const message = parseMessage(raw["message"]);
	if (message !== null) update.message = message;
	const edited = parseMessage(raw["edited_message"]);
	if (edited !== null) update.edited_message = edited;
	const channelPost = parseMessage(raw["channel_post"]);
	if (channelPost !== null) update.channel_post = channelPost;
	const cbq = parseCallbackQuery(raw["callback_query"]);
	if (cbq !== null) update.callback_query = cbq;
	const mr = parseMessageReaction(raw["message_reaction"]);
	if (mr !== null) update.message_reaction = mr;

	if (message !== undefined && message !== null) {
		return {
			raw: update,
			view: {
				updateId,
				chatId: String(message.chat.id),
				senderId: String(message.from?.id ?? 0),
				text: message.text ?? "",
			},
		};
	}
	const chatId = cbq?.message?.chat.id ?? mr?.chat?.id ?? edited?.chat.id ?? 0;
	const senderId = reactionSender(raw["message_reaction"]) ?? 0;
	const fromId =
		cbq !== null && cbq !== undefined
			? String(cbq.from.id)
			: edited?.from?.id !== undefined
				? String(edited.from.id)
				: String(senderId);
	return {
		raw: update,
		view: { updateId, chatId: String(chatId), senderId: fromId, text: "" },
	};
}

/** Reaction actor: Bot API user/actor_chat ids (fake uses a test-only count). */
function reactionSender(value: unknown): number | string | null {
	if (!isRecord(value)) return null;
	const user = isRecord(value["user"]) ? value["user"] : null;
	const userId = user !== null ? idValue(user["id"]) : null;
	if (userId !== null) return userId;
	const actor = isRecord(value["actor_chat"]) ? value["actor_chat"] : null;
	const actorId = actor !== null ? idValue(actor["id"]) : null;
	return actorId;
}
