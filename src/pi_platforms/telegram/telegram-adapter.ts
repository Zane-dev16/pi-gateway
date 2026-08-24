// pi_platforms/telegram/telegram-adapter — THE TELEGRAM CENSUS PORT
// (DEC-024: first production adapter after the three references).
//
// Built ON the polling reference engine (roadmap §Phase 6 heuristic 2):
// offset-commit-before-enqueue, held-inbound redispatch, 409-conflict zombie
// eviction, heartbeat stuck-probe escalation, and FloodWait machinery are
// INHERITED from PollingAdapterCore — extended/configured, never fork-copied.
//
// Shape deltas ported from the READ-ONLY Hermes plugin
// (plugins/platforms/telegram/adapter.py), cited file:symbol:
//   ::_normalize_reaction_event / gateway/platforms/base.py:set_reaction_handler
//     → inbound message_reaction updates normalize + fan out (A2)
//   ::TelegramAdapter.on_processing_start / on_processing_complete
//     → reaction-ack lifecycle 👀 → 👍/👎, cleared on cancel (A1; opt-in)
//   ::send_typing / _record_typing_cooldown / _typing_in_cooldown /
//     base.py:_keep_typing (_typing_paused) / ::_message_thread_id_for_typing
//     → typing variant matrix incl. per-chat transient-failure cooldown,
//       approval-wait pause, forum-thread placement (A11)
//   ::_handle_sticker + gateway/sticker_cache.py → sticker description cache (M7)
//   ::REQUIRES_EDIT_FINALIZE (#25710) → MarkdownV2 conversion ONLY on the
//     finalize edit path; sends take structural-only conversion; native draft/
//     seal lanes stay RAW (DEC-034 parity); plain lanes never convert.
//   ::send inner loop "attempt %d/3" + ::edit_message flood split (≤5 s inline,
//     >5 s non-blocking `flood_control:<wait>`) → FloodWait honored per METHOD
//     CLASS with manifest-declared budgets (Q17).

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { EditOptions } from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ProcessingOutcome } from "../../pi_gateway/guards/index.js";
import {
	buildExecApprovalCallback,
	PLAIN_TEXT_FALLBACK_PREFIX,
	type CallbackAnswer,
	type PluginContext,
} from "../kit/index.js";
import {
	PollingAdapterCore,
	type PollingEngineDeps,
} from "../polling/polling-adapter.js";
import type { FakeTelegramServer } from "../polling/fake-server.js";

import {
	isValidChatAction,
	TELEGRAM_MAX_MESSAGE_UNITS,
	TELEGRAM_TYPING_COOLDOWN_DEFAULT_SECONDS,
	TELEGRAM_TYPING_COOLDOWN_MAX_SECONDS,
	TELEGRAM_TYPING_COOLDOWN_MIN_SECONDS,
	threadIdForTyping,
} from "./manifest.js";
import {
	isPlainLaneContent,
	toTelegramMarkdownV2,
	toTelegramMarkdownV2Full,
} from "./markdown-v2.js";
import {
	parseReactionsEnabled,
	reactionForOutcome,
	REACTION_IN_PROGRESS,
	normalizeMessageReactionUpdate,
	type NormalizedReactionEvent,
} from "./reactions.js";
import {
	buildAnimatedStickerInjection,
	buildStickerInjection,
	type StickerDescriptionCache,
	STICKER_VISION_PROMPT,
} from "./sticker-cache.js";
import type {
	TgWireCallbackQuery,
	TgWireMessage,
	TgWireSticker,
	TgWireUpdate,
	TelegramBotApiFake,
} from "./telegram-fake-server.js";

/** Typing-variant bookkeeping per chat (A11). */
export interface TypingVariant {
	action: string;
	statusText?: string | undefined;
	threadId?: string | undefined;
}

export interface TelegramAdapterDeps extends Omit<PollingEngineDeps, "wire"> {
	/** The REAL-shape fake Bot API server. */
	wire: TelegramBotApiFake;
	/**
	 * Scoped reader for OPTIONAL env (reactions gate). Distinct from the
	 * required-secret reader so enablement stays fail-closed (DEC-009).
	 */
	optionalEnvReader?: ((name: string) => string | undefined) | undefined;
	stickerCache?: StickerDescriptionCache | undefined;
	/**
	 * Vision seam for sticker description (fake in tests — NO network).
	 * Hermes: adapter.py:_handle_sticker analyzes then caches by file_unique_id.
	 */
	stickerVision?:
		| ((prompt: string, sticker: TgWireSticker) => Promise<string>)
		| undefined;
}

interface ResolvedCallbackAuditEntry {
	callbackQueryId: string;
	data: string;
	kind: CallbackAnswer["kind"];
}

export class TelegramAdapter extends PollingAdapterCore {
	/** The real-shape Bot API fake (control plane + raw wire registry). */
	readonly bot: TelegramBotApiFake;

	/** A1 gate — adapter.py:_reactions_enabled (opt-in, default off). */
	readonly reactionsEnabled: boolean;

	private readonly optionalEnvReader: (name: string) => string | undefined;
	private readonly stickerCache: StickerDescriptionCache | undefined;
	private readonly stickerVision:
		| ((prompt: string, sticker: TgWireSticker) => Promise<string>)
		| undefined;

	// ── A2 inbound-reaction fan-out ──────────────────────────────────────
	readonly reactionLog: NormalizedReactionEvent[] = [];
	/** Forum-signal doubling when a reaction carries NO thread id (A2). */
	readonly forumSignalLog: NormalizedReactionEvent[] = [];
	private reactionHandler: ((event: NormalizedReactionEvent) => void) | null =
		null;

	/** edited_message updates normalize to platform events, NEVER turns. */
	readonly editedLog: Array<{ chatId: string; messageId: string }> = [];

	/** Callback taps routed through the ONE query handler (§9.1 audit). */
	readonly callbackAudit: ResolvedCallbackAuditEntry[] = [];

	/** Monotonic approval ids (64-byte callback_data ⇒ ints, never uuids). */
	private approvalSeq = 1000;

	// ── A11 typing state ─────────────────────────────────────────────────
	private readonly typingVariants = new Map<string, TypingVariant>();
	private readonly pausedChats = new Set<string>();
	private readonly typingCooldownUntil = new Map<string, number>();
	readonly typingCooldownLog: Array<{ chatId: string; seconds: number }> = [];

	constructor(deps: TelegramAdapterDeps) {
		super({
			...deps,
			// The inherited engine consumes a STRUCTURAL SUBSET of the wire
			// (openSession/getUpdates/commitOffset/getMe/getWebhookInfo/
			// sendChatAction) — all provided by TelegramBotApiFake.
			wire: deps.wire as unknown as FakeTelegramServer,
		});
		this.bot = deps.wire;
		this.optionalEnvReader = deps.optionalEnvReader ?? (() => undefined);
		this.stickerCache = deps.stickerCache;
		this.stickerVision = deps.stickerVision;
		this.reactionsEnabled = parseReactionsEnabled(
			this.optionalEnvReader("TELEGRAM_REACTIONS"),
		);
		if (deps.scalarMaxUnits === undefined) {
			// Production default: manifest data (adapter.py MAX_MESSAGE_LENGTH
			// 4096 UTF-16 units). Conformance subjects override smaller.
			void this.overrideScalarMaxUnits(TELEGRAM_MAX_MESSAGE_UNITS);
		}
	}

	/** Test/subject seam for the scalar budget (core field is private). */
	private overrideScalarMaxUnits(units: number): void {
		(this as unknown as { scalarMaxUnits: number }).scalarMaxUnits = units;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Update-kind routing — THE update-object parsing delta (§8 shape row)
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * The inherited pipeline flattens every update to a text-shaped event;
	 * kind routing happens HERE against the server's raw registry keyed by
	 * IncomingEvent.messageId (= String(update_id)). Message-kind events
	 * (and harness-injected events without a registry entry) fall through to
	 * the standard guard lane.
	 */
	override async handleIngress(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		const raw =
			event.messageId !== undefined
				? this.bot.rawUpdateFor(event.messageId)
				: undefined;
		if (raw === undefined || raw.message !== undefined) {
			return super.handleIngress(event, sessionKey);
		}
		if (raw.callback_query !== undefined) {
			await this.routeCallbackQuery(raw.callback_query);
			return;
		}
		if (raw.message_reaction !== undefined) {
			this.fanOutReaction(raw);
			return;
		}
		if (raw.edited_message !== undefined) {
			this.editedLog.push({
				chatId: String(raw.edited_message.chat.id),
				messageId: String(raw.edited_message.message_id),
			});
			return;
		}
		// Unwired update kinds (channel_post, poll, …): tolerated no-op —
		// never a turn, never an error (adapter.py:_normalize_platform_event
		// returns None for types without a wired contract).
	}

	/**
	 * adapter.py:_handle_callback_query parity via the kit router: EVERY tap
	 * answers (spinner clears), resolutions strip the host keyboard, and no
	 * tap ever dispatches a turn.
	 */
	private async routeCallbackQuery(cbq: TgWireCallbackQuery): Promise<void> {
		let answer: CallbackAnswer;
		try {
			answer = await this.router.route(cbq.data, {
				userId: String(cbq.from?.id ?? ""),
				...(cbq.message !== undefined
					? { chatId: String(cbq.message.chat.id) }
					: {}),
			});
		} catch (err) {
			answer = {
				kind: "unknown",
				answerText: `⚠️ ${err instanceof Error ? err.message : String(err)}`,
				hostEdit: null,
			};
		}
		this.callbackAudit.push({
			callbackQueryId: cbq.id,
			data: cbq.data,
			kind: answer.kind,
		});
		await this.bot.answerCallbackQuery({
			callback_query_id: cbq.id,
			text: answer.answerText,
		});
		if (answer.kind === "resolved" && cbq.message !== undefined) {
			await this.bot.editMessageReplyMarkup({
				chat_id: cbq.message.chat.id,
				message_id: cbq.message.message_id,
				reply_markup: null,
			});
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// A1 — reaction-ack lifecycle
	// ══════════════════════════════════════════════════════════════════════

	/** adapter.py:on_processing_start — 👀 while processing runs. */
	async onProcessingStart(event: IncomingEvent): Promise<void> {
		await this.applyReaction(event, {
			kind: "set",
			emoji: REACTION_IN_PROGRESS,
		});
	}

	/** adapter.py:on_processing_complete — swap/clear determinism. */
	async onProcessingComplete(
		event: IncomingEvent,
		outcome: ProcessingOutcome,
	): Promise<void> {
		const action = reactionForOutcome(outcome);
		if (action !== null) await this.applyReaction(event, action);
	}

	/** Hooks NEVER break message flow (base.py:_run_processing_hook parity). */
	private async applyReaction(
		event: IncomingEvent,
		action: { kind: "set"; emoji: string } | { kind: "clear" },
	): Promise<void> {
		if (!this.reactionsEnabled) return;
		const chatId = event.source?.chatId;
		const messageId = event.messageId;
		if (!chatId || !messageId) return;
		try {
			await this.bot.setMessageReaction({
				chat_id: chatId,
				message_id: messageId,
				reaction: action.kind === "set" ? action.emoji : null,
			});
		} catch {
			// swallowed — reaction failures are debug-class
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// A2 — inbound reactions fan-out
	// ══════════════════════════════════════════════════════════════════════

	/** base.py:set_reaction_handler parity (ONE handler slot). */
	setReactionHandler(
		fn: ((event: NormalizedReactionEvent) => void) | null,
	): void {
		this.reactionHandler = fn;
	}

	private fanOutReaction(raw: TgWireUpdate): void {
		const normalized = normalizeMessageReactionUpdate(raw.message_reaction);
		if (normalized === null) return; // invalid shapes tolerated, never thrown
		this.reactionLog.push(normalized);
		// A2 forum doubling: reactions WITHOUT thread ids double as a forum
		// signal (topic-less activity marker).
		if (normalized.payload.threadId === undefined) {
			this.forumSignalLog.push(normalized);
		}
		if (this.reactionHandler !== null) {
			try {
				this.reactionHandler(normalized);
			} catch {
				// handler failures never break ingress
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// A11 — typing variants: action matrix, cooldown backoff, pause, threads
	// ══════════════════════════════════════════════════════════════════════

	setTypingVariant(chatId: string, variant: TypingVariant): void {
		this.typingVariants.set(chatId, variant);
	}

	pauseTypingForChat(chatId: string): void {
		this.pausedChats.add(chatId);
	}

	resumeTypingForChat(chatId: string): void {
		this.pausedChats.delete(chatId);
	}

	variantFor(chatId: string): TypingVariant | undefined {
		return this.typingVariants.get(chatId);
	}

	/** A11 thread-status text: the dynamic phrase carried alongside refresh
	 * bubbles for this chat ("" when none). */
	typingStatusTextFor(chatId: string): string {
		return this.typingVariants.get(chatId)?.statusText ?? "";
	}

	typingInCooldown(chatId: string): boolean {
		const until = this.typingCooldownUntil.get(chatId);
		if (until === undefined) return false;
		if (this.clock.nowMs() < until) return true;
		this.typingCooldownUntil.delete(chatId);
		return false;
	}

	/**
	 * adapter.py:_is_transient_typing_error port: retry_after present, 429 /
	 * ≥500 status, or transient markers in the blob.
	 */
	private isTransientTypingError(error: string, retryAfter?: number): boolean {
		if (retryAfter !== undefined && retryAfter !== null) return true;
		const blob = error.toLowerCase();
		return [
			"too many requests",
			"rate limit",
			"timed out",
			"timeout",
			"temporar",
		].some((marker) => blob.includes(marker));
	}

	/** adapter.py:_record_typing_cooldown — retry_after or default, clamped. */
	private recordTypingCooldown(chatId: string, retryAfter?: number): void {
		const raw =
			retryAfter !== undefined &&
			retryAfter !== null &&
			Number.isFinite(retryAfter)
				? Number(retryAfter)
				: TELEGRAM_TYPING_COOLDOWN_DEFAULT_SECONDS;
		const seconds = Math.max(
			TELEGRAM_TYPING_COOLDOWN_MIN_SECONDS,
			Math.min(TELEGRAM_TYPING_COOLDOWN_MAX_SECONDS, raw),
		);
		this.typingCooldownUntil.set(chatId, this.clock.nowMs() + seconds * 1000);
		this.typingCooldownLog.push({ chatId, seconds });
	}

	/**
	 * Send-site typing with the FULL telegram surface: variant-driven action
	 * matrix (setTypingVariant), forum-thread placement (typing PRESERVES
	 * General-topic id "1"), FloodWait honored once over the injected clock,
	 * cooldown suppression after transient failures, approval-wait pause.
	 */
	override async sendTyping(
		chatId: string,
		action?: string,
	): Promise<SendResult> {
		// base.py:_keep_typing _typing_paused parity: paused chats skip the
		// bubble so approval prompts are reachable (skip ≠ failure).
		if (this.pausedChats.has(chatId)) return { success: true };
		// adapter.py:_typing_in_cooldown parity: transient-failure backoff.
		if (this.typingInCooldown(chatId)) return { success: true };

		const requested =
			action !== undefined && isValidChatAction(action) ? action : undefined;
		const variant = this.typingVariants.get(chatId);
		const useAction = requested ?? variant?.action ?? "typing";
		const threadId = threadIdForTyping(variant?.threadId);

		let honoredOnce = false;
		for (;;) {
			const res = await this.bot.sendChatActionEx({
				chat_id: chatId,
				action: useAction,
				message_thread_id: threadId,
			});
			if (res.success) {
				this.typingCooldownUntil.delete(chatId);
				return res;
			}
			const ra = res.retryAfter ?? null;
			if (ra !== null && !honoredOnce) {
				honoredOnce = true;
				await this.clock.sleep(ra * 1000);
				continue;
			}
			if (this.isTransientTypingError(res.error ?? "", ra ?? undefined)) {
				this.recordTypingCooldown(chatId, ra ?? undefined);
			}
			return res;
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// M7 — sticker handling
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * adapter.py:_handle_sticker port: returns the injection text that
	 * replaces the sticker's event text. Animated/video stickers get the
	 * emoji-only injection WITHOUT analysis or caching; static stickers hit
	 * the cache first, then vision (injected seam), caching the result.
	 */
	async handleSticker(sticker: TgWireSticker): Promise<string> {
		const emoji = sticker.emoji ?? "";
		const setName = sticker.set_name ?? "";
		if (sticker.is_animated === true || sticker.is_video === true) {
			return buildAnimatedStickerInjection(emoji);
		}
		const cache = this.stickerCache;
		if (cache !== undefined) {
			const cached = await cache.getCachedDescription(sticker.file_unique_id);
			if (cached !== undefined) {
				return buildStickerInjection(
					cached.description,
					cached.emoji,
					cached.setName,
				);
			}
		}
		let description: string;
		try {
			if (this.stickerVision === undefined) throw new Error("no vision seam");
			description = await this.stickerVision(STICKER_VISION_PROMPT, sticker);
			if (cache !== undefined) {
				await cache.cacheStickerDescription(
					sticker.file_unique_id,
					description,
					emoji,
					setName,
				);
			}
		} catch {
			description =
				emoji !== "" ? `a sticker with emoji ${emoji}` : "a sticker";
		}
		return buildStickerInjection(description, emoji, setName);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Egress deltas — formatting lanes + interactive prompt
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * SEND lane: structural MarkdownV2 conversion + parse_mode stamp
	 * (formatting bound to the send path, §10.1). Plain lanes — the §6.1
	 * fallback body and explicit parse_mode "none" — ship RAW (parse_mode
	 * None). The shared-row forceFormattingError contract is preserved first.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		if (isPlainLaneContent(content, metadata["parse_mode"])) {
			return this.wireTransmitSend(chatId, content, metadata);
		}
		const converted = toTelegramMarkdownV2(content);
		return this.wireTransmitSend(chatId, converted, {
			...metadata,
			parse_mode: "MarkdownV2",
		});
	}

	/**
	 * EDIT lane: REQUIRES_EDIT_FINALIZE (#25710) — full MarkdownV2 conversion
	 * WITH punctuation escaping applies ONLY on finalize=true edits; mid-
	 * stream progressive edits stay RAW prefix-stable bytes. The inherited
	 * super.wireEdit owns the non-blocking flood_control surface.
	 */
	protected override async wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		const out =
			opts.finalize === true && !isPlainLaneContent(content, undefined)
				? toTelegramMarkdownV2Full(content)
				: content;
		return super.wireEdit(chatId, messageId, out, opts);
	}

	/**
	 * §11 step 7 interactive UX: exec-approval prompt sent THROUGH DOOR 1 with
	 * an inline keyboard whose buttons ride the kit grammar (64-byte cap).
	 * The monotonic approval id backs BOTH the pending store and every
	 * callback_data — round-trip proven by the conformance rows.
	 */
	async sendExecApprovalPrompt(
		chatId: string,
		sessionKey: string,
		promptText = "Approve execution?",
	): Promise<SendResult & { approvalId: number }> {
		const approvalId = ++this.approvalSeq;
		this.approvals.register(approvalId, sessionKey);
		const keyboard = {
			inline_keyboard: [["once", "session", "always", "deny"]].map((row) =>
				row.map((choice) => ({
					text: choice,
					callback_data: buildExecApprovalCallback(
						choice as "once" | "session" | "always" | "deny",
						approvalId,
					),
				})),
			),
		};
		const result = await this.send(chatId, promptText, undefined, {
			reply_markup: keyboard,
		});
		return { ...result, approvalId };
	}
}

// ── registration path (04 §4.2) ─────────────────────────────────────────────

/**
 * plugins/platforms/telegram/plugin.yaml + register(ctx) parity: register the
 * telegram platform against the kit PluginContext. Missing required secret ⇒
 * LOUD disable at registration (never silent skip).
 */
export function registerTelegramPlatform(
	ctx: PluginContext,
	factory: () => unknown,
): void {
	ctx.registerPlatform(TELEGRAM_MANIFEST_FOR_REGISTRATION, factory);
}

import { TELEGRAM_MANIFEST } from "./manifest.js";
const TELEGRAM_MANIFEST_FOR_REGISTRATION = TELEGRAM_MANIFEST;

/** Raw-update escape hatch for fixtures (typed re-export). */
export type { TgWireUpdate, TgWireMessage };
