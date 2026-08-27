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
//     → typing variants (status text + forum-thread placement; wire action
//     PINNED to "typing" per :8400/:8412, tg-10) incl. per-chat transient-
//       failure cooldown, approval-wait pause (A11)
//   ::_handle_sticker + gateway/sticker_cache.py → sticker description cache (M7)
//   ::REQUIRES_EDIT_FINALIZE (#25710) → full MarkdownV2 conversion on the
//     finalize edit path WITH parse_mode stamped (tg-3); the SEND lane emits
//     the SAME format_message-style conversion under parse_mode=MarkdownV2
//     (tg-2); native draft/seal lanes stay RAW (DEC-034 parity); plain lanes
//     never convert.
//   ::send inner loop "attempt %d/3" — flood waits ≤5 s sleep inline via the
//     injected clock, >5 s fail closed as `flood_control:<wait>` (#91969,
//     tg-8) + ::edit_message flood split → FloodWait honored per METHOD CLASS
//     with manifest-declared budgets (Q17).
//   ::_delete_webhook_best_effort → deleteWebhook(drop_pending_updates=false)
//     on EVERY connect: cold boot requires success, reconnect best-effort (tg-7).
//   ::_start_updater_with_progress start_polling(allowed_updates=ALL_TYPES)
//     on every poll (tg-1); ::_notification_kwargs important-mode silence
//     (tg-4); ::_thread_kwargs_for_send/_should_thread_reply thread+reply
//     anchors (tg-5/tg-6); ::send_image_file/... media family w/ fallbacks (tg-9).
//   ::_is_callback_user_authorized (:1171) — EVERY gated callback tap passes
//     the session-authz decision chain before resolution; unauthorized taps
//     answer ⛔ but NEVER resolve; empty clicker ids fail closed (#24457)
//     (tg-11 closure via isUserAuthorized + forced fixture override).
//   ::delete_message (:6064, openclaw#72038) — best-effort deleteMessage for
//     the stream consumer's fresh-final cleanup / stale-preview retraction;
//     failures are debug-class and never throw (tg-12 closure).
//   ::send_draft (:6116) legacy lane — draft frames are REAL sendMessageDraft
//     Bot API calls {chat_id, draft_id, text, parse_mode?, …thread kwargs},
//     MarkdownV2-first with one plain-text retry on BadRequest; rich drafts
//     ride sendRichMessageDraft (tg2-6) ahead of it (tg-13 closure).

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { REPLY_TO_METADATA_KEY } from "../../pi_gateway/streaming/adapter-seam.js";
import type { EditOptions } from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ProcessingOutcome } from "../../pi_gateway/guards/index.js";
import {
	buildExecApprovalCallback,
	chunkWithFenceCarry,
	codePointLen,
	extractRetryAfterSeconds,
	PLAIN_TEXT_FALLBACK_PREFIX,
	type CallbackAnswer,
	type CallbackTapContext,
	type PluginContext,
} from "../kit/index.js";
import {
	PollingAdapterCore,
	type PollingEngineDeps,
} from "../polling/polling-adapter.js";
import type { FakeTelegramServer } from "../polling/fake-server.js";

import {
	TELEGRAM_ALLOWED_UPDATES,
	TELEGRAM_BOT_API_MAX_COMMANDS,
	TELEGRAM_CHAT_ACTION,
	TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR,
	TELEGRAM_MAX_MESSAGE_UNITS,
	TELEGRAM_MAX_SEND_ATTEMPTS,
	TELEGRAM_MENU_MAX_COMMANDS,
	TELEGRAM_SEND_FLOOD_INLINE_WAIT_CAP_SECONDS,
	TELEGRAM_TYPING_COOLDOWN_DEFAULT_SECONDS,
	TELEGRAM_TYPING_COOLDOWN_MAX_SECONDS,
	TELEGRAM_TYPING_COOLDOWN_MIN_SECONDS,
	isPrivateDmTopicSend,
	metadataDirectMessagesTopicId,
	metadataReplyToMessageId,
	metadataThreadId,
	notificationKwargs,
	resolveTelegramNotificationsMode,
	threadIdForTyping,
	threadKwargsForSend,
	type TelegramNotificationsMode,
} from "./manifest.js";
import { isPlainLaneContent, toTelegramMarkdownV2Full } from "./markdown-v2.js";
import { normalizeTelegramChatId } from "./telegram-ids.js";
import { isRichEligibleContent } from "./rich-messages.js";
import {
	normalizeMessageEditedEvent,
	type NormalizedEditedEvent,
} from "./platform-events.js";
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
import { isUserAuthorized } from "../../pi_gateway/security/authz/index.js";
import { needsRichRendering, richMessagePayload } from "./rich-messages.js";
import type { DraftFrameArgs } from "../../pi_gateway/streaming/adapter-seam.js";
import { BUILTIN_COMMAND_ROWS } from "../../pi_gateway/commands/builtins.js";

/** Bot API command-menu row (set_my_commands payload). */
export interface TelegramMenuCommand {
	command: string;
	description: string;
}

/** adapter.py extra.dm_topics config rows (DM-topic housekeeping). */
export interface DmTopicConfigEntry {
	chatId: string | number;
	topics: Array<{
		name: string;
		threadId?: number | undefined;
		iconColor?: number | undefined;
		iconCustomEmojiId?: string | undefined;
	}>;
}

/** Typing bookkeeping per chat (A11): status text + forum-thread placement.
 * The WIRE ACTION is always "typing" (adapter.py:send_typing :8400/:8412 —
 * the only action the baseline adapter ever sends); no action variant exists
 * on this seam. */
export interface TypingVariant {
	statusText?: string | undefined;
	threadId?: string | undefined;
}

export interface TelegramAdapterDeps extends Omit<PollingEngineDeps, "wire"> {
	/** The REAL-shape fake Bot API server. */
	wire: TelegramBotApiFake;
	/**
	 * Scoped reader for OPTIONAL env (reactions/rich/link-preview/status
	 * gates). Distinct from the required-secret reader so enablement stays
	 * fail-closed (DEC-009).
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
	/**
	 * Command-menu source for set_my_commands (tg2-3). Defaults to the
	 * gateway builtin registry minus CLI-only rows — the hermes_cli
	 * COMMAND_REGISTRY analog (07 §1: menus derive from THE registry).
	 */
	menuCommands?: (() => readonly TelegramMenuCommand[]) | undefined;
	/** extra.dm_topics config (adapter.py:_dm_topics_config) for post-connect
	 * DM-topic setup. Empty by default — topics are opt-in config. */
	dmTopicsConfig?: readonly DmTopicConfigEntry[] | undefined;
}

/** Metadata key carrying the delivery chat id down the ladder rich seam. */
const TELEGRAM_RICH_LANE_CHAT_KEY = "__tg_rich_chat";

/**
 * Rich failure classifier (formatting-ladder.classifyRichFailure semantics;
 * adapter.py:_is_rich_capability_error/_fallback_error truth): capability =
 * latch once, fallback = degrade without latching, transient = never
 * legacy-resend.
 */
function richFailureClass(
	errorText: string,
): "capability" | "fallback" | "transient" {
	const blob = errorText.toLowerCase();
	if (
		blob.includes("method not found") ||
		blob.includes("endpoint not found") ||
		blob.includes("no such method") ||
		blob.includes("404") ||
		blob.includes("does not exist")
	) {
		return "capability";
	}
	if (
		blob.includes("bad request") ||
		blob.includes("unsupported") ||
		blob.includes("not implemented")
	) {
		return "fallback";
	}
	return "transient";
}

/**
 * tg-13 helper — adapter.py send_draft :6174-6177 trim: oversized draft text
 * is the FIRST CHUNK of THE chat's chunking resolution — literally
 * `truncate_message(content, MAX_MESSAGE_LENGTH, len_fn=utf16_len)[0]`
 * (previews are ephemeral, never split). Pi ports that exactly through the ONE
 * shared chunker (kit/chunking chunkWithFenceCarry over §6.3's per-chat pair:
 * utf16 × TELEGRAM_MAX_MESSAGE_UNITS), preserving its INDICATOR_RESERVE
 * budget, fence-carry scaffolding and the " (1/N)" label — no hand-rolled
 * trim. Upstream quirk kept byte-honest: the FIT CHECK is CODEPOINT-based
 * (`len(content) <= MAX`), only the SPLIT measures utf16 units.
 */

interface ResolvedCallbackAuditEntry {
	callbackQueryId: string;
	data: string;
	kind: CallbackAnswer["kind"];
}

/**
 * Mutable per-chunk send routing state (adapter.py send() per-chunk block):
 * the retry ladder mutates reply/thread anchors across attempts exactly like
 * Hermes reassigns reply_to_id / thread_kwargs inline.
 */
interface TelegramSendRouting {
	/** Private DM-topic lane — never falls out silently on anchor loss. */
	readonly privateDmTopicSend: boolean;
	/** Non-null ⇒ fail-loud BEFORE any transmission (missing DM-topic anchor). */
	readonly failLoud: SendResult | null;
	/** Current reply anchor; null once dropped by the not-found ladder. */
	readonly replyToId: number | null;
	/** Current message_thread_id / direct_messages_topic_id kwargs. */
	threadArgs(): Record<string, unknown>;
	effectiveThreadId(): number | undefined;
	commitConsumedAnchor(): void;
	dropReplyAnchor(): void;
	threadRetrySpent(): boolean;
	spendThreadRetry(): void;
	dropThreadAnchor(): void;
}

export class TelegramAdapter extends PollingAdapterCore {
	/** The real-shape Bot API fake (control plane + raw wire registry). */
	readonly bot: TelegramBotApiFake;

	/** A1 gate — adapter.py:_reactions_enabled (opt-in, default off). */
	readonly reactionsEnabled: boolean;

	/** tg2-6 gates — adapter.py:_rich_messages_enabled / _rich_drafts_enabled
	 * (extra opt-ins, BOTH default off; capability failures latch separately). */
	readonly richMessagesEnabled: boolean;
	readonly richDraftsEnabled: boolean;
	/** tg2-9 gate — adapter.py:_disable_link_previews extra (default off). */
	readonly disableLinkPreviews: boolean;
	/** tg2-3 status indicator — adapter.py:_status_indicator_* extras. */
	private readonly statusIndicatorEnabled: boolean;
	private readonly statusOnlineText: string;
	private readonly statusOfflineText: string;

	/**
	 * adapter.py:_notifications_mode ("important" default, :853): sends are
	 * silent unless metadata["notify"] marks them notify-worthy.
	 */
	readonly notificationsMode: TelegramNotificationsMode;

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
	/** tg2-11: normalized message_edited events, in fire order. */
	readonly editedEventLog: NormalizedEditedEvent[] = [];
	private editedMessageHandler:
		| ((event: NormalizedEditedEvent) => void)
		| null = null;

	/** Callback taps routed through the ONE query handler (§9.1 audit). */
	readonly callbackAudit: ResolvedCallbackAuditEntry[] = [];

	/**
	 * tg-11 forced authorization fixture. The inherited `setClickerAuthorization`
	 * switch stays the SHARED conformance-row control: once armed it FORCES the
	 * clicker verdict (false ⇒ always deny, true ⇒ always allow) exactly like
	 * every other polling engine. Production never arms it, so the real
	 * authorization chain below runs instead.
	 */
	private forcedClickAuthorization: boolean | undefined;

	/** Monotonic approval ids (64-byte callback_data ⇒ ints, never uuids). */
	private approvalSeq = 1000;

	/** tg2-6 capability latches (adapter.py:_rich_send_disabled /
	 * _rich_draft_disabled): set once per process by capability errors. */
	private richSendDisabled = false;
	private richDraftDisabled = false;

	// ── A11 typing state ─────────────────────────────────────────────────
	private readonly typingVariants = new Map<string, TypingVariant>();
	private readonly pausedChats = new Set<string>();
	private readonly typingCooldownUntil = new Map<string, number>();
	readonly typingCooldownLog: Array<{ chatId: string; seconds: number }> = [];

	// ── tg2-3 post-connect housekeeping state ────────────────────────────
	private housekeepingInFlight = false;
	/** Forum chats whose BotCommandScopeChat menu is registered (lazy). */
	private readonly forumCommandsRegistered = new Set<string>();
	private forumCommandChain: Promise<void> = Promise.resolve();
	/** chat:name → message_thread_id cache (adapter.py:_dm_topics). */
	private readonly dmTopics = new Map<string, number>();
	private readonly dmTopicsConfig: readonly DmTopicConfigEntry[];
	private readonly menuCommandsProvider:
		| (() => readonly TelegramMenuCommand[])
		| undefined;

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
		this.notificationsMode = resolveTelegramNotificationsMode(
			this.optionalEnvReader("HERMES_TELEGRAM_NOTIFICATIONS"),
		);
		this.stickerCache = deps.stickerCache;
		this.stickerVision = deps.stickerVision;
		this.reactionsEnabled = parseReactionsEnabled(
			this.optionalEnvReader("TELEGRAM_REACTIONS"),
		);
		// tg2-6 (adapter.py :688/:696): rich_messages/rich_drafts are extra
		// opt-ins, default OFF — current clients make rich messages hard to
		// copy as plain text, so the legacy MarkdownV2 path stays the default.
		this.richMessagesEnabled = this.boolExtra("TELEGRAM_RICH_MESSAGES");
		this.richDraftsEnabled = this.boolExtra("TELEGRAM_RICH_DRAFTS");
		// tg2-9 (adapter.py :677): disable_link_previews extra.
		this.disableLinkPreviews = this.boolExtra("TELEGRAM_DISABLE_LINK_PREVIEWS");
		// tg2-3 status indicator (adapter.py :811): opt-in — it mutates the
		// bot's GLOBAL profile; custom online/offline strings ride env.
		this.statusIndicatorEnabled = this.boolExtra("TELEGRAM_STATUS_INDICATOR");
		this.statusOnlineText = (
			this.optionalEnvReader("TELEGRAM_STATUS_ONLINE") ?? "Online"
		).slice(0, 120);
		this.statusOfflineText = (
			this.optionalEnvReader("TELEGRAM_STATUS_OFFLINE") ?? "Offline"
		).slice(0, 120);
		this.dmTopicsConfig = deps.dmTopicsConfig ?? [];
		this.menuCommandsProvider = deps.menuCommands;
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

	/** Extra-gate coercion (_coerce_bool_extra analog over scoped env). */
	private boolExtra(name: string): boolean {
		const raw = this.optionalEnvReader(name);
		if (raw === undefined || raw === "") return false;
		return !["false", "0", "no", "off"].includes(raw.toLowerCase());
	}

	// ══════════════════════════════════════════════════════════════════
	// Connect bootstrap — webhook clear (tg-7)
	// ══════════════════════════════════════════════════════════════════

	/**
	 * adapter.py:_delete_webhook_best_effort parity: EVERY connect clears any
	 * stale webhook BEFORE polling starts — a live webhook makes getUpdates
	 * fail permanently ("can't use getUpdates while webhook is active") until
	 * the conflict ladder burns out. COLD BOOT requires success (a network
	 * failure aborts connect loudly so the runner can retry with a fresh
	 * adapter); RECONNECTS are best-effort (polling recovery can heal a
	 * transient error). drop_pending_updates stays FALSE either way.
	 */
	override async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		await this.deleteWebhookBestEffort({ requireSuccess: !opts.isReconnect });
		const ok = await super.connect(opts);
		if (ok) this.kickPostConnectHousekeeping();
		return ok;
	}

	/**
	 * adapter.py :5172-5184 clean-shutdown parity: mark the bot "Offline" in
	 * its short description while the HTTP client is still alive (opt-in
	 * extra.status_indicator; best-effort, non-fatal — a hard crash leaves the
	 * last-known status, which is the expected limitation of a profile-text
	 * indicator).
	 */
	override async disconnect(): Promise<void> {
		if (this.statusIndicatorEnabled && this.connected) {
			try {
				await this.bot.setMyShortDescription({
					short_description: this.statusOfflineText,
				});
			} catch {
				// indicator failures are debug-class (:4961)
			}
		}
		await super.disconnect();
	}

	// ════════════════════════════════════════════════════════════════
	// Post-connect housekeeping (tg2-3; adapter.py:_start_post_connect_
	// housekeeping :4078 / _run_post_connect_housekeeping :4110)
	// ════════════════════════════════════════════════════════════════

	/**
	 * Kick deferred post-connect housekeeping OFF the connect path so a slow
	 * Bot API call cannot blow the gateway connect timeout (#46298).
	 * Idempotent: an in-flight housekeeping task is left in place rather than
	 * double-scheduled. Every step is non-fatal.
	 */
	private kickPostConnectHousekeeping(): void {
		if (this.housekeepingInFlight) return;
		this.housekeepingInFlight = true;
		void this.runPostConnectHousekeeping()
			.catch(() => undefined)
			.finally(() => {
				this.housekeepingInFlight = false;
			});
	}

	private async runPostConnectHousekeeping(): Promise<void> {
		// Command menu for all three scopes — Telegram picks the narrowest
		// matching scope per chat type (forum topics fall through to
		// AllGroupChats or Default). Each scope registers INDEPENDENTLY;
		// a scope failure never blocks the others (:4110 parity).
		try {
			const commands = this.menuCommands();
			if (commands.length > 0) {
				for (const scope of [
					{ type: "default" },
					{ type: "all_private_chats" },
					{ type: "all_group_chats" },
				] as const) {
					try {
						await this.bot.setMyCommands({ commands, scope });
					} catch {
						// per-scope failure tolerated (logged in Hermes)
					}
				}
			}
		} catch {
			// menu derivation failure never breaks connect
		}

		// Status indicator (opt-in extra) — non-fatal (:4953).
		if (this.statusIndicatorEnabled) {
			try {
				await this.bot.setMyShortDescription({
					short_description: this.statusOnlineText,
				});
			} catch {
				// swallowed — indicator failures are debug-class
			}
		}

		// DM topics (Bot API 9.4) — runs post-connect so createForumTopic can
		// fire; failures are non-fatal (:3759/:3873 family).
		try {
			await this.setupDmTopics();
		} catch {
			// non-fatal
		}
	}

	/**
	 * Menu command rows for set_my_commands, capped at
	 * TELEGRAM_MENU_MAX_COMMANDS (60 default; Bot API hard cap 100). Defaults
	 * to the gateway builtin registry minus CLI-only rows — Telegram users
	 * cannot reach terminal-only commands, so they never ship in the menu.
	 */
	private menuCommands(): TelegramMenuCommand[] {
		const source =
			this.menuCommandsProvider ?? (() => defaultTelegramMenuCommands());
		return source()
			.slice(
				0,
				Math.min(TELEGRAM_MENU_MAX_COMMANDS, TELEGRAM_BOT_API_MAX_COMMANDS),
			)
			.map((c) => ({
				command: c.command.replace(/-/g, "_"),
				description: c.description,
			}));
	}

	/** adapter.py:_ensure_forum_commands :9645 — forum supergroups don't
	 * inherit AllGroupChats; register BotCommandScopeChat(chat_id) lazily on
	 * first message so the command menu works inside topic views. Serialized
	 * per adapter via a promise chain (the _forum_lock analog); every chat
	 * registers at most once. */
	async ensureForumCommands(message: TgWireMessage): Promise<void> {
		const chat = message.chat;
		if (chat?.is_forum !== true) return;
		const chatId = String(chat.id);
		if (this.forumCommandsRegistered.has(chatId)) return;
		const run = this.forumCommandChain.then(async () => {
			if (this.forumCommandsRegistered.has(chatId)) return;
			const commands = this.menuCommands();
			if (commands.length === 0) return;
			await this.bot.setMyCommands({
				commands,
				scope: { type: "chat", chat_id: normalizeTelegramChatId(chatId) },
			});
			this.forumCommandsRegistered.add(chatId);
		});
		this.forumCommandChain = run.catch(() => undefined);
		await run.catch(() => undefined);
	}

	/**
	 * adapter.py:_create_dm_topic :3759 — createForumTopic in a private chat
	 * (Bot API 9.4). Returns the thread id, or null on failure. Duplicate /
	 * forums-disabled error classes are classified and tolerated.
	 */
	private async createDmTopic(
		chatId: number | string,
		name: string,
		iconColor?: number | undefined,
		iconCustomEmojiId?: string | undefined,
	): Promise<number | null> {
		try {
			const topic = await this.bot.createForumTopic({
				chat_id: normalizeTelegramChatId(chatId),
				name,
				...(iconColor !== undefined ? { icon_color: iconColor } : {}),
				...(iconCustomEmojiId !== undefined
					? { icon_custom_emoji_id: iconCustomEmojiId }
					: {}),
			});
			return topic.message_thread_id;
		} catch {
			// duplicate topic / forums-disabled / other — tolerated (null)
			return null;
		}
	}

	/**
	 * adapter.py:ensure_dm_topic — resolve a DM-topic thread id by name,
	 * loading persisted ids from config and creating + caching new ones.
	 */
	async ensureDmTopic(
		chatId: string | number,
		topicName: string,
		forceCreate = false,
	): Promise<string | null> {
		const name = String(topicName ?? "").trim();
		if (!name) return null;
		const cacheKey = `${String(chatId)}:${name}`;
		const cached = this.dmTopics.get(cacheKey);
		if (cached !== undefined && !forceCreate) return String(cached);

		let entryTopics: DmTopicConfigEntry["topics"] | undefined;
		let chatEntry: DmTopicConfigEntry | undefined;
		for (const entry of this.dmTopicsConfig) {
			if (String(entry.chatId) !== String(chatId)) continue;
			chatEntry = entry;
			entryTopics = entry.topics;
			break;
		}
		const conf = entryTopics?.find((t) => t.name === name);
		if (conf?.threadId !== undefined && !forceCreate) {
			this.dmTopics.set(cacheKey, conf.threadId);
			return String(conf.threadId);
		}
		const threadId = await this.createDmTopic(
			chatId,
			name,
			conf?.iconColor,
			conf?.iconCustomEmojiId,
		);
		if (threadId === null) return null;
		void chatEntry;
		this.dmTopics.set(cacheKey, threadId);
		return String(threadId);
	}

	/** adapter.py:rename_dm_topic :3873 — edit_forum_topic rename. */
	async renameDmTopic(
		chatId: string | number,
		threadId: number,
		name: string,
	): Promise<void> {
		await this.bot.editForumTopic({
			chat_id: normalizeTelegramChatId(chatId),
			message_thread_id: threadId,
			name,
		});
	}

	/** adapter.py:_setup_dm_topics :3957 — load or create configured topics.
	 * Persisted thread_ids load into the cache WITHOUT an API call; only
	 * missing ones hit createForumTopic. */
	private async setupDmTopics(): Promise<void> {
		for (const entry of this.dmTopicsConfig) {
			if (!entry.chatId || !entry.topics) continue;
			for (const topicConf of entry.topics) {
				if (!topicConf.name) continue;
				if (topicConf.threadId !== undefined) {
					this.dmTopics.set(
						`${String(entry.chatId)}:${topicConf.name}`,
						topicConf.threadId,
					);
					continue;
				}
				await this.ensureDmTopic(entry.chatId, topicConf.name);
			}
		}
	}

	/**
	 * adapter.py:_prune_stale_dm_topic_binding :1657 — drop cached bindings
	 * for a topic Telegram has confirmed deleted (#31501: without the prune,
	 * future sends keep steering into the dead thread). Best-effort, never
	 * raises from a send-fallback path.
	 */
	private pruneStaleDmTopicBinding(
		chatId: string | number,
		threadId: number | string,
	): void {
		const prefix = `${String(chatId)}:`;
		for (const [key, value] of [...this.dmTopics]) {
			if (key.startsWith(prefix) && String(value) === String(threadId)) {
				this.dmTopics.delete(key);
			}
		}
	}

	private async deleteWebhookBestEffort(opts: {
		requireSuccess: boolean;
	}): Promise<void> {
		try {
			await this.bot.deleteWebhook({ drop_pending_updates: false });
		} catch (err) {
			if (opts.requireSuccess) {
				throw new Error(
					`Telegram deleteWebhook did not complete during initial connect: ${err instanceof Error ? err.message : String(err)}`,
				);
			}
			// Best-effort reconnect: continue to polling so getUpdates/retry
			// can recover (adapter.py logs + degrades instead of failing).
		}
	}

	/** tg-1 wire parity: EVERY poll requests Update.ALL_TYPES. */
	protected override allowedUpdatesForPoll(): readonly string[] {
		return TELEGRAM_ALLOWED_UPDATES;
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
		if (raw?.message !== undefined) {
			// tg2-3 (_ensure_forum_commands): first message from a forum topic
			// lazily registers BotCommandScopeChat(chat_id). Non-fatal.
			await this.ensureForumCommands(raw.message);
			return super.handleIngress(event, sessionKey);
		}
		if (raw === undefined) {
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
			// tg2-11 (adapter.py:_normalize_message_edited_event :4284): edits
			// normalize to a message_edited platform event fanned out through
			// the SAME handler seam as reactions — never a turn, never an
			// error. The legacy log entry stays for observability.
			this.editedLog.push({
				chatId: String(raw.edited_message.chat.id),
				messageId: String(raw.edited_message.message_id),
			});
			const normalized = normalizeMessageEditedEvent(raw.edited_message);
			if (normalized !== null) this.fanOutEditedEvent(normalized);
			return;
		}
		// Unwired update kinds (channel_post, poll, …): tolerated no-op —
		// never a turn, never an error (adapter.py:_normalize_platform_event
		// returns None for types without a wired contract).
	}

	setClickerAuthorization(allow: boolean): void {
		this.forcedClickAuthorization = allow;
		super.setClickerAuthorization(allow);
	}

	/**
	 * tg-11 closure — adapter.py:_is_callback_user_authorized (:1171) parity:
	 * NO callback tap resolves without passing the session authorization chain,
	 * and an unauthorized tap is answered ⛔ (router-side) but NEVER resolved.
	 *
	 * 1. Empty user id ⇒ DENY unconditionally (:1177 — #24457 fail-closed).
	 * 2. Session-runner auth first: Hermes builds a SessionSource from the
	 * callback snapshot (chat_id = host chat ?? user; chat_type mapped:
	 * private→dm, supergroup→forum when a thread id is present else group) and
	 * calls the runner's `_is_user_authorized` (:1183). Pi's port of THAT
	 * chain is security/authz `isUserAuthorized`, invoked here with
	 * platform="telegram" so TELEGRAM_ALLOWED_USERS / TELEGRAM_GROUP_ALLOWED_* /
	 * TELEGRAM_ALLOW_ALL_USERS / pairing grants / GATEWAY_ALLOW_ALL_USERS all
	 * authorize exactly like message ingress on this platform.
	 * 3. The adapter's env-only fallback (:1216 `_scoped_gate_env`) is SUBSUMED
	 * by the full chain — its allowlist + allow-all branches are gates 9/5/8
	 * there, "*" wildcard included.
	 */
	protected override authorizeCallbackClicker(
		tap: CallbackTapContext,
	): boolean {
		if (this.forcedClickAuthorization !== undefined) {
			return this.forcedClickAuthorization;
		}
		const userId = String(tap.userId ?? "").trim();
		if (userId === "") return false;
		// Chat-type mapping mirrors the SessionSource construction :1192.
		const rawType = String(tap.chatType ?? "")
			.trim()
			.toLowerCase();
		let chatType = rawType === "" ? "dm" : rawType;
		if (chatType === "private") chatType = "dm";
		else if (chatType === "supergroup") {
			chatType =
				tap.threadId !== undefined && tap.threadId !== "" ? "forum" : "group";
		}
		const record = isUserAuthorized({
			platform: "telegram",
			userId,
			chatId:
				tap.chatId !== undefined && tap.chatId !== "" ? tap.chatId : userId,
			userName: tap.userName,
			...(chatType !== "" ? { chatType } : {}),
		});
		return record.allowed;
	}

	/**
	 * adapter.py:_handle_callback_query parity via the kit router: EVERY tap
	 * answers (spinner clears), resolutions strip the host keyboard, and no
	 * tap ever dispatches a turn. The tap context carries the FULL
	 * authorization source shape (`_is_callback_user_authorized` :1192 builds
	 * its SessionSource from chat/thread/user snapshot of the callback query)
	 * — the router forwards it untouched to this adapter's authorizer.
	 */
	private async routeCallbackQuery(cbq: TgWireCallbackQuery): Promise<void> {
		let answer: CallbackAnswer;
		try {
			answer = await this.router.route(cbq.data, {
				userId: String(cbq.from?.id ?? ""),
				...(cbq.message !== undefined
					? { chatId: String(cbq.message.chat.id) }
					: {}),
				...(cbq.message !== undefined
					? { chatType: String(cbq.message.chat.type ?? "") }
					: {}),
				...(cbq.message?.message_thread_id !== undefined
					? { threadId: String(cbq.message.message_thread_id) }
					: {}),
				...(cbq.from?.first_name !== undefined
					? { userName: cbq.from.first_name }
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
			// tg2-2 (adapter.py:_handle_callback_query :7280 / choice-picker
			// :6687): resolved taps edit the host message TEXT with the
			// resolution label — format_message-style MarkdownV2 under
			// parse_mode=MarkdownV2 with reply_markup REMOVED — so the
			// approval/choice outcome lands in chat, not just the spinner.
			try {
				await this.bot.editMessageText({
					chat_id: normalizeTelegramChatId(cbq.message.chat.id),
					message_id: numericOrRaw(cbq.message.message_id),
					text: toTelegramMarkdownV2Full(answer.hostEdit.text),
					parse_mode: "MarkdownV2",
					reply_markup: null,
				});
			} catch {
				// non-fatal if edit fails (Hermes swallows this too)
			}
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
				chat_id: normalizeTelegramChatId(chatId),
				message_id: numericId(messageId),
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
	// A11 — typing: status text/threads per chat, cooldown backoff, pause
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * tg2-11: ONE handler slot for message_edited events (the reaction
	 * fan-out seam analog); handler failures never break ingress.
	 */
	setEditedMessageHandler(
		fn: ((event: NormalizedEditedEvent) => void) | null,
	): void {
		this.editedMessageHandler = fn;
	}

	private fanOutEditedEvent(normalized: NormalizedEditedEvent): void {
		this.editedEventLog.push(normalized);
		if (this.editedMessageHandler !== null) {
			try {
				this.editedMessageHandler(normalized);
			} catch {
				// handler failures never break ingress
			}
		}
	}

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
	 * Send-site typing (A11) — Hermes truth: the wire action is ALWAYS
	 * "typing" (adapter.py:send_typing :8400/:8412); variants carry STATUS
	 * TEXT and forum-thread placement only (typing PRESERVES General-topic id
	 * "1"), never a different action string. FloodWait honored once over the
	 * injected clock; cooldown suppression after transient failures;
	 * approval-wait pause.
	 */
	override async sendTyping(
		chatId: string,
		_action?: string,
	): Promise<SendResult> {
		// base.py:_keep_typing _typing_paused parity: paused chats skip the
		// bubble so approval prompts are reachable (skip ≠ failure).
		if (this.pausedChats.has(chatId)) return { success: true };
		// adapter.py:_typing_in_cooldown parity: transient-failure backoff.
		if (this.typingInCooldown(chatId)) return { success: true };

		const variant = this.typingVariants.get(chatId);
		const threadId = threadIdForTyping(variant?.threadId);

		let honoredOnce = false;
		for (;;) {
			const res = await this.bot.sendChatActionEx({
				chat_id: normalizeTelegramChatId(chatId),
				action: TELEGRAM_CHAT_ACTION,
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

	// ════════════════════════════════════════════════════════════════
	// Egress deltas — formatting lanes + wire arg sets + media family
	// ════════════════════════════════════════════════════════════════

	/** Reply anchors already spent on this chat (tg-5 'first' policy). */
	private readonly consumedReplyAnchors = new Map<string, string>();

	/**
	 * SEND lane (tg-2/tg-4/tg-5/tg-6): the transmitted text is the FULL
	 * format_message-style conversion — structural markdown collapsed AND
	 * every remaining MarkdownV2 special escaped (chunk markers "(1/2)" ship
	 * as "\\(1/2\\)") — always matching the parse_mode=MarkdownV2 stamp.
	 * Plain lanes (§6.1 fallback body / explicit parse_mode "none") ship RAW
	 * with no parse_mode. The shared-row forceFormattingError contract is
	 * preserved first.
	 *
	 * The Bot API ARG SET rides the metadata channel so conformance capture
	 * sees exactly what production would transmit: parse_mode,
	 * disable_notification (important-mode default, notify-metadata override),
	 * message_thread_id (threadIdForSend mapping), reply_to_message_id
	 * ('first'-mode anchor). FloodWait follows the send inner loop: ≤5 s waits
	 * sleep inline via the injected clock and retry (≤3 attempts); larger
	 * waits fail closed with error="flood_control:<wait>" WITHOUT sleeping
	 * (#91969 — a verbatim sleep pinned the gateway 97 minutes).
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
		const plain = isPlainLaneContent(content, metadata["parse_mode"]);
		const text = plain ? content : toTelegramMarkdownV2Full(content);

		// Per-chunk routing resolved ONCE (adapter.py send() index block):
		// the retry ladder below MUTATES it across attempts within this chunk
		// exactly like Hermes mutates reply_to_id/thread_kwargs inline.
		const routing = this.computeSendRouting(chatId, metadata);
		if (routing.failLoud !== null) return routing.failLoud;

		for (let attempt = 0; ; attempt++) {
			// The BUILT ARG SET enumerates EXACTLY what Hermes' send() puts on
			// the wire (adapter.py :5397 send_message kwargs): chat_id (tg2-7
			// normalized), text, parse_mode, thread/topic kwargs, notification
			// kwargs, link-preview kwargs, and the reply anchor. The captured
			// metadata IS that arg set plus an explicit reply_markup — nothing
			// else. Input-namespace metadata (notify / expect_edits /
			// gateway_session_key / raw thread_id strings / final /
			// replyToOverride stamps) can never leak into the payload (tg2-5).
			const args: Record<string, unknown> = {
				chat_id: normalizeTelegramChatId(chatId),
				text,
				...routing.threadArgs(),
				...(routing.replyToId !== null
					? { reply_to_message_id: routing.replyToId }
					: {}),
				...notificationKwargs(this.notificationsMode, metadata),
				...this.linkPreviewKwargs(),
			};
			if (!plain) args["parse_mode"] = "MarkdownV2";
			if (metadata["reply_markup"] !== undefined) {
				args["reply_markup"] = metadata["reply_markup"];
			}

			const res = await this.wireTransmitSend(chatId, text, { ...args });
			if (res.success) {
				routing.commitConsumedAnchor();
				return res;
			}
			const blob = (res.error ?? "").toLowerCase();

			// Hermes BadRequest parity: the reply target was deleted before we
			// could answer — drop the anchor and retry the SAME chunk without it
			// (adapter.py::send "message to be replied not found" branch). A
			// private DM-topic send refuses instead of leaving its lane.
			if (
				routing.replyToId !== null &&
				blob.includes("message to be replied not found")
			) {
				if (routing.privateDmTopicSend) return res;
				routing.dropReplyAnchor();
				continue;
			}

			// Thread-not-found ladder (#31501/#27937): one same-thread retry
			// without sleeping, then retry WITHOUT the thread anchor and prune
			// the stale binding so future sends stop steering into the dead
			// topic. Private/created DM-topic lanes never fall out silently.
			const effectiveThreadId = routing.effectiveThreadId();
			if (
				effectiveThreadId !== undefined &&
				blob.includes("thread not found")
			) {
				if (
					routing.privateDmTopicSend ||
					metadata["telegram_dm_topic_created_for_send"] === true
				) {
					return res;
				}
				if (!routing.threadRetrySpent()) {
					routing.spendThreadRetry();
					continue;
				}
				this.pruneStaleDmTopicBinding(chatId, effectiveThreadId);
				routing.dropThreadAnchor();
				continue;
			}

			const ra =
				res.retryAfter !== undefined && res.retryAfter !== null
					? res.retryAfter
					: extractRetryAfterSeconds(res.error);
			if (ra !== null) {
				if (
					attempt < TELEGRAM_MAX_SEND_ATTEMPTS - 1 &&
					ra <= TELEGRAM_SEND_FLOOD_INLINE_WAIT_CAP_SECONDS
				) {
					await this.clock.sleep(ra * 1000);
					continue;
				}
				// Over-cap flood: fail closed WITHOUT sleeping. The wait lives in
				// the error string only — a machine-readable retryAfter here would
				// make the generic §6.1 ladder re-sleep verbatim (#91969).
				return { success: false, error: `flood_control:${ra}` };
			}
			return res; // non-flood failure — caller lanes own it
		}
	}

	/**
	 * Per-chunk send routing (tg2-4): adapter.py:_compute_single_send_routing
	 * / _is_private_dm_topic_send / _thread_kwargs_for_send / :1552 family.
	 * Resolves the reply anchor ('first'-chunk policy per anchor id; metadata
	 * telegram_reply_to_message_id anchors private DM-topic sends), the
	 * message_thread_id / direct_messages_topic_id kwargs, and the DM-topic
	 * fail-loud gate ("refusing to send outside the requested topic").
	 */
	private computeSendRouting(
		chatId: string,
		metadata: Metadata,
	): TelegramSendRouting {
		const threadRaw = metadataThreadId(metadata);
		const metaReplyTo = metadataReplyToMessageId(metadata);
		const privateDmTopicSend = isPrivateDmTopicSend(threadRaw, metadata);

		// Reply anchor: explicit kit anchor wins; private sends fall back to
		// the metadata telegram_reply_to_message_id (adapter.py
		// _reply_to_source parity inside send()'s per-chunk block).
		const explicitRaw = metadata[REPLY_TO_METADATA_KEY];
		let replyToSource: string | null = null;
		if (
			(typeof explicitRaw === "string" || typeof explicitRaw === "number") &&
			String(explicitRaw) !== ""
		) {
			replyToSource = String(explicitRaw);
		} else if (privateDmTopicSend && metaReplyTo !== null) {
			replyToSource = String(metaReplyTo);
		}

		// should_thread: private sends attach their anchor EVERY chunk (the
		// topic lane identity); regular sends use the 'first'-chunk policy —
		// tracked PER ANCHOR ID since the kit owns chunking.
		const alreadyConsumed =
			!privateDmTopicSend &&
			replyToSource !== null &&
			this.consumedReplyAnchors.get(chatId) === replyToSource;
		let anchored: number | null =
			replyToSource !== null && !alreadyConsumed ? Number(replyToSource) : null;
		if (anchored !== null && !Number.isFinite(anchored)) anchored = null;

		const kwargs = threadKwargsForSend(threadRaw, metadata, anchored);

		// DM-topic fail-loud: refuse to transmit outside the requested topic.
		const failLoud: SendResult | null =
			privateDmTopicSend &&
			anchored === null &&
			kwargs.directMessagesTopicId === undefined
				? {
						success: false,
						error: TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR,
					}
				: null;

		let anchorDropped = false;
		let threadDropped = false;
		let threadRetrySpent = false;
		const consumed = this.consumedReplyAnchors;

		return {
			privateDmTopicSend,
			failLoud,
			get replyToId(): number | null {
				return anchorDropped ? null : anchored;
			},
			threadArgs(): Record<string, unknown> {
				const out: Record<string, unknown> = {};
				if (!threadDropped) {
					if (
						kwargs.messageThreadId !== null &&
						kwargs.messageThreadId !== undefined
					) {
						out["message_thread_id"] = kwargs.messageThreadId;
					}
					if (kwargs.directMessagesTopicId !== undefined) {
						out["direct_messages_topic_id"] = kwargs.directMessagesTopicId;
					}
				}
				return out;
			},
			effectiveThreadId(): number | undefined {
				if (threadDropped) return undefined;
				return typeof kwargs.messageThreadId === "number"
					? kwargs.messageThreadId
					: undefined;
			},
			commitConsumedAnchor(): void {
				// 'first'-chunk policy bookkeeping: this anchor id counts as
				// spent for later chunks of the same delivery.
				if (!privateDmTopicSend && !anchorDropped && replyToSource !== null) {
					consumed.set(chatId, replyToSource);
				}
			},
			dropReplyAnchor(): void {
				anchorDropped = true;
			},
			threadRetrySpent(): boolean {
				return threadRetrySpent;
			},
			spendThreadRetry(): void {
				threadRetrySpent = true;
			},
			dropThreadAnchor(): void {
				threadDropped = true;
			},
		};
	}

	/** tg2-9 (_link_preview_kwargs :1945): extra-gated suppression kwarg. */
	private linkPreviewKwargs(): Record<string, unknown> {
		if (!this.disableLinkPreviews) return {};
		return { link_preview_options: { is_disabled: true } };
	}

	/**
	 * EDIT lane (tg-3): REQUIRES_EDIT_FINALIZE (#25710) — full format_message-
	 * style MarkdownV2 conversion WITH punctuation escaping applies ONLY on
	 * finalize=true edits, which ALSO stamp parse_mode=MarkdownV2 into the
	 * edit kwargs (adapter.py:edit_message finalize branch); mid-stream
	 * progressive edits stay RAW prefix-stable bytes with NO parse_mode. The
	 * inherited super.wireEdit owns the non-blocking flood_control surface.
	 */
	protected override async wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		if (opts.finalize === true && !isPlainLaneContent(content, undefined)) {
			// tg2-6 (adapter.py:_try_edit_rich :2336): rich finalize FIRST when
			// the extra is on — editMessageText carries a rich_message param.
			// Fallback-class failures degrade to the legacy MarkdownV2 edit;
			// transient failures NEVER legacy-resend (duplicate-edit risk).
			const rich = await this.tryEditRich(chatId, messageId, content);
			if (rich !== null) return rich;
			return this.notModifiedNoOp(
				await super.wireEdit(
					chatId,
					messageId,
					toTelegramMarkdownV2Full(content),
					{
						...opts,
						metadata: { parse_mode: "MarkdownV2" },
					},
				),
			);
		}
		return this.notModifiedNoOp(
			await super.wireEdit(chatId, messageId, content, {
				...opts,
			}),
		);
	}

	/**
	 * tg2-1 (adapter.py:edit_message "not modified" branches :5737/:5757/
	 * :5929 + _try_edit_rich): Telegram answers 400 "message is not modified"
	 * when an edit writes identical bytes — Hermes converts it into
	 * SendResult(success=True) at EVERY edit site. Without the mapping the
	 * REQUIRES_EDIT_FINALIZE redundant finalize edit reports failure and
	 * gateway-stream-consumer.finalizeTurn routes it into
	 * sendFallbackContinuation — duplicating the whole message on real
	 * servers.
	 */
	private notModifiedNoOp(res: SendResult): SendResult {
		if (
			!res.success &&
			(res.error ?? "").toLowerCase().includes("not modified")
		) {
			return { success: true };
		}
		return res;
	}

	/**
	 * tg2-6 rich finalize edit (adapter.py:_try_edit_rich :2336). Returns
	 * null = fall through to the legacy MarkdownV2 edit (extra off, content
	 * ineligible, or fallback-class rejection); a SendResult = final outcome
	 * (success, not-modified no-op success, or transient failure with NO
	 * legacy resend).
	 */
	private async tryEditRich(
		chatId: string,
		messageId: string,
		content: string,
	): Promise<SendResult | null> {
		if (
			!this.richMessagesEnabled ||
			this.richSendDisabled ||
			!isRichEligibleContent(content)
		) {
			return null;
		}
		const payload: Record<string, unknown> = {
			chat_id: normalizeTelegramChatId(chatId),
			message_id: Number(messageId),
			rich_message: richMessagePayload(content),
			...this.linkPreviewKwargs(),
		};
		let failure: string | null = null;
		try {
			await this.bot.editMessageText(payload as never);
		} catch (err) {
			failure = err instanceof Error ? err.message : String(err);
		}
		if (failure !== null) {
			const klass = richFailureClass(failure);
			if (klass === "capability") {
				this.richSendDisabled = true;
				return null; // legacy path owns delivery now
			}
			if (failure.toLowerCase().includes("not modified")) {
				return { success: true }; // identical rich content — no-op
			}
			if (klass === "fallback") return null; // degrade WITHOUT latching
			return { success: false, error: failure }; // transient — no resend
		}
		return { success: true };
	}

	// ── Rich send lane (tg2-6; adapter.py:_try_send_rich :2229 /
	//    _should_attempt_rich :2079 / sendRichMessageDraft :2430) ────────

	/**
	 * Tier-1 rich transport behind the EXISTING wireRich seam. With the
	 * rich_messages extra OFF (production default), the inherited scripted-
	 * probe behavior stands (capability-missing = ladder latch once). With
	 * the extra ON, the probe latch is UN-LATCHED: eligible raw markdown goes
	 * to sendRichMessage {chat_id, rich_message, ...routing}; ineligible
	 * content skips tier 1 silently via a fallback-class failure (no doomed
	 * roundtrip, no latch). expect_edits previews skip rich too (Hermes
	 * _should_attempt_rich parity; the ladder also gates this).
	 */
	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.richMessagesEnabled) return super.wireRich(content, metadata);
		if (this.richSendDisabled || metadata["expect_edits"] === true) {
			return { success: false, error: "Bad Request: rich delivery skipped" };
		}
		if (!isRichEligibleContent(content)) {
			return {
				success: false,
				error: "Bad Request: rich skipped - content not rich-eligible",
			};
		}
		const chatRaw = metadata[TELEGRAM_RICH_LANE_CHAT_KEY];
		if (typeof chatRaw !== "string" || chatRaw === "") {
			return super.wireRich(content, metadata);
		}
		const routing = this.computeSendRouting(chatRaw, metadata);
		if (routing.failLoud !== null) {
			// DM-topic refuse stays the LEGACY path result (_compute_single_
			// send_routing returning None parity) via a fallback-class skip.
			return {
				success: false,
				error: "Bad Request: rich skipped - dm-topic routing refused",
			};
		}
		const payload: Record<string, unknown> = {
			chat_id: normalizeTelegramChatId(chatRaw),
			rich_message: richMessagePayload(content),
			// Only non-null routing keys ride the raw endpoint (:2249 parity).
			...routing.threadArgs(),
			...notificationKwargs(this.notificationsMode, metadata),
			...this.linkPreviewKwargs(),
			...(routing.replyToId !== null
				? { reply_parameters: { message_id: routing.replyToId } }
				: {}),
		};
		const res = await this.bot.sendRichMessage(payload);
		if (res.success) routing.commitConsumedAnchor();
		return res;
	}

	/**
	 * Rich DRAFT frames (tg2-6; adapter.py:_try_send_rich_draft :2430): gated
	 * behind BOTH rich extras plus its own capability latch. Any failure
	 * degrades THIS frame to the legacy plain-text draft - drafts are
	 * ephemeral and overwritten by the next frame or the final.
	 */
	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		if (
			this.richMessagesEnabled &&
			this.richDraftsEnabled &&
			!this.richDraftDisabled &&
			isRichEligibleContent(args.content)
		) {
			const kwargs = threadKwargsForSend(
				metadataThreadId(args.metadata as never),
				args.metadata as never,
				null,
			);
			const payload: Record<string, unknown> = {
				chat_id: normalizeTelegramChatId(args.chatId),
				draft_id: args.draftId,
				rich_message: richMessagePayload(args.content),
			};
			if (
				kwargs.messageThreadId !== null &&
				kwargs.messageThreadId !== undefined
			) {
				payload["message_thread_id"] = kwargs.messageThreadId;
			}
			if (kwargs.directMessagesTopicId !== undefined) {
				payload["direct_messages_topic_id"] = kwargs.directMessagesTopicId;
			}
			const res = await this.bot.sendRichMessageDraft(payload);
			if (res.success) return res;
			if (richFailureClass(res.error ?? "") === "capability") {
				this.richDraftDisabled = true; // later frames skip the attempt
			}
			// transient - legacy draft this frame
		}
		return this.sendLegacyNativeDraft(args);
	}

	/**
	 * tg-13 helper — adapter.py send_draft :6174-6177 trim: oversized draft
	 * text is the FIRST CHUNK of THE chat's chunking resolution — literally
	 * `truncate_message(content, MAX_MESSAGE_LENGTH, len_fn=utf16_len)[0]`
	 * (previews are ephemeral, never split). Pi ports that exactly through the
	 * ONE shared chunker (kit/chunking chunkWithFenceCarry over §6.3's per-chat
	 * pair: utf16 × TELEGRAM_MAX_MESSAGE_UNITS), preserving INDICATOR_RESERVE,
	 * fence-carry scaffolding and the " (1/N)" label — no hand-rolled trim.
	 * Upstream quirk kept byte-honest: the FIT CHECK is CODEPOINT-based
	 * (`len(content) <= MAX`), only the SPLIT measures utf16 units.
	 */
	private firstDraftChunk(content: string, chatId: string): string {
		if (codePointLen(content) <= TELEGRAM_MAX_MESSAGE_UNITS) return content;
		return (
			chunkWithFenceCarry(content, this.chatLengthPolicyForChat(chatId))
				.chunks[0] ?? content
		);
	}

	/**
	 * tg-13 closure — adapter.py:send_draft (:6116) legacy plain-text lane.
	 * DM draft frames are REAL Bot API calls named `sendMessageDraft`
	 * carrying {chat_id, draft_id, text, parse_mode?, ...thread kwargs}:
	 *   · MarkdownV2-FIRST — the same format_message-style conversion the
	 *     regular send path uses, parse_mode stamped (adapter.py :6191-6200)
	 *     so the animated preview matches the final message's formatting;
	 *   · a BadRequest on that attempt retries ONCE as raw text (:6200-6208,
	 *     mirroring the (True, False) retry the streaming send loop uses);
	 *   · when rich messages are enabled but rich DRAFTS are not and the
	 *     content needs rich rendering (`plain_rich_preview` :6185-6190),
	 *     drafts stay RAW — the legacy formatter would rewrite pipe tables
	 *     into bullet groups inside an ephemeral preview;
	 *   · oversized text ships as the FIRST CHUNK of the shared chunker at the
	 *     chat's §6.3 pair (utf16 × 4096) — :6176 truncate_message with
	 *     utf16_len takes [0]; never split, " (1/N)" label kept (:6175 fit
	 *     check stays codepoint-based);
	 *   · success carries NO message id — drafts have none (:6207/… returns
	 *     SendResult(success=True, message_id=None)). The seal/final lane is
	 *     UNTOUCHED (DEC-034 chokepoint; Hermes has no Bot API to promote a
	 *     draft — the final sendMessage/sendRichMessage is what persists).
	 */
	private async sendLegacyNativeDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		const text = this.firstDraftChunk(args.content, args.chatId);
		const plainRichPreview =
			this.richMessagesEnabled &&
			!this.richDraftsEnabled &&
			needsRichRendering(text);
		const kwargs = threadKwargsForSend(
			metadataThreadId(args.metadata as never),
			args.metadata as never,
			null,
		);
		const threadArgs: Record<string, unknown> = {};
		if (
			kwargs.messageThreadId !== null &&
			kwargs.messageThreadId !== undefined
		) {
			threadArgs["message_thread_id"] = kwargs.messageThreadId;
		}
		if (kwargs.directMessagesTopicId !== undefined) {
			threadArgs["direct_messages_topic_id"] = kwargs.directMessagesTopicId;
		}
		const modes: readonly boolean[] = plainRichPreview
			? [false]
			: [true, false];
		let lastError: string | null = null;
		for (const useMarkdown of modes) {
			const res = await this.bot.sendMessageDraft({
				chat_id: normalizeTelegramChatId(args.chatId),
				draft_id: args.draftId,
				text: useMarkdown ? toTelegramMarkdownV2Full(text) : text,
				...(useMarkdown ? { parse_mode: "MarkdownV2" } : {}),
				...threadArgs,
			});
			if (res.success) return { success: true }; // drafts carry NO id
			lastError = res.error ?? "draft rejected";
			// Only BadRequest-class failures degrade to the plain attempt
			// (adapter.py _is_bad_request_error); anything else surfaces now.
			if (!lastError.toLowerCase().includes("bad request")) break;
		}
		return { success: false, error: lastError ?? "draft rejected" };
	}

	/**
	 * Stamp THE delivery chat id onto this lane's metadata so the chat-less
	 * ladder seam (wireRich) can build {chat_id} payloads. Each deliverText
	 * call copies its own metadata - concurrent lanes cannot cross-contaminate.
	 */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		return super.deliverText(chatId, content, {
			...metadata,
			[TELEGRAM_RICH_LANE_CHAT_KEY]: chatId,
		});
	}

	/**
	 * tg-12 closure — adapter.py:delete_message (:6064, openclaw#72038 port):
	 * delete a previously sent bot message via the Bot API `deleteMessage`
	 * (works for bot-posted messages of the last 48 h). Best-effort BY
	 * CONTRACT — the stream consumer's fresh-final cleanup (silence-marker
	 * suppression `suppressSilenceMarker` and stale-preview retraction
	 * `abandonStream`) calls it defensively through the adapter-seam probe,
	 * leaves the preview in place on failure, and logs at debug level; a
	 * raise here would wedge the consumer lane. The message id passes through
	 * VERBATIM (:6081 int(message_id) is Python's wire typing, not a
	 * transform); chat_id normalization mirrors :6082 exactly.
	 */
	async deleteMessage(chatId: string, messageId: string): Promise<boolean> {
		try {
			const res = await this.bot.deleteMessage({
				chat_id: normalizeTelegramChatId(chatId),
				message_id: messageId,
			});
			// pi wire contract: failures ride SendResult(success=false) (the pi
			// fake never raises); slack-adapter deleteMessage is the same boolean
			// idiom (`result?.success === true`). Upstream parity: any failure ⇒
			// false, the caller leaves the preview in place.
			return res.success === true;
		} catch {
			return false;
		}
	}

	// ════════════════════════════════════════════════════════════════
	// Outgoing media family (tg-9) — adapter.py:send_image_file /
	// send_document / send_video / send_voice / send_animation
	// ════════════════════════════════════════════════════════════════

	/**
	 * Build the shared media kwargs (tg-9/tg2-4): caption capped at the Bot
	 * API 1024 units, reply anchor, thread/direct-messages-topic routing
	 * (_thread_kwargs_for_send family), and notification kwargs from the
	 * adapter mode + metadata. Absent fields are OMITTED — never null.
	 */
	private mediaKwargs(opts: MediaSendOptions): Record<string, unknown> {
		const kwargs: Record<string, unknown> = {};
		if (opts.caption !== undefined) {
			kwargs["caption"] =
				opts.caption.length > 1024 ? opts.caption.slice(0, 1024) : opts.caption;
		}
		const md = opts.metadata as Record<string, unknown> | undefined;
		const replyRaw =
			opts.replyTo ??
			md?.[REPLY_TO_METADATA_KEY] ??
			metadataReplyToMessageId(md);
		if (
			replyRaw !== undefined &&
			replyRaw !== null &&
			String(replyRaw) !== ""
		) {
			const replyTo = Number(replyRaw);
			if (Number.isFinite(replyTo)) kwargs["reply_to_message_id"] = replyTo;
		}
		const threadRaw =
			typeof opts.threadId === "string" && opts.threadId !== ""
				? opts.threadId
				: metadataThreadId(md);
		const routed = threadKwargsForSend(threadRaw, md, null);
		if (
			routed.messageThreadId !== null &&
			routed.messageThreadId !== undefined
		) {
			kwargs["message_thread_id"] = routed.messageThreadId;
		}
		if (routed.directMessagesTopicId !== undefined) {
			kwargs["direct_messages_topic_id"] = routed.directMessagesTopicId;
		}
		Object.assign(
			kwargs,
			notificationKwargs(this.notificationsMode, opts.metadata),
		);
		return kwargs;
	}

	/**
	 * tg2-12 (adapter.py:_send_with_dm_topic_reply_anchor_retry :1753 +
	 * _should_retry_without_dm_topic_reply_anchor): wrap EVERY media
	 * transmission in the DM-topic anchor retry ladder. When the lane is
	 * marked telegram_dm_topic_reply_fallback and the Bot API rejects with a
	 * BadRequest naming a dead reply target or topic/thread route, retry ONCE
	 * without reply_to_message_id / message_thread_id /
	 * direct_messages_topic_id (pruning the stale binding when a thread id is
	 * dropped). Everything else surfaces as-is.
	 */
	private async transmitMediaWithDmTopicRetry(
		chatId: string,
		method:
			| "sendPhoto"
			| "sendDocument"
			| "sendVoice"
			| "sendAudio"
			| "sendVideo"
			| "sendAnimation",
		args: Record<string, unknown>,
		opts: MediaSendOptions,
	): Promise<SendResult> {
		const md = opts.metadata as Record<string, unknown> | undefined;
		const first = await (
			this.bot[method] as (a: Record<string, unknown>) => Promise<SendResult>
		)(args);
		if (first.success || md?.["telegram_dm_topic_reply_fallback"] !== true) {
			return first;
		}
		const errLower = (first.error ?? "").toLowerCase();
		if (!errLower.includes("bad request")) return first;

		const hasTopicMetadata = metadataDirectMessagesTopicId(md) !== null;
		const topicMarkers = [
			"direct_messages_topic",
			"message thread not found",
			"thread not found",
			"topic_closed",
			"topic_deleted",
			"topic not found",
		];
		const anchorDropped =
			args["reply_to_message_id"] !== undefined &&
			errLower.includes("message to be replied not found");
		const topicDropped =
			hasTopicMetadata && topicMarkers.some((m) => errLower.includes(m));
		if (!anchorDropped && !topicDropped) return first;

		const retryArgs = { ...args };
		delete retryArgs["reply_to_message_id"];
		const droppedThread =
			retryArgs["message_thread_id"] ?? retryArgs["direct_messages_topic_id"];
		delete retryArgs["message_thread_id"];
		delete retryArgs["direct_messages_topic_id"];
		if (droppedThread !== undefined && Number.isFinite(Number(droppedThread))) {
			this.pruneStaleDmTopicBinding(chatId, Number(droppedThread));
		}
		return (
			this.bot[method] as (a: Record<string, unknown>) => Promise<SendResult>
		)(retryArgs);
	}

	/**
	 * adapter.py:send_image_file parity — local image natively via sendPhoto;
	 * ANY photo failure falls back to sendDocument (dimension/limit parity),
	 * matching Hermes' document fallback chain.
	 */
	async sendImageFile(
		chatId: string,
		source: string,
		opts: MediaSendOptions = {},
	): Promise<SendResult> {
		const photo = await this.transmitMediaWithDmTopicRetry(
			chatId,
			"sendPhoto",
			{
				chat_id: normalizeTelegramChatId(chatId),
				photo: source,
				...this.mediaKwargs(opts),
			},
			opts,
		);
		if (photo.success) return photo;
		// Document fallback — no dimension limit, only the 50MB size cap.
		return this.sendDocument(chatId, source, {
			...opts,
			fileName: basenameOf(source),
		});
	}

	/** adapter.py:send_document parity — native file attachment. */
	async sendDocument(
		chatId: string,
		source: string,
		opts: MediaSendOptions & { fileName?: string | undefined } = {},
	): Promise<SendResult> {
		return this.transmitMediaWithDmTopicRetry(
			chatId,
			"sendDocument",
			{
				chat_id: normalizeTelegramChatId(chatId),
				document: source,
				filename: opts.fileName ?? basenameOf(source),
				...this.mediaKwargs(opts),
			},
			opts,
		);
	}

	/** adapter.py:send_video parity — native video message. */
	async sendVideo(
		chatId: string,
		source: string,
		opts: MediaSendOptions = {},
	): Promise<SendResult> {
		return this.transmitMediaWithDmTopicRetry(
			chatId,
			"sendVideo",
			{
				chat_id: normalizeTelegramChatId(chatId),
				video: source,
				...this.mediaKwargs(opts),
			},
			opts,
		);
	}

	/**
	 * adapter.py:send_voice parity (:7760) — extension-routed audio delivery:
	 * .ogg/.opus → sendVoice (round playable bubble); .mp3/.m4a → sendAudio
	 * (Bot API audio formats only); anything else (.wav, .flac, …) falls back
	 * to document delivery instead of raising. Voice captions ride the
	 * MarkdownV2→plain VARIANT LADDER (#32029, tg2-10): the formatted caption
	 * retries as the plain slice on parse-entity rejection; an absent caption
	 * OMITS the field entirely (PTB drops None — a null key never ships).
	 */
	async sendVoice(
		chatId: string,
		audioPath: string,
		opts: MediaSendOptions & {
			durationSeconds?: number | undefined;
		} = {},
	): Promise<SendResult> {
		const ext = extOf(audioPath);
		if (ext === ".ogg" || ext === ".opus") {
			for (const variant of voiceCaptionVariants(opts.caption)) {
				const res = await this.transmitMediaWithDmTopicRetry(
					chatId,
					"sendVoice",
					{
						chat_id: normalizeTelegramChatId(chatId),
						voice: audioPath,
						...(opts.durationSeconds !== undefined
							? { duration: opts.durationSeconds }
							: {}),
						...variant,
						...this.voiceBaseKwargs(opts),
					},
					opts,
				);
				// Only the NEXT (plain) variant retries on entity parse
				// failures; anything else is a real send error for the caller.
				if (res.success || !variant.parse_mode) return res;
				const blob = (res.error ?? "").toLowerCase();
				if (!blob.includes("parse") && !blob.includes("entit")) return res;
			}
			return { success: false, error: "voice caption variants exhausted" };
		}
		if (ext === ".mp3" || ext === ".m4a") {
			return this.transmitMediaWithDmTopicRetry(
				chatId,
				"sendAudio",
				{
					chat_id: normalizeTelegramChatId(chatId),
					audio: audioPath,
					...(opts.caption !== undefined
						? { caption: opts.caption.slice(0, 1024) }
						: {}),
					...(opts.durationSeconds !== undefined
						? { duration: opts.durationSeconds }
						: {}),
					...this.voiceBaseKwargs(opts),
				},
				opts,
			);
		}
		// Formats Telegram can't play natively — document delivery fallback.
		return this.sendDocument(chatId, audioPath, {
			...opts,
			fileName: basenameOf(audioPath),
		});
	}

	/** Shared voice/audio kwargs WITHOUT any caption keys (tg2-10: captions
	 * are owned by the variant ladder; absent captions omit the key). */
	private voiceBaseKwargs(
		opts: MediaSendOptions & { durationSeconds?: number | undefined },
	): Record<string, unknown> {
		return this.mediaKwargs({ ...opts, caption: undefined });
	}

	/**
	 * adapter.py:send_animation parity — native auto-playing GIF; failure
	 * falls back to a regular photo send (Hermes fallback chain).
	 */
	async sendAnimation(
		chatId: string,
		source: string,
		opts: MediaSendOptions = {},
	): Promise<SendResult> {
		const anim = await this.transmitMediaWithDmTopicRetry(
			chatId,
			"sendAnimation",
			{
				chat_id: normalizeTelegramChatId(chatId),
				animation: source,
				...this.mediaKwargs(opts),
			},
			opts,
		);
		if (anim.success) return anim;
		return this.sendImageFile(chatId, source, opts);
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

// ── media-family helpers (tg-9) ────────────────────────────────────────────

/** Shared options for the media-send family (Hermes kwargs parity). */
export interface MediaSendOptions {
	/** Caption — capped at Bot API 1024 UTF-16 units on every method. */
	caption?: string | undefined;
	/** Explicit reply anchor (adapter.py _reply_to_message_id_for_send). */
	replyTo?: string | number | undefined;
	/** Forum/DM topic id — routed through the threadIdForSend mapping. */
	threadId?: string | undefined;
	/** Metadata for notification kwargs ("notify" override) + anchors. */
	metadata?: Metadata | undefined;
}

/** Bot API documents/media carry a basename filename on the wire. */
function basenameOf(source: string): string {
	const cleaned = source.startsWith("file://") ? source.slice(7) : source;
	const idx = cleaned.lastIndexOf("/");
	return idx >= 0 ? cleaned.slice(idx + 1) : cleaned;
}

/** Bot API message ids are ints on the wire (adapter.py int(message_id));
 * non-numeric test-double ids pass through trimmed rather than NaN-coercing. */
function numericId(value: string | number): number {
	return typeof value === "number" ? value : Number.parseInt(value, 10);
}

function numericOrRaw(value: string | number): number | string {
	if (typeof value === "number") {
		return Number.isFinite(value) ? value : String(value);
	}
	const n = Number.parseInt(value, 10);
	return Number.isFinite(n) ? n : value.trim();
}

function extOf(path: string): string {
	const base = basenameOf(path);
	const idx = base.lastIndexOf(".");
	return idx >= 0 ? base.slice(idx).toLowerCase() : "";
}

/**
 * #32029 / tg2-10 caption variant ladder (adapter.py:send_voice
 * _caption_variants): the MarkdownV2-formatted caption FIRST when it fits
 * the 1024 cap, then the plain slice as the parse-failure retry. An absent
 * caption yields ONE variant with NO caption key at all — a null field is
 * never transmitted (PTB drops None).
 */
function voiceCaptionVariants(
	caption: string | undefined,
): Array<{ caption?: string; parse_mode?: string }> {
	if (caption === undefined || caption === "") return [{}];
	const variants: Array<{ caption?: string; parse_mode?: string }> = [];
	const formatted = toTelegramMarkdownV2Full(caption);
	if (formatted.length <= 1024) {
		variants.push({ caption: formatted, parse_mode: "MarkdownV2" });
	}
	variants.push({ caption: caption.slice(0, 1024) });
	return variants;
}

/**
 * Default set_my_commands source: the ONE builtin command registry minus
 * CLI-only rows (07 §1 — menus DERIVE from the registry; zero per-surface
 * hardcoded lists). Aliases never ship (one menu entry per canonical name).
 */
function defaultTelegramMenuCommands(): TelegramMenuCommand[] {
	return BUILTIN_COMMAND_ROWS.filter((row) => row.cliOnly !== true).map(
		(row) => ({ command: row.name, description: row.description }),
	);
}

// ── registration path (04 §4.2) ────────────────────────────────────

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
