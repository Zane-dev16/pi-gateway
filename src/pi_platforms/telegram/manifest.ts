// pi_platforms/telegram/manifest — TELEGRAM MANIFEST DATA (Phase-6 census
// port; DEC-024 first production adapter).
//
// Per-platform numbers live HERE, never in core (Q17/DEC-017). Every constant
// is TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/telegram/) and cited by file:symbol — vendor ground truth
// enters as data, no SDK calls, no network (04 §8 headless rule).

import type { PluginManifest } from "../kit/index.js";

// ── message limits (adapter.py class constants) ─────────────────────────────

/**
 * adapter.py:TelegramAdapter.MAX_MESSAGE_LENGTH = 4096 — Bot API per-message
 * cap. Length is measured in UTF-16 CODE UNITS (adapter.py:message_len_fn →
 * utf16_len; 04 §6.3/A15 pair).
 */
export const TELEGRAM_MAX_MESSAGE_UNITS = 4096;

/**
 * adapter.py:TelegramAdapter._SPLIT_THRESHOLD = 4000 — near-cap chunks are
 * treated as client-split continuations for ingress batching.
 */
export const TELEGRAM_SPLIT_THRESHOLD_UNITS = 4000;

/**
 * adapter.py:TelegramAdapter.RICH_MESSAGE_MAX_CHARS = 32768 — Bot API 10.1
 * rich messages cap the raw markdown/html text at 32,768 UTF-8 characters;
 * content above this rides the legacy chunking path.
 */
export const TELEGRAM_RICH_MESSAGE_MAX_CHARS = 32768;

// ── flood / rate tiers (Q17 manifest budgets) ───────────────────────────────

/**
 * adapter.py send ladder: "Telegram flood control on send (attempt %d/3)" —
 * the send path retries a FloodWait up to THREE attempts honoring the server
 * retry_after (adapter.py::send inner loop).
 */
export const TELEGRAM_MAX_SEND_ATTEMPTS = 3;

/**
 * adapter.py:edit_message FloodWait split: waits ≤ 5.0s sleep inline + retry
 * once; waits > 5.0s return `flood_control:<wait>` with retry_after set so
 * streaming falls back instead of blocking (adapter.py::edit_message :5697
 * `if wait > 5.0`).
 */
export const TELEGRAM_EDIT_FLOOD_INLINE_WAIT_CAP_SECONDS = 5.0;

/**
 * SEND-class inline FloodWait cap (#91969 parity): the send inner loop
 * (adapter.py::send "attempt %d/3") honors the server retry_after INLINE only
 * up to the SAME 5.0s envelope; larger waits fail closed with
 * `error="flood_control:<wait>"` WITHOUT sleeping — a verbatim sleep pinned
 * the gateway for 97 minutes once. Waits are surfaced in the ERROR STRING,
 * never as a machine-readable retryAfter the generic §6.1 ladder would
 * re-sleep verbatim.
 */
export const TELEGRAM_SEND_FLOOD_INLINE_WAIT_CAP_SECONDS = 5.0;

/**
 * adapter.py:_start_updater_with_progress start_polling(
 * allowed_updates=Update.ALL_TYPES) — EVERY poll (cold boot, reconnect,
 * conflict restart) requests the FULL update-type set. Without it real
 * Telegram applies its DEFAULT filter, which EXCLUDES message_reaction /
 * message_reaction_count updates — silently killing inbound-reaction ingress.
 * Transcribed from python-telegram-bot Update.ALL_TYPES.
 */
export const TELEGRAM_ALLOWED_UPDATES: readonly string[] = [
	"message",
	"edited_message",
	"channel_post",
	"edited_channel_post",
	"business_connection",
	"business_message",
	"edited_business_message",
	"deleted_business_messages",
	"message_reaction",
	"message_reaction_count",
	"inline_query",
	"chosen_inline_result",
	"callback_query",
	"shipping_query",
	"pre_checkout_query",
	"purchased_paid_media",
	"poll",
	"poll_answer",
	"my_chat_member",
	"chat_member",
	"chat_join_request",
	"chat_boost",
	"removed_chat_boost",
];

/**
 * Real-Bot-API DEFAULT allowed_updates (what getUpdates delivers when the
 * argument is OMITTED): everything EXCEPT the reaction kinds. The fake models
 * this so a poller that forgets allowed_updates loses reaction updates — the
 * defect the ALL_TYPES contract exists to prevent.
 */
export const TELEGRAM_DEFAULT_ALLOWED_UPDATES: readonly string[] =
	TELEGRAM_ALLOWED_UPDATES.filter(
		(k) => k !== "message_reaction" && k !== "message_reaction_count",
	);

/**
 * adapter.py:~1 edit/s × 0.8s flood envelope (adapter.py streaming-preview
 * comment): progressive edits budgeted at ONE per 0.8s window before the
 * saturated-preview dedup goes quiet until finalize (#30045-class).
 */
export const TELEGRAM_STREAM_EDIT_WINDOW_SECONDS = 0.8;

/**
 * base.py:_keep_typing refresh cadence ("refresh every 2 [s]" — typing
 * bubbles expire ~5 s server-side). Consumed via the polling engine's
 * TYPING_REFRESH_MS; declared here as the Q17 typing tier window.
 */
export const TELEGRAM_TYPING_WINDOW_SECONDS = 2;

/** Q17 RateBudget for telegram (kit capabilities.ts shapes). Tier order is
 * resolution order — governingTier returns the FIRST tier listing an op. */
const RATE_TIERS = [
	{
		name: "stream-edit-envelope",
		ops: ["edit", "draft-start", "draft-stop"] as const,
		limit: 1,
		windowSeconds: TELEGRAM_STREAM_EDIT_WINDOW_SECONDS,
	},
	{
		name: "typing-refresh",
		ops: ["typing"] as const,
		limit: 1,
		windowSeconds: TELEGRAM_TYPING_WINDOW_SECONDS,
	},
	{
		name: "chat-message",
		ops: ["send"] as const,
		limit: 1,
		windowSeconds: 1,
	},
] as const;

export const TELEGRAM_RATE_BUDGET = { tiers: RATE_TIERS };

// ── typing action (A11) ─────────────────────────────────────────────────────

/**
 * adapter.py:send_typing (:8400/:8412) sends sendChatAction with
 * action="typing" — the ONLY action the baseline adapter ever puts on the
 * wire. Although the Bot API defines upload/record variants, no Hermes
 * production site issues them; the variant seam here carries STATUS TEXT and
 * THREAD placement only, never a different action string.
 */
export const TELEGRAM_CHAT_ACTION = "typing";

/**
 * Typing-failure cooldown (A11 backoff variant):
 * adapter.py:_telegram_typing_cooldown_seconds default 30 s, clamped [1,300];
 * _record_typing_cooldown uses the server retry_after when present.
 */
export const TELEGRAM_TYPING_COOLDOWN_DEFAULT_SECONDS = 30;
export const TELEGRAM_TYPING_COOLDOWN_MIN_SECONDS = 1;
export const TELEGRAM_TYPING_COOLDOWN_MAX_SECONDS = 300;

/**
 * adapter.py:_message_thread_id_for_typing asymmetry: SENDS reject forum
 * General-topic thread id "1" (map to None — _message_thread_id_for_send),
 * but TYPING preserves it (sendChatAction needs message_thread_id=1 to place
 * the bubble in the General topic).
 */
export const TELEGRAM_GENERAL_TOPIC_THREAD_ID = "1";

export function threadIdForSend(threadId: string | undefined): number | null {
	if (!threadId || threadId === TELEGRAM_GENERAL_TOPIC_THREAD_ID) return null;
	return Number(threadId);
}

export function threadIdForTyping(threadId: string | undefined): number | null {
	if (!threadId) return null;
	return Number(threadId);
}

// ── DM-topic send routing (tg2-4; adapter.py:_metadata_thread_id /
//    _metadata_direct_messages_topic_id / _metadata_reply_to_message_id /
//    _is_private_dm_topic_send / _thread_kwargs_for_send :1552) ────────────

/**
 * adapter.py:_dm_topic_missing_anchor_error — the EXACT fail-loud error a
 * private DM-topic send without its reply anchor returns (never transmitted
 * outside the requested topic).
 */
export const TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR =
	"Telegram DM topic delivery requires a reply anchor; refusing to send outside the requested topic";

/** adapter.py:_metadata_thread_id port — thread_id or message_thread_id. */
export function metadataThreadId(
	metadata: Record<string, unknown> | undefined,
): string | null {
	if (!metadata) return null;
	const raw = metadata["thread_id"] ?? metadata["message_thread_id"];
	return raw !== undefined && raw !== null ? String(raw) : null;
}

/**
 * adapter.py:_metadata_direct_messages_topic_id port — true Bot API Direct
 * Messages topic ids ride either metadata key.
 */
export function metadataDirectMessagesTopicId(
	metadata: Record<string, unknown> | undefined,
): string | null {
	if (!metadata) return null;
	const raw =
		metadata["direct_messages_topic_id"] ??
		metadata["telegram_direct_messages_topic_id"];
	return raw !== undefined && raw !== null ? String(raw) : null;
}

/** adapter.py:_metadata_reply_to_message_id port (int coercion). */
export function metadataReplyToMessageId(
	metadata: Record<string, unknown> | undefined,
): number | null {
	if (!metadata) return null;
	const raw = metadata["telegram_reply_to_message_id"];
	if (raw === undefined || raw === null) return null;
	const n = Number(raw);
	return Number.isFinite(n) ? n : null;
}

/** Routing kwargs for forum/DM-topic sends (_thread_kwargs_for_send shape):
 * absent fields mean the Bot API key is OMITTED (Python None → PTB drops). */
export interface TelegramThreadKwargs {
	messageThreadId?: number | null | undefined;
	directMessagesTopicId?: number | undefined;
}

/**
 * adapter.py:_is_private_dm_topic_send port — a send that MUST stay inside
 * its requested private topic lane (fail-loud when anchor-less and no
 * direct_messages_topic_id fallback resolves).
 */
export function isPrivateDmTopicSend(
	threadId: string | null,
	metadata: Record<string, unknown> | undefined,
): boolean {
	if (metadataDirectMessagesTopicId(metadata) !== null) {
		return (
			!!metadata &&
			metadata["telegram_dm_topic_reply_fallback"] === true &&
			metadataReplyToMessageId(metadata) !== null
		);
	}
	if (metadata?.["telegram_dm_topic_created_for_send"] === true) return false;
	return !!(
		threadId && metadata?.["telegram_dm_topic_reply_fallback"] === true
	);
}

/**
 * adapter.py:_thread_kwargs_for_send port (:1552). Forum topics route via
 * message_thread_id; true Bot API DM topics opt in with explicit
 * direct_messages_topic_id metadata (paired with message_thread_id=None —
 * OMITTED here); Hermes-created lanes marked telegram_dm_topic_reply_fallback
 * prefer their topic thread id for anchor-less synthetic sends (#87051).
 */
export function threadKwargsForSend(
	threadId: string | null,
	metadata: Record<string, unknown> | undefined,
	replyToMessageId: number | null,
): TelegramThreadKwargs {
	if (metadata?.["telegram_dm_topic_reply_fallback"] === true) {
		let anchor = replyToMessageId;
		if (anchor === null || anchor === undefined) {
			anchor = metadataReplyToMessageId(metadata);
		}
		if (anchor === null || anchor === undefined) {
			// Anchor-less synthetic sends stay in the active topic lane:
			// prefer the Hermes topic thread id when it resolves (#87051).
			const mapped = threadIdForSend(threadId ?? undefined);
			if (mapped !== null) return { messageThreadId: mapped };
			const direct = metadataDirectMessagesTopicId(metadata);
			if (direct !== null) {
				return { messageThreadId: null, directMessagesTopicId: Number(direct) };
			}
			return {};
		}
		return { messageThreadId: threadIdForSend(threadId ?? undefined) };
	}
	const directTopic = metadataDirectMessagesTopicId(metadata);
	if (directTopic !== null) {
		return {
			messageThreadId: null,
			directMessagesTopicId: Number(directTopic),
		};
	}
	return { messageThreadId: threadIdForSend(threadId ?? undefined) };
}

/** Bot API command-menu cap (adapter.py:_TELEGRAM_BOT_API_MAX_COMMANDS). */
export const TELEGRAM_BOT_API_MAX_COMMANDS = 100;

/**
 * hermes_cli/commands.py:_DEFAULT_TELEGRAM_MENU_MAX_COMMANDS = 60 — keeps
 * built-ins plus common commands visible while staying under Telegram's
 * undocumented ~4KB set_my_commands payload threshold.
 */
export const TELEGRAM_MENU_MAX_COMMANDS = 60;

// ── sticker cache (M7) ──────────────────────────────────────────────────────

/**
 * Sticker-description cache retention — EXACT Hermes parity:
 * gateway/sticker_cache.py:get_cached_description NEVER expires entries
 * (cached_at is written but never read; reads are unbounded get-by-key), so
 * the default TTL is Infinity. Tests may inject a finite ttlMs to exercise
 * expiry mechanics on the injected clock; production reads never expire.
 * (The previously cited "DEC-043" was the dingtalk exclusion — unrelated.)
 */
export const TELEGRAM_STICKER_CACHE_TTL_MS = Number.POSITIVE_INFINITY;

// ── notification mode (adapter.py:_notification_kwargs) ─────────────────────

/**
 * adapter.py:_notifications_mode default "important" (:853): every message
 * send arrives SILENTLY (disable_notification=True) unless the caller marks
 * it notify-worthy via metadata["notify"] (turn finals, approvals).
 * "all" restores push-per-message (legacy).
 */
export type TelegramNotificationsMode = "important" | "all";

export const TELEGRAM_NOTIFICATIONS_DEFAULT_MODE: TelegramNotificationsMode =
	"important";

/**
 * adapter.py:_resolve_notifications_mode port (env leg):
 * HERMES_TELEGRAM_NOTIFICATIONS ∈ {"all", "important"}; empty/unknown ⇒ the
 * "important" default (unknown values warn-and-default in Hermes).
 */
export function resolveTelegramNotificationsMode(
	raw: string | undefined,
): TelegramNotificationsMode {
	const mode = (raw ?? "").trim().toLowerCase();
	if (mode === "all") return "all";
	// "important", empty, and UNKNOWN values all land on the default.
	return TELEGRAM_NOTIFICATIONS_DEFAULT_MODE;
}

/**
 * adapter.py:_notification_kwargs port: in "important" mode EVERY send ships
 * disable_notification=True unless metadata["notify"] is truthy; "all" mode
 * never silences. Returns the KWARG OBJECT to spread onto the wire call.
 */
export function notificationKwargs(
	mode: TelegramNotificationsMode,
	metadata: Record<string, unknown> | undefined,
): { disable_notification?: boolean } {
	if (mode !== "important") return {};
	if (metadata?.["notify"]) return {};
	return { disable_notification: true };
}

// ── the PluginManifest (registration path, 04 §4.2) ─────────────────────────

/**
 * Capability mapping note: Hermes' `splits_long_messages=True` means
 * "send() chunks natively". In THIS kit the BASE owns chunking
 * (BasePlatformAdapter.deliverText → chunkWithFenceCarry), so the flag stays
 * FALSE here — declaring it true would make deliverText emit ONE oversized
 * chunk (the kit treats true as "router skips splitting"). The chunking
 * BEHAVIOR is preserved via the scalar 4096-unit UTF-16 policy instead.
 */
export const TELEGRAM_MANIFEST: PluginManifest = {
	name: "telegram",
	description:
		"Telegram bot adapter on the polling transport family (Bot API long-poll)",
	transportShape: "polling",
	requiresEnv: [
		{
			name: "TELEGRAM_BOT_TOKEN",
			description: "Bot token issued by BotFather",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "TELEGRAM_REACTIONS",
			description:
				"Opt-in reaction-ack lifecycle (👀 in-progress → 👍/👎, cleared on cancel)",
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		// See mapping note above — kit-owned chunking keeps this FALSE.
		splitsLongMessages: false,
		typedCommandPrefix: "/",
		interactiveResume: true,
		supportsInchannelContinuable: false,
		// adapter.py:REQUIRES_EDIT_FINALIZE = True — stream_consumer short-
		// circuits unchanged final edits, skipping MarkdownV2 conversion, so
		// finalize must be explicit (#25710 anchor).
		requiresEditFinalize: true,
	},
	rateBudget: TELEGRAM_RATE_BUDGET,
};
