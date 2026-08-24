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
 * streaming falls back instead of blocking (adapter.py::edit_message).
 */
export const TELEGRAM_EDIT_FLOOD_INLINE_WAIT_CAP_SECONDS = 5.0;

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

// ── typing action variants (A11) ────────────────────────────────────────────

/**
 * The Bot API sendChatAction action matrix (Bot API ground truth as exercised
 * by adapter.py:send_typing's "typing" plus the media-send sites that switch
 * the bubble to the upload/record variants while an upload is in flight).
 * Metadata may request any of these; unknown requests fall back to "typing".
 */
export const TELEGRAM_CHAT_ACTIONS: readonly string[] = [
	"typing",
	"upload_photo",
	"record_video",
	"upload_video",
	"record_voice",
	"upload_voice",
	"upload_document",
	"choose_sticker",
	"find_location",
	"record_video_note",
	"upload_video_note",
];

export function isValidChatAction(action: string): boolean {
	return TELEGRAM_CHAT_ACTIONS.includes(action);
}

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

// ── sticker cache (M7) ──────────────────────────────────────────────────────

/**
 * Sticker-description cache retention. DEVIATION NOTE (proposed DEC-043):
 * gateway/sticker_cache.py stores cached_at but NEVER expires entries (reads
 * are unbounded get-by-key). This port bounds the cache with a TTL measured
 * on the injected clock — required by the census-port contract tests
 * (hit/miss/expiry) — defaulting to 30 days; `Infinity` restores exact Hermes
 * read semantics.
 */
export const TELEGRAM_STICKER_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;

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
