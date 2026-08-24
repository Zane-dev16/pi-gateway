// pi_platforms/telegram/reactions — reaction-ack lifecycle (A1) + inbound
// reactions as events (A2), ported from the READ-ONLY Hermes reference:
//   plugins/platforms/telegram/adapter.py:TelegramAdapter.on_processing_start
//     (👀 in-progress reaction when processing begins)
//   adapter.py:TelegramAdapter.on_processing_complete
//     (SUCCESS → 👍, FAILURE → 👎 via set_message_reaction — Telegram REPLACES
//      all existing bot reactions in one call, so unlike base.py's default
//      remove-then-add there is NO remove step; CANCELLED explicitly CLEARS
//      the 👀 so it never lingers)
//   adapter.py:_reactions_enabled (TELEGRAM_REACTIONS opt-in gate; default
//     off; "false"/"0"/"no" disable)
//   adapter.py:_normalize_reaction_event (inbound message_reaction updates
//     normalize to {emojis, custom_emoji_ids, chat_id, message_id,
//     thread_id} with hard caps: 64 reaction items, 64-char emojis,
//     128-char custom ids; invalid shapes return None — tolerated)
//   gap-audit A2: Telegram forum reactions double as a forum signal when
//     message_thread_id is absent.

import type { ProcessingOutcome } from "../../pi_gateway/guards/index.js";

/** adapter.py emoji set (U+1F440 / U+1F44D / U+1F44E). */
export const REACTION_IN_PROGRESS = "\u{1F440}"; // 👀
export const REACTION_OK = "\u{1F44D}"; // 👍
export const REACTION_FAIL = "\u{1F44E}"; // 👎

/**
 * adapter.py:_reactions_enabled parse — truthy unless explicitly disabled.
 * Undefined ⇒ FALSE (opt-in feature, fail-closed).
 */
export function parseReactionsEnabled(value: string | undefined): boolean {
	if (value === undefined || value === "") return false;
	return !["false", "0", "no"].includes(value.toLowerCase());
}

/** Outcome → ack action (adapter.py:on_processing_complete parity). */
export function reactionForOutcome(
	outcome: ProcessingOutcome,
): { kind: "set"; emoji: string } | { kind: "clear" } | null {
	switch (outcome) {
		case "success":
			return { kind: "set", emoji: REACTION_OK };
		case "failure":
			return { kind: "set", emoji: REACTION_FAIL };
		case "cancelled":
			return { kind: "clear" };
		default:
			return null;
	}
}

// ── inbound reactions (A2) ───────────────────────────────────────────────────

/** Wire shape of the Telegram `message_reaction` update payload. */
export interface WireMessageReaction {
	message_id?: number | string | undefined;
	chat?: { id?: number | string } | undefined;
	new_reaction?:
		| Array<{
				emoji?: string | undefined;
				custom_emoji_id?: number | string | undefined;
		  }>
		| undefined;
}

export interface NormalizedReactionEvent {
	platform: "telegram";
	eventType: "reaction";
	payload: {
		emojis: string[];
		customEmojiIds: string[];
		chatId: string;
		messageId: string;
		/** Present for topic'd reactions; ABSENT ⇒ forum-signal doubling. */
		threadId?: string | undefined;
	};
}

const MAX_REACTION_ITEMS = 64;
const MAX_EMOJI_CHARS = 64;
const MAX_CUSTOM_ID_CHARS = 128;

function coerceId(value: unknown): string | null {
	if (typeof value === "boolean") return null;
	if (typeof value === "number" && Number.isFinite(value)) return String(value);
	if (typeof value === "string" && value.length > 0) return value;
	return null;
}

/**
 * adapter.py:_normalize_reaction_event port. Returns null for shapes without
 * a wired contract (missing chat/message ids) — tolerated, never thrown.
 */
export function normalizeMessageReactionUpdate(
	mr: WireMessageReaction | undefined | null,
): NormalizedReactionEvent | null {
	if (mr === undefined || mr === null) return null;
	const chatId = coerceId(mr.chat?.id);
	const messageId = coerceId(mr.message_id);
	if (chatId === null || messageId === null) return null;
	const newReaction = Array.isArray(mr.new_reaction) ? mr.new_reaction : [];

	const emojis: string[] = [];
	const customEmojiIds: string[] = [];
	for (const r of newReaction.slice(0, MAX_REACTION_ITEMS)) {
		const emoji = r?.emoji;
		if (typeof emoji === "string" && emoji.length > 0) {
			emojis.push(emoji.slice(0, MAX_EMOJI_CHARS));
		}
		const customId = coerceId(r?.custom_emoji_id);
		if (customId !== null) {
			customEmojiIds.push(customId.slice(0, MAX_CUSTOM_ID_CHARS));
		}
	}
	return {
		platform: "telegram",
		eventType: "reaction",
		payload: {
			emojis,
			customEmojiIds,
			chatId,
			messageId,
		},
	};
}
