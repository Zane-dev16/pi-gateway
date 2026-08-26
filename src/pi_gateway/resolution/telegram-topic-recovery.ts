// pi_gateway/resolution/telegram-topic-recovery.ts — Telegram DM topic-lane
// pinning: a lobby-shaped reply (no thread_id, or General "1") is rewritten
// to the user's MOST RECENTLY BOUND topic BEFORE session-key derivation, so
// DM topic-mode replies cannot derive a fresh lobby key and split history
// across lanes.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/run.py:_recover_telegram_topic_thread_id   → recoverTelegramTopicThreadId
//   gateway/run.py:_TELEGRAM_GENERAL_TOPIC_IDS         → TELEGRAM_GENERAL_TOPIC_IDS
//   gateway/run.py:_telegram_topic_mode_enabled        → deps.topicModeEnabled (store read)
//   gateway/session.py:build_session_key               → rebuildSessionKey hook factory

import type { TelegramTopicBindingRow } from "../../pi_state/index.js";
import {
	buildSessionKey,
	type IsolationFlags,
	type SessionSource,
} from "./session-key.js";

/**
 * Telegram's General (pinned top) topic in forum-enabled private chats. Bot
 * API behavior varies: some clients omit message_thread_id for General,
 * others send "1". Both are "root" for lobby/lane purposes.
 * (run.py:_TELEGRAM_GENERAL_TOPIC_IDS = frozenset({"", "1"}).)
 */
export const TELEGRAM_GENERAL_TOPIC_IDS: ReadonlySet<string> = new Set([
	"",
	"1",
]);

/** Store-backed readers the recovery walk needs (StateStore fits structurally). */
export interface TelegramTopicRecoveryDeps {
	/** hermes_state.py:is_telegram_topic_mode_enabled parity. */
	topicModeEnabled(chatId: string, userId: string): boolean;
	/** hermes_state.py:list_telegram_topic_bindings_for_chat parity (newest first). */
	listTelegramTopicBindingsForChat(args: {
		chatId: string;
	}): TelegramTopicBindingRow[];
}

export function isTelegramTopicLobbyThread(
	threadId: string | undefined,
): boolean {
	return TELEGRAM_GENERAL_TOPIC_IDS.has(String(threadId ?? ""));
}

/**
 * Pin DM-topic routing to the user's last-active topic.
 *
 * Returns the recovered thread id, or null to leave the source alone:
 *   - non-telegram / non-DM / missing chat or user ⇒ null;
 *   - topic mode not enabled for this chat/user ⇒ null;
 *   - NON-lobby thread ids are NEVER rewritten: an unknown thread id is most
 *     likely the first message in a brand-new Telegram DM topic and must
 *     become its own lane instead of hijacking the latest binding (#31086 —
 *     the rewrite ran before the binding was recorded, so every message in
 *     the new topic looked "unknown" and was re-hijacked forever);
 *   - lobby (''/General '1') ⇒ newest-first scan for THIS user's binding;
 *     a different-user binding is skipped, same-thread hit ⇒ null (no-op).
 */
export function recoverTelegramTopicThreadId(
	source: SessionSource,
	deps: TelegramTopicRecoveryDeps,
): string | null {
	if (source.platform !== "telegram" || source.chatType !== "dm") return null;
	const chatId = source.chatId === undefined ? "" : String(source.chatId);
	const userId = source.userId === undefined ? "" : String(source.userId);
	if (!chatId || !userId) return null;
	let modeEnabled: boolean;
	try {
		modeEnabled = deps.topicModeEnabled(chatId, userId) === true;
	} catch {
		return null; // store read failure must never reroute traffic
	}
	if (!modeEnabled) return null;
	const inbound = String(source.threadId ?? "");
	if (!isTelegramTopicLobbyThread(inbound)) return null;
	let bindings: TelegramTopicBindingRow[];
	try {
		bindings = deps.listTelegramTopicBindingsForChat({ chatId });
	} catch {
		return null;
	}
	for (const b of bindings) {
		// newest-first
		if (String(b.user_id ?? "") !== userId) continue;
		const recovered = String(b.thread_id ?? "");
		if (recovered && recovered !== inbound) return recovered;
		return null;
	}
	return null;
}

export interface TelegramTopicStoreReader {
	isTelegramTopicModeEnabled(args: { chatId: string; userId: string }): boolean;
	listTelegramTopicBindingsForChat(args: {
		chatId: string;
	}): TelegramTopicBindingRow[];
}

/**
 * Guard-hook pair for AdapterSessionGuard (base.py:set_topic_recovery_fn +
 * build_session_key parity): install via attachGuard so handleMessage can
 * rewrite event.source.threadId BEFORE any keying/matching. `flags`/`profile`
 * must match whatever derivation produced the caller's keys.
 */
export function telegramTopicGuardHooks(
	reader: TelegramTopicStoreReader,
	opts: { flags?: IsolationFlags; profile?: string } = {},
): {
	topicThreadRecovery: (source: SessionSource) => string | null;
	rebuildSessionKey: (source: SessionSource) => string;
} {
	return {
		topicThreadRecovery: (source) =>
			recoverTelegramTopicThreadId(source, {
				topicModeEnabled: (chatId, userId) =>
					reader.isTelegramTopicModeEnabled({ chatId, userId }),
				listTelegramTopicBindingsForChat: ({ chatId }) =>
					reader.listTelegramTopicBindingsForChat({ chatId }),
			}),
		rebuildSessionKey: (source) =>
			buildSessionKey(source, opts.flags, opts.profile),
	};
}
