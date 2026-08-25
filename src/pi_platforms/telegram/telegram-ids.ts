// pi_platforms/telegram/telegram-ids — chat-id normalization for Bot API
// calls (tg2-7), ported from the READ-ONLY Hermes reference:
//   plugins/platforms/telegram/telegram_ids.py:normalize_telegram_chat_id
//
// Telegram's Bot API accepts a chat_id in two forms: a numeric ID (an int,
// e.g. 123456789 for a DM or -1001234567890 for a channel/supergroup) or an
// @username string for public channels/groups. Hermes coerces EVERY outgoing
// chat_id through this normalizer at every bot.* call site: numeric values
// ship as JS numbers (wire ints), non-numeric values (usernames) ship as
// STRIPPED strings. A bare int() crash on usernames is exactly what the
// baseline avoids here.

/**
 * telegram_ids.py:normalize_telegram_chat_id port. Numeric values (incl.
 * negative channel ids and a leading +) become numbers; anything else
 * (e.g. "@username") comes back as a trimmed string. Never throws.
 */
export function normalizeTelegramChatId(
	chatId: string | number,
): number | string {
	const s = String(chatId).trim();
	if (/^[+-]?\d+$/.test(s)) return Number.parseInt(s, 10);
	return s;
}
