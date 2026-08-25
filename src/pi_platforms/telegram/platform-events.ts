// pi_platforms/telegram/platform-events — inbound platform-event
// normalization beyond reactions (tg2-11), ported from the READ-ONLY Hermes
// reference:
//   plugins/platforms/telegram/adapter.py:_normalize_message_edited_event
//
// `edited_message` updates normalize to a message_edited platform event with
// a bounded, raw-object-free payload contract (v1, additive): chat_id,
// message_id, thread_id (forum topic when present), text (edited text or
// caption), edited_at (ISO 8601 UTC or null). Malformed identities return
// null so the fire-site drops the event — hostile wire shapes never throw.

/** Wire shape of the Telegram `edited_message` update payload. */
export interface WireEditedMessage {
	message_id?: number | string | undefined;
	from?: { id?: number | string | undefined } | undefined;
	chat?: { id?: number | string } | undefined;
	date?: number | undefined;
	text?: string | undefined;
	caption?: string | undefined;
	message_thread_id?: number | string | undefined;
	is_topic_message?: boolean | undefined;
	edit_date?: number | string | undefined;
}

export interface NormalizedEditedEvent {
	platform: "telegram";
	eventType: "message_edited";
	payload: {
		chatId: string;
		messageId: string;
		/** Forum topic when the edited message is topic'd; else absent. */
		threadId?: string | undefined;
		/** Edited text or caption (≤8192 chars); null when neither. */
		text: string | null;
		/** ISO 8601 UTC edit timestamp; null when the wire omits it. */
		editedAt: string | null;
	};
}

function coerceId(value: unknown): string | null {
	if (typeof value === "boolean") return null;
	if (typeof value === "number" && Number.isFinite(value)) return String(value);
	if (typeof value === "string" && value.length > 0) return value;
	return null;
}

const MAX_EVENT_TEXT_CHARS = 8192;
const MAX_ID_CHARS = 128;

/**
 * adapter.py:_normalize_message_edited_event port. Returns null for updates
 * without a wired identity (missing chat/message ids, boolean-shaped ids).
 */
export function normalizeMessageEditedEvent(
	update: WireEditedMessage | undefined | null,
): NormalizedEditedEvent | null {
	if (update === undefined || update === null) return null;
	const chatId = coerceId(update.chat?.id);
	const messageId = coerceId(update.message_id);
	if (chatId === null || messageId === null) return null;

	const rawText = update.text ?? update.caption;
	const text = typeof rawText === "string" ? rawText : null;

	let threadId: string | undefined;
	const threadRaw = coerceId(update.message_thread_id);
	if (threadRaw !== null && update.is_topic_message === true) {
		threadId = threadRaw.slice(0, MAX_ID_CHARS);
	}

	let editedAt: string | null = null;
	const editDate = update.edit_date;
	if (typeof editDate === "number" && Number.isFinite(editDate)) {
		// Bot API unix seconds → ISO 8601 UTC (Hermes datetime.isoformat()).
		editedAt = new Date(editDate * 1000).toISOString().slice(0, 64);
	} else if (typeof editDate === "string" && editDate !== "") {
		editedAt = editDate.slice(0, 64);
	}

	return {
		platform: "telegram",
		eventType: "message_edited",
		payload: {
			chatId: chatId.slice(0, MAX_ID_CHARS),
			messageId: messageId.slice(0, MAX_ID_CHARS),
			...(threadId !== undefined ? { threadId } : {}),
			text: text === null ? null : text.slice(0, MAX_EVENT_TEXT_CHARS),
			editedAt,
		},
	};
}
