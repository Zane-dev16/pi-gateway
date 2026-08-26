// pi_gateway/guards/events.ts — inbound event shape shared by both guards
// (03-message-routing.md §1–§3).
//
// The adapter builds a routing key from `source` (02 §4) and feeds the event
// through the guard pipeline. `internal: true` marks a FORGED push-lane wake
// (DEC-022): it MUST traverse the exact same guards as user traffic — a wake
// is a turn, never a bypass.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:MessageEvent.is_command / get_command /
//     get_command_args                      → isCommand / getCommand / getCommandArgs
//   gateway/platforms/base.py:_PLAINTEXT_GATEWAY_RESTART_PATTERNS /
//     base.py:coerce_plaintext_gateway_command
//                                           → PLAINTEXT_GATEWAY_RESTART_PATTERNS /
//                                             coercePlaintextGatewayCommand (intake)
//   gateway/platforms/base.py:_merge_caption → mergeCaption
//   gateway/platforms/base.py:merge_pending_message_event
//                                            → mergePendingEvent (§3.1 table)
//   gateway/platforms/base.py:TextDebounceState / _can_merge_text_debounce_events
//                                            → TextDebounceState / canMergeTextDebounceEvents

import type { SessionSource } from "../resolution/session-key.js";

export type MessageType =
	| "text"
	| "photo"
	| "voice"
	| "video"
	| "document"
	| "location"
	| "other";

/** Minimal adapter arrival snapshot for an inbound message. */
export interface IncomingEvent {
	messageId?: string;
	text?: string;
	messageType: MessageType;
	mediaUrls?: string[];
	mediaTypes?: string[];
	/**
	 * DEC-022 push lane: synthetic internal MessageEvent forged from the
	 * session's stored SessionSource. Traverses BOTH guards like any turn.
	 */
	internal?: boolean;
	/** Default true; plugin-originated proactive events set false. */
	allowGatewayControl?: boolean;
	replyToMessageId?: string;
	metadata?: Record<string, unknown>;
	source?: SessionSource;
}

export function allowsGatewayControl(event: IncomingEvent): boolean {
	return event.allowGatewayControl !== false;
}

/** base.py:MessageEvent.is_command — slash-prefixed, control-enabled text. */
export function isCommand(event: IncomingEvent): boolean {
	return (
		allowsGatewayControl(event) &&
		(event.text ?? "").replace(/^\s+/, "").startsWith("/")
	);
}

function strip(value: string): string {
	return value.trim();
}

/**
 * base.py:MessageEvent.get_command — first whitespace word minus the leading
 * `/`, lowercased, `@mention` suffix stripped; file-path-like words ("/" in
 * the name) are NOT commands.
 */
export function getCommand(event: IncomingEvent): string | null {
	if (!isCommand(event)) return null;
	const commandText = (event.text ?? "").replace(/^\s+/, "");
	const parts = commandText.split(/\s+/, 1);
	const raw =
		parts[0] === undefined ? undefined : parts[0].slice(1).toLowerCase();
	if (!raw) return null;
	const at = raw.indexOf("@");
	const name = at >= 0 ? raw.slice(0, at) : raw;
	if (name.includes("/")) return null; // valid command names never contain /
	return name;
}

/** base.py:MessageEvent.get_command_args — everything after the first word. */
export function getCommandArgs(event: IncomingEvent): string | null {
	if (!isCommand(event)) return event.text ?? null;
	const commandText = (event.text ?? "").replace(/^\s+/, "");
	const idx = commandText.search(/\s/);
	if (idx < 0) return "";
	// iOS auto-corrects -- to em/en dashes; normalize back.
	return commandText
		.slice(idx + 1)
		.replaceAll("\u2014\u2014", "--")
		.replaceAll("\u2014", "--")
		.replaceAll("\u2013", "-");
}

/**
 * base.py:_PLAINTEXT_GATEWAY_RESTART_PATTERNS — the EXACT restart-style DM
 * phrases that coerce to /restart. Anchored both ends, punctuation/space tail
 * tolerated, case-insensitive; deliberately NARROW (group chats keep
 * natural-language semantics).
 */
export const PLAINTEXT_GATEWAY_RESTART_PATTERNS: readonly RegExp[] = [
	/^(?:please\s+)?restart\s+(?:the\s+)?gateway[.!?\s]*$/i,
	/^(?:please\s+)?restart\s+(?:the\s+)?hermes\s+gateway[.!?\s]*$/i,
	/^(?:please\s+)?restart\s+hermes[.!?\s]*$/i,
];

/**
 * base.py:coerce_plaintext_gateway_command — rewrite a tiny set of DM
 * plaintext admin phrases into "/restart" (IN PLACE on event.text) so
 * high-impact operational phrases never reach the LLM/tool path, where they
 * can trigger a self-restart from inside the running agent and wedge the
 * gateway in draining while it waits for that same agent. Runs at message
 * intake BEFORE command classification (base.py handle_message entry).
 *
 * Scope is intentionally narrow: TEXT events only, non-slash text only,
 * chat_type === "dm" ONLY (strict — "private"/"direct" do NOT coerce here,
 * matching the Python `!= "dm"` check), exact phrase matches only.
 */
export function coercePlaintextGatewayCommand(event: IncomingEvent): void {
	if (event.messageType !== "text") return;
	const text = (event.text ?? "").trim();
	if (text === "" || text.startsWith("/")) return;
	if (event.source?.chatType !== "dm") return;
	for (const pattern of PLAINTEXT_GATEWAY_RESTART_PATTERNS) {
		if (pattern.test(text)) {
			event.text = "/restart";
			return;
		}
	}
}

/**
 * base.py:BasePlatformAdapter._merge_caption — line-block dedup merge so an
 * album caption arriving twice is not duplicated. Comparison is by
 * "\n\n"-separated block with normalized whitespace (exact match per block,
 * never substring — "Meeting" must not vanish inside "Meeting agenda").
 */
export function mergeCaption(
	existingText: string | undefined,
	newText: string,
): string {
	if (!existingText) return newText;
	const blocks = existingText.split("\n\n").map((b) => strip(b));
	if (!blocks.includes(strip(newText))) {
		return `${existingText}\n\n${newText}`.trim();
	}
	return existingText;
}

/**
 * base.py:merge_pending_message_event — the §3.1 merge table applied when the
 * single pending slot already holds an event:
 *
 *   existing \ incoming | PHOTO            | has media (non-photo) | TEXT
 *   PHOTO               | extend media,    | media merge (promote  | caption onto photos
 *                       | merge caption    | to PHOTO)             |
 *   has media           | media merge      | extend media, caption | caption appended
 *   TEXT (merge_text)   | —                | —                     | append "\n"
 *   anything else       | REPLACE (newest wins), incl. merge_text off
 */
/**
 * Minimal slot-map seam: a plain `Map<string, IncomingEvent>` satisfies this
 * structurally, and the runner's FIFO helpers can pass their narrow view too.
 */
export interface PendingSlotMap {
	get(sessionKey: string): IncomingEvent | undefined;
	set(sessionKey: string, event: IncomingEvent): void;
}

export function mergePendingEvent(
	pendingMessages: PendingSlotMap,
	sessionKey: string,
	event: IncomingEvent,
	options: { mergeText?: boolean } = {},
): void {
	const existing = pendingMessages.get(sessionKey);
	if (existing) {
		const existingIsPhoto = existing.messageType === "photo";
		const incomingIsPhoto = event.messageType === "photo";
		const existingHasMedia = (existing.mediaUrls?.length ?? 0) > 0;
		const incomingHasMedia = (event.mediaUrls?.length ?? 0) > 0;

		if (existingIsPhoto && incomingIsPhoto) {
			existing.mediaUrls = [
				...(existing.mediaUrls ?? []),
				...(event.mediaUrls ?? []),
			];
			existing.mediaTypes = [
				...(existing.mediaTypes ?? []),
				...(event.mediaTypes ?? []),
			];
			if (event.text) {
				existing.text = mergeCaption(existing.text, event.text);
			}
			return;
		}
		if (existingHasMedia || incomingHasMedia) {
			if (incomingHasMedia) {
				existing.mediaUrls = [
					...(existing.mediaUrls ?? []),
					...(event.mediaUrls ?? []),
				];
				existing.mediaTypes = [
					...(existing.mediaTypes ?? []),
					...(event.mediaTypes ?? []),
				];
			}
			if (event.text) {
				existing.text = existing.text
					? mergeCaption(existing.text, event.text)
					: event.text;
			}
			if (existingIsPhoto || incomingIsPhoto) {
				existing.messageType = "photo";
			} else if (
				existing.messageType === "text" &&
				event.messageType !== "text"
			) {
				existing.messageType = event.messageType;
			}
			return;
		}
		if (
			options.mergeText === true &&
			existing.messageType === "text" &&
			event.messageType === "text"
		) {
			if (event.text) {
				existing.text = existing.text
					? `${existing.text}\n${event.text}`
					: event.text;
			}
			return;
		}
	}
	pendingMessages.set(sessionKey, event);
}

/** base.py:TextDebounceState — one buffered busy-text burst per session key. */
export interface TextDebounceState<Task = unknown> {
	event: IncomingEvent;
	task: Task | null;
	firstTs: number;
	lastTs: number;
}

/**
 * base.py:_can_merge_text_debounce_events — sender attribution identity for
 * shared sessions: (platform, sender-id) or DM (platform, "dm", chat_id).
 * A mixed-sender burst must never become one speaker's sentence.
 */
export function canMergeTextDebounceEvents(
	existing: IncomingEvent,
	event: IncomingEvent,
): boolean {
	const identityOf = (
		candidate: IncomingEvent,
	): [string, ...string[]] | null => {
		const source = candidate.source;
		if (!source) return null;
		const sender = source.userIdAlt || source.userId;
		if (sender) return [source.platform, String(sender)];
		if (
			(source.chatType === "dm" || source.chatType === "private") &&
			source.chatId
		) {
			return [source.platform, "dm", String(source.chatId)];
		}
		return null;
	};
	const existingSender = identityOf(existing);
	const incomingSender = identityOf(event);
	if (!existingSender || !incomingSender) return false;
	return (
		existingSender.length === incomingSender.length &&
		existingSender.every((part, i) => part === incomingSender[i])
	);
}
