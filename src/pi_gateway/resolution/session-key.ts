// pi_gateway/resolution/session-key.ts — routing-key construction and the
// ONE shared participant-isolation predicate (02-session-and-state.md §4,
// §4.4).
//
// An ADAPTER builds a routing key; the canonical identity is the RESOLVED
// session row (§4 preamble). Keys are how traffic arrives:
//
//   agent:<ns>:<platform>:<chat_type>[:scope]:[chat_id]:[thread_id][:participant]
//
// `<ns>` carries the profile — default stays byte-identical `agent:main`
// (gateway/session.py:_session_key_namespace). Keys re-key mid-flight under
// Telegram topics / relays / alias remaps; that is WHY identity is the resolved
// row and leases serialize on the compression-lineage root, never the raw key.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/session.py:build_session_key             → buildSessionKey
//   gateway/session.py:is_shared_multi_user_session  → isSharedMultiUserSession
//   gateway/session.py:_session_key_namespace        → sessionKeyNamespace
//
// §4.4 invariant: key construction AND "is this session shared" checks read
// ONE shared predicate — drift mislabels system prompts or drops sender
// attribution. buildSessionKey calls isSharedMultiUserSession for the
// participant-slot decision; there is no second derivation anywhere.

import { canonicalWhatsappIdentifier } from "./whatsapp-identity.js";

export const WHATSAPP_PLATFORM = "whatsapp";
export const SLACK_PLATFORM = "slack";

/** Adapter arrival snapshot (gateway SessionSource, TS-shaped). */
export interface SessionSource {
	/** Arrival surface: 'cli' | 'telegram' | 'whatsapp' | 'slack' | ... */
	platform: string;
	/** 'dm' | 'group' | 'channel' | 'thread'. */
	chatType: string;
	userId?: string;
	/** Preferred participant identifier when the platform offers one. */
	userIdAlt?: string;
	chatId?: string;
	threadId?: string;
	/**
	 * Discord auto-thread continuity: a channel-initiating message carries no
	 * thread_id yet, but the connector tells us the thread id its replies WILL
	 * carry (the message id). Keying on it makes initiator and follow-ups
	 * byte-match (02 §4.4).
	 */
	prospectiveThreadId?: string;
	/** Slack workspace scope prepended before chat/thread/participant slots. */
	scopeId?: string;
	chatName?: string;
}

/** §4.4 policy flags (defaults verified against gateway config). */
export interface IsolationFlags {
	/** Group/channel sessions isolate per sender. Default true. */
	groupSessionsPerUser?: boolean;
	/** Thread sessions isolate per sender. Default false (threads shared). */
	threadSessionsPerUser?: boolean;
}

const DEFAULT_FLAGS = {
	groupSessionsPerUser: true,
	threadSessionsPerUser: false,
};

/**
 * The `agent:<ns>` namespace prefix. Default profile (or None/""/"default") →
 * `agent:main` BYTE-IDENTICAL to every key ever generated, so positional
 * parsers (parts[2] == platform) are unaffected; named profile keeps the same
 * layout under its own namespace.
 */
export function sessionKeyNamespace(profile?: string): string {
	if (!profile || profile === "default") return "agent:main";
	return `agent:${profile}`;
}

/**
 * THE shared predicate (02 §4.4). True when a non-DM session is SHARED across
 * participants:
 *   - DMs are never shared.
 *   - Thread-context sessions (real or prospective thread) are shared unless
 *     thread_sessions_per_user.
 *   - Non-thread group/channel sessions are shared unless
 *     group_sessions_per_user (default true = isolated per sender).
 *
 * Consumers (all read THIS function, never re-derive):
 *   - buildSessionKey's participant-slot decision (below),
 *   - system-prompt context builder: shared sessions get the multi-user line
 *     instead of pinning one user name (a pinned name flips per turn → cache
 *     bust),
 *   - history-build site: `[display name]` prefixing in shared sessions,
 *   - slash-command audience scoping in group contexts.
 *
 * Delta note vs hermes session.py:1049 (proposed DEC entry): Hermes mirrors on
 * `source.thread_id` only, while its own build_session_key decides isolation
 * via `thread_id or prospective_thread_id`. Under Discord auto-thread
 * continuity that lets the INITIATING message classify as non-thread while
 * keying into the (shared) thread session — exactly the key/predicate drift
 * §4.4 bans ("one predicate, no independent re-derivation"). Pi reads the
 * effective thread slot here so both consumers agree by construction.
 */
export function isSharedMultiUserSession(
	source: Pick<SessionSource, "chatType" | "threadId" | "prospectiveThreadId">,
	flags: IsolationFlags = {},
): boolean {
	const groupFlag =
		flags.groupSessionsPerUser ?? DEFAULT_FLAGS.groupSessionsPerUser;
	const threadFlag =
		flags.threadSessionsPerUser ?? DEFAULT_FLAGS.threadSessionsPerUser;
	if (source.chatType === "dm") return false;
	if (source.threadId || source.prospectiveThreadId) return !threadFlag;
	return !groupFlag;
}

function slackScopeId(source: SessionSource): string | undefined {
	return source.platform === SLACK_PLATFORM && source.scopeId
		? String(source.scopeId)
		: undefined;
}

function whatsappCanonical(
	value: string | undefined,
	opts: WhatsappKeyOptions,
): string {
	if (!value) return "";
	return (
		canonicalWhatsappIdentifier(String(value), opts.whatsapp) || String(value)
	);
}

export interface WhatsappKeyOptions {
	/** Passed through to the §4.3 module (tests inject a temp mapping dir). */
	whatsapp?: { sessionDir?: string };
}

/**
 * Build the deterministic routing key from a message source. Single source of
 * truth for session-key construction.
 *
 * DM rules:
 *   - Slack scope_id identifies the workspace before chat/user ids. Discord
 *     guild scope is deliberately NOT added (compatibility freeze, §4.4).
 *   - chat_id isolates each private conversation; WhatsApp DM chat_ids are
 *     canonicalized first (§4.3 — JID/LID flips must converge to ONE key).
 *   - thread_id differentiates threaded DMs within the same DM chat.
 *   - Without chat_id, fall back to the sender's own identifier BEFORE the
 *     bare per-platform sink — otherwise every DM from every user without a
 *     chat_id collapses into one shared session (cross-user history bleed).
 *   - Without any identifier, DMs share one session per platform/chat_type.
 *
 * Group/channel rules:
 *   - chat_id identifies the parent chat; Slack workspace scope precedes it.
 *   - The participant slot (user_id_alt or user_id, WhatsApp-canonicalized)
 *     is appended only when isolation is enabled for the context AND an
 *     identifier exists — decided by isSharedMultiUserSession (ONE predicate,
 *     §4.4); without one, messages fall back to ONE shared session per chat,
 *     never a bare per-platform sink.
 *   - Threads share unless thread_sessions_per_user (expected UX for forum
 *     topics / Discord threads / Slack threads).
 *   - Prospective-thread continuity: when keying on prospective_thread_id the
 *     chat_type slot is rewritten to `thread` so initiator and follow-ups
 *     byte-match.
 */
export function buildSessionKey(
	source: SessionSource,
	flags: IsolationFlags = {},
	profile?: string,
	opts: WhatsappKeyOptions = {},
): string {
	const ns = sessionKeyNamespace(profile);
	const scope = slackScopeId(source);
	const isWhatsapp = source.platform === WHATSAPP_PLATFORM;

	if (source.chatType === "dm") {
		let dmChatId = source.chatId ? String(source.chatId) : "";
		if (dmChatId && isWhatsapp) {
			dmChatId = whatsappCanonical(dmChatId, opts);
		}
		const dmParts = [ns, source.platform, "dm"];
		if (scope) dmParts.push(scope);
		if (dmChatId) {
			dmParts.push(dmChatId);
			if (source.threadId) dmParts.push(String(source.threadId));
			return dmParts.join(":");
		}
		// No chat_id — fall back to the sender's identifier before the bare
		// per-platform sink (WhatsApp-canonicalized, §4.3).
		let dmParticipant = source.userIdAlt || source.userId;
		if (dmParticipant && isWhatsapp) {
			dmParticipant = whatsappCanonical(dmParticipant, opts);
		}
		if (dmParticipant) {
			dmParts.push(String(dmParticipant));
			if (source.threadId) dmParts.push(String(source.threadId));
			return dmParts.join(":");
		}
		if (source.threadId) dmParts.push(String(source.threadId));
		return dmParts.join(":");
	}

	// Group/channel/thread: participant WhatsApp-canonicalized BEFORE the
	// shared-predicate decision (same JID/LID-flip fork bug as the DM case).
	let participantId = source.userIdAlt || source.userId;
	if (participantId && isWhatsapp) {
		participantId = whatsappCanonical(participantId, opts);
	}
	const effectiveThreadId = source.threadId || source.prospectiveThreadId;
	// Rewrite the chat_type slot when keying on a prospective id so the
	// initiating channel message byte-matches the follow-ups arriving IN the
	// thread with chat_type='thread'.
	const chatTypeSlot =
		source.prospectiveThreadId && !source.threadId ? "thread" : source.chatType;

	const keyParts = [ns, source.platform, chatTypeSlot];
	if (scope) keyParts.push(scope);
	if (source.chatId) keyParts.push(String(source.chatId));
	if (effectiveThreadId) keyParts.push(String(effectiveThreadId));

	// ONE shared predicate decides the participant slot (§4.4 invariant):
	// append iff NOT shared AND a participant identifier exists.
	if (!isSharedMultiUserSession(source, flags) && participantId) {
		keyParts.push(String(participantId));
	}
	return keyParts.join(":");
}
