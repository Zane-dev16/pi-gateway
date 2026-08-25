// pi_platforms/whatsapp-personal/behavior — THE WhatsAppBehaviorMixin port
// (gateway/platforms/whatsapp_common.py) as PURE functions. This is the
// shared behavior layer of the Hermes WhatsApp adapters: allow-list / DM /
// group gating, mention detection (explicit @-mentions + configurable regex
// patterns), quoted-reply-to-bot detection, broadcast/channel/newsletter
// filtering, outbound sanitization, and chunk-length budgeting.
//
// The mixin owns no state — every value arrives via the WaGatingPolicy view
// the adapter supplies per check (mirrors "the adapter must set these on
// self before any mixin method is called"). Config-vs-env PRECEDENCE and the
// _dm_allowlist_source live-reload tracking live in the adapter; this module
// is deliberately stateless so both WhatsApp shapes can share it.
//
// Hermes anchors (whatsapp_common.py:WhatsAppBehaviorMixin unless noted):
//   _sanitize_outbound_text            → sanitizeOutboundText
//   _effective_reply_prefix            → effectiveReplyPrefix
//   _outgoing_chunk_limit              → outgoingChunkLimit
//   _coerce_allow_list                 → coerceAllowList
//   _normalize_whatsapp_id             → normalizeWhatsAppId (":" → "@" FIRST occurrence)
//   _is_broadcast_chat                 → isBroadcastChat
//   _open_dm_opted_in                  → openDmOptedIn
//   _matches_whatsapp_allowlist        → matchesAllowlist
//   _is_dm_allowed                     → dmAllowedStrict ("pairing does not imply access")
//   _is_dm_intake_allowed              → dmIntakeAllowed ("pairing handshake path")
//   _is_group_allowed                  → groupAllowed
//   _compile_mention_patterns          → compileMentionPatterns (invalid-skip)
//   _bot_ids_from_message              → botIdsFromMessage
//   _message_is_reply_to_bot           → isReplyToBot
//   _message_mentions_bot              → mentionsBot
//   _message_matches_mention_patterns  → matchesMentionPatterns
//   _clean_bot_mention_text            → cleanBotMentionText
//   _should_process_message            → shouldProcessMessage (EXACT order)

import { normalizeWhatsappIdentifier } from "../../pi_gateway/resolution/whatsapp-identity.js";
import {
	WA_DEFAULT_MODE,
	WA_DEFAULT_REPLY_PREFIX,
	WA_ENV_ALLOW_ALL_USERS,
	WA_ENV_GATEWAY_ALLOW_ALL_USERS,
	WA_MAX_MESSAGE_LENGTH,
	WA_MIN_CHUNK_LIMIT,
} from "./manifest.js";

/** Scoped env reader over WHATSAPP_* names (fail-closed harness shape). */
export type WaEnvReader = (name: string) => string | undefined;

/**
 * The alias-resolution seam (02 §4.3): WhatsApp delivers inbound senders in
 * LID form (`<id>@lid`) while operators configure allowlists with phone
 * numbers, and vice versa. Production resolves through the bridge's
 * lid-mapping files via pi_gateway/resolution/whatsapp-identity
 * (expandWhatsappAliases); tests inject seeded resolvers. The DEFAULT is a
 * no-op resolver whose alias set is the normalized bare identifier — fresh-
 * install semantics (no mapping files ⇒ singleton alias set).
 */
export interface AliasResolver {
	expand(id: string): Set<string>;
}

export const noopAliasResolver: AliasResolver = {
	expand: (id: string) => new Set([normalizeWhatsappIdentifier(id)]),
};

/** Access policy vocabulary (adapter.py __init__ docstring). */
export type WaAccessPolicy = "open" | "allowlist" | "disabled" | "pairing";

/**
 * The gating policy VIEW the host adapter satisfies per check (mixin
 * attribute contract). dmAllowFrom is an ACCESSOR because env-sourced
 * allowlists re-read PER CHECK (_live_dm_allow_from live-reload semantics).
 */
export interface WaGatingPolicy {
	dmPolicy: string;
	/** LIVE DM allowlist (env-sourced re-reads; config-sourced frozen snapshot). */
	dmAllowFrom(): ReadonlySet<string>;
	groupPolicy: string;
	groupAllowFrom: ReadonlySet<string>;
	freeResponseChats: ReadonlySet<string>;
	requireMention: boolean;
	mentionPatterns: readonly RegExp[];
	aliasResolver?: AliasResolver | undefined;
	openDmOptedIn?: (() => boolean) | undefined;
}

/** Inbound bridge message shape (the dict /messages hands back). */
export type WaBridgeMessage = Record<string, unknown>;

// ── truthiness coercion ─────────────────────────────────────────────────────

/** {"true","1","yes","on"} string set + boolean passthrough. */
export function coerceBoolFlag(value: unknown, allowOn = true): boolean {
	if (value === null || value === undefined) return false;
	if (typeof value === "boolean") return value;
	if (typeof value === "number") return value !== 0;
	const lowered = String(value).trim().toLowerCase();
	const accepted = allowOn ? ["true", "1", "yes", "on"] : ["true", "1", "yes"];
	return accepted.includes(lowered);
}

// ── outbound text plane ─────────────────────────────────────────────────────

const OUTBOUND_INVISIBLE_CHARS_RE = /[\u200b\u2060\u2063\ufeff]/g;
const OUTBOUND_ODD_SPACE_RE =
	/[\u00a0\u1680\u180e\u2000-\u200a\u202f\u205f\u3000]/g;

/**
 * _sanitize_outbound_text: remove invisible formatting chars that leak badly
 * in WhatsApp (WORD JOINER U+2060 + NARROW NO-BREAK SPACE render as mojibake)
 * while keeping normal text and emoji joiners intact; odd unicode spaces
 * normalize to plain ASCII space.
 */
export function sanitizeOutboundText(content: string): string {
	if (!content) return content;
	return content
		.replace(OUTBOUND_INVISIBLE_CHARS_RE, "")
		.replace(OUTBOUND_ODD_SPACE_RE, " ");
}

export interface ReplyPrefixInputs {
	/** Resolved WHATSAPP_MODE ("" falls back to "self-chat" — `or` parity). */
	mode?: string | undefined;
	/** config.extra.reply_prefix (None ⇒ fall through to env/default). */
	configuredPrefix?: string | null | undefined;
	/** WHATSAPP_REPLY_PREFIX env read. */
	envPrefix?: string | undefined;
}

/**
 * _effective_reply_prefix: self-chat mode ONLY. A non-self-chat mode yields
 * "" (subclasses without a self-chat concept override to ""); configured
 * prefix beats env beats DEFAULT; "\\n" two-char sequences unescape.
 */
export function effectiveReplyPrefix(inputs: ReplyPrefixInputs): string {
	const mode = inputs.mode || WA_DEFAULT_MODE;
	if (mode !== WA_DEFAULT_MODE) return "";
	if (
		inputs.configuredPrefix !== null &&
		inputs.configuredPrefix !== undefined
	) {
		return inputs.configuredPrefix.replace(/\\n/g, "\n");
	}
	if (inputs.envPrefix !== undefined && inputs.envPrefix !== null) {
		return inputs.envPrefix.replace(/\\n/g, "\n");
	}
	return WA_DEFAULT_REPLY_PREFIX;
}

/**
 * _outgoing_chunk_limit: reserve room for the reply prefix so the final
 * message fits — floored at 1024 so pagination-indicator + fence repair
 * always have room even under a very long user prefix.
 */
export function outgoingChunkLimit(prefixLen: number): number {
	return Math.max(WA_MIN_CHUNK_LIMIT, WA_MAX_MESSAGE_LENGTH - prefixLen);
}

// ── identity helpers ────────────────────────────────────────────────────────

/**
 * _coerce_allow_list: parse allow_from / group_allow_from from config or env.
 * Lists map element-wise; strings split on commas; blanks drop.
 */
export function coerceAllowList(raw: unknown): Set<string> {
	if (raw === null || raw === undefined) return new Set();
	if (Array.isArray(raw)) {
		return new Set(
			raw.map((part) => String(part).trim()).filter((part) => part.length > 0),
		);
	}
	return new Set(
		String(raw)
			.split(",")
			.map((part) => part.trim())
			.filter((part) => part.length > 0),
	);
}

/**
 * _normalize_whatsapp_id: strip + trim, then collapse a device-suffixed JID
 * by replacing the FIRST ":" with "@" (Python str.replace old/new/1 parity —
 * JS String.replace with a string pattern replaces the first occurrence only).
 * Both sides of a membership test normalize identically, which is what the
 * botIds machinery requires.
 */
export function normalizeWhatsAppId(value: unknown): string {
	if (!value) return "";
	let normalized = String(value).trim();
	if (normalized.includes(":") && normalized.includes("@")) {
		normalized = normalized.replace(":", "@");
	}
	return normalized;
}

/**
 * _is_broadcast_chat: pseudo-chats that aren't real conversations — Status
 * updates (Stories), broadcast lists (@broadcast suffix covers
 * status@broadcast plus future variants), and Channel/Newsletter posts. The
 * agent must never reply — even in self-chat mode where the bridge may
 * surface them as fromMe events.
 */
export function isBroadcastChat(chatId: unknown): boolean {
	if (!chatId) return false;
	const cid = String(chatId).trim().toLowerCase();
	if (cid === "status@broadcast") return true;
	if (cid.endsWith("@broadcast") || cid.endsWith("@newsletter")) return true;
	return false;
}

// ── DM/group policies ───────────────────────────────────────────────────────

/** _open_dm_opted_in: GATEWAY_ALLOW_ALL_USERS or WHATSAPP_ALLOW_ALL_USERS. */
export function openDmOptedIn(env: WaEnvReader): boolean {
	if (coerceBoolFlag(env(WA_ENV_GATEWAY_ALLOW_ALL_USERS))) return true;
	return coerceBoolFlag(env(WA_ENV_ALLOW_ALL_USERS));
}

function resolverOf(policy: WaGatingPolicy): AliasResolver {
	return policy.aliasResolver ?? noopAliasResolver;
}

/**
 * _matches_whatsapp_allowlist across phone/LID forms. Order ports exactly:
 * empty list refuses; RAW exact-match fast path (full @g.us group JID or a
 * verbatim entry); "*" wildcard entry; entry's normalized core ∈ candidate's
 * alias set; entry's OWN expanded aliases intersecting the candidate's.
 */
export function matchesAllowlist(
	candidate: string,
	allowFrom: Iterable<string>,
	resolver: AliasResolver,
): boolean {
	const allow = allowFrom instanceof Set ? allowFrom : new Set(allowFrom);
	if (allow.size === 0) return false;
	if (allow.has(candidate)) return true;

	const candidateAliases = resolver.expand(candidate);
	if (candidateAliases.size === 0) return false;
	for (const entry of allow) {
		if (entry === "*") return true;
		if (candidateAliases.has(normalizeWhatsappIdentifier(entry))) return true;
		// Entry may itself be an unmapped form; expand it too so a phone
		// allowlist entry resolves when the inbound sender arrived as a LID.
		for (const alias of resolver.expand(entry)) {
			if (candidateAliases.has(alias)) return true;
		}
	}
	return false;
}

/**
 * _is_dm_intake_allowed — whether a DM may reach gateway intake (pairing
 * handshake path): pairing ADMITS (that is its whole purpose).
 */
export function dmIntakeAllowed(
	policy: WaGatingPolicy,
	senderId: unknown,
	env: WaEnvReader,
): boolean {
	const principal = String(senderId ?? "").trim();
	if (!principal) return false;
	switch (policy.dmPolicy) {
		case "disabled":
			return false;
		case "allowlist":
			return matchesAllowlist(
				principal,
				policy.dmAllowFrom(),
				resolverOf(policy),
			);
		case "pairing":
			return true;
		case "open":
			return policy.openDmOptedIn ? policy.openDmOptedIn() : openDmOptedIn(env);
		default:
			return false;
	}
}

/**
 * _is_dm_allowed — STRICT DM authorization: pairing does NOT imply access.
 */
export function dmAllowedStrict(
	policy: WaGatingPolicy,
	senderId: unknown,
	env: WaEnvReader,
): boolean {
	switch (policy.dmPolicy) {
		case "disabled":
			return false;
		case "allowlist":
			return matchesAllowlist(
				String(senderId ?? "").trim(),
				policy.dmAllowFrom(),
				resolverOf(policy),
			);
		case "pairing":
			return false;
		case "open":
			return policy.openDmOptedIn ? policy.openDmOptedIn() : openDmOptedIn(env);
		default:
			return false;
	}
}

/** _is_group_allowed: group policy has NO pairing admission (⇒ False). */
export function groupAllowed(policy: WaGatingPolicy, chatId: string): boolean {
	switch (policy.groupPolicy) {
		case "disabled":
			return false;
		case "allowlist":
			return matchesAllowlist(
				chatId,
				policy.groupAllowFrom,
				resolverOf(policy),
			);
		case "pairing":
			return false;
		case "open":
			return true;
		default:
			return false;
	}
}

// ── mention machinery ───────────────────────────────────────────────────────

export interface MentionPatternCompileResult {
	compiled: RegExp[];
	/** Patterns skipped for invalid syntax (invalid-skip contract). */
	invalid: string[];
}

/**
 * _compile_mention_patterns: config.extra.mention_patterns wins over the
 * WHATSAPP_MENTION_PATTERNS env carrier (JSON array, else newline-split, else
 * comma-split). Non-string/non-list shapes warn-and-drop; individual invalid
 * regexes SKIP while valid siblings compile (case-insensitive, re.IGNORECASE).
 */
export function compileMentionPatterns(
	extra: Record<string, unknown> | undefined,
	env: WaEnvReader,
): MentionPatternCompileResult {
	let patterns: unknown = extra?.["mention_patterns"];
	if (patterns === null || patterns === undefined) {
		const raw = (env("WHATSAPP_MENTION_PATTERNS") ?? "").trim();
		if (raw) {
			try {
				patterns = JSON.parse(raw);
			} catch {
				let parts = raw
					.split("\n")
					.map((p) => p.trim())
					.filter((p) => p.length > 0);
				if (parts.length === 0) {
					parts = raw
						.split(",")
						.map((p) => p.trim())
						.filter((p) => p.length > 0);
				}
				patterns = parts;
			}
		}
	}
	if (patterns === null || patterns === undefined) {
		return { compiled: [], invalid: [] };
	}
	if (typeof patterns === "string") patterns = [patterns];
	if (!Array.isArray(patterns)) {
		return { compiled: [], invalid: [] }; // warn-and-drop shape
	}

	const compiled: RegExp[] = [];
	const invalid: string[] = [];
	for (const pattern of patterns as unknown[]) {
		if (typeof pattern !== "string" || pattern.trim().length === 0) continue;
		try {
			compiled.push(new RegExp(pattern, "i"));
		} catch {
			invalid.push(pattern); // logged by the caller — never fatal
		}
	}
	return { compiled, invalid };
}

/** _bot_ids_from_message: normalized botIds set from the message data. */
export function botIdsFromMessage(data: WaBridgeMessage): Set<string> {
	const botIds = new Set<string>();
	const raw = data["botIds"];
	for (const candidate of Array.isArray(raw) ? raw : []) {
		const normalized = normalizeWhatsAppId(candidate);
		if (normalized) botIds.add(normalized);
	}
	return botIds;
}

/** _message_is_reply_to_bot: quotedParticipant ∈ botIds. */
export function isReplyToBot(data: WaBridgeMessage): boolean {
	const quotedParticipant = normalizeWhatsAppId(data["quotedParticipant"]);
	if (!quotedParticipant) return false;
	return botIdsFromMessage(data).has(quotedParticipant);
}

/**
 * _message_mentions_bot: explicit mentionedIds ∩ botIds, else a bare-id
 * substring probe over the lowercased body ("@<bare>" or "<bare>").
 */
export function mentionsBot(data: WaBridgeMessage): boolean {
	const botIds = botIdsFromMessage(data);
	if (botIds.size === 0) return false;
	const mentionedRaw = data["mentionedIds"];
	for (const candidate of Array.isArray(mentionedRaw) ? mentionedRaw : []) {
		const nid = normalizeWhatsAppId(candidate);
		if (nid && botIds.has(nid)) return true;
	}

	const body = String(data["body"] ?? "");
	const lowerBody = body.toLowerCase();
	for (const botId of botIds) {
		const bareId = botId.split("@", 1)[0]?.toLowerCase() ?? "";
		if (
			bareId &&
			(lowerBody.includes(`@${bareId}`) || lowerBody.includes(bareId))
		) {
			return true;
		}
	}
	return false;
}

/** _message_matches_mention_patterns: any compiled pattern searches the body. */
export function matchesMentionPatterns(
	data: WaBridgeMessage,
	patterns: readonly RegExp[],
): boolean {
	if (patterns.length === 0) return false;
	const body = String(data["body"] ?? "");
	return patterns.some((pattern) => pattern.test(body));
}

function escapeRegExp(text: string): string {
	return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * _clean_bot_mention_text: strip "@<bareid>[,:-]*\s*" tokens for every known
 * bot id from group bodies; a strip that would EMPTY the text keeps the
 * original (never dispatch a blank turn).
 */
export function cleanBotMentionText(
	text: string,
	data: WaBridgeMessage,
): string {
	if (!text) return text;
	let cleaned = text;
	for (const botId of botIdsFromMessage(data)) {
		const bareId = botId.split("@", 1)[0] ?? "";
		if (bareId) {
			cleaned = cleaned.replace(
				new RegExp(`@${escapeRegExp(bareId)}\\b[:,\\-]*\\s*`, "g"),
				"",
			);
		}
	}
	return cleaned.trim() || text;
}

// ── THE gate (_should_process_message — EXACT order) ────────────────────────

/**
 * _should_process_message port. Order is contractual:
 *   1. broadcast filter (status@broadcast / @broadcast / @newsletter — even
 *      fromMe in self-chat mode);
 *   2. group? group gate : DM intake gate — and DMs that pass intake are
 *      ALWAYS processed (mention machinery never gates DMs);
 *   3. free-response chats bypass;
 *   4. require_mention default FALSE (unset ⇒ groups pass WITHOUT mention);
 *   5. "/"-prefix command bypass;
 *   6. reply-to-bot via quotedParticipant;
 *   7. explicit/bare-id mention probes;
 *   8. configurable regex mention patterns.
 */
export function shouldProcessMessage(
	policy: WaGatingPolicy,
	data: WaBridgeMessage,
	env: WaEnvReader,
): boolean {
	const chatIdRaw = String(data["chatId"] ?? "");
	if (isBroadcastChat(chatIdRaw)) return false;

	const isGroup =
		data["isGroup"] === null || data["isGroup"] === undefined
			? false
			: Boolean(data["isGroup"]);
	if (isGroup) {
		if (!groupAllowed(policy, chatIdRaw)) return false;
	} else {
		const senderId = String(data["senderId"] ?? data["from"] ?? "");
		if (!dmIntakeAllowed(policy, senderId, env)) return false;
		return true;
	}

	// Group messages: mention / free-response ladder.
	if (policy.freeResponseChats.has(chatIdRaw)) return true;
	if (!policy.requireMention) return true;
	const body = String(data["body"] ?? "").trim();
	if (body.startsWith("/")) return true;
	if (isReplyToBot(data)) return true;
	if (mentionsBot(data)) return true;
	return matchesMentionPatterns(data, policy.mentionPatterns);
}
