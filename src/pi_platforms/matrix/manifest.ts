// pi_platforms/matrix/manifest — MATRIX MANIFEST DATA (Phase-6 census port).
//
// Per-platform numbers live HERE, never in core (Q17/DEC-017). Every constant
// is TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/matrix/) and cited by file:symbol — vendor ground truth
// enters as data; no SDK calls, no network (04 §8 headless rule).

import type { PluginManifest } from "../kit/index.js";

// ── message limits ───────────────────────────────────────────────────────────

/**
 * adapter.py:DEFAULT_MAX_MESSAGE_LENGTH = 16000 — outbound chunk budget in
 * CHARACTERS (Python len() parity → code points, kit "chars" unit).
 */
export const MATRIX_MAX_MESSAGE_CHARS = 16000;

/** adapter.py:MATRIX_MAX_MESSAGE_LENGTH_CEILING = 65535 — config clamp top. */
export const MATRIX_MAX_MESSAGE_LENGTH_CEILING = 65535;

/** adapter.py:_resolve_max_message_length — resolved values clamp to ≥500. */
export const MATRIX_MAX_MESSAGE_LENGTH_FLOOR = 500;

// ── sync transport timing (adapter.py:_sync_loop) ───────────────────────────

/**
 * adapter.py:_sync_loop — the sync long-poll timeout is 30 s
 * (`client.sync(since=next_batch, timeout=30000)`).
 */
export const MATRIX_SYNC_LONGPOLL_TIMEOUT_MS = 30_000;

/**
 * adapter.py:_sync_loop — the long-poll is wrapped in `asyncio.wait_for(...,
 * timeout=45.0)`: "Long-poll is 30s, so 45s gives 15s slack for network
 * drain." This IS the stuck-probe watchdog (TCP-level hangs the long-poll
 * cannot catch); TWO consecutive watchdog timeouts feed the recovery ladder.
 */
export const MATRIX_SYNC_WATCHDOG_TIMEOUT_MS = 45_000;

/**
 * adapter.py:_sync_loop — transient sync errors retry after a flat 5 s:
 * "Matrix: sync error: %s — retrying in 5s".
 */
export const MATRIX_SYNC_RETRY_DELAY_MS = 5_000;

/**
 * Family bounding (04 §3.1 ladder): unbounded Hermes retrying is bounded to
 * this many recovery attempts before a LOUD fatal (proposed DEC text in the
 * port report; smallest family-consistent bound — polling MAX_CONFLICT_RETRIES).
 */
export const MATRIX_MAX_RECOVERY_ATTEMPTS = 5;

// ── intake filters (adapter.py __init__ / _on_room_message) ─────────────────

/**
 * adapter.py:_STARTUP_GRACE_SECONDS = 5 — initial-sync backlog events older
 * than startup−grace are dropped ("Startup grace: ignore old messages from
 * initial sync").
 */
export const MATRIX_STARTUP_GRACE_SECONDS = 5;

/**
 * adapter.py:__init__ (~L1253) — event dedup keeps a bounded deque of the
 * newest 1000 processed event ids (oldest evicted ⇒ re-deliverable again).
 */
export const MATRIX_EVENT_DEDUP_CAPACITY = 1000;

// ── room identity cache (A9 channel directory + alias overlay) ──────────────

/** adapter.py:__init__ (~L1240) — identity cache TTL 60 s, max 256 entries. */
export const MATRIX_ROOM_IDENTITY_TTL_MS = 60_000;
export const MATRIX_ROOM_IDENTITY_CACHE_MAX = 256;

// ── typing / reactions (A1/A11 ride-alongs) ─────────────────────────────────

/** adapter.py:send_typing — `set_typing(timeout=30000)`; stop_typing uses 0. */
export const MATRIX_TYPING_TIMEOUT_MS = 30_000;

/** adapter.py:on_processing_start — 👀 while processing (U+1F440). */
export const MATRIX_REACTION_EYES = "\u{1F440}";
/** adapter.py:on_processing_complete — ✅ success / ❌ failure swap. */
export const MATRIX_REACTION_SUCCESS = "\u2705";
export const MATRIX_REACTION_FAILURE = "\u274C";

// ── bang-command alias grammar (adapter.py:_MATRIX_BANG_COMMAND_RE) ─────────

/**
 * adapter.py:_MATRIX_BANG_COMMAND_RE — `!command` tokens normalize to
 * `/command` when the name resolves against known commands; ordinary
 * exclamations stay chat text. Underscore→hyphen variants resolve too
 * (adapter.py:_resolve_matrix_bang_command candidates).
 */
export const MATRIX_BANG_COMMAND_PATTERN =
	"^!([A-Za-z][A-Za-z0-9_-]*)(?=$|\\s)(.*)$";

export function extractBangCommand(
	text: string,
): { name: string; rest: string } | null {
	const m = new RegExp(MATRIX_BANG_COMMAND_PATTERN, "s").exec(text);
	if (m === null) return null;
	return { name: m[1] ?? "", rest: m[2] ?? "" };
}

/**
 * adapter.py:_normalize_matrix_bang_command parity: try the raw lowercased
 * token then its hyphenated variant; emit whichever resolves (alias
 * passthrough). Unresolvable tokens stay normal chat text.
 */
export function normalizeBangCommand(
	text: string,
	isKnownCommand: (name: string) => boolean,
): string {
	if (!text.startsWith("!")) return text;
	const parsed = extractBangCommand(text);
	if (parsed === null) return text;
	const candidates = [parsed.name.toLowerCase()];
	const hyphenated = parsed.name.toLowerCase().replace(/_/g, "-");
	if (hyphenated !== candidates[0]) candidates.push(hyphenated);
	for (const candidate of candidates) {
		if (isKnownCommand(candidate)) return `/${candidate}${parsed.rest}`;
	}
	return text;
}

// ── outbound content building (adapter.py:_build_text_message_content) ──────

/**
 * adapter.py:_OUTBOUND_MENTION_RE shape — @user:server pills in outbound text
 * are collected into `m.mentions.user_ids` (MSC3952), deduplicated.
 * (Ported as the conservative MXID-shaped subset; displayname pills are NOT
 * linkified here because the HTML renderer is a documented exclusion.)
 */
export function extractOutboundMentionUserIds(text: string): string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const m of text.matchAll(/@([A-Za-z0-9._=/+-]+):([A-Za-z0-9.-]+)/g)) {
		const mxid = `@${m[1]}:${m[2]}`;
		if (!seen.has(mxid)) {
			seen.add(mxid);
			out.push(mxid);
		}
	}
	return out;
}

/**
 * adapter.py:_build_text_message_content builds `{msgtype, body,
 * m.mentions?, format?, formatted_body?}`. The markdown→HTML renderer behind
 * `formatted_body` is a DOCUMENTED EXCLUSION of this port (see report);
 * outbound payloads carry the structured body + mention metadata only.
 */
export interface MatrixOutboundContent {
	msgtype: string;
	body: string;
	mentionUserIds: string[];
}

export function buildTextMessageContent(
	text: string,
	msgtype = "m.text",
): MatrixOutboundContent {
	return {
		msgtype,
		body: text,
		mentionUserIds: extractOutboundMentionUserIds(text),
	};
}

// ── the PluginManifest (registration path, 04 §4.2) ─────────────────────────

/**
 * Capability mapping note (telegram-manifest parity): Hermes'
 * splits_long_messages=True means send() chunks natively via
 * truncate_message(16000). In THIS kit the BASE owns chunking, and the
 * behavior is preserved by declaring TRUE plus an adapter deliverText that
 * runs THE kit chunker with the scalar 16000-code-point policy (the flag
 * being true makes the base skip its own split; the adapter supplies it).
 */
export const MATRIX_MANIFEST: PluginManifest = {
	name: "matrix",
	description:
		"Matrix homeserver adapter on the long-poll sync transport family (polling shape)",
	transportShape: "polling",
	requiresEnv: [
		{
			name: "MATRIX_HOMESERVER",
			description: "Matrix homeserver URL (e.g. https://matrix.org)",
			password: false,
		},
		{
			name: "MATRIX_ACCESS_TOKEN",
			description:
				"Matrix access token (or use MATRIX_PASSWORD for password login)",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "MATRIX_PASSWORD",
			description: "Account password (alternative to MATRIX_ACCESS_TOKEN)",
			password: true,
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		splitsLongMessages: true,
		typedCommandPrefix: "/",
		interactiveResume: true,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	// Q17 review note: the Hermes matrix plugin declares NO client-side rate
	// tiers (homeserver-side M_LIMIT_EXCEEDED retry_after_ms is honored at the
	// typing/send sites instead) — rateBudget intentionally absent.
};
