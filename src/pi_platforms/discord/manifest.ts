// pi_platforms/discord/manifest — Discord platform MANIFEST DATA (04 §2/§4;
// Q17/DEC-017 discipline): limits, rate buckets, component caps, and gateway
// protocol constants enter as DATA transcribed from vendor/API ground truth
// and the Hermes reference plugin — per-platform numbers live in adapter
// manifests, never in core.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/discord/adapter.py — constants block (:81-1057),
//     config reads, recovery/backfill knobs, liveness knobs as cited inline.
//   plugins/platforms/discord/recovery.py — ledger retention/status machine.
//
// NOTE on rate buckets: Hermes delegates REST limiting to discord.py's
// internal limiter and only OBSERVES 429s (`_is_discord_rate_limit`
// adapter.py:2336-2369, `_extract_discord_retry_after` adapter.py:2312-2335).
// The per-route bucket table below is VENDOR GROUND TRUTH (X-RateLimit-Bucket
// semantics simplified to data per Q17) so Pi can gate BEFORE egress instead
// of discovering limits by failing.

/** Plugin manifest data (kit registration.ts shape). */
export const DISCORD_MANIFEST = {
	name: "discord",
	description: "Discord bot platform adapter (persistent-ws Gateway v10)",
	transportShape: "ws" as const,
	requiredEnv: ["DISCORD_BOT_TOKEN"],
	optionalEnv: [
		"DISCORD_ALLOWED_USERS",
		"DISCORD_ALLOW_ALL_USERS",
		"DISCORD_HOME_CHANNEL",
	],
};

// ── message & embed limits (adapter.py constants) ────────────────────────────

/** Hard per-message character cap. adapter.py:1045 `MAX_MESSAGE_LENGTH`. */
export const MESSAGE_LENGTH_MAX = 2000;
/**
 * Batching split-point heuristic — a trailing chunk at/over this budget waits
 * for client-split siblings before dispatch. adapter.py:1046.
 */
export const SPLIT_THRESHOLD = 1900;
/** Max chunks per response; overflow notice replaces the tail. adapter.py:1050-1057. */
export const MAX_SPLIT_MESSAGES = 8;
/**
 * Embed description render budget (limit 4096 minus safety margin).
 * adapter.py:7569-7572 ("Discord embed description limit is 4096"), 7639-7642.
 */
export const EMBED_DESCRIPTION_LIMIT = 4096;
export const EMBED_DESCRIPTION_BUDGET = 4088;
export const EMBED_TITLE_LIMIT = 256;
/** Vendor embed caps (transcribed vendor ground truth). */
export const EMBED_FIELDS_MAX = 25;
export const EMBED_FIELD_VALUE_LIMIT = 1024;
export const EMBED_FOOTER_TEXT_LIMIT = 2048;
export const EMBED_TOTAL_LIMIT = 6000;

// ── component caps (adapter.py:87-91, :7667-7670; vendor component model) ────

export const COMPONENT_TYPE_ACTION_ROW = 1;
export const COMPONENT_TYPE_BUTTON = 2;
export const COMPONENT_TYPE_STRING_SELECT = 3;

/** Rows per view. adapter.py:89 `_DISCORD_SELECT_MAX_ROWS`. */
export const COMPONENT_MAX_ROWS = 5;
/** Buttons per action row (vendor grid math: 5×5 ⇒ 24 clarify choices + Other). */
export const BUTTONS_PER_ROW = 5;
/** Select options per menu. adapter.py:88 `_DISCORD_SELECT_MAX_OPTIONS`. */
export const SELECT_MAX_OPTIONS = 25;
/** Select option label/value length cap. adapter.py:87 `_DISCORD_SELECT_FIELD_LIMIT`. */
export const SELECT_FIELD_LIMIT = 100;
/** Button label cap, overflow ellipsized with `…`. adapter.py:90-91. */
export const BUTTON_LABEL_LIMIT = 80;
/** Clarify choice buttons max (+1 "Other"). adapter.py:7667-7670. */
export const CLARIFY_CHOICES_MAX = 24;
/** Vendor custom_id cap (bytes); DEC-016 keeps ids ≤64B (strictest platform). */
export const CUSTOM_ID_MAX_BYTES = 100;
/**
 * Interactive view timeout default, clamped [30,900]s and kept BELOW Discord's
 * 15-minute interaction-token expiry. adapter.py:987-1027
 * (`_DISCORD_PROMPT_TIMEOUT_DEFAULT/MIN/MAX`).
 */
export const VIEW_TIMEOUT_DEFAULT_S = 300;
export const VIEW_TIMEOUT_MIN_S = 30;
export const VIEW_TIMEOUT_MAX_S = 900;
/** Interaction callback ack deadline parity with the kit registry window. */
export { SLACK_ACK_WINDOW_MS as INTERACTION_ACK_WINDOW_MS } from "../kit/index.js";

// ── threads ───────────────────────────────────────────────────────────────────

/** Valid auto-archive durations (minutes). adapter.py:85. */
export const THREAD_AUTO_ARCHIVE_VALID = [60, 1440, 4320, 10080] as const;
export const THREAD_AUTO_ARCHIVE_DEFAULT = 1440;
/**
 * Thread name vendor cap — 80, in BOTH upstream budgets but different units:
 * derive path (:7200-7216) caps at 80 PYTHON CODE POINTS, the semantic-rename
 * path (:7276-7279 + run.py:_sanitize_discord_thread_title) at 80 UTF-16 CODE
 * UNITS truncated via utf16 helpers. The constant names the rename budget
 * (the lane that will consume it when the deferred session-title consumer
 * lands); deriveThreadName applies it under code-point semantics with the
 * quirk documented at the call site.
 */
export const THREAD_NAME_MAX_UTF16_UNITS = 80;
export const THREAD_NAME_FALLBACK = "Pi";

// ── forum channels (adapter.py:_is_forum_parent :7892, _send_to_forum :3593,
//    _derive_forum_thread_name :9879-9888) ────────────────────────────────────

/** Vendor channel type for forum channels — plain sends are REJECTED. */
export const CHANNEL_TYPE_FORUM = 15;
/** Forum post name cap (first line of content). adapter.py:9888. */
export const FORUM_THREAD_NAME_MAX_CHARS = 100;
/** Empty-name fallback. adapter.py:9885. */
export const FORUM_THREAD_NAME_FALLBACK = "New Post";

// ── gateway IDENTIFY intents (adapter.py:connect :1345-1353, discord.py
//    2.7.1 Intents bit positions — Hermes venv ground truth) ───────────────

export const DISCORD_INTENT_GUILDS = 1 << 0;
export const DISCORD_INTENT_MODERATION = 1 << 2;
export const DISCORD_INTENT_EXPRESSIONS = 1 << 3;
export const DISCORD_INTENT_INTEGRATIONS = 1 << 4;
export const DISCORD_INTENT_WEBHOOKS = 1 << 5;
export const DISCORD_INTENT_INVITES = 1 << 6;
export const DISCORD_INTENT_VOICE_STATES = 1 << 7;
export const DISCORD_INTENT_GUILD_MESSAGES = 1 << 9;
export const DISCORD_INTENT_DM_MESSAGES = 1 << 10;
export const DISCORD_INTENT_GUILD_REACTIONS = 1 << 11;
export const DISCORD_INTENT_DM_REACTIONS = 1 << 12;
export const DISCORD_INTENT_GUILD_TYPING = 1 << 13;
export const DISCORD_INTENT_DM_TYPING = 1 << 14;
export const DISCORD_INTENT_MESSAGE_CONTENT = 1 << 15;
export const DISCORD_INTENT_GUILD_SCHEDULED_EVENTS = 1 << 16;
export const DISCORD_INTENT_AUTO_MODERATION_CONFIGURATION = 1 << 20;
export const DISCORD_INTENT_AUTO_MODERATION_EXECUTION = 1 << 21;
export const DISCORD_INTENT_GUILD_POLLS = 1 << 24;
export const DISCORD_INTENT_DM_POLLS = 1 << 25;

/**
 * discord.py `Intents.default()` — every NON-privileged intent (=53575421).
 */
export const DISCORD_INTENTS_DEFAULT =
	DISCORD_INTENT_GUILDS |
	DISCORD_INTENT_MODERATION |
	DISCORD_INTENT_EXPRESSIONS |
	DISCORD_INTENT_INTEGRATIONS |
	DISCORD_INTENT_WEBHOOKS |
	DISCORD_INTENT_INVITES |
	DISCORD_INTENT_VOICE_STATES |
	DISCORD_INTENT_GUILD_MESSAGES |
	DISCORD_INTENT_DM_MESSAGES |
	DISCORD_INTENT_GUILD_REACTIONS |
	DISCORD_INTENT_DM_REACTIONS |
	DISCORD_INTENT_GUILD_TYPING |
	DISCORD_INTENT_DM_TYPING |
	DISCORD_INTENT_GUILD_SCHEDULED_EVENTS |
	DISCORD_INTENT_AUTO_MODERATION_CONFIGURATION |
	DISCORD_INTENT_AUTO_MODERATION_EXECUTION |
	DISCORD_INTENT_GUILD_POLLS |
	DISCORD_INTENT_DM_POLLS;

/**
 * THE effective IDENTIFY bitmask Hermes sends: Intents.default() +
 * message_content + dm_messages + guild_messages (members/voice_states only
 * when allowlist username resolution requires them). VENDOR WIRE FORM IS AN
 * INTEGER BITMASK — a string array never comes online.
 */
export const DISCORD_IDENTIFY_INTENTS =
	DISCORD_INTENTS_DEFAULT |
	DISCORD_INTENT_MESSAGE_CONTENT |
	DISCORD_INTENT_GUILD_MESSAGES |
	DISCORD_INTENT_DM_MESSAGES;

// ── ping / typing safety (A13/A11) ───────────────────────────────────────────

/**
 * Outbound ping safety: allowed_mentions DENIED for everyone/roles by default;
 * users + replied-user allowed — serialized in the VENDOR wire shape
 * (discord.py AllowedMentions.to_dict -> {"parse":["users"],"replied_user":true}).
 * adapter.py:519-552 `_build_allowed_mentions`. The vendor DROPS unknown keys,
 * so camelCase booleans would deserialize to parse=[] and suppress ALL pings.
 */
export const ALLOWED_MENTIONS_DEFAULTS = {
	parse: ["users"],
	replied_user: true,
} as const;
/** Typing refresh cadence — indicator lasts ~10s. adapter.py:5582-5636. */
export const TYPING_INTERVAL_SECONDS = 12;

// ── gateway session health (A13 liveness knobs) ─────────────────────────────

/** Heartbeat ACK max age before a probe counts stale. adapter.py:1138-1141. */
export const HEARTBEAT_ACK_MAX_AGE_SECONDS = 60;
/** Max tolerated gateway latency. adapter.py:1142-1146. */
export const WEBSOCKET_MAX_LATENCY_SECONDS = 30;
/** Liveness probe cadence. adapter.py:1130-1133. */
export const LIVENESS_INTERVAL_SECONDS = 15;
/** Consecutive failed probes before the socket is reaped. adapter.py:1134-1137. */
export const LIVENESS_FAILURE_THRESHOLD = 2;
/** Ready-wait timeout. adapter.py:554-567 (default 30.0s). */
export const READY_TIMEOUT_SECONDS = 30;

// ── recovery ledger + missed-dispatch sweep (A13; recovery.py) ───────────────

export const RECOVERY_RETENTION_DAYS = 30; // recovery.py:26 _RETENTION_DAYS
/** queued|processing rows within this window count as actively claimed. adapter.py:3085-3104. */
export const ACTIVE_CLAIM_WINDOW_SECONDS = 600; // 10 minutes
export const BACKFILL_WINDOW_SECONDS_DEFAULT = 21_600; // 6h; adapter.py:2511-2522
export const BACKFILL_WINDOW_FLOOR_SECONDS = 60;
export const BACKFILL_LIMIT_DEFAULT = 100; // clamp [1,500]; adapter.py:2524-2535
export const BACKFILL_MAX_DISPATCHES_DEFAULT = 10; // clamp [1,100]; adapter.py:2537-2548
/** Ledger status machine. recovery.py:71-87. */
export const RECOVERY_STATUSES = [
	"discovered",
	"queued",
	"processing",
	"responded",
	"processed",
	"cancelled",
	"failed",
] as const;
export type RecoveryStatus = (typeof RECOVERY_STATUSES)[number];

// ── admission policy (Hermes pipeline order) ─────────────────────────────────

/** Admissible message types: default(0) and reply(19) only. adapter.py:1553-1646. */
export const ADMISSIBLE_MESSAGE_TYPES = [0, 19] as const;
export const REQUIRE_MENTION_DEFAULT = true; // adapter.py:6544-6551
export const IGNORE_NO_MENTION_DEFAULT = true; // adapter.py:1627-1631
export const AUTO_THREAD_DEFAULT = true; // adapter.py:8160-8195
export type AllowBots = "none" | "mentions" | "all"; // adapter.py:6697-6700
export const ALLOW_BOTS_DEFAULT: AllowBots = "none";
/** Attachment download cap (0=unlimited). adapter.py:6568-6590. */
export const MAX_ATTACHMENT_BYTES_DEFAULT = 33_554_432; // 32 MiB

// ── gateway protocol opcodes/close codes (vendor ground truth, as data) ──────

export const GATEWAY_OPCODES = {
	DISPATCH: 0,
	HEARTBEAT: 1,
	IDENTIFY: 2,
	PRESENCE_UPDATE: 3,
	VOICE_STATE_UPDATE: 4,
	RESUME: 6,
	RECONNECT: 7,
	REQUEST_MEMBERS: 8,
	INVALID_SESSION: 9,
	HELLO: 10,
	HEARTBEAT_ACK: 11,
} as const;

/** Gateway close codes the adapter classifies (vendor close-code table). */
export const GATEWAY_CLOSE_CODES = {
	UNKNOWN_ERROR: 4000,
	UNKNOWN_OPCODE: 4001,
	AUTHENTICATION_FAILED: 4004,
	RATE_LIMITED: 4008,
	SESSION_TIMED_OUT: 4009,
	INVALID_SEQ: 4007,
} as const;

/** Default HELLO heartbeat_interval (ms). Vendor nominal value. */
export const HEARTBEAT_INTERVAL_MS_DEFAULT = 45_000;

// ── Q17 rate buckets (X-RateLimit-Bucket semantics simplified to DATA) ──────

/** Route ops the bucket table budgets. */
export type RateRouteOp = "send" | "edit" | "typing" | "thread-create" | "ack";

export interface RateBucketSpec {
	/** Bucket id as the server would report it (X-RateLimit-Bucket). */
	id: string;
	limit: number;
	windowSeconds: number;
	routes: readonly RateRouteOp[];
	/** Scope key granularity: per-channel buckets key on chatId; global does not. */
	scope: "channel" | "global";
}

/**
 * Per-route buckets. The channel-messages family (send/edit/typing/thread
 * creation) shares ONE per-channel bucket in vendor behavior; a global bucket
 * caps aggregate throughput. Transcribed vendor ground truth (Q17 manifest
 * data) — see file header note on Hermes' observe-only posture.
 */
export const RATE_BUCKETS: readonly RateBucketSpec[] = [
	{
		id: "channel-messages",
		limit: 5,
		windowSeconds: 5,
		routes: ["send", "edit", "typing", "thread-create"],
		scope: "channel",
	},
	{
		id: "global",
		limit: 50,
		windowSeconds: 1,
		routes: ["send", "edit", "typing", "thread-create", "ack"],
		scope: "global",
	},
];

/** Server-authoritative retry_after floor. adapter.py:2312-2335 (clamp ≥1.0s). */
export const RETRY_AFTER_FLOOR_SECONDS = 1;
/** Cap on sleeping inside vendor-driven limiter paths. adapter.py:81. */
export const RATE_LIMIT_SLEEP_CAP_SECONDS = 30;

// ── native-stream feature-gate markers (capability latch input) ──────────────

/**
 * Error markers meaning the EDIT/streaming FEATURE itself is unavailable for
 * this session (vendor error-code phrases + codes as data):
 *   50001 Missing Access · 50013 Missing Permissions ·
 *   50005 Cannot edit a message authored by another user ·
 *   10003 Unknown Channel · 30005 thread-archive-state rejections.
 * Transient shapes (network loss, 429, 5xx) NEVER match.
 */
export const NATIVE_STREAM_GATE_MARKERS: readonly string[] = [
	"missing access",
	"missing permissions",
	"cannot edit a message authored by another user",
	"unknown channel",
	"50001",
	"50013",
	"50005",
	"10003",
];
