// pi_platforms/feishu/manifest — FEISHU/LARK MANIFEST DATA (Phase-6 census
// port; A12 ride-along). Per-platform numbers live HERE, never in core
// (Q17/DEC-017). Every constant is TRANSCRIBED from the READ-ONLY Hermes
// reference plugin (plugins/platforms/feishu/) and cited by file:symbol —
// vendor ground truth enters as data; no SDK calls, no network (04 §8).

import type { PluginManifest } from "../kit/index.js";

// ── message limits ──────────────────────────────────────────────────────────

/**
 * adapter.py:FeishuAdapter.MAX_MESSAGE_LENGTH = 8000 — Feishu per-message cap,
 * measured in CODEPOINTS (Python len(); kit default "chars" unit matches).
 */
export const FEISHU_MAX_MESSAGE_UNITS = 8000;

/**
 * adapter.py:FeishuAdapter._SPLIT_THRESHOLD = 4000 — inbound client-split
 * chunks at/above this size raise the text-batch flush delay (near-cap split
 * continuations get the longer window before coalescing).
 */
export const FEISHU_SPLIT_THRESHOLD_UNITS = 4000;

/** adapter.py:_MAX_TEXT_INJECT_BYTES = 100*1024 — single text/* attachment
 * inlined into the prompt below this bound. */
export const FEISHU_MAX_TEXT_INJECT_BYTES = 100 * 1024;

// ── batching windows (inbound coalescing, adapter.py::_dispatch_inbound_event)

/** adapter.py:_DEFAULT_TEXT_BATCH_DELAY_SECONDS = 0.6. */
export const FEISHU_TEXT_BATCH_DELAY_MS = 600;
/** adapter.py env default HERMES_FEISHU_TEXT_BATCH_SPLIT_DELAY_SECONDS = 2.0. */
export const FEISHU_TEXT_BATCH_SPLIT_DELAY_MS = 2000;
/** adapter.py:_DEFAULT_TEXT_BATCH_MAX_MESSAGES = 8. */
export const FEISHU_TEXT_BATCH_MAX_MESSAGES = 8;
/** adapter.py:_DEFAULT_TEXT_BATCH_MAX_CHARS = 4000. */
export const FEISHU_TEXT_BATCH_MAX_CHARS = 4000;
/** adapter.py:_DEFAULT_MEDIA_BATCH_DELAY_SECONDS = 0.8. */
export const FEISHU_MEDIA_BATCH_DELAY_MS = 800;

// ── dedup / replay (event-subscription shape) ───────────────────────────────

/**
 * adapter.py:_FEISHU_DEDUP_TTL_SECONDS = 24*60*60 — message_id dedup window.
 * "TTL must outlast the platform's worst-case redelivery gap" (Slack #4777
 * parity); Feishu's long-conn client redelivers unacked events after
 * reconnects, so exactly-once downstream rides THIS window.
 */
export const FEISHU_DEDUP_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * adapter.py:_DEFAULT_DEDUP_CACHE_SIZE = 2048 (min 32 via env
 * HERMES_FEISHU_DEDUP_CACHE_SIZE) — FIFO eviction of the seen-set.
 */
export const FEISHU_DEDUP_CACHE_SIZE = 2048;
export const FEISHU_DEDUP_CACHE_MIN_SIZE = 32;

/**
 * A12 token dedup: adapter.py:_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS = 15*60 —
 * generic card-click tokens dedup for 15 minutes so a replayed card action
 * never double-fires its synthetic COMMAND event.
 */
export const FEISHU_CARD_ACTION_DEDUP_TTL_MS = 15 * 60 * 1000;

/** adapter.py pending-inbound queue: depth 1000 drop-oldest (:1550), drainer
 * poll 0.25 s and give-up 120 s (:2530–2531). */
export const FEISHU_PENDING_INBOUND_MAX_DEPTH = 1000;
export const FEISHU_PENDING_DRAIN_POLL_MS = 250;
export const FEISHU_PENDING_DRAIN_MAX_WAIT_MS = 120_000;

// ── transport ladder / heartbeat ────────────────────────────────────────────

/** adapter.py:_FEISHU_CONNECT_ATTEMPTS = 3 with 2**attempt-second backoff. */
export const FEISHU_CONNECT_ATTEMPTS = 3;
/** adapter.py:_FEISHU_SEND_ATTEMPTS = 3 with 2**attempt-second backoff
 * (_feishu_send_with_retry :4991). */
export const FEISHU_SEND_ATTEMPTS = 3;

/**
 * adapter.py settings ws_reconnect_nonce = 30 (:430/:1660) and
 * ws_reconnect_interval = 120 s (:431/:1661) — monkey-patched onto the
 * lark_oapi ws Client (:1337 _apply_runtime_ws_overrides): the SDK reconnects
 * on a FIXED interval, giving up after nonce consecutive failures.
 * PROPOSED DEC-043 (smallest Hermes-consistent realization; see report):
 * constant-delay ladder (interval) + attempt budget (nonce ⇒ FATAL).
 */
export const FEISHU_WS_RECONNECT_ATTEMPTS = 30;
export const FEISHU_WS_RECONNECT_INTERVAL_MS = 120_000;

/**
 * Ping/pong: Hermes passes ws_ping_interval/ws_ping_timeout = None into the
 * `websockets` connect kwargs (:1346 _connect_with_overrides), i.e. the
 * LIBRARY defaults govern — 20 s ping interval / 20 s ping timeout. The port
 * owns the socket plane headlessly, so those vendor-library numbers are
 * transcribed here as the staleness watchdog's data (PROPOSED DEC-044).
 */
export const FEISHU_WS_PING_INTERVAL_MS = 20_000;
export const FEISHU_WS_PING_TIMEOUT_MS = 20_000;

/** adapter.py disconnect(): CLOSE frame wait 5.0 s (#10202), thread exit 10 s. */
export const FEISHU_WS_CLOSE_FRAME_WAIT_MS = 5_000;
export const FEISHU_WS_THREAD_EXIT_WAIT_MS = 10_000;

// ── interactive cards (A12) ─────────────────────────────────────────────────

/**
 * CARD SCHEMA CAPS (manifest review vs vendor ground truth): Hermes codes NO
 * element-count or byte caps for Feishu card JSON — the only card-related
 * bounds are transport-level (webhook body ≤ 1 MiB). The builders here
 * therefore declare NONE (an honest absence, not an omission); the shared
 * Block Kit caps (kit block-kit.ts MAX_BLOCKS/MAX_SECTION_TEXT_CHARS) do NOT
 * apply to the feishu native mechanism. Anchor:
 * plugins/platforms/feishu/adapter.py:_build_approval_card (no cap logic),
 * :_handle_webhook_request (_FEISHU_WEBHOOK_MAX_BODY_BYTES only).
 */
export const FEISHU_CARD_SCHEMA_CAPS: readonly string[] = [];

/** adapter.py:_APPROVAL_LABEL_MAP (:252) + button builder labels (:2057). */
export const FEISHU_APPROVAL_BUTTONS = {
	approve_once: { label: "✅ Allow Once", type: "primary" },
	approve_session: { label: "✅ Session", type: "primary" },
	approve_always: { label: "✅ Always", type: "primary" },
	deny: { label: "❌ Deny", type: "danger" },
} as const;

/** adapter.py:_APPROVAL_CHOICE_MAP (:245) — wire value → resolver choice. */
export const FEISHU_APPROVAL_CHOICE_MAP: Readonly<Record<string, string>> = {
	approve_once: "once",
	approve_session: "session",
	approve_always: "always",
	deny: "deny",
};

/** Resolved-card headers (adapter.py:_APPROVAL_LABEL_MAP / _build_resolved_approval_card :2199). */
export const FEISHU_RESOLVED_LABELS: Readonly<Record<string, string>> = {
	once: "✅ Approved once",
	session: "✅ Approved for session",
	always: "✅ Approved permanently",
	deny: "❌ Denied",
};

// ── lifecycle reactions (typing substitute) ─────────────────────────────────

/** adapter.py:_FEISHU_REACTION_IN_PROGRESS / _FAILURE (:278–279). */
export const FEISHU_REACTION_IN_PROGRESS = "Typing";
export const FEISHU_REACTION_FAILURE = "CrossMark";
/** adapter.py:_FEISHU_PROCESSING_REACTION_CACHE_SIZE = 1024 LRU (:283). */
export const FEISHU_PROCESSING_REACTION_CACHE_SIZE = 1024;

// ── error-code classes (adapter.py module constants) ────────────────────────

/** adapter.py:_FEISHU_REPLY_FALLBACK_CODES = {230011, 231003} — reply target
 * withdrawn/deleted ⇒ retry once as a fresh create-message. */
export const FEISHU_REPLY_FALLBACK_CODES: readonly number[] = [230011, 231003];
/** Post-type content rejected → immediate plain-text downgrade resend
 * (adapter.py:_POST_CONTENT_INVALID_RE :193, "content format of the post"). */
export const FEISHU_POST_CONTENT_INVALID_MARKER =
	"content format of the post type is incorrect";

// ── webhook mode limits (adapter.py :228–243) ───────────────────────────────

export const FEISHU_WEBHOOK_MAX_BODY_BYTES = 1024 * 1024;
export const FEISHU_WEBHOOK_RATE_WINDOW_SECONDS = 60;
export const FEISHU_WEBHOOK_RATE_LIMIT_MAX = 120;
export const FEISHU_WEBHOOK_RATE_MAX_KEYS = 4096;
export const FEISHU_WEBHOOK_BODY_TIMEOUT_SECONDS = 30;

// ── Q17 rate tiers (budgets as MANIFEST DATA) ───────────────────────────────

const RATE_TIERS = [
	{
		name: "send-ladder",
		ops: ["send"] as const,
		limit: FEISHU_SEND_ATTEMPTS,
		windowSeconds: 5, // 1s + 2s backoff span of one ladder episode
	},
	{
		name: "text-batch-window",
		ops: ["edit", "draft-start", "draft-stop"] as const,
		limit: 1,
		windowSeconds: FEISHU_TEXT_BATCH_DELAY_MS / 1000,
	},
] as const;

export const FEISHU_RATE_BUDGET = { tiers: RATE_TIERS };

// ── token classes (manifest review vs vendor ground truth) ──────────────────

/**
 * TOKEN CLASSES: the adapter manages NO tokens itself — lark_oapi's client
 * builder (`adapter.py:_build_lark_client` :4981) acquires and refreshes
 * tenant_access_token internally from app_id+app_secret; raw BaseRequests
 * declare `AccessTokenType.TENANT`. There is NO user_access_token anywhere in
 * the plugin (grep-verified); drive-comment requests likewise ride TENANT
 * (feishu_comment.py:37 _build_request). Declared here as manifest data so
 * the census records the credential surface honestly.
 */
export const FEISHU_TOKEN_CLASSES = [
	{
		name: "tenant_access_token",
		manager: "sdk-client-internal",
		source: "FEISHU_APP_ID + FEISHU_APP_SECRET",
		anchor: "plugins/platforms/feishu/adapter.py:_build_lark_client",
	},
] as const;

// ── the PluginManifest (registration path, 04 §4.2) ─────────────────────────

/**
 * Capability mapping note (telegram precedent): Hermes declares
 * splits_long_messages=True ("send() chunks natively"); in THIS kit the BASE
 * owns chunking, so the flag stays FALSE and the BEHAVIOR is preserved via
 * the scalar 8000-unit policy. Streaming: Hermes Feishu overrides NOTHING —
 * base supports_draft_streaming() stays False (no draft/stream protocol; the
 * only progressive surface is edit_message PATCH, unused for previews) — so
 * supportsDraftStreaming() is FALSE here, ALWAYS (silent-lie scan anchor).
 */
export const FEISHU_MANIFEST: PluginManifest = {
	name: "feishu",
	description:
		"Feishu/Lark bot adapter on the long-connection WebSocket family (lark_oapi ws client), with webhook mode and A12 ingress classes (card actions, VC meeting invites, Drive comments)",
	transportShape: "ws",
	requiresEnv: [
		{ name: "FEISHU_APP_ID", description: "Feishu app id" },
		{
			name: "FEISHU_APP_SECRET",
			description: "Feishu app secret",
			password: true,
		},
	],
	optionalEnv: [
		{ name: "FEISHU_DOMAIN", description: "'feishu' (CN) | 'lark' (intl)" },
		{ name: "FEISHU_CONNECTION_MODE", description: "websocket | webhook" },
		{
			name: "FEISHU_ENCRYPT_KEY",
			description: "WS/webhook decrypt key",
			password: true,
		},
		{
			name: "FEISHU_VERIFICATION_TOKEN",
			description: "webhook verification token",
			password: true,
		},
		{
			name: "FEISHU_ALLOWED_USERS",
			description: "comma-separated allowed user ids",
		},
		{
			name: "FEISHU_ALLOW_ALL_USERS",
			description: "dev-only allow-all toggle",
		},
		{
			name: "FEISHU_GROUP_POLICY",
			description: "default group policy (allowlist)",
		},
		{
			name: "FEISHU_REQUIRE_MENTION",
			description: "group mention gate (default true)",
		},
		{
			name: "FEISHU_REACTIONS",
			description: "processing reaction lifecycle (default true)",
		},
		{
			name: "FEISHU_BOT_OPEN_ID",
			description: "stale-cache override of bot identity",
		},
		{ name: "FEISHU_HOME_CHANNEL", description: "cron deliver target" },
	],
	capabilities: {
		supportsAsyncDelivery: true,
		// See mapping note above — kit-owned chunking keeps this FALSE.
		splitsLongMessages: false,
		typedCommandPrefix: "/",
		interactiveResume: true,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	rateBudget: FEISHU_RATE_BUDGET,
};
