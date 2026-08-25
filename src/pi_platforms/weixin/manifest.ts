// pi_platforms/weixin/manifest — WeChat iLink bot platform manifest.
//
// ALL numbers below are MANIFEST DATA transcribed from the Hermes reference
// (READ-ONLY): gateway/platforms/weixin.py module constants — cited per
// constant (Q17: per-platform numbers live in manifests, never core).

export const WEIXIN_COPY_LINE_WIDTH = 120; // weixin.py:WEIXIN_COPY_LINE_WIDTH
export const ILINK_BASE_URL = "https://ilinkai.weixin.qq.com"; // ILINK_BASE_URL
export const WEIXIN_CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"; // WEIXIN_CDN_BASE_URL
export const ILINK_APP_ID = "bot"; // ILINK_APP_ID
export const CHANNEL_VERSION = "2.2.0"; // CHANNEL_VERSION
/** (2<<16)|(2<<8)|0 — packed client version sent as iLink-App-ClientVersion. */
export const ILINK_APP_CLIENT_VERSION = 0x020200;

export const EP_GET_UPDATES = "ilink/bot/getupdates"; // EP_GET_UPDATES
export const EP_SEND_MESSAGE = "ilink/bot/sendmessage"; // EP_SEND_MESSAGE
export const EP_SEND_TYPING = "ilink/bot/sendtyping"; // EP_SEND_TYPING
export const EP_GET_CONFIG = "ilink/bot/getconfig"; // EP_GET_CONFIG

export const LONG_POLL_TIMEOUT_MS = 35_000; // LONG_POLL_TIMEOUT_MS
export const API_TIMEOUT_MS = 15_000; // API_TIMEOUT_MS
export const CONFIG_TIMEOUT_MS = 10_000; // CONFIG_TIMEOUT_MS

export const MAX_CONSECUTIVE_FAILURES = 3; // MAX_CONSECUTIVE_FAILURES
export const RETRY_DELAY_SECONDS = 2; // RETRY_DELAY_SECONDS
export const BACKOFF_DELAY_SECONDS = 30; // BACKOFF_DELAY_SECONDS
export const SESSION_EXPIRED_PAUSE_S = 600; // _poll_loop session-expired pause
export const SESSION_EXPIRED_ERRCODE = -14; // SESSION_EXPIRED_ERRCODE
export const RATE_LIMIT_ERRCODE = -2; // RATE_LIMIT_ERRCODE (iLink frequency limit)
export const MESSAGE_DEDUP_TTL_SECONDS = 300; // MESSAGE_DEDUP_TTL_SECONDS

// ── media/item type codes (weixin.py) ────────────────────────────────────────
export const ITEM_TEXT = 1;
export const ITEM_IMAGE = 2;
export const ITEM_VOICE = 3;
export const ITEM_FILE = 4;
export const ITEM_VIDEO = 5;

export const MSG_TYPE_USER = 1; // MSG_TYPE_USER
export const MSG_TYPE_BOT = 2; // MSG_TYPE_BOT
export const MSG_STATE_FINISH = 2; // MSG_STATE_FINISH

export const TYPING_START = 1; // TYPING_START
export const TYPING_STOP = 2; // TYPING_STOP

// ── send pacing (__init__ parity) ────────────────────────────────────────────
export const SEND_CHUNK_DELAY_S = 1.5; // WEIXIN_SEND_CHUNK_DELAY_SECONDS default
export const SEND_CHUNK_RETRIES = 4; // WEIXIN_SEND_CHUNK_RETRIES default
export const SEND_CHUNK_RETRY_DELAY_S = 1.0; // WEIXIN_SEND_CHUNK_RETRY_DELAY_SECONDS
/** Rate-limit backoff multiplier over the plain retry delay (_send_text_chunk). */
export const RATE_LIMIT_BACKOFF_FACTOR = 3;

// ── rate-limit circuit breaker (__init__ parity) ─────────────────────────────
export const RATE_LIMIT_CIRCUIT_THRESHOLD = 1; // WEIXIN_RATE_LIMIT_CIRCUIT_THRESHOLD
export const RATE_LIMIT_CIRCUIT_WINDOW_S = 30.0; // WEIXIN_RATE_LIMIT_CIRCUIT_WINDOW_SECONDS
export const RATE_LIMIT_CIRCUIT_OPEN_S = 30.0; // WEIXIN_RATE_LIMIT_CIRCUIT_OPEN_SECONDS

// ── text debounce batching (__init__ parity) ────────────────────────────────
export const TEXT_BATCH_DELAY_S = 3.0; // text_batch_delay_seconds default
export const TEXT_BATCH_SPLIT_DELAY_S = 5.0; // text_batch_split_delay_seconds default
/** iLink chunks at ~2048 chars; batches at/above this use the SPLIT delay. */
export const TEXT_BATCH_SPLIT_THRESHOLD = 1800; // weixin.py:_SPLIT_THRESHOLD

// ── typing ticket cache (weixin.py:TypingTicketCache) ───────────────────────
export const TYPING_TICKET_TTL_S = 600.0;

// ── capabilities AS DATA (04 §2) ─────────────────────────────────────────────
/**
 * Hermes parity: WeixinAdapter overrides NEITHER supports_async_delivery nor
 * interactive_resume (base defaults hold); splitting rides the shared kit
 * chunker at the conformance surface (the platform's own delivery-unit
 * splitter ships as data in text-splitting.ts with its own contracts).
 */
export const WEIXIN_CAPABILITIES = Object.freeze({
	typedCommandPrefix: "/",
});

// ── Q17 rate budget ──────────────────────────────────────────────────────────
/** iLink frequency limits surface as errcode -2 (backoff-and-retry) plus the
 * adapter-side circuit breaker (threshold 1 / window 30s / open 30s). */
export const WEIXIN_RATE_BUDGET = Object.freeze({
	tiers: [
		{
			name: "ilink-frequency-limit",
			ops: ["send"] as const,
			limit: 1,
			windowSeconds: RATE_LIMIT_CIRCUIT_WINDOW_S,
		},
	],
});

// ── plugin manifest (04 §4.2 registration flow) ──────────────────────────────
import type { PluginManifest } from "../kit/index.js";

export const WEIXIN_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "weixin",
	description:
		"WeChat iLink bot channel (long-polling shape, AES-128-ECB CDN media, copy-friendly delivery)",
	transportShape: "polling" as const,
	requiresEnv: [
		{
			name: "WEIXIN_TOKEN",
			description: "iLink bot token (Bearer); unset ⇒ loud disable",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "WEIXIN_ACCOUNT_ID",
			description: "Account id for multi-account sync-buf persistence",
		},
	],
	capabilities: WEIXIN_CAPABILITIES,
	rateBudget: WEIXIN_RATE_BUDGET,
});
