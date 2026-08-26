// pi_platforms/qqbot/manifest — QQBot official-bot platform manifest.
//
// ALL numbers below are MANIFEST DATA transcribed from the Hermes reference
// (READ-ONLY): gateway/platforms/qqbot/constants.py, keyboards.py,
// chunked_upload.py, adapter.py — cited per constant (Q17: per-platform
// numbers live in manifests, never core; DEC-017 trust posture).
//
// Hermes anchors:
//   qqbot/constants.py:QQBOT_VERSION / API_BASE / TOKEN_URL / GATEWAY_URL_PATH
//   qqbot/constants.py:RECONNECT_BACKOFF / RATE_LIMIT_DELAY / MAX_MESSAGE_LENGTH
//   qqbot/adapter.py:QQAdapter._send_identify (intents bitmask)
//   qqbot/adapter.py:_ensure_token (refresh margin 60s)
//   qqbot/chunked_upload.py:_MD5_10M_SIZE and module constants

export const QQBOT_VERSION = "1.1.0"; // constants.py:QQBOT_VERSION
export const QQBOT_API_BASE = "https://api.sgroup.qq.com"; // constants.py:API_BASE
export const QQBOT_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"; // constants.py:TOKEN_URL
export const QQBOT_GATEWAY_URL_PATH = "/gateway"; // constants.py:GATEWAY_URL_PATH

// ── User-Agent (qqbot/utils.py:build_user_agent) ─────────────────────────────
/**
 * Descriptive User-Agent carried on EVERY authenticated REST leg — gateway-url
 * GET, every _api_request, interaction ACK, and the q.qq.com onboard calls
 * (utils.py:get_api_headers notes q.qq.com answers anti-bot challenge pages
 * without an identifying UA). Hermes format:
 *   `QQBotAdapter/<version> (Python/<py>; <os>; Hermes/<version>)`
 * This port preserves the vendor contract with truthful runtime tokens —
 * the pi-gateway identity tail is DEC-060 (branding tokens).
 */
export const QQBOT_USER_AGENT = `QQBotAdapter/${QQBOT_VERSION} (Node/${process.version}; ${process.platform}; pi-gateway)`;

// ── timeouts & retry (constants.py) ──────────────────────────────────────────
export const QQBOT_DEFAULT_API_TIMEOUT_S = 30.0; // DEFAULT_API_TIMEOUT
export const QQBOT_FILE_UPLOAD_TIMEOUT_S = 120.0; // FILE_UPLOAD_TIMEOUT
export const QQBOT_CONNECT_TIMEOUT_SECONDS = 20.0; // CONNECT_TIMEOUT_SECONDS
/** Fixed reconnect tiers — DATA from constants.py:RECONNECT_BACKOFF. */
export const QQBOT_RECONNECT_BACKOFF_S: readonly number[] = [2, 5, 10, 30, 60];
export const QQBOT_MAX_RECONNECT_ATTEMPTS = 100; // MAX_RECONNECT_ATTEMPTS
/** Close code 4008 → this wait before reconnecting (constants.py:RATE_LIMIT_DELAY). */
export const QQBOT_RATE_LIMIT_DELAY_S = 60; // RATE_LIMIT_DELAY
export const QQBOT_QUICK_DISCONNECT_THRESHOLD_S = 5.0; // QUICK_DISCONNECT_THRESHOLD
export const QQBOT_MAX_QUICK_DISCONNECT_COUNT = 3; // MAX_QUICK_DISCONNECT_COUNT

// ── message limits (constants.py) ────────────────────────────────────────────
export const QQBOT_MAX_MESSAGE_LENGTH = 4000; // MAX_MESSAGE_LENGTH
export const QQBOT_DEDUP_WINDOW_SECONDS = 300; // DEDUP_WINDOW_SECONDS
export const QQBOT_DEDUP_MAX_SIZE = 1000; // DEDUP_MAX_SIZE

// ── QQ message/media type codes (constants.py) ───────────────────────────────
export const QQ_MSG_TYPE_TEXT = 0;
export const QQ_MSG_TYPE_MARKDOWN = 2;
export const QQ_MSG_TYPE_MEDIA = 7;
export const QQ_MSG_TYPE_INPUT_NOTIFY = 6;
export const QQ_MEDIA_TYPE_IMAGE = 1;
export const QQ_MEDIA_TYPE_VIDEO = 2;
export const QQ_MEDIA_TYPE_VOICE = 3;
export const QQ_MEDIA_TYPE_FILE = 4;

// ── gateway intents (adapter.py:_send_identify) ──────────────────────────────
export const QQ_INTENT_DIRECT_MESSAGE = 1 << 12;
export const QQ_INTENT_C2C_GROUP_AT_MESSAGES = 1 << 25;
export const QQ_INTENT_INTERACTION = 1 << 26;
export const QQ_INTENT_PUBLIC_GUILD_MESSAGES = 1 << 30;
export const QQ_IDENTIFY_INTENTS =
	QQ_INTENT_C2C_GROUP_AT_MESSAGES |
	QQ_INTENT_PUBLIC_GUILD_MESSAGES |
	QQ_INTENT_DIRECT_MESSAGE |
	QQ_INTENT_INTERACTION;

// ── token lifecycle (adapter.py:_ensure_token) ───────────────────────────────
export const QQ_TOKEN_DEFAULT_EXPIRES_IN_S = 7200; // expires_in fallback
export const QQ_TOKEN_REFRESH_MARGIN_S = 60; // refresh when < margin remains

// ── heartbeat (adapter.py:_dispatch_payload op 10) ───────────────────────────
export const QQ_HEARTBEAT_DEFAULT_INTERVAL_MS = 30_000; // Hello default
/** Heartbeats go out at 80% of the SERVER-provided interval (_dispatch_payload). */
export const QQ_HEARTBEAT_FRACTION_OF_INTERVAL = 0.8;

// ── close-code classes (adapter.py:_listen_loop) ─────────────────────────────
/** Fatal codes stop reconnection entirely (retryable=false). */
export const QQ_FATAL_CLOSE_CODES: readonly number[] = [
	4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915,
];
/** Session-invalid codes clear session state for a fresh Identify (NOT 4009). */
export const QQ_SESSION_INVALID_CLOSE_CODES: readonly number[] = [
	4006, 4007, 4900, 4901, 4902, 4903, 4904, 4905, 4906, 4907, 4908, 4909, 4910,
	4911, 4912, 4913,
];
export const QQ_INVALID_TOKEN_CLOSE_CODE = 4004;
export const QQ_RATE_LIMITED_CLOSE_CODE = 4008;

// ── send retry ladder (adapter.py:_send_chunk) ───────────────────────────────
export const QQ_SEND_MAX_ATTEMPTS = 3;
export const QQ_SEND_RETRY_BASE_DELAY_S = 1.0; // delay = 1.0 * 2**attempt
export const QQ_UPLOAD_MAX_ATTEMPTS = 3; // _upload_media retry loop
export const QQ_UPLOAD_RETRY_BASE_DELAY_S = 1.5; // 1.5 * (attempt + 1)

// ── chunked upload (chunked_upload.py module constants) ─────────────────────
/** Inline base64/url uploads cap ~10MB; chunked flow covers up to ~100MB. */
export const QQ_INLINE_UPLOAD_LIMIT_BYTES = 10 * 1000 * 1000;
export const QQ_CHUNKED_UPLOAD_LIMIT_BYTES = 100 * 1000 * 1000;
/** First 10,002,432 bytes hashed into `md5_10m` (per QQ API spec). */
export const QQ_MD5_10M_SIZE = 10_002_432;
export const QQ_PART_UPLOAD_TIMEOUT_S = 300.0;
export const QQ_PART_UPLOAD_MAX_RETRIES = 2;
export const QQ_PART_FINISH_RETRY_INTERVAL_S = 1.0;
export const QQ_PART_FINISH_DEFAULT_TIMEOUT_S = 120.0;
export const QQ_PART_FINISH_MAX_TIMEOUT_S = 600.0;
export const QQ_COMPLETE_UPLOAD_MAX_RETRIES = 2;
export const QQ_COMPLETE_UPLOAD_BASE_DELAY_S = 2.0;
export const QQ_DEFAULT_CONCURRENT_PARTS = 1;
export const QQ_MAX_CONCURRENT_PARTS = 10;
/** biz_code 40093002 — upload_prepare daily cumulative quota exceeded. */
export const QQ_BIZ_CODE_DAILY_LIMIT = 40093002;
/** biz_code 40093001 — upload_part_finish transient; retry until timeout. */
export const QQ_BIZ_CODE_PART_RETRYABLE = 40093001;

// ── typing indicator (adapter.py:_TYPING_INPUT_SECONDS/_TYPING_DEBOUNCE_SECONDS)
export const QQ_TYPING_INPUT_SECONDS = 60; // duration reported to QQ (60s indicator)
export const QQ_TYPING_DEBOUNCE_MS = 50_000; // refresh before it expires

// ── send-path connection gate (adapter.py:_wait_for_reconnection) ────────────
/** Max seconds a send waits for the listener to reconnect before failing. */
export const QQ_RECONNECT_WAIT_S = 15.0; // _RECONNECT_WAIT_SECONDS
/** Seconds between is_connected polls while waiting (adapter.py). */
export const QQ_RECONNECT_POLL_INTERVAL_S = 0.5; // _RECONNECT_POLL_INTERVAL

// ── inbound attachments / STT (adapter.py:_download_and_cache, _call_stt) ────
/** httpx timeout on CDN media GETs and the STT transcription POST (30s). */
export const QQ_MEDIA_HTTP_TIMEOUT_S = 30.0;
/** adapter.py:_resolve_stt_config provider→base-url map (data, verbatim). */
export const QQ_STT_PROVIDER_BASE_URLS: Readonly<Record<string, string>> =
	Object.freeze({
		zai: "https://open.bigmodel.cn/api/coding/paas/v4",
		glm: "https://open.bigmodel.cn/api/coding/paas/v4",
		openai: "https://api.openai.com/v1",
	});
/** Env fallbacks resolved by _resolve_qq_secret (hermes setup gateway). */
export const QQ_STT_ENV_API_KEY = "QQ_STT_API_KEY";
export const QQ_STT_ENV_BASE_URL = "QQ_STT_BASE_URL";
export const QQ_STT_ENV_MODEL = "QQ_STT_MODEL";
/** Default models per config shape (_resolve_stt_config). */
export const QQ_STT_DEFAULT_MODEL_EXPLICIT = "whisper-1";
export const QQ_STT_DEFAULT_MODEL_ZAI = "glm-asr";
export const QQ_STT_DEFAULT_BASE_URL_ZAI =
	"https://open.bigmodel.cn/api/coding/paas/v4";

// ── QR scan-to-configure endpoints (constants.py PORTAL_HOST / ONBOARD_*) ────
/** Portal host override rides the optionalEnv QQ_PORTAL_HOST (corporate proxies). */
export const QQ_PORTAL_HOST_DEFAULT = "q.qq.com"; // PORTAL_HOST default
export const QQ_ONBOARD_CREATE_PATH = "/lite/create_bind_task"; // ONBOARD_CREATE_PATH
export const QQ_ONBOARD_POLL_PATH = "/lite/poll_bind_result"; // ONBOARD_POLL_PATH
/** QR target URL template (constants.py:QR_URL_TEMPLATE); attribution token is this port's own. */
export const QQ_QR_CONNECT_URL_TEMPLATE =
	"https://q.qq.com/qqbot/openclaw/connect.html?task_id={task_id}&_wv=2&source=pi-gateway";
export const QQ_ONBOARD_POLL_INTERVAL_S = 2.0; // ONBOARD_POLL_INTERVAL
export const QQ_ONBOARD_API_TIMEOUT_S = 10.0; // ONBOARD_API_TIMEOUT
export const QQ_ONBOARD_MAX_REFRESHES = 3; // onboard.py:_MAX_REFRESHES

/** Bind task statuses (onboard.py:BindStatus IntEnum). */
export const QQ_BIND_STATUS_NONE = 0;
export const QQ_BIND_STATUS_PENDING = 1;
export const QQ_BIND_STATUS_COMPLETED = 2;
export const QQ_BIND_STATUS_EXPIRED = 3;

// ── approvals (adapter.py:send_exec_approval) ────────────────────────────────
/** Matches gateway's default gateway_timeout (keyboards.py ApprovalRequest doc). */
export const QQ_APPROVAL_TIMEOUT_SECONDS = 300;

// ── capabilities AS DATA (04 §2) ─────────────────────────────────────────────
/**
 * Hermes parity: QQAdapter overrides NEITHER supports_async_delivery nor
 * interactive_resume (base defaults hold). Long-message segmentation rides
 * the SHARED kit chunker at the conformance surface (fence-carry + (i/n)
 * indicators; Hermes does the equivalent truncate_message split inside its
 * own send() — observable behavior identical, one resolution point §6.3).
 * NO native draft streaming exists on the QQ v2 wire —
 * supportsDraftStreaming stays false (probe-computed applicability).
 */
export const QQBOT_CAPABILITIES = Object.freeze({
	typedCommandPrefix: "/",
});

// ── Q17 rate budget ──────────────────────────────────────────────────────────
/**
 * The only server-declared pacing datum is the 4008 rate-limited close
 * (RATE_LIMIT_DELAY=60s authoritative wait). Interaction ACKs must be sent
 * "promptly" or the client shows an error icon (keyboards.py module doc).
 */
export const QQBOT_RATE_BUDGET = Object.freeze({
	tiers: [
		{
			name: "rate-limited-close-wait",
			ops: ["send"] as const,
			limit: 1,
			windowSeconds: QQBOT_RATE_LIMIT_DELAY_S,
		},
	],
});

// ── plugin manifest (04 §4.2 registration flow) ──────────────────────────────
import type { PluginManifest } from "../kit/index.js";

export const QQBOT_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "qqbot",
	description:
		"QQ Bot open-platform official gateway (WebSocket shape, markdown v2, inline keyboards, chunked uploads)",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "QQ_APP_ID",
			description: "QQ Open Platform application ID",
		},
		{
			name: "QQ_CLIENT_SECRET",
			description: "QQ Open Platform client secret (app token)",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "QQ_PORTAL_HOST",
			description: "Portal host override for corporate proxies/test envs",
		},
	],
	capabilities: QQBOT_CAPABILITIES,
	rateBudget: QQBOT_RATE_BUDGET,
});
