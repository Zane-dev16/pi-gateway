// pi_platforms/yuanbao/manifest — Tencent Yuanbao bot platform manifest.
//
// ALL numbers are MANIFEST DATA transcribed from the Hermes reference
// gateway/platforms/yuanbao.py (cited per constant; Q17/DEC-017).

export const DEFAULT_WS_GATEWAY_URL =
	"wss://bot-wss.yuanbao.tencent.com/wss/connection"; // DEFAULT_WS_GATEWAY_URL
export const DEFAULT_API_DOMAIN = "https://bot.yuanbao.tencent.com"; // DEFAULT_API_DOMAIN

export const HEARTBEAT_INTERVAL_S = 30.0; // HEARTBEAT_INTERVAL_SECONDS
export const CONNECT_TIMEOUT_S = 15.0; // CONNECT_TIMEOUT_SECONDS
export const AUTH_TIMEOUT_S = 10.0; // AUTH_TIMEOUT_SECONDS
export const MAX_RECONNECT_ATTEMPTS = 100; // MAX_RECONNECT_ATTEMPTS
export const WS_CLOSE_TIMEOUT_S = 1.0; // WS_CLOSE_TIMEOUT_S

/** Close codes that must NEVER trigger reconnection (NO_RECONNECT_CLOSE_CODES). */
export const NO_RECONNECT_CLOSE_CODES: ReadonlySet<number> = new Set([
	4012, 4013, 4014, 4018, 4019, 4021,
]);
/** Permanent auth failures → RE-SIGN the token (AUTH_FAILED_CODES). */
export const AUTH_FAILED_CLOSE_CODES: ReadonlySet<number> = new Set([
	4001, 4002, 4003,
]);
/** Transient auth-adjacent codes: retry with the SAME token (AUTH_RETRYABLE). */
export const AUTH_RETRYABLE_CLOSE_CODES: ReadonlySet<number> = new Set([
	4010, 4011, 4099,
]);

/** Missed-pong threshold before reconnect (HEARTBEAT_TIMEOUT_THRESHOLD). */
export const HEARTBEAT_TIMEOUT_THRESHOLD = 2;

// ── reply heartbeat (HeartbeatManager) ───────────────────────────────────────
export const REPLY_HEARTBEAT_INTERVAL_S = 2.0; // REPLY_HEARTBEAT_INTERVAL_S
export const REPLY_HEARTBEAT_TIMEOUT_S = 30.0; // REPLY_HEARTBEAT_TIMEOUT_S
export const REPLY_REF_TTL_S = 300.0; // REPLY_REF_TTL_S

// ── slow-response notifier (SlowResponseNotifier) ────────────────────────────
export const SLOW_RESPONSE_TIMEOUT_S = 120.0; // SLOW_RESPONSE_TIMEOUT_S
export const SLOW_RESPONSE_MESSAGE =
	"任务有点复杂，正在努力处理中，请耐心等待..."; // SLOW_RESPONSE_MESSAGE

// ── capabilities AS DATA (04 §2) ─────────────────────────────────────────────
/**
 * Hermes parity: YuanbaoAdapter overrides NEITHER supports_async_delivery nor
 * interactive_resume (base defaults hold). NO native draft streaming exists on
 * the wire — supportsDraftStreaming stays false (probe-computed applicability).
 */
export const YUANBAO_CAPABILITIES = Object.freeze({
	typedCommandPrefix: "/",
});

// ── plugin manifest ──────────────────────────────────────────────────────────
import type { PluginManifest } from "../kit/index.js";

export const YUANBAO_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "yuanbao",
	description:
		"Tencent Yuanbao bot (WebSocket shape, binary ConnMsg protobuf wire, reply heartbeats)",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "YUANBAO_APP_ID",
			description: "Yuanbao open-platform app key",
		},
		{
			name: "YUANBAO_APP_SECRET",
			description: "Yuanbao app secret (HMAC signing key)",
			password: true,
		},
	],
	capabilities: YUANBAO_CAPABILITIES,
});
