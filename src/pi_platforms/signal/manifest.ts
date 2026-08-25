// pi_platforms/signal/manifest — Signal platform MANIFEST DATA (04 §2/§4;
// Q17 discipline): every policy-shaped number is DATA transcribed from the
// READ-ONLY Hermes reference — per-platform numbers live in adapter
// manifests, never in core.
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   gateway/platforms/signal.py unless noted:
//     SIGNAL_MAX_ATTACHMENT_SIZE = 100 MB (@~71)
//     MAX_MESSAGE_LENGTH = 8000 "Signal message size limit" (@~67) — consumed
//       by base.max_message_length_for_chat as THE adapter scalar
//     TYPING_INTERVAL = 8.0s (@~68)
//     SSE_RETRY_DELAY_INITIAL = 2.0 / SSE_RETRY_DELAY_MAX = 60.0 (@~69-70;
//       _sse_listener: reset to initial on successful connect, double up to
//       max, +20% jitter against thundering herd)
//     HEALTH_CHECK_INTERVAL = 30.0 / HEALTH_CHECK_STALE_THRESHOLD = 120.0
//       (@~71-72; _health_monitor probes /api/v1/check when SSE idles past
//       the threshold: alive ⇒ refresh activity, dead ⇒ force reconnect)
//     SUPPORTS_MESSAGE_EDITING = False (@~258-261 class attr: "Signal has no
//       real edit API for already-sent messages") ⇒ native draft streaming
//       excluded BY THE PROBE (seal reality), never a hardcoded skip
//     typing-failure breaker (_send_typing... sendTyping body): after 3
//       consecutive failures skip RPC for min(60, 16·2^(fails−3)) seconds
//   gateway/platforms/signal_rate_limit.py:
//     SIGNAL_MAX_ATTACHMENTS_PER_MSG = 32 (per-message attachment cap,
//       source: Signal-{Android,Desktop} source code)
//     SIGNAL_RATE_LIMIT_BUCKET_CAPACITY = 50 (server-side token bucket)
//     SIGNAL_RATE_LIMIT_DEFAULT_RETRY_AFTER = 4s (fallback refill interval
//       for signal-cli < v0.14.3)
//     SIGNAL_RATE_LIMIT_MAX_ATTEMPTS = 2 (initial attempt + 1 retry)
//     SIGNAL_BATCH_PACING_NOTICE_THRESHOLD = 10.0s
//     SIGNAL_RPC_ERROR_RATELIMIT = -5 (signal-cli ≥ v0.14.3 JSON-RPC error
//       code for RateLimitException)
//   gateway/platforms/media_cache.py:DEFAULT_EXT_TO_MIME (byte-identical
//     historical Signal ext→mime table)

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";

/** signal.py:@~71 — attachments above this are skipped at intake. */
export const SIGNAL_MAX_ATTACHMENT_SIZE = 100 * 1024 * 1024;

/**
 * signal.py:@~67 — THE per-chat length scalar (base.py
 * max_message_length_for_chat reads MAX_MESSAGE_LENGTH). Measured in
 * CODEPOINTS (Python len parity); the kit length policy carries the unit.
 */
export const SIGNAL_MAX_MESSAGE_LENGTH = 8000;

/** signal.py:@~68 — typing-indicator refresh cadence. */
export const TYPING_INTERVAL_SECONDS = 8;

/** signal.py:@~69-70 — SSE reconnect ladder bounds (ms here). */
export const SSE_RETRY_DELAY_INITIAL_MS = 2_000;
export const SSE_RETRY_DELAY_MAX_MS = 60_000;
/** signal.py:_sse_listener — jitter fraction vs computed delay. */
export const SSE_JITTER_FRACTION = 0.2;

/** signal.py:@~71-72 — health monitor cadence + stale-SSE concern bar. */
export const HEALTH_CHECK_INTERVAL_MS = 30_000;
export const HEALTH_CHECK_STALE_THRESHOLD_MS = 120_000;

/**
 * signal.py:sendTyping breaker — consecutive-failure ladder. After ≥3
 * consecutive sendTyping failures the adapter skips the RPC entirely for
 * min(60, 16·2^(fails−3)) seconds (16s → 32s → 60s cap).
 */
export function typingBackoffSeconds(consecutiveFailures: number): number {
	return Math.min(60, 16 * 2 ** (consecutiveFailures - 3));
}

// ── rate-limit plane (signal_rate_limit.py) ──────────────────────────────────

/** Per-message attachment cap (Signal-{Android,Desktop} ground truth). */
export const SIGNAL_MAX_ATTACHMENTS_PER_MSG = 32;
/** Server-side token-bucket capacity for the attachment rate limit. */
export const SIGNAL_RATE_LIMIT_BUCKET_CAPACITY = 50;
/** Fallback per-token refill interval (signal-cli < v0.14.3). */
export const SIGNAL_RATE_LIMIT_DEFAULT_RETRY_AFTER_S = 4;
/** Initial attempt + 1 retry on rate-limited attachment sends. */
export const SIGNAL_RATE_LIMIT_MAX_ATTEMPTS = 2;
/** Estimated inter-batch wait above which the user gets a pacing notice. */
export const SIGNAL_BATCH_PACING_NOTICE_THRESHOLD_S = 10;
/** signal-cli ≥ v0.14.3 JSON-RPC error code for RateLimitException. */
export const SIGNAL_RPC_ERROR_RATELIMIT = -5;

// ── capability data (04 §2) ─────────────────────────────────────────────────

/**
 * signal.py class attr SUPPORTS_MESSAGE_EDITING = False (@~258-261). THE
 * input of the streaming-exclusion probe: with no edit API there is no way
 * to seal or reconcile a draft cursor, so native draft streaming is excluded
 * BY THE PROBE from this constant — flipping the data flips the probe (and
 * the lie-scan mutant that flips it fails the streaming family rows).
 */
export const SIGNAL_SUPPORTS_MESSAGE_EDITING = false;

/**
 * Capabilities AS DATA. supportsAsyncDelivery stays the push-shape default
 * TRUE (persistent daemon stream ⇒ forged-event wake lane, DEC-022);
 * splitsLongMessages unset (base default False — long content still splits
 * against the 8000-char scalar via the kit length pair).
 */
export const SIGNAL_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
	});

// ── plugin manifest (kit registration shape) ────────────────────────────────

/**
 * Transport shape: Signal rides a LOCAL signal-cli daemon — inbound over a
 * persistent SSE connection the gateway PULLS, outbound JSON-RPC 2.0 POSTs.
 * Persistent-pull inbound ≙ the "ws" family (DEC-002 third axis is ingress
 * modality; there is no inbound HTTP surface at all).
 *
 * DEC-017 note: NO trust boundary is declared because the adapter has no
 * HTTP-ingress plane to protect — the only network surface is an OUTBOUND
 * connection to the operator's own daemon (signal.py docstring @~1-13:
 * `signal-cli daemon --http 127.0.0.1:8080`).
 */
export const SIGNAL_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "signal",
	description:
		"Signal messenger via a local signal-cli daemon (SSE inbound / JSON-RPC outbound)",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "SIGNAL_HTTP_URL",
			description:
				"Base URL of the signal-cli HTTP daemon (e.g. http://127.0.0.1:8080)",
			url: true,
		},
		{
			name: "SIGNAL_ACCOUNT",
			description: "The sending account (E.164 number or service-id UUID)",
		},
	],
	optionalEnv: [
		{
			name: "SIGNAL_GROUP_ALLOWED_USERS",
			description:
				"Comma-separated group ids; unset disables groups entirely, '*' opens all",
		},
		{
			name: "SIGNAL_ALLOWED_USERS",
			description:
				"DM allowlist ('*' default open) gating 👀 reaction visibility",
		},
		{
			name: "SIGNAL_REQUIRE_MENTION",
			description: "Group messages must @mention the bot account when truthy",
		},
		{
			name: "SIGNAL_REACTIONS",
			description: "Set false/0/no to disable the reaction progress lifecycle",
		},
	],
	capabilities: SIGNAL_CAPABILITIES,
});
