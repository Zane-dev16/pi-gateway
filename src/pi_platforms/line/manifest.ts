// pi_platforms/line/manifest — LINE Messaging API webhook adapter manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference (port semantics, cite anchors, never vendor code).
//
// Hermes anchors (plugins/platforms/line/adapter.py unless noted):
//   LINE_PER_BUBBLE_CHARS = 5000 (@139)          — hard cap per text bubble
//   LINE_SAFE_BUBBLE_CHARS = 4500 (@140)         — conservative chunk budget
//     (register(): max_message_length=LINE_SAFE_BUBBLE_CHARS)
//   LINE_MAX_MESSAGES_PER_CALL = 5 (@141)        — API rejects >5 per call
//   LINE_REPLY_TOKEN_TTL_SECONDS = 50 (@142)     — stash TTL under the ~60s
//     vendor TTL; single-use semantics are the VENDOR's (a burned token is
//     rejected by the API), enforced by the fixture transport
//   WEBHOOK_BODY_MAX_BYTES = 1_048_576 (@145)    — 1 MiB body cap
//   DEFAULT_WEBHOOK_PORT = 8646 (@146); DEFAULT_WEBHOOK_PATH "/line/webhook"
//     (@147); health GET {webhook_path}/health (@~936)
//   verify_line_signature (@~225): HMAC-SHA256 over the RAW body keyed by the
//     channel secret, BASE64-encoded digest, header X-Line-Signature,
//     hmac.compare_digest — see lineHmacScheme below
//   _MessageDeduplicator (@426): bounded set of webhookEventId, max_size=1000,
//     NO TTL; full-set eviction drops the oldest ~10% by insertion time. The
//     port uses BoundedSeenSet(ttlMs:null) — same observable contract at row
//     level (replay dedup + hard memory bound), FIFO eviction instead of the
//     10% batch (proposed DEC note in the adapter header).
//   RequestCache defaults ttl_seconds=3600 / pending_ttl_seconds=86400 (@360)
//   DEFAULT_SLOW_RESPONSE_THRESHOLD = 45.0 s (@174)
//   MEDIA_TOKEN_TTL_SECONDS = 1800 (@183); image ≤10 MB / av ≤200 MB (@184)
//   connect() (@800): missing credentials ⇒ fatal config_missing

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:LINE_PER_BUBBLE_CHARS — hard per-bubble cap. */
export const LINE_PER_BUBBLE_CHARS = 5000;
/** adapter.py:LINE_SAFE_BUBBLE_CHARS — chunking budget (register parity). */
export const LINE_SAFE_BUBBLE_CHARS = 4500;
/** adapter.py:LINE_MAX_MESSAGES_PER_CALL — Reply/Push reject >5 messages. */
export const LINE_MAX_MESSAGES_PER_CALL = 5;
/** adapter.py:LINE_REPLY_TOKEN_TTL_SECONDS — stash TTL (vendor TTL ~60s). */
export const LINE_REPLY_TOKEN_TTL_SECONDS = 50;

/** adapter.py:WEBHOOK_BODY_MAX_BYTES — 1 MiB; webhooks are tiny JSON. */
export const LINE_WEBHOOK_BODY_CAP_BYTES = 1_048_576;
/** adapter.py:DEFAULT_WEBHOOK_PORT. */
export const LINE_DEFAULT_PORT = 8646;
/** adapter.py:DEFAULT_WEBHOOK_PATH. */
export const LINE_DEFAULT_WEBHOOK_PATH = "/line/webhook";
/** adapter.py connect(): add_get(f"{webhook_path}/health"). */
export const LINE_HEALTH_PATH_SUFFIX = "/health";

/** adapter.py:RequestCache.__init__ defaults (READY/DELIVERED/ERROR vs PENDING). */
export const LINE_CACHE_TTL_SECONDS = 3600;
export const LINE_CACHE_PENDING_TTL_SECONDS = 86400;
/** adapter.py:DEFAULT_SLOW_RESPONSE_THRESHOLD — postback button fires here. */
export const LINE_SLOW_RESPONSE_THRESHOLD_SECONDS = 45;
/** adapter.py:_MessageDeduplicator default bound. */
export const LINE_DEDUP_MAX_ENTRIES = 1000;

/** Postback button copy caps (build_postback_button_message @~630). */
export const LINE_BUTTON_TEXT_CAP = 160;
export const LINE_BUTTON_ALT_TEXT_CAP = 400;
export const LINE_POSTBACK_LABEL_CAP = 20;

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged per DEC-026 protocol, not
 * silently): Hermes' LineAdapter overrides NEITHER supports_async_delivery
 * NOR interactive_resume, so it inherits base True/True. The 04 §8 webhook
 * row mandates the stateless pairing for shape="webhook" adapters, and the
 * adapter wires no wake/resume lane: replies ride single-use reply tokens or
 * direct Push calls with no gateway-side completion push. The port declares
 * BOTH FLAGS FALSE — honest capability data for this shape.
 */
export const LINE_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY against the kit union BY DESIGN: LINE signs
 * webhooks with HMAC-SHA256 over the RAW body keyed by the channel secret but
 * presents the digest BASE64-ENCODED in `X-Line-Signature` (adapter.py:
 * verify_line_signature @~225). Every kit scheme is hex/plain-token shaped —
 * declaring any of them would be Lying Data. The scheme is instead declared
 * as first-class manifest data on `lineHmacScheme` below and validated by
 * validateLineTrustBoundary (msgraph-webhook precedent for non-kit schemes);
 * verification runs through verifyLineSignature() in the adapter — kit
 * secureCompare is the ONLY comparison primitive over secret material.
 */
export interface LineTrustBoundary extends TrustBoundaryManifest {
	/** The LINE wire-format scheme as DATA (see note above). */
	lineHmacScheme: {
		header: "x-line-signature";
		digest: "sha256";
		encoding: "base64";
		keyMaterial: "raw-secret";
		signedContent: "body";
		skewSeconds: null;
	};
}

export function declareLineTrustBoundary(): LineTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: LINE_DEDUP_MAX_ENTRIES },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: LINE_WEBHOOK_BODY_CAP_BYTES,
		backpressureWindow: "bounded",
		lineHmacScheme: {
			header: "x-line-signature",
			digest: "sha256",
			encoding: "base64",
			keyMaterial: "raw-secret",
			signedContent: "body",
			skewSeconds: null,
		},
	};
}

/** Construction-time boundary validation (DEC-017 posture). */
export function validateLineTrustBoundary(m: LineTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.lineHmacScheme === undefined) {
		errors.push(
			"line ingress must declare lineHmacScheme (base64 HMAC-SHA256 over raw body)",
		);
	} else {
		if (m.lineHmacScheme.encoding !== "base64") {
			errors.push("LINE signatures are base64-encoded digests");
		}
		if (m.lineHmacScheme.signedContent !== "body") {
			errors.push("LINE signs the RAW body only");
		}
	}
	if (!Number.isFinite(m.bodySizeCapBytes) || m.bodySizeCapBytes <= 0) {
		errors.push("bodySizeCapBytes must be a positive number");
	}
	if (m.idempotency === undefined || m.idempotency.seenSetMaxEntries <= 0) {
		errors.push("idempotency seen-set bounds must be declared");
	}
	if (m.scriptTransformsConfinedToHome !== true) {
		errors.push("script transforms must declare home-directory confinement");
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const LINE_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "line",
	description:
		"LINE Messaging API adapter — reply-token-first webhook shape with Push fallback",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "LINE_CHANNEL_ACCESS_TOKEN",
			description:
				"Long-lived channel access token (LINE Developers Console > Messaging API)",
			password: true,
		},
		{
			name: "LINE_CHANNEL_SECRET",
			description:
				"Channel secret — HMAC-SHA256 key for X-Line-Signature verification",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "LINE_ALLOWED_USERS",
			description: "Comma-separated U-prefixed user IDs allowed to DM the bot",
		},
		{
			name: "LINE_ALLOWED_GROUPS",
			description: "Comma-separated C-prefixed group IDs the bot responds in",
		},
		{
			name: "LINE_ALLOWED_ROOMS",
			description: "Comma-separated R-prefixed room IDs the bot responds in",
		},
		{
			name: "LINE_ALLOW_ALL_USERS",
			description: "Dev-only allowlist bypass",
		},
	],
	capabilities: LINE_CAPABILITIES,
	trustBoundary: declareLineTrustBoundary(),
});
