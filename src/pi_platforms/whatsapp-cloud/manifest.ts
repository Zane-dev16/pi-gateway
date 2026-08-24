// pi_platforms/whatsapp-cloud/manifest — WhatsApp Cloud API adapter manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference (port semantics, cite anchors, never vendor code) plus
// vendor ground truth where Hermes carries no constant (Q17: "per-platform
// numbers live in adapter manifests, not core").
//
// Hermes anchors (gateway/platforms/whatsapp_cloud.py unless noted):
//   module docstring "Phase 5 — 24-hour conversation window + template
//     fallback" (@~25) + interactive comment "They only work *inside* the
//     24-hour conversation window" (@~672)  → MESSAGING_WINDOW_MS class
//   WEBHOOK_MAX_BODY_BYTES = 3 * 1024 * 1024 (@~97)
//   WAMID_DEDUP_CACHE_SIZE = 5000 (@~99, "Meta retries failed webhooks for up
//     to 7 days… FIFO eviction")
//   INTERACTIVE_STATE_CACHE_SIZE = 1000 (@~102)
//   _MEDIA_SIZE_LIMITS {@image:5MB, video:16MB, audio:16MB, document:100MB,
//     sticker:100KB} (@~105-112, Cloud API /media reference)
//   _DEFAULT_MIME (@~118)                   → DEFAULT_MEDIA_MIME
//   _WHATSAPP_MIME_EXTENSION_OVERRIDES (@~136 subset relevant to transport)
//   DEFAULT_API_VERSION="v20.0", GRAPH_API_BASE, DEFAULT_WEBHOOK_PORT=8090,
//     DEFAULT_WEBHOOK_PATH="/whatsapp/webhook" (@~92-96)
//   splits_long_messages = True (@WhatsAppCloudAdapter class attr ~176);
//     send() chunks via truncate_message and quotes reply_to on the FIRST
//     chunk only (send() @~540-600)
//   _verify_signature (@~1520): X-Hub-Signature-256, "sha256=<hex>" over RAW
//     body bytes, hmac.compare_digest → kit trust engine configured with WA
//     scheme data (wire-format family github-x-hub-signature-256)
//   _truncate_button_label limit=20 / list rows 24; _truncate_body limit=1024;
//     ≤3 quick-reply buttons; list ≤10 rows (+1 "Other" row)
//   send_typing (@~608): status:"read" + typing_indicator {type:"text"}
//     coupled into ONE /messages POST against the LATEST inbound wamid;
//     error code 131009 ⇒ info-level ("wamid likely older than 30 days")
//   whatsapp_common.py:MAX_MESSAGE_LENGTH = 4096 ("practical UX limit")

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

// ── webhook plane ────────────────────────────────────────────────────────────

/** whatsapp_cloud.py:WEBHOOK_MAX_BODY_BYTES — Meta's documented payload cap. */
export const WHATSAPP_MAX_BODY_BYTES = 3 * 1024 * 1024;

/**
 * whatsapp_cloud.py:WAMID_DEDUP_CACHE_SIZE — in-memory wamid replay set,
 * FIFO-evicted. Meta retries non-200 webhooks for up to 7 days; the practical
 * duplicate risk window is minutes, so a bounded seen-set is the ported shape.
 */
export const WAMID_DEDUP_CACHE_SIZE = 5000;

/** whatsapp_cloud.py:INTERACTIVE_STATE_CACHE_SIZE — per-chat pending-prompt caps. */
export const INTERACTIVE_STATE_CACHE_SIZE = 1000;

export const GRAPH_API_BASE = "https://graph.facebook.com";
export const DEFAULT_API_VERSION = "v20.0";
export const DEFAULT_WEBHOOK_PATH = "/whatsapp/webhook";
export const HEALTH_PATH = "/health";

// ── media plane (vendor ground truth as DATA) ────────────────────────────────

export type WaMediaKind = "image" | "video" | "audio" | "document" | "sticker";

/**
 * Per-type size caps documented by Meta for the Cloud API /media endpoint
 * (transcribed from whatsapp_cloud.py:_MEDIA_SIZE_LIMITS). The adapter REFUSES
 * uploads above them with a clean error BEFORE any Graph roundtrip
 * (_upload_media: "we refuse uploads above them … instead of round-tripping
 * to Graph just to be rejected").
 */
export const MEDIA_SIZE_LIMITS: Readonly<Record<WaMediaKind, number>> =
	Object.freeze({
		image: 5 * 1024 * 1024, // 5 MB (JPEG, PNG)
		video: 16 * 1024 * 1024, // 16 MB
		audio: 16 * 1024 * 1024, // 16 MB (MP3, AAC, AMR, OGG opus)
		document: 100 * 1024 * 1024, // 100 MB
		sticker: 100 * 1024, // 100 KB animated / 500 KB static
	});

/** whatsapp_cloud.py:_DEFAULT_MIME — fallback mimes when none derivable. */
export const DEFAULT_MEDIA_MIME: Readonly<Record<WaMediaKind, string>> =
	Object.freeze({
		image: "image/jpeg",
		video: "video/mp4",
		audio: "audio/mpeg",
		document: "application/octet-stream",
		sticker: "image/webp",
	});

/**
 * whatsapp_cloud.py:_WHATSAPP_MIME_EXTENSION_OVERRIDES (transport-relevant
 * subset): real-world extensions for inbound cache filenames.
 */
export const MIME_EXTENSION_OVERRIDES: Readonly<Record<string, string>> =
	Object.freeze({
		"audio/ogg": ".ogg",
		"audio/x-opus+ogg": ".ogg",
		"audio/opus": ".ogg",
		"audio/mp4": ".m4a",
		"audio/x-m4a": ".m4a",
		"image/jpeg": ".jpg",
	});

/**
 * Media kinds that accept a caption on the message block (_send_media:
 * caption on image/video/document only; filename on document only).
 */
export const CAPTION_KINDS: readonly WaMediaKind[] = [
	"image",
	"video",
	"document",
];

/** Defense-in-depth media-id guard (_download_media_to_cache). */
export const MEDIA_ID_SAFE_RE = /^[A-Za-z0-9._-]+$/;

// ── text plane ───────────────────────────────────────────────────────────────

/** whatsapp_common.py:MAX_MESSAGE_LENGTH — practical WhatsApp body cap. */
export const WA_MAX_MESSAGE_LENGTH = 4096;

/** _truncate_button_label default 20; list-row titles 24. */
export const BUTTON_TITLE_CAP = 20;
export const LIST_ROW_TITLE_CAP = 24;
/** _truncate_body limit — interactive.body.text cap. */
export const INTERACTIVE_BODY_CAP = 1024;
/** interactive.type=button takes up to 3 quick replies; list up to 10 rows. */
export const MAX_QUICK_BUTTONS = 3;
export const MAX_LIST_ROWS = 10;

// ── messaging-window class (24h) ────────────────────────────────────────────

/**
 * THE messaging-window class as manifest DATA: free-form (session) messages
 * are only deliverable inside 24 hours of the customer's last message;
 * outside it only approved template sends reach the wire. Hermes declares the
 * class in whatsapp_cloud.py's phase-scope docstring (@~25 "Phase 5 — 24-hour
 * conversation window + template fallback") and leans on it operationally for
 * interactive sends (@~672); the Pi port realizes the CLASSIFIER as data so
 * every outbound records its session-vs-template routing decision BEFORE any
 * wire call (see window-policy.ts).
 */
export const MESSAGING_WINDOW_MS = 24 * 60 * 60 * 1000;

// ── Q17 rate budget (vendor ground truth, no Hermes constant exists) ────────

/**
 * Cloud API throughput tiers as declared DATA (Q17). Meta documents a default
 * 80 calls/second per phone number on the /messages endpoint plus 24-hour
 * rolling messaging-limit tiers keyed to business quality rating (250 / 1K /
 * 10K / 100K+ messages per 24h). Hermes carries NO WA rate constant — the
 * adapter consults these tiers via kit governingTier before egress instead of
 * hardcoding numbers at call sites.
 */
export const WA_RATE_BUDGET = {
	tiers: [
		{
			name: "cloud-messages-burst",
			ops: ["send", "typing", "callback-answer"] as const,
			limit: 80, // calls/second per phone number (Meta documented default)
			windowSeconds: 1,
		},
		{
			name: "messaging-tier-1k",
			ops: ["send"] as const,
			limit: 1000, // 24h rolling messaging tier (quality-rated; conservative floor)
			windowSeconds: 24 * 60 * 60,
		},
	],
} as const;

// ── capabilities AS DATA (04 §2) ────────────────────────────────────────────

/**
 * Stateless pairing (04 §8 webhook row): BOTH flags False — nowhere to push
 * later completions, startup resume prompt meaningless (#57056).
 *
 * splitsLongMessages=True ports whatsapp_cloud.py's class attribute verbatim:
 * send() chunks via truncate_message and quotes context on the first chunk
 * only. In the Pi kit this means the ADAPTER owns native splitting inside its
 * wireSend (kit chunkWithFenceCarry ≙ base.truncate_message), NOT that the
 * base pre-chunks (that path would bypass the adapter-owned quote-first-chunk
 * semantics).
 */
export const WHATSAPP_CLOUD_CAPABILITIES: Readonly<
	Partial<CapabilityManifest>
> = Object.freeze({
	supportsAsyncDelivery: false,
	interactiveResume: false,
	splitsLongMessages: true,
});

// ── DEC-017 trust boundary ──────────────────────────────────────────────────

/**
 * THE trust boundary as DATA (DEC-017), validated at construction.
 *
 * signatureSchemes: Meta signs webhook POSTs with `X-Hub-Signature-256:
 * sha256=<hex>` — HMAC-SHA256 of the RAW request body keyed by the app secret
 * (whatsapp_cloud.py:_verify_signature). That construction IS the
 * github-x-hub-signature-256 wire-format family the kit trust engine already
 * implements (same header name, same prefix, same digest, constant-time hex
 * compare via secureCompare); the scheme id names the WIRE FORMAT, not the
 * vendor. Verification runs through verifyHmacSignature() from kit/trust.ts —
 * the ONLY comparison primitive; `===` never touches secret material.
 *
 * No perRouteRateLimit is declared: Meta signals pressure via HTTP error codes
 * and retries deliveries for up to 7 days, so replay protection rides the
 * wamid dedup bound (idempotency.seenSetMaxEntries = WAMID_DEDUP_CACHE_SIZE)
 * rather than an invented requests/minute figure.
 */
export function whatsAppCloudTrustBoundary(): TrustBoundaryManifest {
	return {
		ingress: "http",
		signatureSchemes: ["github-x-hub-signature-256"],
		constantTimeCompare: true,
		idempotency: {
			seenSetMaxEntries: WAMID_DEDUP_CACHE_SIZE,
		},
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: WHATSAPP_MAX_BODY_BYTES,
		backpressureWindow: "bounded",
	};
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const WHATSAPP_CLOUD_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "whatsapp-cloud",
	description:
		"Official Meta WhatsApp Business Platform (Cloud API) adapter — webhook shape",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "WHATSAPP_CLOUD_PHONE_NUMBER_ID",
			description: "The Graph URL path component identifying this sender",
		},
		{
			name: "WHATSAPP_CLOUD_ACCESS_TOKEN",
			description: "System User permanent token for Graph API calls",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "WHATSAPP_CLOUD_APP_SECRET",
			description:
				"HMAC key for X-Hub-Signature-256 verification; unset ⇒ webhook POSTs refused 503",
			password: true,
		},
		{
			name: "WHATSAPP_CLOUD_VERIFY_TOKEN",
			description: "Shared secret for the GET subscription handshake",
			password: true,
		},
		{
			name: "WHATSAPP_CLOUD_WABA_ID",
			description: "WhatsApp Business Account id (analytics)",
		},
	],
	capabilities: WHATSAPP_CLOUD_CAPABILITIES,
	rateBudget: WA_RATE_BUDGET,
	trustBoundary: whatsAppCloudTrustBoundary(),
});
