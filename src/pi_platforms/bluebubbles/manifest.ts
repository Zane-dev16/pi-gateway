// pi_platforms/bluebubbles/manifest — BlueBubbles iMessage adapter MANIFEST
// DATA (04 §2/§4; Q17 discipline): every policy-shaped number is DATA
// transcribed from the READ-ONLY Hermes reference — port semantics, cite
// anchors, never vendor code.
//
// Hermes anchors (gateway/platforms/bluebubbles.py unless noted):
//   MAX_TEXT_LENGTH = 4000 (@~66)                → BB_MAX_TEXT_LENGTH
//   DEFAULT_WEBHOOK_HOST = '127.0.0.1' (@~60)
//   _WEBHOOK_MAX_BODY_BYTES = 1_048_576 (@~61)   → body cap ("webhook events
//     are small JSON/form payloads … keeping oversized/chunked bodies from
//     being buffered unbounded")
//   DEFAULT_WEBHOOK_PORT = 8645 (@~62)
//   DEFAULT_WEBHOOK_PATH = '/bluebubbles-webhook' (@~63)
//   SUPPORTS_MESSAGE_EDITING = False (@~110 class attr) ⇒ native draft
//     streaming excluded BY THE PROBE (signal.py SUPPORTS_MESSAGE_EDITING
//     pattern); flipping this datum flips the probe (lie-scan row)
//   splits_long_messages = True (@~112 class attr) — send() chunks via
//     truncate_message(MAX_MESSAGE_LENGTH) with pagination-suffix STRIP
//   DEFAULT_MENTION_PATTERNS (@~69-72) — VERBATIM regex pair (see the data
//     note below; they contain Hermes wake words as VENDOR WIRE-SHAPE DATA)
//   _TAPBACK_ADDED {2000..2005} / _TAPBACK_REMOVED {3000..3005} (@~75-83)
//   _MESSAGE_EVENTS {'new-message','message','updated-message'} (@~86)
//   _GUID_CACHE_SIZE = 500 (@~100) — LRU cap for resolved chat-GUID lookups
//   _BLUEBUBBLES_IMAGE_EXT_OVERRIDES / _AUDIO (@~44-57) — CLOSED historical
//     mime→ext maps (unlisted mimes fall back .jpg/.mp3, never mimetypes)
//   connect() (@~230): ping /api/v1/ping → /api/v1/server/info capturing
//     private_api + helper_connected booleans; webhook REGISTRATION lifecycle
//     via GET/POST/DELETE /api/v1/webhook (crash-resilient reuse of an
//     existing matching entry; disconnect removes ALL duplicates)
//   _handle_webhook (@~640): auth-token gate (?password=/?guid= query or
//     x-password/x-guid/x-bluebubbles-guid headers — the webhook API cannot
//     send custom headers) → JSON-or-form parse → event filter → record
//     extraction → isFromMe/tapback drops → field chains → mention gating

import type { CapabilityManifest } from '../kit/capabilities.js';
import type { PluginManifest } from '../kit/registration.js';
import type { TrustBoundaryManifest } from '../kit/trust.js';

/** bluebubbles.py:MAX_TEXT_LENGTH — iMessage bubble scalar (class attr MAX_MESSAGE_LENGTH). */
export const BB_MAX_TEXT_LENGTH = 4000;

/** bluebubbles.py:DEFAULT_WEBHOOK_HOST. */
export const BB_DEFAULT_WEBHOOK_HOST = '127.0.0.1';
/** bluebubbles.py:_WEBHOOK_MAX_BODY_BYTES. */
export const BB_WEBHOOK_MAX_BODY_BYTES = 1_048_576;
/** bluebubbles.py:DEFAULT_WEBHOOK_PORT. */
export const BB_DEFAULT_WEBHOOK_PORT = 8645;
/** bluebubbles.py:DEFAULT_WEBHOOK_PATH. */
export const BB_DEFAULT_WEBHOOK_PATH = '/bluebubbles-webhook';

/**
 * bluebubbles.py:_GUID_CACHE_SIZE — LRU cap for resolved chat-GUID lookups
 * (_resolve_chat_guid move-to-end + popitem(last=False)).
 */
export const BB_GUID_CACHE_SIZE = 500;

/**
 * bluebubbles.py:DEFAULT_MENTION_PATTERNS — transcribed BYTE-EXACT as
 * MANIFEST DATA.
 *
 * DATA NOTE (proposed DEC text — logged per DEC-026 protocol, NOT silently
 * renamed): these vendor regexes match the 'hermes' wake word because the
 * upstream adapter's require_mention contract matches against them verbatim;
 * iMessage exposes no bot-mention identity (source comment @~67-69: "When
 * users opt into group mention gating without custom aliases, use
 * conservative Hermes wake words"). The Pi Gateway port keeps them
 * byte-exact: they are WIRE-SHAPE data the compiled contract matches
 * inbound group text against, not product branding — renaming would change
 * which operator messages gate. Operators override via
 * BLUEBUBBLES_MENTION_PATTERNS. If a DEC later mandates pi-native defaults,
 * that is a DATA change behind this constant, not an engine change.
 */
export const BB_DEFAULT_MENTION_PATTERNS: readonly string[] = Object.freeze([
	"(?<![\\w@])@?hermes\\s+agent\\b[:,\\-]?",
	"(?<![\\w@])@?hermes\\b[:,\\-]?",
]);

/** bluebubbles.py:_TAPBACK_ADDED — associatedMessageType codes for ADDED tapbacks. */
export const BB_TAPBACK_ADDED: Readonly<Record<number, string>> = Object.freeze(
	{
		2000: 'love',
		2001: 'like',
		2002: 'dislike',
		2003: 'laugh',
		2004: 'emphasize',
		2005: 'question',
	},
);

/** bluebubbles.py:_TAPBACK_REMOVED — associatedMessageType codes for REMOVED tapbacks. */
export const BB_TAPBACK_REMOVED: Readonly<Record<number, string>> =
	Object.freeze({
		3000: 'love',
		3001: 'like',
		3002: 'dislike',
		3003: 'laugh',
		3004: 'emphasize',
		3005: 'question',
	});

/** bluebubbles.py:_MESSAGE_EVENTS — webhook event types carrying user messages. */
export const BB_MESSAGE_EVENTS: ReadonlySet<string> = new Set([
	'new-message',
	'message',
	'updated-message',
]);

/**
 * bluebubbles.py registration events (_register_webhook payload) — the
 * subset BlueBubbles is told to deliver.
 */
export const BB_WEBHOOK_REGISTER_EVENTS: readonly string[] = Object.freeze([
	'new-message',
	'updated-message',
]);

/**
 * bluebubbles.py:_BLUEBUBBLES_IMAGE_EXT_OVERRIDES — CLOSED historical map:
 * unlisted image mimes fell back to .jpg WITHOUT consulting mimetypes
 * (ext_for_mime use_mimetypes=False, fallback='.jpg').
 */
export const BB_IMAGE_EXT_OVERRIDES: Readonly<Record<string, string>> =
	Object.freeze({
		'image/jpeg': '.jpg',
		'image/png': '.png',
		'image/gif': '.gif',
		'image/webp': '.webp',
		'image/heic': '.jpg', // preserves historical bluebubbles mapping
		'image/heif': '.jpg', // preserves historical bluebubbles mapping
		'image/tiff': '.jpg', // preserves historical bluebubbles mapping
	});

/**
 * bluebubbles.py:_BLUEBUBBLES_AUDIO_EXT_OVERRIDES — CLOSED historical map:
 * unlisted audio mimes fell back to .mp3 WITHOUT consulting mimetypes
 * (x-caf→.mp3 and mp4/aac→.m4a preserve the historical table).
 */
export const BB_AUDIO_EXT_OVERRIDES: Readonly<Record<string, string>> =
	Object.freeze({
		'audio/mp3': '.mp3',
		'audio/mpeg': '.mp3',
		'audio/ogg': '.ogg',
		'audio/wav': '.wav',
		'audio/x-caf': '.mp3', // preserves historical bluebubbles mapping
		'audio/mp4': '.m4a',
		'audio/aac': '.m4a', // preserves historical bluebubbles mapping
	});

// ── capability data (04 §2) ──────────────────────────────────────────────────

/**
 * bluebubbles.py class attrs SUPPORTS_MESSAGE_EDITING = False / splits_long_messages
 * = True. THE input of the streaming-exclusion probe: with no edit API there is
 * no way to seal or reconcile a draft cursor, so native draft streaming is
 * excluded BY THE PROBE from this constant — flipping the data flips the probe
 * (and the lie-scan mutant that flips it fails the streaming family rows).
 */
export const BB_SUPPORTS_MESSAGE_EDITING = false;

/**
 * Capabilities AS DATA.
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol, not
 * silently): Hermes' BlueBubbles adapter overrides NEITHER
 * supports_async_delivery NOR interactive_resume, so it inherits the base
 * defaults (True/True) even though its inbound plane is a stateless local
 * webhook POST and its outbound plane is synchronous REST. The 04 §8
 * webhook-shape row mandates the stateless pairing
 * (`interactive_resume=False` + `supports_async_delivery=False`) for exactly
 * this shape (same reasoning as the msgraph-webhook manifest DIVERGENCE
 * note). The port declares BOTH FLAGS FALSE: the honest capability data for
 * a request/response-shaped adapter.
 *
 * splitsLongMessages=True ports the class attribute verbatim: send() chunks
 * via paragraph split + truncate_message(MAX_TEXT_LENGTH) with the
 * pagination-suffix STRIP — the ADAPTER owns native splitting inside its
 * REST engine (kit chunkWithFenceCarry ≙ base.truncate_message), NOT the
 * base pre-chunking path.
 */
export const BLUEBUBBLES_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
		splitsLongMessages: true,
	});

// ── DEC-017 trust boundary ───────────────────────────────────────────────────

/**
 * THE trust boundary as DATA (DEC-017), following the MSGRAPH PRECEDENT.
 *
 * signatureSchemes is EMPTY by design: BlueBubbles webhook POSTs carry NO
 * HMAC signature scheme. Authenticity rides ONE declared mechanism: the
 * webhook auth token arrives via the ?password=/ ?guid= query string or the
 * x-password / x-guid / x-bluebubbles-guid headers (_handle_webhook token
 * gate @~645) and is compared CONSTANT-TIME against the configured password
 * (kit secureCompare over UTF-8 bytes). The registered webhook URL embeds the
 * password as a query param because the BlueBubbles webhook registration API
 * cannot send custom headers (bluebubbles.py _webhook_register_url docstring)
 * — access-log hygiene rides the log-safe masked variant
 * (_webhook_register_url_for_log).
 *
 * validateTrustBoundaryManifest requires ≥1 HMAC wire-format scheme, which
 * would force Lying Data (declaring a signature format this wire does not
 * use). This adapter therefore validates through validateBlueBubblesTrust-
 * Boundary below: identical checks EXCEPT scheme presence is satisfied by the
 * documented password-token mechanism instead of a fake scheme id.
 *
 * IDEMPOTENCY BOUND (proposed DEC text component): Hermes declares NO
 * delivery-id dedupe for BlueBubbles webhooks — every accepted new-message
 * dispatches, and redelivery suppression is the server's job. The port
 * declares the SMALLEST honest bound (1): no dedupe machinery exists, and
 * inventing a seen-set would be behavior Hermes does not have. The validator's
 * positive-finite requirement is satisfied without implying capacity.
 */
export function blueBubblesWebhookTrustBoundary(): TrustBoundaryManifest {
	return {
		ingress: 'http',
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: 1 },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: BB_WEBHOOK_MAX_BODY_BYTES,
		backpressureWindow: 'bounded',
	};
}

/** Local extended manifest shape: names the non-HMAC authenticity mechanism. */
export interface BlueBubblesTrustBoundary extends TrustBoundaryManifest {
	/**
	 * Constant-time webhook auth-token compare IS the signature scheme (see
	 * above): query password/guid carriers + x-password/x-guid/x-bluebubbles-guid
	 * headers, compared via kit secureCompare against the configured password.
	 */
	bbPasswordTokenCompare: true;
}

export function declareBlueBubblesTrustBoundary(): BlueBubblesTrustBoundary {
	return {
		...blueBubblesWebhookTrustBoundary(),
		bbPasswordTokenCompare: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with the scheme-presence check satisfied
 * by bbPasswordTokenCompare === true (see DIVERGENCE note above). Mirrors
 * validateMsGraphTrustBoundary structure minus the CIDR clause (this wire has
 * no source-allowlist concept — the server is the LOCAL BlueBubbles host).
 */
export function validateBlueBubblesTrustBoundary(
	m: BlueBubblesTrustBoundary,
): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push('trust boundary must declare constantTimeCompare: true');
	}
	if (m.bbPasswordTokenCompare !== true) {
		errors.push(
			'bluebubbles ingress must declare bbPasswordTokenCompare: true (no HMAC scheme exists on this wire)',
		);
	}
	if (!Number.isFinite(m.bodySizeCapBytes) || m.bodySizeCapBytes <= 0) {
		errors.push('bodySizeCapBytes must be a positive number');
	}
	if (m.idempotency === undefined) {
		errors.push('idempotency seen-set bounds must be declared');
	} else if (
		!Number.isFinite(m.idempotency.seenSetMaxEntries) ||
		m.idempotency.seenSetMaxEntries <= 0
	) {
		errors.push('idempotency.seenSetMaxEntries must be positive');
	}
	if (m.scriptTransformsConfinedToHome !== true) {
		errors.push('script transforms must declare home-directory confinement');
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration shape) ─────────────────────────────

export const BLUEBUBBLES_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: 'bluebubbles',
	description:
		'iMessage via a local BlueBubbles macOS server (REST outbound / webhook inbound)',
	transportShape: 'webhook' as const,
	requiresEnv: [
		{
			name: 'BLUEBUBBLES_SERVER_URL',
			description:
				'Base URL of the BlueBubbles server (e.g. http://localhost:1234); scheme-less values are http://-prefixed',
			url: true,
		},
		{
			name: 'BLUEBUBBLES_PASSWORD',
			description:
				'Server password; authenticates REST calls AND inbound webhooks (query/header token gate); unset ⇒ loud disable + connect refusal',
			password: true,
		},
	],
	optionalEnv: [
		{
			name: 'BLUEBUBBLES_WEBHOOK_HOST',
			description:
				'Inbound webhook bind host (default 127.0.0.1; loopback-class hosts register as localhost)',
		},
		{
			name: 'BLUEBUBBLES_WEBHOOK_PORT',
			description: 'Inbound webhook bind port (default 8645)',
		},
		{
			name: 'BLUEBUBBLES_WEBHOOK_PATH',
			description: 'Inbound webhook path (default /bluebubbles-webhook)',
		},
		{
			name: 'BLUEBUBBLES_REQUIRE_MENTION',
			description:
				'Group messages must match a wake-word pattern when truthy (true/1/yes/on)',
		},
		{
			name: 'BLUEBUBBLES_MENTION_PATTERNS',
			description:
				'JSON list or comma/newline-separated regex wake words; unset uses the vendor defaults',
		},
	],
	capabilities: BLUEBUBBLES_CAPABILITIES,
	trustBoundary: declareBlueBubblesTrustBoundary(),
});
