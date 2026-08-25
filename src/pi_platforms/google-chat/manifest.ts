// pi_platforms/google-chat/manifest — Google Chat adapter manifest (HTTP
// events mode). EVERY policy-shaped number is MANIFEST DATA transcribed from
// the READ-ONLY Hermes reference (port semantics, cite anchors, never vendor
// code); vendor ground truth fills gaps Hermes carries no constant for.
//
// Hermes anchors (plugins/platforms/google_chat/adapter.py unless noted):
//   _MAX_TEXT_LENGTH = 4000 (@230; "Google Chat text-message size limit is
//     4096; leave margin")
//   _RATE_LIMIT_WARN_THRESHOLD = 5 (@233)      — per-chat 429 warn counter
//   _RETRY_MAX_ATTEMPTS = 3 / base 1.0 s / max delay 8.0 s / jitter 0.3 /
//     retryable statuses {429,500,502,503,504} (@239-243)
//   GOOGLE_CHAT_MAX_MESSAGES=1, GOOGLE_CHAT_MAX_BYTES=16 MiB FlowControl
//     defaults (@741-746)
//   verify_http_event_request (@1520): OIDC bearer verification — audience-
//     bound ID token via google-auth (delegated), email claim must match the
//     configured service-account allowlist; named rejection reasons
//   dispatch_http_event (@1494): envelope formats 1/2/3 (_extract_message_payload
//     @1255), BOT self-filter, MessageDeduplicator on msg.name
//   gateway/platforms/helpers.py:MessageDeduplicator — max_size=2000,
//     ttl_seconds=300
//   send() (@2057): markdown→Chat dialect BEFORE chunking; typing card
//     PATCHED in place (no delete tombstone); 403 ⇒ fatal chat_forbidden;
//     404 first-chunk patch falls through to create; 429 bumps per-chat hits
//   edit_message (@2297): messages.patch, content capped at 4000 + ellipsis
//   _resolve_thread_id (@2482) priority ladder incl. job_id new-thread rule
//   _create_message (@2584): messageReplyOption=
//     REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD whenever thread.name is present —
//     a messages.create QUERY kwarg, never a body field (Message resource
//     rejects unknown body fields)

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:_MAX_TEXT_LENGTH — Chat hard limit is 4096; margin noted. */
export const GCHAT_MAX_TEXT_LENGTH = 4000;
/** adapter.py:_RATE_LIMIT_WARN_THRESHOLD — per-chat 429 counter warn gate. */
export const GCHAT_RATE_LIMIT_WARN_THRESHOLD = 5;
/** adapter.py outbound retry parameters (PR #14965 ladder). */
export const GCHAT_RETRY_MAX_ATTEMPTS = 3;
export const GCHAT_RETRY_BASE_DELAY_MS = 1_000;
export const GCHAT_RETRY_MAX_DELAY_MS = 8_000;
export const GCHAT_RETRY_JITTER = 0.3;
export const GCHAT_RETRYABLE_HTTP_STATUSES: ReadonlySet<number> = new Set([
	429, 500, 502, 503, 504,
]);

/**
 * Ingress envelope cap. Hermes delegates the HTTP front to Cloud Run and
 * bounds the Pub/Sub lane with GOOGLE_CHAT_MAX_BYTES (FlowControl default
 * 16 MiB, __init__ @746). The port applies THE SAME bound to the HTTP-lane
 * body so the trust boundary declares a real number (proposed DEC text —
 * logged per DEC-026, not silently invented).
 */
export const GCHAT_BODY_CAP_BYTES = 16 * 1024 * 1024;

/** helpers.py:MessageDeduplicator defaults (TTL-bounded seen-set). */
export const GCHAT_DEDUP_MAX_ENTRIES = 2000;
export const GCHAT_DEDUP_TTL_MS = 300_000;

/** adapter.py:_GOOGLE_ID_TOKEN_CERTS_TTL_SECONDS — cert cache window. */
export const GCHAT_ID_TOKEN_CERTS_TTL_SECONDS = 300;

/** adapter.py:_TRUSTED_ATTACHMENT_HOSTS — SSRF guard for attachment URIs. */
export const GCHAT_TRUSTED_ATTACHMENT_HOSTS: readonly string[] = Object.freeze([
	"googleapis.com",
	"chat.google.com",
	"drive.google.com",
	"docs.google.com",
	"lh3.googleusercontent.com",
	"lh4.googleusercontent.com",
	"lh5.googleusercontent.com",
	"lh6.googleusercontent.com",
]);

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged per DEC-026 protocol, not
 * silently): Hermes' GoogleChatAdapter overrides NEITHER flag, inheriting
 * base True/True even though its egress is REST create/patch with no native
 * draft-streaming lanes. The 04 §8 webhook row mandates the stateless pairing
 * for shape="webhook" adapters; api_server/webhook set both False explicitly.
 * The port declares BOTH FLAGS FALSE — honest capability data for this shape.
 */
export const GCHAT_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY against the kit union BY DESIGN: Google delivers
 * Chat HTTP events with an OIDC BEARER ID TOKEN in the Authorization header —
 * no HMAC over the body exists on this wire (verify_http_event_request
 * @1520). Authenticity rides three declared mechanisms: audience-bound token
 * verification (delegated to the injected verifier seam exactly like Hermes
 * delegates to google-auth), the service-account email ALLOWLIST claim check,
 * and constant-time comparisons inside the verifier. Declaring any kit scheme
 * would be Lying Data; validateGchatTrustBoundary requires gchatOidcBearerAuth
 * instead (msgraph-webhook precedent).
 */
export interface GchatTrustBoundary extends TrustBoundaryManifest {
	/** OIDC bearer authenticity mechanism as DATA (see note above). */
	gchatOidcBearerAuth: {
		header: "authorization";
		tokenShape: "oidc-id-token";
		audienceBinding: true;
		senderAllowlistClaim: "email";
	};
}

export function declareGchatTrustBoundary(): GchatTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: GCHAT_DEDUP_MAX_ENTRIES },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: GCHAT_BODY_CAP_BYTES,
		backpressureWindow: "bounded",
		gchatOidcBearerAuth: {
			header: "authorization",
			tokenShape: "oidc-id-token",
			audienceBinding: true,
			senderAllowlistClaim: "email",
		},
	};
}

/** Construction-time boundary validation (DEC-017 posture). */
export function validateGchatTrustBoundary(m: GchatTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.gchatOidcBearerAuth === undefined) {
		errors.push(
			"gchat ingress must declare gchatOidcBearerAuth (OIDC bearer tokens, no HMAC scheme exists on this wire)",
		);
	} else if (
		m.gchatOidcBearerAuth.audienceBinding !== true ||
		m.gchatOidcBearerAuth.senderAllowlistClaim !== "email"
	) {
		errors.push(
			"gchat bearer auth must bind the audience AND allowlist the sender email claim",
		);
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

export const GCHAT_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "google-chat",
	description:
		"Google Chat adapter — OIDC-authenticated HTTP events mode with Chat REST egress",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "GOOGLE_CHAT_HTTP_EVENTS_AUDIENCE",
			description:
				"Expected `aud` claim of the OIDC bearer on every delivered event",
		},
		{
			name: "GOOGLE_CHAT_HTTP_EVENTS_SERVICE_ACCOUNT_EMAIL",
			description:
				"Comma-separated SA emails allowed in the bearer's `email` claim",
		},
	],
	optionalEnv: [
		{
			name: "GOOGLE_CHAT_HTTP_EVENTS_URL",
			description: "This deployment's HTTPS event endpoint (Cloud Run front)",
		},
		{
			name: "GOOGLE_CHAT_PROJECT_ID",
			description: "GCP project for Pub/Sub mode naming",
		},
		{
			name: "GOOGLE_CHAT_MAX_BYTES",
			description: "Envelope byte budget (Pub/Sub FlowControl parity)",
		},
	],
	capabilities: GCHAT_CAPABILITIES,
	trustBoundary: declareGchatTrustBoundary(),
});
