// pi_platforms/sms/manifest — SMS (Twilio) platform adapter manifest. EVERY
// policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY Hermes
// reference (port semantics, cite anchors, never vendor code).
//
// Hermes anchors (plugins/platforms/sms/adapter.py unless noted):
//   TWILIO_API_BASE = "https://api.twilio.com/2010-04-01/Accounts" (@57)
//   MAX_SMS_LENGTH = 1600  # ~10 SMS segments (@58)
//   DEFAULT_WEBHOOK_PORT = 8080 (@59)
//   DEFAULT_WEBHOOK_HOST = "127.0.0.1" (@60)
//   _TWILIO_WEBHOOK_MAX_BODY_BYTES = 65_536  # 64 KiB — Twilio payloads are
//     small (@61)
//   routes: app.router.add_post("/webhooks/twilio", …) + add_get("/health", …)
//     (connect @~130)
//   check_sms_requirements (@66): connected ⇔ aiohttp + TWILIO_ACCOUNT_SID +
//     TWILIO_AUTH_TOKEN — TWILIO_PHONE_NUMBER deliberately NOT gated here;
//   register(ctx) required_env also lists TWILIO_PHONE_NUMBER, but its
//     ABSENCE surfaces through connect()'s named fatal
//     (_set_fatal_error("sms_missing_phone_number", …)) rather than a
//     silent skip. The port mirrors that split: the plugin manifest gates
//     enablement on SID+TOKEN (resolveEnablement parity); the phone number
//     refusal stays in the connect ladder where the source emits it.
//   _handle_webhook (@~250): declared Content-Length cap → actual-bytes cap →
//     parse_qs(keep_blank_values=True) → X-Twilio-Signature validation (only
//     when SMS_WEBHOOK_URL configured) → From+Body required → own-number echo
//     prevention → non-blocking dispatch → ALWAYS empty TwiML response
//   _validate_twilio_signature / _check_signature / _port_variant_url (@~215):
//     X-Twilio-Signature = base64(HMAC-SHA1(authToken, url + concat(sorted
//     param key+values))); variant URL toggles ONLY default ports (443/80);
//     hmac.compare_digest constant-time compare

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** sms/adapter.py:TWILIO_API_BASE. */
export const TWILIO_API_BASE = "https://api.twilio.com/2010-04-01/Accounts";

/**
 * sms/adapter.py:MAX_SMS_LENGTH — "~10 SMS segments". truncate_message parity
 * resolves THE chat length policy at this scalar (kit length-policy).
 */
export const MAX_SMS_LENGTH = 1600;

/** sms/adapter.py:DEFAULT_WEBHOOK_PORT. */
export const DEFAULT_WEBHOOK_PORT = 8080;

/** sms/adapter.py:DEFAULT_WEBHOOK_HOST — loopback-only by default. */
export const DEFAULT_WEBHOOK_HOST = "127.0.0.1";

/**
 * sms/adapter.py:_TWILIO_WEBHOOK_MAX_BODY_BYTES — 64 KiB; Twilio payloads are
 * small. Enforced at BOTH gates (declared Content-Length, then actual bytes)
 * BEFORE the form-parse seam runs.
 */
export const TWILIO_WEBHOOK_MAX_BODY_BYTES = 65_536;

/** sms/adapter.py connect(): POST route registered on the aiohttp app. */
export const SMS_WEBHOOK_PATH = "/webhooks/twilio";

/** sms/adapter.py connect(): GET route returning plain-text "ok". */
export const SMS_HEALTH_PATH = "/health";

/**
 * Idempotency seen-set BOUND (DEC-017 validator requires a declared bound).
 *
 * PROPOSED DEC TEXT (logged per DEC-026 protocol): the Hermes SmsAdapter
 * performs NO MessageSid receipt dedupe — adapter.py carries no seen-set at
 * all, so replay protection rides entirely on the HMAC signature (a forged
 * replay of a captured request IS byte-valid; only Twilio-originated
 * duplicates exist, and Twilio retries only on non-2xx which this endpoint
 * never returns for accepted messages). The port declares a BOUND, not a
 * behavior: 1024 entries is the ceiling a future dedupe lane may not exceed,
 * sized to absorb a multi-day Twilio redelivery burst at realistic SMS rates
 * without unbounded memory. The engine itself implements NO dedupe (source
 * parity); flipping this number changes nothing observable today.
 */
export const SMS_SEEN_SET_BOUND_ENTRIES = 1024;

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol, not
 * silently): Hermes' SmsAdapter overrides NEITHER supports_async_delivery NOR
 * interactive_resume, so it inherits the base defaults (True/True) even though
 * replies go out-of-band via the Twilio REST API — the inbound webhook NEVER
 * carries them, and there is no resume surface on an SMS thread. The 04 §8
 * stateless pairing (`interactive_resume=False` +
 * `supports_async_delivery=False`) matches the fast-TwiML-ack /
 * out-of-band-REST shape exactly (same reasoning as api_server/webhook, which
 * set both False explicitly). splits_long_messages stays UNSET: the base
 * default False IS the Hermes data (SmsAdapter never overrides it; send()
 * still chunks oversized bodies via truncate_message — kit deliverText/
 * wireSend chunking covers that path without the flag).
 */
export const SMS_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017), following the MSGRAPH PRECEDENT.
 *
 * signatureSchemes is EMPTY by design: the kit trust engine's five-name union
 * has no id for the Twilio wire format (`X-Twilio-Signature:
 * base64(HMAC-SHA1(authToken, url + concat(sorted param key+values)))`,
 * adapter.py:_check_signature). Declaring e.g. "gitlab" would be Lying Data —
 * same header-shape family but a different digest, key derivation and
 * encoding. validateTrustBoundaryManifest requires ≥1 scheme name, so this
 * adapter validates through validateSmsTrustBoundary below: identical checks
 * EXCEPT scheme presence is satisfied by the locally extended
 * twilioSignatureHmacSha1 datum naming the REAL mechanism. Comparison runs
 * CONSTANT-TIME via kit secureCompare (hmac.compare_digest parity).
 */
export function smsTrustBoundary(): TrustBoundaryManifest {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: SMS_SEEN_SET_BOUND_ENTRIES },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: TWILIO_WEBHOOK_MAX_BODY_BYTES,
		backpressureWindow: "bounded",
	};
}

/** Local extended manifest shape: names the non-unioned HMAC mechanism. */
export interface SmsTrustBoundary extends TrustBoundaryManifest {
	/**
	 * X-Twilio-Signature = base64(HMAC-SHA1(authToken, url + sorted param
	 * concatenation)) — the REAL scheme on this wire (see above).
	 */
	twilioSignatureHmacSha1: true;
}

export function declareSmsTrustBoundary(): SmsTrustBoundary {
	return {
		...smsTrustBoundary(),
		twilioSignatureHmacSha1: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with the scheme-presence check satisfied
 * by twilioSignatureHmacSha1 === true (see DIVERGENCE note above).
 */
export function validateSmsTrustBoundary(m: SmsTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.twilioSignatureHmacSha1 !== true) {
		errors.push(
			"sms ingress must declare twilioSignatureHmacSha1: true (the kit five-name union has no Twilio wire-format name)",
		);
	}
	if (!Number.isFinite(m.bodySizeCapBytes) || m.bodySizeCapBytes <= 0) {
		errors.push("bodySizeCapBytes must be a positive number");
	}
	if (m.idempotency === undefined) {
		errors.push("idempotency seen-set bounds must be declared");
	} else if (
		!Number.isFinite(m.idempotency.seenSetMaxEntries) ||
		m.idempotency.seenSetMaxEntries <= 0
	) {
		errors.push("idempotency.seenSetMaxEntries must be positive");
	}
	if (m.scriptTransformsConfinedToHome !== true) {
		errors.push("script transforms must declare home-directory confinement");
	}
	if (m.backpressureWindow !== "bounded") {
		errors.push(
			"sms answers within Twilio's synchronous webhook window — backpressureWindow must be bounded",
		);
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const SMS_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "sms",
	description:
		"SMS (Twilio) adapter — inbound webhook + outbound Twilio REST Messages.json",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "TWILIO_ACCOUNT_SID",
			description: "Twilio Account SID (check_sms_requirements parity)",
		},
		{
			name: "TWILIO_AUTH_TOKEN",
			description:
				"Twilio Auth Token — ALSO the HMAC-SHA1 key validating X-Twilio-Signature",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "SMS_ALLOWED_USERS",
			description:
				"Comma-separated E.164 phone numbers allowed to talk to the bot",
		},
		{
			name: "SMS_HOME_CHANNEL",
			description: "Phone number for cron / notification delivery",
		},
	],
	capabilities: SMS_CAPABILITIES,
	trustBoundary: declareSmsTrustBoundary(),
});
