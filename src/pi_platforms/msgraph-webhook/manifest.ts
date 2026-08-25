// pi_platforms/msgraph-webhook/manifest — Microsoft Graph change-notification
// webhook adapter manifest. EVERY policy-shaped number is MANIFEST DATA
// transcribed from the READ-ONLY Hermes reference (port semantics, cite
// anchors, never vendor code); vendor ground truth fills gaps Hermes carries
// no constant for (Q17/DEC-017).
//
// Hermes anchors (gateway/platforms/msgraph_webhook.py unless noted):
//   DEFAULT_PORT = 8646 (@40)                    → MSGRAPH_DEFAULT_PORT
//   DEFAULT_WEBHOOK_PATH = "/msgraph/webhook" (@41)
//   health_path default "/health" (@~78 add_get)
//   DEFAULT_MAX_SEEN_RECEIPTS = 5000 (@42)       → receipt dedupe bound
//   DEFAULT_MAX_BODY_BYTES = 1_048_576 (@43)     → body cap (trust engine's
//     MSGRAPH_BODY_CAP_BYTES is the same constant, ported Phase 4)
//   _handle_validation (@~230): GET ?validationToken echoed VERBATIM as
//     text/plain; bare/missing token ⇒ 400 ("the endpoint can't be
//     enumerated or mistakenly used for data exfiltration")
//   _verify_client_state (@~350): hmac.compare_digest over clientState bytes
//     — shared secret ("generate with openssl rand -hex 32"); expected unset
//     or provided missing ⇒ reject
//   _build_receipt_key (@~104): `id:<notification.id>` when id non-empty,
//     else None ⇒ caller falls back to sha1 of canonical JSON
//   _resource_accepted (@~330): normalized prefix match with `/` boundary;
//     trailing-`*` patterns strip the star then match exact-or-prefix
//   _handle_notification verdicts (@~270): accepted|duplicates ⇒ 202 empty;
//     auth_rejected and not other_rejected ⇒ 403 (forged batch gets a clear
//     reject so the sender stops retrying); else 400 (sender config problem)
//   connect refusal ladder (@~160): no extra.client_state ⇒ refuse; network-
//     accessible bind without allowed_source_cidrs ⇒ refuse with guidance
//   PASSIVE ingestion: NO subscription renewal — lifecycle stays with
//     Graph/operator (06 §8.3; gap-audit A25)

import {
	MSGRAPH_BODY_CAP_BYTES,
	parseCidrAllowlist,
} from "../../pi_gateway/security/trust/index.js";
import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** msgraph_webhook.py:DEFAULT_PORT. */
export const MSGRAPH_DEFAULT_PORT = 8646;
/** msgraph_webhook.py:DEFAULT_WEBHOOK_PATH. */
export const MSGRAPH_DEFAULT_WEBHOOK_PATH = "/msgraph/webhook";
/** msgraph_webhook.py connect(): app.router.add_get("/health", …). */
export const MSGRAPH_HEALTH_PATH = "/health";
/** msgraph_webhook.py:DEFAULT_MAX_SEEN_RECEIPTS — FIFO receipt-dedupe bound. */
export const MSGRAPH_MAX_SEEN_RECEIPTS = 5000;
/** msgraph_webhook.py:DEFAULT_MAX_BODY_BYTES (same constant as the trust engine). */
export const MSGRAPH_BODY_CAP = MSGRAPH_BODY_CAP_BYTES;

/**
 * _render_prompt truncation constants: default rendering dumps pretty JSON
 * capped at 4000 chars; template interpolation renders dict/list payloads as
 * stable JSON capped at 2000 chars.
 */
export const MSGRAPH_PROMPT_RENDER_CAP_CHARS = 4000;
export const MSGRAPH_TEMPLATE_VALUE_CAP_CHARS = 2000;

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol, not
 * silently): Hermes' msgraph adapter overrides NEITHER supports_async_delivery
 * NOR interactive_resume, so it inherits the base defaults (True/True) even
 * though its send() is a log-only stub that can never push a later completion.
 * The 04 §8 webhook-shape row mandates the stateless pairing
 * (`interactive_resume=False` + `supports_async_delivery=False`) for exactly
 * this shape, and api_server/webhook — the other two stateless adapters — set
 * both False explicitly. The port declares BOTH FLAGS FALSE: the honest
 * capability data for an adapter whose only egress is a log line.
 */
export const MSGRAPH_WEBHOOK_CAPABILITIES: Readonly<
	Partial<CapabilityManifest>
> = Object.freeze({
	supportsAsyncDelivery: false,
	interactiveResume: false,
});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: Microsoft Graph change notifications
 * carry NO HMAC signature scheme. Authenticity rides two declared mechanisms:
 *   1. the `clientState` shared secret, compared CONSTANT-TIME
 *      (_verify_client_state: hmac.compare_digest — "a timing-safe compare is
 *      the right primitive" for a shared secret), and
 *   2. the CIDR source allowlist gating admission BEFORE body parse
 *      (_source_ip_allowed; forwarded headers deliberately ignored).
 *
 * validateTrustBoundaryManifest requires ≥1 HMAC wire-format scheme, which
 * would force Lying Data (declaring e.g. the GitLab header format Graph does
 * not use). This adapter therefore validates through
 * validateMsGraphTrustBoundary below: identical checks EXCEPT scheme
 * presence is satisfied by the documented clientState mechanism instead of a
 * fake scheme id.
 */
export function msGraphWebhookTrustBoundary(
	opts: { allowedSourceCidrs?: readonly string[] | undefined } = {},
): TrustBoundaryManifest {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: MSGRAPH_MAX_SEEN_RECEIPTS },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: MSGRAPH_BODY_CAP,
		cidrAllowlist: opts.allowedSourceCidrs ?? [],
		backpressureWindow: "bounded",
	};
}

/** Local extended manifest shape: names the non-HMAC authenticity mechanism. */
export interface MSGraphTrustBoundary extends TrustBoundaryManifest {
	/** Constant-time clientState compare IS the signature scheme (see above). */
	clientStateSecretCompare: true;
}

export function declareMSGraphTrustBoundary(opts: {
	allowedSourceCidrs?: readonly string[] | undefined;
}): MSGraphTrustBoundary {
	return {
		...msGraphWebhookTrustBoundary(opts),
		clientStateSecretCompare: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with the scheme-presence check satisfied
 * by clientStateSecretCompare === true (see DIVERGENCE note above).
 */
export function validateMsGraphTrustBoundary(
	m: MSGraphTrustBoundary,
): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.clientStateSecretCompare !== true) {
		errors.push(
			"msgraph ingress must declare clientStateSecretCompare: true (no HMAC scheme exists on this wire)",
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
	if (!Array.isArray(m.cidrAllowlist)) {
		errors.push("cidrAllowlist must be an array (empty = loopback-only bind)");
	}
	return errors;
}

/** Parse helper re-exported for fixtures/tests (trust-engine parity). */
export { parseCidrAllowlist };

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const MSGRAPH_WEBHOOK_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "msgraph-webhook",
	description:
		"Microsoft Graph change-notification ingress (passive webhook shape)",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "MSGRAPH_CLIENT_STATE",
			description:
				"Shared secret echoed on every notification (openssl rand -hex 32); unset ⇒ loud disable + connect refusal",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "MSGRAPH_ALLOWED_SOURCE_CIDRS",
			description:
				"Comma-separated CIDR allowlist REQUIRED for non-loopback binds (Microsoft Graph published source ranges)",
		},
	],
	capabilities: MSGRAPH_WEBHOOK_CAPABILITIES,
	trustBoundary: declareMSGraphTrustBoundary({}),
});
