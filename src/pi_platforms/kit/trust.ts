// pi_platforms/kit/trust — HTTP-ingress trust boundaries as MANIFEST DATA
// (04-platform-adapters.md §3 + DEC-017: "Feature flags alone are NOT a
// specification of the trust boundary").
//
// Every webhook/api-server ingress declares AS DATA: signature scheme(s)
// (validated with CONSTANT-TIME compare), per-route rate limit, delivery-id
// idempotency bounds, script-transform confinement, body-size caps, seen-set
// bounds, and the backpressure window class (C5: WhatsApp-Cloud-class BOUNDED
// window vs api_server's UNBOUNDED window managed by lifecycle).

import {
	createHmac,
	timingSafeEqual as nodeTimingSafeEqual,
} from "node:crypto";

export type SignatureScheme =
	| "github-x-hub-signature-256"
	| "gitlab"
	| "svix-v1"
	| "linear"
	| "hmac-v2-generic";

/**
 * The bounded-window vs unbounded-lifecycle split (§3 v0.3 correction / C5):
 * WhatsApp-Cloud-class shapes MUST answer within the provider's sync window;
 * api_server-class shapes deliberately run an UNBOUNDED window with
 * cooperative interruption (steer/stop/SSE) managed by lifecycle.
 */
export type BackpressureWindow = "bounded" | "unbounded-lifecycle";

export interface TrustBoundaryManifest {
	ingress: "http";
	signatureSchemes: readonly SignatureScheme[];
	/** Must be literal true — constant-time compare is non-negotiable. */
	constantTimeCompare: true;
	perRouteRateLimit?: { maxPerMinute: number } | undefined;
	/** Delivery-id idempotency seen-set bounds. */
	idempotency: { seenSetMaxEntries: number } | undefined;
	/** Script transforms confined under the home directory (relative_to check). */
	scriptTransformsConfinedToHome: boolean;
	/** Request body-size cap in bytes. */
	bodySizeCapBytes: number;
	/** msgraph-style CIDR allowlist marker (passive mode). */
	cidrAllowlist?: readonly string[] | undefined;
	/** api_server opt-in session headers. */
	sessionHeadersOptIn?: boolean | undefined;
	backpressureWindow: BackpressureWindow;
}

/** Validate a manifest at REGISTRATION time — incomplete boundaries are a hard error. */
export function validateTrustBoundaryManifest(
	m: TrustBoundaryManifest,
): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (!Array.isArray(m.signatureSchemes) || m.signatureSchemes.length === 0) {
		errors.push("at least one signatureScheme must be declared");
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
	return errors;
}

/**
 * CONSTANT-TIME byte comparison (DEC-017). Length mismatch returns false
 * WITHOUT early-exit content comparison; equal lengths compare in constant
 * time via node:crypto.timingSafeEqual.
 */
export function secureCompare(a: string | Buffer, b: string | Buffer): boolean {
	const bufA = Buffer.isBuffer(a) ? a : Buffer.from(a, "utf8");
	const bufB = Buffer.isBuffer(b) ? b : Buffer.from(b, "utf8");
	if (bufA.length !== bufB.length) return false;
	return nodeTimingSafeEqual(bufA, bufB);
}

/**
 * HMAC signature verification helper — computes the expected digest and
 * compares it to the presented signature in CONSTANT TIME. `presented` may
 * carry a `sha256=` prefix (GitHub X-Hub-Signature-256 style).
 */
export function verifyHmacSignature(
	secret: string,
	body: string | Buffer,
	presented: string,
	digest: "sha256" = "sha256",
): boolean {
	const expected = createHmacHex(secret, body, digest);
	const presentedNormalized = presented
		.replace(/^sha256=/, "")
		.trim()
		.toLowerCase();
	if (!/^[0-9a-f]+$/.test(presentedNormalized)) return false;
	return secureCompare(expected, presentedNormalized);
}

function createHmacHex(
	secret: string,
	body: string | Buffer,
	digest: string,
): string {
	return createHmac(digest, secret).update(body).digest("hex");
}
