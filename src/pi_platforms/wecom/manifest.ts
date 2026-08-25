// pi_platforms/wecom/manifest — WeCom callback-mode adapter manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference (port semantics, cite anchors, never vendor code).
//
// Hermes anchors (plugins/platforms/wecom/callback_adapter.py unless noted):
//   DEFAULT_PORT = 8645 (@60), DEFAULT_PATH = "/wecom/callback" (@61),
//     health GET /health (@~330 add_get)
//   _MAX_BODY = 65_536 (@66) — "Cap pre-auth request bodies … bounding the
//     work an unauthenticated POST can force before signature verification";
//     enforced at BOTH layers (aiohttp client_max_size + explicit len guard)
//   ACCESS_TOKEN_TTL_SECONDS = 7200 (@67); _get_access_token refreshes with a
//     60 s early margin (@~470 "expires_at > now + 60")
//   MESSAGE_DEDUP_TTL_SECONDS = 300 (@68); prune trigger len > 2000
//   send() (@~230): proactive message/send, text content[:2048] cap,
//     safe=0; errcode {40001,42001} ⇒ evict cached token + retry ONCE
//   _handle_verify (@~310): GET msg_signature/timestamp/nonce/echostr;
//     per-app verify_url; all apps fail ⇒ 403 "signature verification failed"
//   _handle_callback (@~330): per-app decrypt ladder; WeComCryptoError ⇒ try
//     next app; other exceptions break ⇒ 400 "invalid callback payload";
//     ack-first "success" text/plain (agent reply rides the proactive send)
//   wecom_crypto.py:WXBizMsgCrypt — sha1(sorted([token,timestamp,nonce,
//     encrypt])) hex vs msg_signature; AES key = b64decode(43-char key + "=")
//     (32 bytes ⇒ AES-256-CBC, iv=key[:16]); PKCS7 block 32; plaintext =
//     random(16) || BE32 len || xml || receive_id

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** callback_adapter.py:DEFAULT_PORT. */
export const WECOM_DEFAULT_PORT = 8645;
/** callback_adapter.py:DEFAULT_PATH. */
export const WECOM_DEFAULT_CALLBACK_PATH = "/wecom/callback";
/** callback_adapter.py connect(): add_get("/health"). */
export const WECOM_HEALTH_PATH = "/health";
/** callback_adapter.py:_MAX_BODY — pre-auth body cap (both gates). */
export const WECOM_MAX_BODY_BYTES = 65_536;
/** callback_adapter.py:ACCESS_TOKEN_TTL_SECONDS. */
export const WECOM_ACCESS_TOKEN_TTL_SECONDS = 7200;
/** callback_adapter.py:_get_access_token — early-refresh margin. */
export const WECOM_TOKEN_REFRESH_MARGIN_SECONDS = 60;
/** callback_adapter.py:MESSAGE_DEDUP_TTL_SECONDS + prune bound (>2000). */
export const WECOM_DEDUP_TTL_MS = 300_000;
export const WECOM_DEDUP_PRUNE_BOUND = 2000;
/** callback_adapter.py:send — text.content cap on the proactive send. */
export const WECOM_TEXT_SEND_CAP_CHARS = 2048;
/** callback_adapter.py:send — errcodes that mean "token rejected". */
export const WECOM_TOKEN_REJECTED_ERRCODES: ReadonlySet<number> = new Set([
	40001, 42001,
]);

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged per DEC-026 protocol, not
 * silently): the source overrides NEITHER supports_async_delivery NOR
 * interactive_resume, inheriting base True/True even though replies ride the
 * ack-first callback → proactive message/send round trip. The 04 §8 webhook
 * row mandates the stateless pairing for shape="webhook" adapters. The port
 * declares BOTH FLAGS FALSE — honest capability data for this shape.
 */
export const WECOM_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY against the kit union BY DESIGN: WeCom callback
 * authenticity is a SHA1 digest over sorted(token, timestamp, nonce, encrypt)
 * matched against the `msg_signature` query parameter (wecom_crypto.py:
 * _sha1_signature) PLUS AES-256-CBC envelope confidentiality bound to the
 * corp_id. No kit scheme matches that construction — declaring one would be
 * Lying Data. The scheme lives as first-class data on `wecomCallbackScheme`
 * below; validateWecomTrustBoundary requires it (msgraph-webhook precedent).
 *
 * DEVIATION NOTE (improvement): the source compares the signature with `!=`;
 * the port routes every secret-material comparison through kit secureCompare
 * (constant-time). Same verdicts; DEC-017 posture.
 */
export interface WecomTrustBoundary extends TrustBoundaryManifest {
	wecomCallbackScheme: {
		signatureParam: "msg_signature";
		digest: "sha1-sorted-join";
		signedParts: readonly ("token" | "timestamp" | "nonce" | "encrypt")[];
		envelope: "aes-256-cbc-pkcs7-32";
		receiveIdBinding: true;
	};
}

export function declareWecomTrustBoundary(): WecomTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: WECOM_DEDUP_PRUNE_BOUND },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: WECOM_MAX_BODY_BYTES,
		backpressureWindow: "bounded",
		wecomCallbackScheme: {
			signatureParam: "msg_signature",
			digest: "sha1-sorted-join",
			signedParts: ["token", "timestamp", "nonce", "encrypt"],
			envelope: "aes-256-cbc-pkcs7-32",
			receiveIdBinding: true,
		},
	};
}

/** Construction-time boundary validation (DEC-017 posture). */
export function validateWecomTrustBoundary(m: WecomTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.wecomCallbackScheme === undefined) {
		errors.push(
			"wecom ingress must declare wecomCallbackScheme (SHA1 sorted-join + AES-CBC envelope)",
		);
	} else if (m.wecomCallbackScheme.receiveIdBinding !== true) {
		errors.push("wecom envelope must bind receive_id (corp_id)");
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

export const WECOM_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "wecom-callback",
	description:
		"WeCom self-built-app callback adapter — encrypted XML callbacks with ack-first replies",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "WECOM_CALLBACK_CORP_ID",
			description: "Corp id of the default callback app (receive_id binding)",
		},
		{
			name: "WECOM_CALLBACK_TOKEN",
			description: "Callback verification token (SHA1 sorted-join part)",
			password: true,
		},
		{
			name: "WECOM_CALLBACK_ENCODING_AES_KEY",
			description:
				"43-char base64 AES key for the BizMsgCrypt envelope (decodes to 32 bytes)",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "WECOM_CALLBACK_CORP_SECRET",
			description: "Corp secret for access-token fetches (proactive sends)",
			password: true,
		},
	],
	capabilities: WECOM_CAPABILITIES,
	trustBoundary: declareWecomTrustBoundary(),
});
