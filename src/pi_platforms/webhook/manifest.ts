// pi_platforms/webhook/manifest — the WEBHOOK reference adapter declares its
// ENTIRE trust boundary and capability set AS MANIFEST DATA (04 §3 obligation
// matrix; DEC-017: "Feature flags alone are NOT a specification of the trust
// boundary").
//
// Shape (DEC-002 third column): stateless webhook (WhatsApp-Cloud/api_server-
// like). Hermes anchors (READ-ONLY; semantics ported, cited not vendored):
//   gateway/platforms/webhook.py:_RATE_WINDOW_SECONDS=60, rate_limit default 30,
//     _idempotency_ttl=3600, _max_body_bytes=1MiB, interactive_resume=False
//   gateway/platforms/api_server.py:supports_async_delivery=False,
//     MAX_REQUEST_BYTES=10_000_000, _MAX_SESSION_HEADER_LEN=256
//   gateway/wake.py:_RETRY_DELAYS_SECONDS=(2,5,10), WAKE_TURN_TIMEOUT_SECONDS=600

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** webhook.py:133 `_RATE_WINDOW_SECONDS = 60.0`. */
export const RATE_WINDOW_MS = 60_000;
/** webhook.py rate_limit config default (30/min). */
export const DEFAULT_RATE_LIMIT_PER_MINUTE = 30;
/** webhook.py:223 `_idempotency_ttl = 3600` s. */
export const IDEMPOTENCY_TTL_MS = 3_600_000;
/** webhook.py:_prune_seen_deliveries throttle = min(60, max(1, ttl/10)) s. */
export const IDEMPOTENCY_PRUNE_INTERVAL_MS = 60_000;
/** webhook.py seen-set prune trigger: len > max(rate_limit*2, 128). */
export const IDEMPOTENCY_MIN_BOUND = 128;

/** webhook.py `_max_body_bytes` default (1 MiB). */
export const WEBHOOK_BODY_CAP_BYTES = 1_048_576;
/** api_server.py:239 MAX_REQUEST_BYTES. */
export const API_SERVER_BODY_CAP_BYTES = 10_000_000;

/** Svix/V2 replay skew (webhook.py tolerance_seconds / V2 replay window). */
export const SIGNATURE_SKEW_SECONDS = 300;
/** api_server.py:2255 _MAX_SESSION_HEADER_LEN. */
export const MAX_SESSION_HEADER_LEN = 256;

/**
 * C5-corrected backpressure split: WhatsApp-Cloud-class routes answer within
 * the PROVIDER's sync window (provider-defined ⇒ manifest DATA, never a core
 * constant); api_server-class lanes run UNBOUNDED windows under lifecycle
 * management (runs.ts).
 */
export const DEFAULT_BOUNDED_WINDOW_MS = 5_000;

/** wake.py WAKE_TURN_TIMEOUT_SECONDS — the whole wake turn ceiling. */
export const WAKE_TURN_CEILING_MS = 600_000;
/** wake.py _RETRY_DELAYS_SECONDS — 429/network backoff ladder (4 attempts). */
export const WAKE_RETRY_DELAYS_MS: readonly number[] = [2_000, 5_000, 10_000];

/** api_server.py _IdempotencyCache bounds (TTL 300 s, max 1000 entries). */
export const COMPLETIONS_IDEMPOTENCY_TTL_MS = 300_000;
export const COMPLETIONS_IDEMPOTENCY_MAX_ENTRIES = 1_000;

/**
 * api_server.py self._model_name parity — the virtual model name surfaced in
 * /v1/chat/completions responses when the caller sends no "model" field
 * (wake.ts already uses this identifier as its self-post model).
 */
export const DEFAULT_COMPLETIONS_MODEL = "pi-gateway";

/**
 * Capability pairing for the STATELESS shape (04 §8 webhook row): BOTH flags
 * False. Hermes splits them across webhook.py (interactive_resume=False only)
 * and api_server.py (supports_async_delivery=False); the Pi reference adapter
 * consolidates the pair onto one manifest because DEC-022 keys the wake-lane
 * choice off supports_async_delivery alone.
 */
export const WEBHOOK_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/** All five DEC-017 schemes, declared as data (scheme-specific handlers differ legitimately). */
export const ALL_SIGNATURE_SCHEMES = [
	"github-x-hub-signature-256",
	"gitlab",
	"svix-v1",
	"linear",
	"hmac-v2-generic",
] as const;

/**
 * THE trust boundary as DATA (DEC-017). Validated at construction via
 * validateTrustBoundaryManifest — an incomplete boundary is a hard error.
 */
export function webhookTrustBoundary(
	opts: {
		bodySizeCapBytes?: number | undefined;
		schemes?: readonly (typeof ALL_SIGNATURE_SCHEMES)[number][] | undefined;
		backpressureWindow?: "bounded" | "unbounded-lifecycle" | undefined;
	} = {},
): TrustBoundaryManifest {
	return {
		ingress: "http",
		signatureSchemes: opts.schemes ?? [...ALL_SIGNATURE_SCHEMES],
		constantTimeCompare: true,
		perRouteRateLimit: {
			maxPerMinute: DEFAULT_RATE_LIMIT_PER_MINUTE,
		},
		idempotency: {
			seenSetMaxEntries: Math.max(
				DEFAULT_RATE_LIMIT_PER_MINUTE * 2,
				IDEMPOTENCY_MIN_BOUND,
			),
		},
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: opts.bodySizeCapBytes ?? WEBHOOK_BODY_CAP_BYTES,
		sessionHeadersOptIn: true,
		backpressureWindow: opts.backpressureWindow ?? "bounded",
	};
}

/** One ingress route (webhook.py route_config parity, fields we consume). */
export interface WebhookRouteConfig {
	name: string;
	secret?: string | undefined;
	/** Explicit enabled:false keeps the route registered but rejects events (403). */
	enabled?: boolean | undefined;
	/** Event-type allowlist; empty/undefined admits all. */
	events?: readonly string[] | undefined;
	/**
	 * Pin the accepted signature scheme; absent ⇒ header-presence dispatch over
	 * the manifest's declared schemes (Hermes dispatches on presence globally).
	 */
	signatureScheme?: (typeof ALL_SIGNATURE_SCHEMES)[number] | undefined;
	/** Bounded sync window for agent-mode answers (C5; provider-defined DATA). */
	windowCapMs?: number | undefined;
	/** deliver_only mode: the rendered prompt IS the message — zero-LLM push. */
	deliverOnly?: boolean | undefined;
	/** Prompt template; `{payload}` interpolates pretty-printed JSON. */
	promptTemplate?: string | undefined;
	/** Profile bindings; "*"/undefined admits all configured profiles. */
	profiles?: readonly string[] | undefined;
}

export const WEBHOOK_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "webhook",
	description:
		"Stateless webhook reference adapter (WhatsApp-Cloud/api_server-like shape)",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "WEBHOOK_SECRET",
			description: "Default HMAC secret for webhook routes",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "API_SERVER_KEY",
			description:
				"Bearer key gating /v1 lanes and REQUIRED for the stateless wake rail (DEC-022)",
			password: true,
		},
	],
	capabilities: WEBHOOK_CAPABILITIES,
	trustBoundary: webhookTrustBoundary(),
});
