// pi_platforms/teams/manifest — Microsoft Teams (Bot Framework) adapter
// manifest. EVERY policy-shaped number is MANIFEST DATA transcribed from the
// READ-ONLY Hermes reference (port semantics, cite anchors, never vendor
// code); vendor ground truth fills gaps Hermes carries no constant for
// (Q17/DEC-017).
//
// Hermes anchors (plugins/platforms/teams/adapter.py unless noted):
//   MAX_MESSAGE_LENGTH = 28000 (@~700 "Teams text message limit (~28 KB)")
//   splits_long_messages = True (@~701)          → native chunking in wireSend
//   MessageDeduplicator(max_size=1000) (@~717); helpers.py defaults
//     ttl_seconds=300 (@43)                      → dedupe bound + TTL class
//   _DEFAULT_PORT = 3978 (@122)                  → TEAMS_DEFAULT_PORT
//   _MAX_BODY_BYTES = 1_048_576 (@125)           → body cap
//   _DEFAULT_HOST = None (@131, dual-stack)      → bind posture
//   _WEBHOOK_PATH = "/api/messages" (@132)
//   _DEFAULT_TEAMS_SERVICE_URL = "https://smba.trafficmanager.net/teams/" (@~410)
//   _ALLOWED_TEAMS_SERVICE_HOSTS = {smba.trafficmanager.net,
//     smba.infra.gov.teams.microsoft.us} (@~415) → SSRF/token-exfiltration
//     allowlist (_validate_teams_service_url: https + host + trailing slash)
//   _TEAMS_CONV_ID_RE = ^[A-Za-z0-9:@\-_.]+$ (@~425) → conversation-id charset
//   standalone send: token POST login.microsoftonline.com/{tenant}/oauth2/
//     v2.0/token scope https://api.botframework.com/.default (@~620);
//     activity {"type":"message","text":…,"textFormat":"markdown"} (@~650)
//   _on_message (@~900): bot-id self filter → MessageDeduplicator → conv-ref
//     cache → <at>@mention</at> strip → conversation_type mapping
//     (personal→dm / groupChat→group / channel→channel) → attachment walk →
//     classification precedence DOCUMENT > PHOTO > VIDEO > AUDIO > TEXT
//   _on_card_action (@~1150): default-DENY clicker authz (TEAMS_ALLOWED_USERS
//     / TEAMS_ALLOW_ALL_USERS), choice_map approve_once/approve_session/
//     approve_always/deny → once/session/always/deny, expiry card on
//     already-resolved approvals
//   send() (@~1245): format_message identity → truncate_message chunks →
//     reply(chat_id, reply_to, chunk) ONLY when reply_to.isdigit() and != "0"
//     with FLAT fallback on any exception; SendResult(success, last id)
//   send_typing (@~1280): TypingActivityInput, errors swallowed
//   _send_media_attachment (@~1295): http(s) URLs attached BY REFERENCE;
//     local paths base64 data URIs; Attachment(content_type, content_url);
//     caption rides the activity text
//   register() (@~1380): required_env CLIENT_ID/CLIENT_SECRET/TENANT_ID,
//     max_message_length=28000, platform_hint (Teams renders bold/italic/
//     inline code — NOT complex tables or raw HTML)

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:MAX_MESSAGE_LENGTH — "~28 KB per message". */
export const TEAMS_MAX_MESSAGE_LENGTH = 28_000;
/** adapter.py:__init__ — MessageDeduplicator(max_size=1000). */
export const TEAMS_DEDUP_MAX_SIZE = 1000;
/** helpers.py:MessageDeduplicator ttl_seconds=300 default. */
export const TEAMS_DEDUP_TTL_MS = 300_000;

/** adapter.py:_DEFAULT_PORT. */
export const TEAMS_DEFAULT_PORT = 3978;
/** adapter.py:_WEBHOOK_PATH — the Bot Framework messages endpoint. */
export const TEAMS_WEBHOOK_PATH = "/api/messages";
/** adapter.py:_MAX_BODY_BYTES — activities are JSON well under 1 MiB. */
export const TEAMS_MAX_BODY_BYTES = 1_048_576;
/** adapter.py:_DEFAULT_HOST — None ⇒ dual-stack all-interfaces. */
export const TEAMS_BIND_ALL_INTERFACES = null;

/**
 * Bot Framework service endpoints (SSRF/token-exfiltration allowlist).
 * Operator-supplied service URLs are matched against THIS set; anything else
 * refuses pre-wire (_validate_teams_service_url).
 */
export const DEFAULT_TEAMS_SERVICE_URL =
	"https://smba.trafficmanager.net/teams/";
export const ALLOWED_TEAMS_SERVICE_HOSTS: ReadonlySet<string> = new Set([
	"smba.trafficmanager.net",
	"smba.infra.gov.teams.microsoft.us",
]);

/** adapter.py:_TEAMS_CONV_ID_RE — hostile chat_id cannot escape the URL path. */
export const TEAMS_CONV_ID_RE = /^[A-Za-z0-9:@\-_.]+$/;
/** Same charset rule applies to the tenant id (standalone-send parity). */
export const TEAMS_TENANT_ID_RE = /^[A-Za-z0-9:@\-_.]+$/;

/** Azure AD client-credentials endpoint + Bot Framework scope (standalone parity). */
export const TEAMS_TOKEN_URL_TEMPLATE =
	"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token";
export const TEAMS_TOKEN_SCOPE = "https://api.botframework.com/.default";

/** Outbound activity wire shape (standalone sender ground truth). */
export const TEAMS_TEXT_FORMAT = "markdown";

/** send_exec_approval truncation: card preview cap / button-data cmd cap. */
export const TEAMS_CMD_PREVIEW_CAP = 2000;
export const TEAMS_BTN_DATA_CMD_CAP = 200;

/**
 * Card action → kit exec-approval choice map (_on_card_action choice_map).
 * Unknown hermes_action values answer "Unknown action."
 */
export const TEAMS_HERMES_ACTION_CHOICES: Readonly<Record<string, string>> =
	Object.freeze({
		approve_once: "once",
		approve_session: "session",
		approve_always: "always",
		deny: "deny",
	});

/**
 * Q17 rate budget — vendor ground truth as DATA (Microsoft Learn,
 * "Rate limiting for bots in Microsoft Teams", per agent per thread):
 * send-to-conversation 7 ops / 1 s, 60 ops / 30 s, 1800 ops / 3600 s; global
 * per app per tenant 50 RPS. HTTP 429 (plus 412/502/504) is retried with
 * exponential backoff. Hermes carries NO Teams rate constant — the manifest
 * declares the tiers; the runner consults governingTier before egress.
 */
export const TEAMS_RATE_BUDGET = {
	retryableStatuses: [429, 412, 502, 504],
	tiers: [
		{
			name: "bf-send-per-thread-burst",
			ops: ["send"] as const,
			limit: 7,
			windowSeconds: 1,
		},
		{
			name: "bf-send-per-thread-sustained",
			ops: ["send"] as const,
			limit: 1800,
			windowSeconds: 3600,
		},
		{
			name: "bf-global-per-app-tenant",
			ops: ["send", "typing", "callback-answer"] as const,
			limit: 50,
			windowSeconds: 1,
		},
	],
} as const;

/**
 * Capabilities AS DATA (04 §2) — Hermes parity:
 *   supports_async_delivery TRUE (base default; Teams caches Conversation-
 *     References and proactively sends approval cards — a real outbound
 *     channel, unlike api_server/webhook/msgraph),
 *   interactive_resume TRUE (base default; not overridden by the plugin),
 *   splits_long_messages TRUE (explicit class attribute),
 *   typed_command_prefix "/" (allow_update_command=True registration).
 */
export const TEAMS_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
		splitsLongMessages: true,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: Bot Framework inbound activity auth is
 * NOT an HMAC scheme — the microsoft-teams-apps SDK validates the incoming
 * Bearer token against Azure AD (daemon/SDK machinery Hermes delegates; see
 * the DIVERGENCE/exclusion note in teams-adapter.ts). That delegation is
 * declared as `bearerAuthDelegatedToSdk` instead of faking a wire format.
 *
 * validateTrustBoundaryManifest requires ≥1 HMAC wire-format scheme, which
 * would force lying data; teams validates through validateTeamsTrustBoundary
 * below (identical checks except scheme presence ⇐ the delegated-Bearer
 * declaration).
 */
export function teamsTrustBoundary(): TrustBoundaryManifest {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: TEAMS_DEDUP_MAX_SIZE },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: TEAMS_MAX_BODY_BYTES,
		backpressureWindow: "bounded",
	};
}

/** Local extended manifest shape: names the delegated inbound-auth mechanism. */
export interface TeamsTrustBoundary extends TrustBoundaryManifest {
	bearerAuthDelegatedToSdk: true;
}

export function declareTeamsTrustBoundary(): TeamsTrustBoundary {
	return {
		...teamsTrustBoundary(),
		bearerAuthDelegatedToSdk: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with scheme presence satisfied by the
 * delegated-Bearer declaration (see above).
 */
export function validateTeamsTrustBoundary(m: TeamsTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.bearerAuthDelegatedToSdk !== true) {
		errors.push(
			"teams ingress must declare bearerAuthDelegatedToSdk: true (no HMAC scheme exists on this wire)",
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
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const TEAMS_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "teams",
	description:
		"Microsoft Teams adapter (Bot Framework activities over webhook ingress)",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "TEAMS_CLIENT_ID",
			description: "Azure AD app (client) ID of the registered bot",
		},
		{
			name: "TEAMS_CLIENT_SECRET",
			description: "Azure AD client secret for the bot registration",
			password: true,
		},
		{
			name: "TEAMS_TENANT_ID",
			description: "Azure AD tenant ID hosting the bot registration",
		},
	],
	optionalEnv: [
		{
			name: "TEAMS_SERVICE_URL",
			description:
				"Bot Framework service host override (must be on the allowlisted hosts)",
		},
		{
			name: "TEAMS_ALLOWED_USERS",
			description:
				"CSV of AAD object ids allowed to click approval cards (default DENY)",
		},
		{
			name: "TEAMS_ALLOW_ALL_USERS",
			description: "Explicit opt-in admitting every clicker (dangerous)",
			password: true,
		},
	],
	capabilities: TEAMS_CAPABILITIES,
	rateBudget: TEAMS_RATE_BUDGET,
	trustBoundary: declareTeamsTrustBoundary(),
});
