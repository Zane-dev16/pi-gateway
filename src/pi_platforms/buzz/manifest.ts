// pi_platforms/buzz/manifest — Buzz community-relay platform manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference plugins/platforms/buzz/adapter.py (port semantics, cite
// anchors, never vendor code).
//
// Hermes anchors (adapter.py unless noted):
//   _CHAT_KIND = 9 (@~66)                    → BUZZ_CHAT_KIND
//   _FETCH_LIMIT = 50 (@~68)                 → BUZZ_FETCH_LIMIT
//   _SEEN_CAP = 500 (@~70)                   → BUZZ_SEEN_CAP
//   _DM_DISCOVERY_EVERY = 5 (@~72)           → BUZZ_DM_DISCOVERY_EVERY
//   _DEFAULT_POLL_INTERVAL = 4.0             → BUZZ_DEFAULT_POLL_INTERVAL_S
//   _MIN_POLL_INTERVAL = 1.0                 → BUZZ_MIN_POLL_INTERVAL_S
//   _CLI_TIMEOUT = 30.0                      → BUZZ_CLI_TIMEOUT_S
//   _WS_AUTH_TIMEOUT = 20.0                  → BUZZ_WS_AUTH_TIMEOUT_S
//   _WS_MAX_MESSAGE_BYTES = 2_000_000        → BUZZ_WS_MAX_MESSAGE_BYTES
//   _WS_MEMBERSHIP_KIND = 44100              → BUZZ_WS_MEMBERSHIP_KIND
//   _WS_MEMBERSHIP_SUB_ID "hermes-buzz-membership" → BUZZ_WS_MEMBERSHIP_SUB_ID
//   _DEFAULT_CREDENTIALS_DIR ~/.config/buzz glob "*credentials*.json"
//                                            → BUZZ_CREDENTIALS_DIR / _GLOB
//   credential field order nsec → private_key_hex → private_key
//                                            → BUZZ_CREDENTIAL_FIELDS
//   __init__ require_mention default True, false-tokens {false,0,no,off}
//                                            → BUZZ_REQUIRE_MENTION_DEFAULT / _FALSE_TOKENS
//   __init__ transport ∈ {auto, websocket, poll}, junk ⇒ auto
//                                            → BUZZ_TRANSPORT_MODES
//   register(ctx): required_env ["BUZZ_RELAY_URL","BUZZ_PRIVATE_KEY"] (+ setup
//     prompt(password=True))                 → requiresEnv below
//
// EXCLUSIONS (probe-computed, documented — never silent, never faked green):
//   1. THE LIVE RELAY WEBSOCKET LOOP (_websocket_loop/@~800 + NIP-42 handshake
//      _authenticate_websocket): a persistent authenticated Nostr subscription.
//      The port NEVER opens sockets; the pure crypto core it needs is FULLY
//      ported and contract-tested (nostr-auth.ts + vectors.ts), the transport
//      MODE resolution stays source-true (junk⇒auto, websocket-required fails
//      connect via an injectable ws-starter seam), and the loop itself is
//      excluded by probe. The CLI POLLING plane covers all inbound rows.
//   2. THE REAL buzz CLI BINARY: replaced by the injected FakeBuzzCli seam
//      (cli-wire.ts); argv/env CONTRACTS are behavior-tested instead.
//   3. CREDENTIALS-FILE GLOBS UNDER $HOME (~/.config/buzz): resolved through
//      injected reader/list seams in tests — no real filesystem access.

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:_CHAT_KIND — only kind-9 chat events dispatch to the agent. */
export const BUZZ_CHAT_KIND = 9;
/** adapter.py:_FETCH_LIMIT — events requested per poll / seed call. */
export const BUZZ_FETCH_LIMIT = 50;
/** adapter.py:_SEEN_CAP — per-channel de-dupe bound (events, not bytes). */
export const BUZZ_SEEN_CAP = 500;
/** adapter.py:_DM_DISCOVERY_EVERY — DM rediscovery cadence (poll sweeps). */
export const BUZZ_DM_DISCOVERY_EVERY = 5;
/** adapter.py:_DEFAULT_POLL_INTERVAL seconds. */
export const BUZZ_DEFAULT_POLL_INTERVAL_S = 4;
/** adapter.py:_MIN_POLL_INTERVAL seconds (floor after float parse). */
export const BUZZ_MIN_POLL_INTERVAL_S = 1;
/** adapter.py:_CLI_TIMEOUT seconds before the kill⇒rc124 ladder fires. */
export const BUZZ_CLI_TIMEOUT_S = 30;
/** adapter.py:_WS_AUTH_TIMEOUT seconds for the NIP-42 challenge window. */
export const BUZZ_WS_AUTH_TIMEOUT_S = 20;
/** adapter.py:_WS_MAX_MESSAGE_BYTES — relay frame cap. */
export const BUZZ_WS_MAX_MESSAGE_BYTES = 2_000_000;
/** adapter.py:_WS_MEMBERSHIP_KIND — Buzz channel-membership event kind. */
export const BUZZ_WS_MEMBERSHIP_KIND = 44100;
/** adapter.py:_WS_MEMBERSHIP_SUB_ID — live DM-discovery subscription id. */
export const BUZZ_WS_MEMBERSHIP_SUB_ID = "hermes-buzz-membership";
/** adapter.py:_DEFAULT_CREDENTIALS_DIR (+ its *credentials*.json glob). */
export const BUZZ_CREDENTIALS_DIR = "~/.config/buzz";
export const BUZZ_CREDENTIALS_GLOB = "*credentials*.json";
/** adapter.py:_resolve_private_key field precedence inside a credentials JSON. */
export const BUZZ_CREDENTIAL_FIELDS: readonly string[] = [
	"nsec",
	"private_key_hex",
	"private_key",
];
/** adapter.py:__init__ require_mention default TRUE (channels only). */
export const BUZZ_REQUIRE_MENTION_DEFAULT = true;
/** adapter.py:__init__ false-token set for require_mention parsing. */
export const BUZZ_REQUIRE_MENTION_FALSE_TOKENS: ReadonlySet<string> = new Set([
	"false",
	"0",
	"no",
	"off",
]);
/** adapter.py:__init__ transport modes; anything else resolves to "auto". */
export const BUZZ_TRANSPORT_MODES: readonly string[] = [
	"auto",
	"websocket",
	"poll",
];
/** nostr_auth.py:22242 — the NIP-42 auth-event kind. */
export const BUZZ_AUTH_EVENT_KIND = 22242;

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol,
 * citing the msgraph-webhook/raft precedent rules): Hermes' buzz adapter
 * overrides NEITHER supports_async_delivery NOR interactive_resume, inheriting
 * the base defaults (True/True). Under the msgraph/raft precedent those flags
 * are forced False ONLY when send() cannot ever push a later completion (both
 * of those adapters have log-only no-op sends). Buzz's send() is a REAL
 * synchronous CLI delivery (`messages send` returns accepted/event_id) and
 * inbound polling/WS dispatch feeds live sessions asynchronously — so
 * supportsAsyncDelivery=true AND interactiveResume=true are the HONEST
 * capability data here, matching both the inherited defaults and the DEC-022
 * forged-event wake lane (wakeLane derives "forged-event" from
 * supportsAsyncDelivery — consistent with DEC-022).
 */
export const BUZZ_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
		interactiveResume: true,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: neither inbound plane carries an HMAC
 * wire scheme. The CLI polling plane has NO network ingress at all (outbound
 * request/response to a local binary); the optional WebSocket plane is CLIENT
 * role against the community relay — authenticity rides the NIP-42
 * challenge-response signature scheme declared as first-class manifest data on
 * `nostrKeySchemeIdentity`:
 *
 *   nsec/hex private key → secp256k1 x-only pubkey (BIP-340 derivation) →
 *   signed kind-22242 event over the relay's AUTH challenge (id =
 *   sha256(compact [0,pubkey,created_at,22242,tags,""]), sig = BIP-340).
 *
 * Constant-time comparisons apply wherever secret material is compared
 * (kit secureCompare posture); the signing path compares nothing — the relay
 * verifies our signature. bodySizeCapBytes carries WS_MAX_MESSAGE_BYTES: the
 * only bounded-frame datum this shape declares (client-side inbound cap).
 */
export interface BuzzTrustBoundary extends TrustBoundaryManifest {
	/** THE identity/key scheme as DATA (see note above). */
	nostrKeySchemeIdentity: {
		keyEncodings: readonly ("nsec-bech32" | "hex32")[];
		curve: "secp256k1";
		pubkeyForm: "x-only-bip340";
		authEventKind: typeof BUZZ_AUTH_EVENT_KIND;
		challengeResponse: "nip42";
	};
}

export function declareBuzzTrustBoundary(): BuzzTrustBoundary {
	return {
		ingress: "http", // kit literal; see note — actual planes are CLI-poll (no ingress) + client-role WS
		signatureSchemes: [],
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: BUZZ_SEEN_CAP },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: BUZZ_WS_MAX_MESSAGE_BYTES,
		backpressureWindow: "bounded",
		nostrKeySchemeIdentity: {
			keyEncodings: ["nsec-bech32", "hex32"],
			curve: "secp256k1",
			pubkeyForm: "x-only-bip340",
			authEventKind: BUZZ_AUTH_EVENT_KIND,
			challengeResponse: "nip42",
		},
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture; msgraph precedent):
 * identical checks to kit validateTrustBoundaryManifest EXCEPT scheme
 * presence is satisfied by the declared NIP-42 key-scheme identity instead of
 * a fake HMAC scheme id.
 */
export function validateBuzzTrustBoundary(m: BuzzTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.nostrKeySchemeIdentity.authEventKind !== BUZZ_AUTH_EVENT_KIND) {
		errors.push(
			"buzz ingress must declare the NIP-42 kind-22242 key-scheme identity (no HMAC scheme exists on this wire)",
		);
	}
	if (!Number.isFinite(m.bodySizeCapBytes) || m.bodySizeCapBytes <= 0) {
		errors.push("bodySizeCapBytes must be a positive number");
	}
	if (
		m.idempotency === undefined ||
		!Number.isFinite(m.idempotency.seenSetMaxEntries) ||
		m.idempotency.seenSetMaxEntries <= 0
	) {
		errors.push("idempotency bounds must be declared");
	}
	if (m.scriptTransformsConfinedToHome !== true) {
		errors.push("script transforms must declare home-directory confinement");
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const BUZZ_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "buzz",
	description:
		"Buzz community relay (Block's Nostr-based human+agent platform) via the external buzz CLI — JSON in, JSON out",
	transportShape: "polling" as const,
	requiresEnv: [
		{
			name: "BUZZ_RELAY_URL",
			description:
				"Community relay URL, e.g. https://mycommunity.communities.buzz.xyz",
		},
		{
			name: "BUZZ_PRIVATE_KEY",
			description:
				"Nostr private key (nsec or hex) — travels to the CLI via env only, never argv",
			prompt: true,
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "BUZZ_CHANNELS",
			description:
				"Comma-separated channel UUIDs to watch (empty = all joined)",
		},
		{
			name: "BUZZ_HOME_CHANNEL",
			description: "Home channel UUID for cron/notification delivery",
		},
		{
			name: "BUZZ_POLL_INTERVAL",
			description: "Seconds between poll sweeps (min 1)",
		},
		{
			name: "BUZZ_CLI_PATH",
			description: "Path to the buzz binary (default PATH then ~/bin/buzz)",
		},
		{
			name: "BUZZ_CREDENTIALS_FILE",
			description: "JSON file holding the nsec (fallback for BUZZ_PRIVATE_KEY)",
		},
		{
			name: "BUZZ_ALLOWED_USERS",
			description:
				"Comma-separated npubs/hex pubkeys allowed to talk (empty = no adapter-level filter)",
		},
		{
			name: "BUZZ_ALLOW_ALL_USERS",
			description: "Gateway-central allow-all flag",
		},
		{
			name: "BUZZ_REQUIRE_MENTION",
			description: "Channels require @mention (default true; DMs always pass)",
		},
		{
			name: "BUZZ_TRANSPORT",
			description: "auto | websocket | poll (junk ⇒ auto)",
		},
		{
			name: "BUZZ_AUTH_TAG",
			description: 'Optional four-string ["auth",…] owner-attestation tag JSON',
		},
	],
	capabilities: BUZZ_CAPABILITIES,
	trustBoundary: declareBuzzTrustBoundary(),
});
