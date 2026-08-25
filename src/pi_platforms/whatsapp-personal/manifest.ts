// pi_platforms/whatsapp-personal/manifest — WhatsApp PERSONAL (Baileys
// bridge) adapter manifest. EVERY policy-shaped number is MANIFEST DATA
// transcribed from the READ-ONLY Hermes reference (port semantics, cite
// anchors, never vendor code); vendor ground truth fills gaps Hermes carries
// no constant for (Q17/DEC-017).
//
// Hermes anchors (plugins/platforms/whatsapp/adapter.py unless noted):
//   __init__: config.extra.get("bridge_port", 3000)      → WA_BRIDGE_PORT
//   _poll_messages: await asyncio.sleep(1)               → WA_POLL_INTERVAL_MS
//   _poll_messages except: print("Poll error") + sleep(5)→ WA_POLL_ERROR_BACKOFF_MS
//   GET /messages aiohttp.ClientTimeout(total=30)        → WA_POLL_TIMEOUT_MS
//   POST /send   ClientTimeout(total=30)                 → WA_SEND_TIMEOUT_MS
//   POST /edit   ClientTimeout(total=15)                 → WA_EDIT_TIMEOUT_MS
//   POST /read   ClientTimeout(total=5)                  → WA_READ_RECEIPT_TIMEOUT_MS
//   send() "Small delay between chunks": sleep(0.3)
//     guarded by len(chunks) > 1                         → WA_INTER_CHUNK_DELAY_MS
//   _text_batch_delay_seconds default 5.0 /
//   _text_batch_split_delay_seconds default 10.0 (__init__ coerce_float_extra)
//                                                        → WA_TEXT_BATCH_DELAY_SECONDS
//                                                          WA_TEXT_BATCH_SPLIT_DELAY_SECONDS
//   _SPLIT_THRESHOLD = 6000 ("WhatsApp supports ~65K chars;
//     generous threshold")                               → WA_TEXT_SPLIT_THRESHOLD_CHARS
//   whatsapp_common.py:MAX_MESSAGE_LENGTH = 4096 ("practical UX limit,
//     not protocol max")                                 → WA_MAX_MESSAGE_LENGTH
//   _outgoing_chunk_limit: max(1024, MAX − prefix_len)
//     ("Keep enough space for truncate_message's pagination indicator and
//     code-fence repair even if a user configures a very long prefix")
//                                                        → WA_MIN_CHUNK_LIMIT
//   whatsapp_common.py:DEFAULT_REPLY_PREFIX              → WA_DEFAULT_REPLY_PREFIX
//   whatsapp_common.py:_effective_reply_prefix WHATSAPP_MODE default
//     "self-chat"                                        → WA_DEFAULT_MODE
//   connect() creds.json pre-flight / node-missing / bridge-script-missing
//     fatal codes                                        → FATAL_* codes below
//   module tail _WA_IMAGE_EXTS/_WA_VIDEO_EXTS/_WA_AUDIO_EXTS +
//   _bridge_media_type                                   → media ext sets
//
// Media upload/download FILE caching, QR pairing, npm install, pid-file OS
// mechanics are documented probe-computed EXCLUSIONS (see adapter header) —
// the media EXT SETS are ported as data because they are pure mapping tables.

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

// ── bridge plane ─────────────────────────────────────────────────────────────

/** adapter.py __init__: bridge_port default 3000 (loopback HTTP plane). */
export const WA_BRIDGE_PORT = 3000;
/** adapter.py:_poll_messages trailing sleep(1) — the poll heartbeat. */
export const WA_POLL_INTERVAL_MS = 1000;
/** adapter.py:_poll_messages error path: "Poll error" → sleep(5) → continue. */
export const WA_POLL_ERROR_BACKOFF_MS = 5000;
/** GET /messages ClientTimeout(total=30). */
export const WA_POLL_TIMEOUT_MS = 30_000;
/** POST /send ClientTimeout(total=30). */
export const WA_SEND_TIMEOUT_MS = 30_000;
/** POST /edit ClientTimeout(total=15). */
export const WA_EDIT_TIMEOUT_MS = 15_000;
/** POST /read ClientTimeout(total=5). */
export const WA_READ_RECEIPT_TIMEOUT_MS = 5_000;
/** send(): inter-chunk pacing, applied ONLY when len(chunks) > 1. */
export const WA_INTER_CHUNK_DELAY_MS = 300;

// ── debounce batching plane ──────────────────────────────────────────────────

/** __init__ text_batch_delay_seconds default 5.0. */
export const WA_TEXT_BATCH_DELAY_SECONDS = 5.0;
/** __init__ text_batch_split_delay_seconds default 10.0. */
export const WA_TEXT_BATCH_SPLIT_DELAY_SECONDS = 10.0;
/** WhatsAppBehaviorMixin._SPLIT_THRESHOLD — last-chunk length ≥ this ⇒ split delay. */
export const WA_TEXT_SPLIT_THRESHOLD_CHARS = 6000;

/**
 * PORT HARDENING (no Hermes constant): the reference keeps
 * `_pending_text_batches` as an UNBOUNDED dict keyed by session key. The port
 * bounds it drop-oldest so a hostile session-key fan-out cannot grow without
 * limit; the bound doubles as the declared idempotency bound in the trust
 * boundary (rapid duplicate deliveries within one window collapse into ONE
 * debounced dispatch).
 */
export const WA_PENDING_BATCH_CAP = 256;

/**
 * PORT HARDENING (documented): the source declares NO poll-body byte cap —
 * the only bound is the 30s aiohttp client timeout. The port pins a defensive
 * JSON-array cap (~150 × the 65K WhatsApp message ceiling) so a runaway bridge
 * cannot balloon memory before parse.
 */
export const WA_POLL_BODY_CAP_BYTES = 10 * 1024 * 1024;

// ── text plane ───────────────────────────────────────────────────────────────

/** whatsapp_common.py:MAX_MESSAGE_LENGTH — practical UX cap, not protocol max. */
export const WA_MAX_MESSAGE_LENGTH = 4096;
/** whatsapp_common.py:_outgoing_chunk_limit floor: max(1024, …). */
export const WA_MIN_CHUNK_LIMIT = 1024;

/** whatsapp_common.py:WhatsAppBehaviorMixin.DEFAULT_REPLY_PREFIX verbatim. */
export const WA_DEFAULT_REPLY_PREFIX = "⚕ *Hermes Agent*\n────────────\n";

/** whatsapp_common.py:_effective_reply_prefix WHATSAPP_MODE default. */
export const WA_DEFAULT_MODE = "self-chat";

/** adapter.py _OWNER_REPLY_PREFIX — owner-typed inbound text marker. */
export const WA_OWNER_REPLY_PREFIX = "[owner reply] ";

// ── media ext sets (adapter.py module tail — pure mapping DATA) ─────────────

export const WA_IMAGE_EXTS: ReadonlySet<string> = new Set([
	".jpg",
	".jpeg",
	".png",
	".webp",
	".gif",
]);
export const WA_VIDEO_EXTS: ReadonlySet<string> = new Set([
	".mp4",
	".mov",
	".avi",
	".mkv",
	".webm",
	".3gp",
]);
export const WA_AUDIO_EXTS: ReadonlySet<string> = new Set([
	".ogg",
	".opus",
	".mp3",
	".wav",
	".m4a",
	".flac",
]);

/**
 * adapter.py:_bridge_media_type — local file → bridge /send-media mediaType.
 * Voice notes and audio route to "audio"; forceDocument (the [[as_document]]
 * directive analog) forces every file to "document".
 */
export function bridgeMediaType(
	filePath: string,
	isVoice: boolean,
	forceDocument: boolean,
): "image" | "video" | "audio" | "document" {
	if (forceDocument) return "document";
	const dot = filePath.lastIndexOf(".");
	const ext = dot >= 0 ? filePath.slice(dot).toLowerCase() : "";
	if (isVoice || WA_AUDIO_EXTS.has(ext)) return "audio";
	if (WA_IMAGE_EXTS.has(ext)) return "image";
	if (WA_VIDEO_EXTS.has(ext)) return "video";
	return "document";
}

// ── env names (adapter.py/_wenv reads + plugin.yaml registration) ───────────

export const WA_ENV_ENABLED = "WHATSAPP_ENABLED";
export const WA_ENV_ALLOWED_USERS = "WHATSAPP_ALLOWED_USERS";
export const WA_ENV_ALLOW_ALL_USERS = "WHATSAPP_ALLOW_ALL_USERS";
export const WA_ENV_GATEWAY_ALLOW_ALL_USERS = "GATEWAY_ALLOW_ALL_USERS";
export const WA_ENV_DM_POLICY = "WHATSAPP_DM_POLICY";
export const WA_ENV_GROUP_POLICY = "WHATSAPP_GROUP_POLICY";
export const WA_ENV_REQUIRE_MENTION = "WHATSAPP_REQUIRE_MENTION";
export const WA_ENV_MENTION_PATTERNS = "WHATSAPP_MENTION_PATTERNS";
export const WA_ENV_FREE_RESPONSE_CHATS = "WHATSAPP_FREE_RESPONSE_CHATS";
export const WA_ENV_REPLY_PREFIX = "WHATSAPP_REPLY_PREFIX";
export const WA_ENV_MODE = "WHATSAPP_MODE";

// ── connect-ladder fatal codes (connect() pre-flight order) ─────────────────

/** connect(): node binary missing — checked FIRST. */
export const FATAL_NODE_MISSING = "whatsapp_node_missing";
/** connect(): bridge script path missing — checked SECOND. */
export const FATAL_BRIDGE_MISSING = "whatsapp_bridge_missing";
/** connect(): session_path/creds.json absent — "enabled but not paired",
 * NON-retryable so the user gets a clear pairing message instead of the
 * watcher silently hammering an unconfigured platform. Checked THIRD. */
export const FATAL_NOT_PAIRED = "whatsapp_not_paired";
/** _check_managed_bridge_exit: managed child exited outside planned shutdown. */
export const FATAL_BRIDGE_EXITED = "whatsapp_bridge_exited";

// ── capabilities AS DATA (04 §2) ────────────────────────────────────────────

/**
 * Capabilities AS DATA.
 *
 * DIVERGENCE NOTE (logged per DEC-026 protocol — msgraph/raft precedent
 * wording): Hermes' personal adapter overrides NEITHER supports_async_delivery
 * NOR interactive_resume, so it inherits the base defaults True/True. The
 * stateless pairing (`interactive_resume=False` + `supports_async_delivery=
 * False`) that msgraph/raft adopted does NOT transfer here, because the
 * reasoning behind it — "send() is a log-only stub that can never push a later
 * completion" — does not hold: this adapter's egress is a REAL synchronous
 * POST /send against the loopback bridge, available whenever the bridge is up,
 * so later completions ARE deliverable (they ride send(), not a push lane).
 * Inbound delivery cadence is POLLED (GET /messages every 1s) which gates
 * INGRESS latency, never outbound capability. The port therefore keeps BOTH
 * FLAGS TRUE — honest data AND zero divergence from Hermes inheritance.
 *
 * DEC-022 pairing consistency: supportsAsyncDelivery=true ⇒ wakeLane
 * "forged-event" (internal wakes traverse the standard guards); the derivation
 * lives in BasePlatformAdapter.wakeLane and is asserted consistent by the
 * shared wake.lane-declaration-consistent row.
 *
 * splitsLongMessages=True ports the adapter class attribute
 * `splits_long_messages = True` verbatim: send() chunks via
 * truncate_message ≙ kit chunkWithFenceCarry INSIDE the adapter-owned wireSend
 * (reply-context quoted on the FIRST chunk only — adapter.py:send loop).
 */
export const WHATSAPP_PERSONAL_CAPABILITIES: Readonly<
	Partial<CapabilityManifest>
> = Object.freeze({
	supportsAsyncDelivery: true,
	interactiveResume: true,
	splitsLongMessages: true,
});

// ── DEC-017 trust boundary ──────────────────────────────────────────────────

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: the personal adapter talks to a LOCAL
 * NODE BRIDGE daemon over LOOPBACK HTTP that THIS process spawned (or found
 * healthy on 127.0.0.1). The wire carries NO HMAC scheme and NO shared-secret
 * header at all (adapter.py issues bare GET /messages, POST /send… calls).
 * Authenticity rides the two mechanisms Hermes relies on:
 *   1. loopback binding of the bridge HTTP plane (bridge_port 3000 on
 *      127.0.0.1 — never exposed off-host), and
 *   2. process parentage: the managed bridge child is OUR subprocess
 *      (_check_managed_bridge_exit owns its lifecycle; stale foreign bridges
 *      are evicted, never trusted).
 *
 * validateTrustBoundaryManifest requires ≥1 HMAC wire-format scheme, which
 * would force Lying Data (declaring a signature header the wire does not
 * carry). This adapter validates through validateWaPersonalTrustBoundary
 * below: identical checks EXCEPT scheme presence is satisfied by the
 * documented bridgeLoopbackTrust marker instead of a fake scheme id — the
 * msgraph clientStateSecretCompare precedent (msgraph-webhook/manifest.ts).
 */
export interface WaPersonalTrustBoundary extends TrustBoundaryManifest {
	/**
	 * THE non-HMAC authenticity marker (msgraph precedent): constant-time
	 * secret comparison IS replaced by loopback binding + child-process
	 * parentage on this wire.
	 */
	bridgeLoopbackTrust: true;
}

export function waPersonalTrustBoundary(): WaPersonalTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		// Vacuously satisfied: NO shared-secret comparison exists on this wire
		// at all (nothing to time-harden). The kit type requires the literal.
		constantTimeCompare: true,
		idempotency: { seenSetMaxEntries: WA_PENDING_BATCH_CAP },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: WA_POLL_BODY_CAP_BYTES,
		backpressureWindow: "bounded",
		bridgeLoopbackTrust: true,
	};
}

/** Construction-time boundary validation (DEC-017 posture; msgraph shape). */
export function validateWaPersonalTrustBoundary(
	m: WaPersonalTrustBoundary,
): string[] {
	const errors: string[] = [];
	if (m.bridgeLoopbackTrust !== true) {
		errors.push(
			"whatsapp-personal ingress must declare bridgeLoopbackTrust: true (no HMAC scheme exists on this wire)",
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
		errors.push("idempotency seen-set bounds must be declared");
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow; plugin.yaml parity) ─────────

export const WHATSAPP_PERSONAL_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "whatsapp",
	description:
		"Personal-account WhatsApp via the local Node.js (Baileys) bridge over loopback HTTP — polling shape",
	transportShape: "polling" as const,
	requiresEnv: [
		{
			name: WA_ENV_ENABLED,
			description:
				"Enable the WhatsApp adapter (requires the Node.js bridge running)",
			password: false,
		},
	],
	optionalEnv: [
		{
			name: WA_ENV_ALLOWED_USERS,
			description:
				"Comma-separated WhatsApp user IDs allowed to talk to the bot",
			password: false,
		},
		{
			name: WA_ENV_ALLOW_ALL_USERS,
			description: "Allow any WhatsApp user to trigger the bot (dev only)",
			password: false,
		},
		{
			name: "WHATSAPP_HOME_CHANNEL",
			description: "Default chat ID for cron / notification delivery",
			password: false,
		},
		{
			name: "WHATSAPP_HOME_CHANNEL_NAME",
			description: "Display name for the WhatsApp home channel",
			password: false,
		},
	],
	capabilities: WHATSAPP_PERSONAL_CAPABILITIES,
	trustBoundary: waPersonalTrustBoundary(),
});
