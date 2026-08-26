// pi_platforms/photon/manifest — Photon Spectrum (iMessage) platform manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference (port semantics, cite anchors, never vendor code).
//
// Hermes anchors (plugins/platforms/photon/adapter.py unless noted):
//   _DEFAULT_SIDECAR_PORT = 8789 (@~99)            → PHOTON_DEFAULT_SIDECAR_PORT
//   _DEFAULT_SIDECAR_BIND = "127.0.0.1" (@~100)    → PHOTON_SIDECAR_BIND
//   _MAX_MESSAGE_LENGTH = 8000 (@~104)             → PHOTON_MAX_MESSAGE_LENGTH
//   _DEDUP_MAX_SIZE = 4000 (@~168)                 → PHOTON_DEDUP_MAX_SIZE
//   _DEDUP_WINDOW_SECONDS = 48 * 3600 (@~169)      → PHOTON_DEDUP_WINDOW_SECONDS
//   _FFFC_WAIT_SECONDS = 15.0 (@~171)              → PHOTON_FFFC_WAIT_SECONDS
//   _PHOTON_RETRYABLE_PATTERNS (@~252)             → PHOTON_RETRYABLE_PATTERNS
//   _RICHLINK_PREVIEW_SUPPRESS_SECONDS = 30.0 (@~262)
//   _RICHLINK_PREVIEW_ATTACHMENT_SUFFIX (".pluginpayloadattachment", @~263)
//   _TYPING_COOLDOWN_SECONDS = 5.0 (@~266)         → PHOTON_TYPING_COOLDOWN_MS
//   _DEFAULT_MENTION_PATTERNS (@~277, two Hermes wake words)
//   SUPPORTS_MESSAGE_EDITING = False (@~721)       → static no-edit posture
//   probe defaults: interval 600s / timeout 10s / max failures 3 (@~762-786)
//   _SENT_IDS_MAX = 1000, _LAST_INBOUND_CHATS_MAX = 200 (@~2224-2225)
//   _DM_CHAT_GUID_RE = ^any;-;(\+\d{6,})$ (@~2241) → normalizeChatKey
//   plugin.yaml requires_env/optional_env          → PHOTON_PLUGIN_MANIFEST
//
// DOCUMENTED PROBE-COMPUTED EXCLUSIONS (never silent, never faked green):
//   - SIDEcar PROCESS LIFECYCLE: _start_sidecar/_stop_sidecar/_supervise_sidecar
//     spawn/supervise a Node child (subprocess.Popen of sidecar/index.mjs with
//     stdin-EOF watchdog). The port NEVER spawns OS children; the exact spawn
//     contract is exported as PURE DATA via buildSidecarSpawnCommand().
//   - npm self-heal installs (_reinstall_sidecar_deps, _NPM_REINSTALL_TIMEOUT),
//     stale-deps detection (_sidecar_deps_stale) and the npm error log file.
//   - Runtime record files (_write/_read/_delete_runtime_record, owner-only
//     perms, pid liveness probes) and orphan reaping (lsof/ps/SIGTERM ladder).
//   - gRPC internals + the spectrum-ts SDK patch script; the sidecar control
//     plane enters the port as an INJECTED SidecarTransport seam instead, and
//     inbound rides a PushIngress line driver over the SAME NDJSON semantics.
//   - Inbound attachment BYTE CACHING to disk (_cache_inbound_attachment):
//     metadata-only marker path preserved verbatim; no filesystem writes.
//   - The U+FFFC attachment-wait TIMER TASK (asyncio.create_task +
//     _FFFC_WAIT_SECONDS): registration/cancel-on-arrival/disconnect-clear
//     semantics are kept as state transitions; the wall-clock wait itself is
//     part of the excluded attachment-retrieval machinery.

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:_DEFAULT_SIDECAR_PORT — loopback control channel. */
export const PHOTON_DEFAULT_SIDECAR_PORT = 8789;
/** adapter.py:_DEFAULT_SIDECAR_BIND — loopback by construction. */
export const PHOTON_SIDECAR_BIND = "127.0.0.1";
/**
 * adapter.py:_MAX_MESSAGE_LENGTH — conservative iMessage cap matching
 * BlueBubbles. BOTH the chunk budget AND the hard outbound truncation use it.
 */
export const PHOTON_MAX_MESSAGE_LENGTH = 8000;
/** adapter.py:_DEDUP_MAX_SIZE — HARD seen-set bound (insertion-order eviction). */
export const PHOTON_DEDUP_MAX_SIZE = 4000;
/** adapter.py:_DEDUP_WINDOW_SECONDS — at-least-once gRPC replay window. */
export const PHOTON_DEDUP_WINDOW_SECONDS = 48 * 3600;
export const PHOTON_DEDUP_WINDOW_MS = PHOTON_DEDUP_WINDOW_SECONDS * 1000;
/** adapter.py:_FFFC_WAIT_SECONDS (excluded timer machinery — see header). */
export const PHOTON_FFFC_WAIT_SECONDS = 15.0;

/** adapter.py:_PHOTON_RETRYABLE_PATTERNS — Photon/Envoy transient overload set. */
export const PHOTON_RETRYABLE_PATTERNS: readonly string[] = Object.freeze([
	"internal sidecar error",
	"upstream connect error",
	"upstream unavailable",
	"connection dropped",
	"reset reason: overflow",
	"upstream_overflow",
	"upstream_unavailable",
]);

/** adapter.py:_RICHLINK_PREVIEW_SUPPRESS_SECONDS — preview-artifact window. */
export const PHOTON_RICHLINK_PREVIEW_SUPPRESS_MS = 30.0 * 1000;
/** adapter.py:_RICHLINK_PREVIEW_ATTACHMENT_SUFFIX — OpenGraph preview marker. */
export const PHOTON_RICHLINK_PREVIEW_ATTACHMENT_SUFFIX =
	".pluginpayloadattachment";
/** adapter.py:_TYPING_COOLDOWN_SECONDS — per-chat typing-indicator throttle. */
export const PHOTON_TYPING_COOLDOWN_MS = 5.0 * 1000;
/** adapter.py:_SENT_IDS_MAX — OUR sent-id tracker bound (reaction targeting). */
export const PHOTON_SENT_IDS_MAX = 1000;
/** adapter.py:_LAST_INBOUND_CHATS_MAX — per-chat trackers bound. */
export const PHOTON_LAST_INBOUND_CHATS_MAX = 200;

// ── presence watchdog (__init__ @~762-790) ──────────────────────────────────

/** Conservative default: probe only after 10+ minutes of stream silence. */
export const PHOTON_PROBE_INTERVAL_MS = 600.0 * 1000;
/** Probe HTTP-call timeout — a hung call is the ONLY respawn-counting verdict. */
export const PHOTON_PROBE_TIMEOUT_MS = 10.0 * 1000;
/** Consecutive hung probes before exactly one sidecar respawn. */
export const PHOTON_PROBE_MAX_FAILURES = 3;
/** adapter.py:_sidecar_health_interval — /healthz degraded-stream poll. */
export const PHOTON_SIDECAR_HEALTH_INTERVAL_MS = 15.0 * 1000;

// ── group-chat mention gating (@~277) ───────────────────────────────────────

/**
 * adapter.py:_DEFAULT_MENTION_PATTERNS — THE TWO Hermes wake words, byte-exact
 * source strings. JS lookbehind is supported by the target runtime.
 */
export const PHOTON_DEFAULT_MENTION_PATTERN_SOURCES: readonly string[] =
	Object.freeze([
		"(?<![\\w@])@?hermes\\s+agent\\b[,:\\-]?",
		"(?<![\\w@])@?hermes\\b[,:\\-]?",
	]);

/**
 * adapter.py:_TARGET_NOT_ALLOWED_MESSAGE — canonical user-facing explanation
 * replacing raw upstream error text for error_class=target_not_allowed.
 */
export const PHOTON_TARGET_NOT_ALLOWED_MESSAGE =
	"shared/free-tier Photon lines cannot initiate outbound sends to new " +
	"targets — upgrade to a dedicated line or use another delivery channel";

/** adapter.py:_DM_CHAT_GUID_RE — DM space addressable two ways. */
const PHOTON_DM_CHAT_GUID_RE = /^any;-;(\+\d{6,})$/;

/**
 * adapter.py:_normalize_chat_key — the sidecar's resolveSpace treats the DM
 * GUID (`any;-;+1555…`) and the bare E.164 phone as the SAME space; per-chat
 * trackers normalize to the bare phone.
 */
export function normalizeChatKey(chatId: string): string {
	const match = PHOTON_DM_CHAT_GUID_RE.exec(chatId);
	return match ? (match[1] as string) : chatId;
}

/**
 * Capabilities AS DATA (04 §2).
 *
 * supportsAsyncDelivery TRUE — the sidecar pushes inbound asynchronously over
 * its long-lived gRPC stream (plugin.yaml: "no webhook, no public URL"); DEC-022
 * wakeLane therefore resolves forged-event consistently.
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol, same
 * ruling as the msgraph-webhook/raft ports): Hermes' photon adapter overrides
 * NEITHER supports_async_delivery NOR interactive_resume, so it inherits the
 * base defaults (True/True). interactive_resume=True routes resume prompts
 * through reconcile-by-edit — but PhotonAdapter.SUPPORTS_MESSAGE_EDITING=False
 * (adapter.py @~721, "no real edit API for already-sent messages"), so every
 * edit-reconcile attempt would fail on this wire BY CONSTRUCTION. The port
 * declares interactiveResume FALSE — the honest capability data derived from
 * edit reality — while keeping supportsAsyncDelivery TRUE (source-faithful:
 * inbound truly pushes).
 */
export const PHOTON_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: there is NO HMAC scheme anywhere on
 * this wire. The plugin ships NO webhook and NO public URL (plugin.yaml);
 * the only HTTP plane is the ADAPTER→SIDEcar loopback control channel
 * authenticated by the generated sidecar token carried in the
 * X-Hermes-Sidecar-Token header (compared constant-time server-side). The
 * local validator marker names that mechanism (msgraph clientState precedent).
 */
export interface PhotonTrustBoundary extends TrustBoundaryManifest {
	/** Loopback sidecar-token header compare IS the authenticity mechanism. */
	sidecarTokenHeaderCompare: true;
}

export function declarePhotonTrustBoundary(): PhotonTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		// At-least-once gRPC stream ⇒ delivery-id dedupe with a HARD bound:
		// 4000 ids over a 48h window (adapter.py:_DEDUP_MAX_SIZE/_DEDUP_WINDOW).
		idempotency: { seenSetMaxEntries: PHOTON_DEDUP_MAX_SIZE },
		scriptTransformsConfinedToHome: true,
		// Outbound message text cap — the honest body-size datum on this wire.
		bodySizeCapBytes: PHOTON_MAX_MESSAGE_LENGTH,
		cidrAllowlist: [],
		backpressureWindow: "bounded",
		sidecarTokenHeaderCompare: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with scheme presence satisfied by the
 * documented loopback token mechanism instead of a fake scheme id (msgraph/raft
 * precedent).
 */
export function validatePhotonTrustBoundary(m: PhotonTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.sidecarTokenHeaderCompare !== true) {
		errors.push(
			"photon ingress must declare sidecarTokenHeaderCompare: true (no HMAC scheme exists on this wire)",
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
	if ((m.cidrAllowlist ?? []).length > 0) {
		errors.push(
			"photon binds the sidecar loopback-only — no CIDR allowlist applies",
		);
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow; plugin.yaml transcription) ──

export const PHOTON_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "photon",
	description:
		"iMessage via Photon Spectrum — both directions ride the spectrum-ts " +
		"gRPC stream through a supervised Node sidecar (long-lived stream = " +
		"persistent-ws family); the sidecar control plane enters this port as " +
		"an injected transport seam",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "PHOTON_PROJECT_ID",
			description:
				"Spectrum project id (the project's spectrumProjectId; set by `hermes photon setup`)",
		},
		{
			name: "PHOTON_PROJECT_SECRET",
			description:
				"Project secret paired with the Spectrum project id (set by `hermes photon setup`)",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "PHOTON_SIDECAR_PORT",
			description:
				"Loopback port for the Node sidecar control + inbound channel (default 8789)",
		},
		{
			name: "PHOTON_SIDECAR_AUTOSTART",
			description:
				"Spawn the Node sidecar on connect (true/false, default true)",
		},
		{
			name: "PHOTON_NODE_BIN",
			description: "Path to the node binary (default: shutil.which('node'))",
		},
		{
			name: "PHOTON_DASHBOARD_HOST",
			description:
				"Photon Dashboard API host (default https://app.photon.codes)",
		},
		{
			name: "PHOTON_SPECTRUM_HOST",
			description:
				"Photon Spectrum API host (default https://spectrum.photon.codes)",
		},
		{
			name: "PHOTON_ALLOWED_USERS",
			description:
				"Comma-separated E.164 phone numbers allowed to talk to the bot",
		},
		{
			name: "PHOTON_ALLOW_ALL_USERS",
			description:
				"Allow any sender to trigger the bot (dev only — disables allowlist)",
		},
		{
			name: "PHOTON_REQUIRE_MENTION",
			description:
				"Ignore group-chat messages unless they match a mention wake word (true/false, default false)",
		},
		{
			name: "PHOTON_MENTION_PATTERNS",
			description:
				"Mention wake-word regexes for group chats (JSON list or comma/newline-separated; defaults to Hermes wake words)",
		},
		{
			name: "PHOTON_HOME_CHANNEL",
			description:
				"Default Photon target for cron / notification delivery: Spectrum space id, DM GUID, or bare E.164 phone number",
		},
		{
			name: "PHOTON_HOME_CHANNEL_NAME",
			description: "Human label for the home channel",
		},
		{
			name: "PHOTON_TELEMETRY",
			description:
				"Enable Spectrum SDK telemetry in the sidecar (true/false, default false)",
		},
		{
			name: "PHOTON_MARKDOWN",
			description:
				"Send agent replies as markdown — iMessage renders it natively (true/false, default true)",
		},
		{
			name: "PHOTON_REACTIONS",
			description:
				"Tapback 👀/👍/👎 on messages as processing status and route tapbacks on bot messages to the agent (true/false, default false)",
		},
	],
	capabilities: PHOTON_CAPABILITIES,
	trustBoundary: declarePhotonTrustBoundary(),
});

/**
 * _start_sidecar spawn contract as PURE DATA (the port spawns NO OS children).
 * Transcribed from adapter.py:_start_sidecar env assembly (@~1629-1638): the
 * Node child gets the project credentials (PHOTON_PROJECT_ID/PHOTON_PROJECT_SECRET)
 * + loopback bind/port + the generated sidecar token (PHOTON_SIDECAR_TOKEN,
 * adapter.py @750: scoped secret or token_hex) so the documented child contract
 * can authenticate, and PHOTON_SIDECAR_WATCH_STDIN=1 so gateway death of ANY
 * kind (even SIGKILL) takes the sidecar down via stdin EOF. Values are injected
 * AT SPAWN TIME by the caller (the excluded lifecycle owner).
 */
export function buildSidecarSpawnCommand(opts: {
	nodeBin: string;
	sidecarDir: string;
	port?: number | undefined;
	/** adapter.py env["PHOTON_PROJECT_ID"] = self._project_id. */
	projectId: string;
	/** adapter.py env["PHOTON_PROJECT_SECRET"] = self._project_secret. */
	projectSecret: string;
	/** adapter.py env["PHOTON_SIDECAR_TOKEN"] = self._sidecar_token. */
	sidecarToken: string;
}): {
	argv: readonly string[];
	envVars: Readonly<Record<string, string>>;
} {
	return {
		argv: [opts.nodeBin, `${opts.sidecarDir}/index.mjs`],
		envVars: {
			PHOTON_PROJECT_ID: opts.projectId,
			PHOTON_PROJECT_SECRET: opts.projectSecret,
			PHOTON_SIDECAR_PORT: String(opts.port ?? PHOTON_DEFAULT_SIDECAR_PORT),
			PHOTON_SIDECAR_BIND: PHOTON_SIDECAR_BIND,
			PHOTON_SIDECAR_TOKEN: opts.sidecarToken,
			PHOTON_SIDECAR_WATCH_STDIN: "1",
		},
	};
}

/** The sidecar auth header (adapter.py:_sidecar_call headers). */
export const PHOTON_SIDECAR_TOKEN_HEADER = "X-Hermes-Sidecar-Token";
