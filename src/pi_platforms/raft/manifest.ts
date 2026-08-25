// pi_platforms/raft/manifest — Raft external-agent wake-channel manifest.
// EVERY policy-shaped number is MANIFEST DATA transcribed from the READ-ONLY
// Hermes reference (port semantics, cite anchors, never vendor code).
//
// Hermes anchors (plugins/platforms/raft/adapter.py unless noted):
//   DEFAULT_HOST = "127.0.0.1" (@~78)            → RAFT_DEFAULT_HOST
//   DEFAULT_PORT = 0 (ephemeral) (@~79)          → RAFT_DEFAULT_PORT
//   DEFAULT_PATH = "/wake" (@~80)                → RAFT_DEFAULT_WAKE_PATH
//   DEFAULT_RUNTIME_SESSION = "default" (@~81)   → RAFT_DEFAULT_RUNTIME_SESSION
//   DEFAULT_MAX_BODY_BYTES = 16_384 (@~82)       → RAFT_BODY_CAP_BYTES
//   DEFAULT_ACTIVITY_QUEUE_CAP = 500 (@~83)      → RAFT_ACTIVITY_QUEUE_CAP
//   ACTIVITY_CONTENT_CAP = 4096 (@~84)           → RAFT_ACTIVITY_CONTENT_CAP
//   ACTIVITY_EVENT_SCHEMA = "raft-activity.v1" (@~85)
//   ACTIVITY_DRAIN_SCHEMA = "raft-activity-drain.v1" (@~86)
//   BRIDGE_TOKEN_HEADER = "x-raft-bridge-token" (@~87)
//   _CONTENT_FIELD_NAMES (@~89): body|content|message|messages|preview|
//     snippet|text — the CONTENT-FREE wake contract
//   _SAFE_SCALAR_RE + _MAX_SCALAR_LENGTH=120 (@~96): activity-field charset
//   _validate_activity_event (@~205): schema equality, allowed-fields,
//     safe-scalar requirements, ok|error status, non-negative durationMs,
//     boolean flags, toolInput/toolOutput truncation flags
//   _handle_wake verdict ladder (@~600): 401 unauthorized → 413
//     payload_too_large (declared length AND actual bytes) → 400 invalid_json |
//     invalid_payload | content_not_allowed → 503 not_ready → 202 accepted
//   ActivityQueue (@~270): bounded deque, drop-OLDEST overflow counted in
//     dropped_since_drain; drain(max≤200 default 200) resets the drop counter
//   _spawn_bridge (@~700): `raft --profile <p> agent bridge --wake-adapter
//     wake-channel --wake-channel-endpoint <url>` with RAFT_CHANNEL_TOKEN env —
//     the bridge is an EXTERNAL child the port NEVER spawns (probe-computed
//     exclusion; command shape exported as pure data below)
//   send() (@~760): "adapter send is a no-op; agent delivers via raft CLI"
//   register(ctx): required_env ["RAFT_PROFILE"]; wake-only mode when the CLI
//     is missing or RAFT_PROFILE unset

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";

/** adapter.py:DEFAULT_HOST — loopback by construction. */
export const RAFT_DEFAULT_HOST = "127.0.0.1";
/** adapter.py:DEFAULT_PORT — 0 lets the OS pick an ephemeral port. */
export const RAFT_DEFAULT_PORT = 0;
/** adapter.py:DEFAULT_PATH. */
export const RAFT_DEFAULT_WAKE_PATH = "/wake";
/** adapter.py:DEFAULT_RUNTIME_SESSION. */
export const RAFT_DEFAULT_RUNTIME_SESSION = "default";
/** adapter.py:DEFAULT_MAX_BODY_BYTES — declared-length AND actual-bytes cap. */
export const RAFT_BODY_CAP_BYTES = 16_384;
/** adapter.py:DEFAULT_ACTIVITY_QUEUE_CAP — bounded at-most-once telemetry. */
export const RAFT_ACTIVITY_QUEUE_CAP = 500;
/** adapter.py:ACTIVITY_CONTENT_CAP — toolInput/toolOutput per-field cap. */
export const RAFT_ACTIVITY_CONTENT_CAP = 4096;
/** adapter.py:ACTIVITY_EVENT_SCHEMA / ACTIVITY_DRAIN_SCHEMA. */
export const RAFT_ACTIVITY_EVENT_SCHEMA = "raft-activity.v1";
export const RAFT_ACTIVITY_DRAIN_SCHEMA = "raft-activity-drain.v1";
/** adapter.py:BRIDGE_TOKEN_HEADER. */
export const RAFT_BRIDGE_TOKEN_HEADER = "x-raft-bridge-token";
/** adapter.py:_MAX_SCALAR_LENGTH. */
export const RAFT_MAX_SCALAR_LENGTH = 120;
/** adapter.py:drain default/max (`?max=` clamp, _handle_activity_drain). */
export const RAFT_DRAIN_DEFAULT_MAX = 200;
/**
 * adapter.py:_wake_prompt — the fixed content-free prompt injected as a REAL
 * internal turn on every accepted wake hint.
 */
export const RAFT_WAKE_PROMPT =
	"Raft wake hint received. New Raft messages may be pending. " +
	"If you have not read the Raft manual in this session, run " +
	"`raft manual get raft-cli-overview` before using Raft commands.";

/** adapter.py:_CONTENT_FIELD_NAMES — recursive content-bearing field scan. */
export const RAFT_CONTENT_FIELD_NAMES: ReadonlySet<string> = new Set([
	"body",
	"content",
	"message",
	"messages",
	"preview",
	"snippet",
	"text",
]);

/** adapter.py:_ACTIVITY_ALLOWED_FIELDS — closed event vocabulary. */
export const RAFT_ACTIVITY_ALLOWED_FIELDS: ReadonlySet<string> = new Set([
	"schema",
	"eventId",
	"sessionId",
	"hookEventName",
	"status",
	"occurredAt",
	"toolName",
	"toolInput",
	"toolOutput",
	"toolInputTruncated",
	"toolOutputTruncated",
	"truncated",
	"errorClass",
	"durationMs",
]);

/**
 * Capabilities AS DATA (04 §2).
 *
 * DIVERGENCE NOTE (proposed DEC text — logged here per DEC-026 protocol, same
 * ruling as the msgraph-webhook port): Hermes' raft adapter overrides NEITHER
 * supports_async_delivery NOR interactive_resume, so it inherits the base
 * defaults (True/True) even though send() is a documented NO-OP ("agent
 * delivers via raft CLI") that can never push a later completion anywhere.
 * The 04 §8 webhook-shape row mandates the stateless pairing for exactly this
 * shape; the port declares BOTH FLAGS FALSE — the honest capability data.
 */
export const RAFT_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: false,
		interactiveResume: false,
	});

/**
 * THE trust boundary as DATA (DEC-017).
 *
 * signatureSchemes is EMPTY by design: the wake channel carries NO HMAC wire
 * scheme. Authenticity rides ONE declared mechanism: the auto-generated
 * bridge token compared CONSTANT-TIME over raw header bytes
 * (_validate_bridge_token: hmac.compare_digest(token.encode(), …) — byte
 * compare because compare_digest raises on non-ASCII str). Ingress is
 * loopback-by-construction (DEFAULT_HOST) behind the external bridge.
 */
export interface RaftTrustBoundary extends TrustBoundaryManifest {
	/** Constant-time bridge-token compare IS the signature mechanism. */
	bridgeTokenCompare: true;
}

export function declareRaftTrustBoundary(): RaftTrustBoundary {
	return {
		ingress: "http",
		signatureSchemes: [],
		constantTimeCompare: true,
		// Wakes are content-free hints keyed by delivery id upstream; the
		// adapter itself keeps NO seen-set — idempotency lives with the bridge.
		// The activity queue bound is the honest ingress-bounding datum and is
		// declared through it (validator requires a positive bound).
		idempotency: { seenSetMaxEntries: RAFT_ACTIVITY_QUEUE_CAP },
		scriptTransformsConfinedToHome: true,
		bodySizeCapBytes: RAFT_BODY_CAP_BYTES,
		cidrAllowlist: [],
		backpressureWindow: "bounded",
		bridgeTokenCompare: true,
	};
}

/**
 * Construction-time boundary validation (DEC-017 posture). Same invariants as
 * kit validateTrustBoundaryManifest, with scheme presence satisfied by the
 * documented bridge-token mechanism instead of a fake scheme id (msgraph
 * precedent).
 */
export function validateRaftTrustBoundary(m: RaftTrustBoundary): string[] {
	const errors: string[] = [];
	if (m.constantTimeCompare !== true) {
		errors.push("trust boundary must declare constantTimeCompare: true");
	}
	if (m.bridgeTokenCompare !== true) {
		errors.push(
			"raft ingress must declare bridgeTokenCompare: true (no HMAC scheme exists on this wire)",
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
			"raft binds loopback by construction — no CIDR allowlist applies",
		);
	}
	return errors;
}

// ── plugin manifest (04 §4.2 registration flow) ─────────────────────────────

export const RAFT_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "raft",
	description:
		"Raft workspace external-agent wake channel (loopback HTTP wake endpoint + activity telemetry)",
	transportShape: "webhook" as const,
	requiresEnv: [
		{
			name: "RAFT_PROFILE",
			description:
				"Raft agent profile slug — auto-enables the adapter when set (plugin.yaml required_env)",
		},
	],
	capabilities: RAFT_CAPABILITIES,
	trustBoundary: declareRaftTrustBoundary(),
});

/**
 * _spawn_bridge command shape as PURE DATA (the port never spawns OS
 * children): argv + the env var carrying the bridge token. Exported so rows
 * can pin the exact contract Hermes hands to subprocess.Popen without any
 * process machinery.
 */
export function buildBridgeSpawnCommand(opts: {
	profile: string;
	endpointUrl: string;
}): { argv: readonly string[]; tokenEnvVar: "RAFT_CHANNEL_TOKEN" } {
	return {
		argv: [
			"raft",
			"--profile",
			opts.profile,
			"agent",
			"bridge",
			"--wake-adapter",
			"wake-channel",
			"--wake-channel-endpoint",
			opts.endpointUrl,
		],
		tokenEnvVar: "RAFT_CHANNEL_TOKEN",
	};
}
