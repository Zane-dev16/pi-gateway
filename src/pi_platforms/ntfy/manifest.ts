// pi_platforms/ntfy/manifest — NTFY MANIFEST DATA (Phase-6 census port).
// Every constant TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/ntfy/adapter.py) and cited by file:symbol (Q17/DEC-017).

import type { PluginManifest } from "../kit/index.js";

/** adapter.py:DEFAULT_SERVER. */
export const NTFY_DEFAULT_SERVER = "https://ntfy.sh";

/** adapter.py:MAX_MESSAGE_LENGTH = 4096 — ntfy message body limit (chars). */
export const NTFY_MAX_MESSAGE_CHARS = 4096;

/** adapter.py:DEDUP_WINDOW_SECONDS / DEDUP_MAX_SIZE — redelivery shield. */
export const NTFY_DEDUP_WINDOW_MS = 300_000;
export const NTFY_DEDUP_MAX_SIZE = 1000;

/**
 * adapter.py:RECONNECT_BACKOFF = [2, 5, 10, 30, 60] seconds — THE data-driven
 * fixed ladder; index resets when the stream stayed alive ≥ 60s.
 */
export const NTFY_RECONNECT_BACKOFF_S = [2, 5, 10, 30, 60] as const;

/** Reset threshold for the ladder index (adapter.py:_run_stream). */
export const NTFY_LADDER_RESET_ALIVE_MS = 60_000;

/**
 * adapter.py:STREAM_TIMEOUT_SECONDS = 90 — "ntfy keepalive default is 55s;
 * give margin". The read-timeout IS the watchdog.
 */
export const NTFY_STREAM_TIMEOUT_MS = 90_000;
export const NTFY_KEEPALIVE_DEFAULT_MS = 55_000;

/** Publish POST timeout (adapter.py:send) and connect budget. */
export const NTFY_PUBLISH_TIMEOUT_MS = 15_000;

/** Outbound echo-loop tag (adapter.py:_ECHO_TAG). */
export const NTFY_ECHO_TAG = "hermes-agent";

// ── the PluginManifest ──────────────────────────────────────────────────────

/**
 * Identity note (trust boundary): ntfy has NO native authenticated user
 * identity. The `title` field is publisher-controlled and is NOT used for
 * authorization; each topic is ONE trusted channel and user_id is FIXED to
 * the topic name. NTFY_ALLOWED_USERS only bounds topics when a read token
 * protects them.
 *
 * Transport note: HTTP streaming (/json?poll=false) with keepalive events;
 * publish via HTTP POST with X-Tags echo marker and optional X-Markdown.
 * NO draft streaming, NO edits, NO interactive callbacks on the real surface.
 */
export const NTFY_PLUGIN_MANIFEST: PluginManifest = {
	name: "ntfy",
	description:
		"ntfy push adapter on the persistent HTTP-stream transport family (/json subscribe, POST publish)",
	transportShape: "ws",
	requiresEnv: [
		{
			name: "NTFY_TOPIC",
			description: "Topic to subscribe to (required)",
			password: false,
		},
	],
	optionalEnv: [
		{
			name: "NTFY_SERVER_URL",
			description: "Server URL (default https://ntfy.sh)",
			password: false,
		},
		{
			name: "NTFY_TOKEN",
			description: "Bearer token or user:pass for Basic auth",
			password: true,
		},
		{
			name: "NTFY_PUBLISH_TOPIC",
			description: "Reply topic (defaults to NTFY_TOPIC)",
			password: false,
		},
		{
			name: "NTFY_MARKDOWN",
			description: 'Enable X-Markdown header ("true"/"1"/"yes")',
			password: false,
		},
		{
			name: "NTFY_ALLOWED_USERS",
			description: "Topic allowlist (topics ARE the identities)",
			password: false,
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		splitsLongMessages: false,
		typedCommandPrefix: "/",
		interactiveResume: false,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	// Q17 review note: the ONLY rate control in the Hermes plugin is the fixed
	// RECONNECT_BACKOFF ladder (manifest array above); publishes carry no
	// client-side budget — server 429s surface as plain send failures.
};
