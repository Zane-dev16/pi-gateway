// pi_platforms/mattermost/manifest — MATTERMOST MANIFEST DATA (Phase-6 census
// port). Every constant TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/mattermost/) and cited by file:symbol (Q17/DEC-017).

import type { PluginManifest } from "../kit/index.js";

/**
 * adapter.py:MAX_POST_LENGTH = 4000 — "server default is 16383, but 4000 is
 * the practical limit for readable messages". CHARACTERS (Python len() →
 * code points, kit "chars" unit).
 */
export const MM_MAX_POST_CHARS = 4000;

/** adapter.py:_CHANNEL_TYPE_MAP — channel type codes → chat types. */
export const MM_CHANNEL_TYPE_MAP: Readonly<Record<string, string>> = {
	D: "dm",
	G: "group",
	P: "group", // private channel → treat as group
	O: "channel",
};

export function mmChatTypeForChannelType(code: string): string {
	return MM_CHANNEL_TYPE_MAP[code] ?? "channel";
}

// ── reconnect ladder (adapter.py module constants) ──────────────────────────

/** adapter.py:_RECONNECT_BASE_DELAY / _MAX_DELAY / _JITTER. */
export const MM_RECONNECT_BASE_DELAY_S = 2.0;
export const MM_RECONNECT_MAX_DELAY_S = 60.0;
export const MM_RECONNECT_JITTER_FRACTION = 0.2;

// ── websocket session (adapter.py:_ws_connect_and_listen) ───────────────────

/**
 * adapter.py:_ws_connect_and_listen — `ws_connect(..., heartbeat=30.0)`:
 * protocol ping cadence 30 s; staleness factor + first-ping grace port the
 * persistent-ws family watchdog shape (A23 parity).
 */
export const MM_WS_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * adapter.py:_dedup = MessageDeduplicator() — gateway/platforms/helpers.py
 * defaults max_size=2000, ttl_seconds=300 (#4777 redelivery shield).
 */
export const MM_DEDUP_TTL_MS = 300_000;
export const MM_DEDUP_MAX_ENTRIES = 2000;

/** adapter.py:_MATTERMOST_DISABLE_MENTIONS_PROPS — every outbound post payload. */
export function withMentionsDisabled(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	const props = payload["props"];
	if (props !== null && typeof props === "object") {
		return {
			...payload,
			props: { ...(props as object), disable_mentions: true },
		};
	}
	return { ...payload, props: { disable_mentions: true } };
}

/** adapter.py thread-fallback notice prefix (_post_preserving_thread). */
export const MM_THREAD_FALLBACK_NOTICE =
	"⚠️ Mattermost thread delivery failed; posting final reply in channel.\n\n";

// ── the PluginManifest ──────────────────────────────────────────────────────

/**
 * Capability mapping note (telegram-manifest parity): Hermes'
 * splits_long_messages=True means send() chunks via truncate_message(4000).
 * THIS kit's base skips its own split when the flag is true, so the adapter's
 * deliverText runs THE kit chunker with the scalar 4000-code-point policy.
 *
 * Native streaming note: Hermes mattermost does NOT override send_draft —
 * streaming rides base.py:supports_draft_streaming's edit-based fallback
 * (send + progressive edits). The port realizes that plane INSIDE the adapter
 * (POST start / PATCH cumulative / final PATCH) so seal-interception rows run
 * against real wire ops; declared streaming matches seal reality.
 */
export const MATTERMOST_MANIFEST: PluginManifest = {
	name: "mattermost",
	description:
		"Mattermost adapter on the persistent-ws transport family (v4 REST + WebSocket events)",
	transportShape: "ws",
	requiresEnv: [
		{
			name: "MATTERMOST_URL",
			description: "Mattermost server URL (e.g. https://mm.example.com)",
			password: false,
		},
		{
			name: "MATTERMOST_TOKEN",
			description: "Bot account token or personal-access token",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "MATTERMOST_REPLY_MODE",
			description:
				"How replies are sent: 'thread' (nested) or 'off' (flat). Default: off.",
		},
		{
			name: "MATTERMOST_REQUIRE_MENTION",
			description:
				"Require @bot mention in channels (default true). Set false for free-response everywhere.",
		},
		{
			name: "MATTERMOST_FREE_RESPONSE_CHANNELS",
			description:
				"Comma-separated channel IDs where @mention is not required.",
		},
		{
			name: "MATTERMOST_ALLOWED_CHANNELS",
			description:
				"If set, the bot only responds in these channels (whitelist).",
		},
		{
			name: "MATTERMOST_HOME_CHANNEL",
			description: "Default channel ID for cron / notification delivery",
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		splitsLongMessages: true,
		typedCommandPrefix: "/",
		interactiveResume: true,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	// Q17 review note: no client-side rate tiers exist in the Hermes plugin —
	// server-side 429 Retry-After is CAPTURED at both egress doors and feeds
	// the send-retry AND reconnect ladders instead of a static budget.
};
