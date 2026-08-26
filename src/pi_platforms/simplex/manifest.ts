// pi_platforms/simplex/manifest — SimpleX platform MANIFEST DATA (04 §2/§4;
// Q17 discipline): every policy-shaped number is DATA transcribed from the
// READ-ONLY Hermes reference — per-platform numbers live in adapter
// manifests, never in core.
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   plugins/platforms/simplex/adapter.py unless noted:
//     MAX_MESSAGE_LENGTH = 8000 (@75 "SimpleX has no hard limit; chunk for
//       sanity") — consumed by base.max_message_length_for_chat as THE
//       adapter scalar, measured in CODEPOINTS (Python len parity)
//     WS_RETRY_DELAY_INITIAL = 2.0 / WS_RETRY_DELAY_MAX = 60.0 (@76-77;
//       _ws_listener: reset to initial on successful connect, double up to
//       max, +20% jitter against thundering herd)
//     HEALTH_CHECK_INTERVAL = 30.0 / HEALTH_CHECK_STALE_THRESHOLD = 300.0
//       (@78-79; _health_monitor is deliberately LOG-ONLY: liveness is owned
//       by the KEEPALIVE — in Hermes the websockets client's protocol pings,
//       in this port the adapter-side ping/pong loop below — so application
//       silence NEVER triggers a reconnect of a healthy quiet link)
//     connect(open_timeout=10) (@~296) — reachability-probe handshake bound;
//       expiry resolves FALSE instead of hanging startup/reconnect
//     list_channels /contacts + /groups (timeout=10.0 each) (@~880-935)
//     _ws_listener connect(ping_interval=20, ping_timeout=20) (@~310-315) —
//       the keepalive carrier: pi has NO websockets library, so the port
//       expresses the SAME client keepalive itself (ping every 20s; a ping
//       left unanswered past 20s aborts the link 1011 → SAME ladder)
//     _CORR_PREFIX = "hermes-" (@82) — VENDOR WIRE DATA transcribed verbatim:
//       correlation ids we mint carry this prefix and the inbound echo filter
//       discards any corrId starting with it (proposed DEC text in the port
//       report covers keeping the vendor bytes)
//     _max_pending_corr = 200 (@179; _make_corr_id single-sweep oldest
//       eviction past the bound)
//     _send_command timeout 30.0s (@759 default arg)
//     _text_batch_delay = float(HERMES_SIMPLEX_TEXT_BATCH_DELAY, "0.8")
//       (@194) — quiet-period seconds for rapid-fire text concatenation
//     send() _voice_exts = {".ogg",".mp3",".wav",".m4a",".opus"} (@826);
//       MEDIA:(\S+) tag grammar (@827); composed JSON "[{"msgContent":...}]"
//       "/_send #<id>|@<id> json" command shapes (@~833-845)
//     _is_image_ext / _is_audio_ext (@125-131)
//
//   plugins/platforms/simplex/plugin.yaml:
//     requires_env SIMPLEX_WS_URL; optional_env SIMPLEX_ALLOWED_USERS /
//     SIMPLEX_ALLOW_ALL_USERS / SIMPLEX_AUTO_ACCEPT (default true) /
//     SIMPLEX_GROUP_ALLOWED / SIMPLEX_HOME_CHANNEL(+_NAME) /
//     HERMES_SIMPLEX_TEXT_BATCH_DELAY

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";

/**
 * adapter.py:@75 — THE per-chat length scalar (base.py
 * max_message_length_for_chat reads MAX_MESSAGE_LENGTH). Measured in
 * CODEPOINTS (Python len parity); the kit length policy carries the unit.
 */
export const SIMPLEX_MAX_MESSAGE_LENGTH = 8000;

/** adapter.py:@76-77 — WS reconnect ladder bounds (ms here). */
export const WS_RETRY_DELAY_INITIAL_MS = 2_000;
export const WS_RETRY_DELAY_MAX_MS = 60_000;
/** adapter.py:_ws_listener — jitter fraction vs computed delay. */
export const WS_JITTER_FRACTION = 0.2;

/**
 * adapter.py:@78-79 — health monitor cadence + idle bar. The monitor is
 * DELIBERATELY LOG-ONLY (_health_monitor docstring @~349-356: liveness is
 * carried by protocol ping/pong — websockets' keepalive in Hermes, the
 * adapter-side SIMPLEX_WS_PING_* loop in this port — so treating chat-event
 * silence as staleness causes needless reconnect churn). Rows pin that
 * posture: a stale idle never tears a live link, while a stalled PING does.
 */
export const HEALTH_CHECK_INTERVAL_MS = 30_000;
export const HEALTH_CHECK_STALE_THRESHOLD_MS = 300_000;

/**
 * adapter.py:@82 — VENDOR WIRE DATA, verbatim. Correlation ids minted by the
 * adapter are `hermes-<counter>-<epoch-ms>`; the inbound dispatcher discards
 * ANY event whose corrId starts with this prefix WITHOUT chat-item processing
 * (adapter.py:_handle_event echo filter @386-390). Renaming it would break
 * against real daemons only if they ever special-case the prefix — they do
 * not; the prefix is purely OUR side's self-echo marker, but the port keeps
 * the vendor bytes anyway (proposed DEC).
 */
export const SIMPLEX_CORR_PREFIX = "hermes-";

/** adapter.py:@179 — pending-corr bound; oldest entries evict in one sweep. */
export const SIMPLEX_MAX_PENDING_CORR = 200;

/** adapter.py:@759 — correlated-command response timeout (ms here). */
export const SIMPLEX_COMMAND_TIMEOUT_MS = 30_000;

/** adapter.py:@194 — quiet-period batch delay (default 0.8 s). */
export const SIMPLEX_TEXT_BATCH_DELAY_DEFAULT_S = 0.8;
/** plugin.yaml optional_env — env override for the batch quiet period. */
export const SIMPLEX_TEXT_BATCH_DELAY_ENV = "HERMES_SIMPLEX_TEXT_BATCH_DELAY";

/** plugin.yaml optional_env — constructor reads these via the scoped reader. */
export const SIMPLEX_AUTO_ACCEPT_ENV = "SIMPLEX_AUTO_ACCEPT";
export const SIMPLEX_GROUP_ALLOWED_ENV = "SIMPLEX_GROUP_ALLOWED";

/**
 * adapter.py:__init__ — env-parse SIMPLEX_AUTO_ACCEPT: any '0'/'false'/'no'
 * (case-insensitive) AND the empty string disable; ANY other value enables.
 * Undefined (unset) yields undefined so the caller falls back to the injected
 * option/default — set-but-empty DISABLES (Python getenv('') is not None).
 */
export function simplexAutoAcceptFromEnv(
	raw: string | undefined,
): boolean | undefined {
	if (raw === undefined) return undefined;
	return !["0", "false", "no", ""].includes(raw.trim().toLowerCase());
}

/** adapter.py:_parse_comma_list — split, trim entries, drop empties. */
export function parseCommaList(raw: string): string[] {
	return raw
		.split(",")
		.map((v) => v.trim())
		.filter((v) => v !== "");
}

/** Resolve the batch quiet period in ms (env override parity, scoped read). */
export function simplexTextBatchDelayMs(env?: string | undefined): number {
	const raw = env?.trim();
	if (raw !== undefined && raw !== "") {
		const parsed = Number(raw);
		if (Number.isFinite(parsed) && parsed >= 0) return parsed * 1000;
	}
	return SIMPLEX_TEXT_BATCH_DELAY_DEFAULT_S * 1000;
}

/** adapter.py:connect — reachability-probe handshake bound (open_timeout=10). */
export const SIMPLEX_CONNECT_OPEN_TIMEOUT_MS = 10_000;

/**
 * adapter.py:list_channels — correlated-reply bound for BOTH directory
 * commands (/contacts and /groups, timeout=10.0 each).
 */
export const SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS = 10_000;

/**
 * adapter.py:_ws_listener — connect(ping_interval=20, ping_timeout=20) client
 * keepalive. The health monitor is LOG-ONLY BECAUSE this carrier exists:
 * application silence never reconnects a healthy link, but a ping left
 * unanswered past the timeout marks the link DEAD (websockets aborts 1011)
 * and feeds the SAME reconnect ladder.
 */
export const SIMPLEX_WS_PING_INTERVAL_MS = 20_000;
export const SIMPLEX_WS_PING_TIMEOUT_MS = 20_000;
/** websockets keepalive-expiry abort code (link declared dead). */
export const SIMPLEX_WS_PING_CLOSE_CODE = 1011;

/**
 * adapter.py:send @826 — extensions routed to the inline voice-note player
 * (msgContent.type "voice"); every other media path ships as a document.
 */
export const SIMPLEX_VOICE_EXTS: ReadonlySet<string> = new Set([
	".ogg",
	".mp3",
	".wav",
	".m4a",
	".opus",
]);

/** adapter.py:@125-127 — image extension classifier (lowercased ext). */
export function isImageExt(ext: string): boolean {
	return [".jpg", ".jpeg", ".png", ".gif", ".webp"].includes(ext.toLowerCase());
}

/** adapter.py:@129-131 — audio extension classifier (lowercased ext). */
export function isAudioExt(ext: string): boolean {
	return [".mp3", ".wav", ".ogg", ".m4a", ".aac", ".opus"].includes(
		ext.toLowerCase(),
	);
}

/** adapter.py:send @827 — MEDIA:<path> attachment-tag extraction grammar. */
export const MEDIA_TAG_RE = /MEDIA:(\S+)/g;

// ── capability data (04 §2) ─────────────────────────────────────────────────

/**
 * SimpleX exposes NO message-edit API anywhere in the source adapter (no edit
 * method, no edit wire shape) — the honest datum is FALSE. THE input of the
 * streaming-exclusion probe: with no edit API there is no way to seal or
 * reconcile a draft cursor, so native draft streaming is excluded BY THE
 * PROBE from this constant — flipping the data flips the probe (and the
 * lie-scan mutant that flips it fails the streaming family rows), exactly the
 * signal/manifest.ts discipline.
 */
export const SIMPLEX_SUPPORTS_MESSAGE_EDITING = false;

/**
 * Capabilities AS DATA — base defaults for a persistent daemon stream:
 * supportsAsyncDelivery stays TRUE (persistent daemon stream ⇒ forged-event
 * wake lane, DEC-022) and interactiveResume stays TRUE; splitsLongMessages
 * unset (base default False — long content still splits against the 8000-
 * codepoint scalar via the kit length pair).
 */
export const SIMPLEX_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
		interactiveResume: true,
	});

// ── plugin manifest (kit registration shape) ────────────────────────────────

/**
 * Transport shape (DEC-002 ws family): ONE persistent WebSocket to the local
 * simplex-chat daemon carries BOTH directions — inbound events are pushed and
 * outbound JSON commands ride the SAME socket, correlated by corrId.
 *
 * DEC-017 note: NO trust boundary is declared because the adapter has no
 * HTTP-ingress plane to protect — the only network surface is an OUTBOUND
 * connection to the operator's own daemon (`simplex-chat -p 5225`; adapter.py
 * module docstring @~1-13). Same posture as the Signal precedent.
 */
export const SIMPLEX_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "simplex",
	description:
		"SimpleX Chat via a local simplex-chat daemon (persistent WebSocket; JSON commands correlated by corrId)",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "SIMPLEX_WS_URL",
			description:
				"WebSocket URL of the simplex-chat daemon (e.g. ws://127.0.0.1:5225)",
			url: true,
		},
	],
	optionalEnv: [
		{
			name: "SIMPLEX_ALLOWED_USERS",
			description:
				"Comma-separated SimpleX contact IDs or display names allowed to talk to the bot",
		},
		{
			name: "SIMPLEX_ALLOW_ALL_USERS",
			description: "Allow any contact (dev only — disables the allowlist)",
		},
		{
			name: "SIMPLEX_AUTO_ACCEPT",
			description:
				"Auto-accept incoming contact requests (default true; 'false'/'0'/'no' disables)",
		},
		{
			name: "SIMPLEX_GROUP_ALLOWED",
			description:
				"Comma-separated group IDs to monitor, or '*' for any; omit to disable groups entirely (safer default)",
		},
		{
			name: "SIMPLEX_HOME_CHANNEL",
			description: "Default contact/group ID for cron delivery",
		},
		{
			name: SIMPLEX_TEXT_BATCH_DELAY_ENV,
			description:
				"Quiet-period seconds (default 0.8) used to concatenate rapid-fire inbound texts into one event",
		},
	],
	capabilities: SIMPLEX_CAPABILITIES,
});
