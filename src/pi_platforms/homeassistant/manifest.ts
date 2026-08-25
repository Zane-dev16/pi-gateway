// pi_platforms/homeassistant/manifest — Home Assistant platform MANIFEST
// DATA (04 §2/§4; Q17 discipline): every policy-shaped number and every
// vendor wire string is DATA transcribed from the READ-ONLY Hermes reference
// — per-platform numbers live in adapter manifests, never in core.
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   plugins/platforms/homeassistant/adapter.py unless noted:
//     MAX_MESSAGE_LENGTH = 4096 (@class attr)  → HA_MAX_MESSAGE_LENGTH
//     _BACKOFF_STEPS = [5, 10, 30, 60] (@class attr; _listen_loop indexes
//       with min(idx, len-1) clamp and RESETS idx=0 on successful reconnect)
//     _ws_connect: ws_connect(ws_url, heartbeat=30, timeout=30) — the ws
//       heartbeat parity is 30s (@~_ws_connect aiohttp call)
//     _msg_id counter starts 0, incremented per command (_next_id) ⇒ first
//       command carries id=1
//     extra.get("cooldown_seconds", 30) → HA_DEFAULT_COOLDOWN_SECONDS
//     HASS_URL default "http://homeassistant.local:8123" (__init__)
//     send(): POST {url}/api/services/persistent_notification/create with
//       Bearer token header; payload {"title": "Hermes Agent",
//       "message": content[:MAX_MESSAGE_LENGTH]}
//     _standalone_send: POST {url}/api/services/notify/notify with payload
//       {"message": message, "target": chat_id} (out-of-process cron sender)
//     _format_state_change: the entity-domain formatting table below,
//       transcribed EXACTLY including the "[Home Assistant]" prefix strings
//     connect(): warning when no watch_domains/watch_entities/watch_all is
//       configured ("All state_changed events will be dropped…")
//
// Trust/posture note (signal-precedent manifest comment; DEC-017): NO trust
// boundary is declared because this adapter has NO HTTP-ingress plane to
// protect. The only network surfaces are OUTBOUND connections to the
// operator's own Home Assistant instance: a persistent WebSocket to
// /api/websocket (inbound state_changed events PULLED by the client) and
// outbound REST POSTs for notifications (adapter.py has no HTTP server).

import type { CapabilityManifest } from "../kit/capabilities.js";
import type { PluginManifest } from "../kit/registration.js";

/** adapter.py:MAX_MESSAGE_LENGTH — notification body cap (send truncates). */
export const HA_MAX_MESSAGE_LENGTH = 4096;

/**
 * adapter.py:_BACKOFF_STEPS — reconnection ladder in SECONDS. _listen_loop
 * picks _BACKOFF_STEPS[min(backoff_idx, len-1)] (clamp at 60) and resets
 * backoff_idx=0 on a successful reconnect.
 */
export const HA_BACKOFF_STEPS_SECONDS: readonly number[] = Object.freeze([
	5, 10, 30, 60,
]);

/** Index-clamped ladder read (_listen_loop parity). */
export function haBackoffStepSeconds(index: number): number {
	const clamped = Math.min(
		Math.max(0, index),
		HA_BACKOFF_STEPS_SECONDS.length - 1,
	);
	return HA_BACKOFF_STEPS_SECONDS[clamped] ?? HA_BACKOFF_STEPS_SECONDS[0]!;
}

/** adapter.py:_ws_connect — aiohttp ws_connect(heartbeat=30) parity. */
export const HA_WS_HEARTBEAT_MS = 30_000;

/** adapter.py __init__ — extra.get("cooldown_seconds", 30). */
export const HA_DEFAULT_COOLDOWN_SECONDS = 30;

/** adapter.py __init__ — os.getenv("HASS_URL", …) default. */
export const HA_DEFAULT_URL = "http://homeassistant.local:8123";

/** adapter.py:send — the live-send REST path (persistent_notification). */
export const HA_REST_NOTIFICATION_CREATE =
	"/api/services/persistent_notification/create";

/**
 * adapter.py:_standalone_send — the out-of-process cron sender path
 * (deliver=homeassistant); payload {message, target: chat_id}.
 */
export const HA_REST_NOTIFY_NOTIFY = "/api/services/notify/notify";

/**
 * adapter.py:send payload title — VERBATIM VENDOR WIRE DATA ("Hermes Agent"
 * is what the reference ships on the wire inside every persistent
 * notification). Transcribed without silent rename per the census rule;
 * renaming it is an upstream-facing change that needs its own decision.
 * PROPOSED DEC TEXT: "Pi Gateway renames the HA persistent-notification
 * title from 'Hermes Agent' to 'Pi Agent' in one wave-2 follow-up; the
 * string stays byte-exact until that DEC lands."
 */
export const HA_NOTIFICATION_TITLE = "Hermes Agent";

/** The event channel every formatted state change dispatches onto. */
export const HA_EVENTS_CHAT_ID = "ha_events";
/** The fixed user on dispatched state_changed events. */
export const HA_EVENTS_USER_ID = "homeassistant";

/**
 * adapter.py class attr SUPPORTS_MESSAGE_EDITING-equivalent: the source has
 * NO message-edit API at all (persistent notifications cannot be edited),
 * so THE datum feeding the streaming-exclusion probe is false — same shape
 * as signal/manifest.ts. Flipping this datum flips the probe (lie-scan).
 */
export const HA_SUPPORTS_MESSAGE_EDITING = false;

/**
 * Capabilities AS DATA (04 §2): base defaults — persistent push socket ⇒
 * supportsAsyncDelivery=true (DEC-022 forged-event wake lane) and
 * interactiveResume=true. NO native draft streaming exists anywhere in the
 * source ⇒ streaming support arrives only through the probe-fed datum above.
 */
export const HA_CAPABILITIES: Readonly<Partial<CapabilityManifest>> =
	Object.freeze({
		supportsAsyncDelivery: true,
		interactiveResume: true,
	});

// ── plugin manifest (kit registration shape; plugin.yaml transcription) ─────

/**
 * plugins/platforms/homeassistant/plugin.yaml — requires_env HASS_TOKEN
 * (Long-Lived Access Token), optional_env HASS_URL. Shape: ws (HA WebSocket
 * API inbound + REST outbound; DEC-002 persistent-push family).
 */
export const HA_PLUGIN_MANIFEST: PluginManifest = Object.freeze({
	name: "homeassistant",
	description:
		"Home Assistant via its WebSocket API (state_changed events in, persistent notifications out)",
	transportShape: "ws" as const,
	requiresEnv: [
		{
			name: "HASS_TOKEN",
			description: "Home Assistant Long-Lived Access Token",
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "HASS_URL",
			description:
				"Home Assistant base URL (default: http://homeassistant.local:8123)",
			url: true,
		},
	],
	capabilities: HA_CAPABILITIES,
});

// ── watch-filter config (adapter.py __init__ extra keys, verbatim names) ────

export interface HaWatchConfig {
	url?: string | undefined;
	watch_domains?: readonly string[] | undefined;
	watch_entities?: readonly string[] | undefined;
	ignore_entities?: readonly string[] | undefined;
	watch_all?: boolean | undefined;
	cooldown_seconds?: number | undefined;
}

// ── entity-state formatting table (adapter.py:_format_state_change) ─────────

export interface HaEntityState {
	state?: unknown;
	attributes?: Record<string, unknown> | null | undefined;
}

function attrString(state: HaEntityState | null, key: string): string {
	const raw = state?.attributes?.[key];
	return raw === undefined || raw === null ? "" : String(raw);
}

function stateValue(state: HaEntityState | null | undefined): string {
	if (!state || typeof state !== "object") return "unknown";
	const raw = state.state;
	if (raw === undefined || raw === null) return "unknown";
	return String(raw);
}

/**
 * Convert a state_changed event into a human-readable description
 * (adapter.py:_format_state_change parity, ORDER MATTERS):
 *   - empty new_state ⇒ None (caller drops, counts no-change skip)
 *   - old_val missing ⇒ "unknown"; old == new ⇒ None (no actual change)
 *   - friendly_name attribute falls back to entity_id
 *   - domain table: climate / sensor / binary_sensor / light|switch|fan /
 *     alarm_control_panel / generic fallback — strings VERBATIM.
 */
export function formatStateChange(
	entityId: string,
	oldState: HaEntityState | null,
	newState: HaEntityState | null,
): string | null {
	if (!newState || typeof newState !== "object") return null;
	const oldVal = stateValue(oldState ?? null);
	const newVal = stateValue(newState);
	if (oldVal === newVal) return null;
	const friendlyName = attrString(newState, "friendly_name") || entityId;
	const domain = entityId.includes(".") ? (entityId.split(".")[0] ?? "") : "";

	if (domain === "climate") {
		const tempRaw = newState.attributes?.["current_temperature"];
		const targetRaw = newState.attributes?.["temperature"];
		const temp =
			tempRaw === undefined || tempRaw === null ? "?" : String(tempRaw);
		const target =
			targetRaw === undefined || targetRaw === null ? "?" : String(targetRaw);
		return (
			`[Home Assistant] ${friendlyName}: HVAC mode changed from ` +
			`'${oldVal}' to '${newVal}' (current: ${temp}, target: ${target})`
		);
	}

	if (domain === "sensor") {
		const unit = attrString(newState, "unit_of_measurement");
		return (
			`[Home Assistant] ${friendlyName}: changed from ` +
			`${oldVal}${unit} to ${newVal}${unit}`
		);
	}

	if (domain === "binary_sensor") {
		return (
			`[Home Assistant] ${friendlyName}: ` +
			`${newVal === "on" ? "triggered" : "cleared"} ` +
			`(was ${oldVal === "on" ? "triggered" : "cleared"})`
		);
	}

	if (domain === "light" || domain === "switch" || domain === "fan") {
		return (
			`[Home Assistant] ${friendlyName}: turned ` +
			`${newVal === "on" ? "on" : "off"}`
		);
	}

	if (domain === "alarm_control_panel") {
		return (
			`[Home Assistant] ${friendlyName}: alarm state changed from ` +
			`'${oldVal}' to '${newVal}'`
		);
	}

	// Generic fallback
	return (
		`[Home Assistant] ${friendlyName} (${entityId}): ` +
		`changed from '${oldVal}' to '${newVal}'`
	);
}
