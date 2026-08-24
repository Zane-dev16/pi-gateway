// pi_platforms/slack/manifest — the Slack platform manifest AS DATA
// (04-platform-adapters.md §4.2 plugin path + Q17 rate-tier decision).
//
// "Per-platform numbers live in adapter manifests, not core" (Q17): every
// vendor ground-truth number below is TRANSCRIBED from the READ-ONLY Hermes
// reference and cited to its file:symbol anchor:
//
//   plugins/platforms/slack/plugin.yaml          → requires_env / optional_env
//     (SLACK_BOT_TOKEN xoxb-…, SLACK_APP_TOKEN xapp-… connections:write;
//      optional allowlist/home-channel env specs)
//   plugins/platforms/slack/adapter.py:SlackAdapter.MAX_MESSAGE_LENGTH
//     = 39000  ("Slack API allows 40,000 chars; leave margin")
//   plugins/platforms/slack/adapter.py:SlackAdapter.splits_long_messages
//     = True   (send() chunks natively via truncate_message)
//   plugins/platforms/slack/adapter.py:SlackAdapter.typed_command_prefix
//     = "!"    ("/" is reserved in Slack threads; §2 capability table)
//
// Rate tiers (Q17/DEC-017 posture): tier CLASSES are manifest data consumed
// by the egress gate before any transmission.
//   - Tier-2 class (~20/min per method): chat.postMessage / chat.update /
//     chat.startStream·appendStream·stopStream. Provenance: Q17's own example
//     ("streaming start/stop are Tier-2 ~20/min class limits"). Per the 04 §3
//     C6 correction this figure is EXTERNAL OBSERVATION, never a source
//     constant in Hermes — it is carried here as DECLARED MANIFEST DATA, the
//     only sanctioned home for such a number.
//   - The one VERIFIED in-source rate figure, conversations.replies
//     "(Tier 3, ~50 req/min)" (adapter.py:_fetch_thread_context), governs an
//     INGRESS read path with its own 1s·2^attempt retry ladder — outside this
//     egress gate's op vocabulary (RateOp has no read ops) and therefore not
//     fabricated into a tier here.

import type { PluginManifest } from "../kit/registration.js";
import type { CapabilityManifest } from "../kit/capabilities.js";

/** chat.postMessage-shaped REST sends (Tier-2 per-method class, Q17). */
export const SLACK_TIER2_MESSAGING = "tier2-messaging" as const;
/** chat.update + native *Stream ops (Tier-2 per-method class, Q17). */
export const SLACK_TIER2_STREAMING = "tier2-streaming" as const;

/** Verified Slack capability flags (04 §2 table, slack column). */
export const SLACK_CAPABILITIES: Readonly<CapabilityManifest> = Object.freeze({
	supportsAsyncDelivery: true,
	splitsLongMessages: true,
	typedCommandPrefix: "!",
	interactiveResume: true,
	supportsInchannelContinuable: false,
	requiresEditFinalize: false,
});

/**
 * THE Slack plugin manifest (plugin.yaml + Q17 transcription). Required
 * secrets disable the adapter LOUDLY via resolveEnablement when missing.
 */
export const SLACK_MANIFEST: Readonly<PluginManifest> = Object.freeze({
	name: "slack",
	description:
		"Slack gateway adapter — Socket Mode transport, mrkdwn rendering, " +
		"Block Kit interactive cards, thread-keyed sessions.",
	transportShape: "ws" as const,
	// Kit EnvVarSpec adaptation (noted in report): Hermes' plugin.yaml carries
	// the setup-wizard PROMPT TEXT in `prompt`; the kit types `prompt` as the
	// boolean wizard flag, so the text folds into `description` here.
	requiresEnv: [
		{
			name: "SLACK_BOT_TOKEN",
			description: "Slack bot token (xoxb-...) — prompt: Slack Bot Token",
			url: true,
			password: true,
		},
		{
			name: "SLACK_APP_TOKEN",
			description:
				"Slack app-level token for Socket Mode (xapp-..., scope " +
				"connections:write) — prompt: Slack App Token",
			url: true,
			password: true,
		},
	],
	optionalEnv: [
		{
			name: "SLACK_ALLOWED_USERS",
			description:
				"Comma-separated Slack member IDs allowed to talk to the bot",
		},
		{
			name: "SLACK_ALLOW_ALL_USERS",
			description: "Allow any Slack user to trigger the bot (dev only)",
		},
		{
			name: "SLACK_HOME_CHANNEL",
			description: "Default channel ID for cron / notification delivery",
		},
	],
	capabilities: { ...SLACK_CAPABILITIES },
	rateBudget: {
		tiers: [
			{
				name: SLACK_TIER2_MESSAGING,
				ops: ["send"] as const,
				limit: 20,
				windowSeconds: 60,
			},
			{
				name: SLACK_TIER2_STREAMING,
				ops: ["edit", "draft-start", "draft-stop"] as const,
				limit: 20,
				windowSeconds: 60,
			},
		],
	},
});

/** Slack API allows 40,000 chars; Hermes leaves margin (adapter.py:910). */
export const SLACK_MAX_MESSAGE_UNITS = 39_000;

/** Bounded resolved-map cap (adapter.py:_APPROVAL_RESOLVED_MAX parity). */
export const SLACK_RESOLVED_MAP_MAX = 1000;

/** Default Socket-Mode redelivery dedup window (#4777; event-cursor.ts port).
 * adapter.py:_slack_dedup_ttl_seconds — 3600s covers worst-case reconnect
 * redelivery gaps; memory bounded by LRU pruning, not the TTL. */
export const SLACK_DEDUP_TTL_MS = 3_600_000;
