// authz/platform-tables — per-platform env-var maps and policy vocabulary for
// the §2 decision chain, declared AS DATA (same discipline DEC-017 mandates
// for trust boundaries: feature flags alone are not a specification).
//
// Ported verbatim from gateway/authz_mixin.py::_is_user_authorized's local
// maps + gateway/pairing.py:_PLATFORM_ALLOWLIST_ENV (READ-ONLY Hermes
// reference; semantics ported, no code vendored):
//   platform_env_map            → PLATFORM_ALLOWED_USERS_ENV
//   platform_group_user_env_map → PLATFORM_GROUP_USER_ENV
//   platform_group_chat_env_map → PLATFORM_GROUP_CHAT_ENV
//   platform_allow_all_map      → PLATFORM_ALLOW_ALL_ENV
//   platform_allow_bots_map     → PLATFORM_ALLOW_BOTS_ENV (06 §2.5)
//   _PLATFORM_ALLOWLIST_ENV     → PAIRING_ALLOWLIST_ENV (pairing grant mirror)
//
// Platform names are the lowercase adapter names (Hermes Platform enum values).
// Platforms absent from these maps may supply their env-var names through the
// plugin registry seam (`AuthzDeps.registryEntry`) — parity of the
// gateway.platform_registry lookup inside _is_user_authorized.

/** dm_policy / group_policy vocabulary (06 §2.2). */
export type DmPolicy = "open" | "allowlist" | "disabled" | "pairing";
export type GroupPolicy = "open" | "allowlist" | "disabled";

/** Chat shapes the early chat-ID allowlist gate (§2.1 step 2) applies to. */
export const CHAT_TYPES_WITH_CHANNEL = new Set(["group", "forum", "channel"]);
/** Chat shapes the scoped group-user/group-chat env reads (step 8/9) apply to. */
export const CHAT_TYPES_GROUP_FORUM = new Set(["group", "forum"]);

/**
 * Chat platforms with token/HMAC-authenticated machine ingress (gate 0).
 * homeassistant left under DEC-070 with its adapter; the gate-0 mechanism
 * itself stays (webhook keeps exercising it).
 */
export const SYSTEM_PLATFORMS: ReadonlySet<string> = new Set([
	"webhook",
]);

/** {P}_ALLOWED_USERS — the platform-wide sender allowlist env var. */
export const PLATFORM_ALLOWED_USERS_ENV: Readonly<Record<string, string>> = {
	telegram: "TELEGRAM_ALLOWED_USERS",
	discord: "DISCORD_ALLOWED_USERS",
	whatsapp: "WHATSAPP_ALLOWED_USERS",
	whatsapp_cloud: "WHATSAPP_CLOUD_ALLOWED_USERS",
	slack: "SLACK_ALLOWED_USERS",
	signal: "SIGNAL_ALLOWED_USERS",
	email: "EMAIL_ALLOWED_USERS",
	sms: "SMS_ALLOWED_USERS",
	mattermost: "MATTERMOST_ALLOWED_USERS",
	matrix: "MATRIX_ALLOWED_USERS",
	dingtalk: "DINGTALK_ALLOWED_USERS",
	feishu: "FEISHU_ALLOWED_USERS",
	wecom: "WECOM_ALLOWED_USERS",
	wecom_callback: "WECOM_CALLBACK_ALLOWED_USERS",
	weixin: "WEIXIN_ALLOWED_USERS",
	qqbot: "QQ_ALLOWED_USERS",
	yuanbao: "YUANBAO_ALLOWED_USERS",
};

/** Scoped group SENDER allowlists ({P}_GROUP_ALLOWED_USERS). */
export const PLATFORM_GROUP_USER_ENV: Readonly<Record<string, string>> = {
	telegram: "TELEGRAM_GROUP_ALLOWED_USERS",
};

/**
 * Chat-scoped allowlists ({P}_GROUP_ALLOWED_CHATS shape) — consulted at §2.1
 * step 2 BEFORE the no-user-id guard so anonymous-admin/channel traffic can be
 * authorized without a user id, and again in the configured-allowlist branch.
 */
export const PLATFORM_GROUP_CHAT_ENV: Readonly<Record<string, string>> = {
	telegram: "TELEGRAM_GROUP_ALLOWED_CHATS",
	qqbot: "QQ_GROUP_ALLOWED_USERS",
};

/** {P}_ALLOW_ALL_USERS explicit operator opt-out (step 5). */
export const PLATFORM_ALLOW_ALL_ENV: Readonly<Record<string, string>> = {
	telegram: "TELEGRAM_ALLOW_ALL_USERS",
	discord: "DISCORD_ALLOW_ALL_USERS",
	whatsapp: "WHATSAPP_ALLOW_ALL_USERS",
	whatsapp_cloud: "WHATSAPP_CLOUD_ALLOW_ALL_USERS",
	slack: "SLACK_ALLOW_ALL_USERS",
	signal: "SIGNAL_ALLOW_ALL_USERS",
	email: "EMAIL_ALLOW_ALL_USERS",
	sms: "SMS_ALLOW_ALL_USERS",
	mattermost: "MATTERMOST_ALLOW_ALL_USERS",
	matrix: "MATRIX_ALLOW_ALL_USERS",
	dingtalk: "DINGTALK_ALLOW_ALL_USERS",
	feishu: "FEISHU_ALLOW_ALL_USERS",
	wecom: "WECOM_ALLOW_ALL_USERS",
	wecom_callback: "WECOM_CALLBACK_ALLOW_ALL_USERS",
	weixin: "WEIXIN_ALLOW_ALL_USERS",
	qqbot: "QQ_ALLOW_ALL_USERS",
	yuanbao: "YUANBAO_ALLOW_ALL_USERS",
};

/**
 * {PLATFORM}_ALLOW_BOTS bot-sender bypass (06 §2.5, #4466). Evaluated at
 * step 3, BEFORE the no-user-id deny: some platforms deliver bot traffic with
 * NO user id at all (Slack Workflow Builder posts arrive
 * subtype=bot_message, user=None), so deferring past step 4 would reject
 * them outright.
 *
 * Semantics: "mentions" admits bot traffic whose message addresses the bot;
 * "all" admits any bot-flagged sender; anything else (including an absent
 * variable) denies. Adapters normalize unknown values to "none".
 */
export const PLATFORM_ALLOW_BOTS_ENV: Readonly<Record<string, string>> = {
	discord: "DISCORD_ALLOW_BOTS",
	feishu: "FEISHU_ALLOW_BOTS",
	telegram: "TELEGRAM_ALLOW_BOTS",
	slack: "SLACK_ALLOW_BOTS",
};

/**
 * Pairing grant mirror targets (gateway/pairing.py:_PLATFORM_ALLOWLIST_ENV,
 * #23778): when an operator already runs an allowlist for a platform,
 * approving a pairing code ALSO writes the user into that allowlist (and
 * revocation removes them again), keeping the operator's list the single
 * visible source of truth. Absent platforms keep the pairing store as the
 * sole grant record, honored by the §2.1 step-7 union.
 */
export const PAIRING_ALLOWLIST_ENV: Readonly<Record<string, string>> = {
	telegram: "TELEGRAM_ALLOWED_USERS",
	discord: "DISCORD_ALLOWED_USERS",
	whatsapp: "WHATSAPP_ALLOWED_USERS",
	whatsapp_cloud: "WHATSAPP_CLOUD_ALLOWED_USERS",
	slack: "SLACK_ALLOWED_USERS",
	signal: "SIGNAL_ALLOWED_USERS",
	email: "EMAIL_ALLOWED_USERS",
	sms: "SMS_ALLOWED_USERS",
	mattermost: "MATTERMOST_ALLOWED_USERS",
	matrix: "MATRIX_ALLOWED_USERS",
	dingtalk: "DINGTALK_ALLOWED_USERS",
	feishu: "FEISHU_ALLOWED_USERS",
	wecom: "WECOM_ALLOWED_USERS",
	wecom_callback: "WECOM_CALLBACK_ALLOWED_USERS",
	weixin: "WEIXIN_ALLOWED_USERS",
	qqbot: "QQ_ALLOWED_USERS",
	yuanbao: "YUANBAO_ALLOWED_USERS",
};

/**
 * Parse allowlist values from config or env into a set of strings — parity of
 * gateway/authz_mixin.py:_coerce_allow_set. Handles YAML list inputs AND
 * comma-separated scalar strings ("123,456" → {"123","456"}, never per-char).
 */
export function coerceAllowSet(raw: unknown): Set<string> {
	if (raw === null || raw === undefined) return new Set();
	if (Array.isArray(raw)) {
		return new Set(
			raw.map((part) => String(part).trim()).filter((part) => part !== ""),
		);
	}
	const text = String(raw);
	const out = new Set<string>();
	for (const part of text.split(",")) {
		const trimmed = part.trim();
		if (trimmed !== "") out.add(trimmed);
	}
	return out;
}

/** Truthy vocabulary for ALLOW_ALL-style flags (parity `.lower() in {"true","1","yes"}`). */
export function isTruthyFlag(raw: string | undefined | null): boolean {
	const v = (raw ?? "").trim().toLowerCase();
	return v === "true" || v === "1" || v === "yes";
}

export type AllowBotsMode = "mentions" | "all" | "none";

/**
 * Normalize a {P}_ALLOW_BOTS value. Unknown/absent values normalize to
 * "none" (parity plugins/platforms/slack/adapter.py::_slack_allow_bots —
 * adapters warn on the coercion; the chain itself just denies).
 */
export function normalizeAllowBotsValue(
	raw: string | undefined | null,
): AllowBotsMode {
	const v = (raw ?? "").trim().toLowerCase();
	if (v === "mentions" || v === "all") return v;
	return "none";
}
